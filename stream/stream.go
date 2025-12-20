/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stream

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/condition"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/window"
)

// Window related constants
const (
	WindowStartField = "window_start"
	WindowEndField   = "window_end"
)

// Performance level constants
const (
	PerformanceLevelCritical     = "CRITICAL"
	PerformanceLevelWarning      = "WARNING"
	PerformanceLevelHighLoad     = "HIGH_LOAD"
	PerformanceLevelModerateLoad = "MODERATE_LOAD"
	PerformanceLevelOptimal      = "OPTIMAL"
)

// SQL keyword constants
const (
	SQLKeywordCase = "CASE"
)
const (
	PerformanceConfigKey = "performanceConfig"
)

type Stream struct {
	dataChan       chan map[string]interface{}
	filter         condition.Condition
	Window         window.Window
	aggregator     aggregator.Aggregator
	config         types.Config
	sinks          []func([]map[string]interface{})
	syncSinks      []func([]map[string]interface{}) // Synchronous sinks, executed sequentially
	resultChan     chan []map[string]interface{}    // Result channel
	seenResults    *sync.Map
	done           chan struct{} // Used to close processing goroutines
	sinkWorkerPool chan func()   // Sink worker pool to avoid blocking

	// Thread safety control
	dataChanMux      sync.RWMutex // Read-write lock protecting dataChan access
	sinksMux         sync.RWMutex // Read-write lock protecting sinks access
	expansionMux     sync.Mutex   // Mutex preventing concurrent expansion
	retryMux         sync.Mutex   // Mutex controlling persistence retry
	expanding        int32        // Expansion status flag using atomic operations
	activeRetries    int32        // Active retry count using atomic operations
	maxRetryRoutines int32        // Maximum retry goroutine limit
	stopped          int32        // Stop status flag using atomic operations

	// Performance monitoring metrics
	inputCount   int64 // Input data count
	outputCount  int64 // Output result count
	droppedCount int64 // Dropped data count

	// Log throttling fields for "Result channel is full" messages
	lastDropLogTime int64 // Last time drop log was printed (unix timestamp)
	dropLogCount    int64 // Count of drops since last log

	// Data loss strategy configuration
	allowDataDrop    bool          // Whether to allow data loss
	blockingTimeout  time.Duration // Blocking timeout duration
	overflowStrategy string        // Overflow strategy: "drop", "block", "expand", "persist"

	// Data processing strategy using strategy pattern for better extensibility
	dataStrategy DataProcessingStrategy // Data processing strategy instance

	// Pre-compiled field processing information to avoid repeated parsing
	compiledFieldInfo map[string]*fieldProcessInfo      // Field processing information cache
	compiledExprInfo  map[string]*expressionProcessInfo // Expression processing information cache

	// Unnest function optimization flags
	// hasUnnestFunction 标识查询是否使用了 unnest 函数，在预处理阶段确定
	// 用于优化 expandUnnestResults 函数的性能，避免不必要的字段遍历检查
	hasUnnestFunction bool // Whether the query uses unnest function, determined during preprocessing

}

// NewStream creates Stream using unified configuration
func NewStream(config types.Config) (*Stream, error) {
	factory := NewStreamFactory()
	return factory.CreateStream(config)
}

// NewStreamWithHighPerformance creates high-performance Stream
func NewStreamWithHighPerformance(config types.Config) (*Stream, error) {
	factory := NewStreamFactory()
	return factory.CreateHighPerformanceStream(config)
}

// NewStreamWithLowLatency creates low-latency Stream
func NewStreamWithLowLatency(config types.Config) (*Stream, error) {
	factory := NewStreamFactory()
	return factory.CreateLowLatencyStream(config)
}

// NewStreamWithCustomPerformance creates Stream with custom performance configuration
func NewStreamWithCustomPerformance(config types.Config, perfConfig types.PerformanceConfig) (*Stream, error) {
	factory := NewStreamFactory()
	return factory.CreateCustomPerformanceStream(config, perfConfig)
}

// RegisterFilter registers filter condition, supporting backtick identifiers, LIKE syntax and IS NULL syntax
func (s *Stream) RegisterFilter(conditionStr string) error {
	if strings.TrimSpace(conditionStr) == "" {
		return nil
	}

	processedCondition := s.preprocessFilterCondition(conditionStr)
	filter, err := condition.NewExprCondition(processedCondition)
	if err != nil {
		return fmt.Errorf("compile filter error: %w", err)
	}
	s.filter = filter
	return nil
}

// preprocessFilterCondition preprocesses filter condition
func (s *Stream) preprocessFilterCondition(conditionStr string) string {
	processedCondition := conditionStr
	bridge := functions.GetExprBridge()

	// First preprocess backtick identifiers, remove backticks
	if bridge.ContainsBacktickIdentifiers(conditionStr) {
		if processed, err := bridge.PreprocessBacktickIdentifiers(conditionStr); err == nil {
			processedCondition = processed
		}
	}

	// Preprocess LIKE syntax, convert to expr-lang understandable form
	if bridge.ContainsLikeOperator(processedCondition) {
		if processed, err := bridge.PreprocessLikeExpression(processedCondition); err == nil {
			processedCondition = processed
		}
	}

	// Preprocess IS NULL and IS NOT NULL syntax
	if bridge.ContainsIsNullOperator(processedCondition) {
		if processed, err := bridge.PreprocessIsNullExpression(processedCondition); err == nil {
			processedCondition = processed
		}
	}

	return processedCondition
}

// convertToAggregationFields converts old format configuration to new AggregationField format
func convertToAggregationFields(selectFields map[string]aggregator.AggregateType, fieldAlias map[string]string) []aggregator.AggregationField {
	var fields []aggregator.AggregationField

	for outputAlias, aggType := range selectFields {
		field := aggregator.AggregationField{
			AggregateType: aggType,
			OutputAlias:   outputAlias,
		}

		// Find corresponding input field name
		if inputField, exists := fieldAlias[outputAlias]; exists {
			field.InputField = inputField
		} else {
			// If no alias mapping, input field name equals output alias
			field.InputField = outputAlias
		}

		fields = append(fields, field)
	}

	return fields
}

func (s *Stream) Start() {
	// Create data processor and start
	processor := NewDataProcessor(s)
	go processor.Process()
}

// Emit adds data to stream processing pipeline
// Parameters:
//   - data: data to be processed, must be map[string]interface{} type
func (s *Stream) Emit(data map[string]interface{}) {
	atomic.AddInt64(&s.inputCount, 1)
	// Use strategy pattern to process data, providing better extensibility
	s.dataStrategy.ProcessData(data)
}

// Stop stops stream processing
func (s *Stream) Stop() {
	// Use atomic operation to prevent duplicate stops
	if !atomic.CompareAndSwapInt32(&s.stopped, 0, 1) {
		return // Already stopped, return directly
	}

	close(s.done)

	// Stop window operations first to prevent new window triggers
	if s.Window != nil {
		s.Window.Stop()
	}

	// Close dataChan to signal DataProcessor to exit
	s.dataChanMux.Lock()
	if s.dataChan != nil {
		close(s.dataChan)
		s.dataChan = nil // Set to nil to prevent sending to closed channel
	}
	s.dataChanMux.Unlock()

	// Stop and clean up data processing strategy resources
	if s.dataStrategy != nil {
		if err := s.dataStrategy.Stop(); err != nil {
			logger.Error("Failed to stop data strategy: %v", err)
		}
	}
}

// IsAggregationQuery checks if current stream is an aggregation query
func (s *Stream) IsAggregationQuery() bool {
	return s.config.NeedWindow
}

// ProcessSync synchronously processes single data, returns result immediately
// Only applicable to non-aggregation queries, aggregation queries will return error
// Parameters:
//   - data: data to be processed, must be map[string]interface{} type
//
// Returns:
//   - map[string]interface{}: processed result data, returns nil if doesn't match filter condition
//   - error: processing error, returns error for aggregation queries
func (s *Stream) ProcessSync(data map[string]interface{}) (map[string]interface{}, error) {
	// Check if it's an aggregation query
	if s.config.NeedWindow {
		return nil, fmt.Errorf("Synchronous processing is not supported for aggregation queries.")
	}

	// Apply filter condition
	if s.filter != nil && !s.filter.Evaluate(data) {
		return nil, nil // Doesn't match filter condition, return nil
	}

	// Directly process data and return result
	return s.processDirectDataSync(data)
}

// processDirectDataSync synchronous version of direct data processing
// Parameters:
//   - data: data to be processed, must be map[string]interface{} type
//
// Returns:
//   - map[string]interface{}: processed result data
//   - error: processing error
func (s *Stream) processDirectDataSync(data map[string]interface{}) (map[string]interface{}, error) {
	// Directly use the passed map, no type conversion needed
	dataMap := data

	// Create result map, pre-allocate appropriate capacity
	estimatedSize := len(s.config.FieldExpressions) + len(s.config.SimpleFields)
	if estimatedSize < 8 {
		estimatedSize = 8 // Minimum capacity
	}
	result := make(map[string]interface{}, estimatedSize)

	// Process expression fields
	for fieldName := range s.config.FieldExpressions {
		s.processExpressionField(fieldName, dataMap, result)
	}

	// Use pre-compiled field information to process SimpleFields
	if len(s.config.SimpleFields) > 0 {
		for _, fieldSpec := range s.config.SimpleFields {
			s.processSimpleField(fieldSpec, dataMap, dataMap, result)
		}
	} else if len(s.config.FieldExpressions) == 0 {
		// If no fields specified and no expression fields, keep all fields
		for k, v := range dataMap {
			result[k] = v
		}
	}

	// Increment output count
	atomic.AddInt64(&s.outputCount, 1)

	// Wrap result as array format, maintain consistency with async mode
	results := []map[string]interface{}{result}

	// Trigger AddSink callback, maintain consistency between sync and async modes
	// This way users can get both sync results and async callbacks
	s.callSinksAsync(results)

	return result, nil
}
