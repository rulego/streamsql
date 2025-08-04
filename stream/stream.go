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

// Persistence related constants
const (
	PersistenceEnabled       = "enabled"
	PersistenceMessage       = "message"
	PersistenceNotEnabledMsg = "persistence not enabled"
	PerformanceConfigKey     = "performanceConfig"
)

// SQL keyword constants
const (
	SQLKeywordCase = "CASE"
)

type Stream struct {
	dataChan       chan map[string]interface{}
	filter         condition.Condition
	Window         window.Window
	aggregator     aggregator.Aggregator
	config         types.Config
	sinks          []func([]map[string]interface{})
	resultChan     chan []map[string]interface{} // Result channel
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

	// Performance monitoring metrics
	inputCount   int64 // Input data count
	outputCount  int64 // Output result count
	droppedCount int64 // Dropped data count

	// Data loss strategy configuration
	allowDataDrop      bool                // Whether to allow data loss
	blockingTimeout    time.Duration       // Blocking timeout duration
	overflowStrategy   string              // Overflow strategy: "drop", "block", "expand", "persist"
	persistenceManager *PersistenceManager // Persistence manager

	// Data processing strategy using strategy pattern for better extensibility
	dataStrategy DataProcessingStrategy // Data processing strategy instance

	// Pre-compiled field processing information to avoid repeated parsing
	compiledFieldInfo map[string]*fieldProcessInfo      // Field processing information cache
	compiledExprInfo  map[string]*expressionProcessInfo // Expression processing information cache

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

// NewStreamWithZeroDataLoss 创建零数据丢失Stream
func NewStreamWithZeroDataLoss(config types.Config) (*Stream, error) {
	factory := NewStreamFactory()
	return factory.CreateZeroDataLossStream(config)
}

// NewStreamWithCustomPerformance 创建自定义性能配置的Stream
func NewStreamWithCustomPerformance(config types.Config, perfConfig types.PerformanceConfig) (*Stream, error) {
	factory := NewStreamFactory()
	return factory.CreateCustomPerformanceStream(config, perfConfig)
}

// RegisterFilter 注册过滤条件，支持反引号标识符、LIKE语法和IS NULL语法
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

// preprocessFilterCondition 预处理过滤条件
func (s *Stream) preprocessFilterCondition(conditionStr string) string {
	processedCondition := conditionStr
	bridge := functions.GetExprBridge()

	// 首先预处理反引号标识符，去除反引号
	if bridge.ContainsBacktickIdentifiers(conditionStr) {
		if processed, err := bridge.PreprocessBacktickIdentifiers(conditionStr); err == nil {
			processedCondition = processed
		}
	}

	// 预处理LIKE语法，转换为expr-lang可理解的形式
	if bridge.ContainsLikeOperator(processedCondition) {
		if processed, err := bridge.PreprocessLikeExpression(processedCondition); err == nil {
			processedCondition = processed
		}
	}

	// 预处理IS NULL和IS NOT NULL语法
	if bridge.ContainsIsNullOperator(processedCondition) {
		if processed, err := bridge.PreprocessIsNullExpression(processedCondition); err == nil {
			processedCondition = processed
		}
	}

	return processedCondition
}

// convertToAggregationFields 将旧格式的配置转换为新的AggregationField格式
func convertToAggregationFields(selectFields map[string]aggregator.AggregateType, fieldAlias map[string]string) []aggregator.AggregationField {
	var fields []aggregator.AggregationField

	for outputAlias, aggType := range selectFields {
		field := aggregator.AggregationField{
			AggregateType: aggType,
			OutputAlias:   outputAlias,
		}

		// 查找对应的输入字段名
		if inputField, exists := fieldAlias[outputAlias]; exists {
			field.InputField = inputField
		} else {
			// 如果没有别名映射，输入字段名等于输出别名
			field.InputField = outputAlias
		}

		fields = append(fields, field)
	}

	return fields
}

func (s *Stream) Start() {
	// 创建数据处理器并启动
	processor := NewDataProcessor(s)
	go processor.Process()
}

// Emit 添加数据到流处理管道
// 参数:
//   - data: 要处理的数据，必须是map[string]interface{}类型
func (s *Stream) Emit(data map[string]interface{}) {
	atomic.AddInt64(&s.inputCount, 1)
	// 使用策略模式处理数据，提供更好的扩展性
	s.dataStrategy.ProcessData(data)
}

// Stop 停止流处理
func (s *Stream) Stop() {
	close(s.done)

	// 停止并清理数据处理策略资源
	if s.dataStrategy != nil {
		if err := s.dataStrategy.Stop(); err != nil {
			logger.Error("Failed to stop data strategy: %v", err)
		}
	}

	// 停止持久化管理器
	if s.persistenceManager != nil {
		if err := s.persistenceManager.Stop(); err != nil {
			logger.Error("Failed to stop persistence manager: %v", err)
		}
	}
}

// LoadAndReprocessPersistedData 加载并重新处理持久化数据
func (s *Stream) LoadAndReprocessPersistedData() error {
	if s.persistenceManager == nil {
		return fmt.Errorf("persistence manager not initialized")
	}

	// 加载持久化数据
	err := s.persistenceManager.LoadAndRecoverData()
	if err != nil {
		return fmt.Errorf("failed to load persisted data: %w", err)
	}

	// 检查是否有恢复数据
	if !s.persistenceManager.IsInRecoveryMode() {
		logger.Info("No persistent data to recover")
		return nil
	}

	logger.Info("Starting persistent data recovery process")

	// 启动恢复处理协程
	go s.checkAndProcessRecoveryData()

	logger.Info("Persistent data recovery process started")
	return nil
}

// GetPersistenceStats 获取持久化统计信息
func (s *Stream) GetPersistenceStats() map[string]interface{} {
	if s.persistenceManager == nil {
		return map[string]interface{}{
			PersistenceEnabled: false,
			PersistenceMessage: PersistenceNotEnabledMsg,
		}
	}

	stats := s.persistenceManager.GetStats()
	stats[PersistenceEnabled] = true
	return stats
}

// IsAggregationQuery 检查当前流是否为聚合查询
func (s *Stream) IsAggregationQuery() bool {
	return s.config.NeedWindow
}

// ProcessSync 同步处理单条数据，立即返回结果
// 仅适用于非聚合查询，聚合查询会返回错误
// 参数:
//   - data: 要处理的数据，必须是map[string]interface{}类型
//
// 返回值:
//   - map[string]interface{}: 处理后的结果数据，如果不匹配过滤条件返回nil
//   - error: 处理错误，如果是聚合查询会返回错误
func (s *Stream) ProcessSync(data map[string]interface{}) (map[string]interface{}, error) {
	// 检查是否为聚合查询
	if s.config.NeedWindow {
		return nil, fmt.Errorf("Synchronous processing is not supported for aggregation queries.")
	}

	// 应用过滤条件
	if s.filter != nil && !s.filter.Evaluate(data) {
		return nil, nil // 不匹配过滤条件，返回nil
	}

	// 直接处理数据并返回结果
	return s.processDirectDataSync(data)
}

// processDirectDataSync 同步版本的直接数据处理
// 参数:
//   - data: 要处理的数据，必须是map[string]interface{}类型
//
// 返回值:
//   - map[string]interface{}: 处理后的结果数据
//   - error: 处理错误
func (s *Stream) processDirectDataSync(data map[string]interface{}) (map[string]interface{}, error) {
	// 直接使用传入的map，无需类型转换
	dataMap := data

	// 创建结果map，预分配合适容量
	estimatedSize := len(s.config.FieldExpressions) + len(s.config.SimpleFields)
	if estimatedSize < 8 {
		estimatedSize = 8 // 最小容量
	}
	result := make(map[string]interface{}, estimatedSize)

	// 处理表达式字段
	for fieldName := range s.config.FieldExpressions {
		s.processExpressionField(fieldName, dataMap, result)
	}

	// 使用预编译的字段信息处理SimpleFields
	if len(s.config.SimpleFields) > 0 {
		for _, fieldSpec := range s.config.SimpleFields {
			s.processSimpleField(fieldSpec, dataMap, dataMap, result)
		}
	} else if len(s.config.FieldExpressions) == 0 {
		// 如果没有指定字段且没有表达式字段，保留所有字段
		for k, v := range dataMap {
			result[k] = v
		}
	}

	// 增加输出计数
	atomic.AddInt64(&s.outputCount, 1)

	// 包装结果为数组格式，保持与异步模式的一致性
	results := []map[string]interface{}{result}

	// 触发 AddSink 回调，保持同步和异步模式的一致性
	// 这样用户可以同时获得同步结果和异步回调
	s.callSinksAsync(results)

	return result, nil
}
