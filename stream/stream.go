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
	"github.com/rulego/streamsql/metrics"
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
	dataChan       chan map[string]any
	filter         condition.Condition
	Window         window.Window
	aggregator     aggregator.Aggregator
	tables         *tableStore
	config         types.Config
	sinks          []func([]map[string]any)
	syncSinks      []func([]map[string]any) // Synchronous sinks, executed sequentially
	resultChan     chan []map[string]any    // Result channel
	seenResults    *sync.Map
	done           chan struct{} // Used to close processing goroutines
	sinkWorkerPool chan func()   // Sink worker pool to avoid blocking

	// Thread safety control
	dataChanMux      sync.RWMutex  // Read-write lock protecting dataChan access
	sinksMux         sync.RWMutex  // Read-write lock protecting sinks access
	expansionMux     sync.Mutex    // Mutex preventing concurrent expansion
	retryMux         sync.Mutex    // Mutex controlling persistence retry
	expanding        int32         // Expansion status flag using atomic operations
	activeRetries    int32         // Active retry count using atomic operations
	maxRetryRoutines int32         // Maximum retry goroutine limit
	stopped          int32         // Stop status flag using atomic operations
	startMu          sync.Mutex    // serializes Start's stopped-check+Add with Stop's flag set
	log              logger.Logger // per-instance logger; set at construction, immutable after

	// lifecycle tracks goroutines that run user code or sinks (data processor,
	// window-output consumer, sink workers). Stop joins them so it returns only
	// once no callback can still touch stream state.
	lifecycle sync.WaitGroup

	// Performance monitoring metrics (consolidated in metrics.Registry)
	metricsRegistry *metrics.Registry
	mInput          *metrics.Counter
	mOutput         *metrics.Counter
	mInputDropped   *metrics.Counter
	mOutputDropped  *metrics.Counter

	// Log throttling fields for "Result channel is full" messages
	lastDropLogTime int64 // Last time drop log was printed (unix timestamp)
	dropLogCount    int64 // Count of drops since last log

	// Data loss strategy configuration
	allowDataDrop    bool          // Whether to allow data loss
	blockingTimeout  time.Duration // Blocking timeout duration
	overflowStrategy string        // Overflow strategy: "drop", "block", "expand"

	// Data processing strategy using strategy pattern for better extensibility
	dataStrategy DataProcessingStrategy // Data processing strategy instance

	// Pre-compiled field processing information to avoid repeated parsing
	compiledFieldInfo map[string]*fieldProcessInfo      // Field processing information cache
	compiledExprInfo  map[string]*expressionProcessInfo // Expression processing information cache

	// groupOutputNames holds the OUTPUT column name for each GROUP BY field
	// (parallel to config.GroupFields): the SELECT AS alias if present, else the
	// join-alias-stripped name. The aggregator/global-window emit the qualified
	// group key (needed to resolve values); this maps it to the output name.
	groupOutputNames []string

	// Unnest function optimization flags
	// hasUnnestFunction 标识查询是否使用了 unnest 函数，在预处理阶段确定
	// 用于优化 expandUnnestResults 函数的性能，避免不必要的字段遍历检查
	hasUnnestFunction bool // Whether the query uses unnest function, determined during preprocessing

	// 分析函数状态机引擎，lazy 初始化。直连路径在 WHERE 前求值。
	analytic     *AnalyticEngine
	analyticOnce sync.Once
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
	// Register tracked goroutines before spawning so Stop's join always observes
	// them: one for the data processor, one for the window-output consumer of
	// windowed queries. The stopped-check + Add is serialized with Stop's flag
	// set so Add never races with Wait: a concurrent Start that observes stopped
	// simply doesn't spawn.
	s.startMu.Lock()
	if atomic.LoadInt32(&s.stopped) != 0 {
		s.startMu.Unlock()
		return
	}
	s.lifecycle.Add(1)
	if s.config.NeedWindow {
		s.lifecycle.Add(1)
	}
	s.startMu.Unlock()
	go func() {
		defer s.lifecycle.Done()
		processor.Process()
	}()
}

// Emit adds data to stream processing pipeline
// Parameters:
//   - data: data to be processed, must be map[string]any type
func (s *Stream) Emit(data map[string]any) {
	s.mInput.Inc()
	// Use strategy pattern to process data, providing better extensibility
	s.dataStrategy.ProcessData(data)
}

// Stop stops stream processing
func (s *Stream) Stop() {
	// Set the stopped flag under startMu so a concurrent Start observes it before
	// its lifecycle.Add — otherwise Add races with the Wait below.
	s.startMu.Lock()
	if !atomic.CompareAndSwapInt32(&s.stopped, 0, 1) {
		s.startMu.Unlock()
		return // Already stopped, return directly
	}
	s.startMu.Unlock()

	close(s.done)

	// Stop window operations first to prevent new window triggers
	if s.Window != nil {
		s.Window.Stop()
	}

	// Do not close dataChan: a close races with in-flight producers. Nil makes them stop.
	s.dataChanMux.Lock()
	s.dataChan = nil
	s.dataChanMux.Unlock()

	// Stop and clean up data processing strategy resources
	if s.dataStrategy != nil {
		if err := s.dataStrategy.Stop(); err != nil {
			s.log.Error("Failed to stop data strategy: %v", err)
		}
	}

	// Join all tracked goroutines so Stop returns only once no sink callback or
	// pipeline goroutine can still touch stream state. Bounded by a grace period:
	// a user sink that blocks forever cannot be interrupted (Go has no goroutine
	// kill), so it is abandoned after the grace rather than hanging the caller
	// (e.g. a rulego component Destroy).
	s.waitLifecycle()

	// Release table sources (custom sources may own background refresh goroutines).
	if s.tables != nil {
		s.tables.closeAll()
	}
}

// RegisterTableSource registers a custom table source for stream-table JOIN.
// The source's Init runs here (it may load data from a file/DB/Redis).
func (s *Stream) RegisterTableSource(src TableSource) error {
	if s.tables == nil {
		return fmt.Errorf("stream not initialized")
	}
	return s.tables.register(src)
}

// RegisterMemoryTable registers an in-memory table indexed by keyFields, for
// stream-table JOIN. keyFields order must match the JOIN ON table-side fields.
// Returns the source for incremental Upsert/Delete.
func (s *Stream) RegisterMemoryTable(name string, keyFields []string, rows []map[string]any) (*MemoryTableSource, error) {
	if s.tables == nil {
		return nil, fmt.Errorf("stream not initialized")
	}
	src := NewMemoryTableSource(name, keyFields, rows)
	if err := s.tables.register(src); err != nil {
		return nil, err
	}
	return src, nil
}

// JoinKeyFields returns the table-side key fields for a table by looking up the
// JOIN config that references it. This lets RegisterTable auto-derive the index
// key from the ON clause instead of requiring the caller to redeclare it.
// Returns an error if no JOIN references the table.
func (s *Stream) JoinKeyFields(table string) ([]string, error) {
	for _, jc := range s.config.JoinConfigs {
		if jc.Table == table {
			fields := make([]string, len(jc.OnPairs))
			for i, p := range jc.OnPairs {
				fields[i] = p.TableField
			}
			return fields, nil
		}
	}
	return nil, fmt.Errorf("table %q is not referenced by any JOIN ON clause", table)
}

// UpsertTableRow adds or replaces a row in a registered memory table.
func (s *Stream) UpsertTableRow(name string, row map[string]any) error {
	src, ok := s.tables.get(name)
	if !ok {
		return fmt.Errorf("table %q is not registered", name)
	}
	mts, ok := src.(*MemoryTableSource)
	if !ok {
		return fmt.Errorf("table %q is not an in-memory table", name)
	}
	mts.Upsert(row)
	return nil
}

// defaultStopGrace is the maximum time Stop waits for goroutines to drain.
// Only reached when a user sink blocks; well-behaved sinks drain in microseconds.
const defaultStopGrace = 5 * time.Second

// waitLifecycle blocks until every tracked goroutine exits or the grace period
// elapses. If the grace elapses a sink is likely blocked; its goroutine (and the
// watcher goroutine spawned here) continue until the sink returns.
func (s *Stream) waitLifecycle() {
	drained := make(chan struct{})
	go func() {
		s.lifecycle.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(defaultStopGrace):
		s.log.Warn("Stream.Stop: goroutines did not exit within %s; a sink may be blocked", defaultStopGrace)
	}
}

// IsAggregationQuery checks if current stream is an aggregation query
func (s *Stream) IsAggregationQuery() bool {
	return s.config.NeedWindow
}

// ensureAnalytic 懒初始化分析函数状态机引擎（SELECT 分析函数 + WHERE 占位符调用统一管理）。
func (s *Stream) ensureAnalytic() {
	s.analyticOnce.Do(func() {
		if len(s.config.AnalyticFields) == 0 && len(s.config.WhereAnalyticCalls) == 0 {
			return
		}
		all := make([]types.AnalyticField, 0, len(s.config.AnalyticFields)+len(s.config.WhereAnalyticCalls))
		all = append(all, s.config.AnalyticFields...)
		for _, wc := range s.config.WhereAnalyticCalls {
			all = append(all, types.AnalyticField{
				Alias:      wc.Placeholder,
				FuncName:   wc.FuncName,
				Expression: wc.Expression,
				Args:       wc.Args,
				Over:       wc.Over,
			})
		}
		e, err := NewAnalyticEngine(s, all)
		if err != nil {
			s.log.Error("analytic engine init failed: %v", err)
			return
		}
		s.analytic = e
	})
}

// evalAnalytic 求值分析函数并把结果注入 dataMap（供 WHERE 占位符引用），返回结果供投影。
// 在 WHERE 之前调用（分析函数最先求值，不受 WHERE 影响）。
func (s *Stream) evalAnalytic(dataMap map[string]any) map[string]any {
	s.ensureAnalytic()
	if s.analytic == nil || !s.analytic.HasFields() {
		return nil
	}
	results := s.analytic.Evaluate(dataMap)
	// SELECT 分析函数注入 dataMap：多列函数按 prefix+列名 扇出，供 WHERE/HAVING 引用。
	for _, af := range s.config.AnalyticFields {
		v, ok := results[af.Alias]
		if !ok {
			continue
		}
		if af.MultiColumn {
			if m, ok := v.(map[string]any); ok {
				for k, vv := range m {
					dataMap[k] = vv
				}
			}
			continue
		}
		dataMap[af.Alias] = v
	}
	// WHERE 占位符调用：注入占位符键值，供改写后的 WHERE 引用。
	for _, wc := range s.config.WhereAnalyticCalls {
		if v, ok := results[wc.Placeholder]; ok {
			dataMap[wc.Placeholder] = v
		}
	}
	return results
}

// projectAnalytic 把 SELECT 分析函数结果写入投影输出：单列按 alias，多列按 prefix+列名 扇出。
func (s *Stream) projectAnalytic(result map[string]any, analyticResults map[string]any) {
	if analyticResults == nil {
		return
	}
	for _, af := range s.config.AnalyticFields {
		v, ok := analyticResults[af.Alias]
		if !ok {
			continue
		}
		if af.MultiColumn {
			if m, ok := v.(map[string]any); ok {
				for k, vv := range m {
					result[k] = vv
				}
			}
			continue
		}
		// changed_col 未变化时返回 nil：投影时省略该字段（避免 null 刷屏）。
		if af.FuncName == "changed_col" && v == nil {
			continue
		}
		result[af.Alias] = v
	}
}

// hasOmitEmptyAnalytic 是否含 changed_col/changed_cols 这类变化检测函数：
// 当它们的输出全为空（无变化）时，整行抑制输出。
func (s *Stream) hasOmitEmptyAnalytic() bool {
	for _, af := range s.config.AnalyticFields {
		if af.MultiColumn || af.FuncName == "changed_col" {
			return true
		}
	}
	return false
}

// hasAnalyticFields 是否有 SELECT 分析函数字段。
func (s *Stream) hasAnalyticFields() bool { return len(s.config.AnalyticFields) > 0 }

// applyWindowAnalytic 在窗口查询里对结果行求值分析函数（状态跨窗口保留），
// 把输出合并进结果行，剥离内联聚合隐藏键。返回 false 表示该行应抑制（变化检测无变化）。
func (s *Stream) applyWindowAnalytic(row map[string]any) bool {
	s.ensureAnalytic()
	if s.analytic == nil || !s.analytic.HasFields() {
		return true
	}
	results := s.analytic.Evaluate(row)
	changedAny := false
	for _, af := range s.config.AnalyticFields {
		v, ok := results[af.Alias]
		if !ok {
			continue
		}
		if af.MultiColumn {
			if m, ok := v.(map[string]any); ok {
				for k, vv := range m {
					row[k] = vv
				}
				if len(m) > 0 {
					changedAny = true
				}
			}
			continue
		}
		// changed_col 未变化时返回 nil：投影省略该字段。
		if af.FuncName == "changed_col" && v == nil {
			continue
		}
		row[af.Alias] = v
		changedAny = true
	}
	// 剥离内联聚合隐藏键（不进最终输出）。
	for k := range row {
		if strings.HasPrefix(k, "__winagg_") {
			delete(row, k)
		}
	}
	// omitEmpty：仅选了变化检测函数且本次无变化 → 抑制整行。
	if !changedAny && s.hasOmitEmptyAnalytic() {
		return false
	}
	return true
}

// ProcessSync synchronously processes single data, returns result immediately
// Only applicable to non-aggregation queries, aggregation queries will return error
// Parameters:
//   - data: data to be processed, must be map[string]any type
//
// Returns:
//   - map[string]any: processed result data, returns nil if doesn't match filter condition
//   - error: processing error, returns error for aggregation queries
func (s *Stream) ProcessSync(data map[string]any) (map[string]any, error) {
	// Check if it's an aggregation query
	if s.config.NeedWindow {
		return nil, fmt.Errorf("Synchronous processing is not supported for aggregation queries.")
	}

	// Directly process data and return result. processDirectDataSync applies the
	// filter after JOIN enrichment so WHERE can reference joined columns.
	return s.processDirectDataSync(data)
}

// enrichData 解析流-表 JOIN 富化。返回富化后的 dataMap、是否保留、JOIN 错误。
// 无 JOIN 时零开销直返。同步直连/异步直连/窗口前置三路径共用。
func (s *Stream) enrichData(data map[string]any) (dataMap map[string]any, keep bool, err error) {
	dataMap = data
	if !s.hasJoin() {
		return dataMap, true, nil
	}
	wm, k, jerr := s.enrichJoin(data)
	if jerr != nil {
		return dataMap, false, jerr
	}
	if !k {
		return dataMap, false, nil // INNER JOIN 无匹配：丢弃
	}
	return wm, true, nil
}

// applyWhereAndAnalytic 按 WHERE 是否引用分析函数决定求值序，并应用 WHERE 过滤。
// 返回分析结果（供投影）与是否通过过滤。同步/异步直连路径共用。
func (s *Stream) applyWhereAndAnalytic(dataMap map[string]any) (analyticResults map[string]any, keep bool) {
	whereUsesAnalytic := len(s.config.WhereAnalyticCalls) > 0
	if whereUsesAnalytic {
		analyticResults = s.evalAnalytic(dataMap)
	}
	if s.filter != nil && !s.filter.Evaluate(dataMap) {
		return nil, false
	}
	if !whereUsesAnalytic {
		analyticResults = s.evalAnalytic(dataMap)
	}
	return analyticResults, true
}

// projectDirectRow 投影 SELECT 字段（表达式/简单字段/分析函数），含 omitEmpty 抑制。
// emit=false 表示该行被 omitEmpty 抑制、不应输出。同步/异步直连路径共用。
func (s *Stream) projectDirectRow(dataMap, analyticResults map[string]any) (result map[string]any, emit bool) {
	estimatedSize := len(s.config.FieldExpressions) + len(s.config.SimpleFields)
	if estimatedSize < 8 {
		estimatedSize = 8
	}
	result = make(map[string]any, estimatedSize)
	for fieldName := range s.config.FieldExpressions {
		s.processExpressionField(fieldName, dataMap, result)
	}
	if len(s.config.SimpleFields) > 0 {
		for _, fieldSpec := range s.config.SimpleFields {
			s.processSimpleField(fieldSpec, dataMap, dataMap, result)
		}
	} else if len(s.config.FieldExpressions) == 0 && len(s.config.AnalyticFields) == 0 {
		for k, v := range dataMap {
			result[k] = v
		}
	}
	s.projectAnalytic(result, analyticResults)
	if len(result) == 0 && s.hasOmitEmptyAnalytic() {
		return nil, false
	}
	return result, true
}

// processDirectDataSync synchronous version of direct data processing
// Parameters:
//   - data: data to be processed, must be map[string]any type
//
// Returns:
//   - map[string]any: processed result data
//   - error: processing error
func (s *Stream) processDirectDataSync(data map[string]any) (map[string]any, error) {
	dataMap, keep, err := s.enrichData(data)
	if err != nil {
		return nil, err
	}
	if !keep {
		return nil, nil // INNER JOIN no match: filtered
	}
	analyticResults, pass := s.applyWhereAndAnalytic(dataMap)
	if !pass {
		return nil, nil
	}
	result, emit := s.projectDirectRow(dataMap, analyticResults)
	if !emit {
		return nil, nil
	}
	s.mOutput.Inc()
	s.callSinksAsync([]map[string]any{result})
	return result, nil
}
