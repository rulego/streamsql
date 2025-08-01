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

// 窗口相关常量
const (
	WindowStartField = "window_start"
	WindowEndField   = "window_end"
)

// 溢出策略常量
const (
	StrategyDrop    = "drop"
	StrategyBlock   = "block"
	StrategyExpand  = "expand"
	StrategyPersist = "persist"
)

// 统计信息字段常量
const (
	StatsInputCount    = "input_count"
	StatsOutputCount   = "output_count"
	StatsDroppedCount  = "dropped_count"
	StatsDataChanLen   = "data_chan_len"
	StatsDataChanCap   = "data_chan_cap"
	StatsResultChanLen = "result_chan_len"
	StatsResultChanCap = "result_chan_cap"
	StatsSinkPoolLen   = "sink_pool_len"
	StatsSinkPoolCap   = "sink_pool_cap"
	StatsActiveRetries = "active_retries"
	StatsExpanding     = "expanding"
)

// 详细统计信息字段常量
const (
	StatsBasicStats       = "basic_stats"
	StatsDataChanUsage    = "data_chan_usage"
	StatsResultChanUsage  = "result_chan_usage"
	StatsSinkPoolUsage    = "sink_pool_usage"
	StatsProcessRate      = "process_rate"
	StatsDropRate         = "drop_rate"
	StatsPerformanceLevel = "performance_level"
)

// 性能级别常量
const (
	PerformanceLevelCritical     = "CRITICAL"
	PerformanceLevelWarning      = "WARNING"
	PerformanceLevelHighLoad     = "HIGH_LOAD"
	PerformanceLevelModerateLoad = "MODERATE_LOAD"
	PerformanceLevelOptimal      = "OPTIMAL"
)

// 持久化相关常量
const (
	PersistenceEnabled       = "enabled"
	PersistenceMessage       = "message"
	PersistenceNotEnabledMsg = "persistence not enabled"
	PerformanceConfigKey     = "performanceConfig"
)

// SQL关键字常量
const (
	SQLKeywordCase = "CASE"
)

type Stream struct {
	dataChan       chan interface{}
	filter         condition.Condition
	Window         window.Window
	aggregator     aggregator.Aggregator
	config         types.Config
	sinks          []func(interface{})
	resultChan     chan interface{} // 结果通道
	seenResults    *sync.Map
	done           chan struct{} // 用于关闭处理协程
	sinkWorkerPool chan func()   // Sink工作池，避免阻塞

	// 新增：线程安全控制
	dataChanMux      sync.RWMutex // 保护dataChan访问的读写锁
	sinksMux         sync.RWMutex // 保护sinks访问的读写锁
	expansionMux     sync.Mutex   // 防止并发扩容的互斥锁
	retryMux         sync.Mutex   // 控制持久化重试的互斥锁
	expanding        int32        // 扩容状态标记，使用原子操作
	activeRetries    int32        // 活跃重试计数，使用原子操作
	maxRetryRoutines int32        // 最大重试协程数限制

	// 性能监控指标
	inputCount   int64 // 输入数据计数
	outputCount  int64 // 输出结果计数
	droppedCount int64 // 丢弃数据计数

	// 数据丢失策略配置
	allowDataDrop      bool                // 是否允许数据丢失
	blockingTimeout    time.Duration       // 阻塞超时时间
	overflowStrategy   string              // 溢出策略: "drop", "block", "expand", "persist"
	persistenceManager *PersistenceManager // 持久化管理器

	// 预编译的AddData函数指针，避免每次switch判断
	addDataFunc func(interface{}) // 根据策略预设的函数指针

	// 预编译字段处理信息，避免重复解析
	compiledFieldInfo map[string]*fieldProcessInfo      // 字段处理信息缓存
	compiledExprInfo  map[string]*expressionProcessInfo // 表达式处理信息缓存

}

// NewStream 使用统一配置创建Stream
func NewStream(config types.Config) (*Stream, error) {
	factory := NewStreamFactory()
	return factory.CreateStream(config)
}

// NewStreamWithHighPerformance 创建高性能Stream
func NewStreamWithHighPerformance(config types.Config) (*Stream, error) {
	factory := NewStreamFactory()
	return factory.CreateHighPerformanceStream(config)
}

// NewStreamWithLowLatency 创建低延迟Stream
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

func (s *Stream) Emit(data interface{}) {
	atomic.AddInt64(&s.inputCount, 1)
	// 直接调用预编译的函数指针，避免switch判断
	s.addDataFunc(data)
}

// Stop 停止流处理
func (s *Stream) Stop() {
	close(s.done)

	// 停止持久化管理器
	if s.persistenceManager != nil {
		if err := s.persistenceManager.Stop(); err != nil {
			logger.Error("Failed to stop persistence manager: %v", err)
		}
	}
}

// GetStats, GetDetailedStats, ResetStats, expandDataChannel, persistAndRetryData 方法已移动到stats_manager.go和data_handler.go文件

// LoadAndReprocessPersistedData 加载并重新处理持久化数据
func (s *Stream) LoadAndReprocessPersistedData() error {
	if s.persistenceManager == nil {
		return fmt.Errorf("persistence manager not initialized")
	}

	// 加载持久化数据
	persistedData, err := s.persistenceManager.LoadPersistedData()
	if err != nil {
		return fmt.Errorf("failed to load persisted data: %w", err)
	}

	if len(persistedData) == 0 {
		logger.Info("No persistent data to recover")
		return nil
	}

	logger.Info("Start reprocessing %d persistent data records", len(persistedData))

	// 重新处理每条数据
	successCount := 0
	for i, data := range persistedData {
		// 使用线程安全方式尝试发送数据
		if s.safeSendToDataChan(data) {
			successCount++
			continue
		}

		// 如果通道还是满的，等待一小段时间再试
		time.Sleep(10 * time.Millisecond)
		if s.safeSendToDataChan(data) {
			successCount++
		} else {
			logger.Warn("Failed to recover data record %d, channel still full", i+1)
		}
	}

	logger.Info("Persistent data recovery completed: successful %d/%d records", successCount, len(persistedData))
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
func (s *Stream) ProcessSync(data interface{}) (interface{}, error) {
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
func (s *Stream) processDirectDataSync(data interface{}) (interface{}, error) {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		atomic.AddInt64(&s.droppedCount, 1)
		return nil, fmt.Errorf("Unsupported data type:%T", data)
	}

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
			s.processSimpleField(fieldSpec, dataMap, data, result)
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
