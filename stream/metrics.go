package stream

import (
	"sync/atomic"
)

// 统计信息字段常量
const (
	InputCount    = "input_count"
	OutputCount   = "output_count"
	DroppedCount  = "dropped_count"
	DataChanLen   = "data_chan_len"
	DataChanCap   = "data_chan_cap"
	ResultChanLen = "result_chan_len"
	ResultChanCap = "result_chan_cap"
	SinkPoolLen   = "sink_pool_len"
	SinkPoolCap   = "sink_pool_cap"
	ActiveRetries = "active_retries"
	Expanding     = "expanding"
)

// 详细统计信息字段常量
const (
	BasicStats       = "basic_stats"
	DataChanUsage    = "data_chan_usage"
	ResultChanUsage  = "result_chan_usage"
	SinkPoolUsage    = "sink_pool_usage"
	ProcessRate      = "process_rate"
	DropRate         = "drop_rate"
	PerformanceLevel = "performance_level"
)

// 性能级别常量已在 stream.go 中定义

// AssessPerformanceLevel 评估当前性能水平
// 根据数据使用率和丢弃率评估流处理的性能等级
func AssessPerformanceLevel(dataUsage, dropRate float64) string {
	switch {
	case dropRate > 50:
		return PerformanceLevelCritical // 严重性能问题
	case dropRate > 20:
		return PerformanceLevelWarning // 性能警告
	case dataUsage > 90:
		return PerformanceLevelHighLoad // 高负载
	case dataUsage > 70:
		return PerformanceLevelModerateLoad // 中等负载
	default:
		return PerformanceLevelOptimal // 最佳状态
	}
}

// StatsCollector 统计信息收集器
// 提供线程安全的统计信息收集功能
type StatsCollector struct {
	inputCount   int64
	outputCount  int64
	droppedCount int64
}

// NewStatsCollector 创建新的统计信息收集器
func NewStatsCollector() *StatsCollector {
	return &StatsCollector{}
}

// IncrementInput 增加输入计数
func (sc *StatsCollector) IncrementInput() {
	atomic.AddInt64(&sc.inputCount, 1)
}

// IncrementOutput 增加输出计数
func (sc *StatsCollector) IncrementOutput() {
	atomic.AddInt64(&sc.outputCount, 1)
}

// IncrementDropped 增加丢弃计数
func (sc *StatsCollector) IncrementDropped() {
	atomic.AddInt64(&sc.droppedCount, 1)
}

// GetInputCount 获取输入计数
func (sc *StatsCollector) GetInputCount() int64 {
	return atomic.LoadInt64(&sc.inputCount)
}

// GetOutputCount 获取输出计数
func (sc *StatsCollector) GetOutputCount() int64 {
	return atomic.LoadInt64(&sc.outputCount)
}

// GetDroppedCount 获取丢弃计数
func (sc *StatsCollector) GetDroppedCount() int64 {
	return atomic.LoadInt64(&sc.droppedCount)
}

// Reset 重置统计信息
func (sc *StatsCollector) Reset() {
	atomic.StoreInt64(&sc.inputCount, 0)
	atomic.StoreInt64(&sc.outputCount, 0)
	atomic.StoreInt64(&sc.droppedCount, 0)
}

// GetBasicStats 获取基础统计信息
func (sc *StatsCollector) GetBasicStats(dataChanLen, dataChanCap, resultChanLen, resultChanCap, sinkPoolLen, sinkPoolCap int, activeRetries, expanding int32) map[string]int64 {
	return map[string]int64{
		InputCount:    sc.GetInputCount(),
		OutputCount:   sc.GetOutputCount(),
		DroppedCount:  sc.GetDroppedCount(),
		DataChanLen:   int64(dataChanLen),
		DataChanCap:   int64(dataChanCap),
		ResultChanLen: int64(resultChanLen),
		ResultChanCap: int64(resultChanCap),
		SinkPoolLen:   int64(sinkPoolLen),
		SinkPoolCap:   int64(sinkPoolCap),
		ActiveRetries: int64(activeRetries),
		Expanding:     int64(expanding),
	}
}

// GetDetailedStats 获取详细的性能统计信息
func (sc *StatsCollector) GetDetailedStats(basicStats map[string]int64) map[string]interface{} {
	// 计算使用率
	dataUsage := float64(basicStats[DataChanLen]) / float64(basicStats[DataChanCap]) * 100
	resultUsage := float64(basicStats[ResultChanLen]) / float64(basicStats[ResultChanCap]) * 100
	sinkUsage := float64(basicStats[SinkPoolLen]) / float64(basicStats[SinkPoolCap]) * 100

	// 计算效率指标
	var processRate float64 = 100.0
	var dropRate float64 = 0.0

	if basicStats[InputCount] > 0 {
		processRate = float64(basicStats[OutputCount]) / float64(basicStats[InputCount]) * 100
		dropRate = float64(basicStats[DroppedCount]) / float64(basicStats[InputCount]) * 100
	}

	return map[string]interface{}{
		BasicStats:       basicStats,
		DataChanUsage:    dataUsage,
		ResultChanUsage:  resultUsage,
		SinkPoolUsage:    sinkUsage,
		ProcessRate:      processRate,
		DropRate:         dropRate,
		PerformanceLevel: AssessPerformanceLevel(dataUsage, dropRate),
	}
}