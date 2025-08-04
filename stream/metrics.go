package stream

import (
	"sync/atomic"
)

// Statistics field constants
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

// Detailed statistics field constants
const (
	BasicStats       = "basic_stats"
	DataChanUsage    = "data_chan_usage"
	ResultChanUsage  = "result_chan_usage"
	SinkPoolUsage    = "sink_pool_usage"
	ProcessRate      = "process_rate"
	DropRate         = "drop_rate"
	PerformanceLevel = "performance_level"
)

// Performance level constants are defined in stream.go

// AssessPerformanceLevel evaluates current performance level
// Assesses stream processing performance level based on data usage rate and drop rate
func AssessPerformanceLevel(dataUsage, dropRate float64) string {
	switch {
	case dropRate > 50:
		return PerformanceLevelCritical // Critical performance issue
	case dropRate > 20:
		return PerformanceLevelWarning // Performance warning
	case dataUsage > 90:
		return PerformanceLevelHighLoad // High load
	case dataUsage > 70:
		return PerformanceLevelModerateLoad // Moderate load
	default:
		return PerformanceLevelOptimal // Optimal state
	}
}

// StatsCollector statistics information collector
// Provides thread-safe statistics collection functionality
type StatsCollector struct {
	inputCount   int64
	outputCount  int64
	droppedCount int64
}

// NewStatsCollector creates a new statistics collector
func NewStatsCollector() *StatsCollector {
	return &StatsCollector{}
}

// IncrementInput increments input count
func (sc *StatsCollector) IncrementInput() {
	atomic.AddInt64(&sc.inputCount, 1)
}

// IncrementOutput increments output count
func (sc *StatsCollector) IncrementOutput() {
	atomic.AddInt64(&sc.outputCount, 1)
}

// IncrementDropped increments dropped count
func (sc *StatsCollector) IncrementDropped() {
	atomic.AddInt64(&sc.droppedCount, 1)
}

// GetInputCount gets input count
func (sc *StatsCollector) GetInputCount() int64 {
	return atomic.LoadInt64(&sc.inputCount)
}

// GetOutputCount gets output count
func (sc *StatsCollector) GetOutputCount() int64 {
	return atomic.LoadInt64(&sc.outputCount)
}

// GetDroppedCount gets dropped count
func (sc *StatsCollector) GetDroppedCount() int64 {
	return atomic.LoadInt64(&sc.droppedCount)
}

// Reset resets statistics information
func (sc *StatsCollector) Reset() {
	atomic.StoreInt64(&sc.inputCount, 0)
	atomic.StoreInt64(&sc.outputCount, 0)
	atomic.StoreInt64(&sc.droppedCount, 0)
}

// GetBasicStats gets basic statistics information
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

// GetDetailedStats gets detailed performance statistics
func (sc *StatsCollector) GetDetailedStats(basicStats map[string]int64) map[string]interface{} {
	// Calculate usage rates
	dataUsage := float64(basicStats[DataChanLen]) / float64(basicStats[DataChanCap]) * 100
	resultUsage := float64(basicStats[ResultChanLen]) / float64(basicStats[ResultChanCap]) * 100
	sinkUsage := float64(basicStats[SinkPoolLen]) / float64(basicStats[SinkPoolCap]) * 100

	// Calculate efficiency metrics
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