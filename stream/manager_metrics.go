package stream

import (
	"sync/atomic"
)

// StatsManager 统计信息管理器
type StatsManager struct {
	stream *Stream
	statsCollector *StatsCollector
}

// NewStatsManager 创建统计信息管理器
func NewStatsManager(stream *Stream) *StatsManager {
	return &StatsManager{
		stream: stream,
		statsCollector: NewStatsCollector(),
	}
}

// GetStats 获取流处理统计信息 (线程安全版本)
func (s *Stream) GetStats() map[string]int64 {
	// 线程安全地获取dataChan状态
	s.dataChanMux.RLock()
	dataChanLen := int64(len(s.dataChan))
	dataChanCap := int64(cap(s.dataChan))
	s.dataChanMux.RUnlock()

	return map[string]int64{
		InputCount:    atomic.LoadInt64(&s.inputCount),
		OutputCount:   atomic.LoadInt64(&s.outputCount),
		DroppedCount:  atomic.LoadInt64(&s.droppedCount),
		DataChanLen:   dataChanLen,
		DataChanCap:   dataChanCap,
		ResultChanLen: int64(len(s.resultChan)),
		ResultChanCap: int64(cap(s.resultChan)),
		SinkPoolLen:   int64(len(s.sinkWorkerPool)),
		SinkPoolCap:   int64(cap(s.sinkWorkerPool)),
		ActiveRetries: int64(atomic.LoadInt32(&s.activeRetries)),
		Expanding:     int64(atomic.LoadInt32(&s.expanding)),
	}
}

// GetDetailedStats 获取详细的性能统计信息
func (s *Stream) GetDetailedStats() map[string]interface{} {
	basicStats := s.GetStats()

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

// ResetStats 重置统计信息
func (s *Stream) ResetStats() {
	atomic.StoreInt64(&s.inputCount, 0)
	atomic.StoreInt64(&s.outputCount, 0)
	atomic.StoreInt64(&s.droppedCount, 0)
}