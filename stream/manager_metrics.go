package stream

import (
	"sync/atomic"
)

// StatsManager manages statistics information
type StatsManager struct {
	stream         *Stream
	statsCollector *StatsCollector
}

// NewStatsManager creates a new statistics manager
func NewStatsManager(stream *Stream) *StatsManager {
	return &StatsManager{
		stream:         stream,
		statsCollector: NewStatsCollector(),
	}
}

// GetStats gets stream processing statistics (thread-safe version)
func (s *Stream) GetStats() map[string]int64 {
	// Thread-safely get dataChan status
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

// GetDetailedStats gets detailed performance statistics
func (s *Stream) GetDetailedStats() map[string]interface{} {
	basicStats := s.GetStats()

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

	result := map[string]interface{}{
		BasicStats:       basicStats,
		DataChanUsage:    dataUsage,
		ResultChanUsage:  resultUsage,
		SinkPoolUsage:    sinkUsage,
		ProcessRate:      processRate,
		DropRate:         dropRate,
		PerformanceLevel: AssessPerformanceLevel(dataUsage, dropRate),
	}

	// Add persistence statistics
	if s.persistenceManager != nil {
		result["Persistence"] = s.persistenceManager.GetStats()
	}

	return result
}

// ResetStats resets statistics information
func (s *Stream) ResetStats() {
	atomic.StoreInt64(&s.inputCount, 0)
	atomic.StoreInt64(&s.outputCount, 0)
	atomic.StoreInt64(&s.droppedCount, 0)
}
