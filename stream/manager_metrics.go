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
	"sync/atomic"

	"github.com/rulego/streamsql/metrics"
)

// MetricsRegistry returns the stream's metrics registry.
func (s *Stream) MetricsRegistry() *metrics.Registry {
	return s.metricsRegistry
}

// GetStats gets stream processing statistics (thread-safe version)
func (s *Stream) GetStats() map[string]int64 {
	// Thread-safely get dataChan status
	s.dataChanMux.RLock()
	dataChanLen := int64(len(s.dataChan))
	dataChanCap := int64(cap(s.dataChan))
	s.dataChanMux.RUnlock()

	stats := map[string]int64{
		InputCount:    s.mInput.Value(),
		OutputCount:   s.mOutput.Value(),
		DroppedCount:  s.mDropped.Value(),
		DataChanLen:   dataChanLen,
		DataChanCap:   dataChanCap,
		ResultChanLen: int64(len(s.resultChan)),
		ResultChanCap: int64(cap(s.resultChan)),
		SinkPoolLen:   int64(len(s.sinkWorkerPool)),
		SinkPoolCap:   int64(cap(s.sinkWorkerPool)),
		ActiveRetries: int64(atomic.LoadInt32(&s.activeRetries)),
		Expanding:     int64(atomic.LoadInt32(&s.expanding)),
	}

	if s.Window != nil {
		winStats := s.Window.GetStats()
		for k, v := range winStats {
			stats[k] = v
		}
	}

	return stats
}

// GetDetailedStats gets detailed performance statistics
func (s *Stream) GetDetailedStats() map[string]any {
	basicStats := s.GetStats()

	// Calculate usage rates
	usage := func(length, capacity int64) float64 {
		if capacity <= 0 {
			return 0
		}
		return float64(length) / float64(capacity) * 100
	}
	dataUsage := usage(basicStats[DataChanLen], basicStats[DataChanCap])
	resultUsage := usage(basicStats[ResultChanLen], basicStats[ResultChanCap])
	sinkUsage := usage(basicStats[SinkPoolLen], basicStats[SinkPoolCap])

	// Calculate efficiency metrics
	var processRate float64 = 100.0
	var dropRate float64 = 0.0

	if basicStats[InputCount] > 0 {
		processRate = float64(basicStats[OutputCount]) / float64(basicStats[InputCount]) * 100
		dropRate = float64(basicStats[DroppedCount]) / float64(basicStats[InputCount]) * 100
	}

	result := map[string]any{
		BasicStats:       basicStats,
		DataChanUsage:    dataUsage,
		ResultChanUsage:  resultUsage,
		SinkPoolUsage:    sinkUsage,
		ProcessRate:      processRate,
		DropRate:         dropRate,
		PerformanceLevel: AssessPerformanceLevel(dataUsage, dropRate),
	}

	return result
}

// ResetStats resets statistics information
func (s *Stream) ResetStats() {
	s.mInput.Reset()
	s.mOutput.Reset()
	s.mDropped.Reset()
}
