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

// Statistic field keys returned by GetStats.
const (
	InputCount         = "input_count"
	OutputCount        = "output_count"
	InputDroppedCount  = "input_dropped_count"
	OutputDroppedCount = "output_dropped_count"
	DroppedCount       = "dropped_count"
	DataChanLen        = "data_chan_len"
	DataChanCap        = "data_chan_cap"
	ResultChanLen      = "result_chan_len"
	ResultChanCap      = "result_chan_cap"
	SinkPoolLen        = "sink_pool_len"
	SinkPoolCap        = "sink_pool_cap"
	ActiveRetries      = "active_retries"
	Expanding          = "expanding"
)

// Detailed statistics field keys returned by GetDetailedStats.
const (
	BasicStats       = "basic_stats"
	DataChanUsage    = "data_chan_usage"
	ResultChanUsage  = "result_chan_usage"
	SinkPoolUsage    = "sink_pool_usage"
	ProcessRate      = "process_rate"
	DropRate         = "drop_rate"
	PerformanceLevel = "performance_level"
)

// AssessPerformanceLevel maps data usage and drop rate to a performance level.
func AssessPerformanceLevel(dataUsage, dropRate float64) string {
	switch {
	case dropRate > 50:
		return PerformanceLevelCritical
	case dropRate > 20:
		return PerformanceLevelWarning
	case dataUsage > 90:
		return PerformanceLevelHighLoad
	case dataUsage > 70:
		return PerformanceLevelModerateLoad
	default:
		return PerformanceLevelOptimal
	}
}
