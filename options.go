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

package streamsql

import (
	"time"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/schema"
	"github.com/rulego/streamsql/types"
)

// Option defines the configuration option type for StreamSQL
type Option func(*Streamsql)

// WithLogLevel sets the log level
func WithLogLevel(level logger.Level) Option {
	return func(s *Streamsql) {
		logger.GetDefault().SetLevel(level)
	}
}

// WithDiscardLog disables log output
func WithDiscardLog() Option {
	return func(s *Streamsql) {
		logger.SetDefault(logger.NewDiscardLogger())
	}
}

// WithLogger sets the per-instance logger used by this stream's pipeline. Each
// Streamsql instance logs only to its own logger (no global cross-talk between
// instances). Pass nil to keep the process default (logger.GetDefault).
func WithLogger(l logger.Logger) Option {
	return func(s *Streamsql) {
		if l != nil {
			s.log = l
		}
	}
}

// WithHighPerformance uses high-performance configuration
// Suitable for scenarios requiring maximum throughput
func WithHighPerformance() Option {
	return func(s *Streamsql) {
		s.performanceMode = "high_performance"
	}
}

// WithLowLatency uses low-latency configuration
// Suitable for real-time interactive applications, minimizing latency
func WithLowLatency() Option {
	return func(s *Streamsql) {
		s.performanceMode = "low_latency"
	}
}

// WithCustomPerformance uses custom performance configuration
func WithCustomPerformance(config types.PerformanceConfig) Option {
	return func(s *Streamsql) {
		s.performanceMode = "custom"
		s.customConfig = &config
	}
}

// WithBufferSizes sets custom buffer sizes
func WithBufferSizes(dataChannelSize, resultChannelSize, windowOutputSize int) Option {
	return func(s *Streamsql) {
		s.performanceMode = "custom"
		config := types.DefaultPerformanceConfig()
		config.BufferConfig.DataChannelSize = dataChannelSize
		config.BufferConfig.ResultChannelSize = resultChannelSize
		config.BufferConfig.WindowOutputSize = windowOutputSize
		s.customConfig = &config
	}
}

// WithOverflowStrategy sets the overflow strategy
func WithOverflowStrategy(strategy string, blockTimeout time.Duration) Option {
	return func(s *Streamsql) {
		s.performanceMode = "custom"
		config := types.DefaultPerformanceConfig()
		config.OverflowConfig.Strategy = strategy
		config.OverflowConfig.BlockTimeout = blockTimeout
		config.OverflowConfig.AllowDataLoss = (strategy == "drop")
		s.customConfig = &config
	}
}

// WithWorkerConfig sets the worker pool configuration
func WithWorkerConfig(sinkPoolSize, sinkWorkerCount, maxRetryRoutines int) Option {
	return func(s *Streamsql) {
		s.performanceMode = "custom"
		config := types.DefaultPerformanceConfig()
		config.WorkerConfig.SinkPoolSize = sinkPoolSize
		config.WorkerConfig.SinkWorkerCount = sinkWorkerCount
		config.WorkerConfig.MaxRetryRoutines = maxRetryRoutines
		s.customConfig = &config
	}
}

// WithMonitoring enables detailed monitoring
func WithMonitoring(updateInterval time.Duration, enableDetailedStats bool) Option {
	return func(s *Streamsql) {
		s.performanceMode = "custom"
		config := types.DefaultPerformanceConfig()
		config.MonitoringConfig.EnableMonitoring = true
		config.MonitoringConfig.StatsUpdateInterval = updateInterval
		config.MonitoringConfig.EnableDetailedStats = enableDetailedStats
		s.customConfig = &config
	}
}

// WithSchema registers an input-validation schema for this stream. Emit/EmitSync
// validate data against it and drop rows that fail (Emit logs+drops, EmitSync
// returns the error). Without WithSchema, Emit/EmitSync perform no validation
// (zero overhead). One Streamsql instance is one stream/query; for multiple
// streams, use multiple instances, each with its own schema.
func WithSchema(s schema.Schema) Option {
	return func(ss *Streamsql) {
		ss.schemaValidator = &s
	}
}

// WithAnalyticMaxPartitions caps the number of PARTITION states kept per analytic
// function field (lag/had_changed/changed_col(s)/acc_*/latest with OVER(PARTITION BY...)).
// The least-recently-used partition is evicted above the cap. Only raise it when
// the partition key is genuinely high-cardinality (e.g. tens of thousands of
// devices behind one node) and memory allows: each partition holds one state plus
// its last result, ~150B for acc_* but up to several hundred bytes for
// changed_cols/had_changed('*') on wide rows. Default (n<=0) is 10000.
//
// Note: evicting a partition resets its analytic state silently — acc_* counters
// restart from 0, lag returns its default, had_changed/changed_col treat the next
// row as the first. Size the cap above the peak active-partition count when
// cumulative accuracy matters.
func WithAnalyticMaxPartitions(n int) Option {
	return func(ss *Streamsql) {
		ss.analyticMaxPartitions = n
	}
}

// WithGroupMaxPartitions caps the number of GROUP BY partitions (distinct group
// keys) the aggregator keeps at once. Above the cap the least-recently-used
// group is evicted, discarding its accumulated aggregation state. Independent
// from WithAnalyticMaxPartitions. Default (n<=0) is 10000.
//
// This bounds memory for high-cardinality GROUP BY keys (e.g.
// GROUP BY deviceId, TumblingWindow('30s') behind a 128MB gateway). Note that
// evicting a group silently resets its aggregates — SUM/COUNT restart from 0
// the next time that key appears. Raise the cap above the peak active-group
// count when full accuracy is required.
func WithGroupMaxPartitions(n int) Option {
	return func(ss *Streamsql) {
		ss.groupMaxPartitions = n
	}
}

// WithWindowMaxRows caps the raw rows a time window buffers before it triggers.
// Above the cap the newest arriving rows are dropped, counted in the
// window_rows_dropped_count stat and reported through a throttled warning.
// Default (n<=0) is unbounded.
//
// This bounds the one remaining unbounded path: window buffering holds every raw
// row for the window's duration (~438B/row measured), so window duration × input
// rate sets the peak. A 30s window at 10k msg/s buffers 300k rows ≈ 131MB, which
// alone exceeds a 128MB gateway. Unlike WithGroupMaxPartitions (which bounds
// high-cardinality keys), this bounds throughput × time and applies even with a
// single group.
//
// Note that dropping rows changes results: aggregates are then computed over a
// truncated sample of the interval — COUNT under-reports, AVG skews toward the
// window's earlier rows. Prefer sizing the cap above the expected peak
// (rate × window duration) so it acts as an OOM backstop rather than a routine
// limit, and alert on window_rows_dropped_count. Applies to tumbling / sliding /
// session windows; counting and global windows are already bounded by their count
// threshold and STATETTL.
func WithWindowMaxRows(n int) Option {
	return func(ss *Streamsql) {
		ss.windowMaxRows = n
	}
}

// WithJoinMaxKeys caps the buffered join keys per side per stream-JOIN stage
// (LRU eviction of the least-recently-used key above the cap). Default (n<=0)
// is 10000, aligned with the analytic/CEP partition caps.
//
// This bounds memory for high-cardinality join keys (e.g. deviceId with tens of
// thousands of devices). Note that evicting a key discards its buffered rows
// silently except for the join_stage{i}_keys_evicted metric and a throttled
// warning: unmatched LEFT rows among them are NOT NULL-complemented (same
// semantics as the GROUP BY LRU cap). Raise the cap above the peak active-key
// count (WITHIN × key arrival rate) when match completeness matters.
func WithJoinMaxKeys(n int) Option {
	return func(ss *Streamsql) {
		ss.joinMaxKeys = n
	}
}

// WithJoinMaxRows caps the raw rows buffered per side per stream-JOIN stage.
// Above the cap the newest arriving rows are dropped, counted in the
// join_stage{i}_rows_dropped metric and reported through a throttled warning.
// Default (n<=0) is unbounded; the primary bound is WITHIN (retention).
//
// Size it above the peak (input rate × WITHIN) so it acts as an OOM backstop
// rather than a routine limit: dropping rows silently truncates matches the
// same way WindowMaxRows truncates aggregates (~440B per buffered row).
func WithJoinMaxRows(n int) Option {
	return func(ss *Streamsql) {
		ss.joinMaxRows = n
	}
}
