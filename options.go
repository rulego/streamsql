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

// WithLogger sets the logger used by the engine. The library routes all internal
// logging through a single process-global logger (see logger.SetDefault), so this
// is equivalent to logger.SetDefault(l); it is exposed as an Option so callers
// configure it alongside New. Pass nil to keep the default. A true per-instance
// logger would require threading one through every internal call site.
func WithLogger(l logger.Logger) Option {
	return func(s *Streamsql) {
		if l != nil {
			logger.SetDefault(l)
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

// WithZeroDataLoss uses zero data loss configuration
// Suitable for critical business data, ensuring no data loss
func WithZeroDataLoss() Option {
	return func(s *Streamsql) {
		s.performanceMode = "zero_data_loss"
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
