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
