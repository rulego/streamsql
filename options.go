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

// Option 定义StreamSQL的配置选项类型
type Option func(*Streamsql)

// WithLogLevel 设置日志级别
func WithLogLevel(level logger.Level) Option {
	return func(s *Streamsql) {
		logger.GetDefault().SetLevel(level)
	}
}

// WithDiscardLog 禁用日志输出
func WithDiscardLog() Option {
	return func(s *Streamsql) {
		logger.SetDefault(logger.NewDiscardLogger())
	}
}

// WithHighPerformance 使用高性能配置
// 适用于需要最大吞吐量的场景
func WithHighPerformance() Option {
	return func(s *Streamsql) {
		s.performanceMode = "high_performance"
	}
}

// WithLowLatency 使用低延迟配置
// 适用于实时交互应用，最小化延迟
func WithLowLatency() Option {
	return func(s *Streamsql) {
		s.performanceMode = "low_latency"
	}
}

// WithZeroDataLoss 使用零数据丢失配置
// 适用于关键业务数据，保证数据不丢失
func WithZeroDataLoss() Option {
	return func(s *Streamsql) {
		s.performanceMode = "zero_data_loss"
	}
}

// WithCustomPerformance 使用自定义性能配置
func WithCustomPerformance(config types.PerformanceConfig) Option {
	return func(s *Streamsql) {
		s.performanceMode = "custom"
		s.customConfig = &config
	}
}

// WithPersistence 使用持久化配置预设
func WithPersistence() Option {
	return func(s *Streamsql) {
		s.performanceMode = "custom"
		persistConfig := types.PersistencePerformanceConfig()
		s.customConfig = &persistConfig
	}
}

// WithCustomPersistence 使用自定义持久化配置
func WithCustomPersistence(dataDir string, maxFileSize int64, flushInterval time.Duration) Option {
	return func(s *Streamsql) {
		s.performanceMode = "custom"
		config := types.DefaultPerformanceConfig()
		config.OverflowConfig.Strategy = "persist"
		config.OverflowConfig.PersistenceConfig = &types.PersistenceConfig{
			DataDir:       dataDir,
			MaxFileSize:   maxFileSize,
			FlushInterval: flushInterval,
			MaxRetries:    3,
			RetryInterval: 2 * time.Second,
		}
		s.customConfig = &config
	}
}

// WithBufferSizes 设置自定义缓冲区大小
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

// WithOverflowStrategy 设置溢出策略
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

// WithWorkerConfig 设置工作池配置
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

// WithMonitoring 启用详细监控
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
