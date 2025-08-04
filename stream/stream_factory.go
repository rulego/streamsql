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
	"fmt"
	"sync"

	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/window"
)

// StreamFactory Stream factory responsible for creating different types of Streams
type StreamFactory struct{}

// NewStreamFactory creates Stream factory
func NewStreamFactory() *StreamFactory {
	return &StreamFactory{}
}

// CreateStream creates Stream using unified configuration
func (sf *StreamFactory) CreateStream(config types.Config) (*Stream, error) {
	// If no performance configuration is specified, use default configuration
	if (config.PerformanceConfig == types.PerformanceConfig{}) {
		config.PerformanceConfig = types.DefaultPerformanceConfig()
	}

	return sf.createStreamWithUnifiedConfig(config)
}

// CreateHighPerformanceStream creates high-performance Stream
func (sf *StreamFactory) CreateHighPerformanceStream(config types.Config) (*Stream, error) {
	config.PerformanceConfig = types.HighPerformanceConfig()
	return sf.createStreamWithUnifiedConfig(config)
}

// CreateLowLatencyStream creates low-latency Stream
func (sf *StreamFactory) CreateLowLatencyStream(config types.Config) (*Stream, error) {
	config.PerformanceConfig = types.LowLatencyConfig()
	return sf.createStreamWithUnifiedConfig(config)
}

// CreateZeroDataLossStream creates zero data loss Stream
func (sf *StreamFactory) CreateZeroDataLossStream(config types.Config) (*Stream, error) {
	config.PerformanceConfig = types.ZeroDataLossConfig()
	return sf.createStreamWithUnifiedConfig(config)
}

// CreateCustomPerformanceStream creates Stream with custom performance configuration
func (sf *StreamFactory) CreateCustomPerformanceStream(config types.Config, perfConfig types.PerformanceConfig) (*Stream, error) {
	config.PerformanceConfig = perfConfig
	return sf.createStreamWithUnifiedConfig(config)
}

// createStreamWithUnifiedConfig internal implementation for creating Stream using unified configuration
func (sf *StreamFactory) createStreamWithUnifiedConfig(config types.Config) (*Stream, error) {
	var win window.Window
	var err error

	// Only create window when needed
	if config.NeedWindow {
		win, err = sf.createWindow(config)
		if err != nil {
			return nil, err
		}
	}

	// Create Stream instance
	stream := sf.createStreamInstance(config, win)

	// Initialize persistence manager
	if err := sf.initializePersistenceManager(stream, config.PerformanceConfig); err != nil {
		return nil, err
	}

	// Setup data processing strategy
	if err := sf.setupDataProcessingStrategy(stream, config.PerformanceConfig); err != nil {
		return nil, fmt.Errorf("failed to setup data processing strategy: %w", err)
	}

	// Pre-compile field processing information
	stream.compileFieldProcessInfo()

	// Start worker routines
	sf.startWorkerRoutines(stream, config.PerformanceConfig)

	return stream, nil
}

// createWindow creates window
func (sf *StreamFactory) createWindow(config types.Config) (window.Window, error) {
	// Pass unified performance configuration to window
	windowConfig := config.WindowConfig
	if windowConfig.Params == nil {
		windowConfig.Params = make(map[string]interface{})
	}
	// Pass complete performance configuration to window
	windowConfig.Params[PerformanceConfigKey] = config.PerformanceConfig

	return window.CreateWindow(windowConfig)
}

// createStreamInstance 创建Stream实例
func (sf *StreamFactory) createStreamInstance(config types.Config, win window.Window) *Stream {
	perfConfig := config.PerformanceConfig
	return &Stream{
		dataChan:         make(chan map[string]interface{}, perfConfig.BufferConfig.DataChannelSize),
		config:           config,
		Window:           win,
		resultChan:       make(chan []map[string]interface{}, perfConfig.BufferConfig.ResultChannelSize),
		seenResults:      &sync.Map{},
		done:             make(chan struct{}),
		sinkWorkerPool:   make(chan func(), perfConfig.WorkerConfig.SinkPoolSize),
		allowDataDrop:    perfConfig.OverflowConfig.AllowDataLoss,
		blockingTimeout:  perfConfig.OverflowConfig.BlockTimeout,
		overflowStrategy: perfConfig.OverflowConfig.Strategy,
		maxRetryRoutines: int32(perfConfig.WorkerConfig.MaxRetryRoutines),
	}
}

// initializePersistenceManager 初始化持久化管理器
// 当溢出策略设置为持久化时，检查并初始化持久化配置
func (sf *StreamFactory) initializePersistenceManager(stream *Stream, perfConfig types.PerformanceConfig) error {
	if perfConfig.OverflowConfig.Strategy == StrategyPersist {
		if perfConfig.OverflowConfig.PersistenceConfig == nil {
			return fmt.Errorf("persistence strategy is enabled but PersistenceConfig is not provided. Please configure PersistenceConfig with DataDir, MaxFileSize, and FlushInterval. Example: perfConfig.OverflowConfig.PersistenceConfig = &types.PersistenceConfig{DataDir: \"./data\", MaxFileSize: 10*1024*1024, FlushInterval: 5*time.Second}")
		}
		persistConfig := perfConfig.OverflowConfig.PersistenceConfig
		stream.persistenceManager = NewPersistenceManagerWithConfig(
			persistConfig.DataDir,
			persistConfig.MaxFileSize,
			persistConfig.FlushInterval,
		)
		err := stream.persistenceManager.Start()
		if err != nil {
			return fmt.Errorf("failed to start persistence manager: %w", err)
		}
		// 尝试加载和恢复持久化数据
		return stream.persistenceManager.LoadAndRecoverData()
	}
	return nil
}

// setupDataProcessingStrategy 设置数据处理策略
// 使用策略模式替代函数指针，提供更好的扩展性和可维护性
func (sf *StreamFactory) setupDataProcessingStrategy(stream *Stream, perfConfig types.PerformanceConfig) error {
	// 创建策略工厂
	strategyFactory := NewStrategyFactory()

	// 根据配置创建对应的策略实例
	strategy, err := strategyFactory.CreateStrategy(perfConfig.OverflowConfig.Strategy)
	if err != nil {
		return err
	}

	// 初始化策略
	if err := strategy.Init(stream, perfConfig); err != nil {
		return err
	}

	// 设置策略到Stream实例
	stream.dataStrategy = strategy
	return nil
}

// startWorkerRoutines 启动工作协程
func (sf *StreamFactory) startWorkerRoutines(stream *Stream, perfConfig types.PerformanceConfig) {
	go stream.startSinkWorkerPool(perfConfig.WorkerConfig.SinkWorkerCount)
	go stream.startResultConsumer()
}
