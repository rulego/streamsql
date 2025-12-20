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

// CreateCustomPerformanceStream creates Stream with custom performance configuration
func (sf *StreamFactory) CreateCustomPerformanceStream(config types.Config, perfConfig types.PerformanceConfig) (*Stream, error) {
	config.PerformanceConfig = perfConfig
	return sf.createStreamWithUnifiedConfig(config)
}

// createStreamWithUnifiedConfig internal implementation for creating Stream using unified configuration
func (sf *StreamFactory) createStreamWithUnifiedConfig(config types.Config) (*Stream, error) {
	var win window.Window
	var err error

	// Validate performance configuration
	if err := sf.validatePerformanceConfig(config.PerformanceConfig); err != nil {
		return nil, fmt.Errorf("invalid performance configuration: %w", err)
	}

	// Only create window when needed
	if config.NeedWindow {
		win, err = sf.createWindow(config)
		if err != nil {
			return nil, err
		}
	}

	// Create Stream instance
	stream := sf.createStreamInstance(config, win)

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
	// Set performance configuration directly
	windowConfig.PerformanceConfig = config.PerformanceConfig

	return window.CreateWindow(windowConfig)
}

// createStreamInstance creates Stream instance
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

// setupDataProcessingStrategy sets up data processing strategy
func (sf *StreamFactory) setupDataProcessingStrategy(stream *Stream, perfConfig types.PerformanceConfig) error {
	// Create strategy factory
	strategyFactory := NewStrategyFactory()

	// Create corresponding strategy instance based on configuration
	strategy, err := strategyFactory.CreateStrategy(perfConfig.OverflowConfig.Strategy)
	if err != nil {
		return err
	}

	// Initialize strategy
	if err := strategy.Init(stream, perfConfig); err != nil {
		return err
	}

	// Set strategy to Stream instance
	stream.dataStrategy = strategy
	return nil
}

// validatePerformanceConfig validates performance configuration parameters
func (sf *StreamFactory) validatePerformanceConfig(config types.PerformanceConfig) error {
	// Validate buffer configuration
	if config.BufferConfig.DataChannelSize < 0 {
		return fmt.Errorf("DataChannelSize cannot be negative: %d", config.BufferConfig.DataChannelSize)
	}
	if config.BufferConfig.ResultChannelSize < 0 {
		return fmt.Errorf("ResultChannelSize cannot be negative: %d", config.BufferConfig.ResultChannelSize)
	}

	// Validate worker pool configuration
	if config.WorkerConfig.SinkPoolSize < 0 {
		return fmt.Errorf("SinkPoolSize cannot be negative: %d", config.WorkerConfig.SinkPoolSize)
	}

	// Validate overflow configuration
	validStrategies := map[string]bool{
		"drop":    true,
		"block":   true,
		"expand":  true,
		"persist": true,
	}
	if config.OverflowConfig.Strategy != "" && !validStrategies[config.OverflowConfig.Strategy] {
		return fmt.Errorf("invalid overflow strategy: %s", config.OverflowConfig.Strategy)
	}

	return nil
}

// startWorkerRoutines starts worker goroutines
func (sf *StreamFactory) startWorkerRoutines(stream *Stream, perfConfig types.PerformanceConfig) {
	go stream.startSinkWorkerPool(perfConfig.WorkerConfig.SinkWorkerCount)
}
