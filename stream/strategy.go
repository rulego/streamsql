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
	"sync/atomic"
	"time"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
)

// Overflow strategy constants
const (
	StrategyDrop   = "drop"   // Drop strategy
	StrategyBlock  = "block"  // Blocking strategy
	StrategyExpand = "expand" // Dynamic strategy
)

// DataProcessingStrategy data processing strategy interface
// Defines unified interface for different overflow strategies, providing better extensibility and maintainability
type DataProcessingStrategy interface {
	// ProcessData core method for processing data
	// Parameters:
	//   - data: data to process, must be map[string]interface{} type
	ProcessData(data map[string]interface{})

	// GetStrategyName gets strategy name
	GetStrategyName() string

	// Init initializes strategy
	// Parameters:
	//   - stream: Stream instance reference
	//   - config: performance configuration
	Init(stream *Stream, config types.PerformanceConfig) error

	// Stop stops and cleans up resources
	Stop() error
}

// BlockingStrategy blocking strategy implementation
type BlockingStrategy struct {
	stream *Stream
}

// NewBlockingStrategy creates blocking strategy instance
func NewBlockingStrategy() *BlockingStrategy {
	return &BlockingStrategy{}
}

// ProcessData implements blocking mode data processing
func (bs *BlockingStrategy) ProcessData(data map[string]interface{}) {
	if bs.stream.blockingTimeout <= 0 {
		// No timeout limit, block permanently until success
		dataChan := bs.stream.safeGetDataChan()
		dataChan <- data
		return
	}

	// Blocking with timeout
	timer := time.NewTimer(bs.stream.blockingTimeout)
	defer timer.Stop()

	dataChan := bs.stream.safeGetDataChan()
	select {
	case dataChan <- data:
		// Successfully added data
		return
	case <-timer.C:
		// Timeout but don't drop data, log error but continue blocking
		logger.Error("Data addition timeout, but continue waiting to avoid data loss")
		// Continue blocking indefinitely, re-get current channel reference
		finalDataChan := bs.stream.safeGetDataChan()
		finalDataChan <- data
	}
}

// GetStrategyName gets strategy name
func (bs *BlockingStrategy) GetStrategyName() string {
	return StrategyBlock
}

// Init initializes blocking strategy
func (bs *BlockingStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	bs.stream = stream
	return nil
}

// Stop stops and cleans up blocking strategy resources
func (bs *BlockingStrategy) Stop() error {
	return nil
}

// ExpansionStrategy expansion strategy implementation
type ExpansionStrategy struct {
	stream *Stream
}

// NewExpansionStrategy creates expansion strategy instance
func NewExpansionStrategy() *ExpansionStrategy {
	return &ExpansionStrategy{}
}

// ProcessData implements expansion mode data processing
func (es *ExpansionStrategy) ProcessData(data map[string]interface{}) {
	// First attempt to add data
	if es.stream.safeSendToDataChan(data) {
		return
	}

	// Channel is full, dynamically expand
	es.stream.expandDataChannel()

	// Retry after expansion, re-acquire channel reference
	if es.stream.safeSendToDataChan(data) {
		logger.Debug("Successfully added data after data channel expansion")
		return
	}

	// If still full after expansion, block and wait
	dataChan := es.stream.safeGetDataChan()
	dataChan <- data
}

// GetStrategyName gets strategy name
func (es *ExpansionStrategy) GetStrategyName() string {
	return StrategyExpand
}

// Init initializes expansion strategy
func (es *ExpansionStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	es.stream = stream
	return nil
}

// Stop stops and cleans up expansion strategy resources
func (es *ExpansionStrategy) Stop() error {
	return nil
}

// DropStrategy drop strategy implementation
type DropStrategy struct {
	stream *Stream
}

// NewDropStrategy creates drop strategy instance
func NewDropStrategy() *DropStrategy {
	return &DropStrategy{}
}

// ProcessData implements drop mode data processing
func (ds *DropStrategy) ProcessData(data map[string]interface{}) {
	// Intelligent non-blocking add with layered backpressure control
	if ds.stream.safeSendToDataChan(data) {
		return
	}

	// Data channel is full, use layered backpressure strategy, get channel status
	ds.stream.dataChanMux.RLock()
	chanLen := len(ds.stream.dataChan)
	chanCap := cap(ds.stream.dataChan)
	currentDataChan := ds.stream.dataChan
	ds.stream.dataChanMux.RUnlock()

	usage := float64(chanLen) / float64(chanCap)

	// Adjust strategy based on channel usage rate and buffer size
	var waitTime time.Duration
	var maxRetries int

	switch {
	case chanCap >= 100000: // Extra large buffer (benchmark mode)
		switch {
		case usage > 0.99:
			waitTime = 1 * time.Millisecond // Longer wait
			maxRetries = 3
		case usage > 0.95:
			waitTime = 500 * time.Microsecond
			maxRetries = 2
		case usage > 0.90:
			waitTime = 100 * time.Microsecond
			maxRetries = 1
		default:
			// Drop immediately
			logger.Warn("Data channel is full, dropping input data")
			atomic.AddInt64(&ds.stream.droppedCount, 1)
			return
		}

	case chanCap >= 50000: // High performance mode
		switch {
		case usage > 0.99:
			waitTime = 500 * time.Microsecond
			maxRetries = 2
		case usage > 0.95:
			waitTime = 200 * time.Microsecond
			maxRetries = 1
		case usage > 0.90:
			waitTime = 50 * time.Microsecond
			maxRetries = 1
		default:
			logger.Warn("Data channel is full, dropping input data")
			atomic.AddInt64(&ds.stream.droppedCount, 1)
			return
		}

	default: // Default mode
		switch {
		case usage > 0.99:
			waitTime = 100 * time.Microsecond
			maxRetries = 1
		case usage > 0.95:
			waitTime = 50 * time.Microsecond
			maxRetries = 1
		default:
			logger.Warn("Data channel is full, dropping input data")
			atomic.AddInt64(&ds.stream.droppedCount, 1)
			return
		}
	}

	// Multiple retries to add data, using thread-safe approach
	for retry := 0; retry < maxRetries; retry++ {
		timer := time.NewTimer(waitTime)
		select {
		case currentDataChan <- data:
			// Retry successful
			timer.Stop()
			return
		case <-timer.C:
			// Timeout, continue to next retry or drop
			if retry == maxRetries-1 {
				// Last retry failed, record drop
				logger.Warn("Data channel is full, dropping input data")
				atomic.AddInt64(&ds.stream.droppedCount, 1)
			}
		}
	}
}

// GetStrategyName gets strategy name
func (ds *DropStrategy) GetStrategyName() string {
	return StrategyDrop
}

// Init initializes drop strategy
func (ds *DropStrategy) Init(stream *Stream, config types.PerformanceConfig) error {
	ds.stream = stream
	return nil
}

// Stop stops and cleans up drop strategy resources
func (ds *DropStrategy) Stop() error {
	return nil
}

// StrategyFactory strategy factory
// Uses unified registration mechanism to manage all strategies (built-in and custom)
type StrategyFactory struct {
	// Registered strategy mapping
	strategies map[string]func() DataProcessingStrategy
	mutex      sync.RWMutex // Protects concurrent access
}

// NewStrategyFactory creates strategy factory instance
// Automatically registers all built-in strategies
func NewStrategyFactory() *StrategyFactory {
	factory := &StrategyFactory{
		strategies: make(map[string]func() DataProcessingStrategy),
	}

	// Register built-in strategies
	factory.RegisterStrategy(StrategyBlock, func() DataProcessingStrategy { return NewBlockingStrategy() })
	factory.RegisterStrategy(StrategyExpand, func() DataProcessingStrategy { return NewExpansionStrategy() })
	factory.RegisterStrategy(StrategyDrop, func() DataProcessingStrategy { return NewDropStrategy() })

	return factory
}

// RegisterStrategy registers strategy to factory
// Parameters:
//   - name: strategy name
//   - constructor: strategy constructor function
func (sf *StrategyFactory) RegisterStrategy(name string, constructor func() DataProcessingStrategy) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	sf.strategies[name] = constructor
}

// UnregisterStrategy unregisters strategy
// Parameters:
//   - name: strategy name
func (sf *StrategyFactory) UnregisterStrategy(name string) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	delete(sf.strategies, name)
}

// GetRegisteredStrategies gets all registered strategy names
// Returns:
//   - []string: strategy name list
func (sf *StrategyFactory) GetRegisteredStrategies() []string {
	sf.mutex.RLock()
	defer sf.mutex.RUnlock()

	names := make([]string, 0, len(sf.strategies))
	for name := range sf.strategies {
		names = append(names, name)
	}
	return names
}

// CreateStrategy creates corresponding strategy instance based on strategy name
// Parameters:
//   - strategyName: strategy name
//
// Returns:
//   - DataProcessingStrategy: strategy instance
//   - error: error information
func (sf *StrategyFactory) CreateStrategy(strategyName string) (DataProcessingStrategy, error) {
	sf.mutex.RLock()
	constructor, exists := sf.strategies[strategyName]
	sf.mutex.RUnlock()

	if !exists {
		// If strategy doesn't exist, use default drop strategy
		sf.mutex.RLock()
		defaultConstructor, defaultExists := sf.strategies[StrategyDrop]
		sf.mutex.RUnlock()

		if defaultExists {
			return defaultConstructor(), nil
		}
		// If even default strategy doesn't exist, return error
		return nil, fmt.Errorf("strategy '%s' not found and no default strategy available", strategyName)
	}

	return constructor(), nil
}
