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

package window

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rulego/streamsql/utils/cast"

	"github.com/rulego/streamsql/types"
)

// Ensure SlidingWindow implements the Window interface
var _ Window = (*SlidingWindow)(nil)

// TimedData wraps data with timestamp
type TimedData struct {
	Data      interface{}
	Timestamp time.Time
}

// SlidingWindow represents a sliding window for processing data within time ranges
type SlidingWindow struct {
	// config holds window configuration
	config types.WindowConfig
	// size is the total window size (time range covered by the window)
	size time.Duration
	// slide is the sliding interval for the window
	slide time.Duration
	// mu protects concurrent data access
	mu sync.RWMutex
	// data stores window data
	data []types.Row
	// outputChan is the channel for outputting window data
	outputChan chan []types.Row
	// callback function executed when window triggers
	callback func([]types.Row)
	// ctx controls window lifecycle
	ctx context.Context
	// cancelFunc cancels the context
	cancelFunc context.CancelFunc
	// timer for triggering window periodically
	timer       *time.Ticker
	currentSlot *types.TimeSlot
	// initChan for window initialization
	initChan    chan struct{}
	initialized bool
	// timerMu protects timer access
	timerMu sync.Mutex
	// firstWindowStartTime records when first window started (processing time)
	firstWindowStartTime time.Time
	// Performance statistics
	droppedCount int64 // Number of dropped results
	sentCount    int64 // Number of successfully sent results
}

// NewSlidingWindow creates a new sliding window instance
// size parameter represents the total window size, slide represents the sliding interval
func NewSlidingWindow(config types.WindowConfig) (*SlidingWindow, error) {
	// Get size parameter from params array
	if len(config.Params) < 1 {
		return nil, fmt.Errorf("sliding window requires at least 'size' parameter")
	}

	sizeVal := config.Params[0]
	size, err := cast.ToDurationE(sizeVal)
	if err != nil {
		return nil, fmt.Errorf("invalid size for sliding window: %v", err)
	}

	// Get slide parameter from params array
	if len(config.Params) < 2 {
		return nil, fmt.Errorf("sliding window requires 'slide' parameter")
	}

	slideVal := config.Params[1]
	slide, err := cast.ToDurationE(slideVal)
	if err != nil {
		return nil, fmt.Errorf("invalid slide for sliding window: %v", err)
	}

	// Use unified performance config to get window output buffer size
	bufferSize := 1000 // Default value
	if (config.PerformanceConfig != types.PerformanceConfig{}) {
		bufferSize = config.PerformanceConfig.BufferConfig.WindowOutputSize
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	return &SlidingWindow{
		config:      config,
		size:        size,
		slide:       slide,
		outputChan:  make(chan []types.Row, bufferSize),
		ctx:         ctx,
		cancelFunc:  cancel,
		data:        make([]types.Row, 0),
		initChan:    make(chan struct{}),
		initialized: false,
	}, nil
}

// Add adds data to the sliding window
func (sw *SlidingWindow) Add(data interface{}) {
	// Lock to ensure thread safety
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Add data to the window's data list
	t := GetTimestamp(data, sw.config.TsProp, sw.config.TimeUnit)
	if !sw.initialized {
		sw.currentSlot = sw.createSlot(t)
		// Record when first window started (processing time)
		sw.firstWindowStartTime = time.Now()
		// Don't start timer here, wait for first window to end
		// Send initialization complete signal
		close(sw.initChan)
		sw.initialized = true
	}
	row := types.Row{
		Data:      data,
		Timestamp: t,
	}
	sw.data = append(sw.data, row)
}

// Start starts the sliding window with periodic triggering
// Uses lazy initialization to avoid infinite waiting when no data, ensuring subsequent data can be processed normally
// First window triggers when it ends, then subsequent windows trigger at slide intervals
func (sw *SlidingWindow) Start() {
	go func() {
		// Close output channel when function ends
		defer close(sw.outputChan)

		// Wait for initialization complete or context cancellation
		select {
		case <-sw.initChan:
			// Initialization completed normally, continue processing
		case <-sw.ctx.Done():
			// Context cancelled, exit directly
			return
		}

		// Wait for first window to end, then trigger it
		// After initChan is closed, firstWindowStartTime should be set by Add()
		sw.mu.RLock()
		firstWindowStartTime := sw.firstWindowStartTime
		sw.mu.RUnlock()

		// Verify that firstWindowStartTime is valid (not zero)
		// If zero, it means Add() hasn't been called yet, which shouldn't happen
		// but we handle it gracefully by waiting for window size
		if firstWindowStartTime.IsZero() {
			// This shouldn't happen if Add() is called before Start(),
			// but if it does, wait for window size from now
			firstWindowStartTime = time.Now()
		}

		// Calculate time until first window ends (window size from processing time)
		now := time.Now()
		elapsed := now.Sub(firstWindowStartTime)
		var waitDuration time.Duration
		if elapsed < sw.size {
			// Wait until window size time has passed
			waitDuration = sw.size - elapsed
		} else {
			// First window already ended, trigger immediately
			waitDuration = 0
		}

		// Wait for first window to end
		if waitDuration > 0 {
			select {
			case <-time.After(waitDuration):
				// First window ended, trigger it
				sw.Trigger()
			case <-sw.ctx.Done():
				return
			}
		} else {
			// First window already ended, trigger immediately
			sw.Trigger()
		}

		// Now start the sliding step timer for subsequent windows
		sw.timerMu.Lock()
		sw.timer = time.NewTicker(sw.slide)
		sw.timerMu.Unlock()

		// Continue with periodic triggering at slide intervals
		for {
			// Safely get timer in each loop iteration
			sw.timerMu.Lock()
			timer := sw.timer
			sw.timerMu.Unlock()

			if timer == nil {
				// If timer is nil, wait briefly and retry
				select {
				case <-time.After(10 * time.Millisecond):
					continue
				case <-sw.ctx.Done():
					return
				}
			}

			select {
			// Trigger window when timer expires
			case <-timer.C:
				sw.Trigger()
			// Stop timer and exit loop when context is cancelled
			case <-sw.ctx.Done():
				sw.timerMu.Lock()
				if sw.timer != nil {
					sw.timer.Stop()
				}
				sw.timerMu.Unlock()
				return
			}
		}
	}()
}

// Stop stops the sliding window operations
func (sw *SlidingWindow) Stop() {
	// Call cancel function to stop window operations
	sw.cancelFunc()

	// Safely stop timer
	sw.timerMu.Lock()
	if sw.timer != nil {
		sw.timer.Stop()
	}
	sw.timerMu.Unlock()
}

// Trigger triggers the sliding window to process data within the window
func (sw *SlidingWindow) Trigger() {
	// Lock to ensure thread safety
	sw.mu.Lock()

	// Return directly if no data in window
	if len(sw.data) == 0 {
		sw.mu.Unlock()
		return
	}
	if !sw.initialized {
		sw.mu.Unlock()
		return
	}
	// Calculate next slot for sliding window
	next := sw.NextSlot()
	if next == nil {
		sw.mu.Unlock()
		return
	}

	// Extract Data fields to form []interface{} type data for current window
	resultData := make([]types.Row, 0)
	for _, item := range sw.data {
		if sw.currentSlot.Contains(item.Timestamp) {
			item.Slot = sw.currentSlot
			resultData = append(resultData, item)
		}
	}

	// Retain data that could be in future windows
	// For sliding windows, we need to keep data that falls within:
	// - Current window end + size (for overlapping windows)
	// - Next window end + size (for future windows)
	// Actually, we should keep all data that could be in any future window
	// The latest window that could contain a data point is: next.End + size
	cutoffTime := next.End.Add(sw.size)
	newData := make([]types.Row, 0)
	for _, item := range sw.data {
		// Keep data that could be in future windows (before cutoffTime)
		if item.Timestamp.Before(cutoffTime) {
			newData = append(newData, item)
		}
	}

	// Update window data
	sw.data = newData
	sw.currentSlot = next

	// Get callback reference before releasing lock
	callback := sw.callback

	// Release lock before calling callback and sending to channel to avoid blocking
	sw.mu.Unlock()

	// Execute callback function if set (outside of lock to avoid blocking)
	if callback != nil {
		callback(resultData)
	}

	// Non-blocking send to output channel and update statistics
	var sent bool
	select {
	case sw.outputChan <- resultData:
		// Successfully sent
		sent = true
	default:
		// Channel full, drop result
		sent = false
	}

	// Re-acquire lock to update statistics
	sw.mu.Lock()
	if sent {
		sw.sentCount++
	} else {
		sw.droppedCount++
	}
	sw.mu.Unlock()
}

// GetStats returns window performance statistics
func (sw *SlidingWindow) GetStats() map[string]int64 {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	return map[string]int64{
		"sentCount":    sw.sentCount,
		"droppedCount": sw.droppedCount,
		"bufferSize":   int64(cap(sw.outputChan)),
		"bufferUsed":   int64(len(sw.outputChan)),
	}
}

// ResetStats resets performance statistics
func (sw *SlidingWindow) ResetStats() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.sentCount = 0
	sw.droppedCount = 0
}

// Reset resets the sliding window and clears window data
func (sw *SlidingWindow) Reset() {
	// First cancel context to stop all running goroutines
	sw.cancelFunc()

	// Lock to ensure thread safety
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Stop existing timer
	sw.timerMu.Lock()
	if sw.timer != nil {
		sw.timer.Stop()
		sw.timer = nil
	}
	sw.timerMu.Unlock()

	// Clear window data
	sw.data = nil
	sw.currentSlot = nil
	sw.initialized = false
	sw.initChan = make(chan struct{})
	sw.firstWindowStartTime = time.Time{}

	// Recreate context for next startup
	sw.ctx, sw.cancelFunc = context.WithCancel(context.Background())
}

// OutputChan returns the sliding window's output channel
func (sw *SlidingWindow) OutputChan() <-chan []types.Row {
	return sw.outputChan
}

// SetCallback sets the callback function to execute when sliding window triggers
func (sw *SlidingWindow) SetCallback(callback func([]types.Row)) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.callback = callback
}

func (sw *SlidingWindow) NextSlot() *types.TimeSlot {
	if sw.currentSlot == nil {
		return nil
	}
	start := sw.currentSlot.Start.Add(sw.slide)
	end := sw.currentSlot.End.Add(sw.slide)
	next := types.NewTimeSlot(&start, &end)
	return next
}

func (sw *SlidingWindow) createSlot(t time.Time) *types.TimeSlot {
	// Create a new time slot
	start := t
	end := start.Add(sw.size)
	slot := types.NewTimeSlot(&start, &end)
	return slot
}
