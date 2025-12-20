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
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rulego/streamsql/utils/cast"

	"github.com/rulego/streamsql/types"
)

// debugLogSliding logs debug information only when EnableDebug is true
// This function is optimized to avoid unnecessary string formatting when debug is disabled
func debugLogSliding(format string, args ...interface{}) {
	// Fast path: if debug is disabled, return immediately without evaluating args
	// The compiler should optimize this check away when EnableDebug is a compile-time constant false
	if !EnableDebug {
		return
	}
	log.Printf("[SlidingWindow] "+format, args...)
}

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
	// timer for triggering window periodically (used for ProcessingTime)
	timer       *time.Ticker
	currentSlot *types.TimeSlot
	// initChan for window initialization
	initChan    chan struct{}
	initialized bool
	// timerMu protects timer access
	timerMu sync.Mutex
	// firstWindowStartTime records when first window started (processing time)
	firstWindowStartTime time.Time
	// watermark for event time processing (only used for EventTime)
	watermark *Watermark
	// triggeredWindows stores windows that have been triggered but are still open for late data (for EventTime with allowedLateness)
	triggeredWindows map[string]*triggeredWindowInfo // key: window end time string
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

	// Determine time characteristic (default to ProcessingTime for backward compatibility)
	timeChar := config.TimeCharacteristic
	if timeChar == "" {
		timeChar = types.ProcessingTime
	}

	// Initialize watermark for event time
	var watermark *Watermark
	if timeChar == types.EventTime {
		maxOutOfOrderness := config.MaxOutOfOrderness
		if maxOutOfOrderness == 0 {
			maxOutOfOrderness = 0 // Default: no out-of-orderness allowed
		}
		watermarkInterval := config.WatermarkInterval
		if watermarkInterval == 0 {
			watermarkInterval = 200 * time.Millisecond // Default: 200ms
		}
		idleTimeout := config.IdleTimeout
		// Default: 0 means disabled, no idle source mechanism
		watermark = NewWatermark(maxOutOfOrderness, watermarkInterval, idleTimeout)
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	return &SlidingWindow{
		config:           config,
		size:             size,
		slide:            slide,
		outputChan:       make(chan []types.Row, bufferSize),
		ctx:              ctx,
		cancelFunc:       cancel,
		data:             make([]types.Row, 0),
		initChan:         make(chan struct{}),
		initialized:      false,
		watermark:        watermark,
		triggeredWindows: make(map[string]*triggeredWindowInfo),
	}, nil
}

// Add adds data to the sliding window
func (sw *SlidingWindow) Add(data interface{}) {
	// Lock to ensure thread safety
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Get timestamp
	eventTime := GetTimestamp(data, sw.config.TsProp, sw.config.TimeUnit)

	// Determine time characteristic (default to ProcessingTime for backward compatibility)
	timeChar := sw.config.TimeCharacteristic
	if timeChar == "" {
		timeChar = types.ProcessingTime
	}

	// For event time, update watermark
	if timeChar == types.EventTime && sw.watermark != nil {
		sw.watermark.UpdateEventTime(eventTime)
	}

	// Add data to the window's data list first (needed for late data handling)
	if !sw.initialized {
		if timeChar == types.EventTime {
			// For event time, align window start to window boundaries
			alignedStart := alignWindowStart(eventTime, sw.slide)
			sw.currentSlot = sw.createSlotFromStart(alignedStart)
			debugLogSliding("Add: initialized with EventTime, eventTime=%v, alignedStart=%v, window=[%v, %v)",
				eventTime.UnixMilli(), alignedStart.UnixMilli(),
				sw.currentSlot.Start.UnixMilli(), sw.currentSlot.End.UnixMilli())
		} else {
			// For processing time, use current time or event time as-is
			sw.currentSlot = sw.createSlot(eventTime)
			// Record when first window started (processing time)
			sw.firstWindowStartTime = time.Now()
			debugLogSliding("Add: initialized with ProcessingTime, eventTime=%v, window=[%v, %v)",
				eventTime.UnixMilli(),
				sw.currentSlot.Start.UnixMilli(), sw.currentSlot.End.UnixMilli())
		}
		// Don't start timer here, wait for first window to end
		// Send initialization complete signal
		// Safely close initChan to avoid closing an already closed channel
		select {
		case <-sw.initChan:
			// Already closed, do nothing
		default:
			close(sw.initChan)
		}
		sw.initialized = true
	}
	row := types.Row{
		Data:      data,
		Timestamp: eventTime,
	}
	sw.data = append(sw.data, row)
	debugLogSliding("Add: added data, eventTime=%v, totalData=%d, currentSlot=[%v, %v), inWindow=%v",
		eventTime.UnixMilli(), len(sw.data),
		sw.currentSlot.Start.UnixMilli(), sw.currentSlot.End.UnixMilli(),
		sw.currentSlot.Contains(eventTime))

	// Check if data is late and handle allowedLateness (after data is added)
	if timeChar == types.EventTime && sw.watermark != nil {
		if sw.watermark.IsEventTimeLate(eventTime) {
			allowedLateness := sw.config.AllowedLateness
			if allowedLateness > 0 {
				// IMPORTANT: First check if this late data belongs to any triggered window that's still open
				// This ensures late data is correctly assigned to its original window, even if
				// the event time happens to fall within the current window's range
				belongsToTriggeredWindow := false
				for _, info := range sw.triggeredWindows {
					if info.slot.Contains(eventTime) {
						belongsToTriggeredWindow = true
						// Trigger late update for this window (data is already in sw.data)
						sw.handleLateData(eventTime, allowedLateness)
						break
					}
				}

				// If not belonging to triggered window, check if it belongs to currentSlot
				// This handles the case where watermark has advanced but window hasn't triggered yet
				if !belongsToTriggeredWindow && sw.initialized && sw.currentSlot != nil && sw.currentSlot.Contains(eventTime) {
					// Data belongs to currentSlot, it will be included when window triggers
					// No need to do anything here
				} else if !belongsToTriggeredWindow {
					// Check if this late data belongs to any triggered window that's still open
					sw.handleLateData(eventTime, allowedLateness)
				}
			}
			// If allowedLateness is 0 or data is too late, we still add it but it won't trigger updates
		}
	}
}

// Start starts the sliding window with periodic triggering
// Uses lazy initialization to avoid infinite waiting when no data, ensuring subsequent data can be processed normally
// First window triggers when it ends, then subsequent windows trigger at slide intervals
func (sw *SlidingWindow) Start() {
	// Determine time characteristic (default to ProcessingTime for backward compatibility)
	timeChar := sw.config.TimeCharacteristic
	if timeChar == "" {
		timeChar = types.ProcessingTime
	}

	if timeChar == types.EventTime {
		// Event time: trigger based on watermark
		sw.startEventTime()
	} else {
		// Processing time: trigger based on system clock
		sw.startProcessingTime()
	}
}

// startProcessingTime starts the processing time trigger mechanism
func (sw *SlidingWindow) startProcessingTime() {
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

// startEventTime starts the event time trigger mechanism based on watermark
func (sw *SlidingWindow) startEventTime() {
	go func() {
		// Close output channel when function ends
		defer close(sw.outputChan)
		if sw.watermark != nil {
			defer sw.watermark.Stop()
		}

		// Wait for initialization complete or context cancellation
		select {
		case <-sw.initChan:
			// Initialization completed normally, continue processing
		case <-sw.ctx.Done():
			// Context cancelled, exit directly
			return
		}

		// Process watermark updates
		if sw.watermark != nil {
			for {
				select {
				case watermarkTime := <-sw.watermark.WatermarkChan():
					sw.checkAndTriggerWindows(watermarkTime)
				case <-sw.ctx.Done():
					return
				}
			}
		}
	}()
}

// checkAndTriggerWindows checks if any windows should be triggered based on watermark
func (sw *SlidingWindow) checkAndTriggerWindows(watermarkTime time.Time) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if !sw.initialized || sw.currentSlot == nil {
		debugLogSliding("checkAndTriggerWindows: not initialized or currentSlot is nil")
		return
	}

	allowedLateness := sw.config.AllowedLateness

	// Trigger all windows whose end time is <= watermark
	// In Flink, windows are triggered when watermark >= windowEnd.
	// Watermark calculation: watermark = maxEventTime - maxOutOfOrderness
	// So watermark >= windowEnd means: maxEventTime - maxOutOfOrderness >= windowEnd
	// Which means: maxEventTime >= windowEnd + maxOutOfOrderness
	// This ensures all data for the window has arrived (within maxOutOfOrderness tolerance)
	// Use a small threshold (1ms) only for floating point precision issues
	totalDataCount := len(sw.data)
	debugLogSliding("checkAndTriggerWindows: watermark=%v, totalData=%d, currentSlot=[%v, %v)",
		watermarkTime.UnixMilli(), totalDataCount,
		sw.currentSlot.Start.UnixMilli(), sw.currentSlot.End.UnixMilli())

	for sw.currentSlot != nil {
		windowEnd := sw.currentSlot.End
		windowStart := sw.currentSlot.Start

		// Check if watermark >= windowEnd
		// Use !Before() instead of After() to include equality case
		// This is equivalent to watermarkTime >= windowEnd
		shouldTrigger := !watermarkTime.Before(*windowEnd)

		debugLogSliding("checkAndTriggerWindows: window=[%v, %v), watermark=%v, shouldTrigger=%v",
			windowStart.UnixMilli(), windowEnd.UnixMilli(), watermarkTime.UnixMilli(), shouldTrigger)

		if !shouldTrigger {
			// Watermark hasn't reached windowEnd yet, stop checking
			debugLogSliding("checkAndTriggerWindows: watermark hasn't reached windowEnd, stopping")
			break
		}

		// Save current slot reference before triggering
		// We need to advance sw.currentSlot BEFORE calling triggerSpecificWindowLocked
		// because triggerSpecificWindowLocked releases the lock, and we want to prevent
		// re-entry for the same window.
		slotToTrigger := sw.currentSlot

		// Move to next window immediately
		sw.currentSlot = sw.NextSlot()
		if sw.currentSlot != nil {
			debugLogSliding("checkAndTriggerWindows: moved to next window [%v, %v)",
				sw.currentSlot.Start.UnixMilli(), sw.currentSlot.End.UnixMilli())
		} else {
			debugLogSliding("checkAndTriggerWindows: NextSlot returned nil")
		}

		// Check if window has data before triggering
		hasData := false
		dataInWindow := 0
		var dataTimestamps []int64
		for _, item := range sw.data {
			if slotToTrigger.Contains(item.Timestamp) {
				hasData = true
				dataInWindow++
				dataTimestamps = append(dataTimestamps, item.Timestamp.UnixMilli())
			}
		}

		debugLogSliding("checkAndTriggerWindows: window=[%v, %v), hasData=%v, dataInWindow=%d, dataTimestamps=%v",
			windowStart.UnixMilli(), windowEnd.UnixMilli(), hasData, dataInWindow, dataTimestamps)

		// Trigger current window only if it has data
		if hasData {
			// Count data in window before triggering (re-count? redundant but harmless)
			// Actually we already counted dataInWindow above

			// Save snapshot data before triggering (for Flink-like late update behavior)
			var snapshotData []types.Row
			if allowedLateness > 0 {
				// Create a deep copy of window data for snapshot
				snapshotData = make([]types.Row, 0, dataInWindow)
				for _, item := range sw.data {
					if slotToTrigger.Contains(item.Timestamp) {
						// Create a copy of the row
						snapshotData = append(snapshotData, types.Row{
							Data:      item.Data,
							Timestamp: item.Timestamp,
							Slot:      slotToTrigger,
						})
					}
				}
			}

			debugLogSliding("checkAndTriggerWindows: triggering window [%v, %v) with %d data items",
				windowStart.UnixMilli(), windowEnd.UnixMilli(), dataInWindow)

			sw.triggerSpecificWindowLocked(slotToTrigger)

			debugLogSliding("checkAndTriggerWindows: window triggered successfully")

			// If allowedLateness > 0, keep window open for late data
			if allowedLateness > 0 {
				windowKey := sw.getWindowKey(*slotToTrigger.End)
				closeTime := slotToTrigger.End.Add(allowedLateness)
				sw.triggeredWindows[windowKey] = &triggeredWindowInfo{
					slot:         slotToTrigger,
					closeTime:    closeTime,
					snapshotData: snapshotData, // Save snapshot for late updates
				}
				debugLogSliding("checkAndTriggerWindows: window [%v, %v) kept open for late data until %v",
					windowStart.UnixMilli(), windowEnd.UnixMilli(), closeTime.UnixMilli())
			}
		} else {
			debugLogSliding("checkAndTriggerWindows: window [%v, %v) has no data, skipping trigger",
				windowStart.UnixMilli(), windowEnd.UnixMilli())
		}
	}

	// Close windows that have exceeded allowedLateness
	sw.closeExpiredWindows(watermarkTime)
}

// extractWindowDataLocked extracts window data for the given slot (must be called with lock held)
func (sw *SlidingWindow) extractWindowDataLocked(slot *types.TimeSlot) []types.Row {
	if slot == nil {
		return nil
	}

	// Extract current window data
	resultData := make([]types.Row, 0)
	for _, item := range sw.data {
		if slot.Contains(item.Timestamp) {
			item.Slot = slot
			resultData = append(resultData, item)
		}
	}

	// Skip triggering if window has no data
	// This prevents empty windows from being triggered
	if len(resultData) == 0 {
		return nil
	}

	// Remove data that is no longer needed
	// For sliding windows, data can belong to multiple windows
	// We only remove data that is older than the start of the next window
	// because any data before that will not be included in future windows.

	nextWindowStart := slot.Start.Add(sw.slide)
	newData := make([]types.Row, 0)
	for _, item := range sw.data {
		if !item.Timestamp.Before(nextWindowStart) {
			newData = append(newData, item)
		}
	}
	sw.data = newData

	return resultData
}

// triggerSpecificWindowLocked triggers the specified window (must be called with lock held)
func (sw *SlidingWindow) triggerSpecificWindowLocked(slot *types.TimeSlot) {
	resultData := sw.extractWindowDataLocked(slot)
	if len(resultData) == 0 {
		return
	}

	// Get callback reference before releasing lock
	callback := sw.callback

	// Release lock before calling callback and sending to channel to avoid blocking
	sw.mu.Unlock()

	if callback != nil {
		callback(resultData)
	}

	sw.sendResult(resultData)

	// Re-acquire lock to update statistics
	sw.mu.Lock()
}

// Stop stops the sliding window operations
func (sw *SlidingWindow) Stop() {
	// Call cancel function to stop window operations
	sw.cancelFunc()

	// Safely stop timer (for processing time)
	sw.timerMu.Lock()
	if sw.timer != nil {
		sw.timer.Stop()
	}
	sw.timerMu.Unlock()

	// Stop watermark (for event time)
	if sw.watermark != nil {
		sw.watermark.Stop()
	}

	// Ensure initChan is closed if it hasn't been closed yet
	// This prevents Start() goroutine from blocking on initChan
	sw.mu.Lock()
	if !sw.initialized && sw.initChan != nil {
		select {
		case <-sw.initChan:
			// Already closed, do nothing
		default:
			close(sw.initChan)
		}
	}
	sw.mu.Unlock()
}

// Trigger triggers the sliding window to process data within the window
// For ProcessingTime: called by timer
// For EventTime: called by watermark updates
func (sw *SlidingWindow) Trigger() {
	// Determine time characteristic
	timeChar := sw.config.TimeCharacteristic
	if timeChar == "" {
		timeChar = types.ProcessingTime
	}

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

	if timeChar == types.EventTime {
		// For event time, trigger is handled by watermark mechanism
		// This method is kept for backward compatibility but shouldn't be called directly
		sw.mu.Unlock()
		return
	}

	// Processing time logic
	// Calculate next slot for sliding window
	next := sw.NextSlot()
	if next == nil {
		sw.mu.Unlock()
		return
	}

	// Extract current window data
	currentSlot := sw.currentSlot
	sw.currentSlot = next

	resultData := sw.extractWindowDataLocked(currentSlot)

	if len(resultData) == 0 {
		sw.mu.Unlock()
		return
	}

	// Get callback reference before releasing lock
	callback := sw.callback

	// Release lock before calling callback and sending to channel to avoid blocking
	sw.mu.Unlock()

	if callback != nil {
		callback(resultData)
	}

	sw.sendResult(resultData)
}

func (sw *SlidingWindow) sendResult(data []types.Row) {
	strategy := sw.config.PerformanceConfig.OverflowConfig.Strategy
	timeout := sw.config.PerformanceConfig.OverflowConfig.BlockTimeout

	if strategy == types.OverflowStrategyBlock {
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		select {
		case sw.outputChan <- data:
			atomic.AddInt64(&sw.sentCount, 1)
		case <-time.After(timeout):
			atomic.AddInt64(&sw.droppedCount, 1)
		case <-sw.ctx.Done():
			return
		}
		return
	}

	// Default: "drop" strategy (implemented as Drop Oldest / Smart Drop)
	select {
	case sw.outputChan <- data:
		atomic.AddInt64(&sw.sentCount, 1)
	default:
		// Try to drop oldest data
		select {
		case <-sw.outputChan:
			select {
			case sw.outputChan <- data:
				atomic.AddInt64(&sw.sentCount, 1)
			default:
				atomic.AddInt64(&sw.droppedCount, 1)
			}
		default:
			atomic.AddInt64(&sw.droppedCount, 1)
		}
	}
}

// GetStats returns window performance statistics
func (sw *SlidingWindow) GetStats() map[string]int64 {
	return map[string]int64{
		"sentCount":    atomic.LoadInt64(&sw.sentCount),
		"droppedCount": atomic.LoadInt64(&sw.droppedCount),
		"bufferSize":   int64(cap(sw.outputChan)),
		"bufferUsed":   int64(len(sw.outputChan)),
	}
}

// ResetStats resets performance statistics
func (sw *SlidingWindow) ResetStats() {
	atomic.StoreInt64(&sw.sentCount, 0)
	atomic.StoreInt64(&sw.droppedCount, 0)
}

// Reset resets the sliding window and clears window data
func (sw *SlidingWindow) Reset() {
	// First cancel context to stop all running goroutines
	sw.cancelFunc()

	// Lock to ensure thread safety
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Stop existing timer (for processing time)
	sw.timerMu.Lock()
	if sw.timer != nil {
		sw.timer.Stop()
		sw.timer = nil
	}
	sw.timerMu.Unlock()

	// Stop watermark (for event time)
	if sw.watermark != nil {
		sw.watermark.Stop()
		// Recreate watermark
		timeChar := sw.config.TimeCharacteristic
		if timeChar == "" {
			timeChar = types.ProcessingTime
		}
		if timeChar == types.EventTime {
			maxOutOfOrderness := sw.config.MaxOutOfOrderness
			if maxOutOfOrderness == 0 {
				maxOutOfOrderness = 0
			}
			watermarkInterval := sw.config.WatermarkInterval
			if watermarkInterval == 0 {
				watermarkInterval = 200 * time.Millisecond
			}
			idleTimeout := sw.config.IdleTimeout
			sw.watermark = NewWatermark(maxOutOfOrderness, watermarkInterval, idleTimeout)
		}
	}

	// Clear window data
	sw.data = nil
	sw.currentSlot = nil
	sw.initialized = false
	sw.initChan = make(chan struct{})
	sw.firstWindowStartTime = time.Time{}
	sw.triggeredWindows = make(map[string]*triggeredWindowInfo)

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
	// Create a new time slot (for processing time, no alignment needed)
	start := t
	end := start.Add(sw.size)
	slot := types.NewTimeSlot(&start, &end)
	return slot
}

func (sw *SlidingWindow) createSlotFromStart(start time.Time) *types.TimeSlot {
	// Create a new time slot from aligned start time (for event time)
	end := start.Add(sw.size)
	slot := types.NewTimeSlot(&start, &end)
	return slot
}

// getWindowKey generates a key for a window based on its end time
func (sw *SlidingWindow) getWindowKey(endTime time.Time) string {
	return fmt.Sprintf("%d", endTime.UnixNano())
}

// handleLateData handles late data that arrives within allowedLateness
func (sw *SlidingWindow) handleLateData(eventTime time.Time, allowedLateness time.Duration) {
	// Find which triggered window this late data belongs to
	for _, info := range sw.triggeredWindows {
		if info.slot.Contains(eventTime) {
			// This late data belongs to a triggered window that's still open
			// Trigger window again with updated data (late update)
			sw.triggerLateUpdateLocked(info.slot)
			return
		}
	}
}

// triggerLateUpdateLocked triggers a late update for a window (must be called with lock held)
// This implements Flink-like behavior: late updates include complete window data (original + late data)
func (sw *SlidingWindow) triggerLateUpdateLocked(slot *types.TimeSlot) {
	// Find the triggered window info to get snapshot data
	var windowInfo *triggeredWindowInfo
	windowKey := sw.getWindowKey(*slot.End)
	if info, exists := sw.triggeredWindows[windowKey]; exists {
		windowInfo = info
	}

	// Collect all data for this window: original snapshot + late data from sw.data
	resultData := make([]types.Row, 0)

	// First, add original snapshot data (if exists)
	if windowInfo != nil && len(windowInfo.snapshotData) > 0 {
		// Create copies of snapshot data
		for _, item := range windowInfo.snapshotData {
			resultData = append(resultData, types.Row{
				Data:      item.Data,
				Timestamp: item.Timestamp,
				Slot:      slot, // Update slot reference
			})
		}
	}

	// Then, add late data from sw.data (newly arrived late data)
	lateDataCount := 0
	for _, item := range sw.data {
		if slot.Contains(item.Timestamp) {
			item.Slot = slot
			resultData = append(resultData, item)
			lateDataCount++
		}
	}

	if len(resultData) == 0 {
		return
	}

	// Update snapshot to include late data (for future late updates)
	if windowInfo != nil {
		// Update snapshot with complete data (original + late)
		windowInfo.snapshotData = make([]types.Row, len(resultData))
		for i, item := range resultData {
			windowInfo.snapshotData[i] = types.Row{
				Data:      item.Data,
				Timestamp: item.Timestamp,
				Slot:      slot,
			}
		}
	}

	// Get callback reference before releasing lock
	callback := sw.callback

	// Release lock before calling callback and sending to channel to avoid blocking
	sw.mu.Unlock()

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
}

// closeExpiredWindows closes windows that have exceeded allowedLateness
func (sw *SlidingWindow) closeExpiredWindows(watermarkTime time.Time) {
	expiredWindows := make([]*types.TimeSlot, 0)
	for key, info := range sw.triggeredWindows {
		if !watermarkTime.Before(info.closeTime) {
			// Window has expired, mark for removal
			expiredWindows = append(expiredWindows, info.slot)
			delete(sw.triggeredWindows, key)
		}
	}

	// Clean up data that belongs to expired windows (if any)
	if len(expiredWindows) > 0 {
		newData := make([]types.Row, 0)
		for _, item := range sw.data {
			belongsToExpiredWindow := false
			for _, expiredSlot := range expiredWindows {
				if expiredSlot.Contains(item.Timestamp) {
					belongsToExpiredWindow = true
					break
				}
			}
			if !belongsToExpiredWindow {
				newData = append(newData, item)
			}
		}
		if len(newData) != len(sw.data) {
			sw.data = newData
		}
	}
}
