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

	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
)

// Ensure TumblingWindow implements the Window interface
var _ Window = (*TumblingWindow)(nil)

// triggeredWindowInfo stores information about a triggered window that is still open for late data
type triggeredWindowInfo struct {
	slot      *types.TimeSlot
	closeTime time.Time // window end + allowedLateness
}

// TumblingWindow represents a tumbling window for collecting data and triggering processing at fixed time intervals
type TumblingWindow struct {
	// config holds window configuration
	config types.WindowConfig
	// size is the time size of tumbling window (window duration)
	size time.Duration
	// mu protects concurrent access to window data
	mu sync.RWMutex
	// data stores collected data within the window
	data []types.Row
	// outputChan is a channel for sending data when window triggers
	outputChan chan []types.Row
	// callback is an optional callback function called when window triggers
	callback func([]types.Row)
	// ctx controls window lifecycle
	ctx context.Context
	// cancelFunc cancels window operations
	cancelFunc context.CancelFunc
	// timer for triggering window periodically (used for ProcessingTime)
	timer       *time.Ticker
	currentSlot *types.TimeSlot
	// initChan for window initialization
	initChan    chan struct{}
	initialized bool
	// timerMu protects timer access
	timerMu sync.Mutex
	// watermark for event time processing (only used for EventTime)
	watermark *Watermark
	// pendingWindows stores windows waiting to be triggered (for EventTime)
	pendingWindows map[string]*types.TimeSlot // key: window end time string
	// triggeredWindows stores windows that have been triggered but are still open for late data (for EventTime with allowedLateness)
	triggeredWindows map[string]*triggeredWindowInfo // key: window end time string
	// Performance statistics
	droppedCount int64 // Number of dropped results
	sentCount    int64 // Number of successfully sent results
}

// NewTumblingWindow creates a new tumbling window instance
// Parameter size is the time size of the window
func NewTumblingWindow(config types.WindowConfig) (*TumblingWindow, error) {
	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Get size parameter from params array
	if len(config.Params) == 0 {
		return nil, fmt.Errorf("tumbling window requires 'size' parameter")
	}

	sizeVal := config.Params[0]
	size, err := cast.ToDurationE(sizeVal)
	if err != nil {
		return nil, fmt.Errorf("invalid size for tumbling window: %v", err)
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

	return &TumblingWindow{
		config:           config,
		size:             size,
		outputChan:       make(chan []types.Row, bufferSize),
		ctx:              ctx,
		cancelFunc:       cancel,
		initChan:         make(chan struct{}),
		initialized:      false,
		watermark:        watermark,
		pendingWindows:   make(map[string]*types.TimeSlot),
		triggeredWindows: make(map[string]*triggeredWindowInfo),
	}, nil
}

// Add adds data to the tumbling window
func (tw *TumblingWindow) Add(data interface{}) {
	// Lock to ensure thread safety
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// Get timestamp
	eventTime := GetTimestamp(data, tw.config.TsProp, tw.config.TimeUnit)

	// Determine time characteristic (default to ProcessingTime for backward compatibility)
	timeChar := tw.config.TimeCharacteristic
	if timeChar == "" {
		timeChar = types.ProcessingTime
	}

	// For event time, update watermark and check for late data
	if timeChar == types.EventTime && tw.watermark != nil {
		tw.watermark.UpdateEventTime(eventTime)
		// Check if data is late and handle allowedLateness
		if tw.watermark.IsEventTimeLate(eventTime) {
			// Data is late, check if it's within allowedLateness
			allowedLateness := tw.config.AllowedLateness
			if allowedLateness > 0 {
				// Check if this late data belongs to any triggered window that's still open
				tw.handleLateData(eventTime, allowedLateness)
			}
			// If allowedLateness is 0 or data is too late, we still add it but it won't trigger updates
		}
	}

	// Append data to window's data list
	if !tw.initialized {
		if timeChar == types.EventTime {
			// For event time, align window start to window boundaries
			alignedStart := alignWindowStart(eventTime, tw.size)
			tw.currentSlot = tw.createSlotFromStart(alignedStart)
		} else {
			// For processing time, use current time or event time as-is
			tw.currentSlot = tw.createSlot(eventTime)
		}

		// Only start timer for processing time
		if timeChar == types.ProcessingTime {
			tw.timerMu.Lock()
			tw.timer = time.NewTicker(tw.size)
			tw.timerMu.Unlock()
		}

		tw.initialized = true
		// Send initialization complete signal (after setting timer)
		// Safely close initChan to avoid closing an already closed channel
		select {
		case <-tw.initChan:
			// Already closed, do nothing
		default:
			close(tw.initChan)
		}
	}

	row := types.Row{
		Data:      data,
		Timestamp: eventTime,
	}
	tw.data = append(tw.data, row)
}

func (tw *TumblingWindow) createSlot(t time.Time) *types.TimeSlot {
	// Create a new time slot (for processing time, no alignment needed)
	start := t
	end := start.Add(tw.size)
	slot := types.NewTimeSlot(&start, &end)
	return slot
}

func (tw *TumblingWindow) createSlotFromStart(start time.Time) *types.TimeSlot {
	// Create a new time slot from aligned start time (for event time)
	end := start.Add(tw.size)
	slot := types.NewTimeSlot(&start, &end)
	return slot
}

func (tw *TumblingWindow) NextSlot() *types.TimeSlot {
	if tw.currentSlot == nil {
		return nil
	}
	start := tw.currentSlot.End
	end := start.Add(tw.size)
	return types.NewTimeSlot(start, &end)
}

// Stop stops tumbling window operations
func (tw *TumblingWindow) Stop() {
	// Call cancel function to stop window operations
	tw.cancelFunc()

	// Safely stop timer (for processing time)
	tw.timerMu.Lock()
	if tw.timer != nil {
		tw.timer.Stop()
	}
	tw.timerMu.Unlock()

	// Stop watermark (for event time)
	if tw.watermark != nil {
		tw.watermark.Stop()
	}

	// Ensure initChan is closed if it hasn't been closed yet
	// This prevents Start() goroutine from blocking on initChan
	tw.mu.Lock()
	if !tw.initialized && tw.initChan != nil {
		select {
		case <-tw.initChan:
			// Already closed, do nothing
		default:
			close(tw.initChan)
		}
	}
	tw.mu.Unlock()
}

// Start starts the tumbling window's periodic trigger mechanism
// Uses lazy initialization to avoid infinite waiting when no data, ensuring subsequent data can be processed normally
func (tw *TumblingWindow) Start() {
	// Determine time characteristic (default to ProcessingTime for backward compatibility)
	timeChar := tw.config.TimeCharacteristic
	if timeChar == "" {
		timeChar = types.ProcessingTime
	}

	if timeChar == types.EventTime {
		// Event time: trigger based on watermark
		tw.startEventTime()
	} else {
		// Processing time: trigger based on system clock
		tw.startProcessingTime()
	}
}

// startProcessingTime starts the processing time trigger mechanism
func (tw *TumblingWindow) startProcessingTime() {
	go func() {
		// Close output channel when function ends
		defer close(tw.outputChan)

		// Wait for initialization complete or context cancellation
		select {
		case <-tw.initChan:
			// Initialization completed normally, continue processing
		case <-tw.ctx.Done():
			// Context cancelled, exit directly
			return
		}

		for {
			// Safely get timer in each loop iteration
			tw.timerMu.Lock()
			timer := tw.timer
			tw.timerMu.Unlock()

			if timer == nil {
				// If timer is nil, wait briefly and retry
				select {
				case <-time.After(10 * time.Millisecond):
					continue
				case <-tw.ctx.Done():
					return
				}
			}

			select {
			// Trigger window when timer expires
			case <-timer.C:
				tw.Trigger()
			// Stop timer and exit loop when context is cancelled
			case <-tw.ctx.Done():
				tw.timerMu.Lock()
				if tw.timer != nil {
					tw.timer.Stop()
				}
				tw.timerMu.Unlock()
				return
			}
		}
	}()
}

// startEventTime starts the event time trigger mechanism based on watermark
func (tw *TumblingWindow) startEventTime() {
	go func() {
		// Close output channel when function ends
		defer close(tw.outputChan)
		if tw.watermark != nil {
			defer tw.watermark.Stop()
		}

		// Wait for initialization complete or context cancellation
		select {
		case <-tw.initChan:
			// Initialization completed normally, continue processing
		case <-tw.ctx.Done():
			// Context cancelled, exit directly
			return
		}

		// Process watermark updates
		if tw.watermark != nil {
			for {
				select {
				case watermarkTime := <-tw.watermark.WatermarkChan():
					tw.checkAndTriggerWindows(watermarkTime)
				case <-tw.ctx.Done():
					return
				}
			}
		}
	}()
}

// checkAndTriggerWindows checks if any windows should be triggered based on watermark
func (tw *TumblingWindow) checkAndTriggerWindows(watermarkTime time.Time) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if !tw.initialized || tw.currentSlot == nil {
		return
	}

	allowedLateness := tw.config.AllowedLateness

	// Trigger all windows whose end time is before watermark
	for tw.currentSlot != nil && !tw.currentSlot.End.After(watermarkTime) {
		// Trigger current window
		tw.triggerWindowLocked()

		// If allowedLateness > 0, keep window open for late data
		if allowedLateness > 0 {
			windowKey := tw.getWindowKey(*tw.currentSlot.End)
			closeTime := tw.currentSlot.End.Add(allowedLateness)
			tw.triggeredWindows[windowKey] = &triggeredWindowInfo{
				slot:      tw.currentSlot,
				closeTime: closeTime,
			}
		}

		// Move to next window
		tw.currentSlot = tw.NextSlot()
	}

	// Close windows that have exceeded allowedLateness
	tw.closeExpiredWindows(watermarkTime)
}

// closeExpiredWindows closes windows that have exceeded allowedLateness
func (tw *TumblingWindow) closeExpiredWindows(watermarkTime time.Time) {
	for key, info := range tw.triggeredWindows {
		if !watermarkTime.Before(info.closeTime) {
			// Window has expired, remove it
			delete(tw.triggeredWindows, key)
		}
	}
}

// handleLateData handles late data that arrives within allowedLateness
func (tw *TumblingWindow) handleLateData(eventTime time.Time, allowedLateness time.Duration) {
	// Find which triggered window this late data belongs to
	for _, info := range tw.triggeredWindows {
		if info.slot.Contains(eventTime) {
			// This late data belongs to a triggered window that's still open
			// Trigger window again with updated data (late update)
			tw.triggerLateUpdateLocked(info.slot)
			return
		}
	}
}

// triggerLateUpdateLocked triggers a late update for a window (must be called with lock held)
func (tw *TumblingWindow) triggerLateUpdateLocked(slot *types.TimeSlot) {
	// Extract window data including late data
	resultData := make([]types.Row, 0)
	for _, item := range tw.data {
		if slot.Contains(item.Timestamp) {
			item.Slot = slot
			resultData = append(resultData, item)
		}
	}

	if len(resultData) == 0 {
		return
	}

	// Get callback reference before releasing lock
	callback := tw.callback

	// Release lock before calling callback and sending to channel to avoid blocking
	tw.mu.Unlock()

	if callback != nil {
		callback(resultData)
	}

	// Non-blocking send to output channel and update statistics
	var sent bool
	select {
	case tw.outputChan <- resultData:
		// Successfully sent
		sent = true
	default:
		// Channel full, drop result
		sent = false
	}

	// Re-acquire lock to update statistics
	tw.mu.Lock()
	if sent {
		tw.sentCount++
	} else {
		tw.droppedCount++
	}
}

// getWindowKey generates a key for a window based on its end time
func (tw *TumblingWindow) getWindowKey(endTime time.Time) string {
	return fmt.Sprintf("%d", endTime.UnixNano())
}

// triggerWindowLocked triggers the window (must be called with lock held)
func (tw *TumblingWindow) triggerWindowLocked() {
	if tw.currentSlot == nil {
		return
	}

	// Extract current window data
	resultData := make([]types.Row, 0)
	for _, item := range tw.data {
		if tw.currentSlot.Contains(item.Timestamp) {
			item.Slot = tw.currentSlot
			resultData = append(resultData, item)
		}
	}

	// Remove data that belongs to current window
	newData := make([]types.Row, 0)
	for _, item := range tw.data {
		if !tw.currentSlot.Contains(item.Timestamp) {
			newData = append(newData, item)
		}
	}
	tw.data = newData

	// Get callback reference before releasing lock
	callback := tw.callback

	// Release lock before calling callback and sending to channel to avoid blocking
	tw.mu.Unlock()

	if callback != nil {
		callback(resultData)
	}

	// Non-blocking send to output channel and update statistics
	var sent bool
	select {
	case tw.outputChan <- resultData:
		// Successfully sent
		sent = true
	default:
		// Channel full, drop result
		sent = false
	}

	// Re-acquire lock to update statistics
	tw.mu.Lock()
	if sent {
		tw.sentCount++
	} else {
		tw.droppedCount++
	}
}

// Trigger triggers the tumbling window's processing logic
// For ProcessingTime: called by timer
// For EventTime: called by watermark updates
func (tw *TumblingWindow) Trigger() {
	// Determine time characteristic
	timeChar := tw.config.TimeCharacteristic
	if timeChar == "" {
		timeChar = types.ProcessingTime
	}

	tw.mu.Lock()

	if !tw.initialized {
		tw.mu.Unlock()
		return
	}

	if timeChar == types.EventTime {
		// For event time, trigger is handled by watermark mechanism
		// This method is kept for backward compatibility but shouldn't be called directly
		tw.mu.Unlock()
		return
	}

	// Processing time logic
	// Calculate next window slot
	next := tw.NextSlot()
	// Retain data for next window
	tms := next.Start.Add(-tw.size)
	tme := next.End.Add(tw.size)
	temp := types.NewTimeSlot(&tms, &tme)
	newData := make([]types.Row, 0)
	for _, item := range tw.data {
		if temp.Contains(item.Timestamp) {
			newData = append(newData, item)
		}
	}

	// Extract current window data
	resultData := make([]types.Row, 0)
	for _, item := range tw.data {
		if tw.currentSlot.Contains(item.Timestamp) {
			item.Slot = tw.currentSlot
			resultData = append(resultData, item)
		}
	}

	// If resultData is empty, skip callback to avoid sending empty results
	// This prevents empty results from filling up channels when timer triggers repeatedly
	if len(resultData) == 0 {
		// Update window data even if no result
		tw.data = newData
		tw.currentSlot = next
		tw.mu.Unlock()
		return
	}

	// Update window data
	tw.data = newData
	tw.currentSlot = next

	// Get callback reference before releasing lock
	callback := tw.callback

	// Release lock before calling callback and sending to channel to avoid blocking
	tw.mu.Unlock()

	if callback != nil {
		callback(resultData)
	}

	// Non-blocking send to output channel and update statistics
	var sent bool
	select {
	case tw.outputChan <- resultData:
		// Successfully sent
		sent = true
	default:
		// Channel full, drop result
		sent = false
	}

	// Re-acquire lock to update statistics
	tw.mu.Lock()
	if sent {
		tw.sentCount++
	} else {
		tw.droppedCount++
		// Optional: add logging here
		// log.Printf("Window output channel full, dropped result with %d rows", len(resultData))
	}
	tw.mu.Unlock()
}

// Reset resets tumbling window data
func (tw *TumblingWindow) Reset() {
	// First cancel context to stop all running goroutines
	tw.cancelFunc()

	// Lock to ensure thread safety
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// Stop existing timer (for processing time)
	tw.timerMu.Lock()
	if tw.timer != nil {
		tw.timer.Stop()
		tw.timer = nil
	}
	tw.timerMu.Unlock()

	// Stop watermark (for event time)
	if tw.watermark != nil {
		tw.watermark.Stop()
		// Recreate watermark
		timeChar := tw.config.TimeCharacteristic
		if timeChar == "" {
			timeChar = types.ProcessingTime
		}
		if timeChar == types.EventTime {
			maxOutOfOrderness := tw.config.MaxOutOfOrderness
			if maxOutOfOrderness == 0 {
				maxOutOfOrderness = 0
			}
			watermarkInterval := tw.config.WatermarkInterval
			if watermarkInterval == 0 {
				watermarkInterval = 200 * time.Millisecond
			}
			idleTimeout := tw.config.IdleTimeout
			tw.watermark = NewWatermark(maxOutOfOrderness, watermarkInterval, idleTimeout)
		}
	}

	// Clear window data
	tw.data = nil
	tw.currentSlot = nil
	tw.initialized = false
	tw.initChan = make(chan struct{})
	tw.pendingWindows = make(map[string]*types.TimeSlot)
	tw.triggeredWindows = make(map[string]*triggeredWindowInfo)

	// Recreate context for next startup
	tw.ctx, tw.cancelFunc = context.WithCancel(context.Background())
}

// OutputChan returns a read-only channel for receiving data when window triggers
func (tw *TumblingWindow) OutputChan() <-chan []types.Row {
	return tw.outputChan
}

// SetCallback sets the callback function to execute when tumbling window triggers
func (tw *TumblingWindow) SetCallback(callback func([]types.Row)) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.callback = callback
}

// GetStats returns window performance statistics
func (tw *TumblingWindow) GetStats() map[string]int64 {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	return map[string]int64{
		"sentCount":    tw.sentCount,
		"droppedCount": tw.droppedCount,
		"bufferSize":   int64(cap(tw.outputChan)),
		"bufferUsed":   int64(len(tw.outputChan)),
	}
}

// ResetStats resets performance statistics
func (tw *TumblingWindow) ResetStats() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.sentCount = 0
	tw.droppedCount = 0
}
