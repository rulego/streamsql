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

	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
)

// EnableDebug enables debug logging for window operations
var EnableDebug = false

// debugLog logs debug information only when EnableDebug is true
// This function is optimized to avoid unnecessary string formatting when debug is disabled
func debugLog(format string, args ...any) {
	// Fast path: if debug is disabled, return immediately without evaluating args
	// The compiler should optimize this check away when EnableDebug is a compile-time constant false
	if !EnableDebug {
		return
	}
	log.Printf("[TumblingWindow] "+format, args...)
}

// Ensure TumblingWindow implements the Window interface
var _ Window = (*TumblingWindow)(nil)

// triggeredWindowInfo stores information about a triggered window that is still open for late data
type triggeredWindowInfo struct {
	slot         *types.TimeSlot
	closeTime    time.Time   // window end + allowedLateness
	snapshotData []types.Row // snapshot of window data when first triggered
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
	// wg tracks the background trigger goroutine; Stop/Reset join it before
	// mutating state the goroutine reads (e.g. watermark) outside the data lock.
	wg sync.WaitGroup
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
		cancel()
		return nil, fmt.Errorf("tumbling window requires 'size' parameter")
	}

	sizeVal := config.Params[0]
	size, err := cast.ToDurationE(sizeVal)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("invalid size for tumbling window: %v", err)
	}
	if size <= 0 {
		cancel()
		return nil, fmt.Errorf("tumbling window size must be positive, got: %v", size)
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
		triggeredWindows: make(map[string]*triggeredWindowInfo),
	}, nil
}

// Add adds data to the tumbling window
func (tw *TumblingWindow) Add(data any) {
	// Lock to ensure thread safety
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// Extract event timestamp; event-time drops rows without one instead of
	// silently substituting wall-clock time (which corrupts watermark/placement).
	eventTime, tsOk := extractTimestamp(data, tw.config.TsProp, tw.config.TimeUnit)

	// Determine time characteristic (default to ProcessingTime for backward compatibility)
	timeChar := tw.config.TimeCharacteristic
	if timeChar == "" {
		timeChar = types.ProcessingTime
	}

	if timeChar == types.EventTime {
		if !tsOk {
			return // unplaceable event: drop instead of fake wall-clock time
		}
		if tw.watermark != nil {
			tw.watermark.UpdateEventTime(eventTime)
		}
	} else if !tsOk {
		eventTime = time.Now()
	}

	// Append data to window's data list first (needed for late data handling)
	if !tw.initialized {
		if timeChar == types.EventTime {
			// For event time, align window start to window boundaries
			// Alignment ensures consistent window boundaries across different data sources
			// Alignment granularity equals window size (e.g., 2s window aligns to 2s boundaries)
			alignedStart := alignWindowStart(eventTime, tw.size)
			tw.currentSlot = tw.createSlotFromStart(alignedStart)
			debugLog("Add: initialized with EventTime, eventTime=%v, alignedStart=%v, window=[%v, %v)",
				eventTime.UnixMilli(), alignedStart.UnixMilli(),
				tw.currentSlot.Start.UnixMilli(), tw.currentSlot.End.UnixMilli())
		} else {
			// For processing time, use current time or event time as-is
			// No alignment is performed - window starts immediately when first data arrives
			tw.currentSlot = tw.createSlot(eventTime)
			debugLog("Add: initialized with ProcessingTime, eventTime=%v, window=[%v, %v)",
				eventTime.UnixMilli(),
				tw.currentSlot.Start.UnixMilli(), tw.currentSlot.End.UnixMilli())
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
	debugLog("Add: added data, eventTime=%v, totalData=%d, currentSlot=[%v, %v), inWindow=%v",
		eventTime.UnixMilli(), len(tw.data),
		tw.currentSlot.Start.UnixMilli(), tw.currentSlot.End.UnixMilli(),
		tw.currentSlot.Contains(eventTime))

	// Late data (event time): keep only what will actually be processed — a row in
	// the current not-yet-triggered window, or (when AllowedLateness > 0) a row
	// landing in a triggered window still open for late updates. Drop the rest so
	// tw.data cannot grow without bound under sustained out-of-order input.
	if timeChar == types.EventTime && tw.watermark != nil && tw.watermark.IsEventTimeLate(eventTime) {
		switch {
		case tw.initialized && tw.currentSlot != nil && tw.currentSlot.Contains(eventTime):
			// watermark advanced past the window start but the window has not
			// triggered yet; the row triggers normally, keep it.
		case tw.config.AllowedLateness > 0:
			placed := false
			for _, info := range tw.triggeredWindows {
				if info.slot.Contains(eventTime) {
					tw.handleLateData(eventTime, tw.config.AllowedLateness)
					placed = true
					break
				}
			}
			if !placed {
				// beyond allowed lateness with no open triggered window: drop
				tw.dropLastRow()
			}
		default:
			// AllowedLateness == 0 (default) and not in the current window: drop
			tw.dropLastRow()
		}
	}

}

// dropLastRow removes the row just appended by the current Add call (the last
// element of tw.data) — a late event that cannot be placed. Caller holds tw.mu.
func (tw *TumblingWindow) dropLastRow() {
	if n := len(tw.data); n > 0 {
		tw.data[n-1] = types.Row{}
		tw.data = tw.data[:n-1]
	}
}

func (tw *TumblingWindow) createSlot(t time.Time) *types.TimeSlot {
	// Processing-time windows align to epoch boundaries (like event time): a 1m
	// window ends at whole-minute marks regardless of when the first data arrived.
	start := alignWindowStart(t, tw.size)
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
	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
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
	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
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
		debugLog("checkAndTriggerWindows: not initialized or currentSlot is nil")
		return
	}

	allowedLateness := tw.config.AllowedLateness

	// Trigger all windows whose end time is <= watermark
	// Note: window end time is exclusive [start, end), so we trigger when watermark >= end
	// Windows are triggered when watermark >= windowEnd.
	// However, due to watermark calculation (watermark = maxEventTime - maxOutOfOrderness),
	// watermark may be slightly less than windowEnd. We need to handle this case.
	// If watermark is very close to windowEnd (within a small threshold), we should also trigger.
	triggeredCount := 0
	totalDataCount := len(tw.data)
	debugLog("checkAndTriggerWindows: watermark=%v, totalData=%d, currentSlot=[%v, %v)",
		watermarkTime.UnixMilli(), totalDataCount,
		tw.currentSlot.Start.UnixMilli(), tw.currentSlot.End.UnixMilli())

	for tw.currentSlot != nil {
		windowEnd := tw.currentSlot.End
		windowStart := tw.currentSlot.Start
		// Trigger if watermark >= windowEnd
		// This ensures all data for the window has arrived (within maxOutOfOrderness tolerance)
		shouldTrigger := !watermarkTime.Before(*windowEnd)

		debugLog("checkAndTriggerWindows: window=[%v, %v), watermark=%v, shouldTrigger=%v",
			windowStart.UnixMilli(), windowEnd.UnixMilli(), watermarkTime.UnixMilli(), shouldTrigger)

		if !shouldTrigger {
			// Watermark hasn't reached windowEnd yet, stop checking
			debugLog("checkAndTriggerWindows: watermark hasn't reached windowEnd, stopping")
			break
		}

		// Save current slot reference before triggering (triggerWindowLocked may release lock)
		currentSlotEnd := *tw.currentSlot.End
		currentSlot := tw.currentSlot

		// Check if window has data before triggering
		hasData := false
		dataInWindow := 0
		var dataTimestamps []int64
		for _, item := range tw.data {
			if tw.currentSlot.Contains(item.Timestamp) {
				hasData = true
				dataInWindow++
				dataTimestamps = append(dataTimestamps, item.Timestamp.UnixMilli())
			}
		}

		debugLog("checkAndTriggerWindows: window=[%v, %v), hasData=%v, dataInWindow=%d, dataTimestamps=%v",
			windowStart.UnixMilli(), windowEnd.UnixMilli(), hasData, dataInWindow, dataTimestamps)

		// Trigger current window only if it has data
		if hasData {

			// Save snapshot data before triggering
			var snapshotData []types.Row
			if allowedLateness > 0 {
				// Create a deep copy of window data for snapshot
				snapshotData = make([]types.Row, 0, dataInWindow)
				for _, item := range tw.data {
					if tw.currentSlot.Contains(item.Timestamp) {
						// Create a copy of the row
						snapshotData = append(snapshotData, types.Row{
							Data:      item.Data,
							Timestamp: item.Timestamp,
							Slot:      tw.currentSlot,
						})
					}
				}
			}

			debugLog("checkAndTriggerWindows: triggering window [%v, %v) with %d data items",
				windowStart.UnixMilli(), windowEnd.UnixMilli(), dataInWindow)

			resultData := tw.extractWindowDataLocked()

			// Register the triggered window and advance currentSlot before releasing
			// the lock for the callback, so concurrent Add doesn't see a stale slot.
			if allowedLateness > 0 {
				windowKey := tw.getWindowKey(currentSlotEnd)
				closeTime := currentSlotEnd.Add(allowedLateness)
				tw.triggeredWindows[windowKey] = &triggeredWindowInfo{
					slot:         currentSlot,
					closeTime:    closeTime,
					snapshotData: snapshotData, // Save snapshot for late updates
				}
				debugLog("checkAndTriggerWindows: window [%v, %v) kept open for late data until %v",
					windowStart.UnixMilli(), windowEnd.UnixMilli(), closeTime.UnixMilli())
			}
			tw.currentSlot = tw.NextSlot()
			if tw.currentSlot != nil {
				debugLog("checkAndTriggerWindows: moved to next window [%v, %v)",
					tw.currentSlot.Start.UnixMilli(), tw.currentSlot.End.UnixMilli())
			} else {
				debugLog("checkAndTriggerWindows: NextSlot returned nil, stopping")
			}

			if len(resultData) > 0 {
				callback := tw.callback
				tw.mu.Unlock()
				if callback != nil {
					callback(resultData)
				}
				tw.sendResult(resultData)
				tw.mu.Lock()
			}

			triggeredCount++
			debugLog("checkAndTriggerWindows: window triggered successfully, triggeredCount=%d", triggeredCount)
		} else {
			debugLog("checkAndTriggerWindows: window [%v, %v) has no data, skipping trigger",
				windowStart.UnixMilli(), windowEnd.UnixMilli())
			tw.currentSlot = tw.NextSlot()
			if tw.currentSlot == nil {
				debugLog("checkAndTriggerWindows: NextSlot returned nil, stopping")
				break
			}
		}

		if tw.currentSlot == nil {
			debugLog("checkAndTriggerWindows: currentSlot is nil, breaking")
			break
		}
	}

	debugLog("checkAndTriggerWindows: finished, triggeredCount=%d", triggeredCount)

	// Close windows that have exceeded allowedLateness
	tw.closeExpiredWindows(watermarkTime)
}

// closeExpiredWindows closes windows that have exceeded allowedLateness
func (tw *TumblingWindow) closeExpiredWindows(watermarkTime time.Time) {
	expiredWindows := make([]*types.TimeSlot, 0)
	for key, info := range tw.triggeredWindows {
		if !watermarkTime.Before(info.closeTime) {
			// Window has expired, mark for removal
			expiredWindows = append(expiredWindows, info.slot)
			delete(tw.triggeredWindows, key)
		}
	}

	// Clean up data that belongs to expired windows (if any)
	if len(expiredWindows) > 0 {
		newData := make([]types.Row, 0)
		for _, item := range tw.data {
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
		if len(newData) != len(tw.data) {
			tw.data = newData
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
			resultData := tw.extractLateUpdateDataLocked(info.slot)
			if len(resultData) > 0 {
				callback := tw.callback
				tw.mu.Unlock()
				if callback != nil {
					callback(resultData)
				}
				tw.sendResult(resultData)
				tw.mu.Lock()
			}
			return
		}
	}
}

// extractLateUpdateDataLocked extracts late update data for a window (must be called with lock held)
// Late updates include complete window data (original + late data)
func (tw *TumblingWindow) extractLateUpdateDataLocked(slot *types.TimeSlot) []types.Row {
	// Find the triggered window info to get snapshot data
	var windowInfo *triggeredWindowInfo
	windowKey := tw.getWindowKey(*slot.End)
	if info, exists := tw.triggeredWindows[windowKey]; exists {
		windowInfo = info
	}

	// Collect all data for this window: original snapshot + late data from tw.data
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

	// Add late rows from tw.data and evict them: they're merged into snapshot below.
	kept := make([]types.Row, 0, len(tw.data))
	for _, item := range tw.data {
		if slot.Contains(item.Timestamp) {
			item.Slot = slot
			resultData = append(resultData, item)
		} else {
			kept = append(kept, item)
		}
	}
	tw.data = kept

	if len(resultData) == 0 {
		return nil
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

	return resultData
}

// getWindowKey generates a key for a window based on its end time
func (tw *TumblingWindow) getWindowKey(endTime time.Time) string {
	return fmt.Sprintf("%d", endTime.UnixNano())
}

// extractWindowDataLocked extracts current window data (must be called with lock held)
func (tw *TumblingWindow) extractWindowDataLocked() []types.Row {
	if tw.currentSlot == nil {
		return nil
	}

	// Extract current window data
	resultData := make([]types.Row, 0)
	for _, item := range tw.data {
		if tw.currentSlot.Contains(item.Timestamp) {
			item.Slot = tw.currentSlot
			resultData = append(resultData, item)
		}
	}

	// Skip triggering if window has no data
	// This prevents empty windows from being triggered
	if len(resultData) == 0 {
		return nil
	}

	// Remove data that belongs to current window
	newData := make([]types.Row, 0)
	for _, item := range tw.data {
		if !tw.currentSlot.Contains(item.Timestamp) {
			newData = append(newData, item)
		}
	}
	tw.data = newData

	return resultData
}

func (tw *TumblingWindow) sendResult(data []types.Row) {
	strategy := tw.config.PerformanceConfig.OverflowConfig.Strategy
	timeout := tw.config.PerformanceConfig.OverflowConfig.BlockTimeout

	if strategy == types.OverflowStrategyBlock {
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		select {
		case tw.outputChan <- data:
			atomic.AddInt64(&tw.sentCount, 1)
		case <-time.After(timeout):
			atomic.AddInt64(&tw.droppedCount, 1)
		case <-tw.ctx.Done():
			return
		}
		return
	}

	// Default: "drop" strategy (implemented as Drop Oldest / Smart Drop)
	// If the buffer is full, remove the oldest item to make space for the new item.
	// This ensures that we always keep the most recent data, which is usually preferred in streaming.
	select {
	case tw.outputChan <- data:
		atomic.AddInt64(&tw.sentCount, 1)
	default:
		// Try to drop oldest data
		select {
		case <-tw.outputChan:
			select {
			case tw.outputChan <- data:
				atomic.AddInt64(&tw.sentCount, 1)
			default:
				atomic.AddInt64(&tw.droppedCount, 1)
			}
		default:
			atomic.AddInt64(&tw.droppedCount, 1)
		}
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
	// Retain data for the next and later windows (ts >= next start),
	// dropping the just-emitted window. Mirrors sliding_window eviction.
	nextStart := *next.Start
	newData := make([]types.Row, 0)
	for _, item := range tw.data {
		if !item.Timestamp.Before(nextStart) {
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

	// Use sendResult to respect overflow strategy
	tw.sendResult(resultData)
}

// Reset resets tumbling window data
func (tw *TumblingWindow) Reset() {
	// First cancel context to stop all running goroutines
	tw.cancelFunc()
	// Wait for the goroutine to exit before resetting state.
	tw.wg.Wait()

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
	return map[string]int64{
		"sentCount":    atomic.LoadInt64(&tw.sentCount),
		"droppedCount": atomic.LoadInt64(&tw.droppedCount),
		"bufferSize":   int64(cap(tw.outputChan)),
		"bufferUsed":   int64(len(tw.outputChan)),
	}
}

// ResetStats resets performance statistics
func (tw *TumblingWindow) ResetStats() {
	atomic.StoreInt64(&tw.sentCount, 0)
	atomic.StoreInt64(&tw.droppedCount, 0)
}
