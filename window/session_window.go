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
	"strings"
	"sync"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
)

// Ensure SessionWindow struct implements Window interface
var _ Window = (*SessionWindow)(nil)

// SessionWindow represents a session window
// Session window is an event-time based window that closes when no events arrive for a period of time
type SessionWindow struct {
	// config is the window configuration information
	config types.WindowConfig
	// timeout is the session timeout duration, session will close if no new events within this time
	timeout time.Duration
	// mu is used to protect concurrent access to window data
	mu sync.RWMutex
	// sessionMap stores session data for different keys
	sessionMap map[string]*session
	// outputChan is a channel for sending data when window triggers
	outputChan chan []types.Row
	// callback is an optional callback function called when window triggers
	callback func([]types.Row)
	// ctx is used to control window lifecycle
	ctx context.Context
	// cancelFunc is used to cancel window operations
	cancelFunc context.CancelFunc
	// Channel for initializing window
	initChan    chan struct{}
	initialized bool
	// Lock to protect ticker
	tickerMu sync.Mutex
	ticker   *time.Ticker
	// watermark for event time processing (only used for EventTime)
	watermark *Watermark
	// triggeredSessions stores sessions that have been triggered but are still open for late data (for EventTime with allowedLateness)
	triggeredSessions map[string]*sessionInfo
}

// sessionInfo stores information about a triggered session that is still open for late data
type sessionInfo struct {
	session   *session
	closeTime time.Time // session end + allowedLateness
}

// session stores data and state for a session
type session struct {
	data       []types.Row
	lastActive time.Time
	slot       *types.TimeSlot
}

// NewSessionWindow creates a new session window instance
func NewSessionWindow(config types.WindowConfig) (*SessionWindow, error) {
	// Get timeout parameter from params array
	if len(config.Params) == 0 {
		return nil, fmt.Errorf("session window requires 'timeout' parameter")
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	timeoutVal := config.Params[0]
	timeout, err := cast.ToDurationE(timeoutVal)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("invalid timeout for session window: %v", err)
	}

	// Use unified performance configuration to get window output buffer size
	bufferSize := 100 // Default value
	if (config.PerformanceConfig != types.PerformanceConfig{}) {
		bufferSize = config.PerformanceConfig.BufferConfig.WindowOutputSize
		if bufferSize < 10 {
			bufferSize = 10 // Minimum value
		}
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

	return &SessionWindow{
		config:            config,
		timeout:           timeout,
		sessionMap:        make(map[string]*session),
		outputChan:        make(chan []types.Row, bufferSize),
		ctx:               ctx,
		cancelFunc:        cancel,
		initChan:          make(chan struct{}),
		initialized:       false,
		watermark:         watermark,
		triggeredSessions: make(map[string]*sessionInfo),
	}, nil
}

// Add adds data to session window
func (sw *SessionWindow) Add(data interface{}) {
	// Lock to ensure thread safety
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if !sw.initialized {
		// Safely close initChan to avoid closing an already closed channel
		select {
		case <-sw.initChan:
			// Already closed, do nothing
		default:
			close(sw.initChan)
		}
		sw.initialized = true
	}

	// Get data timestamp
	timestamp := GetTimestamp(data, sw.config.TsProp, sw.config.TimeUnit)

	// Determine time characteristic (default to ProcessingTime for backward compatibility)
	timeChar := sw.config.TimeCharacteristic
	if timeChar == "" {
		timeChar = types.ProcessingTime
	}

	// For event time, update watermark and check for late data
	if timeChar == types.EventTime && sw.watermark != nil {
		sw.watermark.UpdateEventTime(timestamp)
		// Check if data is late and handle allowedLateness
		if sw.watermark.IsEventTimeLate(timestamp) {
			// Data is late, check if it's within allowedLateness
			allowedLateness := sw.config.AllowedLateness
			if allowedLateness > 0 {
				// Check if this late data belongs to any triggered session that's still open
				sw.handleLateData(timestamp, allowedLateness)
			}
			// If allowedLateness is 0 or data is too late, we still add it but it won't trigger updates
		}
	}

	// Create Row object
	row := types.Row{
		Data:      data,
		Timestamp: timestamp,
	}

	// Extract session key (supports multiple group by keys)
	key := extractSessionCompositeKey(data, sw.config.GroupByKeys)

	// Get or create session
	s, exists := sw.sessionMap[key]
	if !exists {
		// Create new session
		// Use the actual timestamp of the first data point as session start
		// No alignment needed - session starts from when first data arrives
		start := timestamp
		end := start.Add(sw.timeout)
		slot := types.NewTimeSlot(&start, &end)

		s = &session{
			data:       []types.Row{},
			lastActive: timestamp,
			slot:       slot,
		}
		sw.sessionMap[key] = s
	} else {
		// Update session end time
		if timestamp.After(s.lastActive) {
			s.lastActive = timestamp
			// Extend session end time
			newEnd := timestamp.Add(sw.timeout)
			if newEnd.After(*s.slot.End) {
				s.slot.End = &newEnd
			}
		}
	}

	// Add data to session
	row.Slot = s.slot
	s.data = append(s.data, row)
}

// Start starts the session window's periodic check mechanism
// Start starts the session window, begins periodic checking of expired sessions
// Uses lazy initialization mode to avoid infinite waiting when no data, while ensuring subsequent data can be processed normally
func (sw *SessionWindow) Start() {
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
func (sw *SessionWindow) startProcessingTime() {
	go func() {
		// Close output channel when function ends
		defer close(sw.outputChan)

		// Wait for initialization completion or context cancellation
		select {
		case <-sw.initChan:
			// Normal initialization completed, continue processing
		case <-sw.ctx.Done():
			// Context cancelled, exit directly
			return
		}

		// Periodically check expired sessions
		sw.tickerMu.Lock()
		sw.ticker = time.NewTicker(sw.timeout / 2)
		ticker := sw.ticker
		sw.tickerMu.Unlock()

		defer func() {
			sw.tickerMu.Lock()
			if sw.ticker != nil {
				sw.ticker.Stop()
			}
			sw.tickerMu.Unlock()
		}()

		for {
			select {
			case <-ticker.C:
				sw.checkExpiredSessions()
			case <-sw.ctx.Done():
				return
			}
		}
	}()
}

// startEventTime starts the event time trigger mechanism based on watermark
func (sw *SessionWindow) startEventTime() {
	go func() {
		// Close output channel when function ends
		defer close(sw.outputChan)
		if sw.watermark != nil {
			defer sw.watermark.Stop()
		}

		// Wait for initialization completion or context cancellation
		select {
		case <-sw.initChan:
			// Normal initialization completed, continue processing
		case <-sw.ctx.Done():
			// Context cancelled, exit directly
			return
		}

		// Process watermark updates
		if sw.watermark != nil {
			for {
				select {
				case watermarkTime := <-sw.watermark.WatermarkChan():
					sw.checkAndTriggerSessions(watermarkTime)
				case <-sw.ctx.Done():
					return
				}
			}
		} else {
			// If watermark is nil, just wait for context cancellation
			<-sw.ctx.Done()
			return
		}
	}()
}

// Stop stops session window operations
func (sw *SessionWindow) Stop() {
	// Call cancel function to stop window operations
	sw.cancelFunc()

	// Safely stop ticker
	sw.tickerMu.Lock()
	if sw.ticker != nil {
		sw.ticker.Stop()
	}
	sw.tickerMu.Unlock()

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

func (sw *SessionWindow) checkExpiredSessions() {
	sw.mu.Lock()
	now := time.Now()
	resultsToSend := sw.collectExpiredSessions(now)
	sw.mu.Unlock()

	sw.sendResults(resultsToSend)
}

func (sw *SessionWindow) checkAndTriggerSessions(watermarkTime time.Time) {
	sw.mu.Lock()
	resultsToSend := sw.collectExpiredSessions(watermarkTime)
	sw.closeExpiredSessions(watermarkTime)
	sw.mu.Unlock()

	sw.sendResults(resultsToSend)
}

func (sw *SessionWindow) collectExpiredSessions(currentTime time.Time) [][]types.Row {
	expiredKeys := []string{}
	for key, s := range sw.sessionMap {
		// For event time, use slot.End to determine if session expired
		// Session expires when watermark >= session end time
		// For processing time, use lastActive + timeout
		if s.slot.End != nil && !currentTime.Before(*s.slot.End) {
			expiredKeys = append(expiredKeys, key)
		} else if currentTime.Sub(s.lastActive) > sw.timeout {
			expiredKeys = append(expiredKeys, key)
		}
	}

	resultsToSend := make([][]types.Row, 0)
	allowedLateness := sw.config.AllowedLateness

	for _, key := range expiredKeys {
		s := sw.sessionMap[key]
		if len(s.data) > 0 {
			result := make([]types.Row, len(s.data))
			copy(result, s.data)
			resultsToSend = append(resultsToSend, result)

			if allowedLateness > 0 {
				closeTime := s.slot.End.Add(allowedLateness)
				sw.triggeredSessions[key] = &sessionInfo{
					session:   s,
					closeTime: closeTime,
				}
			}
		}
		delete(sw.sessionMap, key)
	}

	return resultsToSend
}

func (sw *SessionWindow) sendResults(resultsToSend [][]types.Row) {
	for _, result := range resultsToSend {
		// Skip empty results to avoid filling up channels
		if len(result) == 0 {
			continue
		}

		if sw.callback != nil {
			sw.callback(result)
		}

		select {
		case sw.outputChan <- result:
		default:
		}
	}
}

// Trigger manually triggers all session windows
func (sw *SessionWindow) Trigger() {
	sw.mu.Lock()

	// Collect all results first
	resultsToSend := make([][]types.Row, 0)
	for _, s := range sw.sessionMap {
		if len(s.data) > 0 {
			// Trigger session window
			result := make([]types.Row, len(s.data))
			copy(result, s.data)
			resultsToSend = append(resultsToSend, result)
		}
	}
	// Clear all sessions
	sw.sessionMap = make(map[string]*session)

	// Release lock before sending to channel and calling callback to avoid blocking
	sw.mu.Unlock()

	// Send results and call callbacks outside of lock to avoid blocking
	for _, result := range resultsToSend {
		// Skip empty results to avoid filling up channels
		if len(result) == 0 {
			continue
		}

		// If callback function is set, execute it
		if sw.callback != nil {
			sw.callback(result)
		}

		// Non-blocking send to output channel
		select {
		case sw.outputChan <- result:
			// Successfully sent
		default:
			// Channel full, drop result (could add statistics here if needed)
		}
	}
}

// Reset resets session window data
func (sw *SessionWindow) Reset() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Stop existing ticker
	sw.tickerMu.Lock()
	if sw.ticker != nil {
		sw.ticker.Stop()
		sw.ticker = nil
	}
	sw.tickerMu.Unlock()

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

	// Clear session data
	sw.sessionMap = make(map[string]*session)
	sw.triggeredSessions = make(map[string]*sessionInfo)
	sw.initialized = false
	sw.initChan = make(chan struct{})
}

// OutputChan returns a read-only channel for receiving data when window triggers
func (sw *SessionWindow) OutputChan() <-chan []types.Row {
	return sw.outputChan
}

// SetCallback sets the callback function when session window triggers
func (sw *SessionWindow) SetCallback(callback func([]types.Row)) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.callback = callback
}

// handleLateData handles late data that arrives within allowedLateness
func (sw *SessionWindow) handleLateData(eventTime time.Time, allowedLateness time.Duration) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Find which triggered session this late data belongs to
	for _, info := range sw.triggeredSessions {
		if info.session.slot.Contains(eventTime) {
			// This late data belongs to a triggered session that's still open
			// Trigger session again with updated data (late update)
			sw.triggerLateUpdateLocked(info.session)
			return
		}
	}
}

// triggerLateUpdateLocked triggers a late update for a session (must be called with lock held)
func (sw *SessionWindow) triggerLateUpdateLocked(s *session) {
	if len(s.data) == 0 {
		return
	}

	// Extract session data including late data
	resultData := make([]types.Row, len(s.data))
	copy(resultData, s.data)

	// Get callback reference before releasing lock
	callback := sw.callback

	// Release lock before calling callback and sending to channel to avoid blocking
	sw.mu.Unlock()

	if callback != nil {
		callback(resultData)
	}

	// Non-blocking send to output channel
	select {
	case sw.outputChan <- resultData:
		// Successfully sent
	default:
		// Channel full, drop result
	}

	// Re-acquire lock
	sw.mu.Lock()
}

// closeExpiredSessions closes sessions that have exceeded allowedLateness
func (sw *SessionWindow) closeExpiredSessions(watermarkTime time.Time) {
	for key, info := range sw.triggeredSessions {
		if !watermarkTime.Before(info.closeTime) {
			// Session has expired, remove it
			delete(sw.triggeredSessions, key)
		}
	}
}

// extractSessionCompositeKey builds composite session key from multiple group fields
// If GroupByKeys is empty, returns default key
func extractSessionCompositeKey(data interface{}, keys []string) string {
	if len(keys) == 0 {
		return "default"
	}
	parts := make([]string, 0, len(keys))
	if m, ok := data.(map[string]interface{}); ok {
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%v", m[k]))
		}
		return strings.Join(parts, "|")
	}
	return "default"
}
