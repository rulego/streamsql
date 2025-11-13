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
}

// session stores data and state for a session
type session struct {
	data       []types.Row
	lastActive time.Time
	slot       *types.TimeSlot
}

// NewSessionWindow creates a new session window instance
func NewSessionWindow(config types.WindowConfig) (*SessionWindow, error) {
	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Get timeout parameter from params array
	if len(config.Params) == 0 {
		return nil, fmt.Errorf("session window requires 'timeout' parameter")
	}

	timeoutVal := config.Params[0]
	timeout, err := cast.ToDurationE(timeoutVal)
	if err != nil {
		return nil, fmt.Errorf("invalid timeout for session window: %v", err)
	}

	// Use unified performance configuration to get window output buffer size
	bufferSize := 100 // Default value, session windows typically have smaller buffers
	if (config.PerformanceConfig != types.PerformanceConfig{}) {
		bufferSize = config.PerformanceConfig.BufferConfig.WindowOutputSize / 10 // Session window uses 1/10 of buffer
		if bufferSize < 10 {
			bufferSize = 10 // Minimum value
		}
	}

	return &SessionWindow{
		config:      config,
		timeout:     timeout,
		sessionMap:  make(map[string]*session),
		outputChan:  make(chan []types.Row, bufferSize),
		ctx:         ctx,
		cancelFunc:  cancel,
		initChan:    make(chan struct{}),
		initialized: false,
	}, nil
}

// Add adds data to session window
func (sw *SessionWindow) Add(data interface{}) {
	// Lock to ensure thread safety
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if !sw.initialized {
		close(sw.initChan)
		sw.initialized = true
	}

	// Get data timestamp
	timestamp := GetTimestamp(data, sw.config.TsProp, sw.config.TimeUnit)
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
}

// checkExpiredSessions checks and triggers expired sessions
func (sw *SessionWindow) checkExpiredSessions() {
	sw.mu.Lock()

	now := time.Now()
	expiredKeys := []string{}

	// Find expired sessions
	for key, s := range sw.sessionMap {
		if now.Sub(s.lastActive) > sw.timeout {
			expiredKeys = append(expiredKeys, key)
		}
	}

	// Process expired sessions
	resultsToSend := make([][]types.Row, 0)
	for _, key := range expiredKeys {
		s := sw.sessionMap[key]
		if len(s.data) > 0 {
			// Trigger session window
			result := make([]types.Row, len(s.data))
			copy(result, s.data)
			resultsToSend = append(resultsToSend, result)
		}
		// Delete expired session
		delete(sw.sessionMap, key)
	}

	// Release lock before sending to channel and calling callback to avoid blocking
	sw.mu.Unlock()

	// Send results and call callbacks outside of lock to avoid blocking
	for _, result := range resultsToSend {
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

	// Clear session data
	sw.sessionMap = make(map[string]*session)
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
