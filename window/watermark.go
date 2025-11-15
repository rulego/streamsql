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
	"sync"
	"time"
)

// Watermark represents a watermark for event time processing
// Watermark indicates that no events with timestamp less than watermark time are expected
type Watermark struct {
	// currentWatermark is the current watermark time
	currentWatermark time.Time
	// maxEventTime is the maximum event time seen so far
	maxEventTime time.Time
	// maxOutOfOrderness is the maximum allowed out-of-orderness
	maxOutOfOrderness time.Duration
	// idleTimeout is the idle source timeout: when no data arrives within this duration,
	// watermark advances based on processing time (0 means disabled)
	idleTimeout time.Duration
	// lastEventTime is the time when the last event was received
	lastEventTime time.Time
	// mu protects concurrent access
	mu sync.RWMutex
	// watermarkChan is a channel for watermark updates
	watermarkChan chan time.Time
	// ctx controls watermark lifecycle
	ctx context.Context
	// cancelFunc cancels watermark operations
	cancelFunc context.CancelFunc
}

// NewWatermark creates a new watermark manager
func NewWatermark(maxOutOfOrderness time.Duration, updateInterval time.Duration, idleTimeout time.Duration) *Watermark {
	ctx, cancel := context.WithCancel(context.Background())

	wm := &Watermark{
		currentWatermark:  time.Time{},
		maxEventTime:      time.Time{},
		maxOutOfOrderness: maxOutOfOrderness,
		idleTimeout:       idleTimeout,
		lastEventTime:     time.Time{},
		watermarkChan:     make(chan time.Time, 100),
		ctx:               ctx,
		cancelFunc:        cancel,
	}

	// Start periodic watermark updates
	go wm.updateLoop(updateInterval)

	return wm
}

// updateLoop periodically updates watermark based on max event time
func (wm *Watermark) updateLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wm.update()
		case <-wm.ctx.Done():
			return
		}
	}
}

// update updates watermark based on current max event time
// If idle timeout is configured and data source is idle, watermark advances based on processing time
func (wm *Watermark) update() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if !wm.maxEventTime.IsZero() {
		now := time.Now()
		var newWatermark time.Time

		// Check if data source is idle
		if wm.idleTimeout > 0 && !wm.lastEventTime.IsZero() {
			timeSinceLastEvent := now.Sub(wm.lastEventTime)
			if timeSinceLastEvent > wm.idleTimeout {
				// Data source is idle, advance watermark based on processing time
				// Watermark = current processing time - max out of orderness
				// This ensures windows can close even when no new data arrives
				newWatermark = now.Add(-wm.maxOutOfOrderness)
			} else {
				// Normal update: based on max event time
				newWatermark = wm.maxEventTime.Add(-wm.maxOutOfOrderness)
			}
		} else {
			// Normal update: based on max event time
			newWatermark = wm.maxEventTime.Add(-wm.maxOutOfOrderness)
		}

		if newWatermark.After(wm.currentWatermark) {
			wm.currentWatermark = newWatermark
			// Send watermark update (non-blocking)
			select {
			case wm.watermarkChan <- wm.currentWatermark:
			default:
				// Channel full, skip
			}
		}
	}
}

// UpdateEventTime updates the maximum event time seen
func (wm *Watermark) UpdateEventTime(eventTime time.Time) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Update last event time for idle detection
	wm.lastEventTime = time.Now()

	if wm.maxEventTime.IsZero() || eventTime.After(wm.maxEventTime) {
		wm.maxEventTime = eventTime
		// Immediately update watermark if event time is significantly ahead
		newWatermark := eventTime.Add(-wm.maxOutOfOrderness)
		if newWatermark.After(wm.currentWatermark) {
			wm.currentWatermark = newWatermark
			// Send watermark update (non-blocking)
			select {
			case wm.watermarkChan <- wm.currentWatermark:
			default:
				// Channel full, skip
			}
		}
	}
}

// GetCurrentWatermark returns the current watermark time
func (wm *Watermark) GetCurrentWatermark() time.Time {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return wm.currentWatermark
}

// WatermarkChan returns a channel for receiving watermark updates
func (wm *Watermark) WatermarkChan() <-chan time.Time {
	return wm.watermarkChan
}

// Stop stops the watermark manager
func (wm *Watermark) Stop() {
	wm.cancelFunc()
}

// IsEventTimeLate checks if an event time is late (before current watermark)
func (wm *Watermark) IsEventTimeLate(eventTime time.Time) bool {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return !wm.currentWatermark.IsZero() && eventTime.Before(wm.currentWatermark)
}

// alignWindowStart aligns window start time to window boundaries
// For event time windows, windows are aligned to epoch (00:00:00 UTC)
//
// Alignment granularity: The alignment granularity equals the window size itself.
// For example:
//   - If window size is 2s, alignment granularity is 2s
//   - If window size is 1h, alignment granularity is 1h
//
// Alignment behavior:
//   - Windows are aligned downward to the nearest window boundary from epoch
//   - Formula: alignedTime = (timestamp / windowSize) * windowSize
//   - This ensures consistent window boundaries across different data sources
//
// Example:
//   - First data arrives at 10001ms, window size is 2000ms
//   - Aligned start = (10001000000 / 2000000000) * 2000000000 = 10000000000ns = 10000ms
//   - Window range: [10000ms, 12000ms)
//   - The data at 10001ms will be in this window
//
// Note: This alignment may cause the first window to start before the first data arrives,
// which is expected behavior for event time windows to ensure consistent boundaries.
func alignWindowStart(timestamp time.Time, windowSize time.Duration) time.Time {
	// Convert to Unix timestamp in nanoseconds
	unixNano := timestamp.UnixNano()
	windowSizeNano := windowSize.Nanoseconds()

	// Align to window boundary (downward alignment)
	// This creates consistent window boundaries aligned to epoch
	alignedNano := (unixNano / windowSizeNano) * windowSizeNano

	// Convert back to time.Time
	return time.Unix(0, alignedNano).UTC()
}
