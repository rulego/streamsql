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
	"sync/atomic"
	"time"
)

// DataHandler handles data processing for different strategies
type DataHandler struct {
	stream *Stream
}

// NewDataHandler creates a new data handler
func NewDataHandler(stream *Stream) *DataHandler {
	return &DataHandler{stream: stream}
}

// safeGetDataChan safely gets dataChan reference
func (s *Stream) safeGetDataChan() chan map[string]any {
	s.dataChanMux.RLock()
	defer s.dataChanMux.RUnlock()
	return s.dataChan
}

// safeSendToDataChan attempts a non-blocking send. The send runs under the
// data-channel read lock so a concurrent expandDataChannel cannot swap the
// channel out from under the send and strand the data on an orphaned channel.
func (s *Stream) safeSendToDataChan(data map[string]any) bool {
	if atomic.LoadInt32(&s.stopped) == 1 {
		return false
	}
	s.dataChanMux.RLock()
	defer s.dataChanMux.RUnlock()
	if s.dataChan == nil {
		return false
	}
	select {
	case s.dataChan <- data:
		return true
	default:
		return false
	}
}

// expandDataChannel dynamically expands data channel capacity
func (s *Stream) expandDataChannel() {
	// Use atomic operation to check if expansion is in progress, prevent concurrent expansion
	if !atomic.CompareAndSwapInt32(&s.expanding, 0, 1) {
		s.log.Debug("Channel expansion already in progress, skipping")
		return
	}
	defer atomic.StoreInt32(&s.expanding, 0)

	// Acquire expansion lock to ensure only one goroutine performs expansion
	s.expansionMux.Lock()
	defer s.expansionMux.Unlock()

	// Double-check if expansion is needed (double-checked locking pattern).
	// Growth factor, increment, trigger threshold and the hard ceiling all come
	// from PerformanceConfig.BufferConfig — the expansion knobs are not dead.
	buf := s.config.PerformanceConfig.BufferConfig
	exp := s.config.PerformanceConfig.OverflowConfig.ExpansionConfig
	s.dataChanMux.RLock()
	oldCap := cap(s.dataChan)
	currentLen := len(s.dataChan)
	s.dataChanMux.RUnlock()

	if oldCap <= 0 {
		return
	}
	// Already at the configured ceiling: do not grow further. The expand
	// strategy drops when the channel stays full past the cap.
	if buf.MaxBufferSize > 0 && oldCap >= buf.MaxBufferSize {
		s.log.Debug("Data channel at max buffer size %d, not expanding", buf.MaxBufferSize)
		return
	}

	// No expansion needed if current channel usage is below the trigger threshold.
	threshold := exp.TriggerThreshold
	if threshold <= 0 {
		threshold = 0.8
	}
	if float64(currentLen)/float64(oldCap) < threshold {
		s.log.Debug("Channel usage below threshold, expansion not needed")
		return
	}

	growth := exp.GrowthFactor
	if growth <= 1 {
		growth = 1.5
	}
	minInc := exp.MinIncrement
	if minInc <= 0 {
		minInc = 1000
	}
	newCap := int(float64(oldCap) * growth)
	if newCap < oldCap+minInc {
		newCap = oldCap + minInc
	}
	// Honor the configured ceiling.
	if buf.MaxBufferSize > 0 && newCap > buf.MaxBufferSize {
		newCap = buf.MaxBufferSize
	}
	if newCap <= oldCap {
		// Ceiling prevents any further growth.
		return
	}

	s.log.Debug("Dynamic expansion of data channel: %d -> %d", oldCap, newCap)

	// Create new larger channel
	newChan := make(chan map[string]any, newCap)

	// Safely migrate data using write lock
	s.dataChanMux.Lock()
	oldChan := s.dataChan

	// Quickly migrate data from old channel to new channel
	migrationTimeout := time.NewTimer(5 * time.Second) // 5 second migration timeout
	defer migrationTimeout.Stop()

	migratedCount := 0
	for {
		select {
		case data := <-oldChan:
			select {
			case newChan <- data:
				migratedCount++
			case <-migrationTimeout.C:
				s.log.Warn("Data migration timeout, some data may be lost during expansion")
				goto migration_done
			}
		case <-migrationTimeout.C:
			s.log.Warn("Data migration timeout during channel drain")
			goto migration_done
		default:
			// Old channel is empty, migration completed
			goto migration_done
		}
	}

migration_done:
	// Atomically update channel reference
	s.dataChan = newChan
	s.dataChanMux.Unlock()

	s.log.Debug("Channel expansion completed: migrated %d items", migratedCount)
}
