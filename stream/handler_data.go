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

	"github.com/rulego/streamsql/logger"
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
func (s *Stream) safeGetDataChan() chan map[string]interface{} {
	s.dataChanMux.RLock()
	defer s.dataChanMux.RUnlock()
	return s.dataChan
}

// safeSendToDataChan safely sends data to dataChan
func (s *Stream) safeSendToDataChan(data map[string]interface{}) bool {
	// Check if stream is stopped before attempting to send
	if atomic.LoadInt32(&s.stopped) == 1 {
		return false
	}

	dataChan := s.safeGetDataChan()
	if dataChan == nil {
		return false
	}

	select {
	case dataChan <- data:
		return true
	default:
		return false
	}
}

// expandDataChannel dynamically expands data channel capacity
func (s *Stream) expandDataChannel() {
	// Use atomic operation to check if expansion is in progress, prevent concurrent expansion
	if !atomic.CompareAndSwapInt32(&s.expanding, 0, 1) {
		logger.Debug("Channel expansion already in progress, skipping")
		return
	}
	defer atomic.StoreInt32(&s.expanding, 0)

	// Acquire expansion lock to ensure only one goroutine performs expansion
	s.expansionMux.Lock()
	defer s.expansionMux.Unlock()

	// Double-check if expansion is needed (double-checked locking pattern)
	s.dataChanMux.RLock()
	oldCap := cap(s.dataChan)
	currentLen := len(s.dataChan)
	s.dataChanMux.RUnlock()

	// No expansion needed if current channel usage is below 80%
	if float64(currentLen)/float64(oldCap) < 0.8 {
		logger.Debug("Channel usage below threshold, expansion not needed")
		return
	}

	newCap := int(float64(oldCap) * 1.5) // Expand by 50%
	if newCap < oldCap+1000 {
		newCap = oldCap + 1000 // At least increase by 1000
	}

	logger.Debug("Dynamic expansion of data channel: %d -> %d", oldCap, newCap)

	// Create new larger channel
	newChan := make(chan map[string]interface{}, newCap)

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
				logger.Warn("Data migration timeout, some data may be lost during expansion")
				goto migration_done
			}
		case <-migrationTimeout.C:
			logger.Warn("Data migration timeout during channel drain")
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

	logger.Debug("Channel expansion completed: migrated %d items", migratedCount)
}
