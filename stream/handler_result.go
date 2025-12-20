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

// ResultHandler handles result output and sink function calls
type ResultHandler struct {
	stream *Stream
}

// NewResultHandler creates a new result handler
func NewResultHandler(stream *Stream) *ResultHandler {
	return &ResultHandler{stream: stream}
}

// startSinkWorkerPool starts sink worker pool with configurable worker count
func (s *Stream) startSinkWorkerPool(workerCount int) {
	// Use configured worker count
	if workerCount <= 0 {
		workerCount = 8 // Default value
	}

	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			for {
				select {
				case task := <-s.sinkWorkerPool:
					// Execute sink task
					func() {
						defer func() {
							// Enhanced error recovery to prevent single worker crash
							if r := recover(); r != nil {
								logger.Error("Sink worker %d panic recovered: %v", workerID, r)
							}
						}()
						task()
					}()
				case <-s.done:
					return
				}
			}
		}(i)
	}
}

// startResultConsumer starts automatic result consumer to prevent resultChan blocking
func (s *Stream) startResultConsumer() {
	for {
		select {
		case <-s.resultChan:
			// Auto-consume results to prevent channel blocking
			// This is a fallback mechanism to ensure system doesn't block even without external consumers
		case <-s.done:
			return
		}
	}
}

// sendResultNonBlocking sends results to resultChan in non-blocking way (intelligent backpressure control)
func (s *Stream) sendResultNonBlocking(results []map[string]interface{}) {
	select {
	case s.resultChan <- results:
		// Successfully sent to result channel
		atomic.AddInt64(&s.outputCount, 1)
	default:
		// Result channel is full, use intelligent backpressure control strategy
		s.handleResultChannelBackpressure(results)
	}
}

// handleResultChannelBackpressure handles result channel backpressure with log throttling
func (s *Stream) handleResultChannelBackpressure(results []map[string]interface{}) {
	chanLen := len(s.resultChan)
	chanCap := cap(s.resultChan)

	// Enter backpressure mode if channel usage exceeds 90%
	if float64(chanLen)/float64(chanCap) > 0.9 {
		// Try to clean some old data to make room for new data
		select {
		case <-s.resultChan:
			// Clean one old result, then try to add new result
			select {
			case s.resultChan <- results:
				atomic.AddInt64(&s.outputCount, 1)
			default:
				s.logDroppedDataWithThrottling()
				atomic.AddInt64(&s.droppedCount, 1)
			}
		default:
			s.logDroppedDataWithThrottling()
			atomic.AddInt64(&s.droppedCount, 1)
		}
	} else {
		s.logDroppedDataWithThrottling()
		atomic.AddInt64(&s.droppedCount, 1)
	}
}

// logDroppedDataWithThrottling logs dropped data with throttling to avoid spam
// Logs every 10 seconds or every 1000 drops, whichever comes first
func (s *Stream) logDroppedDataWithThrottling() {
	now := time.Now().Unix()
	lastLogTime := atomic.LoadInt64(&s.lastDropLogTime)
	dropCount := atomic.AddInt64(&s.dropLogCount, 1)

	// Log if 10 seconds have passed since last log OR if 1000 drops have occurred
	if now-lastLogTime >= 10 || dropCount >= 1000 {
		// Try to update the last log time atomically
		if atomic.CompareAndSwapInt64(&s.lastDropLogTime, lastLogTime, now) {
			// Reset drop count and log the summary
			atomic.StoreInt64(&s.dropLogCount, 0)
			totalDropped := atomic.LoadInt64(&s.droppedCount)
			logger.Warn("Result channel is full, dropped %d data items in last period (total dropped: %d)", dropCount, totalDropped+1)
		}
	}
}

// callSinksAsync asynchronously calls all sink functions
func (s *Stream) callSinksAsync(results []map[string]interface{}) {
	// Safely access sinks slice using read lock
	s.sinksMux.RLock()
	defer s.sinksMux.RUnlock()

	if len(s.sinks) == 0 && len(s.syncSinks) == 0 {
		return
	}

	// Directly iterate sinks slice to avoid copy overhead
	// Since submitSinkTask is async, won't hold lock for long time
	for _, sink := range s.sinks {
		s.submitSinkTask(sink, results)
	}

	// Execute synchronous sinks (blocking, sequential)
	for _, sink := range s.syncSinks {
		// Recover panic for each sync sink to prevent crashing the stream
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("Sync sink execution exception: %v", r)
				}
			}()
			sink(results)
		}()
	}
}

// submitSinkTask submits sink task
func (s *Stream) submitSinkTask(sink func([]map[string]interface{}), results []map[string]interface{}) {
	// Capture sink variable to avoid closure issues
	currentSink := sink

	// Submit task to worker pool
	task := func() {
		defer func() {
			// Recover panic to prevent single sink error from affecting entire system
			if r := recover(); r != nil {
				logger.Error("Sink execution exception: %v", r)
			}
		}()
		currentSink(results)
	}

	// Non-blocking task submission
	// Note: Since we use a worker pool, tasks may be executed out of order
	select {
	case s.sinkWorkerPool <- task:
		// Successfully submitted task
	default:
		// Worker pool is full, execute directly in current goroutine (degraded handling)
		// This also helps with backpressure
		task()
	}
}

// AddSink adds a sink function
// Parameters:
//   - sink: result processing function that receives []map[string]interface{} type result data
//
// Note: Sinks are executed asynchronously in a worker pool, so execution order is NOT guaranteed.
// If you need strict ordering, use GetResultsChan() instead.
func (s *Stream) AddSink(sink func([]map[string]interface{})) {
	s.sinksMux.Lock()
	defer s.sinksMux.Unlock()
	s.sinks = append(s.sinks, sink)
}

// AddSyncSink adds a synchronous sink function
// Parameters:
//   - sink: result processing function that receives []map[string]interface{} type result data
//
// Note: Sync sinks are executed sequentially in the result processing goroutine.
// They block subsequent processing, so they should be fast.
func (s *Stream) AddSyncSink(sink func([]map[string]interface{})) {
	s.sinksMux.Lock()
	defer s.sinksMux.Unlock()
	s.syncSinks = append(s.syncSinks, sink)
}

// GetResultsChan gets the result channel
func (s *Stream) GetResultsChan() <-chan []map[string]interface{} {
	return s.resultChan
}
