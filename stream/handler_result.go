package stream

import (
	"sync/atomic"

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

// handleResultChannelBackpressure handles result channel backpressure
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
				logger.Warn("Result channel is full, dropping result data")
				atomic.AddInt64(&s.droppedCount, 1)
			}
		default:
			logger.Warn("Result channel is full, dropping result data")
			atomic.AddInt64(&s.droppedCount, 1)
		}
	} else {
		logger.Warn("Result channel is full, dropping result data")
		atomic.AddInt64(&s.droppedCount, 1)
	}
}

// callSinksAsync asynchronously calls all sink functions
func (s *Stream) callSinksAsync(results []map[string]interface{}) {
	// Safely access sinks slice using read lock
	s.sinksMux.RLock()
	defer s.sinksMux.RUnlock()

	if len(s.sinks) == 0 {
		return
	}

	// Directly iterate sinks slice to avoid copy overhead
	// Since submitSinkTask is async, won't hold lock for long time
	for _, sink := range s.sinks {
		s.submitSinkTask(sink, results)
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
	select {
	case s.sinkWorkerPool <- task:
		// Successfully submitted task
	default:
		// Worker pool is full, execute directly in current goroutine (degraded handling)
		go task()
	}
}

// AddSink adds a sink function
// Parameters:
//   - sink: result processing function that receives []map[string]interface{} type result data
func (s *Stream) AddSink(sink func([]map[string]interface{})) {
	s.sinksMux.Lock()
	defer s.sinksMux.Unlock()
	s.sinks = append(s.sinks, sink)
}

// GetResultsChan gets the result channel
func (s *Stream) GetResultsChan() <-chan []map[string]interface{} {
	return s.resultChan
}
