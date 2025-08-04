// Package window provides window operation implementations, including Tumbling Window
package window

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/rulego/streamsql/utils/timex"
)

// Ensure TumblingWindow implements the Window interface
var _ Window = (*TumblingWindow)(nil)

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
	// timer for triggering window periodically
	timer       *time.Ticker
	currentSlot *types.TimeSlot
	// initChan for window initialization
	initChan    chan struct{}
	initialized bool
	// timerMu protects timer access
	timerMu sync.Mutex
	// Performance statistics
	droppedCount int64 // Number of dropped results
	sentCount    int64 // Number of successfully sent results
}

// NewTumblingWindow creates a new tumbling window instance
// Parameter size is the time size of the window
func NewTumblingWindow(config types.WindowConfig) (*TumblingWindow, error) {
	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	size, err := cast.ToDurationE(config.Params["size"])
	if err != nil {
		return nil, fmt.Errorf("invalid size for tumbling window: %v", err)
	}

	// Use unified performance config to get window output buffer size
	bufferSize := 1000 // Default value
	if perfConfig, exists := config.Params["performanceConfig"]; exists {
		if pc, ok := perfConfig.(types.PerformanceConfig); ok {
			bufferSize = pc.BufferConfig.WindowOutputSize
		}
	}

	return &TumblingWindow{
		config:      config,
		size:        size,
		outputChan:  make(chan []types.Row, bufferSize),
		ctx:         ctx,
		cancelFunc:  cancel,
		initChan:    make(chan struct{}),
		initialized: false,
	}, nil
}

// Add adds data to the tumbling window
func (tw *TumblingWindow) Add(data interface{}) {
	// Lock to ensure thread safety
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// Append data to window's data list
	if !tw.initialized {
		tw.currentSlot = tw.createSlot(GetTimestamp(data, tw.config.TsProp, tw.config.TimeUnit))
		tw.timerMu.Lock()
		tw.timer = time.NewTicker(tw.size)
		tw.timerMu.Unlock()
		tw.initialized = true
		// Send initialization complete signal (after setting timer)
		close(tw.initChan)
	}
	row := types.Row{
		Data:      data,
		Timestamp: GetTimestamp(data, tw.config.TsProp, tw.config.TimeUnit),
	}
	tw.data = append(tw.data, row)
}

func (sw *TumblingWindow) createSlot(t time.Time) *types.TimeSlot {
	// Create a new time slot
	start := timex.AlignTimeToWindow(t, sw.size)
	end := start.Add(sw.size)
	slot := types.NewTimeSlot(&start, &end)
	return slot
}

func (sw *TumblingWindow) NextSlot() *types.TimeSlot {
	if sw.currentSlot == nil {
		return nil
	}
	start := sw.currentSlot.End
	end := sw.currentSlot.End.Add(sw.size)
	return types.NewTimeSlot(start, &end)
}

// Stop stops tumbling window operations
func (tw *TumblingWindow) Stop() {
	// Call cancel function to stop window operations
	tw.cancelFunc()

	// Safely stop timer
	tw.timerMu.Lock()
	if tw.timer != nil {
		tw.timer.Stop()
	}
	tw.timerMu.Unlock()
}

// Start starts the tumbling window's periodic trigger mechanism
// Uses lazy initialization to avoid infinite waiting when no data, ensuring subsequent data can be processed normally
func (tw *TumblingWindow) Start() {
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

// Trigger triggers the tumbling window's processing logic
func (tw *TumblingWindow) Trigger() {
	// Lock to ensure thread safety
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if !tw.initialized {
		return
	}
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

	// Execute callback function if set
	if tw.callback != nil {
		tw.callback(resultData)
	}

	// Update window data
	tw.data = newData
	tw.currentSlot = next

	// Non-blocking send to output channel and update statistics
	select {
	case tw.outputChan <- resultData:
		// Successfully sent, update statistics (within lock)
		tw.sentCount++
	default:
		// Channel full, drop result and update statistics (within lock)
		tw.droppedCount++

		// Optional: add logging here
	} // log.Printf("Window output channel full, dropped result with %d rows", len(resultData))
}

// Reset resets tumbling window data
func (tw *TumblingWindow) Reset() {
	// First cancel context to stop all running goroutines
	tw.cancelFunc()

	// Lock to ensure thread safety
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// Stop existing timer
	tw.timerMu.Lock()
	if tw.timer != nil {
		tw.timer.Stop()
		tw.timer = nil
	}
	tw.timerMu.Unlock()

	// Clear window data
	tw.data = nil
	tw.currentSlot = nil
	tw.initialized = false
	tw.initChan = make(chan struct{})

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
		"sent_count":    tw.sentCount,
		"dropped_count": tw.droppedCount,
		"buffer_size":   int64(cap(tw.outputChan)),
		"buffer_used":   int64(len(tw.outputChan)),
	}
}

// ResetStats resets performance statistics
func (tw *TumblingWindow) ResetStats() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.sentCount = 0
	tw.droppedCount = 0
}
