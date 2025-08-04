package window

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rulego/streamsql/utils/cast"
	"github.com/rulego/streamsql/utils/timex"

	"github.com/rulego/streamsql/types"
)

var _ Window = (*CountingWindow)(nil)

type CountingWindow struct {
	config      types.WindowConfig
	threshold   int
	count       int
	mu          sync.Mutex
	callback    func([]types.Row)
	dataBuffer  []types.Row
	outputChan  chan []types.Row
	ctx         context.Context
	cancelFunc  context.CancelFunc
	ticker      *time.Ticker
	triggerChan chan types.Row
}

func NewCountingWindow(config types.WindowConfig) (*CountingWindow, error) {
	ctx, cancel := context.WithCancel(context.Background())
	threshold := cast.ToInt(config.Params["count"])
	if threshold <= 0 {
		return nil, fmt.Errorf("threshold must be a positive integer")
	}

	// Use unified performance config to get window output buffer size
	bufferSize := 100 // Default value, counting windows usually have smaller buffers
	if perfConfig, exists := config.Params["performanceConfig"]; exists {
		if pc, ok := perfConfig.(types.PerformanceConfig); ok {
			bufferSize = pc.BufferConfig.WindowOutputSize / 10 // Counting window uses 1/10 of buffer
			if bufferSize < 10 {
				bufferSize = 10 // Minimum value
			}
		}
	}

	cw := &CountingWindow{
		threshold:   threshold,
		dataBuffer:  make([]types.Row, 0, threshold),
		outputChan:  make(chan []types.Row, bufferSize),
		ctx:         ctx,
		cancelFunc:  cancel,
		triggerChan: make(chan types.Row, 3),
	}

	if callback, ok := config.Params["callback"].(func([]types.Row)); ok {
		cw.SetCallback(callback)
	}
	return cw, nil
}

func (cw *CountingWindow) Add(data interface{}) {
	// Add data to window data list
	t := GetTimestamp(data, cw.config.TsProp, cw.config.TimeUnit)
	row := types.Row{
		Data:      data,
		Timestamp: t,
	}
	cw.triggerChan <- row
}
func (cw *CountingWindow) Start() {
	go func() {
		defer cw.cancelFunc()

		for {
			select {
			case row, ok := <-cw.triggerChan:
				if !ok {
					// Channel closed, exit loop
					return
				}
				cw.mu.Lock()
				cw.dataBuffer = append(cw.dataBuffer, row)
				cw.count++
				shouldTrigger := cw.count >= cw.threshold
				if shouldTrigger {
					// Process immediately while holding lock
					slot := cw.createSlot(cw.dataBuffer[:cw.threshold])
					data := make([]types.Row, cw.threshold)
					copy(data, cw.dataBuffer[:cw.threshold])
					// Set Slot field to copied data to avoid modifying original dataBuffer
					for i := range data {
						data[i].Slot = slot
					}

					if len(cw.dataBuffer) > cw.threshold {
						remaining := len(cw.dataBuffer) - cw.threshold
						newBuffer := make([]types.Row, remaining, cw.threshold)
						copy(newBuffer, cw.dataBuffer[cw.threshold:])
						cw.dataBuffer = newBuffer
					} else {
						cw.dataBuffer = make([]types.Row, 0, cw.threshold)
					}
					// Reset count
					cw.count = len(cw.dataBuffer)
					cw.mu.Unlock()

					// Handle callback after releasing lock
					go func(data []types.Row) {
						if cw.callback != nil {
							cw.callback(data)
						}
						cw.outputChan <- data
					}(data)
				} else {
					cw.mu.Unlock()
				}

			case <-cw.ctx.Done():
				return
			}
		}
	}()
}

func (cw *CountingWindow) Trigger() {
	// Note: trigger logic has been merged into Start method to avoid data races
	// This method is kept to satisfy Window interface requirements, but actual triggering is handled in Start method
}

func (cw *CountingWindow) Reset() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.count = 0
	cw.dataBuffer = nil
}

func (cw *CountingWindow) OutputChan() <-chan []types.Row {
	return cw.outputChan
}

// func (cw *CountingWindow) GetResults() []interface{} {
// 	return append([]mode.Row, cw.dataBuffer...)
// }

// createSlot creates a new time slot
func (cw *CountingWindow) createSlot(data []types.Row) *types.TimeSlot {
	if len(data) == 0 {
		return nil
	} else if len(data) < cw.threshold {
		start := timex.AlignTime(data[0].Timestamp, cw.config.TimeUnit, true)
		end := timex.AlignTime(data[len(data)-1].Timestamp, cw.config.TimeUnit, false)
		slot := types.NewTimeSlot(&start, &end)
		return slot
	} else {
		start := timex.AlignTime(data[0].Timestamp, cw.config.TimeUnit, true)
		end := timex.AlignTime(data[cw.threshold-1].Timestamp, cw.config.TimeUnit, false)
		slot := types.NewTimeSlot(&start, &end)
		return slot
	}
}
