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
	"reflect"
	"strings"
	"sync"

	"github.com/rulego/streamsql/utils/cast"

	"github.com/rulego/streamsql/types"
)

var _ Window = (*CountingWindow)(nil)

type CountingWindow struct {
	config       types.WindowConfig
	threshold    int
	count        int
	mu           sync.Mutex
	callback     func([]types.Row)
	dataBuffer   []types.Row
	outputChan   chan []types.Row
	ctx          context.Context
	cancelFunc   context.CancelFunc
	triggerChan  chan types.Row
	keyedBuffer  map[string][]types.Row
	keyedCount   map[string]int
	sentCount    int64
	droppedCount int64
	stopped      bool
}

func NewCountingWindow(config types.WindowConfig) (*CountingWindow, error) {
	// Counting window does not support event time
	// It triggers based on count, not time
	if config.TimeCharacteristic == types.EventTime {
		return nil, fmt.Errorf("counting window does not support event time, use processing time instead")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if cancel != nil {
			// cancel will be used in the returned struct
		}
	}()

	// Get count parameter from params array
	if len(config.Params) == 0 {
		cancel()
		return nil, fmt.Errorf("counting window requires 'count' parameter")
	}

	countVal := config.Params[0]
	threshold := cast.ToInt(countVal)
	if threshold <= 0 {
		return nil, fmt.Errorf("threshold must be a positive integer, got: %v", countVal)
	}

	// Use unified performance config to get window output buffer size
	bufferSize := 100 // Default value, counting windows usually have smaller buffers
	if (config.PerformanceConfig != types.PerformanceConfig{}) {
		bufferSize = config.PerformanceConfig.BufferConfig.WindowOutputSize / 10 // Counting window uses 1/10 of buffer
		if bufferSize < 10 {
			bufferSize = 10 // Minimum value
		}
	}

	cw := &CountingWindow{
		config:      config,
		threshold:   threshold,
		dataBuffer:  make([]types.Row, 0, threshold),
		outputChan:  make(chan []types.Row, bufferSize),
		ctx:         ctx,
		cancelFunc:  cancel,
		triggerChan: make(chan types.Row, bufferSize),
		keyedBuffer: make(map[string][]types.Row),
		keyedCount:  make(map[string]int),
	}

	// Set callback if provided
	if config.Callback != nil {
		cw.SetCallback(config.Callback)
	}
	return cw, nil
}

func (cw *CountingWindow) Add(data interface{}) {
	// Check if window is stopped before adding data
	cw.mu.Lock()
	stopped := cw.stopped
	cw.mu.Unlock()

	if stopped {
		// Window is stopped, ignore the data
		return
	}

	t := GetTimestamp(data, cw.config.TsProp, cw.config.TimeUnit)
	row := types.Row{
		Data:      data,
		Timestamp: t,
	}

	select {
	case cw.triggerChan <- row:
	case <-cw.ctx.Done():
	}
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
				key := cw.getKey(row.Data)
				cw.mu.Lock()
				buf := append(cw.keyedBuffer[key], row)
				cw.keyedBuffer[key] = buf
				cw.keyedCount[key] = len(buf)
				if cw.keyedCount[key] >= cw.threshold {
					slot := cw.createSlot(buf[:cw.threshold])
					data := make([]types.Row, cw.threshold)
					copy(data, buf[:cw.threshold])
					for i := range data {
						data[i].Slot = slot
					}
					if len(buf) > cw.threshold {
						rem := make([]types.Row, len(buf)-cw.threshold, cw.threshold)
						copy(rem, buf[cw.threshold:])
						cw.keyedBuffer[key] = rem
					} else {
						cw.keyedBuffer[key] = make([]types.Row, 0, cw.threshold)
					}
					cw.keyedCount[key] = len(cw.keyedBuffer[key])
					cw.mu.Unlock()

					if cw.callback != nil {
						cw.callback(data)
					}

					select {
					case cw.outputChan <- data:
						cw.mu.Lock()
						cw.sentCount++
						cw.mu.Unlock()
					case <-cw.ctx.Done():
						return
					default:
						cw.mu.Lock()
						cw.droppedCount++
						cw.mu.Unlock()
					}
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

func (cw *CountingWindow) Stop() {
	cw.mu.Lock()
	stopped := cw.stopped
	if !stopped {
		cw.stopped = true
	}
	cw.mu.Unlock()

	if !stopped {
		close(cw.triggerChan)
		cw.cancelFunc()
	}
}

func (cw *CountingWindow) Reset() {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	cw.count = 0
	cw.dataBuffer = nil
	cw.keyedBuffer = make(map[string][]types.Row)
	cw.keyedCount = make(map[string]int)
	cw.sentCount = 0
	cw.droppedCount = 0
}

func (cw *CountingWindow) GetStats() map[string]int64 {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	return map[string]int64{
		"sentCount":    cw.sentCount,
		"droppedCount": cw.droppedCount,
		"bufferSize":   int64(cap(cw.outputChan)),
		"bufferUsed":   int64(len(cw.outputChan)),
	}
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
		// Use actual timestamps without alignment
		start := data[0].Timestamp
		end := data[len(data)-1].Timestamp
		slot := types.NewTimeSlot(&start, &end)
		return slot
	} else {
		// Use actual timestamps without alignment
		start := data[0].Timestamp
		end := data[cw.threshold-1].Timestamp
		slot := types.NewTimeSlot(&start, &end)
		return slot
	}
}

func (cw *CountingWindow) getKey(data interface{}) string {
	// Use GroupByKeys array
	keys := cw.config.GroupByKeys
	if len(keys) == 0 {
		return "__global__"
	}
	v := reflect.ValueOf(data)
	keyParts := make([]string, 0, len(keys))
	for _, k := range keys {
		var part string
		switch v.Kind() {
		case reflect.Map:
			if v.Type().Key().Kind() == reflect.String {
				mv := v.MapIndex(reflect.ValueOf(k))
				if mv.IsValid() {
					part = cast.ToString(mv.Interface())
				}
			}
		case reflect.Struct:
			f := v.FieldByName(k)
			if f.IsValid() {
				part = cast.ToString(f.Interface())
			}
		}
		keyParts = append(keyParts, part)
	}
	return strings.Join(keyParts, "|")
}
