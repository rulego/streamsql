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

	cw := &CountingWindow{
		threshold:   threshold,
		dataBuffer:  make([]types.Row, 0, threshold),
		outputChan:  make(chan []types.Row, 10),
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
	// 将数据添加到窗口的数据列表中
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
					// 通道已关闭，退出循环
					return
				}
				cw.mu.Lock()
				cw.dataBuffer = append(cw.dataBuffer, row)
				cw.count++
				shouldTrigger := cw.count >= cw.threshold
				cw.mu.Unlock()
				// 只有当达到阈值时才触发
				if shouldTrigger {
					cw.Trigger()
				}

			case <-cw.ctx.Done():
				return
			}
		}
	}()
}

func (cw *CountingWindow) Trigger() {
	//cw.triggerChan <- struct{}{}
	cw.mu.Lock()
	defer cw.mu.Unlock()

	slot := cw.createSlot(cw.dataBuffer[:cw.threshold])
	for _, r := range cw.dataBuffer[:cw.threshold] {
		// 由于Row是值类型，这里需要通过指针来修改Slot字段
		(&r).Slot = slot
	}
	data := cw.dataBuffer[:cw.threshold]
	if len(cw.dataBuffer) > cw.threshold {
		remaining := len(cw.dataBuffer) - cw.threshold
		newBuffer := make([]types.Row, remaining, cw.threshold)
		copy(newBuffer, cw.dataBuffer[cw.threshold:])
		cw.dataBuffer = newBuffer
	} else {
		cw.dataBuffer = make([]types.Row, 0, cw.threshold)
	}
	// 重置计数
	cw.count = len(cw.dataBuffer)
	go func(data []types.Row) {
		if cw.callback != nil {
			cw.callback(data)
		}
		cw.outputChan <- data
	}(data)
}

func (cw *CountingWindow) Reset() {
	cw.mu.Lock()
	cw.count = 0
	cw.mu.Unlock()
	cw.dataBuffer = nil
}

func (cw *CountingWindow) OutputChan() <-chan []types.Row {
	return cw.outputChan
}

// func (cw *CountingWindow) GetResults() []interface{} {
// 	return append([]mode.Row, cw.dataBuffer...)
// }

// createSlot 创建一个新的时间槽位
func (cw *CountingWindow) createSlot(data []types.Row) *types.TimeSlot {
	if len(data) == 0 {
		return nil
	} else if len(data) < cw.threshold {
		start := timex.AlignTime(data[0].Timestamp, cw.config.TimeUnit, true)
		end := timex.AlignTime(data[len(cw.dataBuffer)-1].Timestamp, cw.config.TimeUnit, false)
		slot := types.NewTimeSlot(&start, &end)
		return slot
	} else {
		start := timex.AlignTime(data[0].Timestamp, cw.config.TimeUnit, true)
		end := timex.AlignTime(data[cw.threshold-1].Timestamp, cw.config.TimeUnit, false)
		slot := types.NewTimeSlot(&start, &end)
		return slot
	}
}
