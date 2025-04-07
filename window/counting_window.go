package window

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rulego/streamsql/model"
	timex "github.com/rulego/streamsql/utils"
	"github.com/spf13/cast"
)

var _ Window = (*CountingWindow)(nil)

type CountingWindow struct {
	config      model.WindowConfig
	threshold   int
	count       int
	mu          sync.Mutex
	callback    func([]model.Row)
	dataBuffer  []model.Row
	outputChan  chan []model.Row
	ctx         context.Context
	cancelFunc  context.CancelFunc
	ticker      *time.Ticker
	triggerChan chan struct{}
}

func NewCountingWindow(config model.WindowConfig) (*CountingWindow, error) {
	ctx, cancel := context.WithCancel(context.Background())
	threshold := cast.ToInt(config.Params["count"])
	if threshold <= 0 {
		return nil, fmt.Errorf("threshold must be a positive integer")
	}

	cw := &CountingWindow{
		threshold:   threshold,
		dataBuffer:  make([]model.Row, 0, threshold),
		outputChan:  make(chan []model.Row, 10),
		ctx:         ctx,
		cancelFunc:  cancel,
		triggerChan: make(chan struct{}, 1),
	}

	if callback, ok := config.Params["callback"].(func([]model.Row)); ok {
		cw.SetCallback(callback)
	}
	return cw, nil
}

func (cw *CountingWindow) Add(data interface{}) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	// 将数据添加到窗口的数据列表中
	t := GetTimestamp(data, cw.config.TsProp)
	row := model.Row{
		Data:      data,
		Timestamp: t,
	}
	cw.dataBuffer = append(cw.dataBuffer, row)
	cw.count++
	shouldTrigger := cw.count == cw.threshold

	if shouldTrigger {
		slot := cw.createSlot(cw.dataBuffer[:cw.threshold])
		for _, r := range cw.dataBuffer[:cw.threshold] {
			// 由于Row是值类型，这里需要通过指针来修改Slot字段
			(&r).Slot = slot
		}
		data := cw.dataBuffer[:cw.threshold]
		if len(cw.dataBuffer) > cw.threshold {
			cw.dataBuffer = cw.dataBuffer[cw.threshold:]
		} else {
			cw.dataBuffer = make([]model.Row, 0, cw.threshold)
		}
		go func() {
			if cw.callback != nil {
				cw.callback(data)
			}
			cw.outputChan <- data
			cw.Reset()
		}()
	}
}
func (cw *CountingWindow) Start() {
	go func() {
		cw.ticker = time.NewTicker(1 * time.Second)
		defer func() {
			cw.ticker.Stop()
			cw.cancelFunc()
		}()

		for {
			select {
			case <-cw.ticker.C:
				cw.Trigger()
			case <-cw.ctx.Done():
				return
			}
		}
	}()
}

func (cw *CountingWindow) Trigger() {
	// cw.triggerChan <- struct{}{}

	// go func() {
	// 	cw.mu.Lock()
	// 	defer cw.mu.Unlock()

	// 	if cw.callback != nil && len(cw.dataBuffer) > 0 {
	// 		var resultData []model.Row
	// 		if len(cw.dataBuffer) > cw.threshold {
	// 			resultData = cw.dataBuffer[:cw.threshold]
	// 		} else {
	// 			resultData = cw.dataBuffer
	// 		}
	// 		slot := cw.createSlot(resultData)
	// 		for _, r := range resultData {
	// 			r.Slot = slot
	// 		}
	// 		cw.callback(resultData)
	// 	}
	// 	cw.Reset()
	// }()
}

func (cw *CountingWindow) Reset() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.count = 0
	cw.dataBuffer = cw.dataBuffer[0:]
}

func (cw *CountingWindow) OutputChan() <-chan []model.Row {
	return cw.outputChan
}

// func (cw *CountingWindow) GetResults() []interface{} {
// 	return append([]mode.Row, cw.dataBuffer...)
// }

// createSlot 创建一个新的时间槽位
func (cw *CountingWindow) createSlot(data []model.Row) *model.TimeSlot {
	if len(data) == 0 {
		return nil
	} else if len(data) < cw.threshold {
		start := timex.AlignTime(data[0].Timestamp, cw.config.TimeUnit, true)
		end := timex.AlignTime(data[len(cw.dataBuffer)-1].Timestamp, cw.config.TimeUnit, false)
		slot := model.NewTimeSlot(&start, &end)
		return slot
	} else {
		start := timex.AlignTime(data[0].Timestamp, cw.config.TimeUnit, true)
		end := timex.AlignTime(data[cw.threshold-1].Timestamp, cw.config.TimeUnit, false)
		slot := model.NewTimeSlot(&start, &end)
		return slot
	}
}
