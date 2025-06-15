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

	// 使用统一的性能配置获取窗口输出缓冲区大小
	bufferSize := 100 // 默认值，计数窗口通常缓冲较小
	if perfConfig, exists := config.Params["performanceConfig"]; exists {
		if pc, ok := perfConfig.(types.PerformanceConfig); ok {
			bufferSize = pc.BufferConfig.WindowOutputSize / 10 // 计数窗口使用1/10的缓冲区
			if bufferSize < 10 {
				bufferSize = 10 // 最小值
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
				if shouldTrigger {
					// 在持有锁的情况下立即处理
					slot := cw.createSlot(cw.dataBuffer[:cw.threshold])
					data := make([]types.Row, cw.threshold)
					copy(data, cw.dataBuffer[:cw.threshold])
					// 设置Slot字段到复制的数据中，避免修改原始dataBuffer
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
					// 重置计数
					cw.count = len(cw.dataBuffer)
					cw.mu.Unlock()

					// 在释放锁后处理回调
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
	// 注意：触发逻辑已合并到Start方法中以避免数据竞争
	// 这个方法保留是为了满足Window接口要求，但实际触发在Start方法中处理
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

// createSlot 创建一个新的时间槽位
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
