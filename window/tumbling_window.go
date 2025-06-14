// Package window 提供了窗口操作的实现，包括滚动窗口（Tumbling Window）。
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

// 确保 TumblingWindow 结构体实现了 Window 接口。
var _ Window = (*TumblingWindow)(nil)

// TumblingWindow 表示一个滚动窗口，用于在固定时间间隔内收集数据并触发处理。
type TumblingWindow struct {
	// config 是窗口的配置信息。
	config types.WindowConfig
	// size 是滚动窗口的时间大小，即窗口的持续时间。
	size time.Duration
	// mu 用于保护对窗口数据的并发访问。
	mu sync.Mutex
	// data 存储窗口内收集的数据。
	data []types.Row
	// outputChan 是一个通道，用于在窗口触发时发送数据。
	outputChan chan []types.Row
	// callback 是一个可选的回调函数，在窗口触发时调用。
	callback func([]types.Row)
	// ctx 用于控制窗口的生命周期。
	ctx context.Context
	// cancelFunc 用于取消窗口的操作。
	cancelFunc context.CancelFunc
	// timer 用于定时触发窗口。
	timer       *time.Ticker
	currentSlot *types.TimeSlot
	// 用于初始化窗口的通道
	initChan    chan struct{}
	initialized bool
}

// NewTumblingWindow 创建一个新的滚动窗口实例。
// 参数 size 是窗口的时间大小。
func NewTumblingWindow(config types.WindowConfig) (*TumblingWindow, error) {
	// 创建一个可取消的上下文。
	ctx, cancel := context.WithCancel(context.Background())
	size, err := cast.ToDurationE(config.Params["size"])
	if err != nil {
		return nil, fmt.Errorf("invalid size for tumbling window: %v", err)
	}
	return &TumblingWindow{
		config:      config,
		size:        size,
		outputChan:  make(chan []types.Row, 10),
		ctx:         ctx,
		cancelFunc:  cancel,
		initChan:    make(chan struct{}),
		initialized: false,
	}, nil
}

// Add 向滚动窗口添加数据。
// 参数 data 是要添加的数据。
func (tw *TumblingWindow) Add(data interface{}) {
	// 加锁以确保并发安全。
	tw.mu.Lock()
	defer tw.mu.Unlock()
	// 将数据追加到窗口的数据列表中。
	if !tw.initialized {
		tw.currentSlot = tw.createSlot(GetTimestamp(data, tw.config.TsProp, tw.config.TimeUnit))
		tw.timer = time.NewTicker(tw.size)
		// 发送初始化完成信号
		close(tw.initChan)
		tw.initialized = true
	}
	row := types.Row{
		Data:      data,
		Timestamp: GetTimestamp(data, tw.config.TsProp, tw.config.TimeUnit),
	}
	tw.data = append(tw.data, row)
}

func (sw *TumblingWindow) createSlot(t time.Time) *types.TimeSlot {
	// 创建一个新的时间槽位
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

// Stop 停止滚动窗口的操作。
func (tw *TumblingWindow) Stop() {
	// 调用取消函数以停止窗口的操作。
	tw.cancelFunc()
}

// Start 启动滚动窗口的定时触发机制。
func (tw *TumblingWindow) Start() {
	go func() {
		<-tw.initChan
		// 在函数结束时关闭输出通道。
		defer close(tw.outputChan)
		for {
			select {
			// 当定时器到期时，触发窗口。
			case <-tw.timer.C:
				// 在调用Trigger前不需要额外加锁，因为Trigger方法内部已经有锁保护
				tw.Trigger()
			// 当上下文被取消时，停止定时器并退出循环。
			case <-tw.ctx.Done():
				tw.timer.Stop()
				return
			}
		}
	}()
}

// Trigger 触发滚动窗口的处理逻辑。
func (tw *TumblingWindow) Trigger() {
	// 加锁以确保并发安全。
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if !tw.initialized {
		return
	}
	// 计算下一个窗口槽位
	next := tw.NextSlot()
	// 保留下一个窗口的数据
	tms := next.Start.Add(-tw.size)
	tme := next.End.Add(tw.size)
	temp := types.NewTimeSlot(&tms, &tme)
	newData := make([]types.Row, 0)
	for _, item := range tw.data {
		if temp.Contains(item.Timestamp) {
			newData = append(newData, item)
		}
	}

	// 提取出当前窗口数据
	resultData := make([]types.Row, 0)
	for _, item := range tw.data {
		if tw.currentSlot.Contains(item.Timestamp) {
			item.Slot = tw.currentSlot
			resultData = append(resultData, item)
		}
	}

	// 如果设置了回调函数，则执行回调函数
	if tw.callback != nil {
		tw.callback(resultData)
	}

	// 更新窗口内的数据
	tw.data = newData
	tw.currentSlot = next
	// 将新的数据发送到输出通道
	tw.outputChan <- resultData
}

// Reset 重置滚动窗口的数据。
func (tw *TumblingWindow) Reset() {
	// 加锁以确保并发安全。
	tw.mu.Lock()
	defer tw.mu.Unlock()
	// 清空窗口数据。
	tw.data = nil
	tw.currentSlot = nil
	tw.initialized = false
	tw.initChan = make(chan struct{})
}

// OutputChan 返回一个只读通道，用于接收窗口触发时的数据。
func (tw *TumblingWindow) OutputChan() <-chan []types.Row {
	return tw.outputChan
}

// SetCallback 设置滚动窗口触发时的回调函数。
// 参数 callback 是要设置的回调函数。
func (tw *TumblingWindow) SetCallback(callback func([]types.Row)) {
	tw.callback = callback
}

// // GetResults 获取当前滚动窗口中的数据副本。
// func (tw *TumblingWindow) GetResults() []interface{} {
// 	// 加锁以确保并发安全。
// 	tw.mu.Lock()
// 	defer tw.mu.Unlock()
// 	// 返回窗口数据的副本。
// 	return append([]interface{}{}, tw.data...)
// }
