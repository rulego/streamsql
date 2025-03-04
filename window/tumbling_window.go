// Package window 提供了窗口操作的实现，包括滚动窗口（Tumbling Window）。
package window

import (
	"context"
	"sync"
	"time"
)

// 确保 TumblingWindow 结构体实现了 Window 接口。
var _ Window = (*TumblingWindow)(nil)

// TumblingWindow 表示一个滚动窗口，用于在固定时间间隔内收集数据并触发处理。
type TumblingWindow struct {
	// size 是滚动窗口的时间大小，即窗口的持续时间。
	size time.Duration
	// mu 用于保护对窗口数据的并发访问。
	mu sync.Mutex
	// data 存储窗口内收集的数据。
	data []interface{}
	// outputChan 是一个通道，用于在窗口触发时发送数据。
	outputChan chan []interface{}
	// callback 是一个可选的回调函数，在窗口触发时调用。
	callback func([]interface{})
	// ctx 用于控制窗口的生命周期。
	ctx context.Context
	// cancelFunc 用于取消窗口的操作。
	cancelFunc context.CancelFunc
	// timer 用于定时触发窗口。
	timer *time.Timer
}

// NewTumblingWindow 创建一个新的滚动窗口实例。
// 参数 size 是窗口的时间大小。
func NewTumblingWindow(size time.Duration) *TumblingWindow {
	// 创建一个可取消的上下文。
	ctx, cancel := context.WithCancel(context.Background())
	return &TumblingWindow{
		size:       size,
		outputChan: make(chan []interface{}, 10),
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

// Add 向滚动窗口添加数据。
// 参数 data 是要添加的数据。
func (tw *TumblingWindow) Add(data interface{}) {
	// 加锁以确保并发安全。
	tw.mu.Lock()
	defer tw.mu.Unlock()
	// 将数据追加到窗口的数据列表中。
	tw.data = append(tw.data, data)
}

// Stop 停止滚动窗口的操作。
func (tw *TumblingWindow) Stop() {
	// 调用取消函数以停止窗口的操作。
	tw.cancelFunc()
}

// Start 启动滚动窗口的定时触发机制。
func (tw *TumblingWindow) Start() {
	go func() {
		// 创建一个定时器，初始时间为窗口大小。
		tw.timer = time.NewTimer(tw.size)
		// 在函数结束时关闭输出通道。
		defer close(tw.outputChan)
		for {
			select {
			// 当定时器到期时，触发窗口。
			case <-tw.timer.C:
				tw.Trigger()
				// 重置定时器以开始下一个窗口周期。
				tw.timer.Reset(tw.size)
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

	// 如果设置了回调函数，则调用它。
	if tw.callback != nil {
		tw.callback(tw.data)
	}

	// 将窗口数据发送到输出通道。
	tw.outputChan <- append([]interface{}{}, tw.data...)
	// 重置窗口数据。
	tw.data = nil
}

// Reset 重置滚动窗口的数据。
func (tw *TumblingWindow) Reset() {
	// 加锁以确保并发安全。
	tw.mu.Lock()
	defer tw.mu.Unlock()
	// 清空窗口数据。
	tw.data = nil
}

// OutputChan 返回一个只读通道，用于接收窗口触发时的数据。
func (tw *TumblingWindow) OutputChan() <-chan []interface{} {
	return tw.outputChan
}

// SetCallback 设置滚动窗口触发时的回调函数。
// 参数 callback 是要设置的回调函数。
func (tw *TumblingWindow) SetCallback(callback func([]interface{})) {
	tw.callback = callback
}

// GetResults 获取当前滚动窗口中的数据副本。
func (tw *TumblingWindow) GetResults() []interface{} {
	// 加锁以确保并发安全。
	tw.mu.Lock()
	defer tw.mu.Unlock()
	// 返回窗口数据的副本。
	return append([]interface{}{}, tw.data...)
}
