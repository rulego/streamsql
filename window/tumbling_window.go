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
	mu sync.RWMutex
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
	// 保护timer的锁
	timerMu sync.Mutex
	// 性能统计
	droppedCount int64 // 丢弃的结果数量
	sentCount    int64 // 成功发送的结果数量
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

	// 使用统一的性能配置获取窗口输出缓冲区大小
	bufferSize := 1000 // 默认值
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

// Add 向滚动窗口添加数据。
// 参数 data 是要添加的数据。
func (tw *TumblingWindow) Add(data interface{}) {
	// 加锁以确保并发安全。
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// 将数据追加到窗口的数据列表中。
	if !tw.initialized {
		tw.currentSlot = tw.createSlot(GetTimestamp(data, tw.config.TsProp, tw.config.TimeUnit))
		tw.timerMu.Lock()
		tw.timer = time.NewTicker(tw.size)
		tw.timerMu.Unlock()
		tw.initialized = true
		// 发送初始化完成信号（在设置timer后）
		close(tw.initChan)
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

	// 安全地停止timer
	tw.timerMu.Lock()
	if tw.timer != nil {
		tw.timer.Stop()
	}
	tw.timerMu.Unlock()
}

// Start 启动滚动窗口的定时触发机制。
func (tw *TumblingWindow) Start() {
	go func() {
		<-tw.initChan
		// 在函数结束时关闭输出通道。
		defer close(tw.outputChan)

		for {
			// 在每次循环中安全地获取timer
			tw.timerMu.Lock()
			timer := tw.timer
			tw.timerMu.Unlock()

			if timer == nil {
				// 如果timer为nil，等待一小段时间后重试
				select {
				case <-time.After(10 * time.Millisecond):
					continue
				case <-tw.ctx.Done():
					return
				}
			}

			select {
			// 当定时器到期时，触发窗口。
			case <-timer.C:
				tw.Trigger()
			// 当上下文被取消时，停止定时器并退出循环。
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

	// 非阻塞发送到输出通道并更新统计信息
	select {
	case tw.outputChan <- resultData:
		// 成功发送，更新统计信息（已在锁内）
		tw.sentCount++
	default:
		// 通道已满，丢弃结果并更新统计信息（已在锁内）
		tw.droppedCount++
		// 可选：在这里添加日志记录
		// log.Printf("Window output channel full, dropped result with %d rows", len(resultData))
	}
}

// Reset 重置滚动窗口的数据。
func (tw *TumblingWindow) Reset() {
	// 首先取消上下文，停止所有正在运行的goroutine
	tw.cancelFunc()

	// 加锁以确保并发安全。
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// 停止现有的timer
	tw.timerMu.Lock()
	if tw.timer != nil {
		tw.timer.Stop()
		tw.timer = nil
	}
	tw.timerMu.Unlock()

	// 清空窗口数据。
	tw.data = nil
	tw.currentSlot = nil
	tw.initialized = false
	tw.initChan = make(chan struct{})

	// 重新创建context，为下次启动做准备
	tw.ctx, tw.cancelFunc = context.WithCancel(context.Background())
}

// OutputChan 返回一个只读通道，用于接收窗口触发时的数据。
func (tw *TumblingWindow) OutputChan() <-chan []types.Row {
	return tw.outputChan
}

// SetCallback 设置滚动窗口触发时的回调函数。
// 参数 callback 是要设置的回调函数。
func (tw *TumblingWindow) SetCallback(callback func([]types.Row)) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.callback = callback
}

// GetStats 获取窗口性能统计信息
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

// ResetStats 重置性能统计
func (tw *TumblingWindow) ResetStats() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.sentCount = 0
	tw.droppedCount = 0
}

// // GetResults 获取当前滚动窗口中的数据副本。
// func (tw *TumblingWindow) GetResults() []interface{} {
// 	// 加锁以确保并发安全。
// 	tw.mu.Lock()
// 	defer tw.mu.Unlock()
// 	// 返回窗口数据的副本。
// 	return append([]interface{}{}, tw.data...)
// }
