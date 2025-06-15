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

// 确保 SlidingWindow 结构体实现了 Window 接口
var _ Window = (*SlidingWindow)(nil)

// TimedData 用于包装数据和时间戳
type TimedData struct {
	Data      interface{}
	Timestamp time.Time
}

// SlidingWindow 表示一个滑动窗口，用于按时间范围处理数据
type SlidingWindow struct {
	// config 窗口的配置信息
	config types.WindowConfig
	// 窗口的总大小，即窗口覆盖的时间范围
	size time.Duration
	// 窗口每次滑动的时间间隔
	slide time.Duration
	// 用于保护数据并发访问的互斥锁
	mu sync.RWMutex
	// 存储窗口内的数据
	data []types.Row
	// 用于输出窗口内数据的通道
	outputChan chan []types.Row
	// 当窗口触发时执行的回调函数
	callback func([]types.Row)
	// 用于控制窗口生命周期的上下文
	ctx context.Context
	// 用于取消上下文的函数
	cancelFunc context.CancelFunc
	// 用于定时触发窗口的定时器
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

// NewSlidingWindow 创建一个新的滑动窗口实例
// 参数 size 表示窗口的总大小，slide 表示窗口每次滑动的时间间隔
func NewSlidingWindow(config types.WindowConfig) (*SlidingWindow, error) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	size, err := cast.ToDurationE(config.Params["size"])
	if err != nil {
		return nil, fmt.Errorf("invalid size for sliding window: %v", err)
	}
	slide, err := cast.ToDurationE(config.Params["slide"])
	if err != nil {
		return nil, fmt.Errorf("invalid slide for sliding window: %v", err)
	}

	// 使用统一的性能配置获取窗口输出缓冲区大小
	bufferSize := 1000 // 默认值
	if perfConfig, exists := config.Params["performanceConfig"]; exists {
		if pc, ok := perfConfig.(types.PerformanceConfig); ok {
			bufferSize = pc.BufferConfig.WindowOutputSize
		}
	}

	return &SlidingWindow{
		config:      config,
		size:        size,
		slide:       slide,
		outputChan:  make(chan []types.Row, bufferSize),
		ctx:         ctx,
		cancelFunc:  cancel,
		data:        make([]types.Row, 0),
		initChan:    make(chan struct{}),
		initialized: false,
	}, nil
}

// Add 向滑动窗口中添加数据
// 参数 data 表示要添加的数据
func (sw *SlidingWindow) Add(data interface{}) {
	// 加锁以保证数据的并发安全
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// 将数据添加到窗口的数据列表中
	t := GetTimestamp(data, sw.config.TsProp, sw.config.TimeUnit)
	if !sw.initialized {
		sw.currentSlot = sw.createSlot(t)
		sw.timerMu.Lock()
		sw.timer = time.NewTicker(sw.slide)
		sw.timerMu.Unlock()
		// 发送初始化完成信号
		close(sw.initChan)
		sw.initialized = true
	}
	row := types.Row{
		Data:      data,
		Timestamp: t,
	}
	sw.data = append(sw.data, row)
}

// Start 启动滑动窗口，开始定时触发窗口
func (sw *SlidingWindow) Start() {
	go func() {
		// 等待初始化信号
		<-sw.initChan
		// 在函数结束时关闭输出通道。
		defer close(sw.outputChan)

		for {
			// 在每次循环中安全地获取timer
			sw.timerMu.Lock()
			timer := sw.timer
			sw.timerMu.Unlock()

			if timer == nil {
				// 如果timer为nil，等待一小段时间后重试
				select {
				case <-time.After(10 * time.Millisecond):
					continue
				case <-sw.ctx.Done():
					return
				}
			}

			select {
			// 当定时器到期时，触发窗口
			case <-timer.C:
				sw.Trigger()
			// 当上下文被取消时，停止定时器并退出循环
			case <-sw.ctx.Done():
				sw.timerMu.Lock()
				if sw.timer != nil {
					sw.timer.Stop()
				}
				sw.timerMu.Unlock()
				return
			}
		}
	}()
}

// Stop 停止滑动窗口的操作
func (sw *SlidingWindow) Stop() {
	// 调用取消函数以停止窗口的操作
	sw.cancelFunc()

	// 安全地停止timer
	sw.timerMu.Lock()
	if sw.timer != nil {
		sw.timer.Stop()
	}
	sw.timerMu.Unlock()
}

// Trigger 触发滑动窗口，处理窗口内的数据
func (sw *SlidingWindow) Trigger() {
	// 加锁以保证数据的并发安全
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// 如果窗口内没有数据，则直接返回
	if len(sw.data) == 0 {
		return
	}
	if !sw.initialized {
		return
	}
	// 计算截止时间，即当前时间减去窗口的总大小
	next := sw.NextSlot()
	// 保留下一个窗口的数据
	tms := next.Start.Add(-sw.size)
	tme := next.End.Add(sw.size)
	temp := types.NewTimeSlot(&tms, &tme)
	newData := make([]types.Row, 0)
	for _, item := range sw.data {
		if temp.Contains(item.Timestamp) {
			newData = append(newData, item)
		}
	}

	// 提取出 Data 字段组成 []interface{} 类型的数据
	resultData := make([]types.Row, 0)
	for _, item := range sw.data {
		if sw.currentSlot.Contains(item.Timestamp) {
			item.Slot = sw.currentSlot
			resultData = append(resultData, item)
		}
	}

	// 如果设置了回调函数，则执行回调函数
	if sw.callback != nil {
		sw.callback(resultData)
	}

	// 更新窗口内的数据
	sw.data = newData
	sw.currentSlot = next

	// 非阻塞发送到输出通道
	sw.sendResultNonBlocking(resultData)
}

// sendResultNonBlocking 非阻塞地发送结果到输出通道
func (sw *SlidingWindow) sendResultNonBlocking(resultData []types.Row) {
	select {
	case sw.outputChan <- resultData:
		// 成功发送
		sw.sentCount++
	default:
		// 通道已满，丢弃结果
		sw.droppedCount++
	}
}

// GetStats 获取窗口性能统计信息
func (sw *SlidingWindow) GetStats() map[string]int64 {
	sw.mu.RLock()
	defer sw.mu.RUnlock()

	return map[string]int64{
		"sent_count":    sw.sentCount,
		"dropped_count": sw.droppedCount,
		"buffer_size":   int64(cap(sw.outputChan)),
		"buffer_used":   int64(len(sw.outputChan)),
	}
}

// ResetStats 重置性能统计
func (sw *SlidingWindow) ResetStats() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.sentCount = 0
	sw.droppedCount = 0
}

// Reset 重置滑动窗口，清空窗口内的数据
func (sw *SlidingWindow) Reset() {
	// 首先取消上下文，停止所有正在运行的goroutine
	sw.cancelFunc()

	// 加锁以保证数据的并发安全
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// 停止现有的timer
	sw.timerMu.Lock()
	if sw.timer != nil {
		sw.timer.Stop()
		sw.timer = nil
	}
	sw.timerMu.Unlock()

	// 清空窗口内的数据
	sw.data = nil
	sw.currentSlot = nil
	sw.initialized = false
	sw.initChan = make(chan struct{})

	// 重新创建context，为下次启动做准备
	sw.ctx, sw.cancelFunc = context.WithCancel(context.Background())
}

// OutputChan 返回滑动窗口的输出通道
func (sw *SlidingWindow) OutputChan() <-chan []types.Row {
	return sw.outputChan
}

// SetCallback 设置滑动窗口触发时执行的回调函数
// 参数 callback 表示要设置的回调函数
func (sw *SlidingWindow) SetCallback(callback func([]types.Row)) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.callback = callback
}

func (sw *SlidingWindow) NextSlot() *types.TimeSlot {
	if sw.currentSlot == nil {
		return nil
	}
	start := sw.currentSlot.Start.Add(sw.slide)
	end := sw.currentSlot.End.Add(sw.slide)
	next := types.NewTimeSlot(&start, &end)
	return next
}

func (sw *SlidingWindow) createSlot(t time.Time) *types.TimeSlot {
	// 创建一个新的时间槽位
	start := timex.AlignTimeToWindow(t, sw.slide)
	end := start.Add(sw.size)
	slot := types.NewTimeSlot(&start, &end)
	return slot
}
