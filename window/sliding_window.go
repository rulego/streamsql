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
	mu sync.Mutex
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
	return &SlidingWindow{
		config:      config,
		size:        size,
		slide:       slide,
		outputChan:  make(chan []types.Row, 10),
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
		sw.timer = time.NewTicker(sw.slide)
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
			select {
			// 当定时器到期时，触发窗口
			case <-sw.timer.C:
				sw.Trigger()
			// 当上下文被取消时，停止定时器并退出循环
			case <-sw.ctx.Done():
				sw.timer.Stop()
				return
			}
		}
	}()
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
	// 将新的数据发送到输出通道
	sw.outputChan <- resultData
}

// Reset 重置滑动窗口，清空窗口内的数据
func (sw *SlidingWindow) Reset() {
	// 加锁以保证数据的并发安全
	sw.mu.Lock()
	defer sw.mu.Unlock()
	// 清空窗口内的数据
	sw.data = nil
	sw.currentSlot = nil
	sw.initialized = false
	sw.initChan = make(chan struct{})
}

// OutputChan 返回滑动窗口的输出通道
func (sw *SlidingWindow) OutputChan() <-chan []types.Row {
	return sw.outputChan
}

// SetCallback 设置滑动窗口触发时执行的回调函数
// 参数 callback 表示要设置的回调函数
func (sw *SlidingWindow) SetCallback(callback func([]types.Row)) {
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

// createSlot 创建一个新的时间槽位
func (sw *SlidingWindow) createSlot(t time.Time) *types.TimeSlot {
	// 创建一个新的时间槽位
	start := timex.AlignTimeToWindow(t, sw.size)
	end := start.Add(sw.size)
	slot := types.NewTimeSlot(&start, &end)
	return slot
}
