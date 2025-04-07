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
	config model.WindowConfig
	// 窗口的总大小，即窗口覆盖的时间范围
	size time.Duration
	// 窗口每次滑动的时间间隔
	slide time.Duration
	// 用于保护数据并发访问的互斥锁
	mu sync.Mutex
	// 存储窗口内的数据
	data []model.Row
	// 用于输出窗口内数据的通道
	outputChan chan []model.Row
	// 当窗口触发时执行的回调函数
	callback func([]model.Row)
	// 用于控制窗口生命周期的上下文
	ctx context.Context
	// 用于取消上下文的函数
	cancelFunc context.CancelFunc
	// 用于定时触发窗口的定时器
	timer       *time.Timer
	startSlot   *model.TimeSlot
	currentSlot *model.TimeSlot
}

// NewSlidingWindow 创建一个新的滑动窗口实例
// 参数 size 表示窗口的总大小，slide 表示窗口每次滑动的时间间隔
func NewSlidingWindow(config model.WindowConfig) (*SlidingWindow, error) {
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
		config:     config,
		size:       size,
		slide:      slide,
		outputChan: make(chan []model.Row, 10),
		ctx:        ctx,
		cancelFunc: cancel,
		data:       make([]model.Row, 0),
	}, nil
}

// Add 向滑动窗口中添加数据
// 参数 data 表示要添加的数据
func (sw *SlidingWindow) Add(data interface{}) {
	// 加锁以保证数据的并发安全
	sw.mu.Lock()
	defer sw.mu.Unlock()
	// 将数据添加到窗口的数据列表中

	if sw.startSlot == nil {
		sw.startSlot = sw.createSlot(GetTimestamp(data, sw.config.TsProp))
		sw.currentSlot = sw.startSlot
	}
	row := model.Row{
		Data:      data,
		Timestamp: GetTimestamp(data, sw.config.TsProp),
	}
	sw.data = append(sw.data, row)
}

func (sw *SlidingWindow) createSlot(t time.Time) *model.TimeSlot {
	// 创建一个新的时间槽位
	start := timex.AlignTimeToWindow(t, sw.size)
	end := start.Add(sw.size)
	slot := model.NewTimeSlot(&start, &end)
	return slot
}

func (sw *SlidingWindow) NextSlot() *model.TimeSlot {
	if sw.currentSlot == nil {
		return nil
	}
	start := sw.currentSlot.Start.Add(sw.slide)
	end := sw.currentSlot.End.Add(sw.slide)
	next := model.NewTimeSlot(&start, &end)
	return next
}

// Start 启动滑动窗口，开始定时触发窗口
func (sw *SlidingWindow) Start() {
	go func() {
		// 创建一个定时器，初始时间为窗口滑动的时间间隔
		sw.timer = time.NewTimer(sw.slide)
		for {
			select {
			// 当定时器到期时，触发窗口
			case <-sw.timer.C:
				sw.Trigger()
				// 重置定时器，以便下一次触发
				sw.timer.Reset(sw.slide)
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

	// 计算截止时间，即当前时间减去窗口的总大小
	next := sw.NextSlot()
	var newData []model.Row
	// 遍历窗口内的数据，只保留在截止时间之后的数据
	for _, item := range sw.data {
		if next.Contains(item.Timestamp) {
			newData = append(newData, item)
		}
	}

	// 提取出 Data 字段组成 []interface{} 类型的数据
	resultData := make([]model.Row, 0)
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
}

// OutputChan 返回滑动窗口的输出通道
func (sw *SlidingWindow) OutputChan() <-chan []model.Row {
	return sw.outputChan
}

// SetCallback 设置滑动窗口触发时执行的回调函数
// 参数 callback 表示要设置的回调函数
func (sw *SlidingWindow) SetCallback(callback func([]model.Row)) {
	sw.callback = callback
}

// GetResults 获取滑动窗口内的当前数据
func (sw *SlidingWindow) GetResults() []interface{} {
	// 加锁以保证数据的并发安全
	sw.mu.Lock()
	defer sw.mu.Unlock()
	// 提取出 Data 字段组成 []interface{} 类型的数据
	resultData := make([]interface{}, 0, len(sw.data))
	for _, item := range sw.data {
		resultData = append(resultData, item.Data)
	}
	return resultData
}
