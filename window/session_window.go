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

// 确保 SessionWindow 结构体实现了 Window 接口
var _ Window = (*SessionWindow)(nil)

// SessionWindow 表示一个会话窗口
// 会话窗口是基于事件时间的窗口，当一段时间内没有事件到达时，会话窗口就会关闭
type SessionWindow struct {
	// config 是窗口的配置信息
	config types.WindowConfig
	// timeout 是会话超时时间，如果在此时间内没有新事件，会话将关闭
	timeout time.Duration
	// mu 用于保护对窗口数据的并发访问
	mu sync.RWMutex
	// sessionMap 存储不同 key 的会话数据
	sessionMap map[string]*session
	// outputChan 是一个通道，用于在窗口触发时发送数据
	outputChan chan []types.Row
	// callback 是一个可选的回调函数，在窗口触发时调用
	callback func([]types.Row)
	// ctx 用于控制窗口的生命周期
	ctx context.Context
	// cancelFunc 用于取消窗口的操作
	cancelFunc context.CancelFunc
	// 用于初始化窗口的通道
	initChan    chan struct{}
	initialized bool
	// 保护ticker的锁
	tickerMu sync.Mutex
	ticker   *time.Ticker
}

// session 存储一个会话的数据和状态
type session struct {
	data       []types.Row
	lastActive time.Time
	slot       *types.TimeSlot
}

// NewSessionWindow 创建一个新的会话窗口实例
func NewSessionWindow(config types.WindowConfig) (*SessionWindow, error) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())
	timeout, err := cast.ToDurationE(config.Params["timeout"])
	if err != nil {
		return nil, fmt.Errorf("invalid timeout for session window: %v", err)
	}

	// 使用统一的性能配置获取窗口输出缓冲区大小
	bufferSize := 100 // 默认值，会话窗口通常缓冲较小
	if perfConfig, exists := config.Params["performanceConfig"]; exists {
		if pc, ok := perfConfig.(types.PerformanceConfig); ok {
			bufferSize = pc.BufferConfig.WindowOutputSize / 10 // 会话窗口使用1/10的缓冲区
			if bufferSize < 10 {
				bufferSize = 10 // 最小值
			}
		}
	}

	return &SessionWindow{
		config:      config,
		timeout:     timeout,
		sessionMap:  make(map[string]*session),
		outputChan:  make(chan []types.Row, bufferSize),
		ctx:         ctx,
		cancelFunc:  cancel,
		initChan:    make(chan struct{}),
		initialized: false,
	}, nil
}

// Add 向会话窗口添加数据
func (sw *SessionWindow) Add(data interface{}) {
	// 加锁以确保并发安全
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if !sw.initialized {
		close(sw.initChan)
		sw.initialized = true
	}

	// 获取数据时间戳
	timestamp := GetTimestamp(data, sw.config.TsProp, sw.config.TimeUnit)
	// 创建 Row 对象
	row := types.Row{
		Data:      data,
		Timestamp: timestamp,
	}

	// 提取会话键
	// 如果配置了 groupby，则使用 groupby 字段作为会话键
	key := extractSessionKey(data, sw.config.GroupByKey)

	// 获取或创建会话
	s, exists := sw.sessionMap[key]
	if !exists {
		// 创建新会话
		start := timex.AlignTime(timestamp, sw.config.TimeUnit, true)
		end := start.Add(sw.timeout)
		slot := types.NewTimeSlot(&start, &end)

		s = &session{
			data:       []types.Row{},
			lastActive: timestamp,
			slot:       slot,
		}
		sw.sessionMap[key] = s
	} else {
		// 更新会话结束时间
		if timestamp.After(s.lastActive) {
			s.lastActive = timestamp
			// 延长会话结束时间
			newEnd := timestamp.Add(sw.timeout)
			if newEnd.After(*s.slot.End) {
				s.slot.End = &newEnd
			}
		}
	}

	// 添加数据到会话
	row.Slot = s.slot
	s.data = append(s.data, row)
}

// Start 启动会话窗口的定时检查机制
// Start 启动会话窗口，开始定期检查过期会话
// 采用延迟初始化模式，避免在没有数据时无限等待，同时确保后续数据能正常处理
func (sw *SessionWindow) Start() {
	go func() {
		// 在函数结束时关闭输出通道
		defer close(sw.outputChan)

		// 等待初始化完成或上下文取消
		select {
		case <-sw.initChan:
			// 正常初始化完成，继续处理
		case <-sw.ctx.Done():
			// 上下文被取消，直接退出
			return
		}

		// 定期检查过期会话
		sw.tickerMu.Lock()
		sw.ticker = time.NewTicker(sw.timeout / 2)
		ticker := sw.ticker
		sw.tickerMu.Unlock()

		defer func() {
			sw.tickerMu.Lock()
			if sw.ticker != nil {
				sw.ticker.Stop()
			}
			sw.tickerMu.Unlock()
		}()

		for {
			select {
			case <-ticker.C:
				sw.checkExpiredSessions()
			case <-sw.ctx.Done():
				return
			}
		}
	}()
}

// Stop 停止会话窗口的操作
func (sw *SessionWindow) Stop() {
	// 调用取消函数以停止窗口的操作
	sw.cancelFunc()

	// 安全地停止ticker
	sw.tickerMu.Lock()
	if sw.ticker != nil {
		sw.ticker.Stop()
	}
	sw.tickerMu.Unlock()
}

// checkExpiredSessions 检查并触发过期会话
func (sw *SessionWindow) checkExpiredSessions() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	expiredKeys := []string{}

	// 查找过期会话
	for key, s := range sw.sessionMap {
		if now.Sub(s.lastActive) > sw.timeout {
			expiredKeys = append(expiredKeys, key)
		}
	}

	// 处理过期会话
	for _, key := range expiredKeys {
		s := sw.sessionMap[key]
		if len(s.data) > 0 {
			// 触发会话窗口
			result := make([]types.Row, len(s.data))
			copy(result, s.data)

			// 如果设置了回调函数，则执行回调函数
			if sw.callback != nil {
				sw.callback(result)
			}

			// 将数据发送到输出通道
			sw.outputChan <- result
		}
		// 删除过期会话
		delete(sw.sessionMap, key)
	}
}

// Trigger 手动触发所有会话窗口
func (sw *SessionWindow) Trigger() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// 遍历所有会话
	for _, s := range sw.sessionMap {
		if len(s.data) > 0 {
			// 触发会话窗口
			result := make([]types.Row, len(s.data))
			copy(result, s.data)

			// 如果设置了回调函数，则执行回调函数
			if sw.callback != nil {
				sw.callback(result)
			}

			// 将数据发送到输出通道
			sw.outputChan <- result
		}
	}
	// 清空所有会话
	sw.sessionMap = make(map[string]*session)
}

// Reset 重置会话窗口的数据
func (sw *SessionWindow) Reset() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// 停止现有的ticker
	sw.tickerMu.Lock()
	if sw.ticker != nil {
		sw.ticker.Stop()
		sw.ticker = nil
	}
	sw.tickerMu.Unlock()

	// 清空会话数据
	sw.sessionMap = make(map[string]*session)
	sw.initialized = false
	sw.initChan = make(chan struct{})
}

// OutputChan 返回一个只读通道，用于接收窗口触发时的数据
func (sw *SessionWindow) OutputChan() <-chan []types.Row {
	return sw.outputChan
}

// SetCallback 设置会话窗口触发时的回调函数
func (sw *SessionWindow) SetCallback(callback func([]types.Row)) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.callback = callback
}

// extractSessionKey 从数据中提取会话键
// 如果未指定键，则返回默认键
func extractSessionKey(data interface{}, keyField string) string {
	if keyField == "" {
		return "default" // 默认会话键
	}

	// 尝试从 map 中提取
	if m, ok := data.(map[string]interface{}); ok {
		if val, exists := m[keyField]; exists {
			return fmt.Sprintf("%v", val)
		}
	}

	return "default"
}
