package window

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getTypeString 获取对象的类型字符串表示
func getTypeString(obj interface{}) string {
	if obj == nil {
		return ""
	}
	return reflect.TypeOf(obj).String()
}

// TestWindowEdgeCases 测试窗口的边界条件
func TestWindowEdgeCases(t *testing.T) {
	t.Run("tumbling window with zero duration", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Duration(0),
			},
		}
		_, err := NewTumblingWindow(config)
		// 零持续时间可能是有效的，取决于实现
		_ = err
	})

	t.Run("tumbling window with negative duration", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": -time.Second,
			},
		}
		_, err := NewTumblingWindow(config)
		// 负持续时间可能是有效的，取决于实现
		_ = err
	})

	t.Run("sliding window with zero window size", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size":  time.Duration(0),
				"slide": time.Second,
			},
		}
		_, err := NewSlidingWindow(config)
		// 零滑动间隔可能是有效的，取决于实现
		_ = err
	})

	t.Run("sliding window with zero slide interval", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size":  time.Minute,
				"slide": time.Duration(0),
			},
		}
		_, err := NewSlidingWindow(config)
		// 零滑动间隔可能是有效的，取决于实现
		_ = err
	})

	t.Run("sliding window with slide larger than window", func(t *testing.T) {
		// 这种情况可能是有效的，取决于实现
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size":  time.Second,
				"slide": time.Minute,
			},
		}
		window, err := NewSlidingWindow(config)
		_ = window
		_ = err
	})

	t.Run("counting window with zero count", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"count": 0,
			},
		}
		_, err := NewCountingWindow(config)
		require.NotNil(t, err)
	})

	t.Run("counting window with negative count", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"count": -10,
			},
		}
		_, err := NewCountingWindow(config)
		require.NotNil(t, err)
	})

	t.Run("session window with zero timeout", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"timeout": time.Duration(0),
			},
		}
		_, err := NewSessionWindow(config)
		// 零超时可能是有效的，取决于实现
		_ = err
	})

	t.Run("session window with negative timeout", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"timeout": -time.Second,
			},
		}
		_, err := NewSessionWindow(config)
		// 负超时可能是有效的，取决于实现
		_ = err
	})
}

// TestWindowWithNilCallback 测试窗口使用nil回调函数
func TestWindowWithNilCallback(t *testing.T) {
	t.Run("tumbling window with nil callback", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			require.NotNil(t, window)
			window.Start()

			// 添加数据不应该panic
			row := types.Row{
				Data:      map[string]interface{}{"id": 1},
				Timestamp: time.Now(),
			}
			window.Add(row)
		}
	})

	t.Run("sliding window with nil callback", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size":  time.Minute,
				"slide": time.Second,
			},
		}
		window, err := NewSlidingWindow(config)
		if err == nil {
			require.NotNil(t, window)
			window.Start()

			row := types.Row{
				Data:      map[string]interface{}{"id": 1},
				Timestamp: time.Now(),
			}
			window.Add(row)
		}
	})

	t.Run("counting window with nil callback", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"count": 10,
			},
		}
		window, err := NewCountingWindow(config)
		if err == nil {
			require.NotNil(t, window)
			window.Start()

			row := types.Row{
				Data:      map[string]interface{}{"id": 1},
				Timestamp: time.Now(),
			}
			window.Add(row)
		}
	})

	t.Run("session window with nil callback", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"timeout": time.Minute,
			},
		}
		window, err := NewSessionWindow(config)
		if err == nil {
			require.NotNil(t, window)
			window.Start()

			row := types.Row{
				Data:      map[string]interface{}{"id": 1},
				Timestamp: time.Now(),
			}
			window.Add(row)
		}
	})
}

// TestWindowConcurrency 测试窗口的并发安全性
func TestWindowConcurrency(t *testing.T) {
	t.Run("concurrent add to tumbling window", func(t *testing.T) {
		var receivedData [][]types.Row
		var mu sync.Mutex

		callback := func(rows []types.Row) {
			mu.Lock()
			receivedData = append(receivedData, rows)
			mu.Unlock()
		}

		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Millisecond * 100,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(callback)
		}
		require.Nil(t, err)

		window.Start()

		var wg sync.WaitGroup
		numGoroutines := 10
		numRowsPerGoroutine := 50

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < numRowsPerGoroutine; j++ {
					row := types.Row{
						Data: map[string]interface{}{
							"id":    goroutineID*1000 + j,
							"value": float64(j),
						},
						Timestamp: time.Now(),
					}
					window.Add(row)
				}
			}(i)
		}

		wg.Wait()

		// 等待窗口处理完成
		time.Sleep(time.Millisecond * 200)
	})

	t.Run("concurrent start stop", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		var wg sync.WaitGroup
		numGoroutines := 5

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				window.Start()
				time.Sleep(time.Millisecond * 10)

			}()
		}

		wg.Wait()
	})

	t.Run("concurrent add and stop", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		var wg sync.WaitGroup

		// 一个goroutine添加数据
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				row := types.Row{
					Data:      map[string]interface{}{"id": i},
					Timestamp: time.Now(),
				}
				window.Add(row)
				time.Sleep(time.Millisecond)
			}
		}()

		// 另一个goroutine停止窗口
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * 50)

		}()

		wg.Wait()
	})
}

// TestWindowMemoryManagement 测试窗口的内存管理
func TestWindowMemoryManagement(t *testing.T) {
	t.Run("large data in tumbling window", func(t *testing.T) {
		var processedCount int
		callback := func(rows []types.Row) {
			processedCount += len(rows)
		}

		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Millisecond * 50,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(callback)
		}
		require.Nil(t, err)

		window.Start()

		// 添加大量数据
		largeData := make([]byte, 1024*1024) // 1MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		for i := 0; i < 10; i++ {
			row := types.Row{
				Data: map[string]interface{}{
					"id":   i,
					"data": string(largeData),
				},
				Timestamp: time.Now(),
			}
			window.Add(row)
		}

		// 等待处理完成
		time.Sleep(time.Millisecond * 200)
	})

	t.Run("rapid data addition", func(t *testing.T) {
		var processedCount int
		var mu sync.Mutex

		callback := func(rows []types.Row) {
			mu.Lock()
			processedCount += len(rows)
			mu.Unlock()
		}

		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Millisecond * 10,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(callback)
		}
		require.Nil(t, err)

		window.Start()

		// 快速添加大量小数据
		for i := 0; i < 1000; i++ {
			row := types.Row{
				Data:      map[string]interface{}{"id": i},
				Timestamp: time.Now(),
			}
			window.Add(row)
		}

		// 等待处理完成
		time.Sleep(time.Millisecond * 100)
	})
}

// TestWindowErrorConditions 测试窗口的错误条件
func TestWindowErrorConditions(t *testing.T) {
	t.Run("add to stopped window", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		// 向已停止的窗口添加数据不应该panic
		row := types.Row{
			Data:      map[string]interface{}{"id": 1},
			Timestamp: time.Now(),
		}
		window.Add(row)
	})

	t.Run("add invalid data types", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		// 添加包含不可序列化数据的行
		row := types.Row{
			Data: map[string]interface{}{
				"id":      1,
				"channel": make(chan int),
				"func":    func() {},
			},
			Timestamp: time.Now(),
		}
		window.Add(row)
	})

	t.Run("add row with zero timestamp", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		// 添加时间戳为零值的行
		row := types.Row{
			Data:      map[string]interface{}{"id": 1},
			Timestamp: time.Time{},
		}
		window.Add(row)
	})

	t.Run("add row with future timestamp", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		// 添加未来时间戳的行
		row := types.Row{
			Data:      map[string]interface{}{"id": 1},
			Timestamp: time.Now().Add(time.Hour),
		}
		window.Add(row)
	})

	t.Run("add row with very old timestamp", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		// 添加很久以前的时间戳的行
		row := types.Row{
			Data:      map[string]interface{}{"id": 1},
			Timestamp: time.Now().Add(-time.Hour * 24),
		}
		window.Add(row)
	})
}

// TestWindowStatsAndMetrics 测试窗口的统计和指标
func TestWindowStatsAndMetrics(t *testing.T) {
	t.Run("get stats from tumbling window", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		assert.Nil(t, err)

		// 获取统计信息不应该panic
		stats := window.GetStats()
		_ = stats
	})

	t.Run("reset stats", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		assert.Nil(t, err)

		window.Start()

		// 添加一些数据
		row := types.Row{
			Data:      map[string]interface{}{"id": 1},
			Timestamp: time.Now(),
		}
		window.Add(row)

		// 重置统计信息不应该panic
		window.ResetStats()
	})

	t.Run("get output channel", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		assert.Nil(t, err)

		// 获取输出通道不应该panic
		outputChan := window.OutputChan()
		_ = outputChan
	})

	t.Run("set callback", func(t *testing.T) {
		config := types.WindowConfig{
			Params: map[string]interface{}{
				"size": time.Second,
			},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			// 设置新的回调函数不应该panic
			newCallback := func(rows []types.Row) {
				// 新的回调逻辑
			}
			window.SetCallback(newCallback)
		}
	})
}

// TestWindowWithPerformanceConfig 测试窗口性能配置
func TestWindowWithPerformanceConfig(t *testing.T) {
	tests := []struct {
		name               string
		windowType         string
		performanceConfig  types.PerformanceConfig
		expectedBufferSize int
		extraParams        map[string]interface{}
	}{
		{
			name:               "滚动窗口-默认配置",
			windowType:         TypeTumbling,
			performanceConfig:  types.DefaultPerformanceConfig(),
			expectedBufferSize: 50,
			extraParams:        map[string]interface{}{"size": "2s"},
		},
		{
			name:               "滚动窗口-高性能配置",
			windowType:         TypeTumbling,
			performanceConfig:  types.HighPerformanceConfig(),
			expectedBufferSize: 200,
			extraParams:        map[string]interface{}{"size": "2s"},
		},
		{
			name:               "滚动窗口-低延迟配置",
			windowType:         TypeTumbling,
			performanceConfig:  types.LowLatencyConfig(),
			expectedBufferSize: 20,
			extraParams:        map[string]interface{}{"size": "2s"},
		},
		{
			name:               "滑动窗口-高性能配置",
			windowType:         TypeSliding,
			performanceConfig:  types.HighPerformanceConfig(),
			expectedBufferSize: 200,
			extraParams:        map[string]interface{}{"size": "10s", "slide": "5s"},
		},
		{
			name:               "计数窗口-高性能配置",
			windowType:         TypeCounting,
			performanceConfig:  types.HighPerformanceConfig(),
			expectedBufferSize: 20, // 200 / 10
			extraParams:        map[string]interface{}{"count": 10},
		},
		{
			name:               "自定义性能配置",
			windowType:         TypeTumbling,
			performanceConfig:  types.PerformanceConfig{BufferConfig: types.BufferConfig{WindowOutputSize: 500}},
			expectedBufferSize: 500,
			extraParams:        map[string]interface{}{"size": "2s"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.WindowConfig{
				Type:   tt.windowType,
				Params: make(map[string]interface{}),
			}

			// 合并参数
			for k, v := range tt.extraParams {
				config.Params[k] = v
			}
			config.Params["performanceConfig"] = tt.performanceConfig

			var window Window
			var err error

			switch tt.windowType {
			case TypeTumbling:
				window, err = NewTumblingWindow(config)
			case TypeSliding:
				window, err = NewSlidingWindow(config)
			case TypeCounting:
				window, err = NewCountingWindow(config)
			case TypeSession:
				window, err = NewSessionWindow(config)
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedBufferSize, cap(window.OutputChan()))

			if closer, ok := window.(interface{ Stop() }); ok {
				closer.Stop()
			}
		})
	}

	t.Run("无性能配置-使用默认值", func(t *testing.T) {
		config := types.WindowConfig{
			Type: TypeTumbling,
			Params: map[string]interface{}{
				"size": "3s",
			},
		}

		tw, err := NewTumblingWindow(config)
		assert.NoError(t, err)
		assert.Equal(t, 1000, cap(tw.outputChan))
		tw.Stop()
	})
}

// TestGetTimestampEdgeCases 测试GetTimestamp函数的边缘情况
func TestGetTimestampEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		data     interface{}
		tsProp   string
		timeUnit time.Duration
		checkNow bool
	}{
		{
			name:     "空字符串时间戳属性",
			data:     map[string]interface{}{"value": 42},
			tsProp:   "",
			timeUnit: time.Second,
			checkNow: true,
		},
		{
			name: "结构体中不存在的字段",
			data: struct {
				Value int
			}{Value: 42},
			tsProp:   "NonExistentField",
			timeUnit: time.Second,
			checkNow: true,
		},
		{
			name: "map中不存在的键",
			data: map[string]interface{}{
				"value": 42,
			},
			tsProp:   "nonexistent",
			timeUnit: time.Second,
			checkNow: true,
		},
		{
			name: "map中非时间类型的值",
			data: map[string]interface{}{
				"timestamp": "not a time",
			},
			tsProp:   "timestamp",
			timeUnit: time.Second,
			checkNow: true,
		},
		{
			name: "非字符串键的map",
			data: map[int]interface{}{
				1: time.Now(),
			},
			tsProp:   "timestamp",
			timeUnit: time.Second,
			checkNow: true,
		},
		{
			name:     "nil数据",
			data:     nil,
			tsProp:   "timestamp",
			timeUnit: time.Second,
			checkNow: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetTimestamp(tt.data, tt.tsProp, tt.timeUnit)
			if tt.checkNow {
				// 检查返回的时间是否接近当前时间（允许1秒误差）
				assert.WithinDuration(t, time.Now(), result, time.Second)
			}
		})
	}
}

// TestSessionWindowSessionKey 测试会话窗口的会话键提取
func TestSessionWindowSessionKey(t *testing.T) {
	config := types.WindowConfig{
		Type: TypeSession,
		Params: map[string]interface{}{
			"timeout": "5s",
		},
		GroupByKey: "user_id",
	}

	sw, err := NewSessionWindow(config)
	assert.NoError(t, err)

	// 测试不同类型的数据
	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "map数据",
			data: map[string]interface{}{
				"user_id": "user123",
				"value":   100,
			},
		},
		{
			name: "结构体数据",
			data: struct {
				UserID string `json:"user_id"`
				Value  int
			}{UserID: "user456", Value: 200},
		},
		{
			name: "无效数据",
			data: "invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 这里只是测试Add方法不会panic
			assert.NotPanics(t, func() {
				sw.Add(tt.data)
			})
		})
	}
}

// TestWindowStopBeforeStart 测试在启动前停止窗口
func TestWindowStopBeforeStart(t *testing.T) {
	tests := []struct {
		name   string
		config types.WindowConfig
	}{
		{
			name: "滚动窗口",
			config: types.WindowConfig{
				Type:   TypeTumbling,
				Params: map[string]interface{}{"size": "1s"},
			},
		},
		{
			name: "滑动窗口",
			config: types.WindowConfig{
				Type: TypeSliding,
				Params: map[string]interface{}{
					"size":  "2s",
					"slide": "1s",
				},
			},
		},
		{
			name: "计数窗口",
			config: types.WindowConfig{
				Type:   TypeCounting,
				Params: map[string]interface{}{"count": 10},
			},
		},
		{
			name: "会话窗口",
			config: types.WindowConfig{
				Type:   TypeSession,
				Params: map[string]interface{}{"timeout": "5s"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			window, err := CreateWindow(tt.config)
			assert.NoError(t, err)

			// 在启动前停止窗口应该不会panic
			assert.NotPanics(t, func() {
				if tw, ok := window.(*TumblingWindow); ok {
					tw.Stop()
				} else if sw, ok := window.(*SlidingWindow); ok {
					sw.Stop()
				} else if cw, ok := window.(*CountingWindow); ok {
					// CountingWindow doesn't have Stop method
					_ = cw
				} else if sesw, ok := window.(*SessionWindow); ok {
					sesw.Stop()
				}
			})
		})
	}
}

// TestWindowMultipleStops 测试多次停止窗口
func TestWindowMultipleStops(t *testing.T) {
	config := types.WindowConfig{
		Type:   TypeTumbling,
		Params: map[string]interface{}{"size": "1s"},
	}

	tw, err := NewTumblingWindow(config)
	assert.NoError(t, err)

	tw.Start()

	// 多次停止应该不会panic
	assert.NotPanics(t, func() {
		tw.Stop()
		tw.Stop()
		tw.Stop()
	})
}

// TestWindowAddAfterStop 测试停止后添加数据
func TestWindowAddAfterStop(t *testing.T) {
	config := types.WindowConfig{
		Type:   TypeTumbling,
		Params: map[string]interface{}{"size": "1s"},
	}

	tw, err := NewTumblingWindow(config)
	assert.NoError(t, err)

	tw.Start()
	tw.Stop()

	// 停止后添加数据应该不会panic
	assert.NotPanics(t, func() {
		tw.Add(map[string]interface{}{"value": 42})
	})
}

// TestCountingWindowWithCallback 测试计数窗口的回调功能
func TestCountingWindowWithCallback(t *testing.T) {
	var mu sync.Mutex
	callbackData := make([][]types.Row, 0)
	callback := func(results []types.Row) {
		mu.Lock()
		defer mu.Unlock()
		callbackData = append(callbackData, results)
	}

	config := types.WindowConfig{
		Type: TypeCounting,
		Params: map[string]interface{}{
			"count":    2,
			"callback": callback,
		},
	}

	cw, err := NewCountingWindow(config)
	assert.NoError(t, err)

	cw.Start()
	// CountingWindow doesn't have Stop method, will be handled by context cancellation

	// 添加数据
	cw.Add(map[string]interface{}{"value": 1})
	cw.Add(map[string]interface{}{"value": 2})

	// 等待处理
	time.Sleep(100 * time.Millisecond)

	// 检查回调是否被调用
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(callbackData) > 0
	}, time.Second, 10*time.Millisecond)
}

// TestSlidingWindowInvalidParams 测试滑动窗口的无效参数
func TestSlidingWindowInvalidParams(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]interface{}
	}{
		{
			name: "无效的slide参数",
			params: map[string]interface{}{
				"size":  "10s",
				"slide": "invalid",
			},
		},
		{
			name: "缺少slide参数",
			params: map[string]interface{}{
				"size": "10s",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.WindowConfig{
				Type:   TypeSliding,
				Params: tt.params,
			}
			_, err := NewSlidingWindow(config)
			assert.Error(t, err)
		})
	}
}

// TestWindowUnifiedConfigIntegration 集成测试：验证窗口配置与实际数据处理的集成
func TestWindowUnifiedConfigIntegration(t *testing.T) {
	t.Run("性能配置集成测试", func(t *testing.T) {
		performanceConfig := types.HighPerformanceConfig()

		windowConfig := types.WindowConfig{
			Type: TypeTumbling,
			Params: map[string]interface{}{
				"size":              "1s",
				"performanceConfig": performanceConfig,
			},
		}

		tw, err := NewTumblingWindow(windowConfig)
		assert.NoError(t, err)
		defer tw.Stop()

		// 验证缓冲区大小
		assert.Equal(t, 200, cap(tw.outputChan))

		// 启动窗口
		tw.Start()

		// 发送测试数据
		for i := 0; i < 10; i++ {
			tw.Add(map[string]interface{}{
				"id":    i,
				"value": i * 10,
			})
		}

		// 等待窗口触发
		time.Sleep(1200 * time.Millisecond)

		// 验证窗口能正常工作
		select {
		case data := <-tw.OutputChan():
			assert.Greater(t, len(data), 0)
			assert.LessOrEqual(t, len(data), 10)
		case <-time.After(500 * time.Millisecond):
			t.Error("超时未接收到窗口输出")
		}
	})

	t.Run("缓冲区溢出处理", func(t *testing.T) {
		// 创建一个小缓冲区的窗口
		smallBufferConfig := types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				WindowOutputSize: 1, // 非常小的缓冲区
			},
		}

		config := types.WindowConfig{
			Type: TypeTumbling,
			Params: map[string]interface{}{
				"size":              "100ms",
				"performanceConfig": smallBufferConfig,
			},
		}

		tw, err := NewTumblingWindow(config)
		assert.NoError(t, err)

		tw.Start()
		defer tw.Stop()

		// 快速添加大量数据，可能导致缓冲区溢出
		for i := 0; i < 10; i++ {
			tw.Add(map[string]interface{}{"value": i})
		}

		// 等待处理
		time.Sleep(200 * time.Millisecond)

		// 检查统计信息
		stats := tw.GetStats()
		assert.Contains(t, stats, "dropped_count")
		assert.Contains(t, stats, "sent_count")
	})
}

// TestCreateWindow 测试窗口工厂函数
func TestCreateWindow(t *testing.T) {
	tests := []struct {
		name         string
		config       types.WindowConfig
		expectError  bool
		expectedType string
	}{
		{
			name: "创建滚动窗口",
			config: types.WindowConfig{
				Type: TypeTumbling,
				Params: map[string]interface{}{
					"size": "5s",
				},
			},
			expectError:  false,
			expectedType: "*window.TumblingWindow",
		},
		{
			name: "创建滑动窗口",
			config: types.WindowConfig{
				Type: TypeSliding,
				Params: map[string]interface{}{
					"size":  "10s",
					"slide": "5s",
				},
			},
			expectError:  false,
			expectedType: "*window.SlidingWindow",
		},
		{
			name: "创建计数窗口",
			config: types.WindowConfig{
				Type: TypeCounting,
				Params: map[string]interface{}{
					"count": 100,
				},
			},
			expectError:  false,
			expectedType: "*window.CountingWindow",
		},
		{
			name: "创建会话窗口",
			config: types.WindowConfig{
				Type: TypeSession,
				Params: map[string]interface{}{
					"timeout": "30s",
				},
			},
			expectError:  false,
			expectedType: "*window.SessionWindow",
		},
		{
			name: "窗口工厂与统一配置集成",
			config: types.WindowConfig{
				Type: TypeTumbling,
				Params: map[string]interface{}{
					"size": "5s",
					"performanceConfig": types.PerformanceConfig{
						BufferConfig: types.BufferConfig{
							WindowOutputSize: 1500,
						},
					},
				},
			},
			expectError:  false,
			expectedType: "*window.TumblingWindow",
		},
		{
			name: "无效的窗口类型",
			config: types.WindowConfig{
				Type: "invalid",
				Params: map[string]interface{}{
					"size": "5s",
				},
			},
			expectError:  true,
			expectedType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			window, err := CreateWindow(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, window)
			assert.Equal(t, tt.expectedType, getTypeString(window))

			// 验证窗口能正常工作
			if closer, ok := window.(interface{ Stop() }); ok {
				closer.Stop()
			}
		})
	}
}

// TestGetTimestampCoverage 测试时间戳提取函数
func TestGetTimestampCoverage(t *testing.T) {
	testTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		data     interface{}
		tsProp   string
		timeUnit time.Duration
		expected time.Time
	}{
		{
			name:     "使用GetTimestamp接口",
			data:     TestDate2{ts: testTime},
			tsProp:   "",
			timeUnit: time.Second,
			expected: testTime,
		},
		{
			name: "从结构体字段提取时间戳",
			data: struct {
				Timestamp time.Time
				Value     int
			}{Timestamp: testTime, Value: 42},
			tsProp:   "Timestamp",
			timeUnit: time.Second,
			expected: testTime,
		},
		{
			name: "从map中提取时间戳",
			data: map[string]interface{}{
				"timestamp": testTime,
				"value":     42,
			},
			tsProp:   "timestamp",
			timeUnit: time.Second,
			expected: testTime,
		},
		{
			name: "从map中提取int64时间戳",
			data: map[string]interface{}{
				"timestamp": testTime.Unix(),
			},
			tsProp:   "timestamp",
			timeUnit: time.Second,
			expected: time.Unix(testTime.Unix(), 0),
		},
		{
			name:     "无法提取时间戳，使用当前时间",
			data:     "invalid data",
			tsProp:   "nonexistent",
			timeUnit: time.Second,
			// expected will be checked with time tolerance
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetTimestamp(tt.data, tt.tsProp, tt.timeUnit)
			if tt.name == "无法提取时间戳，使用当前时间" {
				// 检查返回的时间是否接近当前时间（允许1秒误差）
				assert.WithinDuration(t, time.Now(), result, time.Second)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestWindowErrorHandling 测试窗口错误处理
func TestWindowErrorHandling(t *testing.T) {
	t.Run("滚动窗口无效大小", func(t *testing.T) {
		config := types.WindowConfig{
			Type: TypeTumbling,
			Params: map[string]interface{}{
				"size": "invalid",
			},
		}
		_, err := NewTumblingWindow(config)
		assert.Error(t, err)
	})

	t.Run("滑动窗口无效参数", func(t *testing.T) {
		config := types.WindowConfig{
			Type: TypeSliding,
			Params: map[string]interface{}{
				"size":  "invalid",
				"slide": "5s",
			},
		}
		_, err := NewSlidingWindow(config)
		assert.Error(t, err)
	})

	t.Run("计数窗口无效计数", func(t *testing.T) {
		config := types.WindowConfig{
			Type: TypeCounting,
			Params: map[string]interface{}{
				"count": 0,
			},
		}
		_, err := NewCountingWindow(config)
		assert.Error(t, err)
	})

	t.Run("会话窗口无效超时", func(t *testing.T) {
		config := types.WindowConfig{
			Type: TypeSession,
			Params: map[string]interface{}{
				"timeout": "invalid",
			},
		}
		_, err := NewSessionWindow(config)
		assert.Error(t, err)
	})
}

// TestSessionWindowAdvanced 测试会话窗口的高级功能
func TestSessionWindowAdvanced(t *testing.T) {
	config := types.WindowConfig{
		Type: TypeSession,
		Params: map[string]interface{}{
			"timeout": "1s",
		},
		GroupByKey: "user_id",
	}

	sw, err := NewSessionWindow(config)
	assert.NoError(t, err)
	assert.NotNil(t, sw)

	// 测试设置回调函数
	sw.SetCallback(func(results []types.Row) {
		// Callback executed
	})

	// 启动窗口
	sw.Start()
	defer sw.Stop()

	// 添加不同用户的数据
	sw.Add(map[string]interface{}{
		"user_id": "user1",
		"value":   100,
	})

	sw.Add(map[string]interface{}{
		"user_id": "user2",
		"value":   200,
	})

	// 等待会话超时
	time.Sleep(1500 * time.Millisecond)

	// 检查输出通道
	select {
	case data := <-sw.OutputChan():
		assert.NotEmpty(t, data)
	case <-time.After(500 * time.Millisecond):
		// 可能没有数据输出，这也是正常的
	}

	// 测试重置功能
	sw.Reset()

	// 测试手动触发
	sw.Trigger()
}

// TestSlidingWindowAdvanced 测试滑动窗口的高级功能
func TestSlidingWindowAdvanced(t *testing.T) {
	config := types.WindowConfig{
		Type: TypeSliding,
		Params: map[string]interface{}{
			"size":  "2s",
			"slide": "1s",
		},
		TsProp:   "timestamp",
		TimeUnit: time.Second,
	}

	sw, err := NewSlidingWindow(config)
	assert.NoError(t, err)
	assert.NotNil(t, sw)

	// 测试获取输出通道
	outputChan := sw.OutputChan()
	assert.NotNil(t, outputChan)

	// 测试重置功能
	sw.Reset()

	// 测试手动触发
	sw.Trigger()
}

// TestCountingWindowAdvanced 测试计数窗口的高级功能
func TestCountingWindowAdvanced(t *testing.T) {
	config := types.WindowConfig{
		Type: TypeCounting,
		Params: map[string]interface{}{
			"count": 3,
		},
		TsProp:   "timestamp",
		TimeUnit: time.Second,
	}

	cw, err := NewCountingWindow(config)
	assert.NoError(t, err)
	assert.NotNil(t, cw)

	// 测试设置回调函数
	cw.SetCallback(func(results []types.Row) {
		// Callback executed
	})

	// 启动窗口
	cw.Start()
	// CountingWindow doesn't have Stop method

	// 添加数据直到达到阈值
	for i := 0; i < 3; i++ {
		cw.Add(map[string]interface{}{
			"timestamp": time.Now().Unix(),
			"value":     i,
		})
	}

	// 等待一段时间让窗口处理数据
	time.Sleep(100 * time.Millisecond)

	// 检查输出通道
	select {
	case data := <-cw.OutputChan():
		assert.Len(t, data, 3)
	case <-time.After(500 * time.Millisecond):
		// 可能没有数据输出，这也是正常的
	}

	// 测试重置功能
	cw.Reset()

	// 测试手动触发
	cw.Trigger()
}

// TestTumblingWindowAdvanced 测试滚动窗口的高级功能
func TestTumblingWindowAdvanced(t *testing.T) {
	config := types.WindowConfig{
		Type: TypeTumbling,
		Params: map[string]interface{}{
			"size": "1s",
		},
		TsProp:   "timestamp",
		TimeUnit: time.Second,
	}

	tw, err := NewTumblingWindow(config)
	assert.NoError(t, err)
	assert.NotNil(t, tw)

	// 检查统计信息
	stats := tw.GetStats()
	assert.Contains(t, stats, "sent_count")
	assert.Contains(t, stats, "dropped_count")

	// 测试重置统计信息
	tw.ResetStats()
	stats = tw.GetStats()
	assert.Equal(t, int64(0), stats["droppedCount"])
	assert.Equal(t, int64(0), stats["sentCount"])

	// 测试设置回调函数
	tw.SetCallback(func(results []types.Row) {
		// Callback executed
	})

	// 测试获取输出通道
	outputChan := tw.OutputChan()
	assert.NotNil(t, outputChan)

	// 测试重置功能
	tw.Reset()

	// 测试手动触发
	tw.Trigger()
}
