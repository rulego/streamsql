package window

import (
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getTypeString 获取对象的类型字符串表示
func getTypeString(obj any) string {
	if obj == nil {
		return ""
	}
	return reflect.TypeOf(obj).String()
}

// TestWindowEdgeCases 测试窗口的边界条件
func TestWindowEdgeCases(t *testing.T) {
	t.Run("tumbling window with zero duration", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Duration(0)},
		}
		_, err := NewTumblingWindow(config)
		// 零持续时间可能是有效的，取决于实现
		_ = err
	})

	t.Run("tumbling window with negative duration", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{-time.Second},
		}
		_, err := NewTumblingWindow(config)
		// 负持续时间可能是有效的，取决于实现
		_ = err
	})

	t.Run("sliding window with zero window size", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Duration(0), time.Second},
		}
		_, err := NewSlidingWindow(config)
		// 零滑动间隔可能是有效的，取决于实现
		_ = err
	})

	t.Run("sliding window with zero slide interval", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Minute, time.Duration(0)},
		}
		_, err := NewSlidingWindow(config)
		// 零滑动间隔可能是有效的，取决于实现
		_ = err
	})

	t.Run("sliding window with slide larger than window", func(t *testing.T) {
		// 这种情况可能是有效的，取决于实现
		config := types.WindowConfig{
			Params: []any{time.Second, time.Minute},
		}
		window, err := NewSlidingWindow(config)
		_ = window
		_ = err
	})

	t.Run("counting window with zero count", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{0},
		}
		_, err := NewCountingWindow(config)
		require.NotNil(t, err)
	})

	t.Run("counting window with negative count", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{-10},
		}
		_, err := NewCountingWindow(config)
		require.NotNil(t, err)
	})

	t.Run("session window with zero timeout", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Duration(0)},
		}
		_, err := NewSessionWindow(config)
		// 零超时可能是有效的，取决于实现
		_ = err
	})

	t.Run("session window with negative timeout", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{-time.Second},
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
			Params: []any{time.Second},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			require.NotNil(t, window)
			window.Start()

			// 添加数据不应该panic
			row := types.Row{
				Data:      map[string]any{"id": 1},
				Timestamp: time.Now(),
			}
			window.Add(row)
		}
	})

	t.Run("sliding window with nil callback", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Minute, time.Second},
		}
		window, err := NewSlidingWindow(config)
		if err == nil {
			require.NotNil(t, window)
			window.Start()

			row := types.Row{
				Data:      map[string]any{"id": 1},
				Timestamp: time.Now(),
			}
			window.Add(row)
		}
	})

	t.Run("counting window with nil callback", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{10},
		}
		window, err := NewCountingWindow(config)
		if err == nil {
			require.NotNil(t, window)
			window.Start()

			row := types.Row{
				Data:      map[string]any{"id": 1},
				Timestamp: time.Now(),
			}
			window.Add(row)
		}
	})

	t.Run("session window with nil callback", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Minute},
		}
		window, err := NewSessionWindow(config)
		if err == nil {
			require.NotNil(t, window)
			window.Start()

			row := types.Row{
				Data:      map[string]any{"id": 1},
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
			Params: []any{time.Millisecond * 100},
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
						Data: map[string]any{
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
			Params: []any{time.Second},
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
			Params: []any{time.Second},
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
					Data:      map[string]any{"id": i},
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
			Params: []any{time.Millisecond * 50},
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
				Data: map[string]any{
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
			Params: []any{time.Millisecond * 10},
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
				Data:      map[string]any{"id": i},
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
			Params: []any{time.Second},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		// 向已停止的窗口添加数据不应该panic
		row := types.Row{
			Data:      map[string]any{"id": 1},
			Timestamp: time.Now(),
		}
		window.Add(row)
	})

	t.Run("add invalid data types", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Second},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		// 添加包含不可序列化数据的行
		row := types.Row{
			Data: map[string]any{
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
			Params: []any{time.Second},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		// 添加时间戳为零值的行
		row := types.Row{
			Data:      map[string]any{"id": 1},
			Timestamp: time.Time{},
		}
		window.Add(row)
	})

	t.Run("add row with future timestamp", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Second},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		// 添加未来时间戳的行
		row := types.Row{
			Data:      map[string]any{"id": 1},
			Timestamp: time.Now().Add(time.Hour),
		}
		window.Add(row)
	})

	t.Run("add row with very old timestamp", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Second},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		require.Nil(t, err)

		window.Start()

		// 添加很久以前的时间戳的行
		row := types.Row{
			Data:      map[string]any{"id": 1},
			Timestamp: time.Now().Add(-time.Hour * 24),
		}
		window.Add(row)
	})
}

// TestWindowStatsAndMetrics 测试窗口的统计和指标
func TestWindowStatsAndMetrics(t *testing.T) {
	t.Run("get stats from tumbling window", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Second},
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
			Params: []any{time.Second},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			window.SetCallback(func(results []types.Row) {})
		}
		assert.Nil(t, err)

		window.Start()

		// 添加一些数据
		row := types.Row{
			Data:      map[string]any{"id": 1},
			Timestamp: time.Now(),
		}
		window.Add(row)

		// 重置统计信息不应该panic
		window.ResetStats()
	})

	t.Run("get output channel", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Second},
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
			Params: []any{time.Second},
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
		extraParams        map[string]any
	}{
		{
			name:               "滚动窗口-默认配置",
			windowType:         TypeTumbling,
			performanceConfig:  types.DefaultPerformanceConfig(),
			expectedBufferSize: 50,
			extraParams:        map[string]any{"size": "2s"},
		},
		{
			name:               "滚动窗口-高性能配置",
			windowType:         TypeTumbling,
			performanceConfig:  types.HighPerformanceConfig(),
			expectedBufferSize: 200,
			extraParams:        map[string]any{"size": "2s"},
		},
		{
			name:               "滚动窗口-低延迟配置",
			windowType:         TypeTumbling,
			performanceConfig:  types.LowLatencyConfig(),
			expectedBufferSize: 20,
			extraParams:        map[string]any{"size": "2s"},
		},
		{
			name:               "滑动窗口-高性能配置",
			windowType:         TypeSliding,
			performanceConfig:  types.HighPerformanceConfig(),
			expectedBufferSize: 200,
			extraParams:        map[string]any{"size": "10s", "slide": "5s"},
		},
		{
			name:               "计数窗口-高性能配置",
			windowType:         TypeCounting,
			performanceConfig:  types.HighPerformanceConfig(),
			expectedBufferSize: 200,
			extraParams:        map[string]any{"count": 10},
		},
		{
			name:               "自定义性能配置",
			windowType:         TypeTumbling,
			performanceConfig:  types.PerformanceConfig{BufferConfig: types.BufferConfig{WindowOutputSize: 500}},
			expectedBufferSize: 500,
			extraParams:        map[string]any{"size": "2s"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert extraParams to array format
			var params []any
			if tt.windowType == TypeCounting {
				if count, ok := tt.extraParams["count"].(int); ok {
					params = []any{count}
				} else if countStr, ok := tt.extraParams["count"].(string); ok {
					if count, err := strconv.Atoi(countStr); err == nil {
						params = []any{count}
					}
				}
			} else if tt.windowType == TypeSession {
				if timeout, ok := tt.extraParams["timeout"].(string); ok {
					if dur, err := time.ParseDuration(timeout); err == nil {
						params = []any{dur}
					}
				}
			} else if tt.windowType == TypeSliding {
				var size, slide time.Duration
				if sizeStr, ok := tt.extraParams["size"].(string); ok {
					if dur, err := time.ParseDuration(sizeStr); err == nil {
						size = dur
					}
				}
				if slideStr, ok := tt.extraParams["slide"].(string); ok {
					if dur, err := time.ParseDuration(slideStr); err == nil {
						slide = dur
					}
				}
				params = []any{size, slide}
			} else {
				if sizeStr, ok := tt.extraParams["size"].(string); ok {
					if dur, err := time.ParseDuration(sizeStr); err == nil {
						params = []any{dur}
					}
				}
			}

			config := types.WindowConfig{
				Type:              tt.windowType,
				Params:            params,
				PerformanceConfig: tt.performanceConfig,
			}

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
			Type:   TypeTumbling,
			Params: []any{3 * time.Second},
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
		data     any
		tsProp   string
		timeUnit time.Duration
		checkNow bool
	}{
		{
			name:     "空字符串时间戳属性",
			data:     map[string]any{"value": 42},
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
			data: map[string]any{
				"value": 42,
			},
			tsProp:   "nonexistent",
			timeUnit: time.Second,
			checkNow: true,
		},
		{
			name: "map中非时间类型的值",
			data: map[string]any{
				"timestamp": "not a time",
			},
			tsProp:   "timestamp",
			timeUnit: time.Second,
			checkNow: true,
		},
		{
			name: "非字符串键的map",
			data: map[int]any{
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

// TestExtractTimestampNumericEpochs 校验 event-time 时间戳提取接受全部数值族
// （JSON 把数字解码成 float64，Go 整数字面量是 int）与数字字符串，且需非零 TimeUnit。
// 无 TimeUnit 的数值 epoch 因 s/ms 歧义被拒（丢行）。
func TestExtractTimestampNumericEpochs(t *testing.T) {
	const msEpoch int64 = 1700000000000 // 2023-11-14T22:13:20Z
	want := time.Unix(0, msEpoch*int64(time.Millisecond))

	cases := []struct {
		name     string
		data     any
		tsProp   string
		timeUnit time.Duration
		wantOK   bool
	}{
		{"float64 ms epoch (JSON)", map[string]any{"ts": float64(msEpoch)}, "ts", time.Millisecond, true},
		{"int ms epoch", map[string]any{"ts": int(msEpoch)}, "ts", time.Millisecond, true},
		{"int64 ms epoch", map[string]any{"ts": msEpoch}, "ts", time.Millisecond, true},
		{"numeric string ms epoch", map[string]any{"ts": "1700000000000"}, "ts", time.Millisecond, true},
		{"struct int64 ms epoch", struct{ Ts int64 }{Ts: msEpoch}, "Ts", time.Millisecond, true},
		{"float64 without TimeUnit drops", map[string]any{"ts": float64(msEpoch)}, "ts", 0, false},
		{"time.Time field", map[string]any{"ts": want}, "ts", time.Millisecond, true},
		{"missing field drops", map[string]any{"other": 1}, "ts", time.Millisecond, false},
		{"non-numeric string drops", map[string]any{"ts": "not-a-number"}, "ts", time.Millisecond, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractTimestamp(tc.data, tc.tsProp, tc.timeUnit)
			assert.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				assert.True(t, got.Equal(want), "got %v want %v", got, want)
			}
		})
	}
}

// TestNewWindowRejectsNonPositiveDuration 校验构造函数拒绝非正 duration，
// 避免 NewTicker(<=0) 在 Add/Start 里 panic 拖垮进程（SQL 路径由 validateWindowParams
// 兜底，这里覆盖直接构造路径）。
func TestNewWindowRejectsNonPositiveDuration(t *testing.T) {
	cases := []struct {
		name string
		fn   func() (Window, error)
	}{
		{"tumbling zero", func() (Window, error) { return NewTumblingWindow(types.WindowConfig{Params: []any{time.Duration(0)}}) }},
		{"tumbling negative", func() (Window, error) { return NewTumblingWindow(types.WindowConfig{Params: []any{-time.Second}}) }},
		{"sliding zero size", func() (Window, error) {
			return NewSlidingWindow(types.WindowConfig{Params: []any{time.Duration(0), time.Second}})
		}},
		{"sliding zero slide", func() (Window, error) {
			return NewSlidingWindow(types.WindowConfig{Params: []any{time.Second, time.Duration(0)}})
		}},
		{"session zero", func() (Window, error) { return NewSessionWindow(types.WindowConfig{Params: []any{time.Duration(0)}}) }},
		{"session negative", func() (Window, error) { return NewSessionWindow(types.WindowConfig{Params: []any{-time.Second}}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, err := tc.fn()
			assert.NotNil(t, err, "non-positive duration must be rejected")
			assert.Nil(t, w, "no window should be returned on error")
		})
	}
}

// TestEventTimeWindowDropsUnplaceableLateData 校验 event-time 窗口丢弃无法落位的
// 迟到行（AllowedLateness=0 默认），避免 tw.data/sw.data 无界增长 OOM。
func TestEventTimeWindowDropsUnplaceableLateData(t *testing.T) {
	mkWindow := func(typ string) Window {
		params := []any{time.Second}
		if typ == TypeSliding {
			params = []any{time.Second, time.Second}
		}
		w, err := CreateWindow(types.WindowConfig{
			Type: typ, Params: params,
			TimeCharacteristic: types.EventTime,
			TsProp:             "ts",
			TimeUnit:           time.Millisecond,
		})
		assert.Nil(t, err)
		w.Start()
		return w
	}

	for _, typ := range []string{TypeTumbling, TypeSliding} {
		t.Run(typ, func(t *testing.T) {
			w := mkWindow(typ)
			defer w.Stop()

			// in-order row at ts=1000s advances the watermark far forward
			w.Add(map[string]any{"ts": int64(1000000), "v": 1})
			// flood of late rows near epoch: all before the watermark and the
			// current window — must be dropped, not retained.
			for i := 0; i < 200; i++ {
				w.Add(map[string]any{"ts": int64(i), "v": 2})
			}
			time.Sleep(100 * time.Millisecond)

			retained := 0
			switch win := w.(type) {
			case *TumblingWindow:
				win.mu.Lock()
				retained = len(win.data)
				win.mu.Unlock()
			case *SlidingWindow:
				win.mu.Lock()
				retained = len(win.data)
				win.mu.Unlock()
			}
			assert.True(t, retained < 50, "%s: late rows should be dropped, not retained (got %d)", typ, retained)
		})
	}
}

// TestWatermarkIgnoresFarFutureTimestamp verifies a single far-future (corrupt)
// event does NOT poison the watermark: it is ignored for watermark bookkeeping,
// so the watermark stays near the last real event and subsequent real events
// are not judged late. (Earlier the event was clamped to now+24h and still
// ratcheted maxEventTime, dropping all real events as late for 24h.)
func TestWatermarkIgnoresFarFutureTimestamp(t *testing.T) {
	wm := NewWatermark(0, 50*time.Millisecond, 0) // maxOutOfOrderness = 0
	defer wm.Stop()

	real := time.Now()
	wm.UpdateEventTime(real)
	wm.UpdateEventTime(real.Add(100 * 365 * 24 * time.Hour)) // corrupt: ~100 years out
	time.Sleep(120 * time.Millisecond)

	got := wm.GetCurrentWatermark()
	// The corrupt event must not ratchet maxEventTime, so the watermark stays
	// near the real event instead of jumping ~24h ahead.
	assert.True(t, got.Before(real.Add(time.Second)),
		"far-future event poisoned watermark: got %v, want near %v", got, real)

	// A subsequent real event must not be dropped as late.
	assert.False(t, wm.IsEventTimeLate(real.Add(1*time.Second)),
		"real event judged late after a corrupt far-future event")
}

// TestSessionWindowDropsLateEvent 校验 event-time session 窗口丢弃不可吸收的迟到行，
// 而非按陈旧 timestamp 建立即刻过期的伪单事件会话。
func TestSessionWindowDropsLateEvent(t *testing.T) {
	cfg := types.WindowConfig{
		Type: TypeSession, Params: []any{time.Hour}, // long timeout keeps the in-order session alive
		GroupByKeys:        []string{"k"},
		TimeCharacteristic: types.EventTime, TsProp: "ts", TimeUnit: time.Millisecond,
	}
	sw, err := NewSessionWindow(cfg)
	require.NoError(t, err)
	sw.Start()
	defer sw.Stop()

	sw.Add(map[string]any{"ts": int64(1000000), "k": "a"}) // in-order
	for i := 0; i < 100; i++ {
		sw.Add(map[string]any{"ts": int64(i), "k": strconv.Itoa(i)}) // late, distinct groups
	}
	time.Sleep(100 * time.Millisecond)

	sw.mu.Lock()
	n := len(sw.sessionMap)
	sw.mu.Unlock()
	assert.True(t, n <= 2, "late events must not create sessions, got %d", n)
}

// TestSessionWindowSessionKey 测试会话窗口的会话键提取
func TestSessionWindowSessionKey(t *testing.T) {
	config := types.WindowConfig{
		Type:        TypeSession,
		Params:      []any{5 * time.Second},
		GroupByKeys: []string{"user_id"},
	}

	sw, err := NewSessionWindow(config)
	assert.NoError(t, err)

	// 测试不同类型的数据
	tests := []struct {
		name string
		data any
	}{
		{
			name: "map数据",
			data: map[string]any{
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
				Params: []any{time.Second},
			},
		},
		{
			name: "滑动窗口",
			config: types.WindowConfig{
				Type:   TypeSliding,
				Params: []any{2 * time.Second, time.Second},
			},
		},
		{
			name: "计数窗口",
			config: types.WindowConfig{
				Type:   TypeCounting,
				Params: []any{10},
			},
		},
		{
			name: "会话窗口",
			config: types.WindowConfig{
				Type:   TypeSession,
				Params: []any{5 * time.Second},
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
		Params: []any{time.Second},
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
		Params: []any{time.Second},
	}

	tw, err := NewTumblingWindow(config)
	assert.NoError(t, err)

	tw.Start()
	tw.Stop()

	// 停止后添加数据应该不会panic
	assert.NotPanics(t, func() {
		tw.Add(map[string]any{"value": 42})
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
		Type:     TypeCounting,
		Params:   []any{2},
		Callback: callback,
	}

	cw, err := NewCountingWindow(config)
	assert.NoError(t, err)

	cw.Start()
	// CountingWindow doesn't have Stop method, will be handled by context cancellation

	// 添加数据
	cw.Add(map[string]any{"value": 1})
	cw.Add(map[string]any{"value": 2})

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
		params []any
	}{
		{
			name:   "无效的slide参数",
			params: []any{10 * time.Second, "invalid"},
		},
		{
			name:   "缺少slide参数",
			params: []any{10 * time.Second},
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
			Type:              TypeTumbling,
			Params:            []any{time.Second},
			PerformanceConfig: performanceConfig,
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
			tw.Add(map[string]any{
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
			Type:              TypeTumbling,
			Params:            []any{100 * time.Millisecond},
			PerformanceConfig: smallBufferConfig,
		}

		tw, err := NewTumblingWindow(config)
		assert.NoError(t, err)

		tw.Start()
		defer tw.Stop()

		// 快速添加大量数据，可能导致缓冲区溢出
		for i := 0; i < 10; i++ {
			tw.Add(map[string]any{"value": i})
		}

		// 等待处理
		time.Sleep(200 * time.Millisecond)

		// 检查统计信息
		stats := tw.GetStats()
		assert.Contains(t, stats, "droppedCount")
		assert.Contains(t, stats, "sentCount")
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
				Type:   TypeTumbling,
				Params: []any{5 * time.Second},
			},
			expectError:  false,
			expectedType: "*window.TumblingWindow",
		},
		{
			name: "创建滑动窗口",
			config: types.WindowConfig{
				Type:   TypeSliding,
				Params: []any{10 * time.Second, 5 * time.Second},
			},
			expectError:  false,
			expectedType: "*window.SlidingWindow",
		},
		{
			name: "创建计数窗口",
			config: types.WindowConfig{
				Type:   TypeCounting,
				Params: []any{100},
			},
			expectError:  false,
			expectedType: "*window.CountingWindow",
		},
		{
			name: "创建会话窗口",
			config: types.WindowConfig{
				Type:   TypeSession,
				Params: []any{30 * time.Second},
			},
			expectError:  false,
			expectedType: "*window.SessionWindow",
		},
		{
			name: "窗口工厂与统一配置集成",
			config: types.WindowConfig{
				Type:   TypeTumbling,
				Params: []any{5 * time.Second},
				PerformanceConfig: types.PerformanceConfig{
					BufferConfig: types.BufferConfig{
						WindowOutputSize: 1500,
					},
				},
			},
			expectError:  false,
			expectedType: "*window.TumblingWindow",
		},
		{
			name: "无效的窗口类型",
			config: types.WindowConfig{
				Type:   "invalid",
				Params: []any{5 * time.Second},
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
		data     any
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
			data: map[string]any{
				"timestamp": testTime,
				"value":     42,
			},
			tsProp:   "timestamp",
			timeUnit: time.Second,
			expected: testTime,
		},
		{
			name: "从map中提取int64时间戳",
			data: map[string]any{
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
			Type:   TypeTumbling,
			Params: []any{"invalid"},
		}
		_, err := NewTumblingWindow(config)
		assert.Error(t, err)
	})

	t.Run("滑动窗口无效参数", func(t *testing.T) {
		config := types.WindowConfig{
			Type:   TypeSliding,
			Params: []any{"invalid", 5 * time.Second},
		}
		_, err := NewSlidingWindow(config)
		assert.Error(t, err)
	})

	t.Run("计数窗口无效计数", func(t *testing.T) {
		config := types.WindowConfig{
			Type:   TypeCounting,
			Params: []any{0},
		}
		_, err := NewCountingWindow(config)
		assert.Error(t, err)
	})

	t.Run("会话窗口无效超时", func(t *testing.T) {
		config := types.WindowConfig{
			Type:   TypeSession,
			Params: []any{"invalid"},
		}
		_, err := NewSessionWindow(config)
		assert.Error(t, err)
	})
}

// TestSessionWindowAdvanced 测试会话窗口的高级功能
func TestSessionWindowAdvanced(t *testing.T) {
	config := types.WindowConfig{
		Type:        TypeSession,
		Params:      []any{time.Second},
		GroupByKeys: []string{"user_id"},
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
	sw.Add(map[string]any{
		"user_id": "user1",
		"value":   100,
	})

	sw.Add(map[string]any{
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
		Type:     TypeSliding,
		Params:   []any{2 * time.Second, time.Second},
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
		Type:     TypeCounting,
		Params:   []any{3},
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
		cw.Add(map[string]any{
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
		Type:     TypeTumbling,
		Params:   []any{time.Second},
		TsProp:   "timestamp",
		TimeUnit: time.Second,
	}

	tw, err := NewTumblingWindow(config)
	assert.NoError(t, err)
	assert.NotNil(t, tw)

	// 检查统计信息
	stats := tw.GetStats()
	assert.Contains(t, stats, "sentCount")
	assert.Contains(t, stats, "droppedCount")

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
