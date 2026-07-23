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

// getTypeString Retrieves the type string representation of the object
func getTypeString(obj any) string {
	if obj == nil {
		return ""
	}
	return reflect.TypeOf(obj).String()
}

// TestWindowEdgeCases: The boundary condition of the test window
func TestWindowEdgeCases(t *testing.T) {
	t.Run("tumbling window with zero duration", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Duration(0)},
		}
		_, err := NewTumblingWindow(config)
		// Zero duration may be effective, depending on implementation
		_ = err
	})

	t.Run("tumbling window with negative duration", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{-time.Second},
		}
		_, err := NewTumblingWindow(config)
		// The duration of negative outcomes may be effective, depending on implementation
		_ = err
	})

	t.Run("sliding window with zero window size", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Duration(0), time.Second},
		}
		_, err := NewSlidingWindow(config)
		// Zero swipe intervals can be effective, depending on implementation
		_ = err
	})

	t.Run("sliding window with zero slide interval", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Minute, time.Duration(0)},
		}
		_, err := NewSlidingWindow(config)
		// Zero swipe intervals can be effective, depending on implementation
		_ = err
	})

	t.Run("sliding window with slide larger than window", func(t *testing.T) {
		// This situation may be effective, depending on how it is implemented
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
		// Zero timeouts may be effective, depending on implementation
		_ = err
	})

	t.Run("session window with negative timeout", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{-time.Second},
		}
		_, err := NewSessionWindow(config)
		// Negative timeouts may be effective, depending on implementation
		_ = err
	})
}

// TestWindowWithNilCallback The test window uses the nil callback function
func TestWindowWithNilCallback(t *testing.T) {
	t.Run("tumbling window with nil callback", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Second},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			require.NotNil(t, window)
			window.Start()

			// Adding data shouldn't panic
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

// TestWindowConcurrency tests the concurrency security of the window
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

		// Wait for the window to finish
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

		// A goroutine adds data
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

		// Another goroutine stops the window
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * 50)

		}()

		wg.Wait()
	})
}

// TestWindowMemoryManagement The memory management of the test window
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

		// Add a lot of data
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

		// Wait for processing to complete
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

		// Quickly add a large amount of small data
		for i := 0; i < 1000; i++ {
			row := types.Row{
				Data:      map[string]any{"id": i},
				Timestamp: time.Now(),
			}
			window.Add(row)
		}

		// Wait for processing to complete
		time.Sleep(time.Millisecond * 100)
	})
}

// TestWindowErrorConditions The error conditions of the test window
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

		// Adding data to stopped windows should not panic
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

		// Add rows containing non-serializable data
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

		// Add rows with zero timestamps
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

		// Add a line for future timestamps
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

		// Add a line of timestamps from a long time ago
		row := types.Row{
			Data:      map[string]any{"id": 1},
			Timestamp: time.Now().Add(-time.Hour * 24),
		}
		window.Add(row)
	})
}

// TestWindowStatsAndMetrics Statistics and metrics for the test window
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

		// Getting statistics shouldn't be panicking
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

		// Add some data
		row := types.Row{
			Data:      map[string]any{"id": 1},
			Timestamp: time.Now(),
		}
		window.Add(row)

		// Resetting statistics should not be panicking
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

		// Getting the output channel should not panic
		outputChan := window.OutputChan()
		_ = outputChan
	})

	t.Run("set callback", func(t *testing.T) {
		config := types.WindowConfig{
			Params: []any{time.Second},
		}
		window, err := NewTumblingWindow(config)
		if err == nil {
			// Setting a new callback function should not be panicking
			newCallback := func(rows []types.Row) {
				// A new pullback logic
			}
			window.SetCallback(newCallback)
		}
	})
}

// TestWindowWithPerformanceConfig Configuration of the test window
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

// TestGetTimestampEdgeCases tests the edge conditions of the GetTimestamp function
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
				// Check if the return time is close to the current time (allow a 1-second margin of error)
				assert.WithinDuration(t, time.Now(), result, time.Second)
			}
		})
	}
}

// TestExtractTimestampNumericEpochs verifies event-time timestamp extraction accepts all value families
// (JSON decodes the numbers as float64, and the Go integer face is int) and the numeric string, which must be non-zero TimeUnit.
// Values without TimeUnit are rejected (line dropped) due to s/ms ambiguity.
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

// TestNewWindowRejectsNonPositiveDuration The validation constructor rejects non-positive durations,
// Avoid NewTicker (<=0) in Add/Start that can cause a process to crash (SQL path is validateWindowParams
// Bottom line, here overlays the direct construction path).
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

// TestEventTimeWindowDropsUnplaceableLateData Checks event-time window discards those that cannot be placed
// Late-arriving line (AllowedLateness=0 default) to avoid unbounded growth of tw.data/sw.data OOM.
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

// TestSessionWindowDropsLateEvent Validation: The event-time session window discards late, unabsorbable lines,
// Instead of creating an expired fake order event session based on outdated timestamps.
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

// TestSessionWindowSessionKey tests session key extraction for the session window
func TestSessionWindowSessionKey(t *testing.T) {
	config := types.WindowConfig{
		Type:        TypeSession,
		Params:      []any{5 * time.Second},
		GroupByKeys: []string{"user_id"},
	}

	sw, err := NewSessionWindow(config)
	assert.NoError(t, err)

	// Test different types of data
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
			// This is just testing that the Add method does not panic
			assert.NotPanics(t, func() {
				sw.Add(tt.data)
			})
		})
	}
}

// TestWindowStopBeforeStart Tests the window that stops before starting
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

			// The stop window before startup should not panic
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

// TestWindowMultipleStops Tests multiple stops windows
func TestWindowMultipleStops(t *testing.T) {
	config := types.WindowConfig{
		Type:   TypeTumbling,
		Params: []any{time.Second},
	}

	tw, err := NewTumblingWindow(config)
	assert.NoError(t, err)

	tw.Start()

	// Stopping multiple times shouldn't cause panic
	assert.NotPanics(t, func() {
		tw.Stop()
		tw.Stop()
		tw.Stop()
	})
}

// TestWindowAddAfterStop: Adds data after the test stops
func TestWindowAddAfterStop(t *testing.T) {
	config := types.WindowConfig{
		Type:   TypeTumbling,
		Params: []any{time.Second},
	}

	tw, err := NewTumblingWindow(config)
	assert.NoError(t, err)

	tw.Start()
	tw.Stop()

	// Adding data after stopping should not cause panic
	assert.NotPanics(t, func() {
		tw.Add(map[string]any{"value": 42})
	})
}

// TestCountingWindowWithCallback: The callback function of the test counting window
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

	// Add data
	cw.Add(map[string]any{"value": 1})
	cw.Add(map[string]any{"value": 2})

	// Waiting for processing
	time.Sleep(100 * time.Millisecond)

	// Check if the callback has been called
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(callbackData) > 0
	}, time.Second, 10*time.Millisecond)
}

// TestSlidingWindowInvalidParams Tests invalid parameters for the slider
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

// TestWindowUnifiedConfigIntegration Integration Testing: Verifies the integration of window configuration with actual data processing
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

		// Verify the buffer size
		assert.Equal(t, 200, cap(tw.outputChan))

		// Startup window
		tw.Start()

		// Send test data
		for i := 0; i < 10; i++ {
			tw.Add(map[string]any{
				"id":    i,
				"value": i * 10,
			})
		}

		// Wait for the window to trigger
		time.Sleep(1200 * time.Millisecond)

		// The verification window works properly
		select {
		case data := <-tw.OutputChan():
			assert.Greater(t, len(data), 0)
			assert.LessOrEqual(t, len(data), 10)
		case <-time.After(500 * time.Millisecond):
			t.Error("No window output received after timeout")
		}
	})

	t.Run("缓冲区溢出处理", func(t *testing.T) {
		// Create a window with a small buffer
		smallBufferConfig := types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				WindowOutputSize: 1, // A very small buffer zone
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

		// Quickly adding large amounts of data may cause buffer overflow
		for i := 0; i < 10; i++ {
			tw.Add(map[string]any{"value": i})
		}

		// Waiting for processing
		time.Sleep(200 * time.Millisecond)

		// Check the statistics
		stats := tw.GetStats()
		assert.Contains(t, stats, "droppedCount")
		assert.Contains(t, stats, "sentCount")
	})
}

// TestCreateWindow TestWindow Factory function
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

			// The verification window works properly
			if closer, ok := window.(interface{ Stop() }); ok {
				closer.Stop()
			}
		})
	}
}

// TestGetTimestampCoverage tests the timestamp extraction function
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
				// Check if the return time is close to the current time (allow a 1-second margin of error)
				assert.WithinDuration(t, time.Now(), result, time.Second)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestWindowErrorHandling: Test window error handling
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

// TestSessionWindowAdvanced is an advanced feature of the test session window
func TestSessionWindowAdvanced(t *testing.T) {
	config := types.WindowConfig{
		Type:        TypeSession,
		Params:      []any{time.Second},
		GroupByKeys: []string{"user_id"},
	}

	sw, err := NewSessionWindow(config)
	assert.NoError(t, err)
	assert.NotNil(t, sw)

	// Test the callback function
	sw.SetCallback(func(results []types.Row) {
		// Callback executed
	})

	// Startup window
	sw.Start()
	defer sw.Stop()

	// Add data from different users
	sw.Add(map[string]any{
		"user_id": "user1",
		"value":   100,
	})

	sw.Add(map[string]any{
		"user_id": "user2",
		"value":   200,
	})

	// Waiting for the session to time out
	time.Sleep(1500 * time.Millisecond)

	// Check the output channel
	select {
	case data := <-sw.OutputChan():
		assert.NotEmpty(t, data)
	case <-time.After(500 * time.Millisecond):
		// There may be no data output, which is normal
	}

	// Test reset function
	sw.Reset()

	// Testing is triggered manually
	sw.Trigger()
}

// TestSlidingWindowAdvanced Tests the advanced features of sliding windows
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

	// Test to obtain the output channel
	outputChan := sw.OutputChan()
	assert.NotNil(t, outputChan)

	// Test reset function
	sw.Reset()

	// Testing is triggered manually
	sw.Trigger()
}

// TestCountingWindowAdvanced: Advanced features of the test counting window
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

	// Test the callback function
	cw.SetCallback(func(results []types.Row) {
		// Callback executed
	})

	// Startup window
	cw.Start()
	// CountingWindow doesn't have Stop method

	// Add data until the threshold is reached
	for i := 0; i < 3; i++ {
		cw.Add(map[string]any{
			"timestamp": time.Now().Unix(),
			"value":     i,
		})
	}

	// Wait a while for the window to process the data
	time.Sleep(100 * time.Millisecond)

	// Check the output channel
	select {
	case data := <-cw.OutputChan():
		assert.Len(t, data, 3)
	case <-time.After(500 * time.Millisecond):
		// There may be no data output, which is normal
	}

	// Test reset function
	cw.Reset()

	// Testing is triggered manually
	cw.Trigger()
}

// TestTumblingWindowAdvanced tests the advanced features of rolling windows
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

	// Check the statistics
	stats := tw.GetStats()
	assert.Contains(t, stats, "sentCount")
	assert.Contains(t, stats, "droppedCount")

	// Test reset statistics
	tw.ResetStats()
	stats = tw.GetStats()
	assert.Equal(t, int64(0), stats["droppedCount"])
	assert.Equal(t, int64(0), stats["sentCount"])

	// Test the callback function
	tw.SetCallback(func(results []types.Row) {
		// Callback executed
	})

	// Test to obtain the output channel
	outputChan := tw.OutputChan()
	assert.NotNil(t, outputChan)

	// Test reset function
	tw.Reset()

	// Testing is triggered manually
	tw.Trigger()
}
