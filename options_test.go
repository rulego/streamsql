/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streamsql

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/schema"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWithLogLevel 测试日志级别设置选项
func TestWithLogLevel(t *testing.T) {
	t.Run("设置Debug级别", func(t *testing.T) {
		s := New(WithLogLevel(logger.DEBUG))

		// 验证选项函数执行成功（通过检查没有panic来验证）
		assert.NotNil(t, s)
	})

	t.Run("设置Info级别", func(t *testing.T) {
		s := New(WithLogLevel(logger.INFO))

		assert.NotNil(t, s)
	})

	t.Run("设置Error级别", func(t *testing.T) {
		s := New(WithLogLevel(logger.ERROR))

		assert.NotNil(t, s)
	})
}

// TestWithDiscardLog 测试禁用日志输出选项
func TestWithDiscardLog(t *testing.T) {
	t.Run("禁用日志输出", func(t *testing.T) {
		s := New(WithDiscardLog())

		// 验证日志器是否被设置为丢弃日志器
		// 这里我们检查日志器的类型或行为
		loggerInstance := logger.GetDefault()
		assert.NotNil(t, loggerInstance)

		// 验证选项函数执行成功
		assert.NotNil(t, s)
	})
}

// TestWithHighPerformance 测试高性能配置选项
func TestWithHighPerformance(t *testing.T) {
	t.Run("设置高性能模式", func(t *testing.T) {
		s := New(WithHighPerformance())

		assert.Equal(t, "high_performance", s.performanceMode)
	})
}

// TestWithLowLatency 测试低延迟配置选项
func TestWithLowLatency(t *testing.T) {
	t.Run("设置低延迟模式", func(t *testing.T) {
		s := New(WithLowLatency())

		assert.Equal(t, "low_latency", s.performanceMode)
	})
}

// TestWithCustomPerformance 测试自定义性能配置选项
func TestWithCustomPerformance(t *testing.T) {
	t.Run("设置自定义性能配置", func(t *testing.T) {
		customConfig := types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize:   2000,
				ResultChannelSize: 300,
				WindowOutputSize:  100,
			},
			OverflowConfig: types.OverflowConfig{
				Strategy:      "expand",
				AllowDataLoss: false,
			},
		}

		s := New(WithCustomPerformance(customConfig))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, 2000, s.customConfig.BufferConfig.DataChannelSize)
		assert.Equal(t, 300, s.customConfig.BufferConfig.ResultChannelSize)
		assert.Equal(t, "expand", s.customConfig.OverflowConfig.Strategy)
	})
}

// TestWithBufferSizes 测试缓冲区大小配置选项
func TestWithBufferSizes(t *testing.T) {
	t.Run("设置自定义缓冲区大小", func(t *testing.T) {
		dataChannelSize := 1500
		resultChannelSize := 200
		windowOutputSize := 80

		s := New(WithBufferSizes(dataChannelSize, resultChannelSize, windowOutputSize))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, dataChannelSize, s.customConfig.BufferConfig.DataChannelSize)
		assert.Equal(t, resultChannelSize, s.customConfig.BufferConfig.ResultChannelSize)
		assert.Equal(t, windowOutputSize, s.customConfig.BufferConfig.WindowOutputSize)
	})

	t.Run("设置零值缓冲区大小", func(t *testing.T) {
		s := New(WithBufferSizes(0, 0, 0))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, 0, s.customConfig.BufferConfig.DataChannelSize)
		assert.Equal(t, 0, s.customConfig.BufferConfig.ResultChannelSize)
		assert.Equal(t, 0, s.customConfig.BufferConfig.WindowOutputSize)
	})
}

// TestWithOverflowStrategy 测试溢出策略配置选项
func TestWithOverflowStrategy(t *testing.T) {
	t.Run("设置drop策略", func(t *testing.T) {
		strategy := "drop"
		blockTimeout := 5 * time.Second

		s := New(WithOverflowStrategy(strategy, blockTimeout))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, strategy, s.customConfig.OverflowConfig.Strategy)
		assert.Equal(t, blockTimeout, s.customConfig.OverflowConfig.BlockTimeout)
		assert.True(t, s.customConfig.OverflowConfig.AllowDataLoss)
	})

	t.Run("设置block策略", func(t *testing.T) {
		strategy := "block"
		blockTimeout := 10 * time.Second

		s := New(WithOverflowStrategy(strategy, blockTimeout))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, strategy, s.customConfig.OverflowConfig.Strategy)
		assert.Equal(t, blockTimeout, s.customConfig.OverflowConfig.BlockTimeout)
		assert.False(t, s.customConfig.OverflowConfig.AllowDataLoss)
	})

	t.Run("设置expand策略", func(t *testing.T) {
		strategy := "expand"
		blockTimeout := 3 * time.Second

		s := New(WithOverflowStrategy(strategy, blockTimeout))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, strategy, s.customConfig.OverflowConfig.Strategy)
		assert.Equal(t, blockTimeout, s.customConfig.OverflowConfig.BlockTimeout)
		assert.False(t, s.customConfig.OverflowConfig.AllowDataLoss)
	})
}

// TestWithWorkerConfig 测试工作池配置选项
func TestWithWorkerConfig(t *testing.T) {
	t.Run("设置自定义工作池配置", func(t *testing.T) {
		sinkPoolSize := 10
		sinkWorkerCount := 5
		maxRetryRoutines := 15

		s := New(WithWorkerConfig(sinkPoolSize, sinkWorkerCount, maxRetryRoutines))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, sinkPoolSize, s.customConfig.WorkerConfig.SinkPoolSize)
		assert.Equal(t, sinkWorkerCount, s.customConfig.WorkerConfig.SinkWorkerCount)
		assert.Equal(t, maxRetryRoutines, s.customConfig.WorkerConfig.MaxRetryRoutines)
	})

	t.Run("设置零值工作池配置", func(t *testing.T) {
		s := New(WithWorkerConfig(0, 0, 0))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, 0, s.customConfig.WorkerConfig.SinkPoolSize)
		assert.Equal(t, 0, s.customConfig.WorkerConfig.SinkWorkerCount)
		assert.Equal(t, 0, s.customConfig.WorkerConfig.MaxRetryRoutines)
	})
}

// TestWithMonitoring 测试监控配置选项
func TestWithMonitoring(t *testing.T) {
	t.Run("启用详细监控", func(t *testing.T) {
		updateInterval := 5 * time.Second
		enableDetailedStats := true

		s := New(WithMonitoring(updateInterval, enableDetailedStats))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.True(t, s.customConfig.MonitoringConfig.EnableMonitoring)
		assert.Equal(t, updateInterval, s.customConfig.MonitoringConfig.StatsUpdateInterval)
		assert.Equal(t, enableDetailedStats, s.customConfig.MonitoringConfig.EnableDetailedStats)
	})

	t.Run("启用基础监控", func(t *testing.T) {
		updateInterval := 30 * time.Second
		enableDetailedStats := false

		s := New(WithMonitoring(updateInterval, enableDetailedStats))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.True(t, s.customConfig.MonitoringConfig.EnableMonitoring)
		assert.Equal(t, updateInterval, s.customConfig.MonitoringConfig.StatsUpdateInterval)
		assert.Equal(t, enableDetailedStats, s.customConfig.MonitoringConfig.EnableDetailedStats)
	})
}

// TestOptionsCombination 测试选项组合使用
func TestOptionsCombination(t *testing.T) {
	t.Run("组合多个选项", func(t *testing.T) {
		// 应用多个选项
		s := New(
			WithHighPerformance(),
			WithBufferSizes(3000, 400, 150),
			WithOverflowStrategy("expand", 8*time.Second),
			WithMonitoring(10*time.Second, true),
		)

		// 验证最后应用的选项生效（WithMonitoring是最后一个，会覆盖前面的自定义配置）
		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		// 由于WithMonitoring是最后应用的，它会重置配置为默认值，然后只设置监控相关配置
		assert.Equal(t, 1000, s.customConfig.BufferConfig.DataChannelSize) // 默认值
		assert.Equal(t, "drop", s.customConfig.OverflowConfig.Strategy)    // 默认值
		assert.True(t, s.customConfig.MonitoringConfig.EnableMonitoring)
	})

	t.Run("预设模式后应用自定义选项", func(t *testing.T) {
		// 先设置预设模式，再应用自定义选项
		s := New(
			WithLowLatency(),
			WithBufferSizes(500, 60, 25),
		)

		// 验证最后应用的选项生效（custom模式覆盖low_latency模式）
		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, 500, s.customConfig.BufferConfig.DataChannelSize)
	})
}

// TestOptionsEdgeCases 测试选项边界情况
func TestOptionsEdgeCases(t *testing.T) {
	t.Run("空字符串策略", func(t *testing.T) {
		s := New(WithOverflowStrategy("", 0))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, "", s.customConfig.OverflowConfig.Strategy)
		assert.Equal(t, time.Duration(0), s.customConfig.OverflowConfig.BlockTimeout)
	})

	t.Run("负数缓冲区大小", func(t *testing.T) {
		s := New(WithBufferSizes(-100, -50, -25))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.Equal(t, -100, s.customConfig.BufferConfig.DataChannelSize)
		assert.Equal(t, -50, s.customConfig.BufferConfig.ResultChannelSize)
		assert.Equal(t, -25, s.customConfig.BufferConfig.WindowOutputSize)
	})

	t.Run("零时间间隔监控", func(t *testing.T) {
		s := New(WithMonitoring(0, false))

		assert.Equal(t, "custom", s.performanceMode)
		assert.NotNil(t, s.customConfig)
		assert.True(t, s.customConfig.MonitoringConfig.EnableMonitoring)
		assert.Equal(t, time.Duration(0), s.customConfig.MonitoringConfig.StatsUpdateInterval)
		assert.False(t, s.customConfig.MonitoringConfig.EnableDetailedStats)
	})
}

// captureLogger records formatted messages for assertion.
type captureLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (c *captureLogger) Debug(format string, args ...any) { c.record("DEBUG", format, args...) }
func (c *captureLogger) Info(format string, args ...any)  { c.record("INFO", format, args...) }
func (c *captureLogger) Warn(format string, args ...any)  { c.record("WARN", format, args...) }
func (c *captureLogger) Error(format string, args ...any) { c.record("ERROR", format, args...) }
func (c *captureLogger) SetLevel(logger.Level)            {}

func (c *captureLogger) record(level, format string, args ...any) {
	c.mu.Lock()
	c.msgs = append(c.msgs, level+":"+fmt.Sprintf(format, args...))
	c.mu.Unlock()
}

// TestWithLogger 验证 WithLogger 按实例路由引擎日志（实例间不串扰）。
func TestWithLogger(t *testing.T) {
	sch := schema.Schema{Fields: []schema.FieldDef{{Name: "v", Type: schema.TypeInt, Required: true}}}

	t.Run("路由到自定义 logger", func(t *testing.T) {
		cap := &captureLogger{}
		s := New(WithLogger(cap), WithSchema(sch))
		defer s.Stop()
		require.NoError(t, s.Execute("SELECT v FROM stream"))

		s.Emit(map[string]any{"v": "bad"}) // schema fail -> s.log.Warn (synchronous)

		cap.mu.Lock()
		defer cap.mu.Unlock()
		require.NotEmpty(t, cap.msgs, "engine warn should route to the instance logger")
		assert.Contains(t, cap.msgs[0], "schema validation failed")
	})

	t.Run("nil 保持默认", func(t *testing.T) {
		prev := logger.GetDefault()
		s := New(WithLogger(nil))
		defer s.Stop()
		assert.Same(t, prev, s.log, "nil keeps the process default")
	})

	t.Run("按实例无串扰", func(t *testing.T) {
		capA, capB := &captureLogger{}, &captureLogger{}
		a := New(WithLogger(capA), WithSchema(sch))
		defer a.Stop()
		require.NoError(t, a.Execute("SELECT v FROM stream"))
		b := New(WithLogger(capB), WithSchema(sch))
		defer b.Stop()
		require.NoError(t, b.Execute("SELECT v FROM stream"))

		a.Emit(map[string]any{"v": "bad"}) // -> capA only

		capA.mu.Lock()
		capB.mu.Lock()
		defer capA.mu.Unlock()
		defer capB.mu.Unlock()
		assert.NotEmpty(t, capA.msgs, "A's logger got A's warn")
		assert.Empty(t, capB.msgs, "B's logger must not get A's warn (no cross-talk)")
	})
}
