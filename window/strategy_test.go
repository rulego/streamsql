package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOverflowStrategies 测试不同的缓冲区溢出策略
func TestOverflowStrategies(t *testing.T) {
	t.Run("CountingWindow_StrategyBlock_Timeout", func(t *testing.T) {
		// 配置：窗口大小1（每1条数据触发一次），输出缓冲1，阻塞策略，超时100ms
		config := types.WindowConfig{
			Type:   "CountingWindow",
			Params: []interface{}{1}, // Threshold = 1
			PerformanceConfig: types.PerformanceConfig{
				BufferConfig: types.BufferConfig{
					WindowOutputSize: 1,
				},
				OverflowConfig: types.OverflowConfig{
					Strategy:      types.OverflowStrategyBlock,
					BlockTimeout:  100 * time.Millisecond,
					AllowDataLoss: true, // 允许丢弃统计
				},
			},
		}

		win, err := NewCountingWindow(config)
		require.NoError(t, err)
		win.Start()
		defer win.Stop()

		// 1. 发送第1条数据，触发窗口，填充 outputChan (容量1)
		win.Add(map[string]interface{}{"id": 1})

		// 等待处理
		time.Sleep(50 * time.Millisecond)
		stats := win.GetStats()
		assert.Equal(t, int64(1), stats["sentCount"])
		assert.Equal(t, int64(1), stats["bufferUsed"]) // 应该还在缓冲区中，因为没人读

		// 2. 发送第2条数据，触发窗口
		// 此时 outputChan 已满，sendResult 应该阻塞 100ms 然后超时丢弃
		win.Add(map[string]interface{}{"id": 2})

		// 等待超时 (100ms) + 处理时间
		time.Sleep(200 * time.Millisecond)

		stats = win.GetStats()
		// 第1条仍在缓冲区（因为没人读）
		// 第2条因为阻塞超时被丢弃
		assert.Equal(t, int64(1), stats["bufferUsed"])
		assert.Equal(t, int64(1), stats["droppedCount"])

		// 3. 读取缓冲区中的数据，腾出空间
		select {
		case <-win.OutputChan():
			// 读出第1条
		default:
			t.Fatal("expected data in output channel")
		}

		// 4. 发送第3条数据
		win.Add(map[string]interface{}{"id": 3})
		time.Sleep(50 * time.Millisecond)

		stats = win.GetStats()
		assert.Equal(t, int64(2), stats["sentCount"])    // 第1条和第3条发送成功
		assert.Equal(t, int64(1), stats["droppedCount"]) // 第2条丢弃
	})

	t.Run("SessionWindow_StrategyBlock_Timeout", func(t *testing.T) {
		// 配置：会话超时50ms，输出缓冲1，阻塞策略，超时50ms
		config := types.WindowConfig{
			Type:   "SessionWindow",
			Params: []interface{}{"50ms"},
			PerformanceConfig: types.PerformanceConfig{
				BufferConfig: types.BufferConfig{
					WindowOutputSize: 1,
				},
				OverflowConfig: types.OverflowConfig{
					Strategy:      types.OverflowStrategyBlock,
					BlockTimeout:  50 * time.Millisecond,
					AllowDataLoss: true,
				},
			},
		}

		win, err := NewSessionWindow(config)
		require.NoError(t, err)
		win.Start()
		defer win.Stop()

		// 1. 发送数据，开始一个 session
		win.Add(map[string]interface{}{"id": 1})

		// 2. 等待 session 超时 (50ms) + 检查周期 (timeout/2 = 25ms)
		// 确保 session 被触发并发送到 outputChan
		time.Sleep(100 * time.Millisecond)

		stats := win.GetStats()
		assert.Equal(t, int64(1), stats["sentCount"])
		assert.Equal(t, int64(1), stats["bufferUsed"])

		// 3. 发送数据开始第二个 session (因为上一个已经结束)
		win.Add(map[string]interface{}{"id": 2})

		// 4. 等待 session 超时
		// 此时 outputChan 已满，应该阻塞并丢弃
		time.Sleep(150 * time.Millisecond)

		stats = win.GetStats()
		assert.Equal(t, int64(1), stats["bufferUsed"])
		assert.Equal(t, int64(1), stats["droppedCount"])
	})

	t.Run("CountingWindow_StrategyDrop", func(t *testing.T) {
		// 配置：窗口大小1，输出缓冲1，丢弃策略
		config := types.WindowConfig{
			Type:   "CountingWindow",
			Params: []interface{}{1},
			PerformanceConfig: types.PerformanceConfig{
				BufferConfig: types.BufferConfig{
					WindowOutputSize: 1,
				},
				OverflowConfig: types.OverflowConfig{
					Strategy: types.OverflowStrategyDrop,
				},
			},
		}

		win, err := NewCountingWindow(config)
		require.NoError(t, err)
		win.Start()
		defer win.Stop()

		// 1. 发送第1条数据，填充 outputChan
		win.Add(map[string]interface{}{"id": 1})
		time.Sleep(50 * time.Millisecond)

		// 2. 发送第2条数据
		// outputChan 已满，StrategyDrop 会尝试丢弃旧数据（outputChan头部）来放入新数据
		win.Add(map[string]interface{}{"id": 2})
		time.Sleep(50 * time.Millisecond)

		stats := win.GetStats()
		assert.Equal(t, int64(2), stats["sentCount"])

		// 验证现在缓冲区里是第2条数据
		select {
		case data := <-win.OutputChan():
			assert.Len(t, data, 1)
			assert.Equal(t, 2, cast.ToInt(data[0].Data.(map[string]interface{})["id"]))
		default:
			t.Fatal("expected data in output channel")
		}
	})

	t.Run("TumblingWindow_StrategyBlock_Timeout", func(t *testing.T) {
		// 配置：窗口大小50ms，输出缓冲1，阻塞策略，超时50ms
		config := types.WindowConfig{
			Type:   "TumblingWindow",
			Params: []interface{}{"50ms"},
			PerformanceConfig: types.PerformanceConfig{
				BufferConfig: types.BufferConfig{
					WindowOutputSize: 1,
				},
				OverflowConfig: types.OverflowConfig{
					Strategy:      types.OverflowStrategyBlock,
					BlockTimeout:  50 * time.Millisecond,
					AllowDataLoss: true,
				},
			},
		}

		win, err := NewTumblingWindow(config)
		require.NoError(t, err)
		win.Start()
		defer win.Stop()

		// 1. 发送数据触发第1个窗口
		win.Add(map[string]interface{}{"id": 1})
		// 等待窗口触发 (50ms)
		time.Sleep(100 * time.Millisecond)

		stats := win.GetStats()
		assert.Equal(t, int64(1), stats["sentCount"])
		assert.Equal(t, int64(1), stats["bufferUsed"])

		// 2. 发送数据触发第2个窗口
		// 由于没有读取 outputChan，第2个窗口触发时应该阻塞然后超时
		win.Add(map[string]interface{}{"id": 2})
		// 等待窗口触发 (50ms) + 阻塞超时 (50ms)
		time.Sleep(150 * time.Millisecond)

		stats = win.GetStats()
		assert.Equal(t, int64(1), stats["bufferUsed"])   // 仍然只有第1个窗口的数据
		assert.Equal(t, int64(1), stats["droppedCount"]) // 第2个窗口结果被丢弃
	})
}
