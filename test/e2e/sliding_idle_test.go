package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 稀疏上报场景：设备静默一段时间后再上报，聚合结果不应被延迟到与静默时长相当。
// 回归：滑动窗口 slot 在空闲期不推进，恢复后逐 slide 追赶 → 600ms 窗口空闲 3s 后
// 新事件到 sink 延迟 2.6s。
func TestSlidingWindow_SparseDeviceNoDelay(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	require.NoError(t, ssql.Execute(
		`SELECT deviceId, COUNT(*) AS c FROM stream GROUP BY deviceId, SlidingWindow('600ms','200ms')`))

	var mu sync.Mutex
	var arrivals []time.Time
	ssql.AddSink(func(rows []map[string]any) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
	})

	// 首次上报
	ssql.Emit(map[string]any{"deviceId": "d1", "v": 1})
	time.Sleep(3 * time.Second) // 设备静默

	mu.Lock()
	arrivals = nil
	mu.Unlock()

	// 静默后再次上报
	sent := time.Now()
	ssql.Emit(map[string]any{"deviceId": "d1", "v": 2})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(arrivals)
		var first time.Time
		if n > 0 {
			first = arrivals[0]
		}
		mu.Unlock()
		if n > 0 {
			delay := first.Sub(sent)
			assert.Less(t, delay, time.Second,
				"静默 3s 后新事件延迟 %v，应与静默时长无关（窗口仅 600ms）", delay)
			t.Logf("静默 3s 后新事件到 sink 延迟 = %v", delay.Round(10*time.Millisecond))
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("静默 3s 后新事件 5s 内未到 sink")
}

// 持续上报时滑动窗口行为不变（确认空闲追赶没有误伤正常路径）。
func TestSlidingWindow_ContinuousStreamUnaffected(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	require.NoError(t, ssql.Execute(
		`SELECT COUNT(*) AS c, SUM(v) AS total FROM stream GROUP BY SlidingWindow('600ms','200ms')`))

	var mu sync.Mutex
	var results []map[string]any
	ssql.AddSink(func(rows []map[string]any) {
		mu.Lock()
		results = append(results, rows...)
		mu.Unlock()
	})

	// 连续 1.2s 每 50ms 一条，共 24 条
	for i := 0; i < 24; i++ {
		ssql.Emit(map[string]any{"v": 1})
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(800 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, results, "连续流应持续产出窗口结果")
	// 每个窗口都应有计数，且不应出现空窗口
	for i, r := range results {
		c, ok := r["c"]
		require.True(t, ok, "结果 %d 缺 c 字段: %v", i, r)
		assert.NotZero(t, c, "结果 %d 计数为 0（空窗口不应产出）: %v", i, r)
	}
	t.Logf("连续流产出 %d 个窗口结果", len(results))
}
