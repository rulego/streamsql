package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 窗口行缓冲触顶后丢行会让聚合结果只覆盖窗口的一部分（COUNT 少报），
// 这件事必须能从 metrics 看到，否则用户只看到 COUNT 偏小、无从判断原因。
func TestWindowRowCap_IsObservableViaMetrics(t *testing.T) {
	t.Parallel()
	const (
		capacity = 20
		rows     = 60
	)
	ssql := streamsql.New(streamsql.WithWindowMaxRows(capacity))
	defer ssql.Stop()

	require.NoError(t, ssql.Execute(
		`SELECT COUNT(*) AS c FROM stream GROUP BY TumblingWindow('400ms')`))

	var mu sync.Mutex
	var counts []float64
	ssql.AddSink(func(out []map[string]any) {
		mu.Lock()
		for _, r := range out {
			if c, ok := r["c"]; ok {
				counts = append(counts, toFloat(c))
			}
		}
		mu.Unlock()
	})

	for i := 0; i < rows; i++ {
		ssql.Emit(map[string]any{"deviceId": "d1", "v": float64(i)})
	}
	time.Sleep(900 * time.Millisecond)

	stats := ssql.Stream().GetStats()
	dropped := stats[stream.WindowRowsDroppedCount]

	mu.Lock()
	got := append([]float64(nil), counts...)
	mu.Unlock()

	t.Logf("上限=%d 喂 %d 行 → COUNT=%v，window_rows_dropped_count=%d",
		capacity, rows, got, dropped)

	require.NotEmpty(t, got, "窗口应有输出")
	assert.Positive(t, dropped, "行被丢弃却未计数：静默错值")
	for _, c := range got {
		assert.LessOrEqual(t, c, float64(capacity), "单窗口 COUNT 不应超过行上限")
	}
	// 丢弃行数 + 已计入的行数应覆盖全部输入
	var total float64
	for _, c := range got {
		total += c
	}
	assert.EqualValues(t, rows, int(total)+int(dropped),
		"丢弃数(%d) + 聚合数(%v) 应等于输入总数(%d)", dropped, total, rows)
}

// 行数在上限内时不应有任何丢弃计数（避免误报），且结果不受影响。
func TestWindowRowCap_NoneWhenWithinCap(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New(streamsql.WithWindowMaxRows(1000))
	defer ssql.Stop()

	require.NoError(t, ssql.Execute(
		`SELECT deviceId, COUNT(*) AS c FROM stream GROUP BY deviceId, TumblingWindow('400ms')`))

	var mu sync.Mutex
	total := 0.0
	ssql.AddSink(func(out []map[string]any) {
		mu.Lock()
		for _, r := range out {
			total += toFloat(r["c"])
		}
		mu.Unlock()
	})

	const rows = 50
	for i := 0; i < rows; i++ {
		ssql.Emit(map[string]any{"deviceId": "d1", "v": float64(i)})
	}
	time.Sleep(900 * time.Millisecond)

	stats := ssql.Stream().GetStats()
	assert.Zero(t, stats[stream.WindowRowsDroppedCount], "上限内不应有丢弃计数")

	mu.Lock()
	defer mu.Unlock()
	assert.EqualValues(t, rows, total, "上限内所有行都应计入聚合")
}

// 默认（未设置选项）为无界：不改变现有行为，任何行数都不丢。
func TestWindowRowCap_DefaultIsUnbounded(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	require.NoError(t, ssql.Execute(
		`SELECT COUNT(*) AS c FROM stream GROUP BY TumblingWindow('400ms')`))

	var mu sync.Mutex
	total := 0.0
	ssql.AddSink(func(out []map[string]any) {
		mu.Lock()
		for _, r := range out {
			total += toFloat(r["c"])
		}
		mu.Unlock()
	})

	const rows = 5000
	for i := 0; i < rows; i++ {
		ssql.Emit(map[string]any{"v": float64(i)})
	}
	time.Sleep(900 * time.Millisecond)

	stats := ssql.Stream().GetStats()
	assert.Zero(t, stats[stream.WindowRowsDroppedCount],
		"默认无界，不应有任何丢弃")

	mu.Lock()
	defer mu.Unlock()
	assert.EqualValues(t, rows, total, "默认无界时所有行都应计入")
}

// 上限对滑动窗口同样生效（滑动窗口 size/slide 比值大时缓冲的行最多）。
func TestWindowRowCap_AppliesToSlidingWindow(t *testing.T) {
	t.Parallel()
	const capacity = 15
	ssql := streamsql.New(streamsql.WithWindowMaxRows(capacity))
	defer ssql.Stop()

	require.NoError(t, ssql.Execute(
		`SELECT COUNT(*) AS c FROM stream GROUP BY SlidingWindow('600ms', '300ms')`))

	var mu sync.Mutex
	var maxCount float64
	ssql.AddSink(func(out []map[string]any) {
		mu.Lock()
		for _, r := range out {
			if c := toFloat(r["c"]); c > maxCount {
				maxCount = c
			}
		}
		mu.Unlock()
	})

	for i := 0; i < 60; i++ {
		ssql.Emit(map[string]any{"v": float64(i)})
	}
	time.Sleep(1100 * time.Millisecond)

	stats := ssql.Stream().GetStats()
	assert.Positive(t, stats[stream.WindowRowsDroppedCount], "滑动窗口触顶应计数")

	mu.Lock()
	defer mu.Unlock()
	assert.LessOrEqual(t, maxCount, float64(capacity),
		"滑动窗口单次输出的行数不应超过上限")
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}
