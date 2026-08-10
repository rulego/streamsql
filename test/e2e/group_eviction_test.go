package e2e

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 分组数超上限时聚合结果会被丢弃，这件事必须能从 metrics 看到，
// 否则用户只看到输出里少了一批设备、无从判断原因。
func TestGroupEviction_IsObservableViaMetrics(t *testing.T) {
	t.Parallel()
	const (
		capacity = 10
		devices  = 25
	)
	ssql := streamsql.New(streamsql.WithGroupMaxPartitions(capacity))
	defer ssql.Stop()

	require.NoError(t, ssql.Execute(
		`SELECT deviceId, COUNT(*) AS c FROM stream GROUP BY deviceId, TumblingWindow('400ms')`))

	var mu sync.Mutex
	var got []map[string]any
	ssql.AddSink(func(rows []map[string]any) {
		mu.Lock()
		got = append(got, rows...)
		mu.Unlock()
	})

	for i := 0; i < devices; i++ {
		ssql.Emit(map[string]any{"deviceId": fmt.Sprintf("d%02d", i), "v": 1})
	}
	time.Sleep(900 * time.Millisecond)

	mu.Lock()
	emitted := len(got)
	mu.Unlock()

	stats := ssql.Stream().GetStats()
	evicted := stats[stream.GroupEvictedCount]

	t.Logf("上限=%d 喂 %d 设备 → 输出 %d 组，group_evicted_count=%d",
		capacity, devices, emitted, evicted)

	assert.LessOrEqual(t, emitted, capacity, "输出组数不应超过上限")
	assert.Positive(t, evicted, "分组被丢弃却未计数：静默错值")
	// 丢弃组数 + 输出组数应覆盖全部设备
	assert.EqualValues(t, devices, int(evicted)+emitted,
		"淘汰数(%d) + 输出数(%d) 应等于设备总数(%d)", evicted, emitted, devices)
}

// 分组数在上限内时不应有任何淘汰计数（避免误报）。
func TestGroupEviction_NoneWhenWithinCap(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New(streamsql.WithGroupMaxPartitions(100))
	defer ssql.Stop()

	require.NoError(t, ssql.Execute(
		`SELECT deviceId, COUNT(*) AS c FROM stream GROUP BY deviceId, TumblingWindow('400ms')`))

	var mu sync.Mutex
	groups := map[string]bool{}
	ssql.AddSink(func(rows []map[string]any) {
		mu.Lock()
		for _, r := range rows {
			if d, ok := r["deviceId"].(string); ok {
				groups[d] = true
			}
		}
		mu.Unlock()
	})

	const devices = 20
	for round := 0; round < 3; round++ {
		for i := 0; i < devices; i++ {
			ssql.Emit(map[string]any{"deviceId": fmt.Sprintf("d%02d", i), "v": 1})
		}
	}
	time.Sleep(900 * time.Millisecond)

	stats := ssql.Stream().GetStats()
	assert.Zero(t, stats[stream.GroupEvictedCount], "上限内不应有淘汰计数")

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, groups, devices, "上限内所有设备都应有聚合结果")
}

// 默认上限（未设置选项）下，常规基数不触发淘汰。
func TestGroupEviction_DefaultCapNoEvictionForNormalCardinality(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	require.NoError(t, ssql.Execute(
		`SELECT deviceId, AVG(v) AS a FROM stream GROUP BY deviceId, TumblingWindow('400ms')`))
	ssql.AddSink(func(rows []map[string]any) {})

	for i := 0; i < 500; i++ {
		ssql.Emit(map[string]any{"deviceId": fmt.Sprintf("d%03d", i), "v": float64(i)})
	}
	time.Sleep(900 * time.Millisecond)

	stats := ssql.Stream().GetStats()
	assert.Zero(t, stats[stream.GroupEvictedCount],
		"500 个分组远低于默认上限 10000，不应淘汰")
}
