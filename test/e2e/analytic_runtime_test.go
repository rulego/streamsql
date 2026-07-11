package e2e

import (
	"sync"
	"testing"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 分析函数 OVER 状态机的运行时特性：并发正确性 + 分区上限 + 性能基准。
// 并发测试在 CI/Linux 的 -race 下回归（捕获把 lastResults/partitions/state
// 访问移出 fe.mu 的改动）；本地普通模式验证无 panic/死锁、增量守恒、上限生效。

// --- 并发 ---

// 多 goroutine 各打一个独立分区：互斥串行化后每分区计数精确，无跨分区串扰。
func TestAnalytic_ConcurrentEmitSync_DistinctPartitions(t *testing.T) {
	const g, m = 8, 500
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(
		`SELECT acc_count(v) OVER (PARTITION BY deviceId) AS c FROM stream`))
	defer ssql.Stop()

	last := make([]int, g)
	var wg sync.WaitGroup
	for gid := 0; gid < g; gid++ {
		gid := gid
		wg.Add(1)
		go func() {
			defer wg.Done()
			localMax := 0
			for i := 0; i < m; i++ {
				r, err := ssql.EmitSync(map[string]any{"deviceId": gid, "v": i})
				require.NoError(t, err)
				require.NotNil(t, r)
				if c, ok := r["c"].(int64); ok && int(c) > localMax {
					localMax = int(c)
				}
			}
			last[gid] = localMax
		}()
	}
	wg.Wait()

	for gid := 0; gid < g; gid++ {
		assert.Equal(t, m, last[gid], "分区 %d 末值应为 %d", gid, m)
	}
}

// 同一分区高并发：所有 Apply 在同一把 fe.mu 下串行，返回值集合恰为 {1..total}，
// 故最大返回值 == total（总事件数），证明增量不丢失。
func TestAnalytic_ConcurrentEmitSync_SharedPartition(t *testing.T) {
	const g, m = 8, 500
	const total = g * m
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(
		`SELECT acc_count(v) OVER (PARTITION BY deviceId) AS c FROM stream`))
	defer ssql.Stop()

	var mu sync.Mutex
	gmax := 0
	var wg sync.WaitGroup
	for gid := 0; gid < g; gid++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localMax := 0
			for i := 0; i < m; i++ {
				r, err := ssql.EmitSync(map[string]any{"deviceId": "shared", "v": i})
				require.NoError(t, err)
				require.NotNil(t, r)
				if c, ok := r["c"].(int64); ok && int(c) > localMax {
					localMax = int(c)
				}
			}
			mu.Lock()
			if localMax > gmax {
				gmax = localMax
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	assert.Equal(t, total, gmax, "并发下 acc_count 不得丢增量；max=%d want=%d", gmax, total)
}

// --- 分区上限（LRU）---

// WithAnalyticMaxPartitions(2)：打 3 个分区后最久未用的 dev1 被淘汰，
// 再来 dev1 状态已重置，acc_count 回到 1（而非 2）。
func TestAnalytic_MaxPartitions_EvictionResets(t *testing.T) {
	ssql := streamsql.New(streamsql.WithAnalyticMaxPartitions(2))
	require.NoError(t, ssql.Execute(
		`SELECT acc_count(v) OVER (PARTITION BY deviceId) AS c FROM stream`))
	defer ssql.Stop()

	emit := func(id string) int64 {
		r, err := ssql.EmitSync(map[string]any{"deviceId": id, "v": 1})
		require.NoError(t, err)
		require.NotNil(t, r)
		c, _ := r["c"].(int64)
		return c
	}
	assert.Equal(t, int64(1), emit("dev1"))
	assert.Equal(t, int64(1), emit("dev2"))
	assert.Equal(t, int64(1), emit("dev3")) // cap=2 → dev1 被淘汰
	assert.Equal(t, int64(1), emit("dev1"), "dev1 被淘汰后状态重置，计数回到 1")
}

// 默认上限足够大：同样的序列下 dev1 不被淘汰，再来时计数为 2。
func TestAnalytic_MaxPartitions_DefaultKeeps(t *testing.T) {
	ssql := streamsql.New() // 默认上限
	require.NoError(t, ssql.Execute(
		`SELECT acc_count(v) OVER (PARTITION BY deviceId) AS c FROM stream`))
	defer ssql.Stop()

	emit := func(id string) int64 {
		r, err := ssql.EmitSync(map[string]any{"deviceId": id, "v": 1})
		require.NoError(t, err)
		require.NotNil(t, r)
		c, _ := r["c"].(int64)
		return c
	}
	assert.Equal(t, int64(1), emit("dev1"))
	assert.Equal(t, int64(1), emit("dev2"))
	assert.Equal(t, int64(1), emit("dev3"))
	assert.Equal(t, int64(2), emit("dev1"), "默认上限下 dev1 状态保留，计数累加到 2")
}

// --- 性能基准 ---

func benchEmitSync(b *testing.B, sql string, row map[string]any) {
	b.Helper()
	ssql := streamsql.New()
	if err := ssql.Execute(sql); err != nil {
		b.Fatalf("execute: %v", err)
	}
	defer ssql.Stop()
	// 预热（首次 EmitSync 触发 ensureAnalytic 的 sync.Once 初始化）。
	if _, err := ssql.EmitSync(row); err != nil {
		b.Fatalf("emit: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ssql.EmitSync(row)
	}
}

// 基准：无分析函数的普通投影（analytic 引擎走 nil 快路径）。
func BenchmarkEmitSync_NoAnalytic(b *testing.B) {
	benchEmitSync(b,
		`SELECT deviceId, v FROM stream`,
		map[string]any{"deviceId": "d1", "v": 1})
}

// 分析函数无分区：partitionKey 早返回 ""，仅 lag 状态机开销。
func BenchmarkAnalytic_NoPartition(b *testing.B) {
	benchEmitSync(b,
		`SELECT lag(v) AS p FROM stream`,
		map[string]any{"deviceId": "d1", "v": 1})
}

// PARTITION BY 单列：每事件构造分区键（类型 switch + Builder）。
func BenchmarkAnalytic_Partition1Col(b *testing.B) {
	benchEmitSync(b,
		`SELECT lag(v) OVER (PARTITION BY deviceId) AS p FROM stream`,
		map[string]any{"deviceId": "d1", "v": 1})
}

// PARTITION BY 双列：分区键构造 ×2。
func BenchmarkAnalytic_Partition2Col(b *testing.B) {
	benchEmitSync(b,
		`SELECT lag(v) OVER (PARTITION BY deviceId, region) AS p FROM stream`,
		map[string]any{"deviceId": "d1", "region": "z1", "v": 1})
}
