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

// --- 分区语义正确性（边界组合）---

// PARTITION BY 在 SELECT 侧的状态隔离：交错输入 d1/d2，各分区 lag 只看本分区前值。
// 预期 prev = [nil, nil, 10, 100, 20]（无隔离会把 d1 的值串进 d2）。
func TestAnalytic_PartitionIsolation_Lag(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT deviceId, lag(value) OVER (PARTITION BY deviceId) AS prev FROM stream"))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"deviceId": 1, "value": 10},
		{"deviceId": 2, "value": 100},
		{"deviceId": 1, "value": 20},
		{"deviceId": 2, "value": 200},
		{"deviceId": 1, "value": 30},
	}
	expected := []any{nil, nil, 10, 100, 20}
	for i, in := range inputs {
		r, err := ssql.EmitSync(in)
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Equal(t, expected[i], r["prev"], "row %d", i)
	}
}

// acc_sum 带 PARTITION BY：各分区独立累加。
// 预期 total = [10, 100, 30, 300, 60]。
func TestAnalytic_AccSum_Partition(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT deviceId, acc_sum(value) OVER (PARTITION BY deviceId) AS total FROM stream"))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"deviceId": 1, "value": 10},
		{"deviceId": 2, "value": 100},
		{"deviceId": 1, "value": 20},
		{"deviceId": 2, "value": 200},
		{"deviceId": 1, "value": 30},
	}
	expected := []any{10.0, 100.0, 30.0, 300.0, 60.0}
	for i, in := range inputs {
		r, err := ssql.EmitSync(in)
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Equal(t, expected[i], r["total"], "row %d", i)
	}
}

// 同一查询多个分析函数 + PARTITION BY：lag 与 acc_max 各自维护独立状态。
// 预期 prev = [nil, 10, 5]，mx = [10, 10, 20]。
func TestAnalytic_MultipleAnalytic_Partition(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT deviceId, lag(value) OVER (PARTITION BY deviceId) AS prev, acc_max(value) OVER (PARTITION BY deviceId) AS mx FROM stream"))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"deviceId": 1, "value": 10},
		{"deviceId": 1, "value": 5},
		{"deviceId": 1, "value": 20},
	}
	expectedPrev := []any{nil, 10, 5}
	expectedMx := []any{10.0, 10.0, 20.0}
	for i, in := range inputs {
		r, err := ssql.EmitSync(in)
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Equal(t, expectedPrev[i], r["prev"], "row %d prev", i)
		assert.Equal(t, expectedMx[i], r["mx"], "row %d mx", i)
	}
}

// OVER WHEN × PARTITION BY 组合：每分区独立维护 (历史 + 缓存输出)。
// WHEN 满足才更新状态并重算 lag、刷新缓存；不满足时返回该分区缓存的上一输出（而非上一个满足值）。
// 预期 prev = [nil, 20, 20, nil, 20, 30]。
//   row2(5≤15,F) 复用 d1 缓存 20；row4(10≤15,F,d1) 即便前面插了 d2 行，仍取 d1 自己的缓存 20。
func TestAnalytic_WhenAndPartition_Lag(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT deviceId, lag(value) OVER (PARTITION BY deviceId WHEN value > 15) AS prev FROM stream"))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"deviceId": 1, "value": 20},
		{"deviceId": 1, "value": 30},
		{"deviceId": 1, "value": 5},
		{"deviceId": 2, "value": 40},
		{"deviceId": 1, "value": 10},
		{"deviceId": 1, "value": 25},
	}
	expected := []any{nil, 20, 20, nil, 20, 30}
	for i, in := range inputs {
		r, err := ssql.EmitSync(in)
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Equal(t, expected[i], r["prev"], "row %d", i)
	}
}

// acc_avg 带 PARTITION BY：各分区独立累积均值（count 与 sum 都按分区隔离）。
// 预期 avg = [10, 100, 15, 20, 150]。
func TestAnalytic_AccAvg_Partition(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT deviceId, acc_avg(value) OVER (PARTITION BY deviceId) AS avg FROM stream"))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"deviceId": 1, "value": 10},
		{"deviceId": 2, "value": 100},
		{"deviceId": 1, "value": 20},
		{"deviceId": 1, "value": 30},
		{"deviceId": 2, "value": 200},
	}
	expected := []any{10.0, 100.0, 15.0, 20.0, 150.0}
	for i, in := range inputs {
		r, err := ssql.EmitSync(in)
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Equal(t, expected[i], r["avg"], "row %d", i)
	}
}

// acc_count 带 PARTITION BY：各分区独立计数。
// 预期 cnt = [1, 1, 2, 3, 2]（d1: 1→2→3；d2: 1→2）。
func TestAnalytic_AccCount_Partition(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT deviceId, acc_count(value) OVER (PARTITION BY deviceId) AS cnt FROM stream"))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"deviceId": 1, "value": 10},
		{"deviceId": 2, "value": 100},
		{"deviceId": 1, "value": 20},
		{"deviceId": 1, "value": 30},
		{"deviceId": 2, "value": 200},
	}
	expected := []any{int64(1), int64(1), int64(2), int64(3), int64(2)}
	for i, in := range inputs {
		r, err := ssql.EmitSync(in)
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Equal(t, expected[i], r["cnt"], "row %d", i)
	}
}

// lag 多偏移 + 默认值 + ignoreNull 组合：nil 不入历史，历史不足时返回默认值。
// 序列 [10, 20, nil, 30, 40]，预期 lg = [-1.0, -1.0, 10, 10, 20]。
// 关键判别：row3(nil) 与 row4 都返回 10——nil 被忽略未入历史，故 row4 仍取到 20 之前的 10。
func TestAnalytic_LagOffsetDefaultIgnoreNull(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT lag(value, 2, -1, true) AS lg FROM stream"))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"value": 10},
		{"value": 20},
		{"value": nil},
		{"value": 30},
		{"value": 40},
	}
	expected := []any{-1.0, -1.0, 10, 10, 20}
	for i, in := range inputs {
		r, err := ssql.EmitSync(in)
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Equal(t, expected[i], r["lg"], "row %d", i)
	}
}
