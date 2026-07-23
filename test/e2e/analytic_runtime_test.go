package e2e

import (
	"sync"
	"testing"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Analyze the runtime characteristics of the OVER function state machine: concurrency correctness + partition limit + performance benchmark.
// Concurrent tests regress under CI/Linux -race (capture lastResults/partitions/state).
// Changes to the access removal fe.mu); Authenticated in local normal mode, there is no panic/deadlock, incremental conservation, and upper limit effect.

// --- Concurrent ---

// Multiple goroutines each run an independent partition: after mutex serialization, each partition counts accurately, with no cross-partition crosstalk.
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

// High concurrency within the same partition: All Apply are serialized under the same fe.mu, and the return value set is exactly {1..total},
// Therefore, the maximum return value == total (total number of events) proves that the increment is not lost.
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

// --- Partition Limit (LRU) ---

// WithAnalyticMaxPartitions(2): After running 3 partitions, the longest-unused dev1 is eliminated,
// Next, the dev1 state has been reset, acc_count return to 1 (instead of 2).
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
	assert.Equal(t, int64(1), emit("dev3")) // cap=2 → dev1 is eliminated
	assert.Equal(t, int64(1), emit("dev1"), "dev1 被淘汰后状态重置，计数回到 1")
}

// The default upper limit is large enough: under the same sequence, dev1 is not eliminated, and the count is 2 when it comes back.
func TestAnalytic_MaxPartitions_DefaultKeeps(t *testing.T) {
	ssql := streamsql.New() // Default upper limit
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

// --- Performance Benchmark ---

func benchEmitSync(b *testing.B, sql string, row map[string]any) {
	b.Helper()
	ssql := streamsql.New()
	if err := ssql.Execute(sql); err != nil {
		b.Fatalf("execute: %v", err)
	}
	defer ssql.Stop()
	// Preheating (the first EmitSync triggers ensureAnalytic's sync.Once initialized).
	if _, err := ssql.EmitSync(row); err != nil {
		b.Fatalf("emit: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ssql.EmitSync(row)
	}
}

// Benchmark: Ordinary projection without analysis functions (analytic engine follows the NIL fast path).
func BenchmarkEmitSync_NoAnalytic(b *testing.B) {
	benchEmitSync(b,
		`SELECT deviceId, v FROM stream`,
		map[string]any{"deviceId": "d1", "v": 1})
}

// Parsing functions without partitioning: partitionKey returns "" early, only lags state machine overhead.
func BenchmarkAnalytic_NoPartition(b *testing.B) {
	benchEmitSync(b,
		`SELECT lag(v) AS p FROM stream`,
		map[string]any{"deviceId": "d1", "v": 1})
}

// PARTITION BY Single Column: Constructs partition keys for each event (type switch + Builder).
func BenchmarkAnalytic_Partition1Col(b *testing.B) {
	benchEmitSync(b,
		`SELECT lag(v) OVER (PARTITION BY deviceId) AS p FROM stream`,
		map[string]any{"deviceId": "d1", "v": 1})
}

// PARTITION BY Dual Column: Partition Key Construction ×2.
func BenchmarkAnalytic_Partition2Col(b *testing.B) {
	benchEmitSync(b,
		`SELECT lag(v) OVER (PARTITION BY deviceId, region) AS p FROM stream`,
		map[string]any{"deviceId": "d1", "region": "z1", "v": 1})
}

// --- Semantic correctness of partitions (boundary combinations) ---

// PARTITION BY state isolation on the SELECT side: staggered input d1/d2, and the lag of each partition only looks at the previous value of the partition.
// Expect prev = [nil, nil, 10, 100, 20] (without isolation, the value of d1 will be linked to d2).
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

// acc_sum With PARTITION BY: Each partition is independently accumulated.
// Expected total = [10, 100, 30, 300, 60].
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

// Multiple analysis functions + PARTITION BY: lag and acc_max each maintain their own independent state within the same query.
// Expected prev = [nil, 10, 5], mx = [10, 10, 20].
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

// OVER WHEN × PARTITION BY combination: Each partition is maintained independently (history + cache output).
// Update status and recalculate lag and cache only when WHEN conditions are met; If not met, it returns the previous output of the partition's cache (rather than the previous satisfied value).
// Expected prev = [nil, 20, 20, nil, 20, 30].
//
//	row2(5≤15,F) reuses d1's cache 20; row4(10≤15,F,d1) still takes d1's own cache 20 even if d2 is inserted earlier.
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

// acc_avg with PARTITION BY: The independent cumulative mean of each partition (count and sum are partitioned separately).
// Expected avg = [10, 100, 15, 20, 150].
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

// acc_count With PARTITION BY: Each partition counts independently.
// Expected CNT = [1, 1, 2, 3, 2](d1: 1→2→3; d2: 1→2).
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

// lag multi-offset + default + ignoreNull combination: nil does not enter history; returns default value when history is insufficient.
// Sequence [10, 20, nil, 30, 40], expected lg = [-1.0, -1.0, 10, 10, 20].
// Key determination: Both row3(nil) and row4 return 10—nil is ignored and not historic, so row4 still takes the 10 before 20.
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
