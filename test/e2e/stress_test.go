package e2e

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/require"
)

// Stress Testing: v1.0.3 Analyze the stability of function paths under continuous load—goroutine does not leak, heaps do not grow infinitely,
// No panic. Most likely for v1.0.2→v1.0.3 (OVER State Machine / PARTITION BY / splitAnalyticExprMulti).
// Introducing a return. Local normal mode running (-race reverts from CI/Linux).

// Multiple New → load→Stop cycle: Each Stop should recover all sink/processing coroutines, and NumGoroutine returns to baseline.
// Capture "Stop non-convergence" type leaks—each round leaves k → final values = base + cycles * k, far exceeding the baseline.
func TestStress_NoGoroutineLeak_CreateStop(t *testing.T) {
	runtime.GC()
	base := runtime.NumGoroutine()

	sql := `SELECT deviceId, acc_count(v) OVER (PARTITION BY deviceId) AS c FROM stream`
	const cycles = 30
	for i := 0; i < cycles; i++ {
		s := streamsql.New()
		require.NoError(t, s.Execute(sql))
		for j := 0; j < 200; j++ {
			_, err := s.EmitSync(map[string]any{"deviceId": j % 4, "v": j})
			require.NoError(t, err)
		}
		s.Stop()
	}

	// Stop is grace join, which samples residual coroutines such as margins after exiting.
	deadline := time.Now().Add(3 * time.Second)
	var final int
	for {
		runtime.GC()
		final = runtime.NumGoroutine()
		if final <= base+4 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.LessOrEqual(t, final, base+8,
		"goroutine 泄漏：%d 次 Stop 后未回到基线：base=%d final=%d", cycles, base, final)
	t.Logf("goroutine: base=%d final=%d (cycles=%d)", base, final, cycles)
}

// A single instance sustains 100,000 events (50 partitions, within the default limit): heap increment is controlled with no panic throughout.
// At the same time, continuous throughput is provided as a performance reference. If the heap increment exceeds the threshold, it is suspected to be a per-event or per-partition state-retention leakage.
func TestStress_SustainedLoad_HeapStable(t *testing.T) {
	s := streamsql.New()
	require.NoError(t, s.Execute(
		`SELECT deviceId, lag(v) OVER (PARTITION BY deviceId) AS p, acc_sum(v) OVER (PARTITION BY deviceId) AS t FROM stream`))
	defer s.Stop()

	// Preheat (trigger ensureAnalytic sync.Once initialization), then take the baseline.
	_, err := s.EmitSync(map[string]any{"deviceId": 0, "v": 0})
	require.NoError(t, err)
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heapStart := ms.HeapAlloc

	const events = 100000
	const partitions = 50
	start := time.Now()
	for i := 0; i < events; i++ {
		_, err := s.EmitSync(map[string]any{"deviceId": i % partitions, "v": i})
		require.NoError(t, err)
	}
	dur := time.Since(start)
	runtime.GC()
	runtime.ReadMemStats(&ms)
	heapEnd := ms.HeapAlloc

	t.Logf("Continuous load: %d events / %v = %.0f ops/sec", events, dur, float64(events)/dur.Seconds())
	t.Logf("Dui: %.2fMB → %.2fMB (delta %.2fMB)", float64(heapStart)/1e6, float64(heapEnd)/1e6, float64(int64(heapEnd)-int64(heapStart))/1e6)
	require.Less(t, float64(int64(heapEnd)-int64(heapStart)), 50.0*1e6,
		"堆增量过大，疑为状态留存型泄漏：delta=%.2fMB", float64(int64(heapEnd)-int64(heapStart))/1e6)
}

// The number of partitions far exceeds the default limit: LRU ejection should not leak or panic under continuous load. Each round, the longest group eliminated without a division.
func TestStress_PartitionEviction_NoLeak(t *testing.T) {
	s := streamsql.New()
	require.NoError(t, s.Execute(
		`SELECT deviceId, lag(v) OVER (PARTITION BY deviceId) AS p FROM stream`))
	defer s.Stop()

	_, err := s.EmitSync(map[string]any{"deviceId": 0, "v": 0})
	require.NoError(t, err)
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heapStart := ms.HeapAlloc

	// 50,000 different zones, continuously triggering elimination; Each partition has only one segment, and lag is always nil.
	const distinct = 50000
	start := time.Now()
	for i := 0; i < distinct; i++ {
		_, err := s.EmitSync(map[string]any{"deviceId": i, "v": i})
		require.NoError(t, err)
	}
	dur := time.Since(start)
	runtime.GC()
	runtime.ReadMemStats(&ms)
	heapEnd := ms.HeapAlloc

	t.Logf("Elimination load: %d partition / %v = %.0f ops/sec", distinct, dur, float64(distinct)/dur.Seconds())
	t.Logf("Dui: %.2fMB → %.2fMB (delta %.2fMB)", float64(heapStart)/1e6, float64(heapEnd)/1e6, float64(int64(heapEnd)-int64(heapStart))/1e6)
	// Expulsion should reclaim old partitions, and only reside near the LRU limit; If the residency increases linearly with the total number of partitions, it is considered a deportation failure.
	require.Less(t, float64(int64(heapEnd)-int64(heapStart)), 100.0*1e6,
		"堆随分区数线性增长，疑为 LRU 驱逐未回收：delta=%.2fMB", float64(int64(heapEnd)-int64(heapStart))/1e6)
}

// --- Common Rules and Capacity Benchmarks for 128MB Gateways ---
// Create a new map for each event (close to real access: the gateway parses each incoming message into a map); Single-stream EmitSync.
// Use GOMAXPROCS / GOMEMLIMIT environment variables to simulate the gateway CPU/memory constraints.
// Note: EmitSync is a synchronous single goroutine; single rule throughput is independent of core count—multi-core relies on parallel multi-instance operation.

const gatewayDeviceCount = 100

var gatewayDeviceIDs = func() []string {
	ids := make([]string, gatewayDeviceCount)
	for i := range ids {
		ids[i] = "dev-" + strconv.Itoa(i)
	}
	return ids
}()

func gatewayRow(i int) map[string]any {
	return map[string]any{
		"deviceId":    gatewayDeviceIDs[i%gatewayDeviceCount],
		"temperature": 20.0 + float64(i%100)/10.0,
		"humidity":    50.0 + float64(i%80)/10.0,
	}
}

func benchGatewayRule(b *testing.B, sql string) {
	b.Helper()
	s := streamsql.New()
	if err := s.Execute(sql); err != nil {
		b.Fatalf("execute: %v", err)
	}
	defer s.Stop()
	if _, err := s.EmitSync(gatewayRow(0)); err != nil {
		b.Fatalf("emit: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.EmitSync(gatewayRow(i))
	}
}

// Common rule 1: Filter (highest frequency).
func BenchmarkGateway_Filter(b *testing.B) {
	benchGatewayRule(b, `SELECT deviceId, temperature FROM stream WHERE temperature > 25`)
}

// Common Rule 2: Conversion (unit conversion).
func BenchmarkGateway_Transform(b *testing.B) {
	benchGatewayRule(b, `SELECT deviceId, temperature * 1.8 + 32 AS fahrenheit FROM stream`)
}

// Common Rule 3: Change detection (analysis function + partitioning).
func BenchmarkGateway_AnalyticChange(b *testing.B) {
	benchGatewayRule(b, `SELECT deviceId, temperature, lag(temperature) OVER (PARTITION BY deviceId) AS prev FROM stream`)
}

// Parallel multi-instance (multi-core utilization: gateways run multiple rules simultaneously, one per core). RunParallel extends with GOMAXPROCS,
// Demonstrates the aggregation throughput of multi-core gateways.
func BenchmarkGateway_ParallelInstances(b *testing.B) {
	sql := `SELECT deviceId, temperature FROM stream WHERE temperature > 25`
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		s := streamsql.New()
		if err := s.Execute(sql); err != nil {
			b.Fatalf("execute: %v", err)
		}
		defer s.Stop()
		s.EmitSync(gatewayRow(0))
		for i := 0; pb.Next(); i++ {
			s.EmitSync(gatewayRow(i))
		}
	})
}

// TestWindowEventTime_MultiTimestampAggregation Verify epoch alignment of the event-time TumblingWindow:
// 1s window alignment to the full-second boundary (alignWindowStart); multi-timestamp events must fall within the same aligned window to aggregate together.
// After aligning base to the second boundary, base / base+100 belong to [base, base+1000), and count is always 2.
// (It was once misidentified as "window race"—actually caused by test timestamps crossing epoch alignment boundaries, but the engine was correct.))
func TestWindowEventTime_MultiTimestampAggregation(t *testing.T) {
	const iters = 300
	fails := 0
	for i := 0; i < iters; i++ {
		s := streamsql.New()
		if err := s.Execute(`SELECT count(*) AS c FROM stream GROUP BY TumblingWindow('1s') WITH (TIMESTAMP='ts', TIMEUNIT='ms')`); err != nil {
			t.Fatal(err)
		}
		ch := make(chan []map[string]any, 4)
		s.AddSink(func(r []map[string]any) { ch <- r })
		base := ((time.Now().UnixMilli() - 5000) / 1000) * 1000 // Align to the second boundary: Avoid base/base+100 crossing the 1-second window of epoch alignment
		s.Emit(map[string]any{"ts": base, "v": 10})
		s.Emit(map[string]any{"ts": base + 100, "v": 60})
		s.Emit(map[string]any{"ts": base + 2000, "v": 5})
		select {
		case rows := <-ch:
			if c := asFloat64(rows[0]["c"]); c != 2 {
				fails++
				if fails <= 5 {
					t.Logf("iter %d: count=%v (want 2)", i, c)
				}
			}
		case <-time.After(2 * time.Second):
			fails++
			if fails <= 5 {
				t.Logf("iter %d: timeout", i)
			}
		}
		s.Stop()
	}
	if fails > 0 {
		t.Fatalf("race reproduced: %d/%d iters failed", fails, iters)
	}
}
