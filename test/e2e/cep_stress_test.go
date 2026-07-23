package e2e

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/require"
)

// CEP stress testing (streamsql full-link layer): MATCH_RECOGNIZE → emit→ sink under continuous load via CEP→
// Stability—no sweeper leakage, partitioned LRU recovery, no panic. Isomorphic with stress_test.go (Create/Stop loop,
// Continuous load heap stability, partition eviction), but CEP must use Emit (async, EmitSync denies MATCH_RECOGNIZE),
// Therefore, use sync sink counts to create drain synchronization points: PATTERN(A) matches 1 per event, wantMatches = number of events.
// WITHIN '1h' Start the sweeper (test its stream.Stop join correctly). Local normal mode runs (-race reverted from CI/Linux).

const cepStressSQL = `SELECT * FROM stream
	MATCH_RECOGNIZE (
		PARTITION BY deviceId ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A)
		WITHIN '1h'
	)`

// drainCepMatches loads rows (async emit) in batches, and after each batch the sync sink count catches up, the next batch (back pressure) is added.
// Continuous bare loading will fill dataChan to trigger emit discard (if not full, discard it, WARN "Data channel is full"),
// This caused wantMatches to never reach → timeout. Backpressure causes Emit rates to follow the processor, so channels are not lost when not satisfied;
// PATTERN(A) matches 1 event per event, and the sink count is the number of processed events.
func drainCepMatches(t testing.TB, s *streamsql.Streamsql, rows []map[string]any) time.Duration {
	t.Helper()
	var got int64
	s.AddSyncSink(func([]map[string]any) { atomic.AddInt64(&got, 1) })
	const batch = 256
	start := time.Now()
	for i := 0; i < len(rows); i += batch {
		end := i + batch
		if end > len(rows) {
			end = len(rows)
		}
		for j := i; j < end; j++ {
			s.Emit(rows[j])
		}
		// Wait for the current batch to match output (processor processing and join the queue) before loading the next batch.
		target := int64(end)
		dl := time.Now().Add(120 * time.Second)
		for atomic.LoadInt64(&got) < target {
			if time.Now().After(dl) {
				t.Fatalf("drain Backpressure timeout: got = %d want = %d", atomic.LoadInt64(&got), target)
			}
			runtime.Gosched()
		}
	}
	return time.Since(start)
}

// --- 1. Create/Stop loop without goroutine leakage ---

// 30 rounds New→Execute (WITHIN triggers sweeper) →Emit→Stop. Each round of Stop should reclaim sweeper goroutine +
// Data processing goroutine, NumGoroutine returns to baseline. Capturing the "sweeper not stream.Stop join" type leak:
// Each round retains k → final value base+cycles * k.
func TestStressCEP_NoGoroutineLeak_CreateStop(t *testing.T) {
	runtime.GC()
	base := runtime.NumGoroutine()

	const cycles = 30
	for i := 0; i < cycles; i++ {
		s := streamsql.New(streamsql.WithBufferSizes(4096, 1024, 256))
		require.NoError(t, s.Execute(cepStressSQL))
		rows := make([]map[string]any, 200)
		for j := range rows {
			rows[j] = map[string]any{"deviceId": j % 4, "ts": j, "v": 60.0}
		}
		drainCepMatches(t, s, rows)
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

// --- 2. Continuous load stack stability---

// Single instance sustains 100,000 events (50 partitions): heap increment is controlled with no panic throughout. If the pile increment grows linearly with the number of events,
// Suspected to be state-retained leaks such as partition/output map.
func TestStressCEP_SustainedLoad_HeapStable(t *testing.T) {
	s := streamsql.New(streamsql.WithBufferSizes(4096, 1024, 256))
	require.NoError(t, s.Execute(cepStressSQL))
	defer s.Stop()

	// Preheat (trigger partition/sweeper initialization), then take the baseline.
	drainCepMatches(t, s, []map[string]any{{"deviceId": 0, "ts": 0, "v": 60.0}})
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heapStart := ms.HeapAlloc

	const events = 100000
	const partitions = 50
	rows := make([]map[string]any, events)
	for i := range rows {
		rows[i] = map[string]any{"deviceId": i % partitions, "ts": i, "v": 60.0}
	}
	dur := drainCepMatches(t, s, rows)
	runtime.GC()
	runtime.ReadMemStats(&ms)
	heapEnd := ms.HeapAlloc

	t.Logf("Continuous load: %d events / %v = %.0f ops/sec", events, dur, float64(events)/dur.Seconds())
	t.Logf("Dui: %.2fMB → %.2fMB (delta %.2fMB)",
		float64(heapStart)/1e6, float64(heapEnd)/1e6, float64(int64(heapEnd)-int64(heapStart))/1e6)
	require.Less(t, float64(int64(heapEnd)-int64(heapStart)), 100.0*1e6,
		"堆增量过大，疑为状态留存型泄漏：delta=%.2fMB",
		float64(int64(heapEnd)-int64(heapStart))/1e6)
}

// --- 3. Partitioned LRU Expulsion Without Leakage ---

// 50,000 different partitions (far exceeding the default limit maxPartitions=10,000), one per partition. LRU destroyers should not be used under continuous load
// Leaks should not be panicked. If the residency increases linearly with the total number of partitions, it is considered a deportation failure.
func TestStressCEP_PartitionEviction_NoLeak(t *testing.T) {
	s := streamsql.New(streamsql.WithBufferSizes(4096, 1024, 256))
	require.NoError(t, s.Execute(cepStressSQL))
	defer s.Stop()

	drainCepMatches(t, s, []map[string]any{{"deviceId": 0, "ts": 0, "v": 60.0}})
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heapStart := ms.HeapAlloc

	const distinct = 50000
	rows := make([]map[string]any, distinct)
	for i := range rows {
		rows[i] = map[string]any{"deviceId": i, "ts": i, "v": 60.0}
	}
	dur := drainCepMatches(t, s, rows)
	runtime.GC()
	runtime.ReadMemStats(&ms)
	heapEnd := ms.HeapAlloc

	t.Logf("Elimination load: %d partition / %v = %.0f ops/sec", distinct, dur, float64(distinct)/dur.Seconds())
	t.Logf("Dui: %.2fMB → %.2fMB (delta %.2fMB)",
		float64(heapStart)/1e6, float64(heapEnd)/1e6, float64(int64(heapEnd)-int64(heapStart))/1e6)
	// Expulsion should reclaim old partitions, and only reside near the LRU limit; If the residency increases linearly with the total number of partitions, it is considered a deportation failure.
	require.Less(t, float64(int64(heapEnd)-int64(heapStart)), 150.0*1e6,
		"堆随分区数线性增长，疑为 LRU 驱逐未回收：delta=%.2fMB",
		float64(int64(heapEnd)-int64(heapStart))/1e6)
}
