package e2e

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/require"
)

// CEP 压力测试（streamsql 全链路层）：MATCH_RECOGNIZE 经 Emit→async→CEP→sink 在持续负载下的
// 稳定性——sweeper 不泄漏、分区 LRU 回收、无 panic。与 stress_test.go 同构（Create/Stop 循环、
// 持续负载堆稳定、分区驱逐），但 CEP 必须用 Emit（async，EmitSync 拒绝 MATCH_RECOGNIZE），
// 故用 sync sink 计数做 drain 同步点：PATTERN(A) 每事件 1 匹配，wantMatches=事件数。
// WITHIN '1h' 让 sweeper 启动（测其经 stream.Stop 正确 join）。本地普通模式跑（-race 由 CI/Linux 回归）。

const cepStressSQL = `SELECT * FROM stream
	MATCH_RECOGNIZE (
		PARTITION BY deviceId ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A)
		WITHIN '1h'
	)`

// drainCepMatches 分批灌 rows（async Emit），每批等 sync sink 计数追上再灌下一批（背压）。
// 裸连灌会撑满 dataChan 触发 Emit 丢弃（非阻塞满则丢，WARN "Data channel is full"），
// 导致 wantMatches 永远到不了 → 超时。背压让 Emit 速率跟随 processor，channel 不满不丢；
// PATTERN(A) 每事件 1 匹配，sink 计数即已处理事件数。
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
		// 等本批匹配产出（processor 处理完入队的）再灌下一批。
		target := int64(end)
		dl := time.Now().Add(120 * time.Second)
		for atomic.LoadInt64(&got) < target {
			if time.Now().After(dl) {
				t.Fatalf("drain 背压超时：got=%d want=%d", atomic.LoadInt64(&got), target)
			}
			runtime.Gosched()
		}
	}
	return time.Since(start)
}

// --- 1. Create/Stop 循环无 goroutine 泄漏 ---

// 30 轮 New→Execute（WITHIN 触发 sweeper）→Emit→Stop。每轮 Stop 应回收 sweeper goroutine +
// 数据处理 goroutine，NumGoroutine 回到基线。捕获「sweeper 未被 stream.Stop join」型泄漏：
// 每轮残留 k → 末值 base+cycles*k。
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

	// Stop 为 grace join，给余量等残留协程退出后采样。
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

// --- 2. 持续负载堆稳定 ---

// 单实例持续 10 万事件（50 分区）：堆增量受控、全程无 panic。堆增量若随事件数线性增长，
// 疑为 partition/输出 map 等状态留存型泄漏。
func TestStressCEP_SustainedLoad_HeapStable(t *testing.T) {
	s := streamsql.New(streamsql.WithBufferSizes(4096, 1024, 256))
	require.NoError(t, s.Execute(cepStressSQL))
	defer s.Stop()

	// 预热（触发分区/sweeper 初始化），再取基线。
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

	t.Logf("持续负载: %d 事件 / %v = %.0f ops/sec", events, dur, float64(events)/dur.Seconds())
	t.Logf("堆: %.2fMB → %.2fMB (delta %.2fMB)",
		float64(heapStart)/1e6, float64(heapEnd)/1e6, float64(int64(heapEnd)-int64(heapStart))/1e6)
	require.Less(t, float64(int64(heapEnd)-int64(heapStart)), 100.0*1e6,
		"堆增量过大，疑为状态留存型泄漏：delta=%.2fMB",
		float64(int64(heapEnd)-int64(heapStart))/1e6)
}

// --- 3. 分区 LRU 驱逐不泄漏 ---

// 5 万个不同分区（远超默认上限 maxPartitions=10000），每分区一条。LRU 驱逐在持续负载下不应
// 泄漏、不应 panic。驻留若随总分区数线性增长即为驱逐失效。
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

	t.Logf("淘汰负载: %d 分区 / %v = %.0f ops/sec", distinct, dur, float64(distinct)/dur.Seconds())
	t.Logf("堆: %.2fMB → %.2fMB (delta %.2fMB)",
		float64(heapStart)/1e6, float64(heapEnd)/1e6, float64(int64(heapEnd)-int64(heapStart))/1e6)
	// 驱逐应把旧分区回收，驻留仅近 LRU 上限个；驻留若随总分区数线性增长即为驱逐失效。
	require.Less(t, float64(int64(heapEnd)-int64(heapStart)), 150.0*1e6,
		"堆随分区数线性增长，疑为 LRU 驱逐未回收：delta=%.2fMB",
		float64(int64(heapEnd)-int64(heapStart))/1e6)
}
