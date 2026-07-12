package e2e

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/require"
)

// 压力测试：v1.0.3 分析函数路径在持续负载下的稳定性——goroutine 不泄漏、堆不无限增长、
// 无 panic。针对 v1.0.2→v1.0.3（OVER 状态机 / PARTITION BY / splitAnalyticExprMulti）最可能
// 引入的回归。本地普通模式跑（-race 由 CI/Linux 回归）。

// 多次 New→负载→Stop 循环：每次 Stop 应回收全部 sink/处理协程，NumGoroutine 回到基线。
// 捕获“Stop 不收敛”型泄漏——每轮残留 k 个 → 末值 = base + cycles*k，远超基线。
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

// 单实例持续 10 万事件（50 分区，落在默认上限内）：堆增量受控、全程无 panic。
// 同时给出持续吞吐作为性能参考。堆增量若超阈值，疑为每事件或每分区状态留存型泄漏。
func TestStress_SustainedLoad_HeapStable(t *testing.T) {
	s := streamsql.New()
	require.NoError(t, s.Execute(
		`SELECT deviceId, lag(v) OVER (PARTITION BY deviceId) AS p, acc_sum(v) OVER (PARTITION BY deviceId) AS t FROM stream`))
	defer s.Stop()

	// 预热（触发 ensureAnalytic 的 sync.Once 初始化），再取基线。
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

	t.Logf("持续负载: %d 事件 / %v = %.0f ops/sec", events, dur, float64(events)/dur.Seconds())
	t.Logf("堆: %.2fMB → %.2fMB (delta %.2fMB)", float64(heapStart)/1e6, float64(heapEnd)/1e6, float64(int64(heapEnd) - int64(heapStart))/1e6)
	require.Less(t, float64(int64(heapEnd) - int64(heapStart)), 50.0*1e6,
		"堆增量过大，疑为状态留存型泄漏：delta=%.2fMB", float64(int64(heapEnd) - int64(heapStart))/1e6)
}

// 分区数远超默认上限：LRU 驱逐在持续负载下不应泄漏、不应 panic。每轮把最久未用分区淘汰。
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

	// 5 万个不同分区，持续触发淘汰；每分区仅一条，lag 恒为 nil。
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

	t.Logf("淘汰负载: %d 分区 / %v = %.0f ops/sec", distinct, dur, float64(distinct)/dur.Seconds())
	t.Logf("堆: %.2fMB → %.2fMB (delta %.2fMB)", float64(heapStart)/1e6, float64(heapEnd)/1e6, float64(int64(heapEnd) - int64(heapStart))/1e6)
	// 驱逐应把旧分区回收，驻留仅近 LRU 上限个；驻留若随总分区数线性增长即为驱逐失效。
	require.Less(t, float64(int64(heapEnd) - int64(heapStart)), 100.0*1e6,
		"堆随分区数线性增长，疑为 LRU 驱逐未回收：delta=%.2fMB", float64(int64(heapEnd) - int64(heapStart))/1e6)
}

// --- 128MB 网关常见规则容量基准 ---
// 每事件新建 map（贴近真实接入：网关把每条入消息解析成 map）；单流 EmitSync。
// 用 GOMAXPROCS / GOMEMLIMIT 环境变量模拟网关 CPU/内存约束。
// 注意：EmitSync 是同步单 goroutine，单条规则吞吐与核数无关——多核靠并行多实例。

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

// 常见规则1：过滤（最高频）。
func BenchmarkGateway_Filter(b *testing.B) {
	benchGatewayRule(b, `SELECT deviceId, temperature FROM stream WHERE temperature > 25`)
}

// 常见规则2：转换（单位换算）。
func BenchmarkGateway_Transform(b *testing.B) {
	benchGatewayRule(b, `SELECT deviceId, temperature * 1.8 + 32 AS fahrenheit FROM stream`)
}

// 常见规则3：变化检测（分析函数 + 分区）。
func BenchmarkGateway_AnalyticChange(b *testing.B) {
	benchGatewayRule(b, `SELECT deviceId, temperature, lag(temperature) OVER (PARTITION BY deviceId) AS prev FROM stream`)
}

// 并行多实例（多核利用：网关同跑多条规则，每核一条）。RunParallel 随 GOMAXPROCS 扩展，
// 体现多核网关的聚合吞吐。
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
