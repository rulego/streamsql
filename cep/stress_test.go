package cep

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// CEP 压力测试（引擎核心层，同步 Process）：直接调 engine.Process 绕过 streamsql async 通道，
// 精确控制时序与堆采样。WITHIN sweeper 主动过期 + 吞吐 benchmark。
// 本地普通模式跑（-race 由 CI/Linux 回归）。

func readHeap() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// countRuns 统计所有分区 p.runs 里的活跃部分匹配数（sweep 清理的对象）。同包可访问私有字段。
func (e *Engine) countRuns() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for el := e.lru.Front(); el != nil; el = el.Next() {
		n += len(el.Value.(*partition).runs)
	}
	return n
}

// --- WITHIN sweeper 主动过期 ---

// 空闲分区的超窗部分匹配必须被 sweeper 主动清（不能只靠下一事件被动清）。
// 多分区每分区灌 1 个 A（A 等 B 死等，run 是未 complete 的中间态，驻留 p.runs 不进 pending）；
// ts 用 epoch（UnixMilli）使 sweep 的 wall-clock 判定生效。Sleep > WITHIN + 多个 sweep 周期后，
// 直接读 p.runs 计数断言 sweeper 已清空（精确，不依赖堆噪声）。
// 若 sweeper 失效：空闲无新事件触发被动清，run 驻留，runsAfter == runsBefore。
func TestStressCEP_WithinSweeper_ActiveExpiry(t *testing.T) {
	// B 恒假（v<0，灌的 v=60 永不满足）：run 匹配 A 后死等 B，进「等B」中间态（无 accept），
	// 故走 survivors → p.runs，不进 completions/pending（pending 不由 sweep 清）。
	spec := &types.MatchRecognizeSpec{
		Pattern:  seq(lit("A"), lit("B")),
		Defines:  []types.MatchDefine{def("A", "v > 50"), def("B", "v < 0")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn")},
		Within:   200 * time.Millisecond,
	}
	e, err := NewEngine(spec)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	e.Start()
	defer e.Stop()

	// 每分区灌 1 个 A：seq(A,B) 下后续 A 会杀前一个等 B 的 run，故每分区仅留 1 run 驻留。
	// 用多分区堆出大量驻留 run（每 run 是独立的等 B 中间态）。
	const partitions = 3000
	ts := time.Now().UnixMilli()
	for p := 0; p < partitions; p++ {
		e.Process(map[string]any{"ts": ts, "v": 60.0}, strconv.Itoa(p))
	}

	runsBefore := e.countRuns()
	heap1 := readHeap()
	if runsBefore == 0 {
		t.Fatal("前置失败：灌 A 后无驻留 run，无法验证 sweeper")
	}

	// Sleep 远超 WITHIN（200ms）且覆盖多个 sweep 周期（sweepInterval≈100ms），
	// 让 sweeper 主动扫并清空闲分区的超窗 run。
	time.Sleep(700 * time.Millisecond)

	runsAfter := e.countRuns()
	heap2 := readHeap()

	t.Logf("sweeper 主动过期: p.runs %d → %d；heap %.2fMB → %.2fMB",
		runsBefore, runsAfter, float64(heap1)/1e6, float64(heap2)/1e6)
	// sweeper 生效：超窗 run 被主动清空（runsAfter 应为 0）。
	if runsAfter != 0 {
		t.Fatalf("sweeper 未主动清空闲分区超窗 run：p.runs %d → %d（应清空）", runsBefore, runsAfter)
	}
}

// --- 吞吐 benchmark（同步 Process，测引擎真实吞吐，不受 async 通道影响）---

func benchCep(b *testing.B, spec *types.MatchRecognizeSpec, key func(i int) string, row func(i int) map[string]any) {
	b.Helper()
	e, err := NewEngine(spec)
	if err != nil {
		b.Fatalf("NewEngine: %v", err)
	}
	defer e.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Process(row(i), key(i))
	}
}

// 顺序模式 A B（最常见 CEP 路径）。
func BenchmarkCEP_Sequence(b *testing.B) {
	benchCep(b, &types.MatchRecognizeSpec{
		Pattern:  seq(lit("A"), lit("B")),
		Defines:  []types.MatchDefine{def("A", "v > 50"), def("B", "v < 50")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn")},
	}, func(int) string { return "" },
		func(i int) map[string]any {
			v := 60.0
			if i%2 == 1 {
				v = 40.0
			}
			return map[string]any{"ts": int64(i), "v": v}
		})
}

// 星号贪婪 A* B（测 greedy/pending 选最长路径）。
func BenchmarkCEP_Star(b *testing.B) {
	benchCep(b, &types.MatchRecognizeSpec{
		Pattern:  seq(rep(lit("A"), 0, -1), lit("B")),
		Defines:  []types.MatchDefine{def("A", "v > 50"), def("B", "v < 50")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn")},
	}, func(int) string { return "" },
		func(i int) map[string]any {
			v := 60.0
			if i%2 == 1 {
				v = 40.0
			}
			return map[string]any{"ts": int64(i), "v": v}
		})
}

// 分区顺序模式（PARTITION BY + 分区键计算开销）。
func BenchmarkCEP_Partitioned(b *testing.B) {
	benchCep(b, &types.MatchRecognizeSpec{
		Pattern:    seq(lit("A"), lit("B")),
		Defines:    []types.MatchDefine{def("A", "v > 50"), def("B", "v < 50")},
		OrderBy:    orderBy("ts"),
		Measures:   []types.Measure{measure("MATCH_NUMBER()", "mn")},
		PartitionBy: []string{"deviceId"},
	}, func(i int) string { return "dev-" + strconv.Itoa(i%4) },
		func(i int) map[string]any {
			// v 按 (i/4)%2 交替：同分区事件 i, i+4, i+8… 的 v 才 A B 交替（若按 i%2，
			// dev-0/2 区只来 A、dev-1/3 区只来 B → 零匹配 emit，测的是种子+死而非密集匹配）。
			v := 60.0
			if (i/4)%2 == 1 {
				v = 40.0
			}
			return map[string]any{"deviceId": "dev-" + strconv.Itoa(i%4), "ts": int64(i), "v": v}
		})
}
