package cep

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// CEP stress testing (engine core layer, synchronous process): directly tune the engine.Process bypasses the streamsql async channel,
// Precise timing control and heap sampling. WITHIN sweeper active expiration + throughput benchmark.
// Local normal mode running (-race reverts from CI/Linux).

func readHeap() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// countRuns counts the number of matches for active parts in all partitions (p.runs (objects cleaned by sweep). The same packet can access private fields.
func (e *Engine) countRuns() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for el := e.lru.Front(); el != nil; el = el.Next() {
		n += len(el.Value.(*partition).runs)
	}
	return n
}

// --- WITHIN sweeper Actively expired ---

// The superwindow part of the free partition must be actively cleared by the sweeper (not passively cleared only from the next event).
// Multi-partition fed one A per partition (A equals B dead, run is an incomplete intermediate state, residing p.runs does not enter pending);
// ts uses epoch (UnixMilli) to make the wall-clock check for sweep valid. After Sleep > WITHIN + multiple sweep cycles,
// Directly reading p.runs counts asserts that the sweeper has been emptied (precise, independent of heap noise).
// If sweeper fails: Idle with no new events triggers passive clearing, run retention, runsAfter == runsBefore.
func TestStressCEP_WithinSweeper_ActiveExpiry(t *testing.T) {
	// B is inherently false (v<0, the fed v=60 is never satisfied): run Wait for B after matching A, enter the intermediate state of 'waiting for B' (no accept),
	// Therefore, follow survivors → p.runs, not completions/pending (pending is not cleared by sweep).
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

	// Install one A per partition: seq(A,B) will kill the previous run waiting for B, so only one run resides in each partition.
	// Use multi-partition stacking to produce a large number of dwell runs (each run is an independent intermediate state equal to B).
	const partitions = 3000
	ts := time.Now().UnixMilli()
	for p := 0; p < partitions; p++ {
		e.Process(map[string]any{"ts": ts, "v": 60.0}, strconv.Itoa(p))
	}

	runsBefore := e.countRuns()
	heap1 := readHeap()
	if runsBefore == 0 {
		t.Fatal("Pre-installation failure: No resident run after A installation, unable to verify sweeper")
	}

	// Sleep far exceeds WITHIN (200ms) and covers multiple sweep cycles (sweepInterval ≈100ms),
	// Allows the sweeper to actively sweep and clear the overwindow run of the free partition.
	time.Sleep(700 * time.Millisecond)

	runsAfter := e.countRuns()
	heap2 := readHeap()

	t.Logf("sweeper Actively expired: p.runs %d → %d; heap %.2fMB → %.2fMB",
		runsBefore, runsAfter, float64(heap1)/1e6, float64(heap2)/1e6)
	// sweeper activates: the overwindow run is actively cleared (runsAfter should be 0).
	if runsAfter != 0 {
		t.Fatalf("sweeper Failure to proactively clear idle partitions and overrun windows run:p.runs %d → %d (should be cleared)", runsBefore, runsAfter)
	}
}

// --- Throughput benchmark (synchronized process, tests the engine's true throughput, unaffected by async channels)---

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

// Sequential pattern A B (the most common CEP path).
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

// Asterisk: Greedy A* B (for greedy/pending, choose the longest path).
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

// Partition order mode (PARTITION BY + partition key calculates overhead).
func BenchmarkCEP_Partitioned(b *testing.B) {
	benchCep(b, &types.MatchRecognizeSpec{
		Pattern:     seq(lit("A"), lit("B")),
		Defines:     []types.MatchDefine{def("A", "v > 50"), def("B", "v < 50")},
		OrderBy:     orderBy("ts"),
		Measures:    []types.Measure{measure("MATCH_NUMBER()", "mn")},
		PartitionBy: []string{"deviceId"},
	}, func(i int) string { return "dev-" + strconv.Itoa(i%4) },
		func(i int) map[string]any {
			// v alternates by (i/4)%2: i, i+4, i+8 in the same partition... v alternates between A and B (if i%2,
			// dev-0/2 only comes to A, dev-1/3 only B→ zero-match emit, tested for seed + death rather than dense matching).
			v := 60.0
			if (i/4)%2 == 1 {
				v = 40.0
			}
			return map[string]any{"deviceId": "dev-" + strconv.Itoa(i%4), "ts": int64(i), "v": v}
		})
}
