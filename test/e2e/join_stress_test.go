package e2e

import (
	"runtime"
	"testing"
	"time"

	"github.com/rulego/streamsql"
)

// TestStreamJoinStress200kKeysMemoryBounded 压测 20 万 key 内存有界：
// maxKeys=10000（LRU 闸）下灌 20 万不同 key（输入 chan 容量 ≥ 总量，避免 drop
// 策略在入口截流），缓冲 key 数不得超出闸、淘汰计数可观测。
func TestStreamJoinStress200kKeysMemoryBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}
	const total = 200000
	const maxKeys = 10000
	ssql := streamsql.New(
		streamsql.WithDiscardLog(),
		streamsql.WithJoinMaxKeys(maxKeys),
		streamsql.WithBufferSizes(total+1000, 100, 50),
	)
	if err := ssql.Execute(`
		SELECT s.k FROM bigL AS s JOIN bigR AS v WITHIN 5 SECONDS ON s.k = v.k
		WITH (TIMESTAMP = 'ts')`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	defer ssql.Stop()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	ms := time.Now().UnixMilli()
	// 只灌左流：单调整 ts（避免迟到丢弃），各行进缓冲挂 pending。
	for i := 0; i < total; i++ {
		if err := ssql.EmitTo("bigL", map[string]any{"k": i, "ts": ms + int64(i/100)}); err != nil {
			t.Fatalf("EmitTo %d: %v", i, err)
		}
	}
	// 消费完成信号：LRU 淘汰计数 ≥ total − 2×maxKeys（全部涌入后旧 key 必然被淘汰）。
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if ssql.GetStats()["join_stage1_keys_evicted"] >= total-2*maxKeys {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)

	stats := ssql.GetStats()
	if ev := stats["join_stage1_keys_evicted"]; ev < total-2*maxKeys {
		t.Errorf("keys_evicted = %d, want >= %d (LRU must bound key cardinality)", ev, total-2*maxKeys)
	}
	if br := stats["join_stage1_buffered_rows"]; br > 2*maxKeys {
		t.Errorf("buffered_rows = %d exceeds the 2×maxKeys bound", br)
	}
	if id := stats["join_stage1_input_dropped"]; id != 0 {
		t.Errorf("input_dropped = %d, want 0 (channel sized for the burst)", id)
	}
	t.Logf("total-alloc delta = %dMB, buffered=%d keys_evicted=%d",
		int64(after.TotalAlloc-before.TotalAlloc)/(1<<20),
		stats["join_stage1_buffered_rows"], stats["join_stage1_keys_evicted"])
}

// TestStreamJoinThroughputSmoke 吞吐冒烟：双流 5 万行对打（distinct key，
// maxKeys 提到 6 万排除 LRU 干扰），INNER 一一匹配，验证吞吐下零输入丢弃、产出完整。
func TestStreamJoinThroughputSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in -short mode")
	}
	const total = 50000
	sink := &joinResultSink{}
	ssql := streamsql.New(
		streamsql.WithDiscardLog(),
		streamsql.WithBufferSizes(total+1000, 1000, 50),
		streamsql.WithJoinMaxKeys(60000),
	)
	if err := ssql.Execute(`
		SELECT s.k FROM fastL AS s JOIN fastR AS v WITHIN 30 SECONDS ON s.k = v.k
		WITH (TIMESTAMP = 'ts')`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	defer ssql.Stop()
	ssql.AddSink(sink.add)

	ms := time.Now().UnixMilli()
	for i := 0; i < total; i++ {
		if err := ssql.EmitTo("fastL", map[string]any{"k": i, "ts": ms}); err != nil {
			t.Fatalf("EmitTo L: %v", err)
		}
		if err := ssql.EmitTo("fastR", map[string]any{"k": i, "ts": ms}); err != nil {
			t.Fatalf("EmitTo R: %v", err)
		}
	}
	rows := sink.waitN(t, total, 60*time.Second)
	stats := ssql.GetStats()
	if got := stats["join_stage1_input_dropped"]; got != 0 {
		t.Errorf("input_dropped = %d, want 0 (channel sized for the burst)", got)
	}
	if got := stats["join_stage1_keys_evicted"]; got != 0 {
		t.Errorf("keys_evicted = %d, want 0 (cap above cardinality)", got)
	}
	if got := stats["join_stage1_matches_emitted"]; got != total {
		t.Errorf("matches_emitted = %d, want %d", got, total)
	}
	if len(rows) < total {
		t.Errorf("sink received %d rows, want >= %d", len(rows), total)
	}
}
