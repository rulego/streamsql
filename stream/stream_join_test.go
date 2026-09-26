package stream

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/metrics"
	"github.com/rulego/streamsql/types"
)

// ---- 测试基建：可控时钟 + 输出收集器 ----

type joinTestClock struct{ v int64 }

func (c *joinTestClock) now() int64  { return atomic.LoadInt64(&c.v) }
func (c *joinTestClock) set(v int64) { atomic.StoreInt64(&c.v, v) }
func (c *joinTestClock) add(d time.Duration) {
	atomic.AddInt64(&c.v, int64(d))
}

type emittedJoinRow struct {
	row map[string]any
	ts  int64
}

// newTestStage 构造一级二元 runner（直调测试，无生命周期）。
func newTestStage(cfg types.StreamJoinStage, eventTime bool, tsProp string, idle time.Duration, maxKeys, maxRows int) (*joinStageRuntime, *[]emittedJoinRow, *joinTestClock, *metrics.Registry) {
	clock := &joinTestClock{v: int64(1e18)} // epoch ns 量级
	reg := metrics.NewRegistry()
	out := &[]emittedJoinRow{}
	rt := newJoinStageRuntime(joinStageParams{
		idx: 1, cfg: cfg, sourceAlias: "s",
		eventTime: eventTime, tsProp: tsProp, idleTimeout: idle,
		maxKeys: maxKeys, maxRows: maxRows,
		reg: reg, log: logger.NewDiscardLogger(), now: clock.now,
	})
	rt.emit = func(row map[string]any, ts int64) { *out = append(*out, emittedJoinRow{row: row, ts: ts}) }
	return rt, out, clock, reg
}

func stageMetric(t *testing.T, reg *metrics.Registry, name string) int64 {
	t.Helper()
	m, ok := reg.Get(name)
	if !ok {
		t.Fatalf("metric %q not registered", name)
	}
	v, _ := m.SnapshotValue().(int64)
	return v
}

func innerStage(within time.Duration) types.StreamJoinStage {
	return types.StreamJoinStage{
		RightName: "rightStream", RightAlias: "v", JoinType: "INNER",
		OnPairs: []types.JoinOnPair{{StreamField: "k", TableField: "k"}},
		Within:  within,
	}
}

// ---- 语义（processing-time）----

func TestJoinStageInnerMatchProcessingTime(t *testing.T) {
	rt, out, clock, reg := newTestStage(innerStage(30*time.Second), false, "", 0, 0, 0)
	base := clock.now()

	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "temperature": 80}})
	if len(*out) != 0 {
		t.Fatalf("left alone should emit nothing, got %d", len(*out))
	}
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "vibration": 35}})
	if len(*out) != 1 {
		t.Fatalf("match should emit 1 row, got %d", len(*out))
	}
	row := (*out)[0].row
	if row["temperature"] != 80 {
		t.Errorf("left field flat: temperature = %v", row["temperature"])
	}
	if v, ok := row["v"].(map[string]any); !ok || v["vibration"] != 35 {
		t.Errorf("right row under alias: v = %v", row["v"])
	}
	if s, ok := row["s"].(map[string]any); !ok || s["k"] != "d1" {
		t.Errorf("source alias exposure: s = %v", row["s"])
	}

	// 窗内可重复匹配：第二条左行与仍在缓冲的右行再配一对。
	clock.set(base + int64(5*time.Second))
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "temperature": 82}})
	if len(*out) != 2 {
		t.Fatalf("re-match within window should emit, got %d", len(*out))
	}
	if got := stageMetric(t, reg, "join_stage1_matches_emitted"); got != 2 {
		t.Errorf("matches_emitted = %d, want 2", got)
	}
}

func TestJoinStageOneToLeftCartesian(t *testing.T) {
	// 一对多：两条右行都在窗内，一条左行到达应产出两对。
	rt, out, _, _ := newTestStage(innerStage(30*time.Second), false, "", 0, 0, 0)
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "seq": 1}})
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "seq": 2}})
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "cmd": "x"}})
	if len(*out) != 2 {
		t.Fatalf("one-to-many should emit 2 rows, got %d", len(*out))
	}
}

func TestJoinStageDifferentKeyNoMatch(t *testing.T) {
	rt, out, _, _ := newTestStage(innerStage(30*time.Second), false, "", 0, 0, 0)
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "a"}})
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "b"}})
	if len(*out) != 0 {
		t.Fatalf("different keys must not match, got %d", len(*out))
	}
}

func TestJoinStageOutOfWindowProcessingTime(t *testing.T) {
	rt, out, clock, _ := newTestStage(innerStage(30*time.Second), false, "", 0, 0, 0)
	base := clock.now()
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1"}})
	clock.set(base + int64(31*time.Second)) // 右行到达时左行已在窗外
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1"}})
	if len(*out) != 0 {
		t.Fatalf("out-of-window rows must not match, got %d", len(*out))
	}
}

func TestJoinStageOutOfOrderWithinWindow(t *testing.T) {
	// 乱序到达可配上：processing-time 下到达时刻都在窗内即可。
	rt, out, clock, _ := newTestStage(innerStage(30*time.Second), false, "", 0, 0, 0)
	base := clock.now()
	clock.set(base + int64(10*time.Second))
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "seq": 1}}) // 右先到
	clock.set(base + int64(20*time.Second))
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "cmd": "x"}}) // 左后到
	if len(*out) != 1 {
		t.Fatalf("out-of-order arrival within window should match, got %d", len(*out))
	}
}

func TestJoinStageKeyNormalization(t *testing.T) {
	rt, out, _, _ := newTestStage(innerStage(30*time.Second), false, "", 0, 0, 0)
	// int 与 float64 数值键归一相等。
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": 1}})
	rt.processRightMsg(joinMsg{row: map[string]any{"k": 1.0}})
	if len(*out) != 1 {
		t.Fatalf("int/float64 keys should match, got %d", len(*out))
	}
	// 同形字符串不与数值混。
	rt2, out2, _, _ := newTestStage(innerStage(30*time.Second), false, "", 0, 0, 0)
	rt2.processLeftMsg(joinMsg{row: map[string]any{"k": "1"}})
	rt2.processRightMsg(joinMsg{row: map[string]any{"k": 1}})
	if len(*out2) != 0 {
		t.Fatalf("string/int keys must not match, got %d", len(*out2))
	}
}

func TestJoinStageCompositeKey(t *testing.T) {
	cfg := types.StreamJoinStage{
		RightName: "face", RightAlias: "f", JoinType: "INNER",
		OnPairs: []types.JoinOnPair{{StreamField: "gateId", TableField: "gateId"}, {StreamField: "userId", TableField: "userId"}},
		Within:  5 * time.Second,
	}
	rt, out, _, _ := newTestStage(cfg, false, "", 0, 0, 0)
	rt.processLeftMsg(joinMsg{row: map[string]any{"gateId": "g1", "userId": "u1"}})
	rt.processRightMsg(joinMsg{row: map[string]any{"gateId": "g1", "userId": "u2", "score": 91}})
	if len(*out) != 0 {
		t.Fatalf("composite key mismatch must not match, got %d", len(*out))
	}
	rt.processRightMsg(joinMsg{row: map[string]any{"gateId": "g1", "userId": "u1", "score": 92}})
	if len(*out) != 1 {
		t.Fatalf("composite key match should emit 1, got %d", len(*out))
	}
}

// ---- 语义（event-time，TsProp）----

func TestJoinStageEventTimeProximity(t *testing.T) {
	rt, out, _, reg := newTestStage(innerStage(5*time.Second), true, "ts", 0, 0, 0)
	base := int64(1e18)
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base}})                                // 事件 ts=base
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base + 3*time.Second.Nanoseconds()}}) // 邻近 3s ≤ 5s
	if len(*out) != 1 {
		t.Fatalf("ts proximity within WITHIN should match, got %d", len(*out))
	}
	// 与 processing-time 的差异用例：同行到达但事件 ts 差 > WITHIN 不匹配。
	rt2, out2, _, _ := newTestStage(innerStage(5*time.Second), true, "ts", 0, 0, 0)
	rt2.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base}})
	rt2.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base + 6*time.Second.Nanoseconds()}})
	if len(*out2) != 0 {
		t.Fatalf("ts diff > WITHIN must not match even when arriving together, got %d", len(*out2))
	}
	// 输出 ts = max(两匹配行 ts)。
	if got := (*out)[0].ts; got != base+3*time.Second.Nanoseconds() {
		t.Errorf("emitted ts = %d, want max(L,R) = %d", got, base+3*time.Second.Nanoseconds())
	}
	if got := stageMetric(t, reg, "join_stage1_matches_emitted"); got != 1 {
		t.Errorf("matches = %d", got)
	}
}

func TestJoinStageEventTimeTsUnitNormalization(t *testing.T) {
	// 毫秒 epoch 事件时间应与秒级 WITHIN 正确比较。
	rt, out, _, _ := newTestStage(innerStage(5*time.Second), true, "ts", 0, 0, 0)
	msBase := int64(1.7e12) // ms epoch
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "ts": msBase}})
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "ts": msBase + 3000}}) // +3s（毫秒单位）
	if len(*out) != 1 {
		t.Fatalf("ms-epoch ts should normalize to ns and match, got %d", len(*out))
	}
}

func TestJoinStageEventTimeLateDropped(t *testing.T) {
	rt, out, _, reg := newTestStage(innerStage(5*time.Second), true, "ts", 0, 0, 0)
	base := int64(1e18)
	// 右侧水位先推进 30s，随后到达的旧行按迟到丢弃（迟到=对本侧水位超 WITHIN）。
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base + 30*time.Second.Nanoseconds()}})
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base}}) // 迟到 30s > 5s
	if len(*out) != 0 {
		t.Fatalf("late row must not match, got %d", len(*out))
	}
	if got := stageMetric(t, reg, "join_stage1_late_dropped"); got != 1 {
		t.Errorf("late_dropped = %d, want 1", got)
	}
	// 左行被同侧水位推进懒清理后，旧行同样迟到。
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base}})
	if len(*out) != 0 {
		t.Fatalf("no match expected, got %d", len(*out))
	}
}

func TestJoinStageEventTimeMissingTsField(t *testing.T) {
	rt, out, _, reg := newTestStage(innerStage(5*time.Second), true, "ts", 0, 0, 0)
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1"}}) // TsProp 字段缺失
	if len(*out) != 0 {
		t.Fatalf("no output expected")
	}
	if got := stageMetric(t, reg, "join_stage1_late_dropped"); got != 1 {
		t.Errorf("late_dropped = %d, want 1", got)
	}
}

// ---- LEFT 语义 ----

func TestJoinStageLeftNullOnWindowCloseProcessingTime(t *testing.T) {
	cfg := innerStage(10 * time.Second)
	cfg.JoinType = "LEFT"
	rt, out, clock, reg := newTestStage(cfg, false, "", 0, 0, 0)
	base := clock.now()
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "cmdId": "c1"}})
	// 墙钟推进超窗后 sweeper 补发 NULL。
	clock.set(base + int64(11*time.Second))
	rt.sweepTick()
	if len(*out) != 1 {
		t.Fatalf("LEFT NULL should be emitted after window close, got %d", len(*out))
	}
	row := (*out)[0].row
	if row["cmdId"] != "c1" {
		t.Errorf("left fields flat: %v", row)
	}
	if v, ok := row["v"].(map[string]any); !ok || len(v) != 0 {
		t.Errorf("right alias should be empty row for NULL complement, got %v", row["v"])
	}
	// 幂等：行已驱逐，再扫不重复补发。
	rt.sweepTick()
	if len(*out) != 1 {
		t.Fatalf("LEFT NULL must be emitted exactly once, got %d", len(*out))
	}
	if got := stageMetric(t, reg, "join_stage1_left_timeout_emitted"); got != 1 {
		t.Errorf("left_timeout_emitted = %d, want 1", got)
	}
}

func TestJoinStageLeftMatchNoNull(t *testing.T) {
	cfg := innerStage(10 * time.Second)
	cfg.JoinType = "LEFT"
	rt, out, clock, reg := newTestStage(cfg, false, "", 0, 0, 0)
	base := clock.now()
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1"}})
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "ack": 1}})
	if len(*out) != 1 {
		t.Fatalf("match should emit 1")
	}
	clock.set(base + int64(11*time.Second))
	rt.sweepTick()
	if len(*out) != 1 {
		t.Fatalf("matched LEFT row must not emit NULL, got %d", len(*out))
	}
	if got := stageMetric(t, reg, "join_stage1_left_timeout_emitted"); got != 0 {
		t.Errorf("left_timeout_emitted = %d, want 0", got)
	}
}

// TestJoinStageExpiredRowNotMatchable 锁定"过期即不再匹配"：懒清理门控滑动缝内
// （行已过保留期但尚未被驱逐），对侧迟到行满足 |Δts|≤WITHIN 也不产出匹配——
// 该行的出口是驱逐 / LEFT 补 NULL，与声明的保留期语义严格一致。
func TestJoinStageExpiredRowNotMatchable(t *testing.T) {
	newStage := func(joinType string) (*joinStageRuntime, *[]emittedJoinRow, *joinTestClock, *metrics.Registry) {
		cfg := innerStage(5 * time.Second)
		cfg.JoinType = joinType
		return newTestStage(cfg, true, "ts", 0, 0, 0)
	}

	for _, joinType := range []string{"LEFT", "INNER"} {
		rt, out, clock, reg := newStage(joinType)
		base := int64(1e18)

		// 左 d1 ts=base（首到行触发懒清理：sweptTs=base，缓冲空）。
		rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base}})
		// 左 d2 ts=base+5s：增量 5s ≥ W/4 → 触发清理；d1 过期度恰好 5s 不 > 5s，保留。
		rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d2", "ts": base + int64(5*time.Second)}})
		// 左 d3 ts=base+5.4s：增量 0.4s < W/4 → 门控跳过清理；d1 过期度 5.4s > 5s
		// ——已过保留期但仍在缓冲（清理缝隙）。
		rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d3", "ts": base + int64(int64(5*time.Second)+int64(400*time.Millisecond))}})

		// 右 d1 ts=base+5s：与 d1 行 |Δ|=5s ≤ WITHIN（含边界）满足匹配谓词，
		// 但 d1 已过本侧保留期 → 不得产出匹配。
		rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base + int64(5*time.Second)}})
		if got := stageMetric(t, reg, "join_stage1_matches_emitted"); got != 0 {
			t.Fatalf("[%s] expired row must not match: matches_emitted = %d", joinType, got)
		}
		if len(*out) != 0 {
			t.Fatalf("[%s] no output expected before expiry, got %d rows", joinType, len(*out))
		}

		// sweeper（event-time 不推进水位）按当前水位回收：d1 过期驱逐；
		// LEFT 补一次 NULL，INNER 静默。
		clock.add(5 * time.Second)
		rt.sweepTick()

		if got := stageMetric(t, reg, "join_stage1_matches_emitted"); got != 0 {
			t.Fatalf("[%s] expired row must stay unmatched after sweep: %d", joinType, got)
		}
		switch joinType {
		case "LEFT":
			if got := stageMetric(t, reg, "join_stage1_left_timeout_emitted"); got != 1 {
				t.Fatalf("[LEFT] exactly one NULL complement for the expired row, got %d", got)
			}
			if len(*out) != 1 {
				t.Fatalf("[LEFT] NULL row expected, got %d rows", len(*out))
			}
			if v, ok := (*out)[0].row["v"].(map[string]any); !ok || len(v) != 0 {
				t.Fatalf("[LEFT] NULL complement shape wrong: %v", (*out)[0].row["v"])
			}
		case "INNER":
			if got := stageMetric(t, reg, "join_stage1_left_timeout_emitted"); got != 0 {
				t.Fatalf("[INNER] INNER must not complement NULL, got %d", got)
			}
			if len(*out) != 0 {
				t.Fatalf("[INNER] expired row expires silently, got %d rows", len(*out))
			}
		}
	}
}

func TestJoinStageLeftNullByLazyExpireEventTime(t *testing.T) {
	cfg := innerStage(5 * time.Second)
	cfg.JoinType = "LEFT"
	rt, out, _, _ := newTestStage(cfg, true, "ts", 0, 0, 0)
	base := int64(1e18)
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base}})
	// 同侧后到行把水位推过 base+5s：懒清理补发 NULL（单一出口）。
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d2", "ts": base + 6*time.Second.Nanoseconds()}})
	if len(*out) != 1 {
		t.Fatalf("lazy expire should emit LEFT NULL, got %d", len(*out))
	}
	if v, ok := (*out)[0].row["v"].(map[string]any); !ok || len(v) != 0 {
		t.Errorf("NULL complement shape wrong: %v", (*out)[0].row["v"])
	}
}

func TestJoinStageLeftNullIdempotentAcrossSweeperAndLazy(t *testing.T) {
	// 懒清理与 sweeper 共用行级 matched/驱逐单一出口：不重复、不漏发。
	cfg := innerStage(5 * time.Second)
	cfg.JoinType = "LEFT"
	rt, out, clock, _ := newTestStage(cfg, false, "", 0, 0, 0)
	base := clock.now()
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1"}})
	clock.set(base + int64(6*time.Second))
	rt.sweepTick()
	rt.sweepTick()
	clock.set(base + int64(20*time.Second))
	// 右侧行到达触发右侧懒清理（不应触碰左侧未到期行……已到期但已驱逐）。
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1"}})
	if len(*out) != 1 {
		t.Fatalf("LEFT NULL exactly once across sweeper+lazy paths, got %d", len(*out))
	}
}

func TestJoinStageFlushLeft(t *testing.T) {
	cfg := innerStage(10 * time.Second)
	cfg.JoinType = "LEFT"
	rt, out, _, reg := newTestStage(cfg, false, "", 0, 0, 0)
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1"}})
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d2"}})
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1"}}) // d1 已匹配
	rt.flushLeft()
	if len(*out) != 2 {
		t.Fatalf("flush should emit matched row + unmatched NULL, got %d", len(*out))
	}
	if rt.left.rows != 0 || rt.right.rows != 0 {
		t.Errorf("buffers should be cleared after flush: left=%d right=%d", rt.left.rows, rt.right.rows)
	}
	if got := stageMetric(t, reg, "join_stage1_left_timeout_emitted"); got != 1 {
		t.Errorf("left_timeout_emitted = %d, want 1 (only unmatched)", got)
	}
	// 再 flush 不重复。
	rt.flushLeft()
	if len(*out) != 2 {
		t.Fatalf("second flush must not re-emit, got %d", len(*out))
	}
}

// ---- IdleTimeout ----

func TestJoinStageIdleTimeoutAdvancesWatermark(t *testing.T) {
	cfg := innerStage(5 * time.Second)
	cfg.JoinType = "LEFT"
	rt, out, clock, _ := newTestStage(cfg, true, "ts", 10*time.Second, 0, 0)
	base := clock.now()
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "ts": base}})
	// 双侧都停发：无 IdleTimeout 时水位冻结（event-time），NULL 不发。
	clock.set(base + int64(6*time.Second))
	rt.sweepTick()
	if len(*out) != 0 {
		t.Fatalf("before idle timeout, event-time watermark stays frozen, got %d", len(*out))
	}
	// 空闲超时（10s）后水位推进到墙钟，超窗 pending 补发。
	clock.set(base + int64(11*time.Second))
	rt.sweepTick()
	if len(*out) != 1 {
		t.Fatalf("idle timeout should advance watermark and emit LEFT NULL, got %d", len(*out))
	}
}

// ---- 资源边界 ----

func TestJoinStageMaxRowsDrops(t *testing.T) {
	rt, out, _, reg := newTestStage(innerStage(30*time.Second), false, "", 0, 0, 2)
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "seq": 1}})
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "seq": 2}})
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "d1", "seq": 3}}) // 超 maxRows=2 丢弃
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "d1"}})
	if len(*out) != 2 {
		t.Fatalf("maxRows caps buffered rows, got %d matches", len(*out))
	}
	if got := stageMetric(t, reg, "join_stage1_rows_dropped"); got != 1 {
		t.Errorf("rows_dropped = %d, want 1", got)
	}
}

func TestJoinStageMaxKeysEvictsPendingLeft(t *testing.T) {
	cfg := innerStage(30 * time.Second)
	cfg.JoinType = "LEFT"
	rt, out, _, reg := newTestStage(cfg, false, "", 0, 1, 0)
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "a"}})  // 占住唯一 key
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "b"}})  // 淘汰 a（含 pending LEFT，不补发）
	rt.processRightMsg(joinMsg{row: map[string]any{"k": "a"}}) // a 已不在缓冲
	rt.sweepTick()
	if len(*out) != 0 {
		t.Fatalf("evicted pending LEFT must not emit (documented tradeoff), got %d", len(*out))
	}
	if got := stageMetric(t, reg, "join_stage1_keys_evicted"); got != 1 {
		t.Errorf("keys_evicted = %d, want 1", got)
	}
}

func TestJoinStageGauges(t *testing.T) {
	rt, _, _, reg := newTestStage(innerStage(30*time.Second), true, "ts", 0, 0, 0)
	base := int64(1e18)
	rt.processLeftMsg(joinMsg{row: map[string]any{"k": "a", "ts": base + 5}})
	if got := stageMetric(t, reg, "join_stage1_buffered_rows"); got != 1 {
		t.Errorf("buffered_rows = %d, want 1", got)
	}
	if got := stageMetric(t, reg, "join_stage1_left_watermark"); got != base+5 {
		t.Errorf("left_watermark = %d, want %d", got, base+5)
	}
	if got := stageMetric(t, reg, "join_stage1_right_watermark"); got != 0 {
		t.Errorf("right_watermark = %d, want 0", got)
	}
}

// ---- 级联（直调组合，全链路 e2e 在集成测试）----

// newTestCascade 组装两级 runner：级 1 输出经内联回调流入级 2 左入口（模拟
// flushMode 直调路径；正常模式下经级间 chan，行为等价——扫描/入缓冲逻辑相同）。
func newTestCascade(stage1, stage2 types.StreamJoinStage, eventTime bool, tsProp string) (*joinStageRuntime, *joinStageRuntime, *[]emittedJoinRow, *joinTestClock) {
	clock := &joinTestClock{v: int64(1e18)}
	reg := metrics.NewRegistry()
	out := &[]emittedJoinRow{}
	rt1 := newJoinStageRuntime(joinStageParams{idx: 1, cfg: stage1, sourceAlias: "a", eventTime: eventTime, tsProp: tsProp, reg: reg, log: logger.NewDiscardLogger(), now: clock.now})
	rt2 := newJoinStageRuntime(joinStageParams{idx: 2, cfg: stage2, sourceAlias: "a", eventTime: eventTime, tsProp: tsProp, reg: reg, log: logger.NewDiscardLogger(), now: clock.now})
	rt1.emit = func(row map[string]any, ts int64) {
		rt2.processLeftMsg(joinMsg{row: row, ts: ts, explicit: eventTime})
	}
	rt2.emit = func(row map[string]any, ts int64) { *out = append(*out, emittedJoinRow{row: row, ts: ts}) }
	return rt1, rt2, out, clock
}

func cascadeStages() (types.StreamJoinStage, types.StreamJoinStage) {
	s1 := types.StreamJoinStage{
		RightName: "b", RightAlias: "b", JoinType: "INNER",
		// 级 1 左输入是 FROM 原始行：键是剥掉别名的裸字段（与 parser 产物一致）。
		OnPairs: []types.JoinOnPair{{StreamField: "k", TableField: "k"}}, Within: 5 * time.Second,
	}
	s2 := types.StreamJoinStage{
		RightName: "c", RightAlias: "c", JoinType: "INNER",
		// 级 2 左输入是复合行：键是 "b.k2" 前缀路径（fieldpath 解析）。
		OnPairs: []types.JoinOnPair{{StreamField: "b.k2", TableField: "k2"}}, Within: 5 * time.Second,
	}
	return s1, s2
}

func TestJoinCascadeThreeStreams(t *testing.T) {
	s1, s2 := cascadeStages()
	rt1, rt2, out, _ := newTestCascade(s1, s2, true, "ts")
	base := int64(1e18)
	rt1.processLeftMsg(joinMsg{row: map[string]any{"k": "x", "va": 1, "ts": base}})
	rt1.processRightMsg(joinMsg{row: map[string]any{"k": "x", "k2": "y", "vb": 2, "ts": base}})
	// 复合行已流入级 2 左侧；c 到达后三流合一。
	rt2.processRightMsg(joinMsg{row: map[string]any{"k2": "y", "vc": 3, "ts": base}})
	if len(*out) != 1 {
		t.Fatalf("3-stream cascade should emit 1 row, got %d", len(*out))
	}
	row := (*out)[0].row
	if row["va"] != 1 {
		t.Errorf("FROM fields flat: %v", row)
	}
	if b, ok := row["b"].(map[string]any); !ok || b["vb"] != 2 {
		t.Errorf("stage1 right under alias b: %v", row["b"])
	}
	if c, ok := row["c"].(map[string]any); !ok || c["vc"] != 3 {
		t.Errorf("stage2 right under alias c: %v", row["c"])
	}
}

func TestJoinCascadeIntermediateTsMax(t *testing.T) {
	// 中间行 ts = max(a.ts, b.ts)：c 距较新行 b 在 W2 内、距较旧行 a 可达 W1+W2。
	s1, s2 := cascadeStages()
	rt1, rt2, out, _ := newTestCascade(s1, s2, true, "ts")
	base := int64(1e18)
	rt1.processLeftMsg(joinMsg{row: map[string]any{"k": "x", "ts": base}})                                           // a.ts = base
	rt1.processRightMsg(joinMsg{row: map[string]any{"k": "x", "k2": "y", "ts": base + 4*time.Second.Nanoseconds()}}) // b.ts = base+4s（W1=5s 内）
	// 复合行 ts = max = base+4s。c.ts = base+8s：距 b 4s ≤ W2(5s) → 匹配。
	// （若中间行 ts 取 min=base，c 距 a 8s > W2 → 不匹配——本用例固定 max 裁定。）
	rt2.processRightMsg(joinMsg{row: map[string]any{"k2": "y", "ts": base + 8*time.Second.Nanoseconds()}})
	if len(*out) != 1 {
		t.Fatalf("cascade with ts=max should match, got %d", len(*out))
	}
	// 反证：c 距 b 超 W2 → 不匹配（ts=base+4s 的复合行已超 [c-W2, c+W2]）。
	rt1b, rt2b, outb, _ := newTestCascade(s1, s2, true, "ts")
	rt1b.processLeftMsg(joinMsg{row: map[string]any{"k": "x", "ts": base}})
	rt1b.processRightMsg(joinMsg{row: map[string]any{"k": "x", "k2": "y", "ts": base + 4*time.Second.Nanoseconds()}})
	rt2b.processRightMsg(joinMsg{row: map[string]any{"k2": "y", "ts": base + 10*time.Second.Nanoseconds()}})
	if len(*outb) != 0 {
		t.Fatalf("c beyond W2 of the composite row must not match, got %d", len(*outb))
	}
}

func TestJoinCascadeFlushOrder(t *testing.T) {
	// Stop 按级联序 Flush：级 1 pending LEFT 的 NULL 行先流入级 2，
	// 参与级 2 匹配（此处无 c 匹配 → 级 2 也 LEFT → 再补 NULL）。
	s1, s2 := cascadeStages()
	s2.JoinType = "LEFT"
	rt1, rt2, out, _ := newTestCascade(s1, s2, true, "ts")
	base := int64(1e18)
	rt1.processLeftMsg(joinMsg{row: map[string]any{"k": "x", "va": 1, "ts": base}})
	// 无 b：级 1 INNER 无 pending 概念；改级 1 为 LEFT 验证 NULL 流入级 2。
	s1L, _ := cascadeStages()
	s1L.JoinType = "LEFT"
	rt1L, rt2L, outL, _ := newTestCascade(s1L, s2, true, "ts")
	rt1L.processLeftMsg(joinMsg{row: map[string]any{"k": "x", "va": 1, "ts": base}})
	rt1L.flushLeft() // 上级 Flush：NULL 补发行内联流入级 2
	rt2L.flushLeft() // 下级 Flush：流入的复合行未匹配 → 再补 NULL
	if len(*outL) != 1 {
		t.Fatalf("cascade flush should surface the upstream NULL through stage2 LEFT, got %d", len(*outL))
	}
	row := (*outL)[0].row
	if row["va"] != 1 {
		t.Errorf("FROM fields preserved: %v", row)
	}
	if b, ok := row["b"].(map[string]any); !ok || len(b) != 0 {
		t.Errorf("stage1 NULL complement under b: %v", row["b"])
	}
	if c, ok := row["c"].(map[string]any); !ok || len(c) != 0 {
		t.Errorf("stage2 NULL complement under c: %v", row["c"])
	}
	_ = rt1
	_ = rt2
	_ = out
}
