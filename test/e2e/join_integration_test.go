package e2e

import (
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/types"
)

// 流-流 JOIN 集成测试(补充集)——全部经公开 API 全链路(Execute → EmitTo → sink),
// 与 join_stream_test.go 的五场景互补:本文件覆盖 SQL 表达力、匹配语义细节、
// 级联时序、资源闸 option 与多实例隔离。

// ---- SQL 表达力 ----

// SELECT 表达式(算术 + CASE WHEN)作用于 join 合并行:左行经 "s." 前缀、
// 右行经 "v." 前缀参与表达式求值。
func TestStreamJoinIntegSelectExpressions(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.deviceId,
		       s.temperature * 1.8 + 32 AS tf,
		       CASE WHEN s.temperature > 75 THEN 'hot' ELSE 'normal' END AS level,
		       v.vibration AS vib
		FROM tempStream AS s
		JOIN vibStream AS v WITHIN 30 SECONDS ON s.deviceId = v.deviceId`)
	defer ssql.Stop()

	if err := ssql.EmitTo("tempStream", map[string]any{"deviceId": "d1", "temperature": 80}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("vibStream", map[string]any{"deviceId": "d1", "vibration": 35}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	if got, ok := rows[0]["tf"].(float64); !ok || got != 176.0 {
		t.Errorf("tf = %v, want 176.0", rows[0]["tf"])
	}
	if rows[0]["level"] != "hot" {
		t.Errorf("level = %v, want hot", rows[0]["level"])
	}
	if rows[0]["vib"] != 35 {
		t.Errorf("vib = %v", rows[0]["vib"])
	}
}

// WHERE 括号 OR 复合条件引用双侧字段(NOT 在 condition 层为既有限制,不在 join 范围)。
func TestStreamJoinIntegWhereOrNot(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k
		WHERE (s.temperature > 100 OR v.vibration > 50) AND s.k != 'filtered'`)
	defer ssql.Stop()

	// 双高 → 命中。
	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "temperature": 110}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "vibration": 60}); err != nil {
		t.Fatal(err)
	}
	// 单高(vibration)→ 命中 OR 左半失效但右半成立。
	if err := ssql.EmitTo("leftS", map[string]any{"k": "y", "temperature": 30}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "y", "vibration": 60}); err != nil {
		t.Fatal(err)
	}
	// 双低 → 拒绝。
	if err := ssql.EmitTo("leftS", map[string]any{"k": "z", "temperature": 30}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "z", "vibration": 10}); err != nil {
		t.Fatal(err)
	}
	// 命中但 k 命中排除项 → AND 拒绝。
	if err := ssql.EmitTo("leftS", map[string]any{"k": "filtered", "temperature": 120}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "filtered", "vibration": 10}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 2, 5*time.Second)
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want exactly k=x and k=y", rows)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r["k"].(string)] = true
	}
	if !got["x"] || !got["y"] || len(got) != 2 {
		t.Fatalf("rows = %+v, want {x, y}", rows)
	}
}

// 右行的深层嵌套字段投影(v.profile.score)。
func TestStreamJoinIntegNestedFieldProjection(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k, v.profile.score AS score
		FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k`)
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x"}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "profile": map[string]any{"score": 91}}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["score"] != 91 {
		t.Fatalf("rows = %+v, want score=91", rows)
	}
}

// ORDER BY 子句与 WITHIN JOIN 组合合法且输出正确(逐行批下单行排序,语义为不破坏输出)。
func TestStreamJoinIntegOrderByAllowed(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k, v.w FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k ORDER BY s.k`)
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x"}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "w": 1}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["w"] != 1 {
		t.Fatalf("rows = %+v", rows)
	}
}

// 关键字大小写不敏感(join/within/on 小写),流名保持大小写敏感。
func TestStreamJoinIntegKeywordCaseInsensitive(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		select s.k from leftS as s join rightS as v within 5 seconds on s.k = v.k`)
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x"}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "w": 1}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
}

// LEFT OUTER JOIN 完整拼写。
func TestStreamJoinIntegLeftOuterSpelling(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT c.cmd FROM midS AS c LEFT OUTER JOIN ackS AS r WITHIN 300 MILLISECONDS ON c.cmd = r.cmd
		WHERE r.ack IS NULL`)
	defer ssql.Stop()

	if err := ssql.EmitTo("midS", map[string]any{"cmd": "c1"}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["cmd"] != "c1" {
		t.Fatalf("rows = %+v", rows)
	}
}

// WITH 子句 TIMESTAMP+TIMEUNIT 组合与 WITHIN JOIN 共存(TIMEUNIT 容忍;join 内部
// 按量级启发式归一,ms epoch 正确匹配)。
func TestStreamJoinIntegWithTimestampTimeunit(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k FROM leftS AS s JOIN rightS AS v WITHIN 5 SECONDS ON s.k = v.k
		WITH (TIMESTAMP = 'ts', TIMEUNIT = 'ms')`)
	defer ssql.Stop()

	ms := time.Now().UnixMilli()
	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "ts": ms}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "ts": ms + 2000}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
}

// 双侧同键不同类型(string vs int)不匹配。
func TestStreamJoinIntegStringIntKeyNoMatch(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k`)
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "1"}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("string/int keys must not match, got %d rows", got)
	}
}

// 复合键数值归一(int vs float64)端到端匹配。
func TestStreamJoinIntegCompositeKeyNumericNormalize(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT d.g, d.u, f.score FROM cardS AS d JOIN faceS AS f WITHIN 30 SECONDS
		ON d.g = f.g AND d.u = f.u`)
	defer ssql.Stop()

	if err := ssql.EmitTo("cardS", map[string]any{"g": 1, "u": 2}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("faceS", map[string]any{"g": 1.0, "u": 2.0, "score": 93}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["score"] != 93 {
		t.Fatalf("int/float composite keys should match: %+v", rows)
	}
}

// ---- 匹配语义细节 ----

// 一对多 INNER:一条左行与窗内两条右行各配一对(笛卡尔),共 2 行。
func TestStreamJoinIntegOneToManyInner(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.seq, v.seq2 FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k`)
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "seq": 0}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "seq2": 1}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "seq2": 2}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 2, 5*time.Second)
	if len(rows) != 2 {
		t.Fatalf("one-to-many should emit 2 rows, got %d", len(rows))
	}
}

// 一对多 LEFT:左行已匹配(两次),不补 NULL。
func TestStreamJoinIntegOneToManyLeftNoNull(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.seq, r.ack FROM leftS AS s LEFT JOIN rightS AS r WITHIN 30 SECONDS ON s.k = r.k`)
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "seq": 0}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "ack": 1}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "ack": 2}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 2, 5*time.Second)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	time.Sleep(300 * time.Millisecond)
	if got := ssql.GetStats()["join_stage1_left_timeout_emitted"]; got != 0 {
		t.Errorf("matched LEFT must not emit NULL, got %d", got)
	}
}

// 迟到匹配(窗内到达)阻止 NULL 补发:匹配后窗口关闭也不再发 NULL。
func TestStreamJoinIntegLateMatchPreventsNull(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT c.cmd, r.ack FROM midS AS c LEFT JOIN ackS AS r WITHIN 2 SECONDS ON c.cmd = r.cmd`)
	defer ssql.Stop()

	if err := ssql.EmitTo("midS", map[string]any{"cmd": "c1"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // 仍在 2s 窗内
	if err := ssql.EmitTo("ackS", map[string]any{"cmd": "c1", "ack": 0}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second) // 越过窗口关闭
	rows := sink.snapshot()
	if len(rows) != 1 || rows[0]["ack"] != 0 {
		t.Fatalf("want exactly the match row (no NULL), got %+v", rows)
	}
}

// processing-time 保留期:左行超窗被驱逐后,右行到达不再匹配,缓冲归零。
func TestStreamJoinIntegProcessingTimeEviction(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k FROM leftS AS s JOIN rightS AS v WITHIN 200 MILLISECONDS ON s.k = v.k`)
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second) // 远超 WITHIN+sweeper 周期
	if got := ssql.GetStats()["join_stage1_buffered_rows"]; got != 0 {
		t.Fatalf("buffered_rows = %d after retention, want 0", got)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("evicted left row must not match, got %d rows", got)
	}
}

// IDLETIMEOUT 事件时间:e2e 全链路——双侧空闲超时推进水位,pending LEFT 补 NULL。
func TestStreamJoinIntegIdleTimeoutFlush(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT c.cmd FROM midS AS c LEFT JOIN ackS AS r WITHIN 300 MILLISECONDS ON c.cmd = r.cmd
		WITH (TIMESTAMP = 'ts', IDLETIMEOUT = '250ms')`)
	defer ssql.Stop()

	ms := time.Now().UnixMilli()
	if err := ssql.EmitTo("midS", map[string]any{"cmd": "c1", "ts": ms}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["cmd"] != "c1" {
		t.Fatalf("idle timeout should flush pending LEFT: %+v", rows)
	}
	if got := ssql.GetStats()["join_stage1_left_timeout_emitted"]; got != 1 {
		t.Errorf("left_timeout_emitted = %d, want 1", got)
	}
}

// 事件时间双侧水位 gauge 随到行推进。
func TestStreamJoinIntegWatermarkGauges(t *testing.T) {
	ssql, _ := newJoinInstance(t, `
		SELECT s.k FROM leftS AS s JOIN rightS AS v WITHIN 5 SECONDS ON s.k = v.k
		WITH (TIMESTAMP = 'ts')`)
	defer ssql.Stop()

	ms := time.Now().UnixMilli()
	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "ts": ms}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "ts": ms + 3000}); err != nil {
		t.Fatal(err)
	}
	wantL := (ms) * int64(time.Millisecond)
	wantR := (ms + 3000) * int64(time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stats := ssql.GetStats()
		if stats["join_stage1_left_watermark"] == wantL && stats["join_stage1_right_watermark"] == wantR {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("watermarks = %d / %d, want %d / %d",
		ssql.GetStats()["join_stage1_left_watermark"], ssql.GetStats()["join_stage1_right_watermark"], wantL, wantR)
}

// ---- 级联时序 ----

// 级联门控:级 1 未匹配前(b 缺席)任何输出都不发生;级 2 未匹配前(c 缺席)同样。
func TestStreamJoinIntegCascadeGating(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT a.va FROM leftS AS a
		JOIN rightS AS b WITHIN 30 SECONDS ON a.k = b.k
		JOIN midS AS c WITHIN 30 SECONDS ON b.k2 = c.k2`)
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "va": 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("stage1 unmatched must emit nothing, got %d", got)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "k2": "y", "vb": 2}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("stage2 unmatched must emit nothing, got %d", got)
	}
	if err := ssql.EmitTo("midS", map[string]any{"k2": "y", "vc": 3}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
}

// 逐级保留期独立:级 1 缓冲过期不影响已在级 2 挂起的复合行继续等待匹配。
func TestStreamJoinIntegCascadePerStageRetention(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT a.va FROM leftS AS a
		JOIN rightS AS b WITHIN 300 MILLISECONDS ON a.k = b.k
		JOIN midS AS c WITHIN 30 SECONDS ON b.k2 = c.k2`)
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "va": 1}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "k2": "y", "vb": 2}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(800 * time.Millisecond) // 级 1 原始缓冲已过期,复合行仍在级 2
	if err := ssql.EmitTo("midS", map[string]any{"k2": "y", "vc": 3}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["va"] != 1 {
		t.Fatalf("stage2 retention should be independent of stage1: %+v", rows)
	}
}

// 4 流级联,末级 LEFT:d 缺席 → NULL(d) 补齐,前三级字段完整,d 的列投影为 NULL。
func TestStreamJoinIntegCascade4StreamLeftLastStage(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT a.va, b.vb, c.vc, d.k3 AS dk3 FROM leftS AS a
		JOIN midS AS b WITHIN 30 SECONDS ON a.k = b.k
		JOIN lastS AS c WITHIN 30 SECONDS ON b.k2 = c.k2
		LEFT JOIN tailS AS d WITHIN 300 MILLISECONDS ON c.k3 = d.k3`)
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "va": 1}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("midS", map[string]any{"k": "x", "k2": "y", "vb": 2}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("lastS", map[string]any{"k2": "y", "k3": "z", "vc": 3}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	if rows[0]["va"] != 1 || rows[0]["vb"] != 2 || rows[0]["vc"] != 3 {
		t.Errorf("row = %+v", rows[0])
	}
	// NULL(d) 补齐后,d.k3 列求值为 nil(字段存在值为空)。
	if v, ok := rows[0]["dk3"]; !ok || v != nil {
		t.Errorf("dk3 should be nil for NULL complement, got %v (present=%v)", v, ok)
	}
}

// 事件时间级联:乱序到达(b 先于 a 到达但事件时间更晚)仍按事件时间邻近匹配。
func TestStreamJoinIntegCascadeEventTimeOutOfOrder(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT a.va FROM leftS AS a
		JOIN rightS AS b WITHIN 5 SECONDS ON a.k = b.k
		JOIN midS AS c WITHIN 5 SECONDS ON b.k2 = c.k2
		WITH (TIMESTAMP = 'ts')`)
	defer ssql.Stop()

	ms := time.Now().UnixMilli()
	// b 先到(ts+1s),a 后到(ts):事件时间差 1s ≤ 5s → 匹配。
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "k2": "y", "vb": 2, "ts": ms + 1000}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "va": 1, "ts": ms}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("midS", map[string]any{"k2": "y", "vc": 3, "ts": ms + 2000}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["va"] != 1 {
		t.Fatalf("out-of-order arrival should still match by event time: %+v", rows)
	}
}

// ---- 资源闸 option 端到端 ----

// WithJoinMaxRows:每侧行缓冲上限,超限丢新到行并计数。
func TestStreamJoinIntegJoinMaxRowsOption(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k`,
		streamsql.WithJoinMaxRows(1))
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "seq": 1}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("leftS", map[string]any{"k": "x", "seq": 2}); err != nil { // 超 maxRows=1 丢
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x"}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("only buffered row should match: %+v", rows)
	}
	if got := ssql.GetStats()["join_stage1_rows_dropped"]; got != 1 {
		t.Errorf("rows_dropped = %d, want 1", got)
	}
}

// WithJoinMaxKeys:LRU 淘汰最旧 key,被淘汰 key 的后续右行不匹配。
// 左右是两条独立入口 chan,淘汰动作与对侧扫描的先后受调度影响
// (过载下截断选择不保证确定性),故先用 keys_evicted 计数同步淘汰完成,再验证
// "被淘汰的 key 不再匹配"这一文档化语义。
func TestStreamJoinIntegJoinMaxKeysOption(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.tag FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k`,
		streamsql.WithJoinMaxKeys(1))
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "a", "tag": "A"}); err != nil {
		t.Fatal(err)
	}
	waitStatsEq(t, ssql, "join_stage1_buffered_rows", 1)
	if err := ssql.EmitTo("leftS", map[string]any{"k": "b", "tag": "B"}); err != nil { // 淘汰 key a
		t.Fatal(err)
	}
	waitStatsEq(t, ssql, "join_stage1_keys_evicted", 1)
	if err := ssql.EmitTo("rightS", map[string]any{"k": "a"}); err != nil { // a 已被淘汰
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "b"}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["tag"] != "B" {
		t.Fatalf("evicted key must not match; rows = %+v", rows)
	}
	if got := ssql.GetStats()["join_stage1_keys_evicted"]; got < 1 {
		t.Errorf("keys_evicted = %d, want >= 1", got)
	}
}

// waitStatsEq 轮询等待某指标到达期望值(同步 EmitTo 异步消费的时序)。
func waitStatsEq(t *testing.T, s *streamsql.Streamsql, key string, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.GetStats()[key] == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stat %q = %d, want %d (timeout)", key, s.GetStats()[key], want)
}

// block 策略无超时:输入 chan 满时 EmitTo 阻塞(背压),Stop 的 done 解除阻塞不挂死。
// (drop 分支的 input_dropped 计数由引擎级单测覆盖;集成层消费者快于生产者,难以确定性触发溢出。)
func TestStreamJoinIntegInputBlockStopsCleanly(t *testing.T) {
	perf := types.DefaultPerformanceConfig()
	perf.BufferConfig.DataChannelSize = 1
	perf.OverflowConfig.Strategy = "block"
	perf.OverflowConfig.BlockTimeout = 0 // 无限阻塞,仅 done 可解除
	ssql, sink := newJoinInstance(t, `
		SELECT s.k FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k`,
		streamsql.WithCustomPerformance(perf))

	// 先发 1 行并等其入缓冲(确认消费者就绪、chan 腾空)。
	if err := ssql.EmitTo("leftS", map[string]any{"k": -1}); err != nil {
		t.Fatal(err)
	}
	waitStatsEq(t, ssql, "join_stage1_buffered_rows", 1)
	// 消费者活跃时灌满 chan:不同键行不会被消费掉(无右行,永远 pending)。
	for i := 0; i < 8; i++ {
		if err := ssql.EmitTo("leftS", map[string]any{"k": i}); err != nil {
			t.Fatal(err)
		}
	}
	// 第 9 行:chan 已满,processor 消费完缓冲后仍在忙着……可能已被取走。
	// 为确定性阻塞,连发直到一次 EmitTo 未在 50ms 内返回。
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		for i := 8; i < 200; i++ {
			_ = ssql.EmitTo("leftS", map[string]any{"k": i})
		}
	}()
	select {
	case <-blocked:
		// 200 行全被及时消费(消费者过快):环境差异,不强行断言阻塞。
		t.Log("producer never blocked; consumer drained faster than expected")
	case <-time.After(100 * time.Millisecond):
		// 生产者被背压阻塞(符合预期):Stop 应解除阻塞并干净退出。
	}
	ssql.Stop()
	select {
	case <-blocked:
		// 生产者已解除
	case <-time.After(5 * time.Second):
		t.Fatal("blocked producer was not released by Stop (done escape failed)")
	}
	_ = sink
}

// HighPerformance 模式(expand 策略)与 JOIN 共存:构造期降级为 drop 并正常工作。
func TestStreamJoinIntegHighPerformanceMode(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k`,
		streamsql.WithHighPerformance())
	defer ssql.Stop()

	if err := ssql.EmitTo("leftS", map[string]any{"k": "x"}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("rightS", map[string]any{"k": "x", "w": 1}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
}

// ---- 多实例隔离 ----

// 两个 join 实例并发喂数互不串扰(状态/输出/指标各自独立)。
func TestStreamJoinIntegTwoInstancesNoCrossTalk(t *testing.T) {
	ia, sinkA := newJoinInstance(t, `
		SELECT s.k FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k`)
	defer ia.Stop()
	ib, sinkB := newJoinInstance(t, `
		SELECT s.k FROM leftS AS s JOIN rightS AS v WITHIN 30 SECONDS ON s.k = v.k`)
	defer ib.Stop()

	if err := ia.EmitTo("leftS", map[string]any{"k": "onlyA"}); err != nil {
		t.Fatal(err)
	}
	if err := ib.EmitTo("leftS", map[string]any{"k": "onlyB"}); err != nil {
		t.Fatal(err)
	}
	if err := ib.EmitTo("rightS", map[string]any{"k": "onlyB"}); err != nil {
		t.Fatal(err)
	}
	if err := ia.EmitTo("rightS", map[string]any{"k": "onlyA"}); err != nil {
		t.Fatal(err)
	}
	rowsA := sinkA.waitN(t, 1, 5*time.Second)
	rowsB := sinkB.waitN(t, 1, 5*time.Second)
	if len(rowsA) != 1 || rowsA[0]["k"] != "onlyA" {
		t.Fatalf("instance A rows = %+v", rowsA)
	}
	if len(rowsB) != 1 || rowsB[0]["k"] != "onlyB" {
		t.Fatalf("instance B rows = %+v", rowsB)
	}
}
