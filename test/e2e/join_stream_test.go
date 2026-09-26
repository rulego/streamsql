package e2e

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
)

// ---- 测试基建 ----

// joinResultSink 收集输出行（批次展平），支持轮询等待。
type joinResultSink struct {
	mu   sync.Mutex
	rows []map[string]any
}

func (s *joinResultSink) add(batch []map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, batch...)
}

func (s *joinResultSink) snapshot() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]map[string]any, len(s.rows))
	copy(out, s.rows)
	return out
}

// waitN 轮询等待收满 n 行（10ms 步进），超时返回已达数量供断言失败信息使用。
func (s *joinResultSink) waitN(t *testing.T, n int, timeout time.Duration) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if rows := s.snapshot(); len(rows) >= n {
			return rows
		}
		time.Sleep(10 * time.Millisecond)
	}
	return s.snapshot()
}

func newJoinInstance(t *testing.T, sql string, opts ...streamsql.Option) (*streamsql.Streamsql, *joinResultSink) {
	t.Helper()
	sink := &joinResultSink{}
	all := append([]streamsql.Option{streamsql.WithDiscardLog()}, opts...)
	ssql := streamsql.New(all...)
	if err := ssql.Execute(sql); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	ssql.AddSink(sink.add)
	return ssql, sink
}

// ---- 双源信号互证（INNER，处理时间）----

func TestStreamJoinScenarioDualSourceInner(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.deviceId, s.temperature AS temp, v.vibration AS vib
		FROM tempStream AS s
		JOIN vibrationStream AS v WITHIN 30 SECONDS
		  ON s.deviceId = v.deviceId
		WHERE s.temperature > 75 AND v.vibration > 30`)
	defer ssql.Stop()

	if !ssql.IsStreamJoinQuery() {
		t.Fatal("IsStreamJoinQuery should be true")
	}
	// 高温但低振动：JOIN 匹配后被 WHERE 过滤。
	if err := ssql.EmitTo("tempStream", map[string]any{"deviceId": "d1", "temperature": 80}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("vibrationStream", map[string]any{"deviceId": "d1", "vibration": 10}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	// 不同设备不匹配。
	if err := ssql.EmitTo("tempStream", map[string]any{"deviceId": "d2", "temperature": 90}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("vibrationStream", map[string]any{"deviceId": "d3", "vibration": 50}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	// 双高：产出。
	if err := ssql.EmitTo("tempStream", map[string]any{"deviceId": "d4", "temperature": 82}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("vibrationStream", map[string]any{"deviceId": "d4", "vibration": 35}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}

	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1 (filtered + unmatched excluded): %+v", len(rows), rows)
	}
	if rows[0]["deviceId"] != "d4" || rows[0]["temp"] != 82 || rows[0]["vib"] != 35 {
		t.Errorf("row = %+v", rows[0])
	}
}

// Emit() 在 JOIN 查询等价于 EmitTo(FROM 流)。
func TestStreamJoinEmitRoutesToFromStream(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k, v.w FROM aStream AS s JOIN bStream AS v WITHIN 30 SECONDS ON s.k = v.k`)
	defer ssql.Stop()

	ssql.Emit(map[string]any{"k": "x", "w1": 1}) // → aStream
	if err := ssql.EmitTo("bStream", map[string]any{"k": "x", "w": 2}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["w"] != 2 {
		t.Fatalf("Emit should route to FROM stream: %+v", rows)
	}
}

// ---- 指令下发未回执告警（LEFT 缺席检测）----

func TestStreamJoinScenarioLeftAbsenceAlert(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT c.cmdId, c.deviceId, r.ackCode
		FROM cmdStream AS c
		LEFT JOIN ackStream AS r WITHIN 300 MILLISECONDS
		  ON c.cmdId = r.cmdId
		WHERE r.ackCode IS NULL`)
	defer ssql.Stop()

	// c1 有回执（窗内）→ 不产出行；c2 无回执 → 超窗补 NULL 行（WHERE 命中）。
	if err := ssql.EmitTo("cmdStream", map[string]any{"cmdId": "c1", "deviceId": "dev1"}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("ackStream", map[string]any{"cmdId": "c1", "ackCode": 0}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("cmdStream", map[string]any{"cmdId": "c2", "deviceId": "dev2"}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}

	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want exactly the unmatched cmd NULL-complement: %+v", len(rows), rows)
	}
	if rows[0]["cmdId"] != "c2" || rows[0]["deviceId"] != "dev2" {
		t.Errorf("row = %+v", rows[0])
	}
	if v, ok := rows[0]["ackCode"]; !ok || v != nil {
		t.Errorf("ackCode should be missing/nil for NULL complement, got %v", v)
	}
}

// ---- 车联网工况 × 轨迹（事件时间）----

func TestStreamJoinScenarioEventTimeCanGps(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT can.vin, can.speed, gps.lon, gps.lat
		FROM canStream AS can
		JOIN gpsStream AS gps WITHIN 5 SECONDS
		  ON can.vin = gps.vin
		WITH (TIMESTAMP = 'ts')`)
	defer ssql.Stop()

	ms := time.Now().UnixMilli()
	// 邻近 3s → 匹配。
	if err := ssql.EmitTo("canStream", map[string]any{"vin": "V1", "speed": 60, "ts": ms}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("gpsStream", map[string]any{"vin": "V1", "lon": 116.1, "lat": 39.9, "ts": ms + 3000}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("event-time proximity match missing: %+v", rows)
	}
	if rows[0]["vin"] != "V1" || rows[0]["speed"] != 60 || rows[0]["lon"] != 116.1 {
		t.Errorf("row = %+v", rows[0])
	}
	// 事件时间差 7s > 5s：同行到达也不匹配（与处理时间的差异语义）。
	if err := ssql.EmitTo("canStream", map[string]any{"vin": "V2", "speed": 30, "ts": ms}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("gpsStream", map[string]any{"vin": "V2", "lon": 0, "lat": 0, "ts": ms + 7000}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := len(sink.snapshot()); got != 1 {
		t.Errorf("out-of-proximity rows must not match, rows = %d", got)
	}
}

// ---- 门禁人卡合一（复合键）----

func TestStreamJoinScenarioCompositeKey(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT d.gateId, d.userId, f.score
		FROM cardStream AS d
		JOIN faceStream AS f WITHIN 30 SECONDS
		  ON d.gateId = f.gateId AND d.userId = f.userId`)
	defer ssql.Stop()

	mustEmit := func(stream string, row map[string]any) {
		t.Helper()
		if err := ssql.EmitTo(stream, row); err != nil {
			t.Fatalf("EmitTo %s: %v", stream, err)
		}
	}
	mustEmit("cardStream", map[string]any{"gateId": "g1", "userId": "u1"})
	mustEmit("faceStream", map[string]any{"gateId": "g1", "userId": "u2", "score": 91}) // 人不符
	mustEmit("faceStream", map[string]any{"gateId": "g2", "userId": "u1", "score": 92}) // 门不符
	mustEmit("faceStream", map[string]any{"gateId": "g1", "userId": "u1", "score": 93}) // 双符合

	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["score"] != 93 {
		t.Fatalf("composite key must match only the full pair: %+v", rows)
	}
}

// ---- 对照，不带 WITHIN = 流表 JOIN（现状不变）----

func TestStreamJoinContrastNoWithinIsTableJoin(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	if err := ssql.Execute("SELECT s.deviceId, m.location FROM stream AS s JOIN devices AS m ON s.deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ssql.IsStreamJoinQuery() {
		t.Error("without WITHIN the query must stay a stream-table join")
	}
	if _, err := ssql.RegisterTable("devices", []map[string]any{{"deviceId": "d1", "location": "plantA"}}); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	got, err := ssql.EmitSync(map[string]any{"deviceId": "d1"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got["location"] != "plantA" {
		t.Errorf("table join regression: %v", got)
	}
}

// 表未注册的错误文案追加"双流关联需 WITHIN"提示。
func TestStreamJoinMissingTableHint(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	if err := ssql.Execute("SELECT s.k FROM aStream AS s JOIN bTbl AS v ON s.k = v.k"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	_, err := ssql.EmitSync(map[string]any{"k": "x"})
	if err == nil || !strings.Contains(err.Error(), "not registered") || !strings.Contains(err.Error(), "WITHIN") {
		t.Fatalf("error should hint WITHIN for stream-stream join, got: %v", err)
	}
}

// ---- 输出形状 ----

func TestStreamJoinSelectStarShape(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT * FROM aStream AS s JOIN bStream AS v WITHIN 30 SECONDS ON s.k = v.k`)
	defer ssql.Stop()

	ssql.EmitTo("aStream", map[string]any{"k": "x", "pa": 1})
	ssql.EmitTo("bStream", map[string]any{"k": "x", "pb": 2})
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	row := rows[0]
	if row["pa"] != 1 {
		t.Errorf("left fields flat: %+v", row)
	}
	if v, ok := row["v"].(map[string]any); !ok || v["pb"] != 2 {
		t.Errorf("right row nested under alias: %+v", row["v"])
	}
}

// ---- 运行期校验 ----

func TestStreamJoinEmitSyncRejected(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	if err := ssql.Execute("SELECT s.k FROM aStream AS s JOIN bStream AS v WITHIN 5 SECONDS ON s.k = v.k"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.EmitSync(map[string]any{"k": "x"}); err == nil || !strings.Contains(err.Error(), "stream JOIN") {
		t.Fatalf("EmitSync must be rejected for join queries, got: %v", err)
	}
}

func TestStreamJoinEmitToUnknownStream(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	if err := ssql.Execute("SELECT s.k FROM aStream AS s JOIN bStream AS v WITHIN 5 SECONDS ON s.k = v.k"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if err := ssql.EmitTo("noSuchStream", map[string]any{"k": "x"}); err == nil || !strings.Contains(err.Error(), "unknown input stream") {
		t.Fatalf("unknown stream name must error, got: %v", err)
	}
}

func TestStreamJoinEmitToBeforeExecute(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	if err := ssql.EmitTo("aStream", map[string]any{"k": "x"}); err == nil {
		t.Fatal("EmitTo before Execute must error")
	}
}

func TestStreamJoinDuplicateInputName(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	err := ssql.Execute("SELECT s.k FROM aStream AS s JOIN aStream WITHIN 5 SECONDS ON s.k = aStream.k")
	if err == nil || !strings.Contains(err.Error(), "duplicate input stream name") {
		t.Fatalf("duplicate FROM/JOIN stream name must error, got: %v", err)
	}
}

// ---- 级联 ----

func TestStreamJoinCascadeThreeStreamsInner(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT a.va, b.vb, c.vc
		FROM aStream AS a
		JOIN bStream AS b WITHIN 30 SECONDS ON a.k = b.k
		JOIN cStream AS c WITHIN 30 SECONDS ON b.k2 = c.k2`)
	defer ssql.Stop()

	mustEmit := func(stream string, row map[string]any) {
		t.Helper()
		if err := ssql.EmitTo(stream, row); err != nil {
			t.Fatalf("EmitTo %s: %v", stream, err)
		}
	}
	mustEmit("aStream", map[string]any{"k": "x", "va": 1})
	mustEmit("bStream", map[string]any{"k": "x", "k2": "y", "vb": 2})
	mustEmit("cStream", map[string]any{"k2": "y", "vc": 3})

	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("3-stream cascade should emit 1 row: %+v", rows)
	}
	if rows[0]["va"] != 1 || rows[0]["vb"] != 2 || rows[0]["vc"] != 3 {
		t.Errorf("row = %+v", rows[0])
	}
}

func TestStreamJoinCascadeThreeStreamsLeftAtStage1(t *testing.T) {
	// 级 1 LEFT：b 缺席 → NULL(b) 复合行流入级 2，与 c 匹配（ON b.k2 IS NULL 不成立，
	// 改用 a 侧键设计：级 2 ON 用 a.k 与 c.k —— 复合行含 a 字段平铺）。
	ssql, sink := newJoinInstance(t, `
		SELECT a.va, b.vb, c.vc
		FROM aStream AS a
		LEFT JOIN bStream AS b WITHIN 300 MILLISECONDS ON a.k = b.k
		JOIN cStream AS c WITHIN 30 SECONDS ON a.k = c.k`)
	defer ssql.Stop()

	if err := ssql.EmitTo("aStream", map[string]any{"k": "x", "va": 1}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("cStream", map[string]any{"k": "x", "vc": 3}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	// b 永不到来：级 1 超窗补 NULL(b)，复合行（va=1，b 空）流入级 2 与 c 匹配。
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("stage-1 LEFT NULL row should flow into stage 2 and match c: %+v", rows)
	}
	if rows[0]["va"] != 1 || rows[0]["vc"] != 3 {
		t.Errorf("row = %+v", rows[0])
	}
}

func TestStreamJoinCascadeThreeStreamsLeftAtStage2(t *testing.T) {
	// 级 2 LEFT：a×b 匹配后 c 缺席 → 超窗补 NULL(c)。
	ssql, sink := newJoinInstance(t, `
		SELECT a.va, c.vc
		FROM aStream AS a
		JOIN bStream AS b WITHIN 30 SECONDS ON a.k = b.k
		LEFT JOIN cStream AS c WITHIN 300 MILLISECONDS ON b.k2 = c.k2`)
	defer ssql.Stop()

	if err := ssql.EmitTo("aStream", map[string]any{"k": "x", "va": 1}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("bStream", map[string]any{"k": "x", "k2": "y", "vb": 2}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("stage-2 LEFT should NULL-complement c: %+v", rows)
	}
	if rows[0]["va"] != 1 {
		t.Errorf("row = %+v", rows[0])
	}
	if v, ok := rows[0]["vc"]; !ok || v != nil {
		t.Errorf("vc should be nil for NULL complement, got %v", v)
	}
}

func TestStreamJoinCascadeIntermediateTsMaxEventTime(t *testing.T) {
	// e2e 版中间行 ts=max 裁定：b 比 a 新 4s（W1=5s 内），复合行 ts=b.ts；
	// c 距 b 4s ≤ W2=5s → 匹配；反证 c 距 b 6s > W2 → 不匹配。
	sql := `
		SELECT a.va FROM aStream AS a
		JOIN bStream AS b WITHIN 5 SECONDS ON a.k = b.k
		JOIN cStream AS c WITHIN 5 SECONDS ON b.k2 = c.k2
		WITH (TIMESTAMP = 'ts')`
	ms := time.Now().UnixMilli()

	ssql, sink := newJoinInstance(t, sql)
	if err := ssql.EmitTo("aStream", map[string]any{"k": "x", "va": 1, "ts": ms}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("bStream", map[string]any{"k": "x", "k2": "y", "ts": ms + 4000}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("cStream", map[string]any{"k2": "y", "ts": ms + 8000}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("ts=max cascade should match: %+v", rows)
	}
	ssql.Stop()

	ssql2, sink2 := newJoinInstance(t, sql)
	defer ssql2.Stop()
	if err := ssql2.EmitTo("aStream", map[string]any{"k": "x", "va": 1, "ts": ms}); err != nil {
		t.Fatal(err)
	}
	if err := ssql2.EmitTo("bStream", map[string]any{"k": "x", "k2": "y", "ts": ms + 4000}); err != nil {
		t.Fatal(err)
	}
	if err := ssql2.EmitTo("cStream", map[string]any{"k2": "y", "ts": ms + 10600}); err != nil {
		t.Fatal(err) // 距复合行(b.ts=+4s) 6.6s > W2=5s
	}
	time.Sleep(300 * time.Millisecond)
	if got := len(sink2.snapshot()); got != 0 {
		t.Errorf("c beyond W2 of composite row must not match, got %d", got)
	}
}

func TestStreamJoinCascadeFourStreams(t *testing.T) {
	// >3 流抽验：组合泛化（4 流 3 级）。
	ssql, sink := newJoinInstance(t, `
		SELECT a.va, b.vb, c.vc, d.vd
		FROM aStream AS a
		JOIN bStream AS b WITHIN 30 SECONDS ON a.k = b.k
		JOIN cStream AS c WITHIN 30 SECONDS ON b.k2 = c.k2
		JOIN dStream AS d WITHIN 30 SECONDS ON c.k3 = d.k3`)
	defer ssql.Stop()

	mustEmit := func(stream string, row map[string]any) {
		t.Helper()
		if err := ssql.EmitTo(stream, row); err != nil {
			t.Fatalf("EmitTo %s: %v", stream, err)
		}
	}
	mustEmit("aStream", map[string]any{"k": "x", "va": 1})
	mustEmit("bStream", map[string]any{"k": "x", "k2": "y", "vb": 2})
	mustEmit("cStream", map[string]any{"k2": "y", "k3": "z", "vc": 3})
	mustEmit("dStream", map[string]any{"k3": "z", "vd": 4})

	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("4-stream cascade should emit 1 row: %+v", rows)
	}
	if rows[0]["va"] != 1 || rows[0]["vb"] != 2 || rows[0]["vc"] != 3 || rows[0]["vd"] != 4 {
		t.Errorf("row = %+v", rows[0])
	}
}

// ---- Stop Flush ----

func TestStreamJoinStopFlushLeftPending(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT c.cmdId, r.ackCode
		FROM cmdStream AS c
		LEFT JOIN ackStream AS r WITHIN 10 SECONDS
		  ON c.cmdId = r.cmdId`)
	// 注意：不 defer Stop，手动触发并在 Stop 返回后断言。
	if err := ssql.EmitTo("cmdStream", map[string]any{"cmdId": "c1"}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := ssql.EmitTo("cmdStream", map[string]any{"cmdId": "c2"}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	// 窗口远未到（10s），正常路径不补发；Stop 的 Flush 补发全部 pending。
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("no output expected before window close, got %d", got)
	}
	// 等行已入缓冲（输入 chan 中的在途行不属于 pending，与 dataChan 语义一致）。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ssql.GetStats()["join_stage1_buffered_rows"] >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ssql.Stop()
	rows := sink.snapshot()
	if len(rows) != 2 {
		t.Fatalf("Stop flush should emit both pending LEFT rows, got %d: %+v", len(rows), rows)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r["cmdId"].(string)] = true
	}
	if !got["c1"] || !got["c2"] {
		t.Errorf("flushed rows = %+v", rows)
	}
}

// ---- 指标可观测 ----

func TestStreamJoinMetrics(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k FROM aStream AS s JOIN bStream AS v WITHIN 30 SECONDS ON s.k = v.k`)
	defer ssql.Stop()

	ssql.EmitTo("aStream", map[string]any{"k": "x"})
	ssql.EmitTo("bStream", map[string]any{"k": "x"})
	sink.waitN(t, 1, 5*time.Second)

	stats := ssql.GetStats()
	if got := stats["join_stage1_matches_emitted"]; got != 1 {
		t.Errorf("join_stage1_matches_emitted = %d, want 1", got)
	}
	if got := stats["join_stage1_input_dropped"]; got != 0 {
		t.Errorf("input_dropped = %d, want 0", got)
	}
	if got := stats["join_stage1_buffered_rows"]; got != 2 {
		t.Errorf("buffered_rows = %d, want 2", got)
	}
	if _, ok := stats["join_stage1_left_watermark"]; !ok {
		t.Error("left watermark gauge should be exposed")
	}
}

func TestStreamJoinLateDropMetricEventTime(t *testing.T) {
	ssql, _ := newJoinInstance(t, `
		SELECT s.k FROM aStream AS s JOIN bStream AS v WITHIN 5 SECONDS ON s.k = v.k
		WITH (TIMESTAMP = 'ts')`)
	defer ssql.Stop()

	ms := time.Now().UnixMilli()
	ssql.EmitTo("bStream", map[string]any{"k": "x", "ts": ms + 30000}) // 右侧水位 +30s
	ssql.EmitTo("bStream", map[string]any{"k": "x", "ts": ms})         // 迟到 30s → 丢
	// TsProp 缺失也计迟到丢弃。
	ssql.EmitTo("aStream", map[string]any{"k": "x"})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ssql.GetStats()["join_stage1_late_dropped"] >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("late_dropped = %d, want >= 2", ssql.GetStats()["join_stage1_late_dropped"])
}

// 单流回归：JOIN 查询不影响普通直连/窗口查询行为（全量回归另有门禁，此处抽查）。
func TestStreamJoinDoesNotAffectSingleStream(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, temperature FROM stream WHERE temperature > 30"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	got, err := ssql.EmitSync(map[string]any{"deviceId": "d1", "temperature": 35})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got["deviceId"] != "d1" {
		t.Errorf("single-stream regression: %v", got)
	}
}
