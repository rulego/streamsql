package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectCEP 执行一条 MATCH_RECOGNIZE 查询，按序投入事件，等待 wantMatches 次匹配到达 sink。
// 返回收到的每次匹配（每次 sink 回调的一批行）。
func collectCEP(t *testing.T, sql string, rows []map[string]any, wantMatches int) [][]map[string]any {
	t.Helper()
	s := streamsql.New()
	require.NoError(t, s.Execute(sql), "execute SQL")

	var mu sync.Mutex
	var got [][]map[string]any
	done := make(chan struct{}, 256)
	// 用 AddSyncSink：sync sink 在数据处理器 goroutine 内 inline 顺序执行，保证匹配到达顺序
	// 与产出顺序一致（AddSink 走异步 worker pool，多 worker 不保序）。
	s.AddSyncSink(func(r []map[string]any) {
		mu.Lock()
		got = append(got, r)
		n := len(got)
		mu.Unlock()
		if n <= wantMatches {
			done <- struct{}{}
		}
	})

	for _, r := range rows {
		s.Emit(r)
	}
	// 等待期望匹配数（带超时）。CEP 匹配在触发事件处理期间产生（无需 Stop flush）。
	for i := 0; i < wantMatches; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			s.Stop()
			t.Fatalf("timeout waiting for match %d/%d; got %d", i+1, wantMatches, len(got))
		}
	}
	// 再给一点时间让多余匹配（若有）落地，然后 Stop 收尾。
	time.Sleep(50 * time.Millisecond)
	s.Stop()

	mu.Lock()
	defer mu.Unlock()
	return got
}

// flatten 把多次匹配的行展平为一个切片（用于 ONE ROW PER MATCH 断言）。
func flatten(matches [][]map[string]any) []map[string]any {
	var out []map[string]any
	for _, m := range matches {
		out = append(out, m...)
	}
	return out
}

// 场景1：A{3} 连续越限确认。
func TestCEP_ConsecutiveThreshold(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn, A.v AS peak
			ONE ROW PER MATCH
			PATTERN (A{3})
			WITHIN '1h'
			DEFINE A AS v > 50
		)`
	rows := []map[string]any{
		{"ts": 1, "v": 10},
		{"ts": 2, "v": 60},
		{"ts": 3, "v": 70},
		{"ts": 4, "v": 80},
		{"ts": 5, "v": 5},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 1.0, asFloat64(flat[0]["mn"]))
	assert.Equal(t, 80.0, asFloat64(flat[0]["peak"]))
}

// 场景2：A B 过热后回落（故障前兆）。
func TestCEP_RiseThenDrop(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			ORDER BY ts
			MEASURES A.temp AS peak, B.temp AS drop
			PATTERN (A B)
			DEFINE A AS temp > 100, B AS temp < 100
		)`
	rows := []map[string]any{
		{"ts": 1, "temp": 50},
		{"ts": 2, "temp": 120},
		{"ts": 3, "temp": 90},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 120.0, asFloat64(flat[0]["peak"]))
	assert.Equal(t, 90.0, asFloat64(flat[0]["drop"]))
}

// 场景3：A B+ C 单调上升后转降（PREV 导航 + 聚合 MEASURES）。
func TestCEP_TrendReversal(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			ORDER BY ts
			MEASURES MAX(v) AS peak, FIRST(v) AS start, LAST(v) AS end
			ONE ROW PER MATCH
			PATTERN (A B+ C)
			DEFINE B AS v > PREV(v, 1), C AS v < PREV(v, 1)
		)`
	rows := []map[string]any{
		{"ts": 1, "v": 10},
		{"ts": 2, "v": 20},
		{"ts": 3, "v": 30},
		{"ts": 4, "v": 25},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 30.0, asFloat64(flat[0]["peak"]), "max")
	assert.Equal(t, 10.0, asFloat64(flat[0]["start"]), "first")
	assert.Equal(t, 25.0, asFloat64(flat[0]["end"]), "last")
}

// 场景4：A{5,} 振动突发（5+，以中断事件收尾）。
func TestCEP_VibrationBurst(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			ORDER BY ts
			MEASURES COUNT(*) AS n, MATCH_NUMBER() AS mn
			ONE ROW PER MATCH
			PATTERN (A{5,})
			WITHIN '1h'
			DEFINE A AS type == "vib"
		)`
	rows := []map[string]any{
		{"ts": 1, "type": "vib"},
		{"ts": 2, "type": "vib"},
		{"ts": 3, "type": "vib"},
		{"ts": 4, "type": "vib"},
		{"ts": 5, "type": "vib"},
		{"ts": 6, "type": "vib"},
		{"ts": 7, "type": "normal"},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 6.0, asFloat64(flat[0]["n"]), "6 consecutive vib")
}

// 场景5：Start Process+ End 跨事件类型序列（工作流）。
func TestCEP_CrossEventSequence(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn, COUNT(*) AS steps
			ONE ROW PER MATCH
			PATTERN (Start Process+ End)
			DEFINE Start AS status == "start", Process AS status == "process", End AS status == "end"
		)`
	rows := []map[string]any{
		{"ts": 1, "status": "start"},
		{"ts": 2, "status": "process"},
		{"ts": 3, "status": "process"},
		{"ts": 4, "status": "end"},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 4.0, asFloat64(flat[0]["steps"]))
}

// P1：PARTITION BY 按设备各自匹配。
func TestCEP_PartitionBy(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			PARTITION BY dev
			ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn, A.v AS v
			ONE ROW PER MATCH
			PATTERN (A{2})
			WITHIN '1h'
			DEFINE A AS v > 50
		)`
	rows := []map[string]any{
		{"ts": 1, "dev": "d1", "v": 60},
		{"ts": 2, "dev": "d2", "v": 70},
		{"ts": 3, "dev": "d1", "v": 80},
		{"ts": 4, "dev": "d2", "v": 90},
	}
	got := collectCEP(t, sql, rows, 2)
	assert.Len(t, got, 2, "每设备各一次匹配")
}

// P1：交替 A | B + CLASSIFIER。
func TestCEP_Alternation(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			ORDER BY ts
			MEASURES CLASSIFIER() AS c
			ONE ROW PER MATCH
			PATTERN (A | B)
			DEFINE A AS k == 1, B AS k == 2
		)`
	rows := []map[string]any{
		{"ts": 1, "k": 1},
		{"ts": 2, "k": 2},
		{"ts": 3, "k": 3},
	}
	got := collectCEP(t, sql, rows, 2)
	flat := flatten(got)
	require.Len(t, flat, 2)
	assert.Equal(t, "A", flat[0]["c"])
	assert.Equal(t, "B", flat[1]["c"])
}

// P1：ALL ROWS PER MATCH 逐行输出 + RUNNING 聚合。
func TestCEP_AllRowsPerMatch(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			ORDER BY ts
			MEASURES CLASSIFIER() AS c, COUNT(*) AS n
			ALL ROWS PER MATCH
			PATTERN (A{3})
			WITHIN '1h'
			DEFINE A AS v > 50
		)`
	rows := []map[string]any{
		{"ts": 1, "v": 60},
		{"ts": 2, "v": 70},
		{"ts": 3, "v": 80},
	}
	got := collectCEP(t, sql, rows, 1)
	require.Len(t, got, 1, "一次匹配")
	require.Len(t, got[0], 3, "ALL ROWS 输出 3 行")
	// RUNNING COUNT(*): 1,2,3
	assert.Equal(t, 1.0, asFloat64(got[0][0]["n"]))
	assert.Equal(t, 2.0, asFloat64(got[0][1]["n"]))
	assert.Equal(t, 3.0, asFloat64(got[0][2]["n"]))
	for _, r := range got[0] {
		assert.Equal(t, "A", r["c"])
	}
}

// P1：AFTER MATCH SKIP TO NEXT ROW 允许重叠。
func TestCEP_SkipToNextRow(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn
			ONE ROW PER MATCH
			AFTER MATCH SKIP TO NEXT ROW
			PATTERN (A{2})
			WITHIN '1h'
			DEFINE A AS v > 50
		)`
	// 4 连续：SKIP TO NEXT ROW → 匹配 (1,2),(2,3),(3,4) 共 3 次。
	rows := []map[string]any{
		{"ts": 1, "v": 60},
		{"ts": 2, "v": 70},
		{"ts": 3, "v": 80},
		{"ts": 4, "v": 90},
	}
	got := collectCEP(t, sql, rows, 3)
	assert.Len(t, got, 3)
}

// 分组模式 (A B)+：重复的 A-B 序列。
func TestCEP_GroupRepetition(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn, COUNT(*) AS n
			ONE ROW PER MATCH
			PATTERN ((A B)+)
			WITHIN '1h'
			DEFINE A AS k == 1, B AS k == 2
		)`
	rows := []map[string]any{
		{"ts": 1, "k": 1},
		{"ts": 2, "k": 2},
		{"ts": 3, "k": 1},
		{"ts": 4, "k": 2},
		{"ts": 5, "k": 3}, // 中断收尾
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 4.0, asFloat64(flat[0]["n"]), "两轮 A-B 共 4 行")
}

// Execute 对非法 CEP（缺 PATTERN、排除模式）应 fail-fast。
func TestCEP_ExecuteRejects(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"no pattern", `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts DEFINE A AS v>0)`},
		{"exclusion", `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts PATTERN ({- A -}) DEFINE A AS v>0)`},
		{"with group by", `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts PATTERN (A) DEFINE A AS v>0) GROUP BY TumblingWindow('1s')`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := streamsql.New()
			err := s.Execute(c.sql)
			assert.Error(t, err, c.name)
			s.Stop()
		})
	}
}

// EmitSync 对 CEP 查询应返回错误。
func TestCEP_EmitSyncRejected(t *testing.T) {
	s := streamsql.New()
	require.NoError(t, s.Execute(`SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts PATTERN (A) DEFINE A AS v>0)`))
	defer s.Stop()
	_, err := s.EmitSync(map[string]any{"ts": 1, "v": 1})
	assert.Error(t, err)
}

// === 场景驱动：从真实 IoT 场景推导，含函数组合 ===

// 场景：温度连续上升 3 步，输出上升幅度（PREV 链 + MEASURES 算术 + 符号限定字段）。
func TestCEP_Scenario_RiseStepsWithDelta(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES C.temp AS peak, C.temp - A.temp AS rise
		ONE ROW PER MATCH
		PATTERN (A B C)
		DEFINE B AS temp > PREV(temp, 1), C AS temp > PREV(temp, 1)
	)`
	rows := []map[string]any{
		{"ts": 1, "temp": 10},
		{"ts": 2, "temp": 20},
		{"ts": 3, "temp": 30},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 30.0, asFloat64(flat[0]["peak"]))
	assert.Equal(t, 20.0, asFloat64(flat[0]["rise"]), "30-10=20")
}

// 场景：MEASURES 用 CASE 对匹配做分级（CASE + 聚合占位符）。
func TestCEP_Scenario_CaseLevel(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES CASE WHEN MAX(v) > 200 THEN "critical" WHEN MAX(v) > 100 THEN "warn" ELSE "ok" END AS level, MAX(v) AS peak
		ONE ROW PER MATCH
		PATTERN (A{3})
		WITHIN '1h'
		DEFINE A AS v > 50
	)`
	rows := []map[string]any{
		{"ts": 1, "v": 60},
		{"ts": 2, "v": 70},
		{"ts": 3, "v": 120},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 120.0, asFloat64(flat[0]["peak"]))
	assert.Equal(t, "warn", flat[0]["level"], "MAX=120 → warn")
}

// 场景：DEFINE 复合条件 + 函数（AND + 绝对值）。
func TestCEP_Scenario_DefineWithFunction(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn, v AS v
		ONE ROW PER MATCH
		PATTERN (A)
		WITHIN '1h'
		DEFINE A AS abs(v) > 50 AND type == "spike"
	)`
	rows := []map[string]any{
		{"ts": 1, "v": 10, "type": "spike"},   // abs(10)<50 → 不匹配
		{"ts": 2, "v": 80, "type": "spike"},   // 匹配
		{"ts": 3, "v": 80, "type": "normal"},  // type 不符 → 不匹配
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1, "仅 ts=2 匹配")
	assert.Equal(t, 80.0, asFloat64(flat[0]["v"]))
}

// 场景：输出含分区键（rulego 据此路由到对应设备）。
func TestCEP_Scenario_PartitionKeyInOutput(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		PARTITION BY dev
		ORDER BY ts
		MEASURES A.dev AS dev, MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A{2})
		WITHIN '1h'
		DEFINE A AS v > 50
	)`
	rows := []map[string]any{
		{"ts": 1, "dev": "d1", "v": 60},
		{"ts": 2, "dev": "d1", "v": 70},
		{"ts": 3, "dev": "d2", "v": 80},
		{"ts": 4, "dev": "d2", "v": 90},
	}
	got := collectCEP(t, sql, rows, 2)
	devs := map[string]bool{}
	for _, m := range got {
		for _, r := range m {
			devs[r["dev"].(string)] = true
		}
	}
	assert.True(t, devs["d1"] && devs["d2"], "两设备各一匹配，输出含 dev: %v", devs)
}

// 场景：重试后成功（A+ B：连续失败后一次成功）。
func TestCEP_Scenario_RetryThenSuccess(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES COUNT(*) AS n, MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A+ B)
		WITHIN '1h'
		DEFINE A AS r == "fail", B AS r == "ok"
	)`
	rows := []map[string]any{
		{"ts": 1, "r": "fail"},
		{"ts": 2, "r": "fail"},
		{"ts": 3, "r": "fail"},
		{"ts": 4, "r": "ok"},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 4.0, asFloat64(flat[0]["n"]), "3 fail + 1 ok")
}

// 场景：MEASURES 算术范围（MAX-MIN、AVG）。
func TestCEP_Scenario_ArithmeticMeasures(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MAX(v) - MIN(v) AS rng, AVG(v) AS avg
		ONE ROW PER MATCH
		PATTERN (A{3})
		WITHIN '1h'
		DEFINE A AS v >= 0
	)`
	rows := []map[string]any{
		{"ts": 1, "v": 10},
		{"ts": 2, "v": 50},
		{"ts": 3, "v": 30},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 40.0, asFloat64(flat[0]["rng"]), "50-10=40")
	assert.Equal(t, 30.0, asFloat64(flat[0]["avg"]), "(10+50+30)/3=30")
}

// 场景：可选中间步（Start (Process)? End：Process 0 或 1 次）。
func TestCEP_Scenario_OptionalMiddle(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (Start Process? End)
		WITHIN '1h'
		DEFINE Start AS s == "S", Process AS s == "P", End AS s == "E"
	)`
	rows := []map[string]any{
		{"ts": 1, "s": "S"},
		{"ts": 2, "s": "P"},
		{"ts": 3, "s": "E"},
		{"ts": 4, "s": "S"},
		{"ts": 5, "s": "E"}, // 无 Process 也匹配
	}
	got := collectCEP(t, sql, rows, 2)
	assert.Len(t, got, 2, "S-P-E 与 S-E 各一次")
}

// MEASURES 支持任意标量函数（非导航/聚合）：upper/round/算术组合。
// 顶层 SELECT 在 CEP 不生效，函数须写在 MEASURES 里。
func TestCEP_MeasuresScalarFunctions(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES upper(type) AS t, round(v) AS rv, v + 1 AS vp1
		ONE ROW PER MATCH
		PATTERN (A)
		WITHIN '1h'
		DEFINE A AS v > 0
	)`
	rows := []map[string]any{{"ts": 1, "type": "alert", "v": 3.4}}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, "ALERT", flat[0]["t"], "upper(alert)")
	assert.Equal(t, 3.0, asFloat64(flat[0]["rv"]), "round(3.4)")
	assert.Equal(t, 4.4, asFloat64(flat[0]["vp1"]), "3.4+1")
}

// ALL ROWS PER MATCH 下 FIRST/LAST 按 RUNNING 推进（与 COUNT 一致），而非恒为末行。
func TestCEP_AllRows_FirstLastRunning(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES LAST(v) AS lv, FIRST(v) AS fv, COUNT(*) AS n
		ALL ROWS PER MATCH
		PATTERN (A{3})
		WITHIN '1h'
		DEFINE A AS v > 0
	)`
	rows := []map[string]any{{"ts": 1, "v": 10}, {"ts": 2, "v": 20}, {"ts": 3, "v": 30}}
	got := collectCEP(t, sql, rows, 1)
	require.Len(t, got, 1, "一次匹配")
	require.Len(t, got[0], 3, "ALL ROWS 输出 3 行")
	// LAST RUNNING：10,20,30；FIRST 恒 10；COUNT RUNNING：1,2,3
	assert.Equal(t, 10.0, asFloat64(got[0][0]["lv"]))
	assert.Equal(t, 20.0, asFloat64(got[0][1]["lv"]))
	assert.Equal(t, 30.0, asFloat64(got[0][2]["lv"]))
	assert.Equal(t, 10.0, asFloat64(got[0][0]["fv"]))
	assert.Equal(t, 1.0, asFloat64(got[0][0]["n"]))
}

// 外层 SELECT 投影 MEASURES 列：ONE ROW 下 SELECT 具体别名只输出选中的列。
func TestCEP_SelectProjectsMeasures(t *testing.T) {
	sql := `SELECT mn, peak FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn, A.v AS peak
		ONE ROW PER MATCH
		PATTERN (A{2})
		WITHIN '1h'
		DEFINE A AS v > 50
	)`
	rows := []map[string]any{{"ts": 1, "v": 60}, {"ts": 2, "v": 70}}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 1.0, asFloat64(flat[0]["mn"]))
	assert.Equal(t, 70.0, asFloat64(flat[0]["peak"]))
	_, hasTS := flat[0]["ts"]
	assert.False(t, hasTS, "ONE ROW SELECT 具体列不应漏入未选的输入字段")
}

// 外层 SELECT 表达式对 MEASURES 列求值。
func TestCEP_SelectExpressionOverMeasures(t *testing.T) {
	sql := `SELECT hi - lo AS span, hi FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MAX(v) AS hi, MIN(v) AS lo
		ONE ROW PER MATCH
		PATTERN (A{3})
		WITHIN '1h'
		DEFINE A AS v > 0
	)`
	rows := []map[string]any{{"ts": 1, "v": 10}, {"ts": 2, "v": 50}, {"ts": 3, "v": 30}}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 50.0, asFloat64(flat[0]["hi"]), "MAX=50")
	assert.Equal(t, 40.0, asFloat64(flat[0]["span"]), "hi-lo=50-10=40")
}

// ONE ROW PER MATCH + SELECT *：关系只暴露 MEASURES 列（输入字段不漏入），对齐 Flink。
func TestCEP_SelectStarOneRowMeasuresOnly(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A{2})
		WITHIN '1h'
		DEFINE A AS v > 50
	)`
	rows := []map[string]any{{"ts": 1, "v": 60}, {"ts": 2, "v": 70}}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 1.0, asFloat64(flat[0]["mn"]))
	_, hasTS := flat[0]["ts"]
	assert.False(t, hasTS, "ONE ROW PER MATCH 仅暴露 MEASURES 列，不含输入字段")
}

// ALL ROWS PER MATCH 暴露输入列：外层 SELECT 可引用输入字段（如 ts）。
func TestCEP_AllRowsSelectInputField(t *testing.T) {
	sql := `SELECT ts, c FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES CLASSIFIER() AS c
		ALL ROWS PER MATCH
		PATTERN (A{2})
		WITHIN '1h'
		DEFINE A AS v > 50
	)`
	rows := []map[string]any{{"ts": 1, "v": 60}, {"ts": 2, "v": 70}}
	got := collectCEP(t, sql, rows, 1)
	require.Len(t, got, 1)
	require.Len(t, got[0], 2, "ALL ROWS 输出 2 行")
	assert.Equal(t, "A", got[0][0]["c"])
	assert.Equal(t, 1.0, asFloat64(got[0][0]["ts"]), "首行 ts=1")
	assert.Equal(t, "A", got[0][1]["c"])
	assert.Equal(t, 2.0, asFloat64(got[0][1]["ts"]), "次行 ts=2")
}

// ALL ROWS PER MATCH + SELECT *：含输入字段与 MEASURES 列。
func TestCEP_AllRowsSelectStarIncludesInput(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES CLASSIFIER() AS c
		ALL ROWS PER MATCH
		PATTERN (A{2})
		WITHIN '1h'
		DEFINE A AS v > 50
	)`
	rows := []map[string]any{{"ts": 1, "v": 60}, {"ts": 2, "v": 70}}
	got := collectCEP(t, sql, rows, 1)
	require.Len(t, got, 1)
	require.Len(t, got[0], 2, "ALL ROWS 输出 2 行")
	_, hasV := got[0][0]["v"]
	_, hasC := got[0][0]["c"]
	assert.True(t, hasV && hasC, "ALL ROWS SELECT * 应含输入列 v 与 MEASURES 列 c")
}

// === 缺口补全：此前仅单元/探针覆盖、无 e2e 的场景 ===

// PERMUTE(A, B)：两种顺序都匹配（A,B) 与 (B,A)。
func TestCEP_Permute(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES CLASSIFIER() AS last, MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (PERMUTE(A, B))
		WITHIN '1h'
		DEFINE A AS k == 1, B AS k == 2
	)`
	rows := []map[string]any{{"ts": 1, "k": 1}, {"ts": 2, "k": 2}, {"ts": 3, "k": 2}, {"ts": 4, "k": 1}}
	got := collectCEP(t, sql, rows, 2)
	flat := flatten(got)
	require.Len(t, flat, 2)
	assert.Equal(t, "B", flat[0]["last"], "[A,B]→末符号 B")
	assert.Equal(t, "A", flat[1]["last"], "[B,A]→末符号 A")
}

// WITHIN 过期恢复：超窗的 A-B 作废，窗内的新 A-B 仍匹配。
func TestCEP_WithinExpiryRecovery(t *testing.T) {
	const base = int64(1700000000000)
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A B)
		WITHIN 1 MINUTES
		DEFINE A AS k == 1, B AS k == 2
	)`
	rows := []map[string]any{
		{"ts": base, "k": 1},          // A
		{"ts": base + 70000, "k": 2},  // 距 A 70s > 1min → 过期
		{"ts": base + 100000, "k": 1}, // 新 A
		{"ts": base + 100030, "k": 2}, // 距新 A 30ms < 1min → 匹配
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 1.0, asFloat64(flat[0]["mn"]), "仅第二对在窗内匹配")
}

// NEXT 导航：匹配末行 NEXT 越界为 nil。
func TestCEP_NextNavigation(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES A.k AS ak, NEXT(B.k, 1) AS nxt
		ONE ROW PER MATCH
		PATTERN (A B)
		WITHIN '1h'
		DEFINE A AS k == 1, B AS k == 2
	)`
	rows := []map[string]any{{"ts": 1, "k": 1}, {"ts": 2, "k": 2}}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 1.0, asFloat64(flat[0]["ak"]))
	assert.Nil(t, flat[0]["nxt"], "末行 NEXT 越界应为 nil")
}

// DEFINE 复合：OR 逻辑 + 引用另一符号字段（B AS v > A.v）。
func TestCEP_DefineOrAndCrossSymbol(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A B)
		WITHIN '1h'
		DEFINE A AS v > 10, B AS v > A.v OR k == 9
	)`
	rows := []map[string]any{
		{"ts": 1, "v": 20, "k": 0}, // A: v>10 ✓；后续 B 不满足 → 失败
		{"ts": 2, "v": 5, "k": 0},  // B: 5>A.v(20)? 否；k==9? 否 → 不匹配
		{"ts": 3, "v": 20, "k": 0}, // A
		{"ts": 4, "v": 25, "k": 0}, // B: 25>20 ✓ → 匹配
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1, "仅 A(3)B(4) 匹配")
}

// 多字段 PARTITION BY：按 dev+tenant 联合隔离。
func TestCEP_MultiPartitionBy(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		PARTITION BY dev, tenant
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A{2})
		WITHIN '1h'
		DEFINE A AS v > 50
	)`
	rows := []map[string]any{
		{"ts": 1, "dev": "d1", "tenant": "t1", "v": 60},
		{"ts": 2, "dev": "d1", "tenant": "t2", "v": 70},
		{"ts": 3, "dev": "d1", "tenant": "t1", "v": 80},
		{"ts": 4, "dev": "d1", "tenant": "t2", "v": 90},
	}
	got := collectCEP(t, sql, rows, 2)
	assert.Len(t, got, 2, "dev+tenant 各一组各一次匹配")
}

// SUM 聚合 MEASURES（此前只测了 AVG/MIN/MAX/COUNT）。
func TestCEP_MeasuresSum(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES SUM(v) AS total, COUNT(*) AS n
		ONE ROW PER MATCH
		PATTERN (A{3})
		WITHIN '1h'
		DEFINE A AS v > 0
	)`
	rows := []map[string]any{{"ts": 1, "v": 10}, {"ts": 2, "v": 20}, {"ts": 3, "v": 30}}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 60.0, asFloat64(flat[0]["total"]), "10+20+30")
	assert.Equal(t, 3.0, asFloat64(flat[0]["n"]))
}

// A* 星号量词：0+ 个 A 后接 B。
func TestCEP_StarQuantifier(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES COUNT(*) AS n, MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A* B)
		WITHIN '1h'
		DEFINE A AS k == 1, B AS k == 2
	)`
	rows := []map[string]any{{"ts": 1, "k": 1}, {"ts": 2, "k": 1}, {"ts": 3, "k": 2}}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 3.0, asFloat64(flat[0]["n"]), "A* B：2A+1B=3")
}

// AFTER MATCH SKIP TO LAST <符号>：解析+运行时（首行场景，断言匹配数）。
func TestCEP_SkipToLastSymbol(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		AFTER MATCH SKIP TO LAST B
		PATTERN (A B+ C)
		WITHIN '1h'
		DEFINE A AS k == 1, B AS k == 2, C AS k == 3
	)`
	rows := []map[string]any{
		{"ts": 1, "k": 1}, {"ts": 2, "k": 2}, {"ts": 3, "k": 2}, {"ts": 4, "k": 3},
		{"ts": 5, "k": 2}, {"ts": 6, "k": 3},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 1.0, asFloat64(flat[0]["mn"]))
}

// Stop 冲刷未闭合的贪婪匹配（A+ 无收尾事件，仅 Stop Flush 产出）。
// 同时验证 Flush 输出也经 SELECT 投影（与 Process 一致）。
func TestCEP_FlushUnclosed(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES COUNT(*) AS n
		ONE ROW PER MATCH
		PATTERN (A+)
		WITHIN '1h'
		DEFINE A AS k == 1
	)`
	s := streamsql.New()
	require.NoError(t, s.Execute(sql))
	var mu sync.Mutex
	var got []map[string]any
	s.AddSink(func(r []map[string]any) {
		mu.Lock()
		got = append(got, r...)
		mu.Unlock()
	})
	for _, r := range []map[string]any{{"ts": 1, "k": 1}, {"ts": 2, "k": 1}, {"ts": 3, "k": 1}} {
		s.Emit(r)
	}
	time.Sleep(100 * time.Millisecond) // 等处理器消费
	s.Stop()                           // Flush 产出未闭合的 A+ 突发
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1, "Flush 应输出未闭合的 A+ 突发")
	assert.Equal(t, 3.0, asFloat64(got[0]["n"]), "3 个 A")
}

// 符号限定聚合 SUM(A.v) 仅对 A 标签行求和；SUM(v) 对全部行。
func TestCEP_SymbolScopedAggregate(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES SUM(A.v) AS av, SUM(v) AS allv
		ONE ROW PER MATCH
		PATTERN (A B+)
		WITHIN '1h'
		DEFINE A AS k == 1, B AS k == 2
	)`
	rows := []map[string]any{
		{"ts": 1, "k": 1, "v": 1},   // A
		{"ts": 2, "k": 2, "v": 10},  // B
		{"ts": 3, "k": 2, "v": 100}, // B
		{"ts": 4, "k": 3, "v": 0},   // 非 A/B：收尾 B+
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 1.0, asFloat64(flat[0]["av"]), "SUM(A.v) 仅 A 行 = 1")
	assert.Equal(t, 111.0, asFloat64(flat[0]["allv"]), "SUM(v) 全部行 = 1+10+100")
}

// MATCH_RECOGNIZE ORDER BY DESC 流式下无意义（按到达序），Execute 期 fail-fast。
func TestCEP_RejectsOrderByDesc(t *testing.T) {
	s := streamsql.New()
	err := s.Execute(`SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts DESC PATTERN (A) DEFINE A AS v>0)`)
	assert.Error(t, err, "ORDER BY DESC 应被拒绝")
	s.Stop()
}
