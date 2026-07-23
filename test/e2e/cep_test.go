package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectCEP executes a MATCH_RECOGNIZE query, inserts events in order, and waits for the wantMatches match to reach the sink.
// Returns each received match (a batch of lines for each sink callback).
func collectCEP(t *testing.T, sql string, rows []map[string]any, wantMatches int) [][]map[string]any {
	t.Helper()
	s := streamsql.New()
	require.NoError(t, s.Execute(sql), "execute SQL")

	var mu sync.Mutex
	var got [][]map[string]any
	done := make(chan struct{}, 256)
	// Use AddSyncSink: Sync sink executes inline sequentially within the data processor goroutine to ensure matching of the order of arrival
	// Consistent with the output order (AddSink uses an asynchronous worker pool, multiple workers do not maintain order).
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
	// Waiting for the expected match number (with timeout). CEP matches are generated during triggered event handling (no need to stop flush).
	for i := 0; i < wantMatches; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			s.Stop()
			t.Fatalf("timeout waiting for match %d/%d; got %d", i+1, wantMatches, len(got))
		}
	}
	// Give a little more time for any extra matches (if any) to land, then Stop to wrap up.
	time.Sleep(50 * time.Millisecond)
	s.Stop()

	mu.Lock()
	defer mu.Unlock()
	return got
}

// flatten flattens the rows of multiple matches into a slice (used for ONE ROW PER MATCH Assertions).
func flatten(matches [][]map[string]any) []map[string]any {
	var out []map[string]any
	for _, m := range matches {
		out = append(out, m...)
	}
	return out
}

// Scenario 1: A{3} Consecutive limit exceedance confirmations.
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

// Scenario 2: A and B overheat and then fall back (a warning sign).
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

// Scenario 3: A B+ C monotonically rising and then falling (PREV navigation + aggregation MEASURES).
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

// Scenario 4: A{5,} Vibration Burst (5+, ending with an interrupt event).
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

// Scenario 5: Start Process+ End cross-event type sequence (workflow).
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

// PARTITION BY matches each device individually.
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

// Alternate A | B + CLASSIFIER.
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

// ALL ROWS PER MATCH output line by line + RUNNING aggregation.
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

// AFTER MATCH SKIP TO NEXT ROW allows overlap.
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
	// 4 Consecutive: SKIP TO NEXT ROW → Match (1,2), (2,3), (3,4) 3 times.
	rows := []map[string]any{
		{"ts": 1, "v": 60},
		{"ts": 2, "v": 70},
		{"ts": 3, "v": 80},
		{"ts": 4, "v": 90},
	}
	got := collectCEP(t, sql, rows, 3)
	assert.Len(t, got, 3)
}

// Grouping mode (A B)+: Repeating A-B sequences.
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
		{"ts": 5, "k": 3}, // Interrupt and wrap up
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 4.0, asFloat64(flat[0]["n"]), "两轮 A-B 共 4 行")
}

// Execute should fail-fast for illegal CEPs (missing PATTERN, excluding patterns).
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

// EmitSync should return an error for CEP queries.
func TestCEP_EmitSyncRejected(t *testing.T) {
	s := streamsql.New()
	require.NoError(t, s.Execute(`SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts PATTERN (A) DEFINE A AS v>0)`))
	defer s.Stop()
	_, err := s.EmitSync(map[string]any{"ts": 1, "v": 1})
	assert.Error(t, err)
}

// === Scenario-driven: derived from real IoT scenarios, including function combinations ===

// Scenario: Temperature rises continuously by 3 steps, output increase (PREV chain + MEASURES arithmetic + symbol-limited fields).
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

// Scenario: MEASURES Uses CASE to rank matches (CASE + aggregated placeholder).
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

// Scenario: DEFINE Compound Condition + Function (AND + Absolute Value).
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
		{"ts": 1, "v": 10, "type": "spike"},  // abs(10)<50 → mismatch
		{"ts": 2, "v": 80, "type": "spike"},  // Match
		{"ts": 3, "v": 80, "type": "normal"}, // Type mismatches → mismatches
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1, "仅 ts=2 匹配")
	assert.Equal(t, 80.0, asFloat64(flat[0]["v"]))
}

// Scenario: output with partition keys (rulego routes to the corresponding device).
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

// Scenario: Successful after retry (A+ B: One success after consecutive failures).
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

// Scenario: MEASURES arithmetic range (MAX-MIN, AVG).
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

// Scenario: Optional intermediate step (Start (Process)? End: Process 0 or 1 time).
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
		{"ts": 5, "s": "E"}, // No Process also matches
	}
	got := collectCEP(t, sql, rows, 2)
	assert.Len(t, got, 2, "S-P-E 与 S-E 各一次")
}

// MEASURES supports arbitrary scalar functions (non-navigation/aggregation): upper/round/arithmetic combinations.
// Top-level SELECT does not work in CEP; the function must be written in MEASURES.
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

// Under ALL ROWS PER MATCH, FIRST/LAST advance by RUNNING (consistent with COUNT), rather than always being the last row.
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
	// LAST RUNNING: 10, 20, 30; FIRST constant 10; COUNT RUNNING: 1, 2, 3
	assert.Equal(t, 10.0, asFloat64(got[0][0]["lv"]))
	assert.Equal(t, 20.0, asFloat64(got[0][1]["lv"]))
	assert.Equal(t, 30.0, asFloat64(got[0][2]["lv"]))
	assert.Equal(t, 10.0, asFloat64(got[0][0]["fv"]))
	assert.Equal(t, 1.0, asFloat64(got[0][0]["n"]))
}

// Outer SELECT Projection MEASURES column: Under ONE ROW, the specific SELECT aliases only output the selected columns.
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

// The outer SELECT expression evaluates the MEASURES column.
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

// ONE ROW PER MATCH + SELECT *: The relationship exposes only the MEASURES column (no missing input fields), aligning with Flink.
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

// ALL ROWS PER MATCH Exposed input column: Outer SELECT can reference input fields (such as ts).
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

// ALL ROWS PER MATCH + SELECT *: Includes input fields and MEASURES columns.
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

// === Gap Filling: Previously, only cell/probe coverage was used, with no e2e ===

// PERMUTE(A, B): Both sequences match (A, B) and (B, A).
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

// WITHIN expired recovery: The A-B of the super window is voided, while the new A-B inside the window still matches.
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
		{"ts": base + 70000, "k": 2},  // Expired → 1 minute > from A 70s
		{"ts": base + 100000, "k": 1}, // New A
		{"ts": base + 100030, "k": 2}, // Matches → 1 minute < 30ms from the new A
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 1.0, asFloat64(flat[0]["mn"]), "仅第二对在窗内匹配")
}

// NEXT Navigation: Matches the last line NEXT outbound as nil.
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

// DEFINE Complex: OR logic + reference another symbolic field (B AS v > A.v).
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
		{"ts": 1, "v": 20, "k": 0}, // A: v>10 ✓;  Subsequently, B does not satisfy → failure
		{"ts": 2, "v": 5, "k": 0},  // B: 5>A.v(20)? No; k==9? No → mismatch
		{"ts": 3, "v": 20, "k": 0}, // A
		{"ts": 4, "v": 25, "k": 0}, // B: 25>20 ✓ → match
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1, "仅 A(3)B(4) 匹配")
}

// Multi-field PARTITION BY: Joint isolation by dev+tenant.
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

// SUM Aggregation MEASURES (previously only AVG/MIN/MAX/COUNT were measured).
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

// A* Asterisk: 0+ A's followed by B.
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

// AFTER MATCH SKIP TO LAST <symbol>: parsing and runtime (first-line case; assert the match count).
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

// Stop flushes unclosed greedy matches (A+ has no finishing events, only Stop Flush outputs).
// At the same time, verify that the Flush output is also projected by SELECT (consistent with the Process).
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
	time.Sleep(100 * time.Millisecond) // and other processor consumption
	s.Stop()                           // Flush produces an unclosed A+ burst
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1, "Flush 应输出未闭合的 A+ 突发")
	assert.Equal(t, 3.0, asFloat64(got[0]["n"]), "3 个 A")
}

// Symbol-limited aggregation SUM(A.v) only sums rows of A labels; SUM(v) for all rows.
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
		{"ts": 4, "k": 3, "v": 0},   // Non-A/B: Finishing B+
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 1.0, asFloat64(flat[0]["av"]), "SUM(A.v) 仅 A 行 = 1")
	assert.Equal(t, 111.0, asFloat64(flat[0]["allv"]), "SUM(v) 全部行 = 1+10+100")
}

// MATCH_RECOGNIZE ORDER BY DESC is meaningless in the flow (by order of arrival), Execute period fail-fast.
func TestCEP_RejectsOrderByDesc(t *testing.T) {
	s := streamsql.New()
	err := s.Execute(`SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts DESC PATTERN (A) DEFINE A AS v>0)`)
	assert.Error(t, err, "ORDER BY DESC 应被拒绝")
	s.Stop()
}

// SUBSET in MEASURES reference: SUM(S.v) sums all components of S={A,B}; S.v takes the last row of the components.
// Syntax entry: ONE ROW PER MATCH + outer SELECT specific alias (verifying no missed columns).
func TestCEP_SubsetAggregate(t *testing.T) {
	sql := `SELECT sv, last, mn FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES SUM(S.v) AS sv, SUM(A.v) AS av, S.v AS last, MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A B+)
		SUBSET S = (A, B)
		WITHIN '1h'
		DEFINE A AS k == 1, B AS k == 2
	)`
	rows := []map[string]any{
		{"ts": 1, "k": 1, "v": 1},
		{"ts": 2, "k": 2, "v": 10},
		{"ts": 3, "k": 2, "v": 100},
		{"ts": 4, "k": 3, "v": 0}, // Non-A/B: Finishing B+
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 111.0, asFloat64(flat[0]["sv"]), "SUM(S.v)=1+10+100")
	assert.Equal(t, 100.0, asFloat64(flat[0]["last"]), "S.v=末个 B=100")
	assert.Equal(t, 1.0, asFloat64(flat[0]["mn"]))
	_, hasAV := flat[0]["av"]
	assert.False(t, hasAV, "SELECT 未选 av 不应漏入")
}

// SUBSET is an atom in a PATTERN: PATTERN(S C)(S={A,B})→ (A| B) C, CLASSIFIER returns the true composition.
// Syntax entry: ALL ROWS PER MATCH + outer SELECT input fields ts and MEASURES column c.
func TestCEP_SubsetInPattern(t *testing.T) {
	sql := `SELECT ts, c FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES CLASSIFIER() AS c
		ALL ROWS PER MATCH
		PATTERN (S C)
		SUBSET S = (A, B)
		WITHIN '1h'
		DEFINE A AS k == 1, B AS k == 2, C AS k == 3
	)`
	rows := []map[string]any{
		{"ts": 1, "k": 1}, // A (Expand S for Matching)
		{"ts": 2, "k": 3}, // C
	}
	got := collectCEP(t, sql, rows, 1)
	require.Len(t, got, 1, "一次匹配")
	require.Len(t, got[0], 2, "ALL ROWS 输出 2 行")
	assert.Equal(t, "A", got[0][0]["c"], "首行经 S 展开匹配 A")
	assert.Equal(t, 1.0, asFloat64(got[0][0]["ts"]))
	assert.Equal(t, "C", got[0][1]["c"])
	assert.Equal(t, 2.0, asFloat64(got[0][1]["ts"]))
}

// Under ALL ROWS PER MATCH, FINAL AGGREGATES THE entire segment match (constant), RUNNING cuts to the current row (cumulative).
func TestCEP_FinalVsRunning(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES FINAL SUM(v) AS fs, RUNNING SUM(v) AS rs
		ALL ROWS PER MATCH
		PATTERN (A{3})
		WITHIN '1h'
		DEFINE A AS v > 0
	)`
	rows := []map[string]any{{"ts": 1, "v": 10}, {"ts": 2, "v": 20}, {"ts": 3, "v": 30}}
	got := collectCEP(t, sql, rows, 1)
	require.Len(t, got, 1, "一次匹配")
	require.Len(t, got[0], 3, "ALL ROWS 输出 3 行")
	// FINAL SUM is always 60 (entire segment); RUNNING SUM cumulative 10/30/60.
	for _, r := range got[0] {
		assert.Equal(t, 60.0, asFloat64(r["fs"]), "FINAL SUM 恒 60")
	}
	assert.Equal(t, 10.0, asFloat64(got[0][0]["rs"]), "RUNNING 行0=10")
	assert.Equal(t, 30.0, asFloat64(got[0][1]["rs"]), "RUNNING 行1=30")
	assert.Equal(t, 60.0, asFloat64(got[0][2]["rs"]), "RUNNING 行2=60")
}

// Under ONE ROW PER MATCH, FINAL matches the default (RUNNING) result: cur is already the last row, and both are equal.
func TestCEP_FinalOneRowNoChange(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES FINAL SUM(v) AS fs, SUM(v) AS rs
		ONE ROW PER MATCH
		PATTERN (A{3})
		WITHIN '1h'
		DEFINE A AS v > 0
	)`
	rows := []map[string]any{{"ts": 1, "v": 10}, {"ts": 2, "v": 20}, {"ts": 3, "v": 30}}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1)
	assert.Equal(t, 60.0, asFloat64(flat[0]["fs"]))
	assert.Equal(t, 60.0, asFloat64(flat[0]["rs"]), "ONE ROW 下 FINAL 与默认一致")
}

// WITHIN Active Expire: The sweeper periodically clears partial matches of the overwindow. After the idle exceeds WITHIN and A is actively removed,
// Subsequent B cannot be renewed → no match. A recent epoch timestamp is required (sweeper presses wall-clock to indicate expire).
func TestCEP_WithinSweeperExpires(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A B)
		WITHIN '200ms'
		DEFINE A AS k == 1, B AS k == 2
	)`
	s := streamsql.New()
	require.NoError(t, s.Execute(sql))
	var mu sync.Mutex
	var got [][]map[string]any
	s.AddSyncSink(func(r []map[string]any) {
		mu.Lock()
		got = append(got, r)
		mu.Unlock()
	})
	s.Emit(map[string]any{"ts": time.Now().UnixMilli(), "k": 1}) // A
	time.Sleep(500 * time.Millisecond)                           // >WITHIN and >sweepInterval(100ms): sweeper clears A
	s.Emit(map[string]any{"ts": time.Now().UnixMilli(), "k": 2}) // B comes without A→ No matching
	time.Sleep(200 * time.Millisecond)
	s.Stop()
	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, got, "sweeper 主动过期部分匹配，B 来时无 A → 无匹配")
}

// Comparison: When sweeper is enabled, it does not delete in-window matches (AB arrives consecutively within WITHIN → matches are made).
func TestCEP_WithinSweeperKeepsRecent(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A B)
		WITHIN '500ms'
		DEFINE A AS k == 1, B AS k == 2
	)`
	now := time.Now()
	rows := []map[string]any{
		{"ts": now.UnixMilli(), "k": 1},
		{"ts": now.Add(50 * time.Millisecond).UnixMilli(), "k": 2},
	}
	got := collectCEP(t, sql, rows, 1)
	flat := flatten(got)
	require.Len(t, flat, 1, "窗内连续匹配，sweeper 不应误删")
	assert.Equal(t, 1.0, asFloat64(flat[0]["mn"]))
}

// Greedy A*(A/B DEFINE overlaps v>0) Choose the longest: 3 events → 1 match [A,A,B](n=3).
// The longest is chosen when greed extends to the end of the flush, so a stop post-assertion is used (not collectCEP).
func TestCEP_GreedyStarLongest(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES COUNT(*) AS n, MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A* B)
		WITHIN '1h'
		DEFINE A AS v > 0, B AS v > 0
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
	for _, r := range []map[string]any{{"ts": 1, "v": 1}, {"ts": 2, "v": 2}, {"ts": 3, "v": 3}} {
		s.Emit(r)
	}
	time.Sleep(100 * time.Millisecond)
	s.Stop() // Greed Overlap: Matches after Flush picks the longest output
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1, "贪婪 A* 选最长：1 个匹配 [A,A,B]")
	assert.Equal(t, 3.0, asFloat64(got[0]["n"]), "贪婪匹配 3 行 A,A,B")
}

// Lazy A*? (A/B DEFINE Overlap) Select the shortest: 0 A + B → [B]×3 (each n = 1).
func TestCEP_ReluctantStarShortest(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES COUNT(*) AS n, MATCH_NUMBER() AS mn
		ONE ROW PER MATCH
		PATTERN (A*? B)
		WITHIN '1h'
		DEFINE A AS v > 0, B AS v > 0
	)`
	rows := []map[string]any{{"ts": 1, "v": 1}, {"ts": 2, "v": 2}, {"ts": 3, "v": 3}}
	got := collectCEP(t, sql, rows, 3)
	flat := flatten(got)
	require.Len(t, flat, 3, "懒惰 A*? 每位置选最短 [B]：3 个匹配")
	for i, r := range flat {
		assert.Equal(t, 1.0, asFloat64(r["n"]), "懒惰匹配 %d 应为 1 行", i)
	}
}

// Greedy pending is not deleted by WITHIN sweeper: sweep uses wall-clock, withinOk uses event time,
// Completed valid pendings are only generated by emitGreedy/Flush. Before repairing, sweep would accidentally delete pending and cause loss.
func TestCEP_GreedyPendingNotSwept(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES COUNT(*) AS n
		ONE ROW PER MATCH
		PATTERN (A* B)
		WITHIN '200ms'
		DEFINE A AS v > 0, B AS v > 0
	)`
	now := time.Now()
	s := streamsql.New()
	require.NoError(t, s.Execute(sql))
	var mu sync.Mutex
	var got []map[string]any
	s.AddSink(func(r []map[string]any) {
		mu.Lock()
		got = append(got, r...)
		mu.Unlock()
	})
	for i := 1; i <= 3; i++ {
		s.Emit(map[string]any{"ts": now.Add(time.Duration(i) * 10 * time.Millisecond).UnixMilli(), "v": i})
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(400 * time.Millisecond) // >sweepInterval(100ms): Sweep multiple times, but pending must be retained
	s.Stop()
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1, "贪婪 pending 不被 sweep 删除，Flush 产出最长匹配")
	assert.Equal(t, 3.0, asFloat64(got[0]["n"]), "[A,A,B]")
}
