package e2e

import (
	"reflect"
	"sort"
	"testing"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This document specifically covers scenarios for "multi-feature true combinations": JOIN× analysis, inter-aggregate arithmetic, dual-analysis arithmetic,
// LEFT JOIN× COALESCE NULL filling, multi-group, multi-indicator invariants. Expected values are derived according to SQL semantics,
// Used to verify the correctness of the combined path; Does not duplicate single-attribute files.

// runDirectJoin runs JOIN queries on the direct connection path: after executing, the registry receives each EmitSync line by line that is not nil.
func runDirectJoin(t *testing.T, sql, table string, tableRows []map[string]any, inputs []map[string]any) []map[string]any {
	t.Helper()
	ssql := streamsql.New()
	if err := ssql.Execute(sql); err != nil {
		t.Fatalf("Execute %q: %v", sql, err)
	}
	defer ssql.Stop()
	if _, err := ssql.RegisterTable(table, tableRows); err != nil {
		t.Fatalf("RegisterTable %s: %v", table, err)
	}
	var out []map[string]any
	for _, in := range inputs {
		r, err := ssql.EmitSync(copyRow(in))
		if err != nil {
			t.Fatalf("EmitSync %q: %v", sql, err)
		}
		if r != nil {
			out = append(out, r)
		}
	}
	return out
}

// === JOIN × Analysis Function (After metadata enhancement, lag/cumulative is done by partitioning by enhanced fields) ===

// After JOINing to enhance the location, lag(temp) partitions the previous temp by location.
// meta: d1→plantA, d2→plantB, d3→plantA.
//
// PARTITION BY connects fields m.location via resolvePartitionField and parses the fieldpath
// (If the enhanced row location is nested under m, the bare row[k] constant nil will degenerate into a global partition.)
func TestScenario_JoinAnalytic_LagByLocation(t *testing.T) {
	const sql = `SELECT deviceId, m.location AS loc, lag(temp) OVER (PARTITION BY m.location) AS prev
FROM stream JOIN meta m ON deviceId = m.deviceId`
	meta := []map[string]any{
		{"deviceId": "d1", "location": "plantA"},
		{"deviceId": "d2", "location": "plantB"},
		{"deviceId": "d3", "location": "plantA"},
	}
	in := []map[string]any{
		{"deviceId": "d1", "temp": 10}, // plantA first → prev=nil
		{"deviceId": "d2", "temp": 20}, // plantB First → prev=nil
		{"deviceId": "d3", "temp": 30}, // plantA Article 2 → prev=10
		{"deviceId": "d1", "temp": 40}, // plantA Article 3 → prev=30
		{"deviceId": "d2", "temp": 50}, // plantB Article 2 → prev=20
	}
	got := runDirectJoin(t, sql, "meta", meta, in)
	wantPrev := []any{nil, nil, 10.0, 30.0, 20.0}
	wantLoc := []string{"plantA", "plantB", "plantA", "plantA", "plantB"}
	if len(got) != len(wantPrev) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(wantPrev), got)
	}
	for i, r := range got {
		if r["loc"] != wantLoc[i] {
			t.Errorf("row %d loc=%v want %s", i, r["loc"], wantLoc[i])
		}
		if wantPrev[i] == nil {
			if r["prev"] != nil {
				t.Errorf("row %d prev=%v want nil", i, r["prev"])
			}
		} else if toFloatVal(r["prev"]) != wantPrev[i].(float64) {
			t.Errorf("row %d prev=%v want %v", i, r["prev"], wantPrev[i])
		}
	}
}

// After JOIN enhancement, acc_sum (temp) is used to accumulate across rows by partitioning location (same as LagByLocation, partition keys follow the fieldpath).
func TestScenario_JoinAnalytic_AccSumByLocation(t *testing.T) {
	const sql = `SELECT m.location AS loc, acc_sum(temp) OVER (PARTITION BY m.location) AS s
FROM stream JOIN meta m ON deviceId = m.deviceId`
	meta := []map[string]any{
		{"deviceId": "d1", "location": "plantA"},
		{"deviceId": "d2", "location": "plantB"},
		{"deviceId": "d3", "location": "plantA"},
	}
	in := []map[string]any{
		{"deviceId": "d1", "temp": 10}, // plantA → 10
		{"deviceId": "d2", "temp": 20}, // plantB → 20
		{"deviceId": "d3", "temp": 30}, // plantA → 40
		{"deviceId": "d1", "temp": 40}, // plantA → 80
		{"deviceId": "d2", "temp": 50}, // plantB → 70
	}
	got := runDirectJoin(t, sql, "meta", meta, in)
	want := []float64{10, 20, 40, 80, 70}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if toFloatVal(r["s"]) != want[i] {
			t.Errorf("row %d s=%v want %v", i, r["s"], want[i])
		}
	}
}

// === Arithmetic between aggregations (perform operations after aggregating window results) ===

// Window max-min is extremely poor. CountingWindow(2) Three-window: [3,7]→4, [1,4]→3, [10,2]→8.
func TestScenario_AggArithmetic_WindowRange(t *testing.T) {
	in := []map[string]any{{"v": 3}, {"v": 7}, {"v": 1}, {"v": 4}, {"v": 10}, {"v": 2}}
	got := runWindow(t, `SELECT max(v) - min(v) AS rng FROM stream GROUP BY CountingWindow(2)`, in)
	if vals := sortedFloatField(got, "rng"); !reflect.DeepEqual(vals, []float64{3, 4, 8}) {
		t.Errorf("window max-min range: got %v, want [3 4 8]", vals)
	}
}

// The manual mean sum/count should match the built-in avg (floating-point input avoids division).
// Three-window [3,7]→5, [1,4]→2.5, [10,2]→6.
func TestScenario_AggArithmetic_ManualAvgMatchesBuiltIn(t *testing.T) {
	in := []map[string]any{{"v": 3.0}, {"v": 7.0}, {"v": 1.0}, {"v": 4.0}, {"v": 10.0}, {"v": 2.0}}
	got := runWindow(t, `SELECT sum(v) / count(*) AS manual, avg(v) AS built FROM stream GROUP BY CountingWindow(2)`, in)
	manual := sortedFloatField(got, "manual")
	built := sortedFloatField(got, "built")
	want := []float64{2.5, 5, 6}
	if !reflect.DeepEqual(manual, want) {
		t.Errorf("manual avg sum/count: got %v, want %v", manual, want)
	}
	if !reflect.DeepEqual(built, want) {
		t.Errorf("built-in avg: got %v, want %v", built, want)
	}
}

// === Dual analysis function arithmetic (the same expression contains two analysis calls) ===

// Operating period range: acc_max-acc_min. v:3,1,4,1,5 → acc_max[3,3,4,4,5] acc_min[3,1,1,1,1] → extreme [0,2,3,3,4].
//
// The same expression contains two analysis calls: splitAnalyticExprMulti draws all calls and replaces placeholders
// (__analytic_self__ / __analytic_self_1__), calculate each valuation period separately, inject placeholder, then find the wrapper.
func TestScenario_TwoAnalyticArithmetic_RunningRange(t *testing.T) {
	in := []map[string]any{{"v": 3}, {"v": 1}, {"v": 4}, {"v": 1}, {"v": 5}}
	got := runDirect(t, `SELECT acc_max(v) - acc_min(v) AS rr FROM stream`, in)
	want := []float64{0, 2, 3, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if toFloatVal(r["rr"]) != want[i] {
			t.Errorf("row %d rr=%v want %v", i, r["rr"], want[i])
		}
	}
}

// Three analysis calls + mixed arithmetic: acc_max + acc_min + acc_sum.
// v:3,1,4,1,5 → max[3,3,4,4,5] min[3,1,1,1,1] sum[3,4,8,9,14] → The sum of the three [9,8,13,14,20].
// Verification of the same expression is not limited to two calls; placeholders __analytic_self__/__analytic_self_1__/__analytic_self_2__ are each substituted.
func TestScenario_ThreeAnalyticArithmetic_SumOfRunning(t *testing.T) {
	in := []map[string]any{{"v": 3}, {"v": 1}, {"v": 4}, {"v": 1}, {"v": 5}}
	got := runDirect(t, `SELECT acc_max(v) + acc_min(v) + acc_sum(v) AS s FROM stream`, in)
	want := []float64{9, 8, 13, 14, 20}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if toFloatVal(r["s"]) != want[i] {
			t.Errorf("row %d s=%v want %v", i, r["s"], want[i])
		}
	}
}

// Two analysis calls + multiplication and scalar: acc_max * acc_min.
// v:3,1,4,1,5 → [9,3,4,4,5]. Verification arithmetic operators are not limited by addition or subtraction; all operators supported by expr bridge are acceptable.
func TestScenario_TwoAnalyticArithmetic_Multiply(t *testing.T) {
	in := []map[string]any{{"v": 3}, {"v": 1}, {"v": 4}, {"v": 1}, {"v": 5}}
	got := runDirect(t, `SELECT acc_max(v) * acc_min(v) AS p FROM stream`, in)
	want := []float64{9, 3, 4, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if toFloatVal(r["p"]) != want[i] {
			t.Errorf("row %d p=%v want %v", i, r["p"], want[i])
		}
	}
}

// Complex priority expression: acc_max/10 - acc_min*10 + acc_sum (three calls + multiplication, division, addition, subtraction, mix).
// v:3,1,4,1,5 → max[3,3,4,4,5] min[3,1,1,1,1] sum[3,4,8,9,14]
// row= max/10 - min*10 + sum:-26.7,-5.7,-1.6,-0.6,4.5. Verify that */ takes precedence over +-, and multiple calls are correct.
func TestScenario_ThreeAnalyticArithmetic_ComplexPrecedence(t *testing.T) {
	in := []map[string]any{{"v": 3}, {"v": 1}, {"v": 4}, {"v": 1}, {"v": 5}}
	got := runDirect(t, `SELECT acc_max(v)/10 - acc_min(v)*10 + acc_sum(v) AS c FROM stream`, in)
	want := []float64{-26.7, -5.7, -1.6, -0.6, 4.5}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if abs(toFloatVal(r["c"])-want[i]) > 1e-9 {
			t.Errorf("row %d c=%v want %v", i, r["c"], want[i])
		}
	}
}

// === LEFT JOIN × COALESCE (blank pad without matching row) ===

// If LEFT JOIN has no match, m.location is nil, coalesce is set to the default value.
func TestScenario_LeftJoinCoalesce_FillUnknown(t *testing.T) {
	const sql = `SELECT deviceId, coalesce(m.location, 'unknown') AS loc
FROM stream LEFT JOIN meta m ON deviceId = m.deviceId`
	meta := []map[string]any{
		{"deviceId": "d1", "location": "plantA"},
		{"deviceId": "d2", "location": "plantB"},
	}
	in := []map[string]any{
		{"deviceId": "d1"}, // Match → plantA
		{"deviceId": "d9"}, // No match → unknown
	}
	got := runDirectJoin(t, sql, "meta", meta, in)
	want := []map[string]any{
		{"deviceId": "d1", "loc": "plantA"},
		{"deviceId": "d9", "loc": "unknown"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LEFT JOIN coalesce: got %v, want %v", got, want)
	}
}

// === Multi-group, multi-indicator invariant formula (check the intrinsic relationships between indicators, not by value) ===

// Each window is grouped by device, asserting that each group satisfies: min<=avg<=max, and sum==count*avg.
// CountingWindow(2) is the global counting window: 6 events → 3 windows, which are grouped by device within the window.
func TestScenario_MultiGroupMetrics_Invariants(t *testing.T) {
	in := []map[string]any{
		{"device": "A", "v": 1.0}, {"device": "A", "v": 3.0},
		{"device": "B", "v": 2.0}, {"device": "B", "v": 4.0},
		{"device": "A", "v": 5.0}, {"device": "B", "v": 6.0},
	}
	got := runWindow(t,
		`SELECT device, sum(v) AS s, count(*) AS c, avg(v) AS a, min(v) AS mn, max(v) AS mx
FROM stream GROUP BY device, CountingWindow(2)`, in)
	if len(got) == 0 {
		t.Fatal("expected non-empty window output")
	}
	for _, r := range got {
		s := toFloatVal(r["s"])
		c := toFloatVal(r["c"])
		a := toFloatVal(r["a"])
		mn := toFloatVal(r["mn"])
		mx := toFloatVal(r["mx"])
		if mn > a+1e-9 || a > mx+1e-9 {
			t.Errorf("metric order violated min(%v)<=avg(%v)<=max(%v) in %v", mn, a, mx, r)
		}
		if c > 0 && abs(s-c*a) > 1e-6 {
			t.Errorf("sum!=count*avg: sum=%v count=%v avg=%v in %v", s, c, a, r)
		}
	}
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// === Supplement: Window aggregation results are used for composite projections outside HAVING ===

// In the same window, compute both sum and avg, and perform arithmetic on the result (avg*count should ==sum).
// Verify inter-aggregation consistency: complementary to ManualAvg, here asserting sum - avg*count ≈ 0.
func TestScenario_WindowAggConsistency(t *testing.T) {
	in := []map[string]any{{"v": 2.0}, {"v": 4.0}, {"v": 6.0}, {"v": 8.0}}
	got := runWindow(t,
		`SELECT sum(v) AS s, avg(v) AS a, count(*) AS c FROM stream GROUP BY CountingWindow(2)`, in)
	s := sortedFloatField(got, "s")
	a := sortedFloatField(got, "a")
	c := sortedFloatField(got, "c")
	// Two windows [2,4]→sum6/avg3/count2; [6,8]→sum14/avg7/count2
	wantSum := []float64{6, 14}
	wantAvg := []float64{3, 7}
	wantCnt := []float64{2, 2}
	if !reflect.DeepEqual(s, wantSum) || !reflect.DeepEqual(a, wantAvg) || !reflect.DeepEqual(c, wantCnt) {
		t.Errorf("agg consistency: sum=%v avg=%v count=%v (want sum=%v avg=%v count=%v)", s, a, c, wantSum, wantAvg, wantCnt)
	}
	for i := range s {
		if abs(s[i]-a[i]*c[i]) > 1e-6 {
			t.Errorf("window %d: sum(%v)!=avg(%v)*count(%v)", i, s[i], a[i], c[i])
		}
	}
}

// === Second batch: HAVING citation of unselected aggregation / GROUP BY expressions / multi-OVER partitions / analysis CASE or coalesce / WHERE and analysis order ===

// HAVING references aggregation max(v) that does not appear in SELECT. CountingWindow(3) is the key counting window:
// Each device has 3 triggers. A = [1,2,1]→c3 max2(max>4 ✗ filter); B=[5,6,1]→c3 max6 (>4 ✓).
// Expect only line B c3 — proving that the unselected max(v) is correctly supplemented (otherwise, nil>4 is always false → empty).
//
// extractHavingAggregates registers max(v) as a hidden aggregation __having_0__ lets aggregator complete the computation,
// HAVING text rewritten as __having_0__; Output the stripped key.
func TestScenario_Having_NonSelectedAggregate(t *testing.T) {
	in := []map[string]any{
		{"device": "A", "v": 1}, {"device": "B", "v": 5}, {"device": "A", "v": 2},
		{"device": "B", "v": 6}, {"device": "A", "v": 1}, {"device": "B", "v": 1},
	}
	got := runWindow(t,
		`SELECT device, count(*) AS c FROM stream GROUP BY device, CountingWindow(3) HAVING max(v) > 4`, in)
	byDev := map[string]float64{}
	for _, r := range got {
		d, _ := r["device"].(string)
		byDev[d] = toFloatVal(r["c"])
	}
	if len(byDev) != 1 || byDev["B"] != 3 {
		t.Errorf("HAVING max(v)>4 (max not selected): got %v, want {B:3}", byDev)
	}
}

// GROUP BY function expression upper(device). CountingWindow(4) Single window, after the upper window, split into two groups.
// GROUP BY function expression upper(device). CountingWindow(2) is the key-based counting window: input aa, AA, bb, BB
// → upper are AA, AA, BB, BB. AA triggers c2 when 2 hits, BB triggers c2 when 2 hits. Expectation {AA:2, BB:2}.
// If upper() is not active (grouped by original device), then 4 different values each have 1 line, and no key fills 2 → empty.
//
// parser reads upper(device) as the overall group key; injectGroupKeyExprs is evaluated and injected before Window.Add;
// projectGroupColumns renames the group key to output the alias d.
func TestScenario_GroupBy_FunctionExpression(t *testing.T) {
	in := []map[string]any{
		{"device": "aa"}, {"device": "AA"}, {"device": "bb"}, {"device": "BB"},
	}
	got := runWindow(t,
		`SELECT upper(device) AS d, count(*) AS c FROM stream GROUP BY upper(device), CountingWindow(2)`, in)
	byD := map[string]float64{}
	for _, r := range got {
		d, _ := r["d"].(string)
		byD[d] = toFloatVal(r["c"])
	}
	if len(byD) != 2 || byD["AA"] != 2 || byD["BB"] != 2 {
		t.Errorf("GROUP BY upper(device): got %v, want {AA:2 BB:2}", byD)
	}
}

// GROUP BY time function expression hour(timestamp). hour() takes the hour of the "YYYY-MM-DD HH:MM:SS" string.
// CountingWindow(2) Counts by key (hour): hour10 triggers 2 → c2, hour11 triggers 2 → c2.
// Expectation {10:2, 11:2}. The group key retains the original type, and hour() returns int, so the h column is a numeric value 10/11.
func TestScenario_GroupBy_HourExpression(t *testing.T) {
	in := []map[string]any{
		{"timestamp": "2026-07-12 10:00:00"},
		{"timestamp": "2026-07-12 10:30:00"}, // hour 10
		{"timestamp": "2026-07-12 11:00:00"}, // hour 11
		{"timestamp": "2026-07-12 11:30:00"}, // hour 11
	}
	got := runWindow(t,
		`SELECT hour(timestamp) AS h, count(*) AS c FROM stream GROUP BY hour(timestamp), CountingWindow(2)`, in)
	byH := map[int]float64{}
	for _, r := range got {
		byH[int(toFloatVal(r["h"]))] = toFloatVal(r["c"])
	}
	if len(byH) != 2 || byH[10] != 2 || byH[11] != 2 {
		t.Errorf("GROUP BY hour(timestamp): got %v, want {10:2 11:2}", byH)
	}
}

// For the same SELECT with two analysis functions and different PARTITION BY, the state machine should be independent and free of crosstalk.
// p1=lag(a) PARTITION BY k1; p2=lag(b) PARTITION BY k2.
func TestScenario_MultiOver_DifferentPartitions(t *testing.T) {
	in := []map[string]any{
		{"k1": 1, "a": 10, "k2": "x", "b": 100},
		{"k1": 2, "a": 20, "k2": "x", "b": 200},
		{"k1": 1, "a": 30, "k2": "y", "b": 300},
		{"k1": 2, "a": 40, "k2": "x", "b": 400},
	}
	got := runDirect(t, `SELECT lag(a) OVER (PARTITION BY k1) AS p1, lag(b) OVER (PARTITION BY k2) AS p2 FROM stream`, in)
	// p1 According to k1: k1=1 [row 0,2] →nil,10; k1=2 [row 1,3] →nil,20
	// p2 According to k2: x[row 0,1,3]→nil,100,200; y[row 2]→nil
	assertNumericField(t, "multi-over p1", got, "p1", []any{nil, nil, 10.0, 20.0})
	assertNumericField(t, "multi-over p2", got, "p2", []any{nil, 100.0, nil, 200.0})
}

// The analysis function is embedded in the CASE: lag(temp) parses and then substitutes back into the CASE for evaluation.
// temp 10,25,15,30 → lag[nil,10,25,15] → lag>20? up:down → [down, down, up, down] (nil is a false → down).
// Scalar Suite Analysis: applyWrapper evaluates via expr bridge → expr package, and CASE evaluates via expr package.
func TestScenario_AnalyticInside_Case(t *testing.T) {
	in := []map[string]any{{"temp": 10}, {"temp": 25}, {"temp": 15}, {"temp": 30}}
	got := runDirect(t, `SELECT CASE WHEN lag(temp) > 20 THEN 'up' ELSE 'down' END AS trend FROM stream`, in)
	want := []string{"down", "down", "up", "down"}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if r["trend"] != want[i] {
			t.Errorf("row %d trend=%v want %s (full %v)", i, r["trend"], want[i], got)
		}
	}
}

// Control group: JOIN + parsing function, but PARTITION BY is **stream with columns** deviceId (in-line existence).
// Comparison with the Comparison (partition key is the Connect field): Here, the deviceId partition should be correctly pressed.
func TestScenario_JoinAnalytic_PartitionByStreamColumn(t *testing.T) {
	const sql = `SELECT deviceId, lag(temp) OVER (PARTITION BY deviceId) AS prev
FROM stream JOIN meta m ON deviceId = m.deviceId`
	meta := []map[string]any{
		{"deviceId": "d1", "location": "plantA"},
		{"deviceId": "d2", "location": "plantB"},
	}
	in := []map[string]any{
		{"deviceId": "d1", "temp": 10},
		{"deviceId": "d2", "temp": 20},
		{"deviceId": "d1", "temp": 30},
		{"deviceId": "d2", "temp": 40},
	}
	got := runDirectJoin(t, sql, "meta", meta, in)
	assertNumericField(t, "JOIN+analytic 分区流列", got, "prev", []any{nil, nil, 10.0, 20.0})
}

// coalesce package analysis function: first line lag = nil → set to default -1.
// temp 10,20,30 → lag[nil,10,20] → coalesce(lag,-1)=[-1,10,20].
// Scalar sleeve analysis: applyWrapper does not short circuit on nil, coalesce(nil, -1) →-1.
func TestScenario_CoalesceWraps_Analytic(t *testing.T) {
	in := []map[string]any{{"temp": 10}, {"temp": 20}, {"temp": 30}}
	got := runDirect(t, `SELECT coalesce(lag(temp), -1) AS safe_prev FROM stream`, in)
	assertNumericField(t, "coalesce(lag,-1)", got, "safe_prev", []any{-1.0, 10.0, 20.0})
}

// The evaluation order of WHERE and the analysis function (standard SQL: WHERE filters first, analysis only looks at the rows after filtering).
// temp 10,20,15,30,WHERE temp>12 Retain 20,15,30; lag on filtered stream → [nil,20,15];
// d=temp-lag → [nil,-5,15]. Regular WHERE scenes; The CDC model (WHERE citation analysis) still leads the analysis.
func TestScenario_WhereVsAnalytic_Ordering(t *testing.T) {
	in := []map[string]any{{"temp": 10}, {"temp": 20}, {"temp": 15}, {"temp": 30}}
	got := runDirect(t, `SELECT temp, temp - lag(temp) AS d FROM stream WHERE temp > 12`, in)
	assertNumericField(t, "WHERE先于分析", got, "d", []any{nil, -5.0, 15.0})
}

// Analyze function combinations and boundary testing: coexist with multiple functions, combine with partitions/windows/conditions to identify potential bugs.

// Multiple analysis functions coexist within the same query, each in its own independent state.
func TestCombo_MultipleAnalytics_Direct(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(
		`SELECT lag(a) AS p, acc_sum(b) AS s, changed_col(true, c) AS cc FROM stream`))
	defer ssql.Stop()
	in := []map[string]any{{"a": 1, "b": 10, "c": "x"}, {"a": 2, "b": 20, "c": "x"}, {"a": 3, "b": 30, "c": "y"}}
	var got []map[string]any
	for _, r := range in {
		out, err := ssql.EmitSync(copyRow(r))
		require.NoError(t, err)
		got = append(got, out)
	}
	// lag: nil,1,2; acc_sum(b): 10,30,60; changed_col(c): x (beginning), nil (unchanged), y (changed→cc omitted nil)
	assert.Nil(t, got[0]["p"])
	assert.InDelta(t, 10.0, toFloatVal(got[0]["s"]), 0.01)
	assert.Equal(t, "x", got[0]["cc"])
	assert.InDelta(t, 1.0, toFloatVal(got[1]["p"]), 0.01)
	assert.InDelta(t, 30.0, toFloatVal(got[1]["s"]), 0.01)
	_, hasCC1 := got[1]["cc"] // c unchanged → cc omitted
	assert.False(t, hasCC1)
	assert.InDelta(t, 2.0, toFloatVal(got[2]["p"]), 0.01)
	assert.InDelta(t, 60.0, toFloatVal(got[2]["s"]), 0.01)
	assert.Equal(t, "y", got[2]["cc"])
}

// acc_sum + PARTITION BY: Each partition is accumulated separately.
func TestCombo_AccSum_Partition(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(
		`SELECT k, acc_sum(v) OVER (PARTITION BY k) AS s FROM stream`))
	defer ssql.Stop()
	in := []map[string]any{{"k": "A", "v": 1}, {"k": "B", "v": 2}, {"k": "A", "v": 3}, {"k": "B", "v": 4}}
	var got []map[string]any
	for _, r := range in {
		out, err := ssql.EmitSync(copyRow(r))
		require.NoError(t, err)
		got = append(got, out)
	}
	// Input order: A1, B2, A3, B4 → A cumulative 1,4; B cumulative 2,6 (partition-independent, no mixing)
	assert.Equal(t, "A", got[0]["k"])
	assert.InDelta(t, 1.0, toFloatVal(got[0]["s"]), 0.01)
	assert.Equal(t, "B", got[1]["k"])
	assert.InDelta(t, 2.0, toFloatVal(got[1]["s"]), 0.01)
	assert.Equal(t, "A", got[2]["k"])
	assert.InDelta(t, 4.0, toFloatVal(got[2]["s"]), 0.01)
	assert.Equal(t, "B", got[3]["k"])
	assert.InDelta(t, 6.0, toFloatVal(got[3]["s"]), 0.01)
}

// Condition ACC + PARTITION: The start/reset lifecycle for each partition.
func TestCombo_CondAcc_Partition(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(
		`SELECT k, acc_count(v, v > 1, v < 0) OVER (PARTITION BY k) AS c FROM stream`))
	defer ssql.Stop()
	// A: 1,2,3,-1,1 → 0,1,2,0,0; B: 5.5 → 5>1 Total: 1.2
	in := []map[string]any{{"k": "A", "v": 1}, {"k": "A", "v": 2}, {"k": "B", "v": 5}, {"k": "A", "v": 3}, {"k": "B", "v": 5}, {"k": "A", "v": -1}}
	var got []map[string]any
	for _, r := range in {
		out, err := ssql.EmitSync(copyRow(r))
		require.NoError(t, err)
		got = append(got, out)
	}
	byK := map[string][]int64{}
	for _, r := range got {
		k, _ := r["k"].(string)
		byK[k] = append(byK[k], toInt64Val(r["c"]))
	}
	// A in order: 0, 1, 2, 0 (A zeros -1); B: 1,2
	assert.Equal(t, []int64{0, 1, 2, 0}, byK["A"], "A 分区条件累计")
	assert.Equal(t, []int64{1, 2}, byK["B"], "B 分区条件累计")
}

// lag into window: outputs a lag across windows when aggregated.
func TestCombo_Lag_InWindow(t *testing.T) {
	d := []map[string]any{{"t": 10.0}, {"t": 20.0}, {"t": 30.0}, {"t": 40.0}}
	got := runWindow(t, `SELECT lag(avg(t)) AS p FROM stream GROUP BY CountingWindow(1)`, d)
	// One window per event, avg = t: 10, 20, 30, 40; lag across windows: nil, 10, 20, 30
	var vals []float64
	nilCnt := 0
	for _, r := range got {
		if r["p"] == nil {
			nilCnt++
			continue
		}
		vals = append(vals, toFloatVal(r["p"]))
	}
	sort.Float64s(vals)
	assert.Equal(t, 1, nilCnt, "首窗 lag 应为 nil")
	assert.Equal(t, []float64{10, 20, 30}, vals, "lag(avg) 跨窗口非 nil 值: 10,20,30")
}

// had_changed In-window: Detects whether the window aggregate output has changed.
func TestCombo_HadChanged_InWindow(t *testing.T) {
	d := []map[string]any{{"t": 10.0}, {"t": 10.0}, {"t": 20.0}, {"t": 20.0}}
	got := runWindow(t, `SELECT had_changed(true, avg(t)) AS h FROM stream GROUP BY CountingWindow(2)`, d)
	// Window Average: 10, 20 → had_changed: true (initial), true (variable)
	bools := []bool{}
	for _, r := range got {
		b, _ := r["h"].(bool)
		bools = append(bools, b)
	}
	// Both windows are true (first + change); If crosstalk/misalignment occurs, false will appear
	require.Len(t, bools, 2, "应 2 个窗口输出")
	for _, b := range bools {
		assert.True(t, b, "had_changed(avg) 首窗与变化窗均应 true")
	}
}

// Dual inline aggregation within the window: changed_cols tracks both AVG and MAX simultaneously.
func TestCombo_Window_TwoAggregates(t *testing.T) {
	d := []map[string]any{{"t": 10.0}, {"t": 20.0}, {"t": 30.0}, {"t": 30.0}}
	got := runWindow(t, `SELECT changed_cols("c_", true, avg(t), max(t)) FROM stream GROUP BY CountingWindow(2)`, d)
	// Window 1 [10,20]: avg=15, max=20 (both → and first change) {c_avg:15, c_max:20}
	// Window 2 [30,30]: avg=30, max=30 (all changes) {c_avg:30, c_max:30}
	keys := map[string]bool{}
	for _, r := range got {
		for k := range r {
			if k == "c_avg" || k == "c_max" {
				keys[k] = true
			}
		}
	}
	assert.True(t, keys["c_avg"] && keys["c_max"], "双内联聚合应产出 c_avg 与 c_max 列；got=%v", got)
}

// Document Case 9 Mirroring: Device-Level Cross-Window Changes — changed_col (avg) partitioned by GROUP BY.
func TestCombo_DocCase_GroupedCrossWindow(t *testing.T) {
	in := []map[string]any{
		{"deviceId": "A", "temp": 10.0}, {"deviceId": "A", "temp": 20.0},
		{"deviceId": "A", "temp": 30.0}, {"deviceId": "A", "temp": 40.0},
		{"deviceId": "B", "temp": 5.0}, {"deviceId": "B", "temp": 5.0},
	}
	got := runWindow(t, `SELECT deviceId, changed_col(true, avg(temp)) AS chg FROM stream GROUP BY deviceId, CountingWindow(2)`, in)
	// Window A averages 15.35; Window B averages 5. Each piece of equipment is independent: A→{15,35}, B→{5}.
	byDev := map[string][]float64{}
	for _, r := range got {
		k, _ := r["deviceId"].(string)
		if r["chg"] != nil {
			byDev[k] = append(byDev[k], toFloatVal(r["chg"]))
		}
	}
	for _, v := range byDev {
		sort.Float64s(v)
	}
	assert.Equal(t, []float64{15, 35}, byDev["A"], "A 设备跨窗口变化")
	assert.Equal(t, []float64{5}, byDev["B"], "B 设备跨窗口变化")
}

// D1: In window queries, the analysis function references the raw column → an error (no longer silently returns column name).
func TestCombo_D1_RawColumnInWindowErrors(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	err := ssql.Execute(`SELECT lag(temperature) AS p FROM stream GROUP BY CountingWindow(2)`)
	assert.Error(t, err, "裸原始列进窗口分析函数应报错")
	assert.Contains(t, err.Error(), "raw column")
}

// D9: Analytical suite analysis / aggregate suite analysis → error report; Analytical set aggregation is still allowed.
func TestCombo_D9_NestedAnalyticErrors(t *testing.T) {
	for _, sql := range []string{
		`SELECT lag(lag(a)) AS p FROM stream`,
		`SELECT sum(lag(a)) AS s FROM stream GROUP BY CountingWindow(2)`,
	} {
		ssql := streamsql.New()
		err := ssql.Execute(sql)
		assert.Error(t, err, "嵌套分析应报错: %s", sql)
		ssql.Stop()
	}
	// Analytical set aggregation (within the window) remains valid.
	ok := streamsql.New()
	defer ok.Stop()
	assert.NoError(t, ok.Execute(`SELECT changed_cols("t", true, avg(temperature)) FROM stream GROUP BY CountingWindow(2)`))
}

// D5: acc_avg Null cumulative returns nil (consistent with acc_max/min), not 0.0.
func TestCombo_D5_AccAvgEmptyNil(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT acc_avg(s) AS a FROM stream`))
	defer ssql.Stop()
	// Non-numeric input → count remains at 0 → nil
	r, err := ssql.EmitSync(map[string]any{"s": "not-a-number"})
	require.NoError(t, err)
	assert.Nil(t, r["a"], "空 acc_avg 应返回 nil 而非 0.0")
}

func toInt64Val(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}
