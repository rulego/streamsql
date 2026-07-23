package e2e

import (
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	streamsql "github.com/rulego/streamsql"
)

// Standard sample sets of analysis functions, covering change detection / lag / latest
// It serves as a regression access control for analyzing function syntax.

func analyticDemo() []map[string]any {
	return []map[string]any{
		{"ts": 1, "temperature": 23, "humidity": 88},
		{"ts": 2, "temperature": 23, "humidity": 88},
		{"ts": 3, "temperature": 23, "humidity": 88},
		{"ts": 4, "temperature": 25, "humidity": 88},
		{"ts": 5, "temperature": 25, "humidity": 90},
		{"ts": 6, "temperature": 25, "humidity": 91},
		{"ts": 7, "temperature": 25, "humidity": 91},
		{"ts": 8, "temperature": 25, "humidity": 91},
	}
}

func runWindow(t *testing.T, sql string, inputs []map[string]any) []map[string]any {
	t.Helper()
	ssql := streamsql.New()
	if err := ssql.Execute(sql); err != nil {
		t.Fatalf("Execute %q: %v", sql, err)
	}
	defer ssql.Stop() // Failure Path Bottom Cover (t.Fatalf → Goexit still executed); The normal path has explicitly stopped and explicitly stopped.
	var mu sync.Mutex
	var out []map[string]any
	ssql.AddSink(func(r []map[string]any) {
		mu.Lock()
		out = append(out, r...)
		mu.Unlock()
	})
	for _, in := range inputs {
		ssql.Emit(copyRow(in))
	}
	time.Sleep(500 * time.Millisecond) // Let the window trigger + sink callback to finish
	ssql.Stop()                        // Stop and join sink worker: After running the callback, no more concurrent writes out
	mu.Lock()
	result := out
	mu.Unlock()
	return result
}

func runDirect(t *testing.T, sql string, inputs []map[string]any) []map[string]any {
	t.Helper()
	ssql := streamsql.New()
	if err := ssql.Execute(sql); err != nil {
		t.Fatalf("Execute %q: %v", sql, err)
	}
	defer ssql.Stop()
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

func copyRow(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// assertRows asserts the result rows in order (direct path results are ordered).
func assertRows(t *testing.T, sql string, got, want []map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s\n got: %v\nwant: %v", sql, got, want)
	}
}

func TestAnalytic_ChangedCols(t *testing.T) {
	d := analyticDemo()
	// Single column without prefix: initial value + change value
	assertRows(t, "E1", runDirect(t, `SELECT changed_cols("", true, temperature) FROM stream`, d),
		[]map[string]any{{"temperature": 23}, {"temperature": 25}})
	// Many are prefixed with a row of belts
	assertRows(t, "E2", runDirect(t, `SELECT changed_cols("c_", true, temperature, humidity) FROM stream`, d),
		[]map[string]any{
			{"c_temperature": 23, "c_humidity": 88},
			{"c_temperature": 25},
			{"c_humidity": 90},
			{"c_humidity": 91},
		})
	// Do not ignore null + *: output at least the change in ts per line
	assertRows(t, "E3", runDirect(t, `SELECT changed_cols("c_", false, "*") FROM stream`, d),
		[]map[string]any{
			{"c_ts": 1, "c_temperature": 23, "c_humidity": 88},
			{"c_ts": 2},
			{"c_ts": 3},
			{"c_ts": 4, "c_temperature": 25},
			{"c_ts": 5, "c_humidity": 90},
			{"c_ts": 6, "c_humidity": 91},
			{"c_ts": 7},
			{"c_ts": 8},
		})
}

func TestAnalytic_HadChanged_Where(t *testing.T) {
	d := analyticDemo()
	assertRows(t, "E5", runDirect(t, `SELECT ts, temperature, humidity FROM stream WHERE had_changed(true, temperature, humidity) = true`, d),
		[]map[string]any{
			{"ts": 1, "temperature": 23, "humidity": 88},
			{"ts": 4, "temperature": 25, "humidity": 88},
			{"ts": 5, "temperature": 25, "humidity": 90},
			{"ts": 6, "temperature": 25, "humidity": 91},
		})
	assertRows(t, "E6", runDirect(t, `SELECT ts, temperature, humidity FROM stream WHERE had_changed(true, temperature) = true AND had_changed(true, humidity) = false`, d),
		[]map[string]any{{"ts": 4, "temperature": 25, "humidity": 88}})
}

func TestAnalytic_ChangedCol_AliasAndWhere(t *testing.T) {
	d := analyticDemo()
	// Alias: nil field omitted, full nil row suppression
	assertRows(t, "E7", runDirect(t, `SELECT changed_col(true, temperature) AS myTemp, changed_col(true, humidity) AS myHum FROM stream`, d),
		[]map[string]any{
			{"myTemp": 23, "myHum": 88},
			{"myTemp": 25},
			{"myHum": 90},
			{"myHum": 91},
		})
	// WHERE comparison
	assertRows(t, "E8", runDirect(t, `SELECT ts, temperature, humidity FROM stream WHERE changed_col(true, temperature) > 24`, d),
		[]map[string]any{{"ts": 4, "temperature": 25, "humidity": 88}})
}

func TestAnalytic_LagLatest(t *testing.T) {
	d := analyticDemo()
	// lag: The first event has no previous value
	got := runDirect(t, `SELECT temperature, lag(temperature) AS prev FROM stream`, d)
	if len(got) != 8 || got[0]["prev"] != nil || got[1]["prev"] != 23 || got[3]["prev"] != 23 {
		t.Errorf("lag unexpected: %v", got)
	}
	// latest: Latest non-null value
	gotL := runDirect(t, `SELECT latest(temperature) AS lt FROM stream`, d)
	if len(gotL) != 8 || gotL[0]["lt"] != 23 || gotL[3]["lt"] != 25 {
		t.Errorf("latest unexpected: %v", gotL)
	}
}

func TestAnalytic_LagWhenHadChanged(t *testing.T) {
	// lag Example 3: WHEN embedding had_changed + arithmetic ts - lag(ts,1,ts,true)
	in := []map[string]any{
		{"ts": 1, "Status": "A", "statusCode": 100},
		{"ts": 5, "Status": "A", "statusCode": 100},
		{"ts": 8, "Status": "B", "statusCode": 200},
		{"ts": 12, "Status": "B", "statusCode": 300},
	}
	// Only assert that parsable execution + prevStatus is correct (WHEN gating semantics for duration are in the design documentation)
	got := runDirect(t, `SELECT ts, lag(Status) AS prevStatus, ts - lag(ts, 1, ts, true) OVER (WHEN had_changed(true, statusCode)) AS duration FROM stream`, in)
	wantPrev := []any{nil, "A", "A", "B"}
	for i, r := range got {
		if r["prevStatus"] != wantPrev[i] {
			t.Errorf("prevStatus[%d]=%v want %v", i, r["prevStatus"], wantPrev[i])
		}
	}
}

func TestAnalytic_Acc(t *testing.T) {
	v := []map[string]any{{"v": 1}, {"v": 2}, {"v": 3}}
	assertRows(t, "acc_sum", runDirect(t, `SELECT acc_sum(v) AS s FROM stream`, v),
		[]map[string]any{{"s": float64(1)}, {"s": float64(3)}, {"s": float64(6)}})
	assertRows(t, "acc_max", runDirect(t, `SELECT acc_max(v) AS m FROM stream`, v),
		[]map[string]any{{"m": float64(1)}, {"m": float64(2)}, {"m": float64(3)}})
	assertRows(t, "acc_min", runDirect(t, `SELECT acc_min(v) AS m FROM stream`, v),
		[]map[string]any{{"m": float64(1)}, {"m": float64(1)}, {"m": float64(1)}})
	assertRows(t, "acc_count", runDirect(t, `SELECT acc_count(v) AS c FROM stream`, v),
		[]map[string]any{{"c": int64(1)}, {"c": int64(2)}, {"c": int64(3)}})
	assertRows(t, "acc_avg", runDirect(t, `SELECT acc_avg(v) AS a FROM stream`, v),
		[]map[string]any{{"a": float64(1)}, {"a": float64(1.5)}, {"a": float64(2)}})
}

func TestAnalytic_AccConditional(t *testing.T) {
	// acc_count(a, a>1, a<0): a>1 starts accumulation, a<0 resets to zero. a:1,2,1,3,-1,1 → 0,1,2,3,0,0
	in := []map[string]any{{"a": 1}, {"a": 2}, {"a": 1}, {"a": 3}, {"a": -1}, {"a": 1}}
	assertRows(t, "条件 acc_count", runDirect(t, `SELECT acc_count(a, a > 1, a < 0) AS c FROM stream`, in),
		[]map[string]any{{"c": int64(0)}, {"c": int64(1)}, {"c": int64(2)}, {"c": int64(3)}, {"c": int64(0)}, {"c": int64(0)}})
}

// sortedFloatField extracts the value of a field from the result row and sorts it (window sink is asynchronously unsorted, compared by set).
func sortedFloatField(rows []map[string]any, key string) []float64 {
	var out []float64
	for _, r := range rows {
		out = append(out, toFloatVal(r[key]))
	}
	sort.Float64s(out)
	return out
}

func toFloatVal(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}

// Grouping + Analysis: The analysis function is partitioned by the GROUP BY key, with each group independent across window states (no crosstalk).
func TestAnalytic_Window_GroupPartition(t *testing.T) {
	in := []map[string]any{
		{"deviceId": "A", "temp": 10.0}, {"deviceId": "B", "temp": 5.0},
		{"deviceId": "A", "temp": 20.0}, {"deviceId": "B", "temp": 15.0},
		{"deviceId": "A", "temp": 30.0},
	}
	// CountingWindow(1): One window per event. acc_sum(avg(temp)) Accumulates across windows, partitioned by deviceId.
	got := runWindow(t, `SELECT deviceId, acc_sum(avg(temp)) AS s FROM stream GROUP BY deviceId, CountingWindow(1)`, in)
	byDev := map[string][]float64{}
	for _, r := range got {
		d, _ := r["deviceId"].(string)
		byDev[d] = append(byDev[d], toFloatVal(r["s"]))
	}
	for _, v := range byDev {
		sort.Float64s(v)
	}
	// A: avg=10, 20, 30 → cumulative 10, 30, 60; B: avg=5, 15 → cumulative 5, 20. Independent districts.
	if len(byDev["A"]) != 3 || byDev["A"][0] != 10 || byDev["A"][1] != 30 || byDev["A"][2] != 60 ||
		len(byDev["B"]) != 2 || byDev["B"][0] != 5 || byDev["B"][1] != 20 {
		t.Errorf("Partition error: A=%v B=%v (want A=[10,30,60] B=[5,20])", byDev["A"], byDev["B"])
	}
}

// E4: Window Change Detection—changed_cols Wrap AVG to perform change detection on window output.
func TestAnalytic_Window_ChangedColsOverAgg(t *testing.T) {
	d := analyticDemo()
	// CountingWindow(2) → Window Average 23,24,25,25; changed_cols Cross-window comparison:
	// 23 (head), 24 (variant), 25 (invariant), 25 (unchanged → suppressed) → tavg:23, 24, 25
	got := runWindow(t, `SELECT changed_cols("t", true, avg(temperature)) FROM stream GROUP BY CountingWindow(2)`, d)
	vals := sortedFloatField(got, "tavg")
	want := []float64{23, 24, 25}
	if len(vals) != len(want) || (len(vals) == 3 && (vals[0] != 23 || vals[1] != 24 || vals[2] != 25)) {
		t.Errorf("E4 tavg=%v want %v", vals, want)
	}
}

// Accumulate within the window—acc_sum packet avg, sum across windows.
func TestAnalytic_Window_AccSumOverAgg(t *testing.T) {
	d := analyticDemo()
	// 4 window avg = 23, 24, 25, 25 → cumulative sum across windows 23, 47, 72, 97
	got := runWindow(t, `SELECT acc_sum(avg(temperature)) AS s FROM stream GROUP BY CountingWindow(2)`, d)
	vals := sortedFloatField(got, "s")
	want := []float64{23, 47, 72, 97}
	if len(vals) != len(want) {
		t.Errorf("acc_sum(avg) got %d rows, want %d: %v", len(vals), len(want), vals)
		return
	}
	for i := range want {
		if vals[i] != want[i] {
			t.Errorf("acc_sum(avg)=%v want %v", vals, want)
			break
		}
	}
}

// acc_sum Direct Connection vs. Windows: Same data, different results (cumulative frequency/objects/rows all vary).
// Direct connection of events accumulates the original v; acc_sum in the window must be wrapped and aggregated, and the results of the window must be accumulated across windows.
func TestAnalytic_AccSum_WindowVsDirect(t *testing.T) {
	v := []map[string]any{{"v": 1}, {"v": 2}, {"v": 3}, {"v": 4}}

	direct := runDirect(t, `SELECT acc_sum(v) AS s FROM stream`, v)
	var directVals []float64
	for _, r := range direct {
		directVals = append(directVals, toFloatVal(r["s"]))
	}

	win := runWindow(t, `SELECT acc_sum(sum(v)) AS s FROM stream GROUP BY CountingWindow(2)`, v)
	winVals := sortedFloatField(win, "s")

	wantDirect := []float64{1, 3, 6, 10}
	wantWin := []float64{3, 10}
	if !reflect.DeepEqual(directVals, wantDirect) {
		t.Errorf("Direct connection acc_sum(v) = %v want %v", directVals, wantDirect)
	}
	if !reflect.DeepEqual(winVals, wantWin) {
		t.Errorf("Window acc_sum(sum(v))) = %v want %v", winVals, wantWin)
	}
}

// acc_sum Cumulative State Isolated by Instance: Two instances cross Emit, each independently accumulated without affecting each other.
func TestAnalytic_AccSum_InstanceIsolation(t *testing.T) {
	a := streamsql.New()
	defer a.Stop()
	if err := a.Execute("SELECT acc_sum(v) AS s FROM stream"); err != nil {
		t.Fatalf("Execute A: %v", err)
	}
	b := streamsql.New()
	defer b.Stop()
	if err := b.Execute("SELECT acc_sum(v) AS s FROM stream"); err != nil {
		t.Fatalf("Execute B: %v", err)
	}

	emit := func(s *streamsql.Streamsql, v float64) map[string]any {
		r, err := s.EmitSync(map[string]any{"v": v})
		if err != nil {
			t.Fatalf("EmitSync v=%v: %v", v, err)
		}
		if r == nil {
			t.Fatalf("EmitSync v=%v Returns nil", v)
		}
		return r
	}
	check := func(r map[string]any, want float64) {
		if got := toFloatVal(r["s"]); got != want {
			t.Errorf("s=%v want %v", got, want)
		}
	}

	// Cross-Emit: If accumulated states are shared globally, they will cause mutual contamination
	check(emit(a, 1), 1)
	check(emit(b, 10), 10)
	check(emit(a, 2), 3)
	check(emit(b, 20), 30)
	check(emit(a, 3), 6)
}

// === Regression of parse release but previously runtime silence error/empty (B1-B4 fix) ===

// B1: Arithmetic expression package analysis function—the analysis results are substituted back into the outer expression for further evaluation.
func TestRuntimeFix_B1_ArithmeticAroundAnalytic(t *testing.T) {
	d := []map[string]any{
		{"k": "d1", "ts": 1}, {"k": "d1", "ts": 2}, {"k": "d1", "ts": 3}, {"k": "d2", "ts": 10},
	}
	// ts - lag(ts):d1 → [nil,1,1];  d2 first line has no lag → nil.
	got := runDirect(t, `SELECT ts - lag(ts) OVER (PARTITION BY k) AS d FROM stream`, d)
	assertNumericField(t, "B1 ts-lag", got, "d", []any{nil, 1.0, 1.0, nil})
	// 100 - lag(ts): d1 → [nil,99,98]; d2 → nil.
	got = runDirect(t, `SELECT 100 - lag(ts) OVER (PARTITION BY k) AS d FROM stream`, d)
	assertNumericField(t, "B1 100-lag", got, "d", []any{nil, 99.0, 98.0, nil})
	// Pure analysis fields are not affected by reproduction: lag is still [nil,1,2,nil].
	got = runDirect(t, `SELECT lag(ts) OVER (PARTITION BY k) AS p FROM stream`, d)
	assertNumericField(t, "B1 plain lag", got, "p", []any{nil, 1.0, 2.0, nil})
}

// B2: Use the bare analysis function as a WHERE condition—the value-type analysis function should follow the nil determination (even if it changes to 0, select it).
func TestRuntimeFix_B2_BareAnalyticInWhere(t *testing.T) {
	d := []map[string]any{{"temp": 5}, {"temp": 5}, {"temp": 0}, {"temp": 3}}
	// changed_col: Varied rows (including changes to 0) → 5, 0, 3; the second unchanged row is filtered.
	got := runDirect(t, `SELECT temp FROM stream WHERE changed_col(true, temp)`, d)
	assertTempSeq(t, "B2 changed_col", got, []float64{5, 0, 3})
	// Explicitly > 0 excludes 0 → 5,3 (old behavior unchanged).
	got = runDirect(t, `SELECT temp FROM stream WHERE changed_col(true, temp) > 0`, d)
	assertTempSeq(t, "B2 changed_col>0", got, []float64{5, 3})
	// had_changed Raw work WHERE: Return bool direct judgment → 5, 0, 3.
	got = runDirect(t, `SELECT temp FROM stream WHERE had_changed(true, temp)`, d)
	assertTempSeq(t, "B2 had_changed", got, []float64{5, 0, 3})
}

// B3: The window analysis function parameter is a "aggregation + operation" composite expression — extract the inner layer of aggregation and leave the outer layer operators.
func TestRuntimeFix_B3_CompositeArgInlineAgg(t *testing.T) {
	d := []map[string]any{{"temp": 23}, {"temp": 25}, {"temp": 25}, {"temp": 30}}
	// CountingWindow(2) → Two windows avg=24, 27.5; avg(temp)+1 → 25, 28.5.
	got := runWindow(t, `SELECT changed_col(true, avg(temp) + 1) AS c FROM stream GROUP BY CountingWindow(2)`, d)
	if vals := sortedFloatField(got, "c"); !reflect.DeepEqual(vals, []float64{25, 28.5}) {
		t.Errorf("B3 avg+1: got %v, want [25, 28.5]", vals)
	}
	// Pure AVG (temp) baseline unchanged → 24, 27.5.
	got = runWindow(t, `SELECT changed_col(true, avg(temp)) AS c FROM stream GROUP BY CountingWindow(2)`, d)
	if vals := sortedFloatField(got, "c"); !reflect.DeepEqual(vals, []float64{24, 27.5}) {
		t.Errorf("B3 avg baseline: got %v, want [24, 27.5]", vals)
	}
}

// B4: Window analysis function parameters use limited columns (tables). column) — Runtime stripping of prefixes to the GROUP BY key does not return literal strings.
func TestRuntimeFix_B4_QualifiedColumnArg(t *testing.T) {
	d := []map[string]any{{"k": "d1"}, {"k": "d1"}, {"k": "d2"}, {"k": "d2"}}
	got := runWindow(t, `SELECT changed_col(true, stream.k) AS c FROM stream GROUP BY k, CountingWindow(2)`, d)
	if len(got) == 0 {
		t.Fatalf("B4: expected non-empty output")
	}
	for _, r := range got {
		if s, _ := r["c"].(string); s == "stream.k" {
			t.Errorf("B4: qualified arg leaked literal %q in %v", "stream.k", r)
		}
		if c, _ := r["c"].(string); c != r["k"] {
			t.Errorf("B4: c=%v should equal group key k=%v in %v", r["c"], r["k"], r)
		}
	}
}

// assertNumericField Sequentially asserts field values: want[i]==nil Expectation nil; Otherwise, compare by floating-point (accommodates int/float64).
func assertNumericField(t *testing.T, label string, got []map[string]any, key string, want []any) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %d rows, want %d (%v)", label, len(got), len(want), got)
		return
	}
	for i, row := range got {
		v := row[key]
		if want[i] == nil {
			if v != nil {
				t.Errorf("%s row %d: want nil, got %v", label, i, v)
			}
			continue
		}
		if toFloatVal(v) != toFloatVal(want[i]) {
			t.Errorf("%s row %d: got %v, want %v", label, i, v, want[i])
		}
	}
}

// assertTempSeq asserts the floating-point sequence of temp fields in order (direct path order).
func assertTempSeq(t *testing.T, label string, got []map[string]any, want []float64) {
	t.Helper()
	vals := make([]float64, 0, len(got))
	for _, r := range got {
		vals = append(vals, toFloatVal(r["temp"]))
	}
	if !reflect.DeepEqual(vals, want) {
		t.Errorf("%s: got %v, want %v", label, vals, want)
	}
}
