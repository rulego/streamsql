package cep

import (
	"testing"
)

func syms(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

// --- EvalDefine ---

func TestEvalDefine_BareComparison(t *testing.T) {
	if !EvalDefine("v > 50", nil, nil, map[string]any{"v": 60}, "A", syms("A")) {
		t.Errorf("v=60 > 50 want true")
	}
	if EvalDefine("v > 50", nil, nil, map[string]any{"v": 40}, "A", syms("A")) {
		t.Errorf("v=40 > 50 want false")
	}
}

// Null conditions (undefined symbols) are always true (SQL standard).
func TestEvalDefine_EmptyIsTrue(t *testing.T) {
	if !EvalDefine("", nil, nil, map[string]any{"v": 1}, "A", syms("A")) {
		t.Errorf("empty DEFINE must be true")
	}
}

// PREV:buffer Fetchs the field of the previous line when not null.
func TestEvalDefine_Prev(t *testing.T) {
	buf := []map[string]any{{"v": 10.0}}
	labels := []string{"A"}
	// 20 > PREV(v,1)=10 → true
	if !EvalDefine("v > PREV(v, 1)", buf, labels, map[string]any{"v": 20.0}, "A", syms("A")) {
		t.Errorf("20 > PREV(10) want true")
	}
	// 5 > 10 → false
	if EvalDefine("v > PREV(v, 1)", buf, labels, map[string]any{"v": 5.0}, "A", syms("A")) {
		t.Errorf("5 > PREV(10) want false")
	}
}

// PREV crossing boundaries to return to nil → is relatively fake (no front-row drive).
func TestEvalDefine_PrevNullEmptyBuffer(t *testing.T) {
	if EvalDefine("v > PREV(v, 1)", nil, nil, map[string]any{"v": 20.0}, "A", syms("A")) {
		t.Errorf("PREV nil → comparison should be false")
	}
}

// Symbol-qualified fields A.v are equivalent to candidate row fields.
func TestEvalDefine_SymbolQualified(t *testing.T) {
	if !EvalDefine("A.v > 5", nil, nil, map[string]any{"v": 10.0}, "A", syms("A")) {
		t.Errorf("A.v=10 > 5 want true")
	}
}

// Compound condition: AND + strings are equal.
func TestEvalDefine_AndStringEq(t *testing.T) {
	cond := "v > 5 AND type == \"x\""
	if !EvalDefine(cond, nil, nil, map[string]any{"v": 10.0, "type": "x"}, "A", syms("A")) {
		t.Errorf("want true")
	}
	if EvalDefine(cond, nil, nil, map[string]any{"v": 10.0, "type": "y"}, "A", syms("A")) {
		t.Errorf("type mismatch want false")
	}
}

// --- EvalMeasure(candidate=nil, MEASURES path)---

func TestEvalMeasure_BareField(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}}
	labels := []string{"A", "A"}
	v, _ := EvalMeasure("v", rows, labels, 1, 1, syms("A"))
	if asFloat(v) != 20.0 {
		t.Errorf("bare field at cur=1 want 20, got %v", v)
	}
}

func TestEvalMeasure_PrevFirstLast(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}, {"v": 30.0}}
	labels := []string{"A", "A", "A"}
	if v, _ := EvalMeasure("PREV(v, 1)", rows, labels, 2, 1, syms("A")); asFloat(v) != 20.0 {
		t.Errorf("PREV at cur=2 want 20, got %v", v)
	}
	if v, _ := EvalMeasure("FIRST(v)", rows, labels, 2, 1, syms("A")); asFloat(v) != 10.0 {
		t.Errorf("FIRST want 10, got %v", v)
	}
	if v, _ := EvalMeasure("LAST(v)", rows, labels, 2, 1, syms("A")); asFloat(v) != 30.0 {
		t.Errorf("LAST want 30, got %v", v)
	}
}

func TestEvalMeasure_ClassifierAndMatchNumber(t *testing.T) {
	rows := []map[string]any{{"v": 1.0}, {"v": 2.0}}
	labels := []string{"A", "B"}
	if v, _ := EvalMeasure("CLASSIFIER()", rows, labels, 1, 1, syms("A", "B")); v != "B" {
		t.Errorf("CLASSIFIER at cur=1 want B, got %v", v)
	}
	if v, _ := EvalMeasure("MATCH_NUMBER()", rows, labels, 1, 7, syms("A", "B")); asFloat(v) != 7 {
		t.Errorf("MATCH_NUMBER want 7, got %v", v)
	}
}

// Aggregate: RUNNING (cur=2 to the current row, i.e., all 3 rows).
func TestEvalMeasure_Aggregates(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}, {"v": 30.0}}
	labels := []string{"A", "A", "A"}
	sym := syms("A")
	check := func(expr string, want float64) {
		t.Helper()
		v, _ := EvalMeasure(expr, rows, labels, 2, 1, sym)
		if asFloat(v) != want {
			t.Errorf("%s want %v, got %v", expr, want, v)
		}
	}
	check("COUNT(*)", 3)
	check("SUM(v)", 60)
	check("AVG(v)", 20)
	check("MIN(v)", 10)
	check("MAX(v)", 30)
}

// When aggregated RUNNING:cur=1, only the first two rows are counted.
func TestEvalMeasure_AggregateRunning(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}, {"v": 30.0}}
	labels := []string{"A", "A", "A"}
	v, _ := EvalMeasure("SUM(v)", rows, labels, 1, 1, syms("A")) // cur=1 → the first 2 rows
	if asFloat(v) != 30.0 {
		t.Errorf("RUNNING SUM at cur=1 want 30, got %v", v)
	}
}

// Arithmetic combination: MAX - MIN.
func TestEvalMeasure_Arithmetic(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 5.0}, {"v": 30.0}}
	labels := []string{"A", "A", "A"}
	v, _ := EvalMeasure("MAX(v) - MIN(v)", rows, labels, 2, 1, syms("A"))
	if asFloat(v) != 25.0 {
		t.Errorf("MAX-MIN want 25, got %v", v)
	}
}

// Symbol Limit Field: Take the last line of the symbol.
func TestEvalMeasure_SymbolField(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}}
	labels := []string{"A", "A"}
	v, _ := EvalMeasure("A.v", rows, labels, 1, 1, syms("A"))
	if asFloat(v) != 20.0 {
		t.Errorf("A.v (last A) want 20, got %v", v)
	}
}

// RUNNING semantics of FIRST/LAST: ALL ROWS PER MATCH advance with current row (consistent with COUNT).
// LAST(v) at cur=0/1/2 → 10/20/30 (rather than FINAL at the end row 30).
func TestEvalMeasure_FirstLastRunning(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}, {"v": 30.0}}
	labels := []string{"A", "A", "A"}
	sym := syms("A")
	if v, _ := EvalMeasure("LAST(v)", rows, labels, 0, 1, sym); asFloat(v) != 10.0 {
		t.Errorf("LAST at cur=0 want 10 (running), got %v", v)
	}
	if v, _ := EvalMeasure("LAST(v)", rows, labels, 1, 1, sym); asFloat(v) != 20.0 {
		t.Errorf("LAST at cur=1 want 20 (running), got %v", v)
	}
	if v, _ := EvalMeasure("LAST(v)", rows, labels, 2, 1, sym); asFloat(v) != 30.0 {
		t.Errorf("LAST at cur=2 want 30, got %v", v)
	}
	// FIRST always takes the first line (RUNNING and FINAL are the same).
	if v, _ := EvalMeasure("FIRST(v)", rows, labels, 1, 1, sym); asFloat(v) != 10.0 {
		t.Errorf("FIRST at cur=1 want 10, got %v", v)
	}
}

// The n<=0 of FIRST/LAST should not be crossed by panic; the clamp should be n=1.
func TestEvalMeasure_FirstLastZeroN(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}}
	labels := []string{"A", "A"}
	sym := syms("A")
	if v, _ := EvalMeasure("FIRST(v, 0)", rows, labels, 1, 1, sym); asFloat(v) != 10.0 {
		t.Errorf("FIRST(v,0) want 10 (clamped n=1), got %v", v)
	}
	if v, _ := EvalMeasure("LAST(v, 0)", rows, labels, 1, 1, sym); asFloat(v) != 20.0 {
		t.Errorf("LAST(v,0) want 20 (clamped n=1), got %v", v)
	}
	if v, _ := EvalMeasure("FIRST(v, -2)", rows, labels, 1, 1, sym); asFloat(v) != 10.0 {
		t.Errorf("FIRST(v,-2) want 10, got %v", v)
	}
}

// Symbol-limited aggregation SUM(A.v) sums only the row of the symbol label (not all rows).
func TestEvalMeasure_AggregateSymbolScoped(t *testing.T) {
	rows := []map[string]any{{"v": 1.0}, {"v": 2.0}, {"v": 3.0}}
	labels := []string{"A", "B", "A"}
	sym := syms("A", "B")
	// SUM(A.v) = 1 + 3 = 4 (only row A); SUM(v) = 1 + 2 + 3 = 6 (all rows).
	if v, _ := EvalMeasure("SUM(A.v)", rows, labels, 2, 1, sym); asFloat(v) != 4.0 {
		t.Errorf("SUM(A.v) want 4 (A-scoped), got %v", v)
	}
	if v, _ := EvalMeasure("SUM(v)", rows, labels, 2, 1, sym); asFloat(v) != 6.0 {
		t.Errorf("SUM(v) want 6 (all), got %v", v)
	}
}

// COUNT(expr) Counts non-NULL values (including string/uint), not just numeric values.
func TestEvalMeasure_CountNonNull(t *testing.T) {
	rows := []map[string]any{{"name": "a"}, {"name": "b"}, {"name": "c"}}
	labels := []string{"A", "A", "A"}
	sym := syms("A")
	if v, _ := EvalMeasure("COUNT(name)", rows, labels, 2, 1, sym); asFloat(v) != 3.0 {
		t.Errorf("COUNT(name) want 3 (non-NULL strings), got %v", v)
	}
	// uint columns can also be aggregated by SUM (cast.ToFloat64E supported).
	urows := []map[string]any{{"n": uint64(2)}, {"n": uint64(3)}}
	ulabels := []string{"A", "A"}
	if v, _ := EvalMeasure("SUM(n)", urows, ulabels, 1, 1, sym); asFloat(v) != 5.0 {
		t.Errorf("SUM(uint) want 5, got %v", v)
	}
}

// --- tokenize ---

func TestTokenize_Simple(t *testing.T) {
	toks, err := tokenize("v > PREV(v, 1)")
	if err != nil {
		t.Fatalf("tokenize error: %v", err)
	}
	if len(toks) == 0 {
		t.Fatalf("expected tokens")
	}
	// The first token is the identifier v
	if toks[0].kind != ekIdent || toks[0].val != "v" {
		t.Errorf("first token=%+v want ident v", toks[0])
	}
}

func TestTokenize_UnterminatedQuote(t *testing.T) {
	if _, err := tokenize("'unclosed"); err == nil {
		t.Errorf("expected error for unterminated quote")
	}
}

// --- SUBSET (filter symbols by member set) ---

// SUM(S.v) sums all components of the SUBSET; COUNT(S.v) counts the non-NULL row.
func TestAggregate_SubsetScoped(t *testing.T) {
	ctx := &matchCtx{
		rows:    []map[string]any{{"v": 1.0}, {"v": 2.0}, {"v": 3.0}},
		labels:  []string{"A", "B", "A"},
		cur:     2,
		subsets: map[string][]string{"S": {"A", "B"}},
	}
	if v := aggregate("SUM", []string{"S.v"}, ctx, false); asFloat(v) != 6.0 {
		t.Errorf("SUM(S.v) want 6 (A+B+A=1+2+3), got %v", v)
	}
	if v := aggregate("COUNT", []string{"S.v"}, ctx, false); asFloat(v) != 3.0 {
		t.Errorf("COUNT(S.v) want 3, got %v", v)
	}
	if v := aggregate("MAX", []string{"S.v"}, ctx, false); asFloat(v) != 3.0 {
		t.Errorf("MAX(S.v) want 3, got %v", v)
	}
	// SUM(A.v) still counts only line A (the normal symbol path is not affected).
	if v := aggregate("SUM", []string{"A.v"}, ctx, false); asFloat(v) != 4.0 {
		t.Errorf("SUM(A.v) want 4 (1+3), got %v", v)
	}
}

// resolveSymbolField: SUBSET takes the last row of components; DEFINE candidate labels take candidate rows when they belong to the category.
func TestResolveSymbolField_Subset(t *testing.T) {
	ctx := &matchCtx{
		rows:    []map[string]any{{"v": 1.0}, {"v": 2.0}, {"v": 3.0}},
		labels:  []string{"A", "B", "A"},
		subsets: map[string][]string{"S": {"A", "B"}},
	}
	// The first row in reverse order belonging to S: idx2 = A → v = 3.
	if v := resolveSymbolField(ctx, "S", "v"); asFloat(v) != 3.0 {
		t.Errorf("S.v want 3 (last S member), got %v", v)
	}
	// Candidate is B (S classification→ Choose candidate row.
	cand := &matchCtx{
		rows:      []map[string]any{{"v": 1.0}},
		labels:    []string{"A"},
		candidate: map[string]any{"v": 9.0},
		candLabel: "B",
		subsets:   map[string][]string{"S": {"A", "B"}},
	}
	if v := resolveSymbolField(cand, "S", "v"); asFloat(v) != 9.0 {
		t.Errorf("candidate S.v want 9, got %v", v)
	}
}

// --- FINAL semantics (FINAL matches the whole segment under ALL ROWS, RUNNING cuts to the current row)---

// FINAL SUM/LAST FETCH THE ROW SET MATCHING segments (not cur truncation); RUNNING: Capture the current line.
func TestEvalMeasure_AggregateFinal(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}, {"v": 30.0}}
	labels := []string{"A", "A", "A"}
	sym := syms("A")
	// FINAL SUM(v) takes all 3 rows = 60 when cur=1; RUNNING SUM(v) takes the first 2 rows = 30.
	if v, _ := EvalMeasure("FINAL SUM(v)", rows, labels, 1, 1, sym); asFloat(v) != 60.0 {
		t.Errorf("FINAL SUM at cur=1 want 60, got %v", v)
	}
	if v, _ := EvalMeasure("RUNNING SUM(v)", rows, labels, 1, 1, sym); asFloat(v) != 30.0 {
		t.Errorf("RUNNING SUM at cur=1 want 30, got %v", v)
	}
	// FINAL LAST(v) = last line 30 (unrelated to cur); RUNNING LAST(v) at cur=1 = 20.
	if v, _ := EvalMeasure("FINAL LAST(v)", rows, labels, 1, 1, sym); asFloat(v) != 30.0 {
		t.Errorf("FINAL LAST want 30, got %v", v)
	}
	if v, _ := EvalMeasure("RUNNING LAST(v)", rows, labels, 1, 1, sym); asFloat(v) != 20.0 {
		t.Errorf("RUNNING LAST at cur=1 want 20, got %v", v)
	}
}

// No prefix defaults to RUNNING (backward compatible); FINAL FIRST always starts with the first line (consistent with RUNNING).
func TestEvalMeasure_FinalDefault(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}, {"v": 30.0}}
	labels := []string{"A", "A", "A"}
	sym := syms("A")
	if v, _ := EvalMeasure("SUM(v)", rows, labels, 1, 1, sym); asFloat(v) != 30.0 {
		t.Errorf("default SUM at cur=1 want 30 (RUNNING), got %v", v)
	}
	if v, _ := EvalMeasure("FINAL FIRST(v)", rows, labels, 1, 1, sym); asFloat(v) != 10.0 {
		t.Errorf("FINAL FIRST want 10, got %v", v)
	}
}

// EvalMeasureWithSubsets correctly determines the SUBSET constraint aggregation; Old EvalMeasure (nil subsets) returns 0.
func TestEvalMeasure_SubsetViaWithSubsets(t *testing.T) {
	rows := []map[string]any{{"v": 1.0}, {"v": 2.0}, {"v": 3.0}}
	labels := []string{"A", "B", "A"}
	sub := map[string][]string{"S": {"A", "B"}}
	v, _ := EvalMeasureWithSubsets("SUM(S.v)", rows, labels, 2, 1, syms("A", "B", "S"), sub)
	if asFloat(v) != 6.0 {
		t.Errorf("WithSubsets SUM(S.v) want 6, got %v", v)
	}
	v0, _ := EvalMeasure("SUM(S.v)", rows, labels, 2, 1, syms("A", "B", "S"))
	if asFloat(v0) != 0 {
		t.Errorf("EvalMeasure(no subsets) SUM(S.v) want 0 (not supported), got %v", v0)
	}
}
