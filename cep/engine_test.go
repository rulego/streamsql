package cep

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// --- Test Assistance: Pattern Tree Construction + Engine Running ---

func lit(s string) *types.PatternNode {
	return &types.PatternNode{Kind: types.PatternLiteral, Symbol: s}
}
func seq(cs ...*types.PatternNode) *types.PatternNode {
	return &types.PatternNode{Kind: types.PatternSequence, Children: cs}
}
func altNode(cs ...*types.PatternNode) *types.PatternNode {
	return &types.PatternNode{Kind: types.PatternAlternation, Children: cs}
}
func rep(c *types.PatternNode, min, max int) *types.PatternNode {
	return &types.PatternNode{Kind: types.PatternRepetition, Children: []*types.PatternNode{c}, Quant: &types.Quantifier{Min: min, Max: max, Greedy: true}}
}

func def(symbol, cond string) types.MatchDefine { return types.MatchDefine{Symbol: symbol, Cond: cond} }
func measure(expr, alias string) types.Measure  { return types.Measure{Expr: expr, Alias: alias} }
func orderBy(field string) []types.OrderByField { return []types.OrderByField{{Expression: field}} }

// runEvents builds the engine and installs events sequentially, collecting all output rows (including Flush).
func runEvents(t *testing.T, spec *types.MatchRecognizeSpec, rows []map[string]any) []map[string]any {
	t.Helper()
	e, err := NewEngine(spec)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	var out []map[string]any
	for _, r := range rows {
		out = append(out, e.Process(r, "")...)
	}
	out = append(out, e.Flush()...)
	return out
}

func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	}
	return 0
}

// --- Scenario testing (end-to-end, deriving from scenarios, not backward from code)---

// Scenario 1: A{3} Continuous limit overreach confirmation (anti-shake).
func TestScenario1_ConsecutiveThreshold(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:  rep(lit("A"), 3, 3),
		Defines:  []types.MatchDefine{def("A", "v > 50")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn"), measure("A.v", "peak")},
	}
	rows := []map[string]any{
		{"ts": 1, "v": 10}, {"ts": 2, "v": 60}, {"ts": 3, "v": 70}, {"ts": 4, "v": 80}, {"ts": 5, "v": 5},
	}
	out := runEvents(t, spec, rows)
	if len(out) != 1 {
		t.Fatalf("want 1 match, got %d: %v", len(out), out)
	}
	if asFloat(out[0]["mn"]) != 1 {
		t.Errorf("mn=%v want 1", out[0]["mn"])
	}
	if asFloat(out[0]["peak"]) != 80 {
		t.Errorf("peak=%v want 80", out[0]["peak"])
	}
}

// Scenario 2: A and B overheat and then fall back (a warning sign).
func TestScenario2_RiseThenDrop(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:  seq(lit("A"), lit("B")),
		Defines:  []types.MatchDefine{def("A", "temp > 100"), def("B", "temp < 100")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("A.temp", "peak"), measure("B.temp", "drop")},
	}
	rows := []map[string]any{{"ts": 1, "temp": 50}, {"ts": 2, "temp": 120}, {"ts": 3, "temp": 90}}
	out := runEvents(t, spec, rows)
	if len(out) != 1 || asFloat(out[0]["peak"]) != 120 || asFloat(out[0]["drop"]) != 90 {
		t.Fatalf("want 1 match peak=120 drop=90, got %v", out)
	}
}

// Scenario 3: A B+ C monotonically rises and then falls (PREV + aggregate MEASURES).
func TestScenario3_TrendReversal(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: seq(lit("A"), rep(lit("B"), 1, -1), lit("C")),
		Defines: []types.MatchDefine{def("B", "v > PREV(v, 1)"), def("C", "v < PREV(v, 1)")},
		OrderBy: orderBy("ts"),
		Measures: []types.Measure{
			measure("MAX(v)", "peak"), measure("FIRST(v)", "start"), measure("LAST(v)", "end"),
		},
	}
	rows := []map[string]any{{"ts": 1, "v": 10}, {"ts": 2, "v": 20}, {"ts": 3, "v": 30}, {"ts": 4, "v": 25}}
	out := runEvents(t, spec, rows)
	if len(out) != 1 {
		t.Fatalf("want 1 match, got %d: %v", len(out), out)
	}
	if asFloat(out[0]["peak"]) != 30 || asFloat(out[0]["start"]) != 10 || asFloat(out[0]["end"]) != 25 {
		t.Errorf("peak/start/end=%v want 30/10/25", out[0])
	}
}

// Scenario 4: A{5,} Vibration Burst (5+, ending with an interrupt event).
func TestScenario4_VibrationBurst(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:  rep(lit("A"), 5, -1),
		Defines:  []types.MatchDefine{def("A", "type == \"vib\"")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("COUNT(*)", "n"), measure("MATCH_NUMBER()", "mn")},
	}
	rows := []map[string]any{
		{"ts": 1, "type": "vib"}, {"ts": 2, "type": "vib"}, {"ts": 3, "type": "vib"},
		{"ts": 4, "type": "vib"}, {"ts": 5, "type": "vib"}, {"ts": 6, "type": "vib"},
		{"ts": 7, "type": "normal"},
	}
	out := runEvents(t, spec, rows)
	if len(out) != 1 || asFloat(out[0]["n"]) != 6 {
		t.Fatalf("want 1 burst n=6, got %v", out)
	}
}

// Scenario 5: Start Process+ End cross-event type sequence (workflow).
func TestScenario5_CrossEventSequence(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: seq(lit("Start"), rep(lit("Process"), 1, -1), lit("End")),
		Defines: []types.MatchDefine{
			def("Start", "status == \"start\""), def("Process", "status == \"process\""), def("End", "status == \"end\""),
		},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn"), measure("COUNT(*)", "n")},
	}
	rows := []map[string]any{
		{"ts": 1, "status": "start"}, {"ts": 2, "status": "process"}, {"ts": 3, "status": "process"}, {"ts": 4, "status": "end"},
	}
	out := runEvents(t, spec, rows)
	if len(out) != 1 || asFloat(out[0]["n"]) != 4 {
		t.Fatalf("want 1 match n=4, got %v", out)
	}
}

// --- Measure Words / Alternation / PERMUTE ---

// A? B(A) optional: A, B → n=2; without A, direct B→n=1.
func TestQuantifier_Optional(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:  seq(rep(lit("A"), 0, 1), lit("B")),
		Defines:  []types.MatchDefine{def("B", "k == 2")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("COUNT(*)", "n")},
	}
	rows := []map[string]any{{"ts": 1, "k": 1}, {"ts": 2, "k": 2}, {"ts": 3, "k": 2}}
	out := runEvents(t, spec, rows)
	if len(out) != 2 {
		t.Fatalf("want 2 matches, got %d: %v", len(out), out)
	}
}

// A | B alternates + CLASSIFIER.
func TestAlternation(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:  altNode(lit("A"), lit("B")),
		Defines:  []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("CLASSIFIER()", "c")},
	}
	rows := []map[string]any{{"ts": 1, "k": 1}, {"ts": 2, "k": 2}, {"ts": 3, "k": 3}}
	out := runEvents(t, spec, rows)
	if len(out) != 2 || out[0]["c"] != "A" || out[1]["c"] != "B" {
		t.Errorf("classifiers=%v want [A B]", out)
	}
}

// PERMUTE(A, B): matches both sequences.
func TestPermute(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:  &types.PatternNode{Kind: types.PatternPermute, Children: []*types.PatternNode{lit("A"), lit("B")}},
		Defines:  []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("CLASSIFIER()", "last")},
	}
	rows := []map[string]any{{"ts": 1, "k": 1}, {"ts": 2, "k": 2}, {"ts": 3, "k": 2}, {"ts": 4, "k": 1}}
	out := runEvents(t, spec, rows)
	if len(out) != 2 {
		t.Fatalf("want 2 (A,B and B,A), got %d: %v", len(out), out)
	}
}

// --- Navigation ---

// NEXT crosses the boundary at the last line and returns nil.
func TestNextNavigation(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:  seq(lit("A"), lit("B")),
		Defines:  []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("A.k", "ak"), measure("NEXT(B.k, 1)", "next")},
	}
	rows := []map[string]any{{"ts": 1, "k": 1}, {"ts": 2, "k": 2}}
	out := runEvents(t, spec, rows)
	if len(out) != 1 || asFloat(out[0]["ak"]) != 1 || out[0]["next"] != nil {
		t.Errorf("got %v want ak=1 next=nil", out)
	}
}

// --- WITHIN (single match time upper bound + expired reset)---

func TestWithinExpiry(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: seq(lit("A"), lit("B")),
		Defines: []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2")},
		OrderBy: orderBy("ts"), Within: 10 * time.Millisecond,
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn")},
	}
	rows := []map[string]any{{"ts": 1, "k": 1}, {"ts": 1000000000, "k": 2}}
	if out := runEvents(t, spec, rows); len(out) != 0 {
		t.Fatalf("want 0 (expired), got %d: %v", len(out), out)
	}
}

func TestWithinOK(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: seq(lit("A"), lit("B")),
		Defines: []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2")},
		OrderBy: orderBy("ts"), Within: 1 * time.Hour,
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn")},
	}
	rows := []map[string]any{{"ts": 1, "k": 1}, {"ts": 2, "k": 2}}
	if out := runEvents(t, spec, rows); len(out) != 1 {
		t.Fatalf("want 1, got %d", len(out))
	}
}

// WITHIN expires and restored: the first pair of super windows is voided, while the second pair still matches within the window.
func TestWithinResetRecovery(t *testing.T) {
	const base = int64(1700000000000)
	spec := &types.MatchRecognizeSpec{
		Pattern: seq(lit("A"), lit("B")),
		Defines: []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2")},
		OrderBy: orderBy("ts"), Within: 1 * time.Minute,
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn")},
	}
	rows := []map[string]any{
		{"ts": base, "k": 1},          // A
		{"ts": base + 70000, "k": 2},  // 70s > 1m → expired
		{"ts": base + 100000, "k": 1}, // New A
		{"ts": base + 100030, "k": 2}, // 30ms < 1m → matching
	}
	out := runEvents(t, spec, rows)
	if len(out) != 1 || asFloat(out[0]["mn"]) != 1 {
		t.Fatalf("want 1 match (recovered), got %d: %v", len(out), out)
	}
}

// --- Zoning / Bounded ---

// Partition isolation: Different partitions match independently.
func TestPartitionIsolation(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:  seq(lit("A"), lit("B")),
		Defines:  []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn")},
	}
	e, _ := NewEngine(spec)
	var out []map[string]any
	out = append(out, e.Process(map[string]any{"ts": 1, "k": 1}, "p1")...)
	out = append(out, e.Process(map[string]any{"ts": 2, "k": 1}, "p2")...)
	out = append(out, e.Process(map[string]any{"ts": 3, "k": 2}, "p1")...)
	out = append(out, e.Process(map[string]any{"ts": 4, "k": 1}, "p2")...)
	out = append(out, e.Flush()...)
	if len(out) != 1 {
		t.Fatalf("want 1 (p1 only), got %d: %v", len(out), out)
	}
}

// maxRuns limit: Class A* state explosions are interrupted but do not crash.
func TestMaxRunsCap(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:  seq(rep(lit("A"), 0, -1), lit("B")),
		Defines:  []types.MatchDefine{def("A", "k == 1")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn")},
	}
	e, _ := NewEngine(spec)
	e.SetMaxRuns(50)
	e.SetMaxPartitions(10)
	for i := 0; i < 10000; i++ {
		e.Process(map[string]any{"ts": int64(i), "k": 1}, "")
	}
	e.Flush() // If you don't collapse, you pass
}

// --- Validate (fail-fast)---

func TestValidateExclusionRejected(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: &types.PatternNode{Kind: types.PatternExclusion, Children: []*types.PatternNode{lit("A")}},
		OrderBy: orderBy("ts"),
	}
	if err := Validate(spec); err == nil {
		t.Errorf("expected Validate to reject exclusion")
	}
}

func TestValidateMissing(t *testing.T) {
	if err := Validate(&types.MatchRecognizeSpec{OrderBy: orderBy("ts")}); err == nil {
		t.Errorf("expected error for missing Pattern")
	}
	if err := Validate(&types.MatchRecognizeSpec{Pattern: lit("A")}); err == nil {
		t.Errorf("expected error for missing ORDER BY")
	}
}

// Structurally malformed DEFINE/MEASURES expressions (parentheses not even) are rejected by Validate (fail-fast).
func TestValidateMalformedExpr(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: lit("A"),
		OrderBy: orderBy("ts"),
		Defines: []types.MatchDefine{{Symbol: "A", Cond: "(v > 5"}},
	}
	if err := Validate(spec); err == nil {
		t.Errorf("Validate should reject malformed DEFINE expr")
	}
	spec2 := &types.MatchRecognizeSpec{
		Pattern:  lit("A"),
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{{Expr: "MAX(v", Alias: "m"}},
	}
	if err := Validate(spec2); err == nil {
		t.Errorf("Validate should reject malformed MEASURES expr")
	}
}

// --- NULL Spread ---

// For single-symbol DEFINE with PREV: first line PREV=nil → compared to false → mismatch.
func TestNullInDefine(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:  lit("A"),
		Defines:  []types.MatchDefine{def("A", "v > PREV(v, 1)")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("MATCH_NUMBER()", "mn")},
	}
	rows := []map[string]any{{"ts": 1, "v": 10}, {"ts": 2, "v": 20}}
	if out := runEvents(t, spec, rows); len(out) != 0 {
		t.Fatalf("want 0 (PREV nil → false), got %d: %v", len(out), out)
	}
}

// --- SUBSET ---

// SUBSET in MEASURES reference: SUM(S.v) sums all components of S={A,B}; S.v takes the last row of the components.
func TestSubset_Measures(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: seq(lit("A"), rep(lit("B"), 1, -1)),
		Subsets: []types.MatchSubset{{Name: "S", Symbols: []string{"A", "B"}}},
		Defines: []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2")},
		OrderBy: orderBy("ts"),
		Measures: []types.Measure{
			measure("SUM(S.v)", "sv"), measure("SUM(A.v)", "av"), measure("S.v", "last"),
		},
	}
	rows := []map[string]any{
		{"ts": 1, "k": 1, "v": 1},   // A
		{"ts": 2, "k": 2, "v": 10},  // B
		{"ts": 3, "k": 2, "v": 100}, // B
		{"ts": 4, "k": 3, "v": 0},   // Non-A/B: Finishing B+
	}
	out := runEvents(t, spec, rows)
	if len(out) != 1 {
		t.Fatalf("want 1 match, got %d: %v", len(out), out)
	}
	if asFloat(out[0]["sv"]) != 111 { // S = {A, B} All: 1+10+100
		t.Errorf("SUM(S.v)=%v want 111", out[0]["sv"])
	}
	if asFloat(out[0]["av"]) != 1 { // Only A
		t.Errorf("SUM(A.v)=%v want 1", out[0]["av"])
	}
	if asFloat(out[0]["last"]) != 100 { // S component last row = last B
		t.Errorf("S.v=%v want 100", out[0]["last"])
	}
}

// SUBSET is an atom in a PATTERN: PATTERN(S C)(S={A,B})→ (A| B) C. match-state carries the real ingredients.
func TestSubset_InPattern(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:      seq(lit("S"), lit("C")),
		Subsets:      []types.MatchSubset{{Name: "S", Symbols: []string{"A", "B"}}},
		Defines:      []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2"), def("C", "k == 3")},
		OrderBy:      orderBy("ts"),
		RowsPerMatch: types.RowsPerMatchAll,
		Measures:     []types.Measure{measure("CLASSIFIER()", "c")},
	}
	rows := []map[string]any{
		{"ts": 1, "k": 1}, // A (Expand S for Matching)
		{"ts": 2, "k": 3}, // C
	}
	out := runEvents(t, spec, rows)
	if len(out) != 2 {
		t.Fatalf("want 2 rows (A,C), got %d: %v", len(out), out)
	}
	if out[0]["c"] != "A" {
		t.Errorf("row0 classifier=%v want A (S components)", out[0]["c"])
	}
	if out[1]["c"] != "C" {
		t.Errorf("row1 classifier=%v want C", out[1]["c"])
	}
}

// Nested SUBSET: S2=(S1, C), S1=(A, B). SUM(S2.v) covers all A/B/C components.
func TestSubset_Nested(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: seq(lit("A"), lit("B"), lit("C")),
		Subsets: []types.MatchSubset{
			{Name: "S1", Symbols: []string{"A", "B"}},
			{Name: "S2", Symbols: []string{"S1", "C"}},
		},
		Defines:  []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2"), def("C", "k == 3")},
		OrderBy:  orderBy("ts"),
		Measures: []types.Measure{measure("SUM(S2.v)", "s2"), measure("SUM(S1.v)", "s1")},
	}
	rows := []map[string]any{
		{"ts": 1, "k": 1, "v": 1},
		{"ts": 2, "k": 2, "v": 2},
		{"ts": 3, "k": 3, "v": 3},
	}
	out := runEvents(t, spec, rows)
	if len(out) != 1 || asFloat(out[0]["s2"]) != 6 || asFloat(out[0]["s1"]) != 3 {
		t.Fatalf("want SUM(S2.v)=6, SUM(S1.v)=3, got %v", out)
	}
}

// Validate refuses to reference SUBSETS of unknown symbols.
func TestSubset_ValidateUnknownMember(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: lit("A"),
		Subsets: []types.MatchSubset{{Name: "S", Symbols: []string{"A", "X"}}}, // X Unknown
		OrderBy: orderBy("ts"),
	}
	if err := Validate(spec); err == nil {
		t.Errorf("Validate should reject SUBSET with unknown member X")
	}
}

// Validate rejects the SUBSET loop definition.
func TestSubset_ValidateCycle(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: seq(lit("A"), lit("B")),
		Subsets: []types.MatchSubset{
			{Name: "S1", Symbols: []string{"A", "S2"}},
			{Name: "S2", Symbols: []string{"B", "S1"}}, // S1↔S2 ring
		},
		OrderBy: orderBy("ts"),
	}
	if err := Validate(spec); err == nil {
		t.Errorf("Validate should reject cyclic SUBSET definition")
	}
}

// Greedy A* (DEFINE overlap) chooses the longest [A,A,B]; Lazy A*? Choose the shortest [B]×3.
func TestQuantifier_GreedyVsReluctant(t *testing.T) {
	star := func(greedy bool) *types.PatternNode {
		return &types.PatternNode{Kind: types.PatternRepetition, Children: []*types.PatternNode{lit("A")}, Quant: &types.Quantifier{Min: 0, Max: -1, Greedy: greedy}}
	}
	mk := func(greedy bool) *types.MatchRecognizeSpec {
		return &types.MatchRecognizeSpec{
			Pattern:  seq(star(greedy), lit("B")),
			Defines:  []types.MatchDefine{def("A", "v > 0"), def("B", "v > 0")},
			OrderBy:  orderBy("ts"),
			Measures: []types.Measure{measure("COUNT(*)", "n")},
		}
	}
	rows := []map[string]any{{"ts": 1, "v": 1}, {"ts": 2, "v": 2}, {"ts": 3, "v": 3}}
	// Greed: Extend to the end of the flow, Flush chooses the longest → [A,A,B] 1 match n=3.
	gOut := runEvents(t, mk(true), rows)
	if len(gOut) != 1 || asFloat(gOut[0]["n"]) != 3 {
		t.Fatalf("greedy A* want 1 match n=3, got %v", gOut)
	}
	// Laziness: Immediately select the shortest → [B]×3 per position.
	lOut := runEvents(t, mk(false), rows)
	if len(lOut) != 3 {
		t.Fatalf("reluctant A*? want 3 matches, got %d: %v", len(lOut), lOut)
	}
	for i, r := range lOut {
		if asFloat(r["n"]) != 1 {
			t.Errorf("reluctant match %d n want 1, got %v", i, r["n"])
		}
	}
}

// SUBSET Members exceed maxSubsetMembers (8) Error at compile time (not silence truncation).
func TestSubset_ValidateTooManyMembers(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: lit("A"),
		Subsets: []types.MatchSubset{{Name: "S", Symbols: []string{"A", "B", "C", "D", "E", "F", "G", "H", "I"}}}, // 9 > 8
		OrderBy: orderBy("ts"),
	}
	if err := Validate(spec); err == nil {
		t.Errorf("Validate should reject SUBSET with >%d members", maxSubsetMembers)
	}
}
