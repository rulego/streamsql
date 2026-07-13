package cep

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// --- 测试辅助：模式树构造 + 引擎运行 ---

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

func def(symbol, cond string) types.MatchDefine  { return types.MatchDefine{Symbol: symbol, Cond: cond} }
func measure(expr, alias string) types.Measure   { return types.Measure{Expr: expr, Alias: alias} }
func orderBy(field string) []types.OrderByField  { return []types.OrderByField{{Expression: field}} }

// runEvents 建引擎并按序投入事件，收集全部输出行（含 Flush）。
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

// --- 场景测试（端到端，从场景推导，不从代码反推）---

// 场景1：A{3} 连续越限确认（防抖）。
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

// 场景2：A B 过热后回落（故障前兆）。
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

// 场景3：A B+ C 单调上升后转降（PREV + 聚合 MEASURES）。
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

// 场景4：A{5,} 振动突发（5+，以中断事件收尾）。
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

// 场景5：Start Process+ End 跨事件类型序列（工作流）。
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

// --- 量词 / 交替 / PERMUTE ---

// A? B（A 可选）：A,B → n=2；无 A 直接 B → n=1。
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

// A | B 交替 + CLASSIFIER。
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

// PERMUTE(A, B)：两种顺序都匹配。
func TestPermute(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: &types.PatternNode{Kind: types.PatternPermute, Children: []*types.PatternNode{lit("A"), lit("B")}},
		Defines: []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2")},
		OrderBy: orderBy("ts"),
		Measures: []types.Measure{measure("CLASSIFIER()", "last")},
	}
	rows := []map[string]any{{"ts": 1, "k": 1}, {"ts": 2, "k": 2}, {"ts": 3, "k": 2}, {"ts": 4, "k": 1}}
	out := runEvents(t, spec, rows)
	if len(out) != 2 {
		t.Fatalf("want 2 (A,B and B,A), got %d: %v", len(out), out)
	}
}

// --- 导航 ---

// NEXT 在末行位置越界返回 nil。
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

// --- WITHIN（单次匹配时间上界 + 过期重置）---

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

// WITHIN 过期后恢复：第一对超窗作废，第二对在窗内仍匹配。
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
		{"ts": base + 70000, "k": 2},  // 70s > 1m → 过期
		{"ts": base + 100000, "k": 1}, // 新 A
		{"ts": base + 100030, "k": 2}, // 30ms < 1m → 匹配
	}
	out := runEvents(t, spec, rows)
	if len(out) != 1 || asFloat(out[0]["mn"]) != 1 {
		t.Fatalf("want 1 match (recovered), got %d: %v", len(out), out)
	}
}

// --- 分区 / 有界 ---

// 分区隔离：不同分区各自独立匹配。
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

// maxRuns 上限：A* 类状态爆炸被截断，不崩溃。
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
	e.Flush() // 不崩溃即通过
}

// --- Validate（构造期 fail-fast）---

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

// 结构畸形的 DEFINE/MEASURES 表达式（括号不配平）被 Validate 拒绝（fail-fast）。
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

// --- NULL 传播 ---

// 单符号 DEFINE 用 PREV：首行 PREV=nil → 比较为假 → 不匹配。
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

// SUBSET 在 MEASURES 引用：SUM(S.v) 对 S={A,B} 全部成分行求和；S.v 取成分末行。
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
		{"ts": 1, "k": 1, "v": 1},  // A
		{"ts": 2, "k": 2, "v": 10}, // B
		{"ts": 3, "k": 2, "v": 100}, // B
		{"ts": 4, "k": 3, "v": 0},  // 非 A/B：收尾 B+
	}
	out := runEvents(t, spec, rows)
	if len(out) != 1 {
		t.Fatalf("want 1 match, got %d: %v", len(out), out)
	}
	if asFloat(out[0]["sv"]) != 111 { // S={A,B} 全部：1+10+100
		t.Errorf("SUM(S.v)=%v want 111", out[0]["sv"])
	}
	if asFloat(out[0]["av"]) != 1 { // 只 A
		t.Errorf("SUM(A.v)=%v want 1", out[0]["av"])
	}
	if asFloat(out[0]["last"]) != 100 { // S 成分末行 = 最后 B
		t.Errorf("S.v=%v want 100", out[0]["last"])
	}
}

// SUBSET 在 PATTERN 里作原子：PATTERN(S C)（S={A,B}）→ (A|B) C，match-state 携带真实成分。
func TestSubset_InPattern(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern:     seq(lit("S"), lit("C")),
		Subsets:     []types.MatchSubset{{Name: "S", Symbols: []string{"A", "B"}}},
		Defines:     []types.MatchDefine{def("A", "k == 1"), def("B", "k == 2"), def("C", "k == 3")},
		OrderBy:     orderBy("ts"),
		RowsPerMatch: types.RowsPerMatchAll,
		Measures:    []types.Measure{measure("CLASSIFIER()", "c")},
	}
	rows := []map[string]any{
		{"ts": 1, "k": 1}, // A（经 S 展开匹配）
		{"ts": 2, "k": 3}, // C
	}
	out := runEvents(t, spec, rows)
	if len(out) != 2 {
		t.Fatalf("want 2 rows (A,C), got %d: %v", len(out), out)
	}
	if out[0]["c"] != "A" {
		t.Errorf("row0 classifier=%v want A (S 成分)", out[0]["c"])
	}
	if out[1]["c"] != "C" {
		t.Errorf("row1 classifier=%v want C", out[1]["c"])
	}
}

// 嵌套 SUBSET：S2=(S1, C)、S1=(A, B)。SUM(S2.v) 覆盖 A/B/C 全部。
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

// Validate 拒绝引用未知符号的 SUBSET。
func TestSubset_ValidateUnknownMember(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: lit("A"),
		Subsets: []types.MatchSubset{{Name: "S", Symbols: []string{"A", "X"}}}, // X 未知
		OrderBy: orderBy("ts"),
	}
	if err := Validate(spec); err == nil {
		t.Errorf("Validate should reject SUBSET with unknown member X")
	}
}

// Validate 拒绝循环 SUBSET 定义。
func TestSubset_ValidateCycle(t *testing.T) {
	spec := &types.MatchRecognizeSpec{
		Pattern: seq(lit("A"), lit("B")),
		Subsets: []types.MatchSubset{
			{Name: "S1", Symbols: []string{"A", "S2"}},
			{Name: "S2", Symbols: []string{"B", "S1"}}, // S1↔S2 环
		},
		OrderBy: orderBy("ts"),
	}
	if err := Validate(spec); err == nil {
		t.Errorf("Validate should reject cyclic SUBSET definition")
	}
}

// 贪婪 A*（DEFINE 重叠）选最长 [A,A,B]；懒惰 A*? 选最短 [B]×3。
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
	// 贪婪：延伸到流末 Flush 选最长 → [A,A,B] 1 个匹配 n=3。
	gOut := runEvents(t, mk(true), rows)
	if len(gOut) != 1 || asFloat(gOut[0]["n"]) != 3 {
		t.Fatalf("greedy A* want 1 match n=3, got %v", gOut)
	}
	// 懒惰：每位置立即选最短 → [B]×3。
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
