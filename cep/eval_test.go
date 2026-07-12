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

// 空条件（未定义符号）恒为真（SQL 标准）。
func TestEvalDefine_EmptyIsTrue(t *testing.T) {
	if !EvalDefine("", nil, nil, map[string]any{"v": 1}, "A", syms("A")) {
		t.Errorf("empty DEFINE must be true")
	}
}

// PREV：buffer 非空时取上一行的字段。
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

// PREV 越界返回 nil → 比较为假（首行无前驱）。
func TestEvalDefine_PrevNullEmptyBuffer(t *testing.T) {
	if EvalDefine("v > PREV(v, 1)", nil, nil, map[string]any{"v": 20.0}, "A", syms("A")) {
		t.Errorf("PREV nil → comparison should be false")
	}
}

// 符号限定字段 A.v 等同候选行字段。
func TestEvalDefine_SymbolQualified(t *testing.T) {
	if !EvalDefine("A.v > 5", nil, nil, map[string]any{"v": 10.0}, "A", syms("A")) {
		t.Errorf("A.v=10 > 5 want true")
	}
}

// 复合条件 AND + 字符串相等。
func TestEvalDefine_AndStringEq(t *testing.T) {
	cond := "v > 5 AND type == \"x\""
	if !EvalDefine(cond, nil, nil, map[string]any{"v": 10.0, "type": "x"}, "A", syms("A")) {
		t.Errorf("want true")
	}
	if EvalDefine(cond, nil, nil, map[string]any{"v": 10.0, "type": "y"}, "A", syms("A")) {
		t.Errorf("type mismatch want false")
	}
}

// --- EvalMeasure（candidate=nil，MEASURES 路径）---

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

// 聚合：RUNNING（到当前行 cur=2 即全部 3 行）。
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

// 聚合 RUNNING：cur=1 时只算前两行。
func TestEvalMeasure_AggregateRunning(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}, {"v": 30.0}}
	labels := []string{"A", "A", "A"}
	v, _ := EvalMeasure("SUM(v)", rows, labels, 1, 1, syms("A")) // cur=1 → 前 2 行
	if asFloat(v) != 30.0 {
		t.Errorf("RUNNING SUM at cur=1 want 30, got %v", v)
	}
}

// 算术组合：MAX - MIN。
func TestEvalMeasure_Arithmetic(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 5.0}, {"v": 30.0}}
	labels := []string{"A", "A", "A"}
	v, _ := EvalMeasure("MAX(v) - MIN(v)", rows, labels, 2, 1, syms("A"))
	if asFloat(v) != 25.0 {
		t.Errorf("MAX-MIN want 25, got %v", v)
	}
}

// 符号限定字段：取该符号最末出现行。
func TestEvalMeasure_SymbolField(t *testing.T) {
	rows := []map[string]any{{"v": 10.0}, {"v": 20.0}}
	labels := []string{"A", "A"}
	v, _ := EvalMeasure("A.v", rows, labels, 1, 1, syms("A"))
	if asFloat(v) != 20.0 {
		t.Errorf("A.v (last A) want 20, got %v", v)
	}
}

// FIRST/LAST 的 RUNNING 语义：ALL ROWS PER MATCH 下随当前行推进（与 COUNT 一致）。
// LAST(v) at cur=0/1/2 → 10/20/30（而非恒为末行 30 的 FINAL）。
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
	// FIRST 恒为首行（RUNNING 与 FINAL 一致）。
	if v, _ := EvalMeasure("FIRST(v)", rows, labels, 1, 1, sym); asFloat(v) != 10.0 {
		t.Errorf("FIRST at cur=1 want 10, got %v", v)
	}
}

// FIRST/LAST 的 n<=0 不应越界 panic，钳为 n=1。
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

// 符号限定聚合 SUM(A.v) 仅对该符号标签行求和（非全部行）。
func TestEvalMeasure_AggregateSymbolScoped(t *testing.T) {
	rows := []map[string]any{{"v": 1.0}, {"v": 2.0}, {"v": 3.0}}
	labels := []string{"A", "B", "A"}
	sym := syms("A", "B")
	// SUM(A.v)=1+3=4（只 A 行）；SUM(v)=1+2+3=6（全部行）。
	if v, _ := EvalMeasure("SUM(A.v)", rows, labels, 2, 1, sym); asFloat(v) != 4.0 {
		t.Errorf("SUM(A.v) want 4 (A-scoped), got %v", v)
	}
	if v, _ := EvalMeasure("SUM(v)", rows, labels, 2, 1, sym); asFloat(v) != 6.0 {
		t.Errorf("SUM(v) want 6 (all), got %v", v)
	}
}

// COUNT(expr) 计非 NULL 值（含字符串/uint），非仅数值。
func TestEvalMeasure_CountNonNull(t *testing.T) {
	rows := []map[string]any{{"name": "a"}, {"name": "b"}, {"name": "c"}}
	labels := []string{"A", "A", "A"}
	sym := syms("A")
	if v, _ := EvalMeasure("COUNT(name)", rows, labels, 2, 1, sym); asFloat(v) != 3.0 {
		t.Errorf("COUNT(name) want 3 (non-NULL strings), got %v", v)
	}
	// uint 列也能被 SUM 聚合（cast.ToFloat64E 支持）。
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
	// 首个 token 是标识符 v
	if toks[0].kind != ekIdent || toks[0].val != "v" {
		t.Errorf("first token=%+v want ident v", toks[0])
	}
}

func TestTokenize_UnterminatedQuote(t *testing.T) {
	if _, err := tokenize("'unclosed"); err == nil {
		t.Errorf("expected error for unterminated quote")
	}
}
