package rsql

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// 解析一条 MATCH_RECOGNIZE 查询，返回 config 与 spec（断言解析成功）。
func mustParseMR(t *testing.T, sql string) (*types.Config, *types.MatchRecognizeSpec) {
	t.Helper()
	cfg, _, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if cfg.MatchRecognize == nil {
		t.Fatalf("MatchRecognize not set")
	}
	if cfg.Mode != types.ExecCEP {
		t.Fatalf("Mode=%v want ExecCEP", cfg.Mode)
	}
	if cfg.NeedWindow {
		t.Fatalf("CEP query must not set NeedWindow")
	}
	return cfg, cfg.MatchRecognize
}

func TestMR_BasicStructure(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		PARTITION BY dev
		ORDER BY ts
		MEASURES MATCH_NUMBER() AS mn, A.v AS peak
		ONE ROW PER MATCH
		PATTERN (A{3})
		WITHIN '5m'
		DEFINE A AS v > 50
	)`
	_, mr := mustParseMR(t, sql)
	if len(mr.PartitionBy) != 1 || mr.PartitionBy[0] != "dev" {
		t.Errorf("PartitionBy=%v want [dev]", mr.PartitionBy)
	}
	if len(mr.OrderBy) != 1 || mr.OrderBy[0].Expression != "ts" {
		t.Errorf("OrderBy=%+v", mr.OrderBy)
	}
	if len(mr.Measures) != 2 {
		t.Fatalf("Measures=%+v", mr.Measures)
	}
	if mr.Measures[0].Expr != "MATCH_NUMBER ( )" || mr.Measures[0].Alias != "mn" {
		t.Errorf("measure0=%+v", mr.Measures[0])
	}
	if mr.RowsPerMatch != types.RowsPerMatchOne {
		t.Errorf("RowsPerMatch=%v want One", mr.RowsPerMatch)
	}
	if mr.Within != 5*time.Minute {
		t.Errorf("Within=%v want 5m", mr.Within)
	}
	if len(mr.Defines) != 1 || mr.Defines[0].Symbol != "A" {
		t.Errorf("Defines=%+v", mr.Defines)
	}
}

func TestMR_PatternQuantifiers(t *testing.T) {
	cases := []struct {
		pat   string
		kind  types.PatternKind
		min   int
		max   int
	}{
		{`A{3}`, types.PatternRepetition, 3, 3},
		{`A{2,}`, types.PatternRepetition, 2, -1},
		{`A{2,4}`, types.PatternRepetition, 2, 4},
		{`A?`, types.PatternRepetition, 0, 1},
		{`A*`, types.PatternRepetition, 0, -1},
		{`A+`, types.PatternRepetition, 1, -1},
	}
	for _, c := range cases {
		sql := `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts PATTERN (` + c.pat + `) DEFINE A AS v > 0)`
		_, mr := mustParseMR(t, sql)
		if mr.Pattern.Kind != c.kind {
			t.Errorf("%s: kind=%v want %v", c.pat, mr.Pattern.Kind, c.kind)
		}
		if mr.Pattern.Quant == nil {
			t.Fatalf("%s: nil quantifier", c.pat)
		}
		if mr.Pattern.Quant.Min != c.min || mr.Pattern.Quant.Max != c.max {
			t.Errorf("%s: quant=%+v want min=%d max=%d", c.pat, mr.Pattern.Quant, c.min, c.max)
		}
	}
}

func TestMR_PatternAlternationGroup(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts PATTERN ((A | B) C+) DEFINE A AS v>0)`
	_, mr := mustParseMR(t, sql)
	if mr.Pattern.Kind != types.PatternSequence {
		t.Fatalf("outer kind=%v want Sequence", mr.Pattern.Kind)
	}
	if len(mr.Pattern.Children) != 2 {
		t.Fatalf("children=%d want 2", len(mr.Pattern.Children))
	}
	if mr.Pattern.Children[0].Kind != types.PatternGroup {
		t.Errorf("child0 kind=%v want Group", mr.Pattern.Children[0].Kind)
	}
	if mr.Pattern.Children[0].Children[0].Kind != types.PatternAlternation {
		t.Errorf("inside group kind=%v want Alternation", mr.Pattern.Children[0].Children[0].Kind)
	}
}

func TestMR_AllRowsPerMatch(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts ALL ROWS PER MATCH PATTERN (A+) DEFINE A AS v>0)`
	_, mr := mustParseMR(t, sql)
	if mr.RowsPerMatch != types.RowsPerMatchAll {
		t.Errorf("RowsPerMatch=%v want All", mr.RowsPerMatch)
	}
}

func TestMR_AfterMatchSkip(t *testing.T) {
	cases := []struct {
		clause string
		skip   types.AfterMatchSkip
		sym    string
	}{
		{`AFTER MATCH SKIP PAST LAST ROW`, types.SkipPastLastRow, ""},
		{`AFTER MATCH SKIP TO NEXT ROW`, types.SkipToNextRow, ""},
		{`AFTER MATCH SKIP TO FIRST A`, types.SkipToFirst, "A"},
		{`AFTER MATCH SKIP TO LAST B`, types.SkipToLast, "B"},
	}
	for _, c := range cases {
		sql := `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts ` + c.clause + ` PATTERN (A) DEFINE A AS v>0)`
		_, mr := mustParseMR(t, sql)
		if mr.Skip != c.skip {
			t.Errorf("%s: skip=%v want %v", c.clause, mr.Skip, c.skip)
		}
		if mr.SkipSymbol != c.sym {
			t.Errorf("%s: skipSymbol=%q want %q", c.clause, mr.SkipSymbol, c.sym)
		}
	}
}

func TestMR_WithinUnits(t *testing.T) {
	cases := []struct {
		within string
		want   time.Duration
	}{
		{`WITHIN '500ms'`, 500 * time.Millisecond},
		{`WITHIN 5 SECONDS`, 5 * time.Second},
		{`WITHIN 2 MINUTES`, 2 * time.Minute},
		{`WITHIN 1 HOURS`, 1 * time.Hour},
	}
	for _, c := range cases {
		sql := `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts PATTERN (A) ` + c.within + ` DEFINE A AS v>0)`
		_, mr := mustParseMR(t, sql)
		if mr.Within != c.want {
			t.Errorf("%s: Within=%v want %v", c.within, mr.Within, c.want)
		}
	}
}

// 错误路径：缺 PATTERN、缺 ORDER BY、与 GROUP BY 冲突（在 ToStreamConfig 期 fail-fast）。
func TestMR_Errors(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"no pattern", `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts DEFINE A AS v>0)`},
		{"no order by", `SELECT * FROM stream MATCH_RECOGNIZE (PATTERN (A) DEFINE A AS v>0)`},
		{"with group by", `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts PATTERN (A) DEFINE A AS v>0) GROUP BY TumblingWindow('1s')`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := Parse(c.sql)
			if err == nil {
				t.Errorf("expected error for %q, got nil", c.name)
			}
		})
	}
}

// 排除模式 {- -} 在 parser 层识别为 PatternExclusion（设计：parser 识别全语法），
// 编译期由 cep.Validate 拒绝（见 cep 包测试）。
func TestMR_ExclusionParsed(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts PATTERN ({- A -}) DEFINE A AS v>0)`
	_, mr := mustParseMR(t, sql)
	if mr.Pattern.Kind != types.PatternExclusion {
		t.Errorf("kind=%v want PatternExclusion", mr.Pattern.Kind)
	}
}

// EmitSync 拒绝 CEP 查询的行为在 stream 层（见 e2e/cep_test 与 streamsql.EmitSync）。
