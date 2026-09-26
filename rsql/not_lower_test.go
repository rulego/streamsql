package rsql

import (
	"strings"
	"testing"
)

// SQL NOT lower 修复的解析级断言:expr-lang 只认小写 not/!,大写 NOT 原样进条件串
// 会被当未定义标识符——NOT(...) 静默编译通过、运行期恒假(整条 WHERE 静默丢全部
// 行),裸 NOT 直接编译错。修复后 NOT lower 为 !(IS NOT 中的 NOT 保留,由 IS NULL
// 预处理整体改写)。

func TestParseWhereNotLowering(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string // 期望条件串包含/不包含的片段
	}{
		{"not with parens", "SELECT a FROM t WHERE NOT (x > 1 OR y > 2)", "! ( x > 1 || y > 2 )"},
		{"not bare field", "SELECT a FROM t WHERE NOT x > 1", "! x > 1"},
		{"is not null preserved", "SELECT a FROM t WHERE x IS NOT NULL", "x IS NOT NULL"},
		{"not and is-not-null mix", "SELECT a FROM t WHERE NOT (x IS NOT NULL) AND y IS NOT NULL", "! ( x IS NOT NULL ) && y IS NOT NULL"},
		{"lowercase not also lowered (lexer is case-insensitive)", "SELECT a FROM t WHERE not (x > 1)", "! ( x > 1 )"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := NewParser(c.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if !strings.Contains(stmt.Condition, c.want) {
				t.Errorf("condition = %q, want it to contain %q", stmt.Condition, c.want)
			}
		})
	}
}

func TestParseHavingNotLowering(t *testing.T) {
	p := NewParser("SELECT count(a) FROM t GROUP BY TumblingWindow('1s') HAVING NOT (count(a) > 10)")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !strings.Contains(stmt.Having, "! (") {
		t.Errorf("having = %q, want NOT lowered to !", stmt.Having)
	}
	if strings.Contains(stmt.Having, "NOT") {
		t.Errorf("having = %q still contains raw NOT", stmt.Having)
	}
}

func TestTriggerWhenNotLowering(t *testing.T) {
	p := NewParser("SELECT * FROM t GLOBAL WINDOW TRIGGER WHEN NOT (count(*) > 100)")
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !strings.Contains(stmt.Window.TriggerCondition, "!( count") && !strings.Contains(stmt.Window.TriggerCondition, "! ( count") {
		t.Errorf("trigger condition = %q, want NOT lowered", stmt.Window.TriggerCondition)
	}
}

func TestMatchRecognizeDefineNotLowering(t *testing.T) {
	_, _, err := Parse(`SELECT * FROM t MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES A.temperature AS t0
		ONE ROW PER MATCH
		PATTERN (A B)
		DEFINE A AS NOT (A.temperature > 50), B AS B.temperature < 30
	)`)
	if err != nil {
		t.Fatalf("Parse with NOT in DEFINE: %v", err)
	}
}
