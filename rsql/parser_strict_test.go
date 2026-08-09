package rsql

import (
	"fmt"
	"strings"
	"testing"
)

// 子句语法错必须如实上报，不能被错误恢复吞掉。
// 回归：畸形 MATCH_RECOGNIZE 曾静默丢弃整个 MR 子句、模式文本被当成 ORDER BY 字段名，
// 查询退化成全量透传（CEP 变转发），Parse 却返回 nil。
func TestClauseSyntaxErrorReported(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "MR右括号缺失",
			sql:  `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts MEASURES A.v AS av PATTERN (A B DEFINE A AS v > 1, B AS v > 2)`,
		},
		{
			name: "MR整个括号缺失",
			sql:  `SELECT * FROM stream MATCH_RECOGNIZE ORDER BY ts PATTERN (A) DEFINE A AS v > 1`,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			p := NewParser(c.sql)
			stmt, err := p.Parse()
			if err == nil {
				t.Fatalf("畸形 SQL 应报错，实际 err=nil；stmt.MatchRecognize=%v OrderBy=%v",
					stmt != nil && stmt.MatchRecognize != nil, stmt.OrderBy)
			}
			if !p.HasErrors() {
				t.Errorf("HasErrors 应为 true（错误须登记进 errorRecovery）")
			}
			t.Logf("如实上报: %v", err)
		})
	}
}

// 合法长子句不得被截断。回归：固定 100 次迭代上限使 WHERE 超 24 个 AND 条件后
// 剩余条件被静默丢弃，过滤器失效、错误放行数据。
func TestLongClauseNotTruncated(t *testing.T) {
	t.Run("WHERE大量AND条件", func(t *testing.T) {
		for _, n := range []int{24, 25, 40, 120} {
			var conds []string
			for i := 0; i < n; i++ {
				conds = append(conds, fmt.Sprintf("f%d > 0", i))
			}
			conds = append(conds, "gate > 100") // 末位哨兵，被截断则消失
			sql := "SELECT deviceId FROM stream WHERE " + strings.Join(conds, " AND ")

			stmt, err := NewParser(sql).Parse()
			if err != nil {
				t.Fatalf("n=%d 合法长 WHERE 不应报错: %v", n, err)
			}
			if !strings.Contains(stmt.Condition, "gate") {
				t.Errorf("n=%d WHERE 被截断：末位条件 gate>100 丢失，实际=%q", n, stmt.Condition)
			}
			// 每个条件都应保留
			for i := 0; i < n; i++ {
				if !strings.Contains(stmt.Condition, fmt.Sprintf("f%d", i)) {
					t.Errorf("n=%d 条件 f%d 丢失", n, i)
					break
				}
			}
		}
	})

	t.Run("GROUP BY大量字段", func(t *testing.T) {
		for _, n := range []int{20, 30, 60} {
			var fields []string
			for i := 0; i < n; i++ {
				fields = append(fields, fmt.Sprintf("g%d", i))
			}
			sql := fmt.Sprintf("SELECT COUNT(*) AS c FROM stream GROUP BY %s, TumblingWindow('5s')",
				strings.Join(fields, ","))
			stmt, err := NewParser(sql).Parse()
			if err != nil {
				t.Fatalf("n=%d 合法长 GROUP BY 不应报错: %v", n, err)
			}
			// GroupBy 含 n 个字段 + 窗口项
			var found int
			for _, g := range stmt.GroupBy {
				if strings.HasPrefix(g, "g") {
					found++
				}
			}
			if found != n {
				t.Errorf("n=%d GROUP BY 字段被截断：期望 %d 个，实际 %d 个（%v）", n, n, found, stmt.GroupBy)
			}
		}
	})

	t.Run("HAVING大量条件", func(t *testing.T) {
		var conds []string
		for i := 0; i < 40; i++ {
			conds = append(conds, fmt.Sprintf("c%d > 0", i))
		}
		conds = append(conds, "gate > 100")
		sql := "SELECT COUNT(*) AS c FROM stream GROUP BY TumblingWindow('5s') HAVING " +
			strings.Join(conds, " AND ")
		stmt, err := NewParser(sql).Parse()
		if err != nil {
			t.Fatalf("合法长 HAVING 不应报错: %v", err)
		}
		if !strings.Contains(stmt.Having, "gate") {
			t.Errorf("HAVING 被截断：末位条件丢失，实际=%q", stmt.Having)
		}
	})

	t.Run("WITH大量属性", func(t *testing.T) {
		sql := `SELECT COUNT(*) AS c FROM stream GROUP BY TumblingWindow('5s') WITH (TIMESTAMP='ts', TIMEUNIT='ms')`
		if _, err := NewParser(sql).Parse(); err != nil {
			t.Fatalf("WITH 子句不应报错: %v", err)
		}
	})
}

// 死循环防护仍须生效：构造让 lexer 不推进的输入，应报错而非挂死。
func TestLoopGuardStillCatchesStall(t *testing.T) {
	g := newLoopGuard(NewLexer("x"))
	// 位置恒为 0，不推进
	stalls := 0
	for i := 0; i < 100; i++ {
		if !g.advanced() {
			stalls = i
			break
		}
	}
	if stalls == 0 {
		t.Errorf("loopGuard 未在位置不推进时判定死循环")
	}
	t.Logf("第 %d 轮判定 stall", stalls)
}
