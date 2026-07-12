package e2e

import (
	"testing"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
)

// SQL 解析期校验的集成测试（rsql/ast.go ToStreamConfig）。
// 汇总各类"应在解析期被拒"与"应被放行"的 SQL，锁定校验边界、防回归。
//
// 校验分几类：
//  1. 分析函数不得嵌进标量函数（UPPER/ABS/ROUND...）；算术表达式里带 OVER 的分析函数合法
//     （如 ts - lag(ts) OVER(...)），不在此拦；字符串字面量（单/双引号，含转义引号）里的
//     "lag(" 不得误判为函数调用。
//  2. 窗口查询里分析函数的参数必须是聚合函数或 GROUP BY 字段，不得引用裸原始列（D1）。
//  3. 嵌套：分析套分析、聚合套分析非法（D9）；分析套聚合（内联写法）合法。
//  4. 分析函数 alias 不得与其它输出列同名（静默覆盖=半成品，D3）。
//  5. GROUP BY 窗口之上不得叠 OVER(...)（阈值/持续检测改用 HAVING）。
//  6. WHERE 中的分析函数（含 OVER）由直连路径状态机求值，不受 SELECT 标量守卫约束。

// assertRejectExec 期望 Execute 在解析期报错，且错误信息包含 sub。
func assertRejectExec(t *testing.T, sql, sub string) {
	t.Helper()
	ssql := streamsql.New()
	defer ssql.Stop()
	err := ssql.Execute(sql)
	if !assert.Error(t, err, "应被解析期拒绝: %s", sql) {
		return
	}
	if sub != "" {
		assert.Contains(t, err.Error(), sub, "错误信息不符: %s", sql)
	}
}

// assertAcceptExec 期望 Execute 解析通过（不报错）。
func assertAcceptExec(t *testing.T, sql string) {
	t.Helper()
	assert.NoError(t, errExec(sql), "不应被解析期拒绝: %s", sql)
}

func errExec(sql string) error {
	ssql := streamsql.New()
	defer ssql.Stop()
	return ssql.Execute(sql)
}

// --- 1. 分析函数嵌进标量函数：放行（标量套分析走 wrapper 回代，支持 coalesce/CASE/UPPER 等）---

func TestSQLCheck_AnalyticInScalarFunction_Accept(t *testing.T) {
	for _, sql := range []string{
		`SELECT UPPER(changed_col(true, temperature)) AS c FROM stream`,                    // 标量套分析（小写）
		`SELECT UPPER(CHANGED_COL(true, temperature)) AS c FROM stream`,                    // 标量套分析（大写）
		`SELECT ROUND(lag(temperature), 2) AS c FROM stream`,                               // 标量套 lag
		`SELECT ABS(acc_sum(v)) AS a FROM stream`,                                          // 标量套 acc
		`SELECT ROUND(UPPER(changed_col(true, temperature)), 2) AS c FROM stream`,          // 深层嵌套标量套分析
		`SELECT CONCAT('prefix', changed_col(true, temperature)) AS s FROM stream`,         // 多参标量套分析
		`SELECT LOWER(latest(temperature)) AS s FROM stream`,                               // 标量套 latest
		`SELECT coalesce(lag(temp), -1) AS s FROM stream`,                                  // 标量套 lag + 默认值
		`SELECT CASE WHEN lag(temp) > 20 THEN 'up' ELSE 'down' END AS s FROM stream`,       // CASE 套分析
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// --- 1b. 分析函数合法位置 + 字符串字面量不误伤：放行 ---

func TestSQLCheck_AnalyticValidPlacement_Accept(t *testing.T) {
	for _, sql := range []string{
		`SELECT UPPER(name) AS n FROM stream`,                                    // 纯标量
		`SELECT temperature * 1.8 + 32 AS f FROM stream`,                         // 纯算术
		`SELECT changed_col(true, temperature) AS c FROM stream`,                 // 顶层分析
		`SELECT lag(temperature) OVER (PARTITION BY deviceId) AS p FROM stream`,  // 分析 + OVER
		`SELECT acc_sum(v) AS s FROM stream`,                                     // acc 顶层
		// acc 全族顶层合法。
		`SELECT acc_max(v) AS m FROM stream`,
		`SELECT acc_min(v) AS m FROM stream`,
		`SELECT acc_count(v) AS c FROM stream`,
		`SELECT acc_avg(v) AS a FROM stream`,
		// had_changed 多列变参顶层合法。
		`SELECT had_changed(true, a, b) AS h FROM stream`,
		// OVER 子句多种合法写法。
		`SELECT lag(temp, 1) OVER () AS p FROM stream`,                             // 空 OVER
		`SELECT lag(temp, 1, 0) OVER (PARTITION BY deviceId) AS p FROM stream`,     // lag 带默认值
		`SELECT acc_sum(v) OVER (PARTITION BY deviceId) AS s FROM stream`,          // acc + OVER
		`SELECT latest(temp) OVER (PARTITION BY deviceId) AS l FROM stream`,        // latest + OVER
		`SELECT lag(temp) OVER (PARTITION BY deviceId WHEN temp > 0) AS p FROM stream`, // OVER(WHEN) 输入门控
		// 算术表达式里带 OVER 的分析函数：合法（标量套分析检查仅拦"标量函数"顶层，不拦算术）。
		`SELECT ts - lag(ts) OVER (PARTITION BY k) AS d FROM stream`,
		`SELECT 100 - lag(ts) OVER (PARTITION BY k) AS d FROM stream`, // 分析函数在算术右侧
		// 多个分析字段、分析字段与普通列/标量共存：合法。
		`SELECT lag(a) OVER (PARTITION BY k) AS p, changed_col(true, b) AS c FROM stream`,
		`SELECT lag(a) OVER (PARTITION BY k) AS p, name AS n FROM stream`,
		`SELECT lag(a) OVER (PARTITION BY k) AS p, UPPER(name) AS u FROM stream`,
		// 字符串字面量里形如 "分析函数名(" 的文本不得误判为函数调用（曾误伤）。
		`SELECT CONCAT('lag is great', name) AS s FROM stream`,  // 字面量含分析名但无 "("
		`SELECT CONCAT('see lag(', name) AS s FROM stream`,      // 单引号字面量含 "lag("
		`SELECT UPPER('changed_col(x)') AS s FROM stream`,       // 单引号字面量含 "changed_col("
		`SELECT UPPER('latest(y)') AS s FROM stream`,            // 单引号字面量含 "latest("
		`SELECT UPPER('acc_sum(z)') AS s FROM stream`,           // 单引号字面量含 "acc_sum("
		`SELECT UPPER('had_changed(w)') AS s FROM stream`,       // 单引号字面量含 "had_changed("
		// 转义单引号（''）字面量里的分析名不得误判：字面量边界靠转义引号，naive 剥离会漏。
		`SELECT UPPER('it''s lag(x)') AS s FROM stream`,           // 转义引号后字面量内含 "lag("
		`SELECT UPPER('a''b''changed_col(x)') AS s FROM stream`,   // 多个转义引号
		// 双引号字面量（changed_cols 前缀用双引号）里的分析名同样不得误判。
		`SELECT UPPER("lag(x)") AS s FROM stream`,                 // 双引号字面量含 "lag("
		`SELECT UPPER("changed_col(x)") AS s FROM stream`,         // 双引号字面量含 "changed_col("
		`SELECT CONCAT("t_lag_", name) AS s FROM stream`,          // 双引号前缀，无分析名
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// --- 2. 窗口查询里的分析函数 ---

func TestSQLCheck_WindowAnalytic_Accept(t *testing.T) {
	// 参数为聚合函数或 GROUP BY 字段：合法。
	for _, sql := range []string{
		`SELECT changed_col(true, avg(temperature)) AS c FROM stream GROUP BY CountingWindow(2)`,
		`SELECT changed_cols("t", true, avg(temperature), max(temperature)) FROM stream GROUP BY CountingWindow(2)`,
		`SELECT deviceId, changed_col(true, avg(temp)) AS chg FROM stream GROUP BY deviceId, CountingWindow(2)`,
		`SELECT acc_sum(avg(temperature)) AS total FROM stream GROUP BY CountingWindow(2)`,
		// 分析套不同聚合：合法（分析函数对窗口聚合输出求值）。
		`SELECT changed_col(true, max(temp)) AS c FROM stream GROUP BY CountingWindow(2)`,
		`SELECT acc_sum(max(temp)) AS s FROM stream GROUP BY CountingWindow(2)`,
		`SELECT had_changed(true, avg(temp)) AS h FROM stream GROUP BY CountingWindow(2)`,
		// 限定列恰好是 GROUP BY 键：取列名后缀判等，合法（防误伤限定列）。
		`SELECT changed_col(true, stream.deviceId) AS c FROM stream GROUP BY deviceId, CountingWindow(2)`,
		// 复杂表达式参数（含运算符）不在裸列拦截范围：合法。
		`SELECT changed_col(true, avg(temp) + 1) AS c FROM stream GROUP BY CountingWindow(2)`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

func TestSQLCheck_WindowAnalytic_RawColumn_Reject(t *testing.T) {
	// D1：窗口查询里分析函数引用裸原始列（未聚合、非分组键）→ 解析期拒绝。
	for _, sql := range []string{
		`SELECT lag(temperature) AS p FROM stream GROUP BY CountingWindow(2)`,
		`SELECT deviceId, lag(temperature) AS p FROM stream GROUP BY deviceId, CountingWindow(2)`,
		// 限定列但后缀名非 GROUP BY 键：同样视为裸原始列（防误伤：限定不等于豁免）。
		`SELECT changed_col(true, stream.deviceId) AS c FROM stream GROUP BY CountingWindow(2)`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertRejectExec(t, sql, "raw column")
		})
	}
}

// --- 3. 嵌套函数（D9）---

func TestSQLCheck_NestedAnalytic_Reject(t *testing.T) {
	// 分析套分析、聚合套分析 → 非法。
	for _, sql := range []string{
		`SELECT lag(lag(a)) AS p FROM stream`,                                // 分析套分析
		`SELECT had_changed(true, changed_col(true, a)) AS h FROM stream`,    // 分析套分析
		`SELECT sum(lag(a)) AS s FROM stream GROUP BY CountingWindow(2)`,     // 聚合套分析（窗口）
		`SELECT max(acc_sum(v)) AS m FROM stream GROUP BY CountingWindow(2)`, // 聚合套 acc
	} {
		t.Run(sql, func(t *testing.T) {
			assertRejectExec(t, sql, "") // 各路径错误信息不同，只断言报错
		})
	}
}

func TestSQLCheck_AnalyticWrappingAggregate_Accept(t *testing.T) {
	// 分析套聚合（内联写法）→ 合法。
	for _, sql := range []string{
		`SELECT changed_cols("t", true, avg(temperature)) FROM stream GROUP BY CountingWindow(2)`,
		`SELECT lag(avg(temperature)) AS p FROM stream GROUP BY CountingWindow(2)`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// 嵌套错误信息锁定：各非法嵌套路径的报错文案不同，逐条钉死防回归。

func TestSQLCheck_NestedAnalytic_ErrorMessages_Reject(t *testing.T) {
	for _, c := range []struct {
		sql string
		sub string
	}{
		{`SELECT lag(lag(a)) AS p FROM stream`, "analytic functions cannot be nested"},                            // 分析套分析
		{`SELECT changed_col(true, lag(a)) AS c FROM stream`, "analytic functions cannot be nested"},              // 分析套分析（反向）
		{`SELECT lag(had_changed(true, a)) AS p FROM stream`, "analytic functions cannot be nested in had_changed"}, // 分析套分析（具名）
		{`SELECT sum(lag(a)) AS s FROM stream GROUP BY CountingWindow(2)`, "analytic functions cannot be nested"}, // 聚合套分析
		{`SELECT sum(count(x)) AS s FROM stream GROUP BY CountingWindow(2)`, "aggregate function calls cannot be nested"}, // 聚合套聚合
		{`SELECT max(sum(x)) AS m FROM stream GROUP BY CountingWindow(2)`, "aggregate function calls cannot be nested"},   // 聚合套聚合
	} {
		t.Run(c.sql, func(t *testing.T) {
			assertRejectExec(t, c.sql, c.sub)
		})
	}
}

// --- 4. 分析函数 alias 与其它输出列同名（D3）---

func TestSQLCheck_AnalyticAliasCollision_Reject(t *testing.T) {
	// 分析函数 alias 与普通列/其它分析 alias 同名 → 静默覆盖，解析期拒绝。
	for _, sql := range []string{
		`SELECT temperature, lag(temperature) AS temperature FROM stream`,          // alias 撞普通列
		`SELECT lag(a) AS x, changed_col(true, a) AS x FROM stream`,                // 两个分析 alias 撞
		`SELECT lag(a) AS k, had_changed(true, a) AS k FROM stream`,                // 两个分析 alias 撞
	} {
		t.Run(sql, func(t *testing.T) {
			assertRejectExec(t, sql, "duplicate output column")
		})
	}
}

func TestSQLCheck_AnalyticAliasDistinct_Accept(t *testing.T) {
	// alias 各异（含与普通列区分）→ 合法（防误伤）。
	for _, sql := range []string{
		`SELECT temperature, lag(temperature) AS temp_lag FROM stream`,
		`SELECT lag(a) AS la, changed_col(true, a) AS ca FROM stream`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// --- 5. GROUP BY 窗口之上不得叠 OVER(...)（阈值/持续检测用 HAVING）---

func TestSQLCheck_OverOnWindow_Reject(t *testing.T) {
	// parser 接受 GROUP BY <window> OVER(...) 语法并存入 Window.Over，
	// ToStreamConfig 显式拒绝并指引改用 HAVING（parseGroupBy 错误会被 errorRecovery 吞，故延后拒）。
	for _, sql := range []string{
		`SELECT avg(temp) AS m FROM stream GROUP BY CountingWindow(2) OVER (WHEN x > 0)`,
		`SELECT deviceId, max(temp) AS m FROM stream GROUP BY deviceId, CountingWindow(2) OVER (WHEN x > 0)`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertRejectExec(t, sql, "OVER(...) on a GROUP BY window is not supported")
		})
	}
}

// HAVING 是窗口阈值/持续检测的正规写法：合法（防误伤——别把替代方案也拦了）。

func TestSQLCheck_HavingOnWindow_Accept(t *testing.T) {
	for _, sql := range []string{
		`SELECT avg(concurrency) AS m FROM stream GROUP BY CountingWindow(2) HAVING min(concurrency) > 200`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// --- 6. WHERE 中的分析函数（SELECT 标量守卫不得波及 WHERE）---

func TestSQLCheck_WhereAnalytic_Accept(t *testing.T) {
	// WHERE 里的分析函数（含 OVER）走直连路径状态机求值，不受"标量套分析"守卫约束。
	for _, sql := range []string{
		`SELECT * FROM stream WHERE changed_col(true, temp) > 0`,
		`SELECT temp FROM stream WHERE changed_col(true, temp)`,
		`SELECT lag(temp) OVER (PARTITION BY k) AS p FROM stream WHERE lag(temp) OVER (PARTITION BY k) > 0`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// OVER 子句只支持 PARTITION BY / WHEN，不支持 ORDER BY（无 frame）：解析期即报错。

func TestSQLCheck_OrderByInOver_Reject(t *testing.T) {
	assertRejectExec(t, `SELECT lag(temp) OVER (PARTITION BY deviceId ORDER BY ts) AS p FROM stream`, "")
}
