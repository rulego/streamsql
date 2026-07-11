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
//     （如 ts - lag(ts) OVER(...)），不在此拦；字符串字面量里的 "lag(" 不得误判。
//  2. 窗口查询里分析函数的参数必须是聚合函数或 GROUP BY 字段，不得引用裸原始列（D1）。
//  3. 嵌套：分析套分析、聚合套分析非法（D9）；分析套聚合（内联写法）合法。

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

// --- 1. 分析函数嵌进标量函数：解析期拒绝 ---

func TestSQLCheck_AnalyticInScalarFunction_Reject(t *testing.T) {
	for _, sql := range []string{
		`SELECT UPPER(changed_col(true, temperature)) AS c FROM stream`,                    // 标量套分析（小写）
		`SELECT UPPER(CHANGED_COL(true, temperature)) AS c FROM stream`,                    // 标量套分析（大写）
		`SELECT ROUND(lag(temperature), 2) AS c FROM stream`,                               // 标量套 lag
		`SELECT ABS(acc_sum(v)) AS a FROM stream`,                                          // 标量套 acc
		`SELECT ROUND(UPPER(changed_col(true, temperature)), 2) AS c FROM stream`,          // 深层嵌套标量套分析
		`SELECT CONCAT('prefix', changed_col(true, temperature)) AS s FROM stream`,         // 多参标量套分析
		`SELECT LOWER(latest(temperature)) AS s FROM stream`,                               // 标量套 latest
	} {
		t.Run(sql, func(t *testing.T) {
			assertRejectExec(t, sql, "standalone field or with OVER")
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
		// 算术表达式里带 OVER 的分析函数：合法（标量套分析检查仅拦"标量函数"顶层，不拦算术）。
		`SELECT ts - lag(ts) OVER (PARTITION BY k) AS d FROM stream`,
		// 字符串字面量里形如 "分析函数名(" 的文本不得误判为函数调用（曾误伤）。
		`SELECT CONCAT('lag is great', name) AS s FROM stream`,  // 字面量含分析名但无 "("
		`SELECT CONCAT('see lag(', name) AS s FROM stream`,      // 字面量含 "lag("
		`SELECT UPPER('changed_col(x)') AS s FROM stream`,       // 字面量含 "changed_col("
		`SELECT UPPER('latest(y)') AS s FROM stream`,            // 字面量含 "latest("
		`SELECT UPPER('acc_sum(z)') AS s FROM stream`,           // 字面量含 "acc_sum("
		`SELECT UPPER('had_changed(w)') AS s FROM stream`,       // 字面量含 "had_changed("
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
