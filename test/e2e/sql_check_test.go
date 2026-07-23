package e2e

import (
	"testing"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
)

// Integrated testing of SQL parsing period checks (rsql/ast.go ToStreamConfig).
// Aggregate various SQL items that "should be rejected during parsing" and "should be allowed to be released," lock validation boundaries, and prevent regression.
//
// Validation is divided into several categories:
//  1. Analysis functions must not be embedded in scalar functions (UPPER/ABS/ROUND...));  The analysis function with OVER in the arithmetic expression is valid
//     (e.g., ts - lag(ts) OVER(...)), not stopped here; String literals (single/double quotes, including escape quotes).
//     "lag(" must not be mistakenly identified as a function call.
//  2. In window queries, the parameters of the analysis function must be aggregate functions or GROUP BY fields; you cannot reference the raw column (D1).
//  3. Nested: Nested analysis and aggregate nest analysis are illegal (D9); Analytical set aggregation (inline writing) is legal.
//  4. The alias analysis function must not be the same name as other output columns (silent coverage = semi-finished product, D3).
//  5. Do not stack OVER on top of the GROUP BY window(...) (Threshold/continuous detection switches to HAVING).
//  6. The analysis function in WHERE (including OVER) is evaluated by the directly connected path state machine and is not constrained by SELECT scalar guards.

// assertRejectExec expects Execute to report an error at parse time, and the error message contains sub.
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

// assertAcceptExec expects Execute to parse successfully (no errors).
func assertAcceptExec(t *testing.T, sql string) {
	t.Helper()
	assert.NoError(t, errExec(sql), "不应被解析期拒绝: %s", sql)
}

func errExec(sql string) error {
	ssql := streamsql.New()
	defer ssql.Stop()
	return ssql.Execute(sql)
}

// --- 1. Analysis function embedding scalar functions: release (scalar sleeve analysis follows wrapper subgeneration, supports coalesce/CASE/UPPER, etc.)---

func TestSQLCheck_AnalyticInScalarFunction_Accept(t *testing.T) {
	for _, sql := range []string{
		`SELECT UPPER(changed_col(true, temperature)) AS c FROM stream`,              // Scalar Set Analysis (lowercase)
		`SELECT UPPER(CHANGED_COL(true, temperature)) AS c FROM stream`,              // Scalar Suite Analysis (in words)
		`SELECT ROUND(lag(temperature), 2) AS c FROM stream`,                         // Scalar set lag
		`SELECT ABS(acc_sum(v)) AS a FROM stream`,                                    // Scalar sleeve ACC
		`SELECT ROUND(UPPER(changed_col(true, temperature)), 2) AS c FROM stream`,    // Deep nested scalar sleeve analysis
		`SELECT CONCAT('prefix', changed_col(true, temperature)) AS s FROM stream`,   // Multi-parameter scalar set analysis
		`SELECT LOWER(latest(temperature)) AS s FROM stream`,                         // Scalar application latest
		`SELECT coalesce(lag(temp), -1) AS s FROM stream`,                            // Scalar set lag + default values
		`SELECT CASE WHEN lag(temp) > 20 THEN 'up' ELSE 'down' END AS s FROM stream`, // CASE set analysis
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// --- 1b. Analyze the function at the valid position + string literal count without accident: Release ---

func TestSQLCheck_AnalyticValidPlacement_Accept(t *testing.T) {
	for _, sql := range []string{
		`SELECT UPPER(name) AS n FROM stream`,                                   // Pure scalar
		`SELECT temperature * 1.8 + 32 AS f FROM stream`,                        // Pure arithmetic
		`SELECT changed_col(true, temperature) AS c FROM stream`,                // Top-level analysis
		`SELECT lag(temperature) OVER (PARTITION BY deviceId) AS p FROM stream`, // Analysis + OVER
		`SELECT acc_sum(v) AS s FROM stream`,                                    // acc top level
		// ACC The top level of the entire clan is legal.
		`SELECT acc_max(v) AS m FROM stream`,
		`SELECT acc_min(v) AS m FROM stream`,
		`SELECT acc_count(v) AS c FROM stream`,
		`SELECT acc_avg(v) AS a FROM stream`,
		// had_changed Multi-column variable parameter top-level valid.
		`SELECT had_changed(true, a, b) AS h FROM stream`,
		// The OVER clause has multiple legal writing methods.
		`SELECT lag(temp, 1) OVER () AS p FROM stream`,                                 // Empty OVER
		`SELECT lag(temp, 1, 0) OVER (PARTITION BY deviceId) AS p FROM stream`,         // lag with default values
		`SELECT acc_sum(v) OVER (PARTITION BY deviceId) AS s FROM stream`,              // acc + OVER
		`SELECT latest(temp) OVER (PARTITION BY deviceId) AS l FROM stream`,            // latest + OVER
		`SELECT lag(temp) OVER (PARTITION BY deviceId WHEN temp > 0) AS p FROM stream`, // OVER(WHEN) Input gating
		// Analysis functions with OVER in arithmetic expressions: valid (scalar suite analysis checks only block the top level of the "scalar function", not arithmetic).
		`SELECT ts - lag(ts) OVER (PARTITION BY k) AS d FROM stream`,
		`SELECT 100 - lag(ts) OVER (PARTITION BY k) AS d FROM stream`, // The analysis function is on the right side of arithmetic
		// Multiple analysis fields, analysis fields coexisting with regular columns/scalars: legal.
		`SELECT lag(a) OVER (PARTITION BY k) AS p, changed_col(true, b) AS c FROM stream`,
		`SELECT lag(a) OVER (PARTITION BY k) AS p, name AS n FROM stream`,
		`SELECT lag(a) OVER (PARTITION BY k) AS p, UPPER(name) AS u FROM stream`,
		// Text in string literals like "Analysis function_name(" must not be mistakenly interpreted as a function call (has been mistakenly damaged).
		`SELECT CONCAT('lag is great', name) AS s FROM stream`, // The literal quantity contains the analysis name but does not have "("
		`SELECT CONCAT('see lag(', name) AS s FROM stream`,     // Single quote literal numbers containing "lag("
		`SELECT UPPER('changed_col(x)') AS s FROM stream`,      // Single quotation letters containing "changed_col("
		`SELECT UPPER('latest(y)') AS s FROM stream`,           // Single quotation literals containing "latest("
		`SELECT UPPER('acc_sum(z)') AS s FROM stream`,          // Single quotation literal numbers containing "acc_sum("
		`SELECT UPPER('had_changed(w)') AS s FROM stream`,      // Single quotation letters containing "had_changed("
		// Escape single quotation marks ('') The analysis name in literal quantity must not be misinterpreted: the boundary of literal quantity is based on escape quotes; naive stripping will cause leakage.
		`SELECT UPPER('it''s lag(x)') AS s FROM stream`,         // The extremity after escape quotes contains "lag(")
		`SELECT UPPER('a''b''changed_col(x)') AS s FROM stream`, // Multiple escape quotation marks
		// The analysis name in double quotation literals (changed_cols prefix in double quotes) must also not be misinterpreted.
		`SELECT UPPER("lag(x)") AS s FROM stream`,         // Double quotation letters containing "lag("
		`SELECT UPPER("changed_col(x)") AS s FROM stream`, // Double quotation literal values containing "changed_col("
		`SELECT CONCAT("t_lag_", name) AS s FROM stream`,  // Double quotation prefix, no analytic name
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// --- 2. Analysis functions in window queries ---

func TestSQLCheck_WindowAnalytic_Accept(t *testing.T) {
	// Parameters are aggregate function or GROUP BY field: Valid.
	for _, sql := range []string{
		`SELECT changed_col(true, avg(temperature)) AS c FROM stream GROUP BY CountingWindow(2)`,
		`SELECT changed_cols("t", true, avg(temperature), max(temperature)) FROM stream GROUP BY CountingWindow(2)`,
		`SELECT deviceId, changed_col(true, avg(temp)) AS chg FROM stream GROUP BY deviceId, CountingWindow(2)`,
		`SELECT acc_sum(avg(temperature)) AS total FROM stream GROUP BY CountingWindow(2)`,
		// Analysis set different aggregations: Valid (parser function evaluates the window aggregation output).
		`SELECT changed_col(true, max(temp)) AS c FROM stream GROUP BY CountingWindow(2)`,
		`SELECT acc_sum(max(temp)) AS s FROM stream GROUP BY CountingWindow(2)`,
		`SELECT had_changed(true, avg(temp)) AS h FROM stream GROUP BY CountingWindow(2)`,
		// The restricted column happens to be the GROUP BY key: select column name suffix and judge, etc., which is valid (to prevent accidental damage to restricted columns).
		`SELECT changed_col(true, stream.deviceId) AS c FROM stream GROUP BY deviceId, CountingWindow(2)`,
		// Complex expression parameters (including operators) are not within the bare list interception range: Valid.
		`SELECT changed_col(true, avg(temp) + 1) AS c FROM stream GROUP BY CountingWindow(2)`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

func TestSQLCheck_WindowAnalytic_RawColumn_Reject(t *testing.T) {
	// D1: In window queries, the parser function references the raw columns (not aggregated, non-grouped keys) → rejects during parsing.
	for _, sql := range []string{
		`SELECT lag(temperature) AS p FROM stream GROUP BY CountingWindow(2)`,
		`SELECT deviceId, lag(temperature) AS p FROM stream GROUP BY deviceId, CountingWindow(2)`,
		// Restricted columns but suffix names not GROUP BY keys: Also treated as bare original columns (Accidental damage prevention: restricted does not equal exemption).
		`SELECT changed_col(true, stream.deviceId) AS c FROM stream GROUP BY CountingWindow(2)`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertRejectExec(t, sql, "raw column")
		})
	}
}

// --- 3. Nested Functions (D9)---

func TestSQLCheck_NestedAnalytic_Reject(t *testing.T) {
	// Analytical suite analysis, aggregate suite analysis → illegal.
	for _, sql := range []string{
		`SELECT lag(lag(a)) AS p FROM stream`,                                // Analysis set
		`SELECT had_changed(true, changed_col(true, a)) AS h FROM stream`,    // Analysis set
		`SELECT sum(lag(a)) AS s FROM stream GROUP BY CountingWindow(2)`,     // Aggregate Suite Analysis (Window)
		`SELECT max(acc_sum(v)) AS m FROM stream GROUP BY CountingWindow(2)`, // Polymerization sleeve ACC
	} {
		t.Run(sql, func(t *testing.T) {
			assertRejectExec(t, sql, "") // Each path has different error messages, only assertioning errors
		})
	}
}

func TestSQLCheck_AnalyticWrappingAggregate_Accept(t *testing.T) {
	// Analytical set aggregation (inline writing) → Legal.
	for _, sql := range []string{
		`SELECT changed_cols("t", true, avg(temperature)) FROM stream GROUP BY CountingWindow(2)`,
		`SELECT lag(avg(temperature)) AS p FROM stream GROUP BY CountingWindow(2)`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// Nested error message locking: Each illegal nested path has different error messages, pinned one by one to prevent regression.

func TestSQLCheck_NestedAnalytic_ErrorMessages_Reject(t *testing.T) {
	for _, c := range []struct {
		sql string
		sub string
	}{
		{`SELECT lag(lag(a)) AS p FROM stream`, "analytic functions cannot be nested"},                                    // Analysis set
		{`SELECT changed_col(true, lag(a)) AS c FROM stream`, "analytic functions cannot be nested"},                      // Analysis Set Analysis (Reverse Direction)
		{`SELECT lag(had_changed(true, a)) AS p FROM stream`, "analytic functions cannot be nested in had_changed"},       // Analysis Suite Analysis (Named)
		{`SELECT sum(lag(a)) AS s FROM stream GROUP BY CountingWindow(2)`, "analytic functions cannot be nested"},         // Polymerization set analysis
		{`SELECT sum(count(x)) AS s FROM stream GROUP BY CountingWindow(2)`, "aggregate function calls cannot be nested"}, // Polymerization sets
		{`SELECT max(sum(x)) AS m FROM stream GROUP BY CountingWindow(2)`, "aggregate function calls cannot be nested"},   // Polymerization sets
	} {
		t.Run(c.sql, func(t *testing.T) {
			assertRejectExec(t, c.sql, c.sub)
		})
	}
}

// --- 4. The analysis function alias shares the same name as other output columns (D3)---

func TestSQLCheck_AnalyticAliasCollision_Reject(t *testing.T) {
	// The analysis function alias shares the same name as the regular column/other analysis alias → muted coverage, rejected during the parsing period.
	for _, sql := range []string{
		`SELECT temperature, lag(temperature) AS temperature FROM stream`, // Alias crashed into a regular line
		`SELECT lag(a) AS x, changed_col(true, a) AS x FROM stream`,       // Two analyses of alias collide
		`SELECT lag(a) AS k, had_changed(true, a) AS k FROM stream`,       // Two analyses of alias collide
	} {
		t.Run(sql, func(t *testing.T) {
			assertRejectExec(t, sql, "duplicate output column")
		})
	}
}

func TestSQLCheck_AnalyticAliasDistinct_Accept(t *testing.T) {
	// Alias vary (including distinction from regular columns) → Legal (to prevent accidental damage).
	for _, sql := range []string{
		`SELECT temperature, lag(temperature) AS temp_lag FROM stream`,
		`SELECT lag(a) AS la, changed_col(true, a) AS ca FROM stream`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// --- 5. GROUP BY cannot be stacked above the window OVER(...) (HAVING for threshold/continuous detection)---

func TestSQLCheck_OverOnWindow_Reject(t *testing.T) {
	// parser accepts the GROUP BY <window> OVER(...) syntax and stores it in Window.Over,
	// ToStreamConfig explicitly rejects and guides to switch to HAVING (parseGroupBy errors are swallowed by errorRecovery, so postponed rejection).
	for _, sql := range []string{
		`SELECT avg(temp) AS m FROM stream GROUP BY CountingWindow(2) OVER (WHEN x > 0)`,
		`SELECT deviceId, max(temp) AS m FROM stream GROUP BY deviceId, CountingWindow(2) OVER (WHEN x > 0)`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertRejectExec(t, sql, "OVER(...) on a GROUP BY window is not supported")
		})
	}
}

// HAVING is the standard way to use window threshold/continuous detection: legal (to prevent accidental damage—don't block alternative solutions too).

func TestSQLCheck_HavingOnWindow_Accept(t *testing.T) {
	for _, sql := range []string{
		`SELECT avg(concurrency) AS m FROM stream GROUP BY CountingWindow(2) HAVING min(concurrency) > 200`,
	} {
		t.Run(sql, func(t *testing.T) {
			assertAcceptExec(t, sql)
		})
	}
}

// --- 6. Analysis function in WHERE (SELECT scalar guards must not affect WHERE)---

func TestSQLCheck_WhereAnalytic_Accept(t *testing.T) {
	// The analysis functions (including OVER) in WHERE are directly connected to the path state machine and are not constrained by the "scalar suite analysis" guard.
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

// The OVER clause only supports PARTITION BY / WHEN, does not support ORDER BY (FRAME): an error occurs during the parsing period.

func TestSQLCheck_OrderByInOver_Reject(t *testing.T) {
	assertRejectExec(t, `SELECT lag(temp) OVER (PARTITION BY deviceId ORDER BY ts) AS p FROM stream`, "")
}
