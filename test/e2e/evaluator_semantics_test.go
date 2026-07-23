package e2e

import (
	"testing"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Evaluator semantic probe: Deduces expected values from SQL standard semantics to verify NULL propagation
// Objective: To determine whether the three sets of evaluator seams (NULL three-value, float64 trap, substring routing) are potential risks or existing bugs.

// NULL should be propagated in arithmetic (SQL:NULL + n = NULL) and should not be treated as 0.
func TestEvalSem_NullInArithmetic(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT temperature + 10 AS x FROM stream"))
	defer ssql.Stop()

	// Normal value: 5 + 10 = 15
	r1, err := ssql.EmitSync(map[string]any{"temperature": 5.0})
	require.NoError(t, err)
	require.NotNil(t, r1)
	assert.Equal(t, 15.0, asFloat64(r1["x"]))

	// NIL should propagate as nil, not 0+10=10
	r2, err := ssql.EmitSync(map[string]any{"temperature": nil})
	require.NoError(t, err)
	if r2 != nil {
		assert.Nil(t, r2["x"], "NULL + 10 应为 NULL（SQL 传播），不应当作 0 得 10")
	}
}

// Comparison of NULL and Numeric Values in WHERE Values: In SQL three-value logic, NULL > 20 is UNKNOWN, so rows should be filtered out.
func TestEvalSem_NullInWhereComparison(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT temperature FROM stream WHERE temperature > 20"))
	defer ssql.Stop()

	// Normal values pass
	r1, err := ssql.EmitSync(map[string]any{"temperature": 25.0})
	require.NoError(t, err)
	require.NotNil(t, r1, "25 > 20 应通过 WHERE")

	// nil comparison should be UNKNOWN → rows filtered (r2=nil)
	r2, err := ssql.EmitSync(map[string]any{"temperature": nil})
	require.NoError(t, err)
	assert.Nil(t, r2, "NULL > 20 应为 UNKNOWN，行应被过滤")
}

// String Participation in Arithmetic: In SQL, string and numeric operations should be type error/NULL, and string length should not be treated as a value.
// Detect float64 traps (non-numeric strings → float64(len)) of the expr packet evaluateNode.
func TestEvalSem_StringInArithmetic(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT name * 2 AS x FROM stream"))
	defer ssql.Stop()

	r, err := ssql.EmitSync(map[string]any{"name": "abc"})
	require.NoError(t, err)
	// "abc" is 3 lengths; hitting the float64 trap grants 6.0. The correct SQL result should be NULL/error, definitely not 6.0.
	if r != nil {
		assert.NotEqual(t, 6.0, asFloat64(r["x"]), "字符串不应被当作长度 3 参与算术得 6.0（float64 陷阱）")
	}
}

// NULL branch in CASE: Missed and without ELSE → NULL (SQL standard).
func TestEvalSem_CaseNoMatchNull(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT CASE WHEN temperature > 100 THEN 'hot' END AS label FROM stream"))
	defer ssql.Stop()

	r, err := ssql.EmitSync(map[string]any{"temperature": 50.0})
	require.NoError(t, err)
	require.NotNil(t, r)
	assert.Nil(t, r["label"], "未命中且无 ELSE，CASE 应为 NULL")
}
