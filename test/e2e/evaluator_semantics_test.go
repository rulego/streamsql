package e2e

import (
	"testing"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 求值器语义探针：从 SQL 标准语义推导期望值，验证 NULL 传播 / 字符串参与算术 / 路由边界。
// 目的：探明三套求值器接缝（NULL 三值、float64 陷阱、substring 路由）是潜在风险还是现行 bug。

// NULL 在算术里应传播（SQL：NULL + n = NULL），不应被当作 0。
func TestEvalSem_NullInArithmetic(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT temperature + 10 AS x FROM stream"))
	defer ssql.Stop()

	// 正常值：5 + 10 = 15
	r1, err := ssql.EmitSync(map[string]any{"temperature": 5.0})
	require.NoError(t, err)
	require.NotNil(t, r1)
	assert.Equal(t, 15.0, asFloat64(r1["x"]))

	// nil 应传播为 nil，而非 0+10=10
	r2, err := ssql.EmitSync(map[string]any{"temperature": nil})
	require.NoError(t, err)
	if r2 != nil {
		assert.Nil(t, r2["x"], "NULL + 10 应为 NULL（SQL 传播），不应当作 0 得 10")
	}
}

// WHERE 中 NULL 与数值比较：SQL 三值逻辑下 NULL > 20 为 UNKNOWN，行应被过滤掉。
func TestEvalSem_NullInWhereComparison(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT temperature FROM stream WHERE temperature > 20"))
	defer ssql.Stop()

	// 正常值通过
	r1, err := ssql.EmitSync(map[string]any{"temperature": 25.0})
	require.NoError(t, err)
	require.NotNil(t, r1, "25 > 20 应通过 WHERE")

	// nil 比较应为 UNKNOWN → 行被过滤（r2=nil）
	r2, err := ssql.EmitSync(map[string]any{"temperature": nil})
	require.NoError(t, err)
	assert.Nil(t, r2, "NULL > 20 应为 UNKNOWN，行应被过滤")
}

// 字符串参与算术：SQL 下字符串与数值运算应为类型错误/NULL，不应把字符串长度当数值。
// 探测 expr 包 evaluateNode 的 float64 陷阱（非数字字符串 → float64(len)）。
func TestEvalSem_StringInArithmetic(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT name * 2 AS x FROM stream"))
	defer ssql.Stop()

	r, err := ssql.EmitSync(map[string]any{"name": "abc"})
	require.NoError(t, err)
	// "abc" 长度为 3；若命中 float64 陷阱会得到 6.0。SQL 正确结果应是 NULL/错误，绝不是 6.0。
	if r != nil {
		assert.NotEqual(t, 6.0, asFloat64(r["x"]), "字符串不应被当作长度 3 参与算术得 6.0（float64 陷阱）")
	}
}

// CASE 中 NULL 分支：未命中且无 ELSE → NULL（SQL 标准）。
func TestEvalSem_CaseNoMatchNull(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT CASE WHEN temperature > 100 THEN 'hot' END AS label FROM stream"))
	defer ssql.Stop()

	r, err := ssql.EmitSync(map[string]any{"temperature": 50.0})
	require.NoError(t, err)
	require.NotNil(t, r)
	assert.Nil(t, r["label"], "未命中且无 ELSE，CASE 应为 NULL")
}
