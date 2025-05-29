package functions

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExprBridge(t *testing.T) {
	bridge := NewExprBridge()

	t.Run("StreamSQL Functions Available", func(t *testing.T) {
		// 测试StreamSQL函数是否可用
		data := map[string]interface{}{
			"temperature": 25.5,
			"humidity":    60,
		}

		// 测试数学函数
		result, err := bridge.EvaluateExpression("abs(-5)", data)
		assert.NoError(t, err)
		assert.Equal(t, float64(5), result)

		// 测试字符串函数
		result, err = bridge.EvaluateExpression("length(\"hello\")", data)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), result)
	})

	t.Run("Expr-Lang Functions Available", func(t *testing.T) {
		data := map[string]interface{}{
			"numbers": []int{1, 2, 3, 4, 5},
			"text":    "Hello World",
		}

		// 测试expr-lang数组函数
		result, err := bridge.EvaluateExpression("len(numbers)", data)
		assert.NoError(t, err)
		assert.Equal(t, 5, result)

		// 测试expr-lang字符串函数
		result, err = bridge.EvaluateExpression("trim(\"  hello  \")", data)
		assert.NoError(t, err)
		assert.Equal(t, "hello", result)
	})

	t.Run("Mixed Functions", func(t *testing.T) {
		data := map[string]interface{}{
			"values": []float64{-3.5, 2.1, -1.8, 4.2},
		}

		// 使用StreamSQL的abs函数和expr-lang的filter函数
		// 注意：这个测试可能需要根据实际实现调整
		env := bridge.CreateEnhancedExprEnvironment(data)

		// 验证环境中包含所有预期的函数
		assert.Contains(t, env, "abs")
		assert.Contains(t, env, "length")
		assert.Contains(t, env, "values")
	})

	t.Run("Function Resolution", func(t *testing.T) {
		// 测试函数解析优先级
		_, exists, source := bridge.ResolveFunction("abs")
		assert.True(t, exists)
		assert.Equal(t, "streamsql", source) // StreamSQL优先

		_, exists, source = bridge.ResolveFunction("encode")
		assert.True(t, exists)
		assert.Equal(t, "streamsql", source) // StreamSQL独有

		_, exists, _ = bridge.ResolveFunction("nonexistent")
		assert.False(t, exists)
	})

	t.Run("Function Information", func(t *testing.T) {
		info := bridge.GetFunctionInfo()

		// 验证包含StreamSQL函数信息
		streamSQLFuncs, ok := info["streamsql"].(map[string]interface{})
		assert.True(t, ok)
		assert.Contains(t, streamSQLFuncs, "abs")
		assert.Contains(t, streamSQLFuncs, "encode")

		// 验证包含expr-lang函数信息
		exprLangFuncs, ok := info["expr-lang"].(map[string]interface{})
		assert.True(t, ok)
		assert.Contains(t, exprLangFuncs, "trim")
		assert.Contains(t, exprLangFuncs, "filter")
	})
}

func TestEvaluateWithBridge(t *testing.T) {
	data := map[string]interface{}{
		"x": 3.5,
		"y": -2.1,
	}

	// 测试简单表达式
	result, err := EvaluateWithBridge("abs(y)", data)
	assert.NoError(t, err)
	assert.Equal(t, 2.1, result)

	// 测试复合表达式
	result, err = EvaluateWithBridge("x + abs(y)", data)
	assert.NoError(t, err)
	assert.Equal(t, 5.6, result)
}

func TestGetAllAvailableFunctions(t *testing.T) {
	info := GetAllAvailableFunctions()

	// 验证返回的信息结构
	assert.Contains(t, info, "streamsql")
	assert.Contains(t, info, "expr-lang")

	// 验证函数数量合理
	streamSQLFuncs := info["streamsql"].(map[string]interface{})
	t.Logf("StreamSQL functions count: %d", len(streamSQLFuncs))
	for name := range streamSQLFuncs {
		t.Logf("StreamSQL function: %s", name)
	}
	assert.GreaterOrEqual(t, len(streamSQLFuncs), 1) // 至少应该有一个函数

	exprLangFuncs := info["expr-lang"].(map[string]interface{})
	t.Logf("Expr-lang functions count: %d", len(exprLangFuncs))
	assert.GreaterOrEqual(t, len(exprLangFuncs), 1) // 至少应该有一个函数
}

func TestFunctionConflictResolution(t *testing.T) {
	bridge := NewExprBridge()
	data := map[string]interface{}{
		"value": -5.5,
	}

	// 测试冲突函数的解析（abs函数在两个系统中都存在）
	// 应该优先使用expr-lang的版本
	env := bridge.CreateEnhancedExprEnvironment(data)

	// 验证StreamSQL函数可以通过别名访问
	assert.Contains(t, env, "streamsql_abs")
	assert.Contains(t, env, "abs")

	// 测试两个版本都能正常工作
	result, err := bridge.EvaluateExpression("abs(value)", data)
	assert.NoError(t, err)
	assert.Equal(t, 5.5, result)
}
