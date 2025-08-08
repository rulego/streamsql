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
		assert.Equal(t, int(5), result)
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
	// for name := range streamSQLFuncs {
	// 	t.Logf("StreamSQL function: %s", name)
	// }
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

func TestExprBridgeAdvancedFunctions(t *testing.T) {
	bridge := NewExprBridge()

	t.Run("String Concatenation Detection", func(t *testing.T) {
		data := map[string]interface{}{
			"name": "John",
			"age":  25,
		}

		// 测试字符串连接表达式检测
		isConcat := bridge.isStringConcatenationExpression("name + ' is ' + age", data)
		assert.True(t, isConcat)

		isConcat = bridge.isStringConcatenationExpression("abs(-5)", data)
		assert.False(t, isConcat)
	})

	t.Run("Fallback to Custom Expression", func(t *testing.T) {
		data := map[string]interface{}{
			"text": "hello",
		}

		// 测试回退到自定义表达式处理
		result, err := bridge.fallbackToCustomExpr("text + ' world'", data)
		assert.NoError(t, err)
		assert.Equal(t, "hello world", result)
	})

	t.Run("String Concatenation Evaluation", func(t *testing.T) {
		data := map[string]interface{}{
			"first":  "Hello",
			"second": "World",
		}

		// 测试字符串连接求值
		result, err := bridge.evaluateStringConcatenation("first + ' ' + second", data)
		assert.NoError(t, err)
		assert.Equal(t, "Hello World", result)
	})

	t.Run("Simple Numeric Expression", func(t *testing.T) {
		data := map[string]interface{}{
			"x": 10,
			"y": 5,
		}

		// 测试简单数值表达式
		result, err := bridge.evaluateSimpleNumericExpression("x + y", data)
		assert.NoError(t, err)
		assert.Equal(t, float64(15), result)
	})

	t.Run("Function Call Detection", func(t *testing.T) {
		// 测试函数调用检测
		assert.True(t, bridge.isFunctionCall("abs(-5)"))
		assert.True(t, bridge.isFunctionCall("length('hello')"))
		assert.False(t, bridge.isFunctionCall("x + y"))
		assert.False(t, bridge.isFunctionCall("simple_variable"))
	})

	t.Run("Like Expression Preprocessing", func(t *testing.T) {
		// 测试LIKE表达式预处理
		processed, err := bridge.PreprocessLikeExpression("name LIKE '%john%'")
		assert.NoError(t, err)
		assert.Contains(t, processed, "contains")
	})

	t.Run("IsNull Expression Preprocessing", func(t *testing.T) {
		// 测试IS NULL表达式预处理
		processed, err := bridge.PreprocessIsNullExpression("field IS NULL")
		assert.NoError(t, err)
		assert.Contains(t, processed, "== nil")
	})

	t.Run("Backtick Identifiers", func(t *testing.T) {
		// 测试反引号标识符检测
		assert.True(t, bridge.ContainsBacktickIdentifiers("`field_name` = 1"))
		assert.False(t, bridge.ContainsBacktickIdentifiers("field_name = 1"))

		// 测试反引号标识符预处理
		processed, err := bridge.PreprocessBacktickIdentifiers("`field_name` = 1")
		assert.NoError(t, err)
		assert.Contains(t, processed, "field_name")
	})

	t.Run("Like Pattern Matching", func(t *testing.T) {
		// 测试LIKE模式匹配
		assert.True(t, bridge.matchesLikePattern("hello", "h%"))
		assert.True(t, bridge.matchesLikePattern("world", "%d"))
		assert.False(t, bridge.matchesLikePattern("hello", "x%"))

		// 测试递归LIKE匹配
		assert.True(t, bridge.likeMatch("hello", "h%o", 0, 0))
		assert.False(t, bridge.likeMatch("hello", "x%", 0, 0))
	})

	t.Run("Type Conversion", func(t *testing.T) {
		// 测试类型转换
		result, err := bridge.toFloat64(10)
		assert.NoError(t, err)
		assert.Equal(t, float64(10), result)

		result, err = bridge.toFloat64("10.5")
		assert.NoError(t, err)
		assert.Equal(t, float64(10.5), result)

		_, err = bridge.toFloat64("invalid")
		assert.Error(t, err)
	})

	t.Run("Expr Lang Function Detection", func(t *testing.T) {
		// 测试expr-lang函数检测
		assert.True(t, bridge.IsExprLangFunction("trim"))
		assert.True(t, bridge.IsExprLangFunction("len"))
		assert.True(t, bridge.IsExprLangFunction("abs"))
		assert.False(t, bridge.IsExprLangFunction("nonexistent"))
	})

	t.Run("Like to Function Conversion", func(t *testing.T) {
		// 测试LIKE转换为函数调用
		result := bridge.convertLikeToFunction("name", "%john%")
		assert.Contains(t, result, "contains")
		assert.Contains(t, result, "name")
		assert.Contains(t, result, "john")
	})
}

// TestExprBridgeComplexExpressions 测试复杂表达式处理
func TestExprBridgeComplexExpressions(t *testing.T) {
	bridge := NewExprBridge()

	tests := []struct {
		name       string
		expression string
		data       map[string]interface{}
		expected   interface{}
		wantErr    bool
	}{
		{
			name:       "math_and_string",
			expression: "length('test')",
			data:       map[string]interface{}{},
			expected:   4,
			wantErr:    false,
		},
		{
			name:       "nested_function_calls",
			expression: "abs(sqrt(16) - 5)",
			data:       map[string]interface{}{},
			expected:   float64(1),
			wantErr:    false,
		},
		{
			name:       "array_operations",
			expression: "array_length([1, 2, 3, 4])",
			data:       map[string]interface{}{},
			expected:   4,
			wantErr:    false,
		},
		{
			name:       "string_with_variables",
			expression: "upper(name)",
			data:       map[string]interface{}{"name": "john"},
			expected:   "JOHN",
			wantErr:    false,
		},
		{
			name:       "conditional_expression",
			expression: "age > 18 ? 'adult' : 'minor'",
			data:       map[string]interface{}{"age": 25},
			expected:   "adult",
			wantErr:    false,
		},
		{
			name:       "complex_math",
			expression: "power(2, 3) + mod(10, 3)",
			data:       map[string]interface{}{},
			expected:   float64(9),
			wantErr:    false,
		},
		{
			name:       "array_contains_check",
			expression: "array_contains([1, 2, 3], 2)",
			data:       map[string]interface{}{},
			expected:   true,
			wantErr:    false,
		},
		{
			name:       "string_concatenation",
			expression: "concat(first_name, ' ', last_name)",
			data:       map[string]interface{}{"first_name": "John", "last_name": "Doe"},
			expected:   "John Doe",
			wantErr:    false,
		},
		{
			name:       "invalid_function",
			expression: "nonexistent_function(1)",
			data:       map[string]interface{}{},
			expected:   nil,
			wantErr:    true,
		},
		{
			name:       "invalid_syntax",
			expression: "length(",
			data:       map[string]interface{}{},
			expected:   nil,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bridge.EvaluateExpression(tt.expression, tt.data)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
