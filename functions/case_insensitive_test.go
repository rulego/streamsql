package functions

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCaseInsensitiveFunctions 测试函数大小写不敏感
func TestCaseInsensitiveFunctions(t *testing.T) {
	tests := []struct {
		name         string
		functionName string
		expected     bool
	}{
		{"小写concat", "concat", true},
		{"大写CONCAT", "CONCAT", true},
		{"混合大小写Concat", "Concat", true},
		{"混合大小写cOnCaT", "cOnCaT", true},
		{"小写upper", "upper", true},
		{"大写UPPER", "UPPER", true},
		{"混合大小写Upper", "Upper", true},
		{"小写lower", "lower", true},
		{"大写LOWER", "LOWER", true},
		{"混合大小写Lower", "Lower", true},
		{"小写length", "length", true},
		{"大写LENGTH", "LENGTH", true},
		{"混合大小写Length", "Length", true},
		{"小写trim", "trim", true},
		{"大写TRIM", "TRIM", true},
		{"混合大小写Trim", "Trim", true},
		{"不存在的函数", "nonexistent", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, exists := Get(tt.functionName)
			assert.Equal(t, tt.expected, exists, "函数 %s 的查找结果应该是 %v", tt.functionName, tt.expected)
		})
	}
}

// TestConcatFunctionCaseInsensitive 测试CONCAT函数的大小写不敏感执行
func TestConcatFunctionCaseInsensitive(t *testing.T) {
	tests := []struct {
		name         string
		functionName string
		args         []interface{}
		expected     string
	}{
		{"小写concat", "concat", []interface{}{"hello", " ", "world"}, "hello world"},
		{"大写CONCAT", "CONCAT", []interface{}{"hello", " ", "world"}, "hello world"},
		{"混合大小写Concat", "Concat", []interface{}{"hello", " ", "world"}, "hello world"},
		{"混合大小写cOnCaT", "cOnCaT", []interface{}{"hello", " ", "world"}, "hello world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.functionName)
			assert.True(t, exists, "函数 %s 应该存在", tt.functionName)

			ctx := &FunctionContext{
				Data: make(map[string]interface{}),
			}

			result, err := fn.Execute(ctx, tt.args)
			assert.NoError(t, err, "函数 %s 执行不应该出错", tt.functionName)
			assert.Equal(t, tt.expected, result, "函数 %s 的执行结果应该正确", tt.functionName)
		})
	}
}

// TestStringFunctionsCaseInsensitive 测试所有字符串函数的大小写不敏感
func TestStringFunctionsCaseInsensitive(t *testing.T) {
	ctx := &FunctionContext{
		Data: make(map[string]interface{}),
	}

	// 测试UPPER函数
	t.Run("UPPER函数大小写不敏感", func(t *testing.T) {
		functionNames := []string{"upper", "UPPER", "Upper", "uPpEr"}
		for _, name := range functionNames {
			fn, exists := Get(name)
			assert.True(t, exists, "函数 %s 应该存在", name)

			result, err := fn.Execute(ctx, []interface{}{"hello"})
			assert.NoError(t, err, "函数 %s 执行不应该出错", name)
			assert.Equal(t, "HELLO", result, "函数 %s 的执行结果应该正确", name)
		}
	})

	// 测试LOWER函数
	t.Run("LOWER函数大小写不敏感", func(t *testing.T) {
		functionNames := []string{"lower", "LOWER", "Lower", "lOwEr"}
		for _, name := range functionNames {
			fn, exists := Get(name)
			assert.True(t, exists, "函数 %s 应该存在", name)

			result, err := fn.Execute(ctx, []interface{}{"HELLO"})
			assert.NoError(t, err, "函数 %s 执行不应该出错", name)
			assert.Equal(t, "hello", result, "函数 %s 的执行结果应该正确", name)
		}
	})

	// 测试LENGTH函数
	t.Run("LENGTH函数大小写不敏感", func(t *testing.T) {
		functionNames := []string{"length", "LENGTH", "Length", "lEnGtH"}
		for _, name := range functionNames {
			fn, exists := Get(name)
			assert.True(t, exists, "函数 %s 应该存在", name)

			result, err := fn.Execute(ctx, []interface{}{"hello"})
			assert.NoError(t, err, "函数 %s 执行不应该出错", name)
			assert.Equal(t, int64(5), result, "函数 %s 的执行结果应该正确", name)
		}
	})

	// 测试TRIM函数
	t.Run("TRIM函数大小写不敏感", func(t *testing.T) {
		functionNames := []string{"trim", "TRIM", "Trim", "tRiM"}
		for _, name := range functionNames {
			fn, exists := Get(name)
			assert.True(t, exists, "函数 %s 应该存在", name)

			result, err := fn.Execute(ctx, []interface{}{"  hello  "})
			assert.NoError(t, err, "函数 %s 执行不应该出错", name)
			assert.Equal(t, "hello", result, "函数 %s 的执行结果应该正确", name)
		}
	})
}

// TestMathFunctionsCaseInsensitive 测试数学函数的大小写不敏感
func TestMathFunctionsCaseInsensitive(t *testing.T) {
	ctx := &FunctionContext{
		Data: make(map[string]interface{}),
	}

	// 测试ABS函数
	t.Run("ABS函数大小写不敏感", func(t *testing.T) {
		functionNames := []string{"abs", "ABS", "Abs", "aBs"}
		for _, name := range functionNames {
			fn, exists := Get(name)
			assert.True(t, exists, "函数 %s 应该存在", name)

			result, err := fn.Execute(ctx, []interface{}{-5.5})
			assert.NoError(t, err, "函数 %s 执行不应该出错", name)
			assert.Equal(t, 5.5, result, "函数 %s 的执行结果应该正确", name)
		}
	})

	// 测试SQRT函数
	t.Run("SQRT函数大小写不敏感", func(t *testing.T) {
		functionNames := []string{"sqrt", "SQRT", "Sqrt", "sQrT"}
		for _, name := range functionNames {
			fn, exists := Get(name)
			assert.True(t, exists, "函数 %s 应该存在", name)

			result, err := fn.Execute(ctx, []interface{}{9.0})
			assert.NoError(t, err, "函数 %s 执行不应该出错", name)
			assert.Equal(t, 3.0, result, "函数 %s 的执行结果应该正确", name)
		}
	})
}

// TestAggregationFunctionsCaseInsensitive 测试聚合函数的大小写不敏感
func TestAggregationFunctionsCaseInsensitive(t *testing.T) {
	// 测试SUM函数
	t.Run("SUM函数大小写不敏感", func(t *testing.T) {
		functionNames := []string{"sum", "SUM", "Sum", "sUm"}
		for _, name := range functionNames {
			fn, exists := Get(name)
			assert.True(t, exists, "函数 %s 应该存在", name)
			assert.Equal(t, TypeAggregation, fn.GetType(), "函数 %s 应该是聚合函数", name)
		}
	})

	// 测试AVG函数
	t.Run("AVG函数大小写不敏感", func(t *testing.T) {
		functionNames := []string{"avg", "AVG", "Avg", "aVg"}
		for _, name := range functionNames {
			fn, exists := Get(name)
			assert.True(t, exists, "函数 %s 应该存在", name)
			assert.Equal(t, TypeAggregation, fn.GetType(), "函数 %s 应该是聚合函数", name)
		}
	})

	// 测试COUNT函数
	t.Run("COUNT函数大小写不敏感", func(t *testing.T) {
		functionNames := []string{"count", "COUNT", "Count", "cOuNt"}
		for _, name := range functionNames {
			fn, exists := Get(name)
			assert.True(t, exists, "函数 %s 应该存在", name)
			assert.Equal(t, TypeAggregation, fn.GetType(), "函数 %s 应该是聚合函数", name)
		}
	})
}
