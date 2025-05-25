package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExpressionEvaluation(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		data     map[string]interface{}
		expected float64
		hasError bool
	}{
		// 基本运算测试
		{"Simple Addition", "a + b", map[string]interface{}{"a": 5, "b": 3}, 8, false},
		{"Simple Subtraction", "a - b", map[string]interface{}{"a": 5, "b": 3}, 2, false},
		{"Simple Multiplication", "a * b", map[string]interface{}{"a": 5, "b": 3}, 15, false},
		{"Simple Division", "a / b", map[string]interface{}{"a": 6, "b": 3}, 2, false},
		{"Modulo", "a % b", map[string]interface{}{"a": 7, "b": 4}, 3, false},
		{"Power", "a ^ b", map[string]interface{}{"a": 2, "b": 3}, 8, false},

		// 复合表达式测试
		{"Complex Expression", "a + b * c", map[string]interface{}{"a": 5, "b": 3, "c": 2}, 11, false},
		{"Complex Expression With Parentheses", "(a + b) * c", map[string]interface{}{"a": 5, "b": 3, "c": 2}, 16, false},
		{"Multiple Operations", "a + b * c - d / e", map[string]interface{}{"a": 5, "b": 3, "c": 2, "d": 8, "e": 4}, 9, false},

		// 函数调用测试
		{"Abs Function", "abs(a - b)", map[string]interface{}{"a": 3, "b": 5}, 2, false},
		{"Sqrt Function", "sqrt(a)", map[string]interface{}{"a": 16}, 4, false},
		{"Round Function", "round(a)", map[string]interface{}{"a": 3.7}, 4, false},

		// 转换测试
		{"String to Number", "a + b", map[string]interface{}{"a": "5", "b": 3}, 8, false},

		// 复杂表达式测试
		{"Temperature Conversion", "temperature * 1.8 + 32", map[string]interface{}{"temperature": 25}, 77, false},
		{"Complex Math", "sqrt(abs(a * b - c / d))", map[string]interface{}{"a": 10, "b": 2, "c": 5, "d": 1}, 4.5, false},

		// 错误测试
		{"Division by Zero", "a / b", map[string]interface{}{"a": 5, "b": 0}, 0, true},
		{"Missing Field", "a + b", map[string]interface{}{"a": 5}, 0, true},
		{"Invalid Function", "unknown(a)", map[string]interface{}{"a": 5}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.expr)
			assert.NoError(t, err, "Expression parsing should not fail")

			result, err := expr.Evaluate(tt.data)
			if tt.hasError {
				assert.Error(t, err, "Expected error")
			} else {
				assert.NoError(t, err, "Evaluation should not fail")
				assert.InDelta(t, tt.expected, result, 0.001, "Result should match expected value")
			}
		})
	}
}

func TestGetFields(t *testing.T) {
	tests := []struct {
		expr           string
		expectedFields []string
	}{
		{"a + b", []string{"a", "b"}},
		{"a + b * c", []string{"a", "b", "c"}},
		{"temperature * 1.8 + 32", []string{"temperature"}},
		{"abs(humidity - 50)", []string{"humidity"}},
		{"sqrt(x^2 + y^2)", []string{"x", "y"}},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			expr, err := NewExpression(tt.expr)
			assert.NoError(t, err, "Expression parsing should not fail")

			fields := expr.GetFields()

			// 由于map迭代顺序不确定，我们只检查长度和包含关系
			assert.Equal(t, len(tt.expectedFields), len(fields), "Number of fields should match")

			for _, field := range tt.expectedFields {
				found := false
				for _, f := range fields {
					if f == field {
						found = true
						break
					}
				}
				assert.True(t, found, "Field %s should be found", field)
			}
		})
	}
}

func TestParseError(t *testing.T) {
	tests := []struct {
		name string
		expr string
	}{
		{"Empty Expression", ""},
		{"Mismatched Parentheses", "a + (b * c"},
		{"Invalid Character", "a # b"},
		{"Double Operator", "a + * b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewExpression(tt.expr)
			assert.Error(t, err, "Expression parsing should fail")
		})
	}
}
