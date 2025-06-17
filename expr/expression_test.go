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
		{"Complex Math", "sqrt(abs(a * b - c / d))", map[string]interface{}{"a": 10, "b": 2, "c": 5, "d": 1}, 3.872983346207417, false},

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

// TestCaseExpressionParsing 测试CASE表达式的解析功能
func TestCaseExpressionParsing(t *testing.T) {
	tests := []struct {
		name     string
		exprStr  string
		data     map[string]interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "简单的搜索CASE表达式",
			exprStr:  "CASE WHEN temperature > 30 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 35.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "简单CASE表达式 - 值匹配",
			exprStr:  "CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END",
			data:     map[string]interface{}{"status": "active"},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "CASE表达式 - ELSE分支",
			exprStr:  "CASE WHEN temperature > 50 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 25.5},
			expected: 0.0,
			wantErr:  false,
		},
		{
			name:     "复杂搜索CASE表达式",
			exprStr:  "CASE WHEN temperature > 30 THEN 'HOT' WHEN temperature > 20 THEN 'WARM' ELSE 'COLD' END",
			data:     map[string]interface{}{"temperature": 25.0},
			expected: 4.0, // 字符串"WARM"的长度
			wantErr:  false,
		},
		{
			name:     "数值比较的简单CASE",
			exprStr:  "CASE temperature WHEN 25 THEN 1 WHEN 30 THEN 2 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 30.0},
			expected: 2.0,
			wantErr:  false,
		},
		{
			name:     "布尔值CASE表达式",
			exprStr:  "CASE WHEN temperature > 25 AND humidity > 50 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 30.0, "humidity": 60.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "多条件CASE表达式_AND",
			exprStr:  "CASE WHEN temperature > 30 AND humidity < 60 THEN 1 WHEN temperature > 20 THEN 2 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 35.0, "humidity": 50.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "多条件CASE表达式_OR",
			exprStr:  "CASE WHEN temperature > 40 OR humidity > 80 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 25.0, "humidity": 85.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "函数调用在CASE中_ABS",
			exprStr:  "CASE WHEN ABS(temperature) > 30 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": -35.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "函数调用在CASE中_ROUND",
			exprStr:  "CASE WHEN ROUND(temperature) = 25 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 24.7},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "复杂条件组合",
			exprStr:  "CASE WHEN temperature > 30 AND (humidity > 60 OR pressure < 1000) THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 35.0, "humidity": 55.0, "pressure": 950.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "CASE中的算术表达式",
			exprStr:  "CASE WHEN temperature * 1.8 + 32 > 100 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 40.0}, // 40*1.8+32 = 104
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "字符串函数在CASE中",
			exprStr:  "CASE WHEN LENGTH(device_name) > 5 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"device_name": "sensor123"},
			expected: 1.0, // LENGTH函数正常工作，"sensor123"长度为9 > 5，返回1
			wantErr:  false,
		},
		{
			name:     "简单CASE与函数",
			exprStr:  "CASE ABS(temperature) WHEN 30 THEN 1 WHEN 25 THEN 2 ELSE 0 END",
			data:     map[string]interface{}{"temperature": -30.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "CASE结果中的函数",
			exprStr:  "CASE WHEN temperature > 30 THEN ABS(temperature) ELSE ROUND(temperature) END",
			data:     map[string]interface{}{"temperature": 35.5},
			expected: 35.5,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := NewExpression(tt.exprStr)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err, "Expression creation should not fail")
			assert.NotNil(t, expression, "Expression should not be nil")

			// 测试表达式计算
			result, err := expression.Evaluate(tt.data)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err, "Expression evaluation should not fail")
			assert.Equal(t, tt.expected, result, "Expression result should match expected value")
		})
	}
}

// TestCaseExpressionFieldExtraction 测试CASE表达式的字段提取功能
func TestCaseExpressionFieldExtraction(t *testing.T) {
	testCases := []struct {
		name           string
		exprStr        string
		expectedFields []string
	}{
		{
			name:           "简单CASE字段提取",
			exprStr:        "CASE WHEN temperature > 30 THEN 1 ELSE 0 END",
			expectedFields: []string{"temperature"},
		},
		{
			name:           "多字段CASE字段提取",
			exprStr:        "CASE WHEN temperature > 30 AND humidity < 60 THEN 1 ELSE 0 END",
			expectedFields: []string{"temperature", "humidity"},
		},
		{
			name:           "简单CASE字段提取",
			exprStr:        "CASE status WHEN 'active' THEN temperature ELSE humidity END",
			expectedFields: []string{"status", "temperature", "humidity"},
		},
		{
			name:           "函数CASE字段提取",
			exprStr:        "CASE WHEN ABS(temperature) > 30 THEN device_id ELSE location END",
			expectedFields: []string{"temperature", "device_id", "location"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expression, err := NewExpression(tc.exprStr)
			assert.NoError(t, err, "表达式创建应该成功")

			fields := expression.GetFields()

			// 验证所有期望的字段都被提取到了
			for _, expectedField := range tc.expectedFields {
				assert.Contains(t, fields, expectedField, "应该包含字段: %s", expectedField)
			}
		})
	}
}

// TestCaseExpressionWithNullComparisons 测试CASE表达式中的NULL比较
func TestCaseExpressionWithNullComparisons(t *testing.T) {
	tests := []struct {
		name     string
		exprStr  string
		data     map[string]interface{}
		expected interface{} // 使用interface{}以支持NULL值
		isNull   bool
	}{
		{
			name:     "NULL值在CASE条件中 - 应该走ELSE分支",
			exprStr:  "CASE WHEN temperature > 30 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": nil},
			expected: 0.0,
			isNull:   false,
		},
		{
			name:     "IS NULL条件 - 应该匹配",
			exprStr:  "CASE WHEN temperature IS NULL THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": nil},
			expected: 1.0,
			isNull:   false,
		},
		{
			name:     "IS NOT NULL条件 - 不应该匹配",
			exprStr:  "CASE WHEN temperature IS NOT NULL THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": nil},
			expected: 0.0,
			isNull:   false,
		},
		{
			name:     "CASE表达式返回NULL",
			exprStr:  "CASE WHEN temperature > 30 THEN temperature ELSE NULL END",
			data:     map[string]interface{}{"temperature": 25.0},
			expected: nil,
			isNull:   true,
		},
		{
			name:     "CASE表达式返回有效值",
			exprStr:  "CASE WHEN temperature > 30 THEN temperature ELSE NULL END",
			data:     map[string]interface{}{"temperature": 35.0},
			expected: 35.0,
			isNull:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := NewExpression(tt.exprStr)
			assert.NoError(t, err, "表达式解析应该成功")

			// 测试支持NULL的计算方法
			result, isNull, err := expression.EvaluateWithNull(tt.data)
			assert.NoError(t, err, "表达式计算应该成功")

			if tt.isNull {
				assert.True(t, isNull, "表达式应该返回NULL")
			} else {
				assert.False(t, isNull, "表达式不应该返回NULL")
				assert.Equal(t, tt.expected, result, "表达式结果应该匹配期望值")
			}
		})
	}
}

// TestNegativeNumberSupport 专门测试负数支持
func TestNegativeNumberSupport(t *testing.T) {
	tests := []struct {
		name     string
		exprStr  string
		data     map[string]interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "负数常量在THEN中",
			exprStr:  "CASE WHEN temperature > 0 THEN 1 ELSE -1 END",
			data:     map[string]interface{}{"temperature": -5.0},
			expected: -1.0,
			wantErr:  false,
		},
		{
			name:     "负数常量在WHEN中",
			exprStr:  "CASE WHEN temperature < -10 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": -15.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "负数小数",
			exprStr:  "CASE WHEN temperature > 0 THEN 1.5 ELSE -2.5 END",
			data:     map[string]interface{}{"temperature": -1.0},
			expected: -2.5,
			wantErr:  false,
		},
		{
			name:     "负数在算术表达式中",
			exprStr:  "CASE WHEN temperature + (-10) > 0 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 15.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "负数与函数",
			exprStr:  "CASE WHEN ABS(temperature) > 10 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": -15.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "负数在简单CASE中",
			exprStr:  "CASE temperature WHEN -10 THEN 1 WHEN -20 THEN 2 ELSE 0 END",
			data:     map[string]interface{}{"temperature": -10.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "负零",
			exprStr:  "CASE WHEN temperature = -0 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 0.0},
			expected: 1.0,
			wantErr:  false,
		},
		// 基本负数运算
		{
			name:     "直接负数",
			exprStr:  "-5",
			data:     map[string]interface{}{},
			expected: -5.0,
			wantErr:  false,
		},
		{
			name:     "负数加法",
			exprStr:  "-5 + 3",
			data:     map[string]interface{}{},
			expected: -2.0,
			wantErr:  false,
		},
		{
			name:     "负数乘法",
			exprStr:  "-3 * 4",
			data:     map[string]interface{}{},
			expected: -12.0,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := NewExpression(tt.exprStr)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err, "负数表达式解析应该成功")
			assert.NotNil(t, expression, "表达式不应为空")

			// 测试表达式计算
			result, err := expression.Evaluate(tt.data)
			assert.NoError(t, err, "负数表达式计算应该成功")
			assert.Equal(t, tt.expected, result, "负数表达式结果应该匹配期望值")
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
