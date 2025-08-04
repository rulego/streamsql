package expr

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpressionEvaluation(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		data     map[string]interface{}
		expected float64
		hasError bool
	}{
		// Basic arithmetic tests
		{"Simple Addition", "a + b", map[string]interface{}{"a": 5, "b": 3}, 8, false},
		{"Simple Subtraction", "a - b", map[string]interface{}{"a": 5, "b": 3}, 2, false},
		{"Simple Multiplication", "a * b", map[string]interface{}{"a": 5, "b": 3}, 15, false},
		{"Simple Division", "a / b", map[string]interface{}{"a": 6, "b": 3}, 2, false},
		{"Modulo", "a % b", map[string]interface{}{"a": 7, "b": 4}, 3, false},
		{"Power", "a ^ b", map[string]interface{}{"a": 2, "b": 3}, 8, false},

		// Compound expression tests
		{"Complex Expression", "a + b * c", map[string]interface{}{"a": 5, "b": 3, "c": 2}, 11, false},
		{"Complex Expression With Parentheses", "(a + b) * c", map[string]interface{}{"a": 5, "b": 3, "c": 2}, 16, false},
		{"Multiple Operations", "a + b * c - d / e", map[string]interface{}{"a": 5, "b": 3, "c": 2, "d": 8, "e": 4}, 9, false},

		// Function call tests
		{"Abs Function", "abs(a - b)", map[string]interface{}{"a": 3, "b": 5}, 2, false},
		{"Sqrt Function", "sqrt(a)", map[string]interface{}{"a": 16}, 4, false},
		{"Round Function", "round(a)", map[string]interface{}{"a": 3.7}, 4, false},

		// Conversion tests
		{"String to Number", "a + b", map[string]interface{}{"a": "5", "b": 3}, 8, false},

		// Complex expression tests
		{"Temperature Conversion", "temperature * 1.8 + 32", map[string]interface{}{"temperature": 25}, 77, false},
		{"Complex Math", "sqrt(abs(a * b - c / d))", map[string]interface{}{"a": 10, "b": 2, "c": 5, "d": 1}, 3.872983346207417, false},

		// Error tests
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

// TestCaseExpressionParsing tests CASE expression parsing functionality
func TestCaseExpressionParsing(t *testing.T) {
	tests := []struct {
		name     string
		exprStr  string
		data     map[string]interface{}
		expected float64
		wantErr  bool
	}{
		{
			name:     "Simple search CASE expression",
			exprStr:  "CASE WHEN temperature > 30 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 35.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "Simple CASE expression - value matching",
			exprStr:  "CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END",
			data:     map[string]interface{}{"status": "active"},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "CASE expression - ELSE branch",
			exprStr:  "CASE WHEN temperature > 50 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 25.5},
			expected: 0.0,
			wantErr:  false,
		},
		{
			name:     "Complex search CASE expression",
			exprStr:  "CASE WHEN temperature > 30 THEN 'HOT' WHEN temperature > 20 THEN 'WARM' ELSE 'COLD' END",
			data:     map[string]interface{}{"temperature": 25.0},
			expected: 4.0, // Length of string "WARM"
			wantErr:  false,
		},
		{
			name:     "Simple CASE with numeric comparison",
			exprStr:  "CASE temperature WHEN 25 THEN 1 WHEN 30 THEN 2 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 30.0},
			expected: 2.0,
			wantErr:  false,
		},
		{
			name:     "Boolean CASE expression",
			exprStr:  "CASE WHEN temperature > 25 AND humidity > 50 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 30.0, "humidity": 60.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "Multi-condition CASE expression with AND",
			exprStr:  "CASE WHEN temperature > 30 AND humidity < 60 THEN 1 WHEN temperature > 20 THEN 2 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 35.0, "humidity": 50.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "Multi-condition CASE expression with OR",
			exprStr:  "CASE WHEN temperature > 40 OR humidity > 80 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 25.0, "humidity": 85.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "Function call in CASE - ABS",
			exprStr:  "CASE WHEN ABS(temperature) > 30 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": -35.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "Function call in CASE - ROUND",
			exprStr:  "CASE WHEN ROUND(temperature) = 25 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 24.7},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "Complex condition combination",
			exprStr:  "CASE WHEN temperature > 30 AND (humidity > 60 OR pressure < 1000) THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 35.0, "humidity": 55.0, "pressure": 950.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "Arithmetic expression in CASE",
			exprStr:  "CASE WHEN temperature * 1.8 + 32 > 100 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"temperature": 40.0}, // 40*1.8+32 = 104
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "String function in CASE",
			exprStr:  "CASE WHEN LENGTH(device_name) > 5 THEN 1 ELSE 0 END",
			data:     map[string]interface{}{"device_name": "sensor123"},
			expected: 1.0, // LENGTH function works normally, "sensor123" length is 9 > 5, returns 1
			wantErr:  false,
		},
		{
			name:     "Simple CASE with function",
			exprStr:  "CASE ABS(temperature) WHEN 30 THEN 1 WHEN 25 THEN 2 ELSE 0 END",
			data:     map[string]interface{}{"temperature": -30.0},
			expected: 1.0,
			wantErr:  false,
		},
		{
			name:     "Function in CASE result",
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

			// Test expression evaluation
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

// TestExpressionTokenization 测试表达式分词功能
func TestExpressionTokenization(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		expected []string
	}{
		{"Simple Expression", "a + b", []string{"a", "+", "b"}},
		{"With Numbers", "a + 123", []string{"a", "+", "123"}},
		{"With Parentheses", "(a + b) * c", []string{"(", "a", "+", "b", ")", "*", "c"}},
		{"With Functions", "abs(a)", []string{"abs", "(", "a", ")"}},
		{"With Decimals", "a + 3.14", []string{"a", "+", "3.14"}},
		{"With Negative Numbers", "-5 + a", []string{"-5", "+", "a"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, err := tokenize(tt.expr)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, tokens, "Tokenization should match expected")
		})
	}
}

// TestExpressionValidation 测试表达式验证功能
func TestExpressionValidation(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		valid   bool
		errorMsg string
	}{
		{"Valid Simple Expression", "a + b", true, ""},
		{"Valid Complex Expression", "(a + b) * c / d", true, ""},
		{"Invalid Empty Expression", "", false, "empty expression"},
		{"Invalid Mismatched Parentheses", "(a + b", false, "mismatched parentheses"},
		{"Invalid Double Operator", "a + + b", false, "consecutive operators"},
		{"Invalid Starting Operator", "+ a", false, "expression cannot start with operator"},
		{"Invalid Ending Operator", "a +", false, "expression cannot end with operator"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBasicSyntax(tt.expr)
			if tt.valid {
				assert.NoError(t, err, "Expression should be valid")
			} else {
				assert.Error(t, err, "Expression should be invalid")
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg, "Error message should contain expected text")
				}
			}
		})
	}
}

// TestExpressionOperatorPrecedence 测试运算符优先级
func TestExpressionOperatorPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		data     map[string]interface{}
		expected float64
	}{
		{"Addition and Multiplication", "2 + 3 * 4", map[string]interface{}{}, 14}, // 2 + (3 * 4) = 14
		{"Subtraction and Division", "10 - 8 / 2", map[string]interface{}{}, 6},   // 10 - (8 / 2) = 6
		{"Power and Multiplication", "2 * 3 ^ 2", map[string]interface{}{}, 18},   // 2 * (3 ^ 2) = 18
		{"Parentheses Override", "(2 + 3) * 4", map[string]interface{}{}, 20},     // (2 + 3) * 4 = 20
		{"Complex Expression", "2 + 3 * 4 - 5 / 2", map[string]interface{}{}, 11.5}, // 2 + (3 * 4) - (5 / 2) = 11.5
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.expr)
			require.NoError(t, err, "Expression parsing should not fail")

			result, err := expr.Evaluate(tt.data)
			require.NoError(t, err, "Expression evaluation should not fail")
			assert.InDelta(t, tt.expected, result, 0.001, "Result should match expected value")
		})
	}
}

// TestExpressionFunctions 测试内置函数
func TestExpressionFunctions(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		data     map[string]interface{}
		expected float64
		wantErr  bool
	}{
		{"ABS Positive", "abs(5)", map[string]interface{}{}, 5, false},
		{"ABS Negative", "abs(-5)", map[string]interface{}{}, 5, false},
		{"ABS Zero", "abs(0)", map[string]interface{}{}, 0, false},
		{"SQRT Valid", "sqrt(16)", map[string]interface{}{}, 4, false},
		{"SQRT Zero", "sqrt(0)", map[string]interface{}{}, 0, false},
		{"SQRT Negative", "sqrt(-1)", map[string]interface{}{}, 0, true},
		{"ROUND Positive", "round(3.7)", map[string]interface{}{}, 4, false},
		{"ROUND Negative", "round(-3.7)", map[string]interface{}{}, -4, false},
		{"ROUND Half", "round(3.5)", map[string]interface{}{}, 4, false},
		{"FLOOR Positive", "floor(3.7)", map[string]interface{}{}, 3, false},
		{"FLOOR Negative", "floor(-3.7)", map[string]interface{}{}, -4, false},
		{"CEIL Positive", "ceil(3.2)", map[string]interface{}{}, 4, false},
		{"CEIL Negative", "ceil(-3.2)", map[string]interface{}{}, -3, false},
		{"MAX Two Values", "max(5, 3)", map[string]interface{}{}, 5, false},
		{"MIN Two Values", "min(5, 3)", map[string]interface{}{}, 3, false},
		{"POW Function", "pow(2, 3)", map[string]interface{}{}, 8, false},
		{"LOG Function", "log(10)", map[string]interface{}{}, math.Log(10), false},
		{"LOG10 Function", "log10(100)", map[string]interface{}{}, 2, false},
		{"EXP Function", "exp(1)", map[string]interface{}{}, math.E, false},
		{"SIN Function", "sin(0)", map[string]interface{}{}, 0, false},
		{"COS Function", "cos(0)", map[string]interface{}{}, 1, false},
		{"TAN Function", "tan(0)", map[string]interface{}{}, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.expr)
			require.NoError(t, err, "Expression parsing should not fail")

			result, err := expr.Evaluate(tt.data)
			if tt.wantErr {
				assert.Error(t, err, "Expected error")
			} else {
				require.NoError(t, err, "Expression evaluation should not fail")
				assert.InDelta(t, tt.expected, result, 0.001, "Result should match expected value")
			}
		})
	}
}

// TestExpressionDataTypeConversion 测试数据类型转换
func TestExpressionDataTypeConversion(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		data     map[string]interface{}
		expected float64
		wantErr  bool
	}{
		{"String to Number", "a + 5", map[string]interface{}{"a": "10"}, 15, false},
		{"Integer to Float", "a + 3.5", map[string]interface{}{"a": 5}, 8.5, false},
		{"Float to Float", "a + b", map[string]interface{}{"a": 3.14, "b": 2.86}, 6.0, false},
		{"Boolean True", "a + 1", map[string]interface{}{"a": true}, 2, false},
		{"Boolean False", "a + 1", map[string]interface{}{"a": false}, 1, false},
		{"Invalid String", "a + 5", map[string]interface{}{"a": "invalid"}, 0, true},
		{"Nil Value", "a + 5", map[string]interface{}{"a": nil}, 0, true},
		{"Complex Type", "a + 5", map[string]interface{}{"a": map[string]interface{}{}}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.expr)
			require.NoError(t, err, "Expression parsing should not fail")

			result, err := expr.Evaluate(tt.data)
			if tt.wantErr {
				assert.Error(t, err, "Expected error")
			} else {
				require.NoError(t, err, "Expression evaluation should not fail")
				assert.InDelta(t, tt.expected, result, 0.001, "Result should match expected value")
			}
		})
	}
}

// TestExpressionEdgeCases 测试边界情况
func TestExpressionEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		data     map[string]interface{}
		expected float64
		wantErr  bool
	}{
		{"Very Large Number", "a + 1", map[string]interface{}{"a": 1e308}, 1e308 + 1, false},
		{"Very Small Number", "a + 1", map[string]interface{}{"a": 1e-308}, 1, false},
		{"Infinity", "a + 1", map[string]interface{}{"a": math.Inf(1)}, math.Inf(1), false},
		{"Negative Infinity", "a + 1", map[string]interface{}{"a": math.Inf(-1)}, math.Inf(-1), false},
		{"NaN", "a + 1", map[string]interface{}{"a": math.NaN()}, 0, true},
		{"Division by Zero", "5 / 0", map[string]interface{}{}, 0, true},
		{"Modulo by Zero", "5 % 0", map[string]interface{}{}, 0, true},
		{"Zero Power Zero", "0 ^ 0", map[string]interface{}{}, 1, false}, // 0^0 = 1 by convention
		{"Negative Power", "2 ^ -3", map[string]interface{}{}, 0.125, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.expr)
			require.NoError(t, err, "Expression parsing should not fail")

			result, err := expr.Evaluate(tt.data)
			if tt.wantErr {
				assert.Error(t, err, "Expected error")
			} else {
				require.NoError(t, err, "Expression evaluation should not fail")
				if math.IsInf(tt.expected, 0) {
					assert.True(t, math.IsInf(result, 0), "Result should be infinity")
				} else {
					assert.InDelta(t, tt.expected, result, 0.001, "Result should match expected value")
				}
			}
		})
	}
}

// TestExpressionConcurrency 测试并发安全性
func TestExpressionConcurrency(t *testing.T) {
	expr, err := NewExpression("a + b * c")
	require.NoError(t, err, "Expression parsing should not fail")

	// 并发执行多个计算
	const numGoroutines = 100
	results := make(chan float64, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(index int) {
			data := map[string]interface{}{
				"a": float64(index),
				"b": float64(index * 2),
				"c": float64(index * 3),
			}
			result, err := expr.Evaluate(data)
			assert.NoError(t, err, "Concurrent evaluation should not fail")
			results <- result
		}(i)
	}

	// 收集结果
	for i := 0; i < numGoroutines; i++ {
		result := <-results
		// 验证结果是合理的（非零且非NaN）
		assert.False(t, math.IsNaN(result), "Result should not be NaN")
		assert.True(t, result >= 0, "Result should be non-negative for this test")
	}
}

// TestExpressionComplexNesting 测试复杂嵌套表达式
func TestExpressionComplexNesting(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		data     map[string]interface{}
		expected float64
	}{
		{
			"Deeply Nested Parentheses",
			"((a + b) * (c - d)) / ((e + f) * (g - h))",
			map[string]interface{}{"a": 1, "b": 2, "c": 5, "d": 3, "e": 2, "f": 3, "g": 7, "h": 2},
			0.24, // ((1+2)*(5-3))/((2+3)*(7-2)) = (3*2)/(5*5) = 6/25 = 0.24
		},
		{
			"Nested Functions",
			"sqrt(abs(a - b) + pow(c, 2))",
			map[string]interface{}{"a": 3, "b": 7, "c": 3},
			3.606, // sqrt(abs(3-7) + pow(3,2)) = sqrt(4 + 9) = sqrt(13) ≈ 3.606
		},
		{
			"Mixed Operations",
			"a * b + c / d - e % f + pow(g, h)",
			map[string]interface{}{"a": 2, "b": 3, "c": 8, "d": 2, "e": 7, "f": 3, "g": 2, "h": 3},
			17, // 2*3 + 8/2 - 7%3 + pow(2,3) = 6 + 4 - 1 + 8 = 17
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.expr)
			require.NoError(t, err, "Expression parsing should not fail")

			result, err := expr.Evaluate(tt.data)
			require.NoError(t, err, "Expression evaluation should not fail")
			assert.InDelta(t, tt.expected, result, 0.1, "Result should match expected value")
		})
	}
}

// TestExpressionStringHandling 测试字符串处理
func TestExpressionStringHandling(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		data     map[string]interface{}
		expected float64
		wantErr  bool
	}{
		{"String Length", "len(name)", map[string]interface{}{"name": "hello"}, 5, false},
		{"Empty String Length", "len(name)", map[string]interface{}{"name": ""}, 0, false},
		{"String Comparison Equal", "name == 'test'", map[string]interface{}{"name": "test"}, 1, false},
		{"String Comparison Not Equal", "name != 'test'", map[string]interface{}{"name": "hello"}, 1, false},
		{"String to Number Conversion", "val + 10", map[string]interface{}{"val": "5"}, 15, false},
		{"Invalid String to Number", "val + 10", map[string]interface{}{"val": "abc"}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := NewExpression(tt.expr)
			require.NoError(t, err, "Expression parsing should not fail")

			result, err := expr.Evaluate(tt.data)
			if tt.wantErr {
				assert.Error(t, err, "Expected error")
			} else {
				require.NoError(t, err, "Expression evaluation should not fail")
				assert.InDelta(t, tt.expected, result, 0.001, "Result should match expected value")
			}
		})
	}
}

// TestExpressionPerformance 测试表达式性能
func TestExpressionPerformance(t *testing.T) {
	// 创建一个复杂表达式
	expr, err := NewExpression("sqrt(pow(a, 2) + pow(b, 2)) + abs(c - d) * (e + f) / (g + 1)")
	require.NoError(t, err, "Expression parsing should not fail")

	data := map[string]interface{}{
		"a": 3.0, "b": 4.0, "c": 10.0, "d": 7.0, "e": 2.0, "f": 3.0, "g": 4.0,
	}

	// 执行多次计算以测试性能
	const iterations = 10000
	for i := 0; i < iterations; i++ {
		_, err := expr.Evaluate(data)
		assert.NoError(t, err, "Performance test evaluation should not fail")
	}
}

// TestExpressionMemoryUsage 测试内存使用
func TestExpressionMemoryUsage(t *testing.T) {
	// 创建多个表达式实例
	const numExpressions = 1000
	expressions := make([]*Expression, numExpressions)

	for i := 0; i < numExpressions; i++ {
		expr, err := NewExpression("a + b * c")
		require.NoError(t, err, "Expression creation should not fail")
		expressions[i] = expr
	}

	// 验证所有表达式都能正常工作
	data := map[string]interface{}{"a": 1, "b": 2, "c": 3}
	for i, expr := range expressions {
		result, err := expr.Evaluate(data)
		assert.NoError(t, err, "Expression %d evaluation should not fail", i)
		assert.Equal(t, 7.0, result, "Expression %d result should be correct", i)
	}
}
