package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseExpression 测试表达式解析功能
func TestParseExpression(t *testing.T) {
	tests := []struct {
		name        string
		tokens      []string
		expectError bool
		description string
	}{
		{
			name:        "empty tokens",
			tokens:      []string{},
			expectError: true,
			description: "should return error for empty tokens",
		},
		{
			name:        "valid simple expression",
			tokens:      []string{"field1"},
			expectError: false,
			description: "should parse simple field",
		},
		{
			name:        "valid case expression",
			tokens:      []string{"CASE", "WHEN", "field1", "THEN", "1", "END"},
			expectError: false,
			description: "should parse CASE expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseExpression(tt.tokens)
			if tt.expectError && err == nil {
				t.Errorf("expected error but got none: %s", tt.description)
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestParseUnaryExpression(t *testing.T) {
	tests := []struct {
		name        string
		tokens      []string
		expectError bool
		description string
	}{
		{
			name:        "empty tokens",
			tokens:      []string{},
			expectError: true,
			description: "should return error for empty tokens",
		},
		{
			name:        "unary minus with number",
			tokens:      []string{"-", "5"},
			expectError: false,
			description: "should parse unary minus with number",
		},
		{
			name:        "unary minus with field",
			tokens:      []string{"-", "field1"},
			expectError: false,
			description: "should parse unary minus with field",
		},
		{
			name:        "unary minus with expression",
			tokens:      []string{"-", "(", "field1", "+", "field2", ")"},
			expectError: false,
			description: "should parse unary minus with expression",
		},
		{
			name:        "unary minus with function",
			tokens:      []string{"-", "sum", "(", "field1", ")"},
			expectError: false,
			description: "should parse unary minus with function",
		},
		{
			name:        "unary minus with string",
			tokens:      []string{"-", "'value'"},
			expectError: false,
			description: "should parse unary minus with string",
		},
		{
			name:        "unary minus with missing operand",
			tokens:      []string{"-"},
			expectError: true,
			description: "should return error for missing operand",
		},
		{
			name:        "nested unary minus",
			tokens:      []string{"-", "-", "5"},
			expectError: false,
			description: "should parse nested unary minus",
		},
		{
			name:        "unary minus with complex expression",
			tokens:      []string{"-", "field1", "*", "field2"},
			expectError: false,
			description: "should parse unary minus with complex expression",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseUnaryExpression(tt.tokens)
			if tt.expectError && err == nil {
				t.Errorf("expected error but got none: %s", tt.description)
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestParseExpressionWithPrecedence 测试运算符优先级解析
func TestParseExpressionWithPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		tokens   []string
		expected string // 用字符串表示预期的树结构
	}{
		{"加法和乘法", []string{"a", "+", "b", "*", "c"}, "(a + (b * c))"},
		{"乘法和除法", []string{"a", "*", "b", "/", "c"}, "((a * b) / c)"},
		{"幂运算", []string{"a", "^", "b", "^", "c"}, "(a ^ (b ^ c))"},
		{"混合运算", []string{"a", "+", "b", "*", "c", "^", "d"}, "(a + (b * (c ^ d)))"},
		{"比较运算符", []string{"a", "+", "b", ">", "c"}, "((a + b) > c)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseExpression(tt.tokens)
			require.NoError(t, err, "解析不应该失败")
			actual := nodeToString(result)
			assert.Equal(t, tt.expected, actual, "运算符优先级应该正确")
		})
	}
}

// TestParseFunction 测试函数解析
func TestParseFunction(t *testing.T) {
	tests := []struct {
		name     string
		tokens   []string
		pos      int
		expected *ExprNode
		newPos   int
		wantErr  bool
	}{
		{
			"无参数函数",
			[]string{"now", "(", ")"},
			0,
			&ExprNode{Type: TypeFunction, Value: "now", Args: []*ExprNode{}},
			3,
			false,
		},
		{
			"单参数函数",
			[]string{"abs", "(", "x", ")"},
			0,
			&ExprNode{
				Type:  TypeFunction,
				Value: "abs",
				Args:  []*ExprNode{{Type: TypeField, Value: "x"}},
			},
			4,
			false,
		},
		{
			"多参数函数",
			[]string{"max", "(", "a", ",", "b", ",", "c", ")"},
			0,
			&ExprNode{
				Type:  TypeFunction,
				Value: "max",
				Args: []*ExprNode{
					{Type: TypeField, Value: "a"},
					{Type: TypeField, Value: "b"},
					{Type: TypeField, Value: "c"},
				},
			},
			8,
			false,
		},
		{
			"嵌套函数",
			[]string{"sqrt", "(", "pow", "(", "x", ",", "2", ")", ")"},
			0,
			&ExprNode{
				Type:  TypeFunction,
				Value: "sqrt",
				Args: []*ExprNode{
					{
						Type:  TypeFunction,
						Value: "pow",
						Args: []*ExprNode{
							{Type: TypeField, Value: "x"},
							{Type: TypeNumber, Value: "2"},
						},
					},
				},
			},
			9,
			false,
		},
		// 错误情况
		{"缺少左括号", []string{"abs", "x", ")"}, 0, nil, 0, true},
		{"缺少右括号", []string{"abs", "(", "x"}, 0, nil, 0, true},
		{"参数分隔符错误", []string{"max", "(", "a", ";", "b", ")"}, 0, nil, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, newPos, err := parseFunction(tt.tokens, tt.pos)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "函数解析不应该失败")
				assert.Equal(t, tt.newPos, newPos, "位置应该正确")
				assertNodeEqual(t, tt.expected, result)
			}
		})
	}
}

// TestGetOperatorPrecedence 测试运算符优先级获取
func TestGetOperatorPrecedence(t *testing.T) {
	tests := []struct {
		op       string
		expected int
	}{
		{"OR", 1},
		{"AND", 2},
		{"NOT", 3},
		{"=", 4},
		{"==", 4},
		{"!=", 4},
		{"<>", 4},
		{"<", 4},
		{">", 4},
		{"<=", 4},
		{">=", 4},
		{"LIKE", 4},
		{"IS", 4},
		{"+", 5},
		{"-", 5},
		{"*", 6},
		{"/", 6},
		{"%", 6},
		{"^", 7},
		{"unknown", 0},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			result := getOperatorPrecedence(tt.op)
			assert.Equal(t, tt.expected, result, "运算符优先级应该正确")
		})
	}
}

// TestIsRightAssociative 测试右结合性判断
func TestIsRightAssociative(t *testing.T) {
	tests := []struct {
		op       string
		expected bool
	}{
		{"^", true},
		{"+", false},
		{"-", false},
		{"*", false},
		{"/", false},
		{"=", false},
		{"AND", false},
		{"OR", false},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			result := isRightAssociative(tt.op)
			assert.Equal(t, tt.expected, result, "右结合性判断应该正确")
		})
	}
}

// 辅助函数：连接字符串数组
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	if len(strs) == 1 {
		return strs[0]
	}

	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}
