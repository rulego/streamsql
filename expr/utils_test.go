package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUnquoteString Removes quotes from the test string
func TestUnquoteString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"单引号字符串", "'hello'", "hello"},
		{"双引号字符串", "\"world\"", "world"},
		{"空单引号字符串", "''", ""},
		{"空双引号字符串", "\"\"", ""},
		{"包含空格的字符串", "'hello world'", "hello world"},
		{"包含特殊字符的字符串", "'hello@#$%'", "hello@#$%"},
		{"无引号字符串", "hello", "hello"},
		{"只有左引号", "'hello", "'hello"},
		{"只有右引号", "hello'", "hello'"},
		{"引号不匹配", "'hello\"", "'hello\""},
		{"嵌套引号", "'he\"llo'", "he\"llo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := unquoteString(tt.input)
			assert.Equal(t, tt.expected, result, "去引号结果应该正确")
		})
	}
}

// TestUnquoteBacktick tests for the removal of backquotes
func TestUnquoteBacktick(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"反引号字段", "`field`", "field"},
		{"包含空格的反引号字段", "`field name`", "field name"},
		{"包含特殊字符的反引号字段", "`user.name`", "user.name"},
		{"空反引号", "``", ""},
		{"无反引号", "field", "field"},
		{"只有左反引号", "`field", "`field"},
		{"只有右反引号", "field`", "field`"},
		{"嵌套反引号", "`fie`ld`", "fie`ld"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := unquoteBacktick(tt.input)
			assert.Equal(t, tt.expected, result, "去反引号结果应该正确")
		})
	}
}

// TestFormatError test error formatting
func TestFormatError(t *testing.T) {
	tests := []struct {
		name     string
		message  string
		args     []any
		expected string
	}{
		{"简单错误", "invalid value", nil, "invalid value"},
		{"带参数的错误", "invalid value: %v", []any{"test"}, "invalid value: test"},
		{"多参数错误", "error at position %d: %s", []any{5, "syntax error"}, "error at position 5: syntax error"},
		{"数字参数", "value %d is out of range [%d, %d]", []any{10, 1, 5}, "value 10 is out of range [1, 5]"},
		{"浮点数参数", "result: %.2f", []any{3.14159}, "result: 3.14"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := formatError(tt.message, tt.args...)
			assert.Equal(t, tt.expected, err.Error(), "错误格式化结果应该正确")
		})
	}
}

// TestCopyNode Copies test nodes
func TestCopyNode(t *testing.T) {
	tests := []struct {
		name string
		node *ExprNode
	}{
		{
			"数字节点",
			&ExprNode{Type: TypeNumber, Value: "123"},
		},
		{
			"字段节点",
			&ExprNode{Type: TypeField, Value: "field1"},
		},
		{
			"运算符节点",
			&ExprNode{
				Type:  TypeOperator,
				Value: "+",
				Left:  &ExprNode{Type: TypeNumber, Value: "1"},
				Right: &ExprNode{Type: TypeNumber, Value: "2"},
			},
		},
		{
			"函数节点",
			&ExprNode{
				Type:  TypeFunction,
				Value: "abs",
				Args:  []*ExprNode{{Type: TypeNumber, Value: "1"}},
			},
		},
		{
			"CASE节点",
			&ExprNode{
				Type: TypeCase,
				CaseExpr: &CaseExpression{
					WhenClauses: []WhenClause{
						{
							Condition: &ExprNode{Type: TypeField, Value: "a"},
							Result:    &ExprNode{Type: TypeNumber, Value: "1"},
						},
					},
					ElseResult: &ExprNode{Type: TypeNumber, Value: "0"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copied := copyNode(tt.node)

			// Check that the replicated node is not the same object
			assert.NotSame(t, tt.node, copied, "复制的节点应该是不同的对象")

			// Check if the values are equal
			assertNodeEqual(t, tt.node, copied)

			// Modify the original node to ensure the copied node is not affected
			originalValue := tt.node.Value
			tt.node.Value = "modified"
			assert.NotEqual(t, tt.node.Value, copied.Value, "修改原节点不应该影响复制的节点")

			// Restore the original value
			tt.node.Value = originalValue
		})
	}
}

// TestCopyNode_Nil Test the null node copy
func TestCopyNode_Nil(t *testing.T) {
	result := copyNode(nil)
	assert.Nil(t, result, "复制nil节点应该返回nil")
}

// TestGetNodeType Tests to get the node type
func TestGetNodeType(t *testing.T) {
	tests := []struct {
		name     string
		node     *ExprNode
		expected string
	}{
		{"数字节点", &ExprNode{Type: TypeNumber}, "number"},
		{"字段节点", &ExprNode{Type: TypeField}, "field"},
		{"字符串节点", &ExprNode{Type: TypeString}, "string"},
		{"运算符节点", &ExprNode{Type: TypeOperator}, "operator"},
		{"函数节点", &ExprNode{Type: TypeFunction}, "function"},
		{"括号节点", &ExprNode{Type: TypeParenthesis}, "parenthesis"},
		{"CASE节点", &ExprNode{Type: TypeCase}, "case"},
		{"未知类型", &ExprNode{Type: "unknown"}, "unknown"},
		{"空节点", nil, "nil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getNodeType(tt.node)
			assert.Equal(t, tt.expected, result, "节点类型应该正确")
		})
	}
}

// TestGetNodeValue tests to obtain the node value
func TestGetNodeValue(t *testing.T) {
	tests := []struct {
		name     string
		node     *ExprNode
		expected string
	}{
		{"数字节点", &ExprNode{Value: "123"}, "123"},
		{"字段节点", &ExprNode{Value: "field1"}, "field1"},
		{"空值节点", &ExprNode{Value: ""}, ""},
		{"空节点", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getNodeValue(tt.node)
			assert.Equal(t, tt.expected, result, "节点值应该正确")
		})
	}
}

// TestSetNodeValue tests the node values
func TestSetNodeValue(t *testing.T) {
	tests := []struct {
		name     string
		node     *ExprNode
		newValue string
	}{
		{"设置数字节点值", &ExprNode{Type: TypeNumber, Value: "123"}, "456"},
		{"设置字段节点值", &ExprNode{Type: TypeField, Value: "field1"}, "field2"},
		{"设置空值", &ExprNode{Type: TypeString, Value: "hello"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setNodeValue(tt.node, tt.newValue)
			assert.Equal(t, tt.newValue, tt.node.Value, "节点值应该被正确设置")
		})
	}
}

// TestSetNodeValue_Nil Test setting of the null node value
func TestSetNodeValue_Nil(t *testing.T) {
	// This shouldn't be a panic
	assert.NotPanics(t, func() {
		setNodeValue(nil, "test")
	}, "设置nil节点值不应该panic")
}

// TestIsArithmeticOperator tests the arithmetic operator's judgment
func TestIsArithmeticOperator(t *testing.T) {
	tests := []struct {
		operator string
		expected bool
	}{
		{"+", true},
		{"-", true},
		{"*", true},
		{"/", true},
		{"%", true},
		{"^", true},
		{">", false},
		{"<", false},
		{"==", false},
		{"AND", false},
		{"OR", false},
		{"LIKE", false},
		{"unknown", false},
	}

	for _, tt := range tests {
		t.Run(tt.operator, func(t *testing.T) {
			result := isArithmeticOperator(tt.operator)
			assert.Equal(t, tt.expected, result, "算术运算符判断应该正确")
		})
	}
}

// TestIsLogicalOperator checks the logical operator
func TestIsLogicalOperator(t *testing.T) {
	tests := []struct {
		operator string
		expected bool
	}{
		{"AND", true},
		{"OR", true},
		{"NOT", true},
		{"+", false},
		{"-", false},
		{">", false},
		{"<", false},
		{"==", false},
		{"LIKE", false},
		{"unknown", false},
	}

	for _, tt := range tests {
		t.Run(tt.operator, func(t *testing.T) {
			result := isLogicalOperator(tt.operator)
			assert.Equal(t, tt.expected, result, "逻辑运算符判断应该正确")
		})
	}
}

// TestIsUnaryOperator tests the unary operator's check
func TestIsUnaryOperator(t *testing.T) {
	tests := []struct {
		operator string
		expected bool
	}{
		{"NOT", true},
		{"+", false},
		{"-", false},
		{"*", false},
		{"AND", false},
		{"OR", false},
		{"unknown", false},
	}

	for _, tt := range tests {
		t.Run(tt.operator, func(t *testing.T) {
			result := isUnaryOperator(tt.operator)
			assert.Equal(t, tt.expected, result, "一元运算符判断应该正确")
		})
	}
}

// TestIsKeyword to determine the test keyword
func TestIsKeyword(t *testing.T) {
	tests := []struct {
		word     string
		expected bool
	}{
		{"CASE", true},
		{"WHEN", true},
		{"THEN", true},
		{"ELSE", true},
		{"END", true},
		{"AND", true},
		{"OR", true},
		{"NOT", true},
		{"LIKE", true},
		{"IS", true},
		{"NULL", true},
		{"TRUE", true},
		{"FALSE", true},
		// Case Test
		{"case", true},
		{"Case", true},
		{"when", true},
		{"and", true},
		// Non-keywords
		{"field", false},
		{"value", false},
		{"123", false},
		{"unknown", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.word, func(t *testing.T) {
			result := isKeyword(tt.word)
			assert.Equal(t, tt.expected, result, "关键字判断应该正确")
		})
	}
}

// TestNormalizeIdentifier Standardizes the test identifier
func TestNormalizeIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"小写", "field", "field"},
		{"大写", "FIELD", "field"},
		{"混合大小写", "FieldName", "fieldname"},
		{"下划线", "field_name", "field_name"},
		{"数字", "field123", "field123"},
		{"空字符串", "", ""},
		{"单字符", "A", "a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeIdentifier(tt.input)
			assert.Equal(t, tt.expected, result, "标识符规范化应该正确")
		})
	}
}

// assertNodeEqual asserts that two expression nodes are equal (test auxiliary function)
func assertNodeEqual(t *testing.T, expected, actual *ExprNode) {
	if expected == nil && actual == nil {
		return
	}

	if expected == nil {
		assert.Fail(t, "Expected node is nil but actual is not")
		return
	}

	if actual == nil {
		assert.Fail(t, "Actual node is nil but expected is not")
		return
	}

	// Compare node types and values
	assert.Equal(t, expected.Type, actual.Type, "节点类型应该相等")
	assert.Equal(t, expected.Value, actual.Value, "节点值应该相等")

	// Recursively compare left and right child nodes
	assertNodeEqual(t, expected.Left, actual.Left)
	assertNodeEqual(t, expected.Right, actual.Right)

	// Compare function parameters
	assert.Equal(t, len(expected.Args), len(actual.Args), "函数参数数量应该相等")
	for i := range expected.Args {
		assertNodeEqual(t, expected.Args[i], actual.Args[i])
	}

	// Compare CASE expressions
	if expected.CaseExpr == nil && actual.CaseExpr == nil {
		return
	}

	if expected.CaseExpr == nil || actual.CaseExpr == nil {
		assert.Fail(t, "CASE表达式不匹配")
		return
	}

	assertNodeEqual(t, expected.CaseExpr.Value, actual.CaseExpr.Value)
	assertNodeEqual(t, expected.CaseExpr.ElseResult, actual.CaseExpr.ElseResult)

	assert.Equal(t, len(expected.CaseExpr.WhenClauses), len(actual.CaseExpr.WhenClauses), "WHEN子句数量应该相等")
	for i := range expected.CaseExpr.WhenClauses {
		assertNodeEqual(t, expected.CaseExpr.WhenClauses[i].Condition, actual.CaseExpr.WhenClauses[i].Condition)
		assertNodeEqual(t, expected.CaseExpr.WhenClauses[i].Result, actual.CaseExpr.WhenClauses[i].Result)
	}
}

func TestNodeToString(t *testing.T) {
	tests := []struct {
		name     string
		node     *ExprNode
		expected string
	}{
		{
			name:     "nil node",
			node:     nil,
			expected: "<nil>",
		},
		{
			name: "number node",
			node: &ExprNode{
				Type:  TypeNumber,
				Value: "123",
			},
			expected: "123",
		},
		{
			name: "string node",
			node: &ExprNode{
				Type:  TypeString,
				Value: "'hello'",
			},
			expected: "'hello'",
		},
		{
			name: "field node",
			node: &ExprNode{
				Type:  TypeField,
				Value: "field1",
			},
			expected: "field1",
		},
		{
			name: "operator node",
			node: &ExprNode{
				Type:  TypeOperator,
				Value: "+",
				Left: &ExprNode{
					Type:  TypeField,
					Value: "a",
				},
				Right: &ExprNode{
					Type:  TypeField,
					Value: "b",
				},
			},
			expected: "(a + b)",
		},
		{
			name: "function node",
			node: &ExprNode{
				Type:  TypeFunction,
				Value: "sum",
				Args: []*ExprNode{
					{Type: TypeField, Value: "field1"},
					{Type: TypeField, Value: "field2"},
				},
			},
			expected: "sum(field1, field2)",
		},
		{
			name: "case node",
			node: &ExprNode{
				Type: TypeCase,
				CaseExpr: &CaseExpression{
					Value: &ExprNode{Type: TypeField, Value: "status"},
					WhenClauses: []WhenClause{
						{
							Condition: &ExprNode{Type: TypeString, Value: "'active'"},
							Result:    &ExprNode{Type: TypeNumber, Value: "1"},
						},
					},
					ElseResult: &ExprNode{Type: TypeNumber, Value: "0"},
				},
			},
			expected: "CASE status WHEN 'active' THEN 1 ELSE 0 END",
		},
		{
			name: "unknown type",
			node: &ExprNode{
				Type:  "unknown",
				Value: "value",
			},
			expected: "<unknown:value>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := nodeToString(tt.node)
			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestCaseExprToString(t *testing.T) {
	tests := []struct {
		name     string
		caseExpr *CaseExpression
		expected string
	}{
		{
			name:     "nil case expression",
			caseExpr: nil,
			expected: "<nil case>",
		},
		{
			name: "simple case expression",
			caseExpr: &CaseExpression{
				Value: &ExprNode{Type: TypeField, Value: "status"},
				WhenClauses: []WhenClause{
					{
						Condition: &ExprNode{Type: TypeString, Value: "'active'"},
						Result:    &ExprNode{Type: TypeNumber, Value: "1"},
					},
					{
						Condition: &ExprNode{Type: TypeString, Value: "'inactive'"},
						Result:    &ExprNode{Type: TypeNumber, Value: "0"},
					},
				},
				ElseResult: &ExprNode{Type: TypeNumber, Value: "-1"},
			},
			expected: "CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END",
		},
		{
			name: "search case expression",
			caseExpr: &CaseExpression{
				WhenClauses: []WhenClause{
					{
						Condition: &ExprNode{Type: TypeOperator, Value: ">", Left: &ExprNode{Type: TypeField, Value: "age"}, Right: &ExprNode{Type: TypeNumber, Value: "18"}},
						Result:    &ExprNode{Type: TypeString, Value: "'adult'"},
					},
					{
						Condition: &ExprNode{Type: TypeOperator, Value: ">", Left: &ExprNode{Type: TypeField, Value: "age"}, Right: &ExprNode{Type: TypeNumber, Value: "12"}},
						Result:    &ExprNode{Type: TypeString, Value: "'teen'"},
					},
				},
				ElseResult: &ExprNode{Type: TypeString, Value: "'child'"},
			},
			expected: "CASE WHEN (age > 18) THEN 'adult' WHEN (age > 12) THEN 'teen' ELSE 'child' END",
		},
		{
			name: "case expression without else",
			caseExpr: &CaseExpression{
				Value: &ExprNode{Type: TypeField, Value: "type"},
				WhenClauses: []WhenClause{
					{
						Condition: &ExprNode{Type: TypeString, Value: "'A'"},
						Result:    &ExprNode{Type: TypeNumber, Value: "1"},
					},
				},
			},
			expected: "CASE type WHEN 'A' THEN 1 END",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := caseExprToString(tt.caseExpr)
			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}
