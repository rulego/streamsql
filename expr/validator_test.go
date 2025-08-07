package expr

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestValidateExpressionNode 测试表达式节点验证功能
func TestValidateExpressionNode(t *testing.T) {
	tests := []struct {
		name    string
		node    *ExprNode
		wantErr bool
	}{
		{
			"有效数字节点",
			&ExprNode{Type: TypeNumber, Value: "123"},
			false,
		},
		{
			"有效字段节点",
			&ExprNode{Type: TypeField, Value: "field1"},
			false,
		},
		{
			"有效字符串节点",
			&ExprNode{Type: TypeString, Value: "'hello'"},
			false,
		},
		{
			"有效运算符节点",
			&ExprNode{
				Type:  TypeOperator,
				Value: "+",
				Left:  &ExprNode{Type: TypeNumber, Value: "1"},
				Right: &ExprNode{Type: TypeNumber, Value: "2"},
			},
			false,
		},
		{
			"有效函数节点",
			&ExprNode{
				Type:  TypeFunction,
				Value: "abs",
				Args:  []*ExprNode{{Type: TypeNumber, Value: "1"}},
			},
			false,
		},
		{
			"有效CASE节点",
			&ExprNode{
				Type: TypeCase,
				CaseExpr: &CaseExpression{
					WhenClauses: []WhenClause{
						{
							Condition: &ExprNode{
								Type:  TypeOperator,
								Value: ">",
								Left:  &ExprNode{Type: TypeField, Value: "a"},
								Right: &ExprNode{Type: TypeNumber, Value: "0"},
							},
							Result: &ExprNode{Type: TypeNumber, Value: "1"},
						},
					},
					ElseResult: &ExprNode{Type: TypeNumber, Value: "0"},
				},
			},
			false,
		},
		// 错误情况
		{"空节点", nil, true},
		{"无效数字", &ExprNode{Type: TypeNumber, Value: "abc"}, true},
		{"无效字段名", &ExprNode{Type: TypeField, Value: "123field"}, true},
		{"无效字符串", &ExprNode{Type: TypeString, Value: "hello"}, true},
		{"运算符缺少左操作数", &ExprNode{
			Type:  TypeOperator,
			Value: "+",
			Right: &ExprNode{Type: TypeNumber, Value: "1"},
		}, true},
		{"运算符缺少右操作数", &ExprNode{
			Type:  TypeOperator,
			Value: "+",
			Left:  &ExprNode{Type: TypeNumber, Value: "1"},
		}, true},
		{"无效运算符", &ExprNode{
			Type:  TypeOperator,
			Value: "@",
			Left:  &ExprNode{Type: TypeNumber, Value: "1"},
			Right: &ExprNode{Type: TypeNumber, Value: "2"},
		}, true},
		{"无效函数", &ExprNode{
			Type:  TypeFunction,
			Value: "unknown",
			Args:  []*ExprNode{{Type: TypeNumber, Value: "1"}},
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateExpression(tt.node)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "验证不应该失败")
			}
		})
	}
}

// TestValidateExpression 测试公共表达式验证接口
func TestValidateExpression(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		// 有效表达式
		{"简单数字", "123", false},
		{"简单字段", "field1", false},
		{"算术表达式", "1 + 2", false},
		{"比较表达式", "field1 > 10", false},
		{"函数调用", "abs(-5)", false},
		{"复杂表达式", "(field1 + field2) * 2", false},
		{"字符串比较", "name = 'test'", false},
		{"CASE表达式", "CASE WHEN field1 > 0 THEN 1 ELSE 0 END", false},
		{"逻辑表达式", "field1 > 0 AND field2 < 100", false},
		{"嵌套函数", "max(abs(field1), abs(field2))", false},

		// 无效表达式
		{"空表达式", "", true},
		{"只有空格", "   ", true},
		{"括号不匹配1", "(1 + 2", true},
		{"括号不匹配2", "1 + 2)", true},
		{"连续运算符", "1 + + 2", true},
		{"运算符开头", "+ 1 + 2", true},
		{"运算符结尾", "1 + 2 +", true},
		{"空括号", "()", true},
		{"无效数字", "12.34.56", true},
		{"无效字符串", "'unclosed string", true},
		{"无效函数", "unknown_func(1)", true},
		{"无效字段名", "123field", true},
		{"tokenize错误", "field with invalid 'quote", true},
		{"解析错误", "(((", true},
		{"AST验证错误", "123abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateExpression(tt.expr)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "验证不应该失败")
			}
		})
	}
}

// TestValidateNumberNode 测试数字节点验证
func TestValidateNumberNode(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"正整数", "123", false},
		{"负整数", "-123", false},
		{"零", "0", false},
		{"正小数", "3.14", false},
		{"负小数", "-3.14", false},
		{"科学计数法", "1.5e10", false},
		{"负科学计数法", "-1.5e-3", false},
		{"小数点开头", ".5", false},
		{"小数点结尾", "5.", false},
		// 错误情况
		{"空字符串", "", true},
		{"字母", "abc", true},
		{"多个小数点", "3.14.15", true},
		{"多个负号", "--5", true},
		{"负号在中间", "3-5", true},
		{"只有小数点", ".", true},
		{"只有负号", "-", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &ExprNode{Type: TypeNumber, Value: tt.value}
			err := validateNumberNode(node)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "数字验证不应该失败")
			}
		})
	}
}

// TestValidateStringNode 测试字符串节点验证
func TestValidateStringNode(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"单引号字符串", "'hello'", false},
		{"双引号字符串", "\"world\"", false},
		{"空字符串", "''", false},
		{"空双引号字符串", "\"\"", false},
		{"包含转义的字符串", "'hello\\world'", false},
		{"包含单引号的双引号字符串", "\"hello'world\"", false},
		{"包含双引号的单引号字符串", "'hello\"world'", false},
		// 错误情况
		{"未闭合单引号", "'hello", true},
		{"未闭合双引号", "\"hello", true},
		{"没有引号", "hello", true},
		{"空值", "", true},
		{"只有单引号", "'", true},
		{"只有双引号", "\"", true},
		{"引号不匹配", "'hello\"", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &ExprNode{Type: TypeString, Value: tt.value}
			err := validateStringNode(node)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "字符串验证不应该失败")
			}
		})
	}
}

// TestValidateFieldNode 测试字段节点验证
func TestValidateFieldNode(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"简单字段名", "field1", false},
		{"下划线开头", "_field", false},
		{"包含数字", "field123", false},
		{"驼峰命名", "fieldName", false},
		{"蛇形命名", "field_name", false},
		{"大写字段", "FIELD", false},
		{"反引号字段", "`field name`", false},
		{"反引号包含特殊字符", "`user.name`", false},
		{"反引号包含空格", "`user name`", false},
		// 错误情况
		{"空字段名", "", true},
		{"数字开头", "123field", true},
		{"包含特殊字符", "field-name", true},
		{"包含空格", "field name", true},
		{"包含点号", "field.name", true},
		{"未闭合反引号", "`field", true},
		{"只有反引号", "`", true},
		{"空反引号", "``", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &ExprNode{Type: TypeField, Value: tt.value}
			err := validateFieldNode(node)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "字段验证不应该失败")
			}
		})
	}
}

// TestValidateOperatorNode 测试运算符节点验证
func TestValidateOperatorNode(t *testing.T) {
	tests := []struct {
		name     string
		operator string
		left     *ExprNode
		right    *ExprNode
		wantErr  bool
	}{
		{
			"有效加法",
			"+",
			&ExprNode{Type: TypeNumber, Value: "1"},
			&ExprNode{Type: TypeNumber, Value: "2"},
			false,
		},
		{
			"有效比较",
			">",
			&ExprNode{Type: TypeField, Value: "a"},
			&ExprNode{Type: TypeNumber, Value: "0"},
			false,
		},
		{
			"有效逻辑运算",
			"AND",
			&ExprNode{
				Type:  TypeOperator,
				Value: ">",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeNumber, Value: "0"},
			},
			&ExprNode{
				Type:  TypeOperator,
				Value: "<",
				Left:  &ExprNode{Type: TypeField, Value: "b"},
				Right: &ExprNode{Type: TypeNumber, Value: "10"},
			},
			false,
		},
		{
			"有效NOT运算（单操作数）",
			"NOT",
			&ExprNode{
				Type:  TypeOperator,
				Value: ">",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeNumber, Value: "0"},
			},
			nil,
			false,
		},
		// 错误情况
		{"无效运算符", "@", &ExprNode{Type: TypeNumber, Value: "1"}, &ExprNode{Type: TypeNumber, Value: "2"}, true},
		{"缺少左操作数", "+", nil, &ExprNode{Type: TypeNumber, Value: "2"}, true},
		{"缺少右操作数（双操作数运算符）", "+", &ExprNode{Type: TypeNumber, Value: "1"}, nil, true},
		{"NOT运算符有右操作数", "NOT", &ExprNode{Type: TypeNumber, Value: "1"}, &ExprNode{Type: TypeNumber, Value: "2"}, true},
		{"左操作数验证失败", "+", &ExprNode{Type: TypeNumber, Value: "abc"}, &ExprNode{Type: TypeNumber, Value: "2"}, true},
		{"右操作数验证失败", "+", &ExprNode{Type: TypeNumber, Value: "1"}, &ExprNode{Type: TypeNumber, Value: "abc"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &ExprNode{
				Type:  TypeOperator,
				Value: tt.operator,
				Left:  tt.left,
				Right: tt.right,
			}
			err := validateOperatorNode(node)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "运算符验证不应该失败")
			}
		})
	}
}

// TestValidateFunctionNode 测试函数节点验证
func TestValidateFunctionNode(t *testing.T) {
	// 测试函数Value为空的情况
	t.Run("函数名为空", func(t *testing.T) {
		node := &ExprNode{
			Type:  TypeFunction,
			Value: "",
			Args:  []*ExprNode{{Type: TypeNumber, Value: "1"}},
		}
		err := validateFunctionNode(node)
		assert.Error(t, err, "函数名为空时应该返回错误")
		assert.Contains(t, err.Error(), "function node has empty value")
	})

	tests := []struct {
		name     string
		funcName string
		args     []*ExprNode
		wantErr  bool
	}{
		{
			"ABS函数",
			"abs",
			[]*ExprNode{{Type: TypeNumber, Value: "1"}},
			false,
		},
		{
			"MAX函数",
			"max",
			[]*ExprNode{
				{Type: TypeNumber, Value: "1"},
				{Type: TypeNumber, Value: "2"},
				{Type: TypeNumber, Value: "3"},
			},
			false,
		},
		{
			"POW函数",
			"pow",
			[]*ExprNode{
				{Type: TypeNumber, Value: "2"},
				{Type: TypeNumber, Value: "3"},
			},
			false,
		},
		{
			"COUNT函数（无参数）",
			"count",
			[]*ExprNode{},
			false,
		},
		// 错误情况
		{"未知函数", "unknown", []*ExprNode{{Type: TypeNumber, Value: "1"}}, true},
		{"ABS参数数量错误", "abs", []*ExprNode{}, true},
		{"POW参数数量错误", "pow", []*ExprNode{{Type: TypeNumber, Value: "2"}}, true},
		{"参数验证失败", "abs", []*ExprNode{{Type: TypeNumber, Value: "abc"}}, true},
		{"参数表达式验证失败", "abs", []*ExprNode{{Type: TypeField, Value: "invalid field name!"}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &ExprNode{
				Type:  TypeFunction,
				Value: tt.funcName,
				Args:  tt.args,
			}
			err := validateFunctionNode(node)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "函数验证不应该失败")
			}
		})
	}
}

// TestValidateFunctionArgs 测试函数参数验证
func TestValidateFunctionArgs(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []*ExprNode
		wantErr  bool
	}{
		// 单参数函数
		{"ABS正确参数", "abs", []*ExprNode{{Type: TypeNumber, Value: "1"}}, false},
		{"ABS参数过少", "abs", []*ExprNode{}, true},
		{"ABS参数过多", "abs", []*ExprNode{{Type: TypeNumber, Value: "1"}, {Type: TypeNumber, Value: "2"}}, true},

		// 双参数函数
		{"POW正确参数", "pow", []*ExprNode{{Type: TypeNumber, Value: "2"}, {Type: TypeNumber, Value: "3"}}, false},
		{"POW参数过少", "pow", []*ExprNode{{Type: TypeNumber, Value: "2"}}, true},
		{"POW参数过多", "pow", []*ExprNode{{Type: TypeNumber, Value: "2"}, {Type: TypeNumber, Value: "3"}, {Type: TypeNumber, Value: "4"}}, true},

		// 可变参数函数
		{"MAX单参数", "max", []*ExprNode{{Type: TypeNumber, Value: "1"}}, false},
		{"MAX多参数", "max", []*ExprNode{{Type: TypeNumber, Value: "1"}, {Type: TypeNumber, Value: "2"}, {Type: TypeNumber, Value: "3"}}, false},
		{"MAX无参数", "max", []*ExprNode{}, true},

		// 无参数函数（如果有的话）
		{"COUNT无参数", "count", []*ExprNode{}, false},
		{"COUNT有参数", "count", []*ExprNode{{Type: TypeNumber, Value: "1"}}, false}, // COUNT可以有参数

		// 未知函数
		{"未知函数", "unknown", []*ExprNode{{Type: TypeNumber, Value: "1"}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建一个函数节点来测试
			node := &ExprNode{
				Type:  TypeFunction,
				Value: tt.funcName,
				Args:  tt.args,
			}
			// 使用validateFunctionNode来验证函数名和参数数量
			err := validateFunctionNode(node)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "函数参数验证不应该失败")
			}
		})
	}
}

// TestValidateCaseNode 测试CASE节点验证
func TestValidateCaseNode(t *testing.T) {
	tests := []struct {
		name     string
		caseExpr *CaseExpression
		wantErr  bool
	}{
		{
			"有效CASE表达式",
			&CaseExpression{
				WhenClauses: []WhenClause{
					{
						Condition: &ExprNode{
							Type:  TypeOperator,
							Value: ">",
							Left:  &ExprNode{Type: TypeField, Value: "a"},
							Right: &ExprNode{Type: TypeNumber, Value: "0"},
						},
						Result: &ExprNode{Type: TypeNumber, Value: "1"},
					},
				},
				ElseResult: &ExprNode{Type: TypeNumber, Value: "0"},
			},
			false,
		},
		{
			"简单CASE表达式（带Value）",
			&CaseExpression{
				Value: &ExprNode{Type: TypeField, Value: "status"},
				WhenClauses: []WhenClause{
					{
						Condition: &ExprNode{Type: TypeString, Value: "'active'"},
						Result: &ExprNode{Type: TypeNumber, Value: "1"},
					},
					{
						Condition: &ExprNode{Type: TypeString, Value: "'inactive'"},
						Result: &ExprNode{Type: TypeNumber, Value: "0"},
					},
				},
				ElseResult: &ExprNode{Type: TypeNumber, Value: "-1"},
			},
			false,
		},
		{
			"多个WHEN子句",
			&CaseExpression{
				WhenClauses: []WhenClause{
					{
						Condition: &ExprNode{
							Type:  TypeOperator,
							Value: ">",
							Left:  &ExprNode{Type: TypeField, Value: "a"},
							Right: &ExprNode{Type: TypeNumber, Value: "0"},
						},
						Result: &ExprNode{Type: TypeNumber, Value: "1"},
					},
					{
						Condition: &ExprNode{
							Type:  TypeOperator,
							Value: "<",
							Left:  &ExprNode{Type: TypeField, Value: "a"},
							Right: &ExprNode{Type: TypeNumber, Value: "0"},
						},
						Result: &ExprNode{Type: TypeNumber, Value: "-1"},
					},
				},
				ElseResult: &ExprNode{Type: TypeNumber, Value: "0"},
			},
			false,
		},
		{
			"没有ELSE子句",
			&CaseExpression{
				WhenClauses: []WhenClause{
					{
						Condition: &ExprNode{
							Type:  TypeOperator,
							Value: ">",
							Left:  &ExprNode{Type: TypeField, Value: "a"},
							Right: &ExprNode{Type: TypeNumber, Value: "0"},
						},
						Result: &ExprNode{Type: TypeNumber, Value: "1"},
					},
				},
				ElseResult: nil,
			},
			false,
		},
		// 错误情况
		{"没有WHEN子句", &CaseExpression{WhenClauses: []WhenClause{}, ElseResult: &ExprNode{Type: TypeNumber, Value: "0"}}, true},
		{"WHEN条件为空", &CaseExpression{
			WhenClauses: []WhenClause{
				{Condition: nil, Result: &ExprNode{Type: TypeNumber, Value: "1"}},
			},
		}, true},
		{"WHEN结果为空", &CaseExpression{
			WhenClauses: []WhenClause{
				{Condition: &ExprNode{Type: TypeField, Value: "a"}, Result: nil},
			},
		}, true},
		{"WHEN条件验证失败", &CaseExpression{
			WhenClauses: []WhenClause{
				{Condition: &ExprNode{Type: TypeNumber, Value: "abc"}, Result: &ExprNode{Type: TypeNumber, Value: "1"}},
			},
		}, true},
		{"WHEN结果验证失败", &CaseExpression{
			WhenClauses: []WhenClause{
				{Condition: &ExprNode{Type: TypeField, Value: "a"}, Result: &ExprNode{Type: TypeNumber, Value: "abc"}},
			},
		}, true},
		{"ELSE结果验证失败", &CaseExpression{
			WhenClauses: []WhenClause{
				{Condition: &ExprNode{Type: TypeField, Value: "a"}, Result: &ExprNode{Type: TypeNumber, Value: "1"}},
			},
			ElseResult: &ExprNode{Type: TypeNumber, Value: "abc"},
		}, true},
		{"简单CASE的Value验证失败", &CaseExpression{
			Value: &ExprNode{Type: TypeNumber, Value: "invalid_number"},
			WhenClauses: []WhenClause{
				{Condition: &ExprNode{Type: TypeString, Value: "'test'"}, Result: &ExprNode{Type: TypeNumber, Value: "1"}},
			},
		}, true},
	}

	// 添加CaseExpr为nil的测试用例
	t.Run("CaseExpr为nil", func(t *testing.T) {
		node := &ExprNode{Type: TypeCase, CaseExpr: nil}
		err := validateCaseNode(node)
		assert.Error(t, err, "CaseExpr为nil时应该返回错误")
		assert.Contains(t, err.Error(), "CASE expression is missing")
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &ExprNode{Type: TypeCase, CaseExpr: tt.caseExpr}
			err := validateCaseNode(node)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "CASE节点验证不应该失败")
			}
		})
	}
}

// TestIsValidFieldName 测试字段名验证
func TestIsValidFieldName(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		expected  bool
	}{
		// 有效情况
		{"简单字段名", "field", true},
		{"下划线开头", "_field", true},
		{"包含数字", "field123", true},
		{"驼峰命名", "fieldName", true},
		{"蛇形命名", "field_name", true},
		{"大写字段", "FIELD", true},
		{"单字符字段", "a", true},
		{"单下划线", "_", true},
		{"反引号字段", "`field name`", true},
		{"反引号包含特殊字符", "`user.name`", true},
		{"反引号包含数字开头", "`123field`", true},
		{"反引号包含连字符", "`field-name`", true},
		{"反引号包含各种符号", "`field@#$%^&*()`", true},

		// 无效情况
		{"空字段名", "", false},
		{"数字开头", "123field", false},
		{"包含连字符", "field-name", false},
		{"包含空格（无反引号）", "field name", false},
		{"包含点号（无反引号）", "field.name", false},
		{"包含特殊字符@", "field@name", false},
		{"包含特殊字符#", "field#name", false},
		{"包含特殊字符$", "field$name", false},
		{"包含特殊字符%", "field%name", false},
		{"未闭合反引号", "`field", false},
		{"空反引号", "``", false},
		{"反引号内包含反引号", "`field`name`", false},
		{"只有反引号开头", "`", false},
		{"非ASCII字符", "字段名", false},
		{"包含非ASCII字符", "field字段", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidFieldName(tt.fieldName)
			assert.Equal(t, tt.expected, result, "字段名验证结果应该正确")
		})
	}
}

// TestValidateTokens 测试标记列表验证
func TestValidateTokens(t *testing.T) {
	tests := []struct {
		name    string
		tokens  []string
		wantErr bool
	}{
		{"有效标记列表", []string{"a", "+", "b"}, false},
		{"有效函数调用", []string{"abs", "(", "x", ")"}, false},
		{"有效CASE表达式", []string{"CASE", "WHEN", "a", ">", "0", "THEN", "1", "ELSE", "0", "END"}, false},
		// 错误情况
		{"空标记列表", []string{}, true},
		{"括号不匹配", []string{"(", "a", "+", "b"}, true},
		{"连续运算符", []string{"a", "+", "+", "b"}, true},
		{"运算符开头", []string{"+", "a"}, true},
		{"运算符结尾", []string{"a", "+"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTokens(tt.tokens)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "标记验证不应该失败")
			}
		})
	}
}

// TestValidateParentheses 测试括号验证
func TestValidateParentheses(t *testing.T) {
	tests := []struct {
		name    string
		tokens  []string
		wantErr bool
	}{
		{"匹配的括号", []string{"(", "a", "+", "b", ")"}, false},
		{"嵌套括号", []string{"(", "(", "a", "+", "b", ")", "*", "c", ")"}, false},
		{"函数括号", []string{"abs", "(", "x", ")"}, false},
		{"无括号", []string{"a", "+", "b"}, false},
		// 错误情况
		{"缺少右括号", []string{"(", "a", "+", "b"}, true},
		{"缺少左括号", []string{"a", "+", "b", ")"}, true},
		{"括号顺序错误", []string{")", "a", "+", "b", "("}, true},
		{"嵌套不匹配", []string{"(", "(", "a", "+", "b", ")"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateParentheses(tt.tokens)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "括号验证不应该失败")
			}
		})
	}
}

// TestValidateTokenOrder 测试标记顺序验证
func TestValidateTokenOrder(t *testing.T) {
	tests := []struct {
		name    string
		tokens  []string
		wantErr bool
	}{
		{"正确顺序", []string{"a", "+", "b"}, false},
		{"函数调用", []string{"abs", "(", "x", ")"}, false},
		{"复杂表达式", []string{"a", "+", "b", "*", "c"}, false},
		// 错误情况
		{"连续运算符", []string{"a", "+", "+", "b"}, true},
		{"运算符开头", []string{"+", "a"}, true},
		{"运算符结尾", []string{"a", "+"}, true},
		{"连续操作数", []string{"a", "b", "+", "c"}, true},
		{"CASE关键字组合", []string{"CASE", "WHEN", "field", "THEN", "value", "END"}, false},
		{"操作符后跟一元操作符", []string{"a", "AND", "NOT", "b"}, false},
		{"连续二元操作符", []string{"a", "+", "*", "b"}, true},
		{"以一元操作符开始", []string{"NOT", "a"}, false},
		{"以二元操作符开始", []string{"-", "a"}, true},
		{"逗号分隔的参数", []string{"func", "(", "a", ",", "b", ")"}, false},
		{"括号和操作数混合", []string{"(", "a", ")", "+", "b"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTokenOrder(tt.tokens)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "标记顺序验证不应该失败")
			}
		})
	}
}

// TestValidateSyntax 测试语法验证
func TestValidateParenthesisNode(t *testing.T) {
	tests := []struct {
		name     string
		node     *ExprNode
		wantErr  bool
		errMsg   string
	}{
		{
			name: "有效的括号表达式",
			node: &ExprNode{
				Type: TypeParenthesis,
				Left: &ExprNode{
					Type:  TypeField,
					Value: "field1",
				},
			},
			wantErr: false,
		},
		{
			name: "括号内为空",
			node: &ExprNode{
				Type: TypeParenthesis,
				Left: nil,
			},
			wantErr: true,
			errMsg:  "parenthesis node missing inner expression",
		},
		{
			name: "括号内表达式无效",
			node: &ExprNode{
				Type: TypeParenthesis,
				Left: &ExprNode{
					Type:  TypeField,
					Value: "", // 空字段名
				},
			},
			wantErr: true,
			errMsg:  "field node has empty value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateParenthesisNode(tt.node)
			if tt.wantErr {
				if err == nil {
					t.Errorf("validateParenthesisNode() expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("validateParenthesisNode() error = %v, want error containing %v", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateParenthesisNode() error = %v, want nil", err)
				}
			}
		})
	}
}

func TestValidateSyntax(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		// 有效表达式
		{"有效算术表达式", "a + b", false},
		{"有效函数调用", "abs(x)", false},
		{"有效CASE表达式", "CASE WHEN a > 0 THEN 1 END", false},
		{"有效比较表达式", "field >= 10", false},
		{"有效逻辑表达式", "a != b", false},
		{"有效不等于表达式", "a <> b", false},
		{"有效小于等于表达式", "a <= b", false},
		{"复杂表达式", "a + b * c", false},

		// 错误情况
		{"空表达式", "", true},
		{"只有空格", "   ", true},
		{"空括号", "()", true},
		{"括号不匹配1", "(a + b", true},
		{"括号不匹配2", "a + b)", true},
		{"连续运算符（空格分隔）", "a + + b", true},
		{"连续运算符（直接相邻）", "a+-b", true},
		{"运算符开头", "+ a + b", true},
		{"运算符结尾", "a + b +", true},
		{"乘法运算符开头", "* a + b", true},
		{"除法运算符结尾", "a + b /", true},
		{"模运算符开头", "% a + b", true},
		{"幂运算符结尾", "a + b ^", true},
		{"等号运算符开头", "= a + b", true},
		{"不等号运算符结尾", "a + b !=", true},
		{"大于号运算符开头", "> a + b", true},
		{"小于号运算符结尾", "a + b <", true},
		{"大于等于运算符开头", ">= a + b", true},
		{"小于等于运算符结尾", "a + b <=", true},
		{"不等于运算符开头", "<> a + b", true},
		{"多个连续运算符组合1", "a + * b", true},
		{"多个连续运算符组合2", "a / % b", true},
		{"多个连续运算符组合3", "a ^ + b", true},
		{"多个连续运算符组合4", "a = > b", true},
		{"多个连续运算符组合5", "a < = b", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSyntax(tt.expr)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				assert.NoError(t, err, "语法验证不应该失败")
			}
		})
	}
}
