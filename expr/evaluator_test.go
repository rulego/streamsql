package expr

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEvaluateNode 测试节点求值功能
func TestEvaluateNode(t *testing.T) {
	data := map[string]interface{}{
		"a":    10.0,
		"b":    5.0,
		"c":    2.0,
		"name": "test",
		"flag": true,
	}

	tests := []struct {
		name     string
		node     *ExprNode
		expected float64
		wantErr  bool
	}{
		{
			"数字节点",
			&ExprNode{Type: TypeNumber, Value: "123"},
			123.0,
			false,
		},
		{
			"字段节点",
			&ExprNode{Type: TypeField, Value: "a"},
			10.0,
			false,
		},
		{
			"加法运算",
			&ExprNode{
				Type:  TypeOperator,
				Value: "+",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "b"},
			},
			15.0,
			false,
		},
		{
			"乘法运算",
			&ExprNode{
				Type:  TypeOperator,
				Value: "*",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "b"},
			},
			50.0,
			false,
		},
		{
			"除法运算",
			&ExprNode{
				Type:  TypeOperator,
				Value: "/",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "b"},
			},
			2.0,
			false,
		},
		{
			"幂运算",
			&ExprNode{
				Type:  TypeOperator,
				Value: "^",
				Left:  &ExprNode{Type: TypeField, Value: "c"},
				Right: &ExprNode{Type: TypeNumber, Value: "3"},
			},
			8.0,
			false,
		},
		{
			"取模运算",
			&ExprNode{
				Type:  TypeOperator,
				Value: "%",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeNumber, Value: "3"},
			},
			1.0,
			false,
		},
		{
			"函数调用",
			&ExprNode{
				Type:  TypeFunction,
				Value: "abs",
				Args:  []*ExprNode{{Type: TypeNumber, Value: "-5"}},
			},
			5.0,
			false,
		},
		{
			"括号表达式",
			&ExprNode{
				Type: TypeParenthesis,
				Left: &ExprNode{
					Type:  TypeOperator,
					Value: "+",
					Left:  &ExprNode{Type: TypeField, Value: "a"},
					Right: &ExprNode{Type: TypeField, Value: "b"},
				},
			},
			15.0,
			false,
		},
		// 错误情况
		{"不存在的字段", &ExprNode{Type: TypeField, Value: "unknown"}, 0, true},
		{"除零错误", &ExprNode{
			Type:  TypeOperator,
			Value: "/",
			Left:  &ExprNode{Type: TypeNumber, Value: "1"},
			Right: &ExprNode{Type: TypeNumber, Value: "0"},
		}, 0, true},
		{"无效的运算符", &ExprNode{
			Type:  TypeOperator,
			Value: "@",
			Left:  &ExprNode{Type: TypeNumber, Value: "1"},
			Right: &ExprNode{Type: TypeNumber, Value: "2"},
		}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateNode(tt.node, data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "求值不应该失败")
				assert.Equal(t, tt.expected, result, "求值结果应该正确")
			}
		})
	}
}

// TestEvaluateNodeWithNull 测试支持NULL值的节点求值
func TestEvaluateNodeWithNull(t *testing.T) {
	data := map[string]interface{}{
		"a":         10.0,
		"b":         nil,
		"c":         5.0,
		"flag":      true,
		"nested.field": 20.0,
	}

	tests := []struct {
		name       string
		node       *ExprNode
		expected   float64
		expectedNull bool
		wantErr    bool
	}{
		{
			"空节点",
			nil,
			0,
			true,
			false,
		},
		{
			"数字节点",
			&ExprNode{Type: TypeNumber, Value: "123"},
			123.0,
			false,
			false,
		},
		{
			"字段节点（存在）",
			&ExprNode{Type: TypeField, Value: "a"},
			10.0,
			false,
			false,
		},
		{
			"字段节点（NULL值）",
			&ExprNode{Type: TypeField, Value: "b"},
			0,
			true,
			false,
		},
		{
			"字段节点（不存在）",
			&ExprNode{Type: TypeField, Value: "unknown"},
			0,
			true,
			false,
		},
		{
			"字段节点（反引号）",
			&ExprNode{Type: TypeField, Value: "`a`"},
			10.0,
			false,
			false,
		},
		{
			"嵌套字段",
			&ExprNode{Type: TypeField, Value: "nested.field"},
			0,
			true,
			false,
		},
		{
			"布尔字段",
			&ExprNode{Type: TypeField, Value: "flag"},
			1.0,
			false,
			false,
		},
		{
			"加法运算（正常）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "+",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeNumber, Value: "5"},
			},
			15.0,
			false,
			false,
		},
		{
			"加法运算（左NULL）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "+",
				Left:  &ExprNode{Type: TypeField, Value: "b"},
				Right: &ExprNode{Type: TypeNumber, Value: "5"},
			},
			0,
			true,
			false,
		},
		{
			"加法运算（右NULL）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "+",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "b"},
			},
			0,
			true,
			false,
		},
		{
			"IS NULL比较（真）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS",
				Left:  &ExprNode{Type: TypeField, Value: "b"},
				Right: &ExprNode{Type: TypeField, Value: "NULL"},
			},
			1,
			false,
			false,
		},
		{
			"IS NULL比较（假）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "NULL"},
			},
			0,
			false,
			false,
		},
		{
			"括号表达式",
			&ExprNode{
				Type: TypeParenthesis,
				Left: &ExprNode{Type: TypeField, Value: "a"},
			},
			10.0,
			false,
			false,
		},
		{
			"函数调用",
			&ExprNode{
				Type:  TypeFunction,
				Value: "abs",
				Args:  []*ExprNode{{Type: TypeNumber, Value: "-5"}},
			},
			5.0,
			false,
			false,
		},
		// 错误情况
		{
			"无效数字",
			&ExprNode{Type: TypeNumber, Value: "invalid"},
			0,
			false,
			true,
		},
		{
			"数字字段",
			&ExprNode{Type: TypeField, Value: "c"},
			5.0,
			false,
			false,
		},
		{
			"除零错误",
			&ExprNode{
				Type:  TypeOperator,
				Value: "/",
				Left:  &ExprNode{Type: TypeNumber, Value: "1"},
				Right: &ExprNode{Type: TypeNumber, Value: "0"},
			},
			0,
			false,
			true,
		},
		{
			"未知运算符",
			&ExprNode{
				Type:  TypeOperator,
				Value: "@",
				Left:  &ExprNode{Type: TypeNumber, Value: "1"},
				Right: &ExprNode{Type: TypeNumber, Value: "2"},
			},
			0,
			false,
			true,
		},
		{
			"未知节点类型",
			&ExprNode{Type: "unknown"},
			0,
			false,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, isNull, err := evaluateNodeWithNull(tt.node, data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "求值不应该失败")
				assert.Equal(t, tt.expected, result, "求值结果应该正确")
				assert.Equal(t, tt.expectedNull, isNull, "NULL状态应该正确")
			}
		})
	}
}

// TestEvaluateIsOperator 测试IS运算符
func TestEvaluateIsOperator(t *testing.T) {
	data := map[string]interface{}{
		"a":    10.0,
		"b":    nil,
		"flag": true,
		"name": "test",
	}

	tests := []struct {
		name     string
		node     *ExprNode
		expected interface{}
		wantErr  bool
	}{
		{
			"IS NULL（真）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS",
				Left:  &ExprNode{Type: TypeField, Value: "b"},
				Right: &ExprNode{Type: TypeField, Value: "NULL"},
			},
			true,
			false,
		},
		{
			"IS NULL（假）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "NULL"},
			},
			false,
			false,
		},
		{
			"IS NOT NULL（真）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS NOT",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "NULL"},
			},
			true,
			false,
		},
		{
			"IS NOT NULL（假）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS NOT",
				Left:  &ExprNode{Type: TypeField, Value: "b"},
				Right: &ExprNode{Type: TypeField, Value: "NULL"},
			},
			false,
			false,
		},
		{
			"IS 相等比较（真）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeNumber, Value: "10"},
			},
			true,
			false,
		},
		{
			"IS 相等比较（假）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeNumber, Value: "5"},
			},
			false,
			false,
		},
		{
			"IS NOT 不等比较（真）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS NOT",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeNumber, Value: "5"},
			},
			true,
			false,
		},
		{
			"IS NOT 不等比较（假）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS NOT",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeNumber, Value: "10"},
			},
			false,
			false,
		},
		{
			"不存在字段IS NULL",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS",
				Left:  &ExprNode{Type: TypeField, Value: "nonexistent"},
				Right: &ExprNode{Type: TypeField, Value: "NULL"},
			},
			true,
			false,
		},
		// 错误情况
		{
			"缺少右操作数",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: nil,
			},
			nil,
			true,
		},
		{
			"不支持的IS运算符",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS UNKNOWN",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "NULL"},
			},
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateIsOperator(tt.node, data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "IS运算符求值不应该失败")
				assert.Equal(t, tt.expected, result, "IS运算符求值结果应该正确")
			}
		})
	}
}

// TestEvaluateBoolFunction 测试布尔函数求值
func TestEvaluateBoolFunction(t *testing.T) {
	data := map[string]interface{}{}

	tests := []struct {
		name     string
		node     *ExprNode
		expected bool
		wantErr  bool
	}{
		{
			"ABS函数（非零）",
			&ExprNode{
				Type:  TypeFunction,
				Value: "abs",
				Args:  []*ExprNode{{Type: TypeNumber, Value: "-5"}},
			},
			true,
			false,
		},
		{
			"ABS函数（零）",
			&ExprNode{
				Type:  TypeFunction,
				Value: "abs",
				Args:  []*ExprNode{{Type: TypeNumber, Value: "0"}},
			},
			false,
			false,
		},
		// 错误情况
		{
			"未知函数",
			&ExprNode{
				Type:  TypeFunction,
				Value: "unknown_func",
				Args:  []*ExprNode{{Type: TypeNumber, Value: "5"}},
			},
			false,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateBoolFunction(tt.node, data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "布尔函数求值不应该失败")
				assert.Equal(t, tt.expected, result, "布尔函数求值结果应该正确")
			}
		})
	}
}

// TestEvaluateFieldNode 测试字段节点求值
func TestEvaluateFieldNode(t *testing.T) {
	data := map[string]interface{}{
		"int_field":    42,
		"float_field":  3.14,
		"string_field": "hello",
		"bool_field":   true,
		"nil_field":    nil,
	}

	tests := []struct {
		name      string
		fieldName string
		expected  float64
		wantErr   bool
	}{
		{"整数字段", "int_field", 42.0, false},
		{"浮点数字段", "float_field", 3.14, false},
		{"字符串字段（数字）", "string_field", 0, true}, // 字符串"hello"无法转换为数字
		{"布尔字段", "bool_field", 1.0, false},
		{"nil字段", "nil_field", 0, true},
		{"不存在的字段", "unknown_field", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &ExprNode{Type: TypeField, Value: tt.fieldName}
			result, err := evaluateFieldNode(node, data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "字段求值不应该失败")
				assert.Equal(t, tt.expected, result, "字段求值结果应该正确")
			}
		})
	}
}

// TestEvaluateOperatorNode 测试运算符节点求值
func TestEvaluateOperatorNode(t *testing.T) {
	data := map[string]interface{}{
		"a": 10.0,
		"b": 3.0,
		"c": 0.0,
	}

	tests := []struct {
		name     string
		operator string
		left     *ExprNode
		right    *ExprNode
		expected float64
		wantErr  bool
	}{
		{
			"加法",
			"+",
			&ExprNode{Type: TypeField, Value: "a"},
			&ExprNode{Type: TypeField, Value: "b"},
			13.0,
			false,
		},
		{
			"减法",
			"-",
			&ExprNode{Type: TypeField, Value: "a"},
			&ExprNode{Type: TypeField, Value: "b"},
			7.0,
			false,
		},
		{
			"乘法",
			"*",
			&ExprNode{Type: TypeField, Value: "a"},
			&ExprNode{Type: TypeField, Value: "b"},
			30.0,
			false,
		},
		{
			"除法",
			"/",
			&ExprNode{Type: TypeField, Value: "a"},
			&ExprNode{Type: TypeField, Value: "b"},
			10.0 / 3.0,
			false,
		},
		{
			"取模",
			"%",
			&ExprNode{Type: TypeField, Value: "a"},
			&ExprNode{Type: TypeField, Value: "b"},
			1.0,
			false,
		},
		{
			"幂运算",
			"^",
			&ExprNode{Type: TypeField, Value: "b"},
			&ExprNode{Type: TypeNumber, Value: "2"},
			9.0,
			false,
		},
		// 错误情况
		{
			"除零",
			"/",
			&ExprNode{Type: TypeField, Value: "a"},
			&ExprNode{Type: TypeField, Value: "c"},
			0,
			true,
		},
		{
			"模零",
			"%",
			&ExprNode{Type: TypeField, Value: "a"},
			&ExprNode{Type: TypeField, Value: "c"},
			0,
			true,
		},
		{
			"无效运算符",
			"@",
			&ExprNode{Type: TypeField, Value: "a"},
			&ExprNode{Type: TypeField, Value: "b"},
			0,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &ExprNode{
				Type:  TypeOperator,
				Value: tt.operator,
				Left:  tt.left,
				Right: tt.right,
			}
			result, err := evaluateOperatorNode(node, data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "运算符求值不应该失败")
				assert.InDelta(t, tt.expected, result, 1e-10, "运算符求值结果应该正确")
			}
		})
	}
}

// TestEvaluateFunctionNode 测试函数节点求值
func TestEvaluateFunctionNode(t *testing.T) {
	data := map[string]interface{}{}

	tests := []struct {
		name     string
		funcName string
		args     []*ExprNode
		expected float64
		wantErr  bool
	}{
		{
			"ABS函数",
			"abs",
			[]*ExprNode{{Type: TypeNumber, Value: "-5"}},
			5.0,
			false,
		},
		{
			"SQRT函数",
			"sqrt",
			[]*ExprNode{{Type: TypeNumber, Value: "16"}},
			4.0,
			false,
		},
		{
			"POW函数",
			"pow",
			[]*ExprNode{
				{Type: TypeNumber, Value: "2"},
				{Type: TypeNumber, Value: "3"},
			},
			8.0,
			false,
		},
		{
			"MAX函数",
			"max",
			[]*ExprNode{
				{Type: TypeNumber, Value: "5"},
				{Type: TypeNumber, Value: "3"},
				{Type: TypeNumber, Value: "8"},
			},
			8.0,
			false,
		},
		{
			"MIN函数",
			"min",
			[]*ExprNode{
				{Type: TypeNumber, Value: "5"},
				{Type: TypeNumber, Value: "3"},
				{Type: TypeNumber, Value: "8"},
			},
			3.0,
			false,
		},
		{
			"SUM函数",
			"sum",
			[]*ExprNode{
				{Type: TypeNumber, Value: "1"},
				{Type: TypeNumber, Value: "2"},
				{Type: TypeNumber, Value: "3"},
			},
			6.0,
			false,
		},
		{
			"AVG函数",
			"avg",
			[]*ExprNode{
				{Type: TypeNumber, Value: "2"},
				{Type: TypeNumber, Value: "4"},
				{Type: TypeNumber, Value: "6"},
			},
			4.0,
			false,
		},
		{
			"COUNT函数",
			"count",
			[]*ExprNode{
				{Type: TypeNumber, Value: "1"},
				{Type: TypeNumber, Value: "2"},
				{Type: TypeNumber, Value: "3"},
			},
			3.0,
			false,
		},
		{
			"ROUND函数",
			"round",
			[]*ExprNode{{Type: TypeNumber, Value: "3.7"}},
			4.0,
			false,
		},
		{
			"FLOOR函数",
			"floor",
			[]*ExprNode{{Type: TypeNumber, Value: "3.7"}},
			3.0,
			false,
		},
		{
			"CEIL函数",
			"ceil",
			[]*ExprNode{{Type: TypeNumber, Value: "3.2"}},
			4.0,
			false,
		},
		// 三角函数
		{
			"SIN函数",
			"sin",
			[]*ExprNode{{Type: TypeNumber, Value: "0"}},
			0.0,
			false,
		},
		{
			"COS函数",
			"cos",
			[]*ExprNode{{Type: TypeNumber, Value: "0"}},
			1.0,
			false,
		},
		// 对数函数
		{
			"LOG函数",
			"log",
			[]*ExprNode{{Type: TypeNumber, Value: "10"}},
			math.Log10(10),
			false,
		},
		{
			"LN函数",
			"ln",
			[]*ExprNode{{Type: TypeNumber, Value: "1"}},
			0.0,
			false,
		},
		{
			"EXP函数",
			"exp",
			[]*ExprNode{{Type: TypeNumber, Value: "0"}},
			1.0,
			false,
		},
		// 错误情况
		{"未知函数", "unknown", []*ExprNode{{Type: TypeNumber, Value: "1"}}, 0, true},
		{"参数数量错误", "abs", []*ExprNode{}, 0, true},
		{"SQRT负数", "sqrt", []*ExprNode{{Type: TypeNumber, Value: "-1"}}, 0, true},
		{"LOG零或负数", "log", []*ExprNode{{Type: TypeNumber, Value: "0"}}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &ExprNode{
				Type:  TypeFunction,
				Value: tt.funcName,
				Args:  tt.args,
			}
			result, err := evaluateFunctionNode(node, data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "函数求值不应该失败")
				assert.InDelta(t, tt.expected, result, 1e-10, "函数求值结果应该正确")
			}
		})
	}
}

// TestEvaluateNodeValue 测试节点值求值（支持NULL）
func TestEvaluateNodeValue(t *testing.T) {
	data := map[string]interface{}{
		"a":        10.0,
		"b":        nil,
		"name":     "test",
		"empty":    "",
		"zero":     0,
		"negative": -5,
	}

	tests := []struct {
		name     string
		node     *ExprNode
		expected interface{}
		wantErr  bool
	}{
		{
			"数字节点",
			&ExprNode{Type: TypeNumber, Value: "123"},
			123.0,
			false,
		},
		{
			"字符串节点",
			&ExprNode{Type: TypeString, Value: "'hello'"},
			"hello",
			false,
		},
		{
			"字段节点（数字）",
			&ExprNode{Type: TypeField, Value: "a"},
			10.0,
			false,
		},
		{
			"字段节点（NULL）",
			&ExprNode{Type: TypeField, Value: "b"},
			nil,
			false,
		},
		{
			"字段节点（字符串）",
			&ExprNode{Type: TypeField, Value: "name"},
			"test",
			false,
		},
		{
			"字段节点（空字符串）",
			&ExprNode{Type: TypeField, Value: "empty"},
			"",
			false,
		},
		{
			"字段节点（零值）",
			&ExprNode{Type: TypeField, Value: "zero"},
			0,
			false,
		},
		{
			"字段节点（负数）",
			&ExprNode{Type: TypeField, Value: "negative"},
			-5,
			false,
		},
		{
			"等于比较（相等）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "==",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeNumber, Value: "10"},
			},
			true,
			false,
		},
		{
			"等于比较（不相等）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "==",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeNumber, Value: "5"},
			},
			false,
			false,
		},
		{
			"IS NULL比较（真）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS",
				Left:  &ExprNode{Type: TypeField, Value: "b"},
				Right: &ExprNode{Type: TypeField, Value: "NULL"},
			},
			true,
			false,
		},
		{
			"IS NULL比较（假）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "IS",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "NULL"},
			},
			false,
			false,
		},
		// 错误情况
		{"不存在的字段", &ExprNode{Type: TypeField, Value: "unknown"}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateNodeValue(tt.node, data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "节点值求值不应该失败")
				assert.Equal(t, tt.expected, result, "节点值求值结果应该正确")
			}
		})
	}
}

// TestCompareValues 测试值比较功能
func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		operator string
		left     interface{}
		right    interface{}
		expected bool
		wantErr  bool
	}{
		// 数字比较
		{"数字相等", "==", 5.0, 5.0, true, false},
		{"数字不等", "!=", 5.0, 3.0, true, false},
		{"数字大于", ">", 5.0, 3.0, true, false},
		{"数字小于", "<", 3.0, 5.0, true, false},
		{"数字大于等于", ">=", 5.0, 5.0, true, false},
		{"数字小于等于", "<=", 3.0, 5.0, true, false},

		// 字符串比较
		{"字符串相等", "==", "hello", "hello", true, false},
		{"字符串不等", "!=", "hello", "world", true, false},
		{"字符串大于", ">", "world", "hello", true, false},
		{"字符串小于", "<", "hello", "world", true, false},

		// LIKE模式匹配
		{"LIKE匹配", "LIKE", "hello", "h%", true, false},
		{"LIKE不匹配", "LIKE", "hello", "w%", false, false},
		{"LIKE通配符", "LIKE", "hello", "h_llo", true, false},
		{"LIKE完全匹配", "LIKE", "hello", "hello", true, false},

		// 混合类型比较
		{"数字与字符串", "==", 5.0, "5", true, false},
		{"布尔值比较", "==", true, true, true, false},
		{"布尔值与数字", "==", true, 1.0, true, false},
		{"布尔值与数字（假）", "==", false, 0.0, true, false},

		// 错误情况
		{"无效运算符", "@", 5.0, 3.0, false, true},
		{"不兼容类型", ">", "hello", 5.0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := compareValues(tt.left, tt.right, tt.operator)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "值比较不应该失败")
				assert.Equal(t, tt.expected, result, "值比较结果应该正确")
			}
		})
	}
}

// TestMatchLikePattern 测试LIKE模式匹配
func TestMatchLikePattern(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		pattern  string
		expected bool
	}{
		{"完全匹配", "hello", "hello", true},
		{"百分号通配符开头", "hello", "%llo", true},
		{"百分号通配符结尾", "hello", "hel%", true},
		{"百分号通配符中间", "hello", "h%o", true},
		{"百分号通配符全部", "hello", "%", true},
		{"下划线通配符", "hello", "h_llo", true},
		{"下划线通配符多个", "hello", "h___o", true},
		{"混合通配符", "hello world", "h%w_rld", true},
		{"不匹配", "hello", "world", false},
		{"长度不匹配", "hello", "h_", false},
		{"空字符串", "", "", true},
		{"空模式", "hello", "", false},
		{"空文本匹配百分号", "", "%", true},
		{"大小写敏感", "Hello", "hello", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchLikePattern(tt.text, tt.pattern)
			assert.Equal(t, tt.expected, result, "LIKE模式匹配结果应该正确")
		})
	}
}

// TestEvaluateBoolNode 测试布尔节点求值
func TestEvaluateBoolNode(t *testing.T) {
	data := map[string]interface{}{
		"a":    10.0,
		"b":    5.0,
		"flag": true,
		"name": "test",
		"zero": 0,
		"empty": "",
		"null_field": nil,
	}

	tests := []struct {
		name     string
		node     *ExprNode
		expected bool
		wantErr  bool
	}{
		{
			"数字比较（真）",
			&ExprNode{
				Type:  TypeOperator,
				Value: ">",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "b"},
			},
			true,
			false,
		},
		{
			"数字比较（假）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "<",
				Left:  &ExprNode{Type: TypeField, Value: "a"},
				Right: &ExprNode{Type: TypeField, Value: "b"},
			},
			false,
			false,
		},
		{
			"AND运算（真）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "AND",
				Left: &ExprNode{
					Type:  TypeOperator,
					Value: ">",
					Left:  &ExprNode{Type: TypeField, Value: "a"},
					Right: &ExprNode{Type: TypeNumber, Value: "0"},
				},
				Right: &ExprNode{
					Type:  TypeOperator,
					Value: ">",
					Left:  &ExprNode{Type: TypeField, Value: "b"},
					Right: &ExprNode{Type: TypeNumber, Value: "0"},
				},
			},
			true,
			false,
		},
		{
			"OR运算（真）",
			&ExprNode{
				Type:  TypeOperator,
				Value: "OR",
				Left: &ExprNode{
					Type:  TypeOperator,
					Value: ">",
					Left:  &ExprNode{Type: TypeField, Value: "a"},
					Right: &ExprNode{Type: TypeNumber, Value: "100"},
				},
				Right: &ExprNode{
					Type:  TypeOperator,
					Value: ">",
					Left:  &ExprNode{Type: TypeField, Value: "b"},
					Right: &ExprNode{Type: TypeNumber, Value: "0"},
				},
			},
			true,
			false,
		},
		{
			"NOT运算",
			&ExprNode{
				Type:  TypeOperator,
				Value: "NOT",
				Left: &ExprNode{
					Type:  TypeOperator,
					Value: ">",
					Left:  &ExprNode{Type: TypeField, Value: "a"},
					Right: &ExprNode{Type: TypeNumber, Value: "100"},
				},
			},
			true,
			false,
		},
		{
			"字符串LIKE匹配",
			&ExprNode{
				Type:  TypeOperator,
				Value: "LIKE",
				Left:  &ExprNode{Type: TypeField, Value: "name"},
				Right: &ExprNode{Type: TypeString, Value: "'t%'"},
			},
			true,
			false,
		},
		// 新增测试用例以提高覆盖率
		{
			"字段节点（真值）",
			&ExprNode{Type: TypeField, Value: "flag"},
			true,
			false,
		},
		{
			"字段节点（假值）",
			&ExprNode{Type: TypeField, Value: "zero"},
			false,
			false,
		},
		{
			"字段节点（不存在字段）",
			&ExprNode{Type: TypeField, Value: "nonexistent"},
			false,
			false,
		},
		{
			"数字节点（非零）",
			&ExprNode{Type: TypeNumber, Value: "5"},
			true,
			false,
		},
		{
			"数字节点（零）",
			&ExprNode{Type: TypeNumber, Value: "0"},
			false,
			false,
		},
		{
			"字符串节点（非空）",
			&ExprNode{Type: TypeString, Value: "'hello'"},
			true,
			false,
		},
		{
			"字符串节点（空）",
			&ExprNode{Type: TypeString, Value: "''"},
			false,
			false,
		},
		{
			"括号表达式",
			&ExprNode{
				Type: TypeParenthesis,
				Left: &ExprNode{Type: TypeField, Value: "flag"},
			},
			true,
			false,
		},
		{
			"函数节点",
			&ExprNode{
				Type:  TypeFunction,
				Value: "abs",
				Args:  []*ExprNode{{Type: TypeNumber, Value: "-5"}},
			},
			true,
			false,
		},
		// 错误情况
		{"非布尔运算符", &ExprNode{
			Type:  TypeOperator,
			Value: "+",
			Left:  &ExprNode{Type: TypeField, Value: "a"},
			Right: &ExprNode{Type: TypeField, Value: "b"},
		}, false, true},
		{"空节点", nil, false, true},
		{"空括号表达式", &ExprNode{Type: TypeParenthesis}, false, true},
		{"无效数字", &ExprNode{Type: TypeNumber, Value: "invalid"}, false, true},
		{"不支持的节点类型", &ExprNode{Type: "unknown"}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateBoolNode(tt.node, data)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "布尔节点求值不应该失败")
				assert.Equal(t, tt.expected, result, "布尔节点求值结果应该正确")
			}
		})
	}
}
