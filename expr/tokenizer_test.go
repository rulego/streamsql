package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTokenize 测试分词功能
func TestTokenize(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		expected []string
		wantErr  bool
	}{
		// 基本分词测试
		{"简单表达式", "a + b", []string{"a", "+", "b"}, false},
		{"数字和运算符", "123 + 456", []string{"123", "+", "456"}, false},
		{"小数", "3.14 * 2", []string{"3.14", "*", "2"}, false},
		{"负数", "-5 + 3", []string{"-5", "+", "3"}, false},
		{"负小数", "-3.14 * 2", []string{"-3.14", "*", "2"}, false},

		// 括号和函数
		{"括号表达式", "(a + b) * c", []string{"(", "a", "+", "b", ")", "*", "c"}, false},
		{"函数调用", "abs(x)", []string{"abs", "(", "x", ")"}, false},
		{"函数参数", "max(a, b)", []string{"max", "(", "a", ",", "b", ")"}, false},

		// 比较运算符
		{"等于运算符", "a == b", []string{"a", "==", "b"}, false},
		{"不等于运算符", "a != b", []string{"a", "!=", "b"}, false},
		{"大于等于", "a >= b", []string{"a", ">=", "b"}, false},
		{"小于等于", "a <= b", []string{"a", "<=", "b"}, false},
		{"不等于SQL风格", "a <> b", []string{"a", "<>", "b"}, false},

		// 字符串字面量
		{"单引号字符串", "'hello'", []string{"'hello'"}, false},
		{"双引号字符串", "\"world\"", []string{"\"world\""}, false},
		{"字符串比较", "name == 'test'", []string{"name", "==", "'test'"}, false},
		{"包含转义的字符串", "'hello\\world'", []string{"'hello\\world'"}, false},

		// 反引号标识符
		{"反引号字段", "`field name`", []string{"`field name`"}, false},
		{"反引号表达式", "`user.name` + `user.age`", []string{"`user.name`", "+", "`user.age`"}, false},

		// CASE表达式
		{"简单CASE", "CASE WHEN a > 0 THEN 1 ELSE 0 END", []string{"CASE", "WHEN", "a", ">", "0", "THEN", "1", "ELSE", "0", "END"}, false},

		// 复杂表达式
		{"复杂算术", "a + b * c - d / e", []string{"a", "+", "b", "*", "c", "-", "d", "/", "e"}, false},
		{"幂运算", "a ^ b", []string{"a", "^", "b"}, false},
		{"取模运算", "a % b", []string{"a", "%", "b"}, false},

		// 空白字符处理
		{"多个空格", "a   +   b", []string{"a", "+", "b"}, false},
		{"制表符", "a\t+\tb", []string{"a", "+", "b"}, false},
		{"换行符", "a\n+\nb", []string{"a", "+", "b"}, false},

		// 错误情况
		{"空表达式", "", nil, true},
		{"只有空格", "   ", nil, true},
		{"未闭合字符串", "'hello", nil, true},
		{"未闭合反引号", "`field", nil, true},
		{"无效字符", "a @ b", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tokenize(tt.expr)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "分词不应该失败")
				assert.Equal(t, tt.expected, result, "分词结果应该匹配")
			}
		})
	}
}

// TestIsDigit 测试数字字符判断
func TestIsDigit(t *testing.T) {
	tests := []struct {
		ch       byte
		expected bool
	}{
		{'0', true},
		{'5', true},
		{'9', true},
		{'a', false},
		{'A', false},
		{' ', false},
		{'.', false},
		{'+', false},
	}

	for _, tt := range tests {
		t.Run(string(tt.ch), func(t *testing.T) {
			result := isDigit(tt.ch)
			assert.Equal(t, tt.expected, result, "数字字符判断应该正确")
		})
	}
}

// TestIsLetter 测试字母字符判断
func TestIsLetter(t *testing.T) {
	tests := []struct {
		ch       byte
		expected bool
	}{
		{'a', true},
		{'z', true},
		{'A', true},
		{'Z', true},
		{'0', false},
		{'9', false},
		{' ', false},
		{'_', false},
		{'+', false},
	}

	for _, tt := range tests {
		t.Run(string(tt.ch), func(t *testing.T) {
			result := isLetter(tt.ch)
			assert.Equal(t, tt.expected, result, "字母字符判断应该正确")
		})
	}
}

// TestIsNumber 测试数字字符串判断
func TestIsNumber(t *testing.T) {
	tests := []struct {
		s        string
		expected bool
	}{
		{"123", true},
		{"0", true},
		{"3.14", true},
		{"-5", true},
		{"-3.14", true},
		{"1e10", true},
		{"1.5e-3", true},
		{"abc", false},
		{"12a", false},
		{"", false},
		{".", false},
		{"--5", false},
	}

	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			result := isNumber(tt.s)
			assert.Equal(t, tt.expected, result, "数字字符串判断应该正确")
		})
	}
}

// TestIsIdentifier 测试标识符判断
func TestIsIdentifier(t *testing.T) {
	tests := []struct {
		s        string
		expected bool
	}{
		{"abc", true},
		{"_var", true},
		{"var123", true},
		{"CamelCase", true},
		{"snake_case", true},
		{"123abc", false},
		{"", false},
		{"var-name", false},
		{"var.name", true},
		{"var[0]", true},
		{"var['key']", true},
		{"var name", false},
	}

	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			result := isIdentifier(tt.s)
			assert.Equal(t, tt.expected, result, "标识符判断应该正确")
		})
	}
}

// TestIsOperator 测试运算符判断
func TestIsOperator(t *testing.T) {
	tests := []struct {
		s        string
		expected bool
	}{
		{"+", true},
		{"-", true},
		{"*", true},
		{"/", true},
		{"%", true},
		{"^", true},
		{">", true},
		{"<", true},
		{">=", true},
		{"<=", true},
		{"==", true},
		{"=", true},
		{"!=", true},
		{"<>", true},
		{"AND", true},
		{"OR", true},
		{"NOT", true},
		{"LIKE", true},
		{"IS", true},
		{"abc", false},
		{"123", false},
		{"(", false},
		{")", false},
	}

	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			result := isOperator(tt.s)
			assert.Equal(t, tt.expected, result, "运算符判断应该正确")
		})
	}
}

// TestIsComparisonOperator 测试比较运算符判断
func TestIsComparisonOperator(t *testing.T) {
	tests := []struct {
		s        string
		expected bool
	}{
		{">", true},
		{"<", true},
		{">=", true},
		{"<=", true},
		{"==", true},
		{"=", true},
		{"!=", true},
		{"<>", true},
		{"+", false},
		{"-", false},
		{"*", false},
		{"/", false},
		{"AND", false},
		{"OR", false},
	}

	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			result := isComparisonOperator(tt.s)
			assert.Equal(t, tt.expected, result, "比较运算符判断应该正确")
		})
	}
}

// TestIsStringLiteral 测试字符串字面量判断
func TestIsStringLiteral(t *testing.T) {
	tests := []struct {
		s        string
		expected bool
	}{
		{"'hello'", true},
		{"\"world\"", true},
		{"''", true},
		{"\"\"", true},
		{"'hello", false},
		{"hello'", false},
		{"\"hello", false},
		{"hello\"", false},
		{"hello", false},
		{"", false},
		{"'", false},
		{"\"", false},
	}

	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			result := isStringLiteral(tt.s)
			assert.Equal(t, tt.expected, result, "字符串字面量判断应该正确")
		})
	}
}

// TestTokenizeComplexExpressions 测试复杂表达式分词
func TestTokenizeComplexExpressions(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		expected []string
	}{
		{
			"温度转换表达式",
			"temperature * 1.8 + 32",
			[]string{"temperature", "*", "1.8", "+", "32"},
		},
		{
			"复杂CASE表达式",
			"CASE WHEN temperature > 30 AND humidity < 60 THEN 'HOT' ELSE 'NORMAL' END",
			[]string{"CASE", "WHEN", "temperature", ">", "30", "AND", "humidity", "<", "60", "THEN", "'HOT'", "ELSE", "'NORMAL'", "END"},
		},
		{
			"嵌套函数调用",
			"sqrt(pow(a, 2) + pow(b, 2))",
			[]string{"sqrt", "(", "pow", "(", "a", ",", "2", ")", "+", "pow", "(", "b", ",", "2", ")", ")"},
		},
		{
			"负数在比较运算符后",
			"a > -5 AND b <= -3.14",
			[]string{"a", ">", "-5", "AND", "b", "<=", "-3.14"},
		},
		{
			"幂运算后的负数",
			"a ^ -2",
			[]string{"a", "^", "-2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tokenize(tt.expr)
			require.NoError(t, err, "复杂表达式分词不应该失败")
			assert.Equal(t, tt.expected, result, "复杂表达式分词结果应该匹配")
		})
	}
}

// TestTokenizeEdgeCases 测试边界情况
func TestTokenizeEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		expected []string
		wantErr  bool
	}{
		{"只有数字", "123", []string{"123"}, false},
		{"只有小数点开头的数字", ".5", []string{".5"}, false},
		{"连续运算符（应该在解析阶段检测）", "a + + b", []string{"a", "+", "+", "b"}, false},
		{"多个小数点", "3.14.15", []string{"3.14", ".", "15"}, false}, // 分词器不检查语法错误
		{"空字符串转义", "''", []string{"''"}, false},
		{"包含空格的反引号标识符", "`user name`", []string{"`user name`"}, false},
		{"特殊字符在字符串中", "'hello@world#test'", []string{"'hello@world#test'"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tokenize(tt.expr)
			if tt.wantErr {
				assert.Error(t, err, "应该返回错误")
			} else {
				require.NoError(t, err, "分词不应该失败")
				assert.Equal(t, tt.expected, result, "分词结果应该匹配")
			}
		})
	}
}
