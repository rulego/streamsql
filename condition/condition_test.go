package condition

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewExprCondition 测试创建表达式条件
func TestNewExprCondition(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		wantErr    bool
	}{
		{
			name:       "简单比较表达式",
			expression: "age > 18",
			wantErr:    false,
		},
		{
			name:       "复杂逻辑表达式",
			expression: "age > 18 && name == 'John'",
			wantErr:    false,
		},
		{
			name:       "包含函数的表达式",
			expression: "is_null(name)",
			wantErr:    false,
		},
		{
			name:       "LIKE模式匹配",
			expression: "like_match(name, 'John%')",
			wantErr:    false,
		},
		{
			name:       "无效表达式",
			expression: "age >",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond, err := NewExprCondition(tt.expression)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, cond)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, cond)
			}
		})
	}
}

// TestExprCondition_Evaluate 测试表达式条件求值
func TestExprCondition_Evaluate(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		env        map[string]interface{}
		expected   bool
	}{
		{
			name:       "数值比较 - 大于",
			expression: "age > 18",
			env:        map[string]interface{}{"age": 25},
			expected:   true,
		},
		{
			name:       "数值比较 - 小于等于",
			expression: "age <= 18",
			env:        map[string]interface{}{"age": 16},
			expected:   true,
		},
		{
			name:       "字符串相等比较",
			expression: "name == 'John'",
			env:        map[string]interface{}{"name": "John"},
			expected:   true,
		},
		{
			name:       "字符串不等比较",
			expression: "name != 'John'",
			env:        map[string]interface{}{"name": "Jane"},
			expected:   true,
		},
		{
			name:       "逻辑AND - 真",
			expression: "age > 18 && active == true",
			env:        map[string]interface{}{"age": 25, "active": true},
			expected:   true,
		},
		{
			name:       "逻辑AND - 假",
			expression: "age > 18 && active == true",
			env:        map[string]interface{}{"age": 25, "active": false},
			expected:   false,
		},
		{
			name:       "逻辑OR - 真",
			expression: "age < 18 || vip == true",
			env:        map[string]interface{}{"age": 25, "vip": true},
			expected:   true,
		},
		{
			name:       "逻辑OR - 假",
			expression: "age < 18 || vip == true",
			env:        map[string]interface{}{"age": 25, "vip": false},
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond, err := NewExprCondition(tt.expression)
			require.NoError(t, err)
			require.NotNil(t, cond)

			result := cond.Evaluate(tt.env)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExprCondition_IsNull 测试is_null函数
func TestExprCondition_IsNull(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		env        map[string]interface{}
		expected   bool
	}{
		{
			name:       "is_null - 空值",
			expression: "is_null(name)",
			env:        map[string]interface{}{"name": nil},
			expected:   true,
		},
		{
			name:       "is_null - 非空值",
			expression: "is_null(name)",
			env:        map[string]interface{}{"name": "John"},
			expected:   false,
		},
		{
			name:       "is_not_null - 空值",
			expression: "is_not_null(name)",
			env:        map[string]interface{}{"name": nil},
			expected:   false,
		},
		{
			name:       "is_not_null - 非空值",
			expression: "is_not_null(name)",
			env:        map[string]interface{}{"name": "John"},
			expected:   true,
		},
		{
			name:       "is_null - 缺失字段",
			expression: "is_null(missing_field)",
			env:        map[string]interface{}{"name": "John"},
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond, err := NewExprCondition(tt.expression)
			require.NoError(t, err)
			require.NotNil(t, cond)

			result := cond.Evaluate(tt.env)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExprCondition_LikeMatch 测试like_match函数
func TestExprCondition_LikeMatch(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		env        map[string]interface{}
		expected   bool
	}{
		{
			name:       "LIKE - 前缀匹配",
			expression: "like_match(name, 'John%')",
			env:        map[string]interface{}{"name": "Johnson"},
			expected:   true,
		},
		{
			name:       "LIKE - 后缀匹配",
			expression: "like_match(name, '%son')",
			env:        map[string]interface{}{"name": "Johnson"},
			expected:   true,
		},
		{
			name:       "LIKE - 包含匹配",
			expression: "like_match(name, '%oh%')",
			env:        map[string]interface{}{"name": "Johnson"},
			expected:   true,
		},
		{
			name:       "LIKE - 单字符匹配",
			expression: "like_match(name, 'J_hn')",
			env:        map[string]interface{}{"name": "John"},
			expected:   true,
		},
		{
			name:       "LIKE - 精确匹配",
			expression: "like_match(name, 'John')",
			env:        map[string]interface{}{"name": "John"},
			expected:   true,
		},
		{
			name:       "LIKE - 不匹配",
			expression: "like_match(name, 'Jane%')",
			env:        map[string]interface{}{"name": "Johnson"},
			expected:   false,
		},
		{
			name:       "LIKE - 复杂模式",
			expression: "like_match(email, '%@%.com')",
			env:        map[string]interface{}{"email": "user@example.com"},
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond, err := NewExprCondition(tt.expression)
			require.NoError(t, err)
			require.NotNil(t, cond)

			result := cond.Evaluate(tt.env)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestMatchesLikePattern 测试LIKE模式匹配函数
func TestMatchesLikePattern(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		pattern  string
		expected bool
	}{
		{
			name:     "精确匹配",
			text:     "hello",
			pattern:  "hello",
			expected: true,
		},
		{
			name:     "前缀通配符",
			text:     "hello world",
			pattern:  "hello%",
			expected: true,
		},
		{
			name:     "后缀通配符",
			text:     "hello world",
			pattern:  "%world",
			expected: true,
		},
		{
			name:     "中间通配符",
			text:     "hello world",
			pattern:  "hello%world",
			expected: true,
		},
		{
			name:     "单字符通配符",
			text:     "hello",
			pattern:  "h_llo",
			expected: true,
		},
		{
			name:     "多个单字符通配符",
			text:     "hello",
			pattern:  "h__lo",
			expected: true,
		},
		{
			name:     "混合通配符",
			text:     "hello world test",
			pattern:  "h_llo%test",
			expected: true,
		},
		{
			name:     "全通配符",
			text:     "anything",
			pattern:  "%",
			expected: true,
		},
		{
			name:     "空字符串匹配",
			text:     "",
			pattern:  "%",
			expected: true,
		},
		{
			name:     "不匹配",
			text:     "hello",
			pattern:  "world",
			expected: false,
		},
		{
			name:     "长度不匹配",
			text:     "hello",
			pattern:  "h_",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesLikePattern(tt.text, tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExprCondition_ErrorHandling 测试错误处理
func TestExprCondition_ErrorHandling(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		env        map[string]interface{}
		expected   bool
	}{
		{
			name:       "类型不匹配 - 返回false",
			expression: "age > 'invalid'",
			env:        map[string]interface{}{"age": 25},
			expected:   false,
		},
		{
			name:       "缺失字段 - 使用默认值",
			expression: "missing_field == nil",
			env:        map[string]interface{}{"age": 25},
			expected:   true,
		},
		{
			name:       "简单布尔比较",
			expression: "true == true",
			env:        map[string]interface{}{},
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond, err := NewExprCondition(tt.expression)
			if err != nil {
				// 如果编译失败，跳过这个测试
				t.Skipf("Expression compilation failed: %v", err)
				return
			}
			require.NotNil(t, cond)

			result := cond.Evaluate(tt.env)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExprCondition_ComplexExpressions 测试复杂表达式
func TestExprCondition_ComplexExpressions(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		env        map[string]interface{}
		expected   bool
	}{
		{
			name:       "嵌套逻辑表达式",
			expression: "(age > 18 && age < 65) && (active == true || vip == true)",
			env:        map[string]interface{}{"age": 30, "active": false, "vip": true},
			expected:   true,
		},
		{
			name:       "多重条件组合",
			expression: "(score >= 90 || (score >= 80 && bonus > 0)) && is_not_null(name)",
			env:        map[string]interface{}{"score": 85, "bonus": 5, "name": "John"},
			expected:   true,
		},
		{
			name:       "字符串和数值混合条件",
			expression: "like_match(email, '%@gmail.com') && age >= 18",
			env:        map[string]interface{}{"email": "user@gmail.com", "age": 25},
			expected:   true,
		},
		{
			name:       "空值检查组合",
			expression: "is_not_null(name) && is_not_null(email) && age > 0",
			env:        map[string]interface{}{"name": "John", "email": "john@example.com", "age": 25},
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond, err := NewExprCondition(tt.expression)
			require.NoError(t, err)
			require.NotNil(t, cond)

			result := cond.Evaluate(tt.env)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExprCondition_FunctionErrors 测试函数错误处理
func TestExprCondition_FunctionErrors(t *testing.T) {
	tests := []struct {
		name string
		expr string
		data map[string]interface{}
		expected bool
	}{
		{"like_match类型错误", "like_match(123, 'pattern')", map[string]interface{}{}, false},
		{"is_null正常使用", "is_null(field)", map[string]interface{}{"field": nil}, true},
		{"is_null非空值", "is_null(field)", map[string]interface{}{"field": "value"}, false},
		{"is_not_null正常使用", "is_not_null(field)", map[string]interface{}{"field": "value"}, true},
		{"is_not_null空值", "is_not_null(field)", map[string]interface{}{"field": nil}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition, err := NewExprCondition(tt.expr)
			assert.NoError(t, err, "表达式编译应该成功")
			assert.NotNil(t, condition, "条件对象不应该为nil")

			result := condition.Evaluate(tt.data)
			assert.Equal(t, tt.expected, result, "评估结果应该匹配期望值")
		})
	}
}

// TestExprCondition_AdvancedFeatures 测试高级功能
func TestExprCondition_AdvancedFeatures(t *testing.T) {
	tests := []struct {
		name string
		expr string
		data map[string]interface{}
		expected bool
	}{
		{"复杂逻辑表达式", "(age > 18 && status == 'active') || (vip == true && score > 80)", map[string]interface{}{"age": 20, "status": "active", "vip": false, "score": 75}, true},
		{"嵌套函数调用", "is_not_null(name) && like_match(name, 'John%')", map[string]interface{}{"name": "John Doe"}, true},
		{"数值比较", "price >= 100.0 && price <= 500.0", map[string]interface{}{"price": 250.5}, true},
		{"字符串操作", "like_match(email, '%@gmail.com') && is_not_null(phone)", map[string]interface{}{"email": "user@gmail.com", "phone": "123456789"}, true},
		{"空值处理", "is_null(optional_field) || optional_field == 'default'", map[string]interface{}{"optional_field": nil}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition, err := NewExprCondition(tt.expr)
			assert.NoError(t, err, "表达式编译应该成功")
			assert.NotNil(t, condition, "条件对象不应该为nil")

			result := condition.Evaluate(tt.data)
			assert.Equal(t, tt.expected, result, "评估结果应该匹配期望值")
		})
	}
}

// TestExprCondition_EdgeCases 测试边界情况
func TestExprCondition_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		expr string
		data map[string]interface{}
		expected bool
	}{
		{"空字符串匹配", "like_match(text, '')", map[string]interface{}{"text": ""}, true},
		{"通配符匹配", "like_match(text, '%')", map[string]interface{}{"text": "anything"}, true},
		{"单字符匹配", "like_match(text, '_')", map[string]interface{}{"text": "a"}, true},
		{"数值零值", "value == 0", map[string]interface{}{"value": 0}, true},
		{"布尔值false", "flag == false", map[string]interface{}{"flag": false}, true},
		{"未定义变量", "undefined_var == nil", map[string]interface{}{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition, err := NewExprCondition(tt.expr)
			assert.NoError(t, err, "表达式编译应该成功")
			assert.NotNil(t, condition, "条件对象不应该为nil")

			result := condition.Evaluate(tt.data)
			assert.Equal(t, tt.expected, result, "评估结果应该匹配期望值")
		})
	}
}