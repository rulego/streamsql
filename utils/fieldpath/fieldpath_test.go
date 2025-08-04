package fieldpath

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFieldPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected []FieldPart
		hasError bool
	}{
		{
			name: "简单字段",
			path: "name",
			expected: []FieldPart{
				{Type: "field", Name: "name"},
			},
		},
		{
			name: "嵌套字段",
			path: "user.profile.name",
			expected: []FieldPart{
				{Type: "field", Name: "user"},
				{Type: "field", Name: "profile"},
				{Type: "field", Name: "name"},
			},
		},
		{
			name: "数组索引",
			path: "data[0]",
			expected: []FieldPart{
				{Type: "field", Name: "data"},
				{Type: "array_index", Index: 0, Key: "0", KeyType: "number"},
			},
		},
		{
			name: "数组索引与字段",
			path: "users[1].name",
			expected: []FieldPart{
				{Type: "field", Name: "users"},
				{Type: "array_index", Index: 1, Key: "1", KeyType: "number"},
				{Type: "field", Name: "name"},
			},
		},
		{
			name: "字符串键",
			path: "config['database']",
			expected: []FieldPart{
				{Type: "field", Name: "config"},
				{Type: "map_key", Key: "database", KeyType: "string"},
			},
		},
		{
			name: "双引号字符串键",
			path: "settings[\"timeout\"]",
			expected: []FieldPart{
				{Type: "field", Name: "settings"},
				{Type: "map_key", Key: "timeout", KeyType: "string"},
			},
		},
		{
			name: "负数索引",
			path: "items[-1]",
			expected: []FieldPart{
				{Type: "field", Name: "items"},
				{Type: "array_index", Index: -1, Key: "-1", KeyType: "number"},
			},
		},
		{
			name: "混合复杂访问",
			path: "users[0].profile['name']",
			expected: []FieldPart{
				{Type: "field", Name: "users"},
				{Type: "array_index", Index: 0, Key: "0", KeyType: "number"},
				{Type: "field", Name: "profile"},
				{Type: "map_key", Key: "name", KeyType: "string"},
			},
		},
		{
			name: "多维数组",
			path: "matrix[1][2]",
			expected: []FieldPart{
				{Type: "field", Name: "matrix"},
				{Type: "array_index", Index: 1, Key: "1", KeyType: "number"},
				{Type: "array_index", Index: 2, Key: "2", KeyType: "number"},
			},
		},
		{
			name:     "无效括号",
			path:     "data[abc",
			hasError: true,
		},
		{
			name:     "无效键格式",
			path:     "data[abc]",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accessor, err := ParseFieldPath(tt.path)

			if tt.hasError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, accessor)
			assert.Equal(t, tt.expected, accessor.Parts)
		})
	}
}

func TestGetNestedFieldComplex(t *testing.T) {
	// 创建复杂的测试数据
	testData := map[string]interface{}{
		"users": []interface{}{
			map[string]interface{}{
				"id": 1,
				"profile": map[string]interface{}{
					"name":  "Alice",
					"email": "alice@example.com",
					"preferences": map[string]interface{}{
						"theme": "dark",
						"lang":  "en",
					},
				},
				"scores": []interface{}{95, 87, 92},
			},
			map[string]interface{}{
				"id": 2,
				"profile": map[string]interface{}{
					"name":  "Bob",
					"email": "bob@example.com",
				},
				"scores": []interface{}{88, 94, 89},
			},
		},
		"config": map[string]interface{}{
			"database": "mysql://localhost:3306",
			"redis":    "redis://localhost:6379",
			"settings": map[string]interface{}{
				"timeout": 5000,
				"retries": 3,
			},
		},
		"matrix": []interface{}{
			[]interface{}{1, 2, 3},
			[]interface{}{4, 5, 6},
			[]interface{}{7, 8, 9},
		},
	}

	tests := []struct {
		name     string
		path     string
		expected interface{}
		found    bool
	}{
		{
			name:     "数组索引访问",
			path:     "users[0]",
			expected: testData["users"].([]interface{})[0],
			found:    true,
		},
		{
			name:     "数组元素字段",
			path:     "users[1].profile.name",
			expected: "Bob",
			found:    true,
		},
		{
			name:     "嵌套Map键访问",
			path:     "users[0].profile['name']",
			expected: "Alice",
			found:    true,
		},
		{
			name:     "Map键访问",
			path:     "config['database']",
			expected: "mysql://localhost:3306",
			found:    true,
		},
		{
			name:     "嵌套配置访问",
			path:     "config.settings['timeout']",
			expected: 5000,
			found:    true,
		},
		{
			name:     "数组中的数组",
			path:     "users[0].scores[2]",
			expected: 92,
			found:    true,
		},
		{
			name:     "二维数组访问",
			path:     "matrix[1][2]",
			expected: 6,
			found:    true,
		},
		{
			name:     "负数索引",
			path:     "users[-1].profile.name",
			expected: "Bob",
			found:    true,
		},
		{
			name:     "负数索引访问数组",
			path:     "users[0].scores[-1]",
			expected: 92,
			found:    true,
		},
		{
			name:  "不存在的字段",
			path:  "users[0].profile.nonexistent",
			found: false,
		},
		{
			name:  "超出索引范围",
			path:  "users[10].name",
			found: false,
		},
		{
			name:  "不存在的键",
			path:  "config['nonexistent']",
			found: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := GetNestedField(testData, tt.path)
			assert.Equal(t, tt.found, found, "found状态应该匹配")
			if tt.found {
				assert.Equal(t, tt.expected, result, "结果值应该匹配")
			}
		})
	}
}

func TestSetNestedFieldComplex(t *testing.T) {
	t.Run("设置简单嵌套字段", func(t *testing.T) {
		data := make(map[string]interface{})
		err := SetNestedField(data, "user.profile.name", "Alice")
		assert.NoError(t, err)

		result, found := GetNestedField(data, "user.profile.name")
		assert.True(t, found)
		assert.Equal(t, "Alice", result)
	})

	t.Run("设置到现有数据", func(t *testing.T) {
		data := map[string]interface{}{
			"user": map[string]interface{}{
				"id": 1,
			},
		}

		err := SetNestedField(data, "user.profile.name", "Bob")
		assert.NoError(t, err)

		result, found := GetNestedField(data, "user.profile.name")
		assert.True(t, found)
		assert.Equal(t, "Bob", result)

		// 确保原有数据仍然存在
		id, found := GetNestedField(data, "user.id")
		assert.True(t, found)
		assert.Equal(t, 1, id)
	})

	t.Run("覆盖非Map类型", func(t *testing.T) {
		data := map[string]interface{}{
			"user": "string_value",
		}

		err := SetNestedField(data, "user.profile.name", "Charlie")
		assert.NoError(t, err)

		result, found := GetNestedField(data, "user.profile.name")
		assert.True(t, found)
		assert.Equal(t, "Charlie", result)
	})
}

func TestIsNestedFieldComplex(t *testing.T) {
	tests := []struct {
		field    string
		expected bool
	}{
		{"name", false},
		{"user.name", true},
		{"data[0]", true},
		{"users[1].name", true},
		{"config['key']", true},
		{"matrix[0][1]", true},
		{"a.b.c.d", true},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			result := IsNestedField(tt.field)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractTopLevelFieldComplex(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"name", "name"},
		{"user.profile.name", "user"},
		{"data[0]", "data"},
		{"users[1].name", "users"},
		{"config['database']", "config"},
		{"matrix[0][1]", "matrix"},
		{"a.b[0].c['key']", "a"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := ExtractTopLevelField(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateFieldPath(t *testing.T) {
	tests := []struct {
		path     string
		hasError bool
	}{
		{"name", false},
		{"user.profile.name", false},
		{"data[0]", false},
		{"users[1].name", false},
		{"config['database']", false},
		{"matrix[0][1]", false},
		{"data[abc]", true}, // 无效括号内容
		{"data[", true},     // 未闭合括号
		{"data]", false},    // 仅右括号不算错误（当作普通字段名）
		{"", true},          // 空路径
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			err := ValidateFieldPath(tt.path)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNormalizeFieldPath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"name", "name"},
		{"user.profile.name", "user.profile.name"},
		{"data[0]", "data[0]"},
		{"users[1].name", "users[1].name"},
		{"config['database']", "config['database']"},
		{"matrix[0][1]", "matrix[0][1]"},
		{"a.b[0].c['key']", "a.b[0].c['key']"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := NormalizeFieldPath(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetFieldPathDepth(t *testing.T) {
	tests := []struct {
		path     string
		expected int
	}{
		{"", 0},
		{"name", 1},
		{"user.name", 2},
		{"data[0]", 2},
		{"users[1].name", 3},
		{"config['database']", 2},
		{"matrix[0][1]", 3},
		{"a.b.c.d", 4},
		{"users[0].profile['name']", 4},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := GetFieldPathDepth(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestGetAllReferencedFields 测试GetAllReferencedFields函数
func TestGetAllReferencedFields(t *testing.T) {
	tests := []struct {
		name      string
		fieldPaths []string
		expected  []string
	}{
		{
			name:      "空列表",
			fieldPaths: []string{},
			expected:  []string{},
		},
		{
			name:      "单个简单字段",
			fieldPaths: []string{"name"},
			expected:  []string{"name"},
		},
		{
			name:      "多个不同顶级字段",
			fieldPaths: []string{"device.info.name", "sensor.temperature", "data[0].value"},
			expected:  []string{"device", "sensor", "data"},
		},
		{
			name:      "重复顶级字段",
			fieldPaths: []string{"user.name", "user.email", "user.profile.age"},
			expected:  []string{"user"},
		},
		{
			name:      "包含空字符串",
			fieldPaths: []string{"user.name", "", "device.id"},
			expected:  []string{"user", "device"},
		},
		{
			name:      "数组和map访问",
			fieldPaths: []string{"items[0].name", "config['database']", "matrix[1][2]"},
			expected:  []string{"items", "config", "matrix"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetAllReferencedFields(tt.fieldPaths)
			// 由于返回的是map的keys，顺序可能不同，所以需要排序比较
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

// TestFieldAccessError 测试FieldAccessError类型
func TestFieldAccessError(t *testing.T) {
	err := &FieldAccessError{
		Path:    "invalid.path[abc]",
		Message: "invalid bracket content",
	}

	expected := "field access error for path 'invalid.path[abc]': invalid bracket content"
	assert.Equal(t, expected, err.Error())
}

// TestSetNestedFieldEdgeCases 测试SetNestedField函数的边缘情况
func TestSetNestedFieldEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		fieldPath string
		value     interface{}
		hasError  bool
		errorMsg  string
	}{
		{
			name:      "空字段路径",
			fieldPath: "",
			value:     "test",
			hasError:  true,
			errorMsg:  "empty field path",
		},
		{
			name:      "无效字段路径",
			fieldPath: "field[abc]",
			value:     "test",
			hasError:  false, // 会fallback到简单设置
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make(map[string]interface{})
			err := SetNestedField(data, tt.fieldPath, tt.value)

			if tt.hasError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestGetNestedFieldEdgeCases 测试GetNestedField函数的边缘情况
func TestGetNestedFieldEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		data      interface{}
		fieldPath string
		expected  interface{}
		found     bool
	}{
		{
			name:      "空字段路径",
			data:      map[string]interface{}{"test": "value"},
			fieldPath: "",
			expected:  nil,
			found:     false,
		},
		{
			name:      "nil数据",
			data:      nil,
			fieldPath: "test",
			expected:  nil,
			found:     false,
		},
		{
			name: "数组越界访问",
			data: map[string]interface{}{
				"items": []interface{}{"a", "b"},
			},
			fieldPath: "items[5]",
			expected:  nil,
			found:     false,
		},
		{
			name: "负数索引访问",
			data: map[string]interface{}{
				"items": []interface{}{"a", "b", "c"},
			},
			fieldPath: "items[-1]",
			expected:  "c",
			found:     true,
		},
		{
			name: "map中不存在的键",
			data: map[string]interface{}{
				"config": map[string]interface{}{"key1": "value1"},
			},
			fieldPath: "config['nonexistent']",
			expected:  nil,
			found:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := GetNestedField(tt.data, tt.fieldPath)
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.found, found)
		})
	}
}
