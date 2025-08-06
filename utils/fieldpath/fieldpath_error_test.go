package fieldpath

import (
	"testing"
)

// TestSetNestedField 测试设置嵌套字段功能
func TestSetNestedField(t *testing.T) {
	tests := []struct {
		name      string
		data      map[string]interface{}
		path      string
		value     interface{}
		expectErr bool
	}{
		{
			name:  "设置简单字段",
			data:  make(map[string]interface{}),
			path:  "name",
			value: "test",
		},
		{
			name:  "设置嵌套字段",
			data:  make(map[string]interface{}),
			path:  "user.name",
			value: "John",
		},
		{
			name:      "空路径",
			data:      make(map[string]interface{}),
			path:      "",
			value:     "test",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SetNestedField(tt.data, tt.path, tt.value)
			if tt.expectErr {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestGetNestedFieldBasic 测试基本嵌套字段访问功能
func TestGetNestedFieldBasic(t *testing.T) {
	data := map[string]interface{}{
		"user": map[string]interface{}{
			"name": "John",
			"age":  30,
		},
		"items": []interface{}{
			map[string]interface{}{"id": 1, "name": "item1"},
			map[string]interface{}{"id": 2, "name": "item2"},
		},
	}

	tests := []struct {
		name     string
		path     string
		expected interface{}
		found    bool
	}{
		{
			name:     "简单字段访问",
			path:     "user.name",
			expected: "John",
			found:    true,
		},
		{
			name:     "数组索引访问",
			path:     "items[0].name",
			expected: "item1",
			found:    true,
		},
		{
			name:     "不存在的字段",
			path:     "user.email",
			expected: nil,
			found:    false,
		},
		{
			name:     "超出数组范围",
			path:     "items[10].name",
			expected: nil,
			found:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := GetNestedField(data, tt.path)
			if found != tt.found {
				t.Errorf("expected found: %v, got: %v", tt.found, found)
				return
			}
			if found && result != tt.expected {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}

// TestIsNestedField 测试嵌套字段检测功能
func TestIsNestedField(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		expected bool
	}{
		{
			name:     "简单字段",
			field:    "name",
			expected: false,
		},
		{
			name:     "嵌套字段",
			field:    "user.name",
			expected: true,
		},
		{
			name:     "数组访问",
			field:    "items[0]",
			expected: true,
		},
		{
			name:     "复杂路径",
			field:    "data.items[0].metadata.name",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsNestedField(tt.field)
			if result != tt.expected {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}

// TestFieldPathErrorHandling 测试错误处理
func TestFieldPathErrorHandling(t *testing.T) {
	data := map[string]interface{}{
		"valid": "value",
	}

	tests := []struct {
		name string
		path string
	}{
		{
			name: "空路径",
			path: "",
		},
		{
			name: "无效路径",
			path: "invalid.path",
		},
		{
			name: "nil数据",
			path: "valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var testData interface{}
			if tt.name != "nil数据" {
				testData = data
			}

			// 这些调用不应该panic
			_, _ = GetNestedField(testData, tt.path)
			_ = IsNestedField(tt.path)
		})
	}
}

// TestExtractTopLevelField 测试提取顶级字段功能
func TestExtractTopLevelField(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "简单字段",
			path:     "name",
			expected: "name",
		},
		{
			name:     "嵌套字段",
			path:     "user.name",
			expected: "user",
		},
		{
			name:     "数组访问",
			path:     "items[0]",
			expected: "items",
		},
		{
			name:     "空路径",
			path:     "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractTopLevelField(tt.path)
			if result != tt.expected {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}

// TestMapKeyAccess 测试Map键访问功能
func TestMapKeyAccess(t *testing.T) {
	data := map[string]interface{}{
		"stringMap": map[string]interface{}{
			"key1": "value1",
			"123":  "numericKey",
		},
		"intMap": map[int]interface{}{
			1: "intValue1",
			2: "intValue2",
		},
	}

	tests := []struct {
		name     string
		path     string
		expected interface{}
		found    bool
	}{
		{
			name:     "字符串键访问",
			path:     "stringMap['key1']",
			expected: "value1",
			found:    true,
		},
		{
			name:     "数字键访问",
			path:     "stringMap['123']",
			expected: "numericKey",
			found:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := GetNestedField(data, tt.path)
			if found != tt.found {
				t.Errorf("expected found: %v, got: %v", tt.found, found)
				return
			}
			if found && result != tt.expected {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}

// TestNegativeArrayIndex 测试负数组索引功能
func TestNegativeArrayIndex(t *testing.T) {
	data := map[string]interface{}{
		"items": []interface{}{"first", "second", "third"},
	}

	tests := []struct {
		name     string
		path     string
		expected interface{}
		found    bool
	}{
		{
			name:     "负索引访问最后一个元素",
			path:     "items[-1]",
			expected: "third",
			found:    true,
		},
		{
			name:     "负索引访问倒数第二个元素",
			path:     "items[-2]",
			expected: "second",
			found:    true,
		},
		{
			name:     "负索引超出范围",
			path:     "items[-10]",
			expected: nil,
			found:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := GetNestedField(data, tt.path)
			if found != tt.found {
				t.Errorf("expected found: %v, got: %v", tt.found, found)
				return
			}
			if found && result != tt.expected {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}

// TestFieldAccessErrorMessage 测试字段访问错误消息
func TestFieldAccessErrorMessage(t *testing.T) {
	err := &FieldAccessError{
		Path:    "test.path",
		Message: "test error",
	}

	expected := "field access error for path 'test.path': test error"
	if err.Error() != expected {
		t.Errorf("expected: %v, got: %v", expected, err.Error())
	}
}

// TestParseFieldPathErrors 测试ParseFieldPath的错误处理
func TestParseFieldPathErrors(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{
			name: "未匹配的左括号",
			path: "data[0",
		},
		{
			name: "无效的括号内容",
			path: "data[abc]",
		},
		{
			name: "空括号",
			path: "data[]",
		},
		{
			name: "未闭合的引号",
			path: "data['key]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseFieldPath(tt.path)
			if err == nil {
				t.Error("expected error but got none")
			}
		})
	}
}

// TestGetNestedFieldErrorCases 测试GetNestedField的错误情况
func TestGetNestedFieldErrorCases(t *testing.T) {
	tests := []struct {
		name     string
		data     interface{}
		path     string
		expected interface{}
		found    bool
	}{
		{
			name:     "nil数据",
			data:     nil,
			path:     "field",
			expected: nil,
			found:    false,
		},
		{
			name:     "空字符串路径",
			data:     map[string]interface{}{"test": "value"},
			path:     "",
			expected: nil,
			found:    false,
		},
		{
			name:     "指针数据",
			data:     &map[string]interface{}{"test": "value"},
			path:     "test",
			expected: "value",
			found:    true,
		},
		{
			name:     "nil指针",
			data:     (*map[string]interface{})(nil),
			path:     "test",
			expected: nil,
			found:    false,
		},
		{
			name:     "非map非struct数据",
			data:     "string data",
			path:     "field",
			expected: nil,
			found:    false,
		},
		{
			name:     "map中的数字键作为字符串",
			data:     map[string]interface{}{"123": "numeric key"},
			path:     "123",
			expected: "numeric key",
			found:    true,
		},
		{
			name:     "map中不存在的键",
			data:     map[string]interface{}{"existing": "value"},
			path:     "nonexistent",
			expected: nil,
			found:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, found := GetNestedField(tt.data, tt.path)
			if found != tt.found {
				t.Errorf("expected found: %v, got: %v", tt.found, found)
				return
			}
			if found && result != tt.expected {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}

// TestStructFieldAccess 测试结构体字段访问
func TestStructFieldAccess(t *testing.T) {
	type TestStruct struct {
		Name string
		Age  int
	}

	data := map[string]interface{}{
		"user": TestStruct{
			Name: "John",
			Age:  30,
		},
	}

	result, found := GetNestedField(data, "user.Name")
	if !found {
		t.Error("expected to find field")
	}
	if result != "John" {
		t.Errorf("expected: John, got: %v", result)
	}

	// 测试不存在的结构体字段
	result, found = GetNestedField(data, "user.NonExistent")
	if found {
		t.Error("expected not to find field")
	}

	// 测试结构体指针
	ptrData := map[string]interface{}{
		"user": &TestStruct{
			Name: "Jane",
			Age:  25,
		},
	}
	result, found = GetNestedField(ptrData, "user.Name")
	if !found {
		t.Error("expected to find field in struct pointer")
	}
	if result != "Jane" {
		t.Errorf("expected: Jane, got: %v", result)
	}
}

// 辅助函数
func createDeepNestedData(depth int) interface{} {
	if depth <= 0 {
		return "deep_value"
	}
	return map[string]interface{}{
		"level": createDeepNestedData(depth - 1),
	}
}

func createLargeArray(size int) []interface{} {
	arr := make([]interface{}, size)
	for i := 0; i < size; i++ {
		arr[i] = i
	}
	return arr
}

func createComplexData() interface{} {
	return map[string]interface{}{
		"data": map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app.kubernetes.io/name": "test-app",
						},
					},
				},
			},
		},
	}
}

func createCircularReference() interface{} {
	data := map[string]interface{}{
		"self": map[string]interface{}{},
	}
	data["self"].(map[string]interface{})["ref"] = data
	return data
}

func createLargeDataStructure() interface{} {
	data := make(map[string]interface{})
	for i := 0; i < 100; i++ { // 减少数据量避免测试超时
		data["key_"+string(rune('0'+i%10))] = map[string]interface{}{
			"nested": map[string]interface{}{
				"value": i,
			},
		}
	}
	return data
}

// TestSetNestedFieldErrors 测试SetNestedField的错误处理
func TestSetNestedFieldErrors(t *testing.T) {
	tests := []struct {
		name      string
		data      map[string]interface{}
		path      string
		value     interface{}
		expectErr bool
	}{
		{
			name:      "空路径",
			data:      make(map[string]interface{}),
			path:      "",
			value:     "test",
			expectErr: true,
		},
		{
			name:      "复杂路径中间部分包含数组索引",
			data:      make(map[string]interface{}),
			path:      "data[0].field",
			value:     "test",
			expectErr: true,
		},
		{
			name:      "最后部分不是字段",
			data:      make(map[string]interface{}),
			path:      "data.field[0]",
			value:     "test",
			expectErr: true,
		},
		{
			name:      "覆盖非map类型的中间值",
			data:      map[string]interface{}{"existing": "not a map"},
			path:      "existing.new",
			value:     "test",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SetNestedField(tt.data, tt.path, tt.value)
			if tt.expectErr {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestValidateFieldPathExtended 测试ValidateFieldPath函数的扩展情况
func TestValidateFieldPathExtended(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		expectErr bool
	}{
		{
			name:      "复杂嵌套路径",
			path:      "field.nested[0]['key'].deep",
			expectErr: false,
		},
		{
			name:      "多重数组访问",
			path:      "matrix[0][1][2]",
			expectErr: false,
		},
		{
			name:      "混合引号类型",
			path:      "field['key1'][\"key2\"]",
			expectErr: false,
		},
		{
			name:      "负数索引",
			path:      "array[-1]",
			expectErr: false,
		},
		{
			name:      "特殊字符在键中",
			path:      "field['key-with-dash']",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFieldPath(tt.path)
			if tt.expectErr {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestGetFieldPathDepthExtended 测试GetFieldPathDepth函数的扩展情况
func TestGetFieldPathDepthExtended(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected int
	}{
		{
			name:     "超深嵌套",
			path:     "a.b.c.d.e.f.g",
			expected: 7,
		},
		{
			name:     "多维数组",
			path:     "matrix[0][1][2][3]",
			expected: 5,
		},
		{
			name:     "混合复杂路径",
			path:     "data[0].items['key'].nested[1].value",
			expected: 7,
		},
		{
			name:     "无效路径回退测试",
			path:     "field[invalid.path",
			expected: 2, // 回退到简单计算: ["field[invalid", "path"]
		},
		{
			name:     "空括号内容",
			path:     "field[].nested",
			expected: 2, // 解析失败，回退到简单计算
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetFieldPathDepth(tt.path)
			if result != tt.expected {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}

// TestNormalizeFieldPathExtended 测试NormalizeFieldPath函数的扩展情况
func TestNormalizeFieldPathExtended(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "复杂混合路径",
			path:     "data[0].items['key'].nested[1]",
			expected: "data[0].items['key'].nested[1]",
		},
		{
			name:     "负数索引",
			path:     "array[-1]",
			expected: "array[-1]",
		},
		{
			name:     "多维数组",
			path:     "matrix[0][1][2]",
			expected: "matrix[0][1][2]",
		},
		{
			name:     "数字键",
			path:     "map[123]",
			expected: "map[123]",
		},
		{
			name:     "空字符串键",
			path:     "field['']",
			expected: "field['']",
		},
		{
			name:     "特殊字符键",
			path:     "field['key-with-special_chars']",
			expected: "field['key-with-special_chars']",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeFieldPath(tt.path)
			if result != tt.expected {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}

// TestGetAllReferencedFieldsExtended 测试GetAllReferencedFields函数的扩展情况
func TestGetAllReferencedFieldsExtended(t *testing.T) {
	tests := []struct {
		name     string
		paths    []string
		expected []string
	}{
		{
			name:     "复杂路径混合",
			paths:    []string{"data[0].items['key']", "config.database.host", "metrics[-1].value"},
			expected: []string{"data", "config", "metrics"},
		},
		{
			name:     "大量重复字段",
			paths:    []string{"user.name", "user.age", "user.profile.email", "user.settings.theme"},
			expected: []string{"user"},
		},
		{
			name:     "混合简单和复杂路径",
			paths:    []string{"simple", "complex.nested[0]['key']", "array[1]"},
			expected: []string{"simple", "complex", "array"},
		},
		{
			name:     "包含nil和空值",
			paths:    []string{"valid", "", "another.field"},
			expected: []string{"valid", "another"},
		},
		{
			name:     "特殊字符字段名",
			paths:    []string{"field-with-dash.nested", "field_with_underscore[0]"},
			expected: []string{"field-with-dash", "field_with_underscore"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetAllReferencedFields(tt.paths)
			// 由于返回的是map的键，顺序可能不同，所以需要检查长度和包含关系
			if len(result) != len(tt.expected) {
				t.Errorf("expected length: %v, got: %v", len(tt.expected), len(result))
				return
			}

			// 检查每个期望的字段都在结果中
			for _, expected := range tt.expected {
				found := false
				for _, actual := range result {
					if actual == expected {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected field %v not found in result %v", expected, result)
				}
			}
		})
	}
}

// TestMapWithIntKeys 测试整数键的Map访问
func TestMapWithIntKeys(t *testing.T) {
	data := map[string]interface{}{
		"intMap": map[interface{}]interface{}{
			1:   "value1",
			2:   "value2",
			"3": "value3", // 字符串键
		},
	}

	// 测试通过数组索引访问整数键Map（应该成功，因为会尝试整数键）
	result, found := GetNestedField(data, "intMap[1]")
	if !found {
		t.Error("expected to find int key when accessing with index")
	}
	if result != "value1" {
		t.Errorf("expected: value1, got: %v", result)
	}

	// 测试访问字符串键
	result, found = GetNestedField(data, "intMap[\"3\"]")
	if !found {
		t.Error("expected to find string key in map")
	}
	if result != "value3" {
		t.Errorf("expected: value3, got: %v", result)
	}

	// 测试不存在的键
	result, found = GetNestedField(data, "intMap[\"999\"]")
	if found {
		t.Error("expected not to find non-existent key")
	}
}

// TestArrayAsMapAccess 测试数组作为Map访问的情况
func TestArrayAsMapAccess(t *testing.T) {
	data := map[string]interface{}{
		"mixedMap": map[interface{}]interface{}{
			0:   "zero",
			"1": "one",
			2:   "two",
		},
	}

	// 测试数字键访问
	result, found := GetNestedField(data, "mixedMap[0]")
	if !found {
		t.Error("expected to find numeric key 0")
	}
	if result != "zero" {
		t.Errorf("expected: zero, got: %v", result)
	}

	// 测试字符串形式的数字键
	result, found = GetNestedField(data, "mixedMap['1']")
	if !found {
		t.Error("expected to find string key '1'")
	}
	if result != "one" {
		t.Errorf("expected: one, got: %v", result)
	}
}

// TestComplexErrorScenarios 测试复杂的错误场景
func TestComplexErrorScenarios(t *testing.T) {
	// 测试解析失败时的回退机制
	data := map[string]interface{}{
		"simple": map[string]interface{}{
			"field": "value",
		},
	}

	// 这个路径会导致解析失败，但应该回退到简单访问
	result, found := GetNestedField(data, "simple.field")
	if !found {
		t.Error("expected fallback to simple access to work")
	}
	if result != "value" {
		t.Errorf("expected: value, got: %v", result)
	}

	// 测试SetNestedField的解析失败回退
	testData := make(map[string]interface{})
	err := SetNestedField(testData, "simple.field", "test")
	if err != nil {
		t.Errorf("unexpected error in fallback: %v", err)
	}

	// 验证值被正确设置
	if testData["simple"].(map[string]interface{})["field"] != "test" {
		t.Error("fallback setting failed")
	}
}
