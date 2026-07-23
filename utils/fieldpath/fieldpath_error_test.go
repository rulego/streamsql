package fieldpath

import (
	"testing"
)

// TestSetNestedField tests the nested field function
func TestSetNestedField(t *testing.T) {
	tests := []struct {
		name      string
		data      map[string]any
		path      string
		value     any
		expectErr bool
	}{
		{
			name:  "设置简单字段",
			data:  make(map[string]any),
			path:  "name",
			value: "test",
		},
		{
			name:  "设置嵌套字段",
			data:  make(map[string]any),
			path:  "user.name",
			value: "John",
		},
		{
			name:      "空路径",
			data:      make(map[string]any),
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

// TestGetNestedFieldBasic tests basic nested field access functionality
func TestGetNestedFieldBasic(t *testing.T) {
	data := map[string]any{
		"user": map[string]any{
			"name": "John",
			"age":  30,
		},
		"items": []any{
			map[string]any{"id": 1, "name": "item1"},
			map[string]any{"id": 2, "name": "item2"},
		},
	}

	tests := []struct {
		name     string
		path     string
		expected any
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

// TestIsNestedField Tests nested field detection functionality
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

// TestFieldPathErrorHandling Test error handling
func TestFieldPathErrorHandling(t *testing.T) {
	data := map[string]any{
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
			var testData any
			if tt.name != "nil数据" {
				testData = data
			}

			// These calls shouldn't panic
			_, _ = GetNestedField(testData, tt.path)
			_ = IsNestedField(tt.path)
		})
	}
}

// TestExtractTopLevelField tests the top-level field extraction function
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

// TestMapKeyAccess Tests the Map key access function
func TestMapKeyAccess(t *testing.T) {
	data := map[string]any{
		"stringMap": map[string]any{
			"key1": "value1",
			"123":  "numericKey",
		},
		"intMap": map[int]any{
			1: "intValue1",
			2: "intValue2",
		},
	}

	tests := []struct {
		name     string
		path     string
		expected any
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

// TestNegativeArrayIndex tests the negative array index function
func TestNegativeArrayIndex(t *testing.T) {
	data := map[string]any{
		"items": []any{"first", "second", "third"},
	}

	tests := []struct {
		name     string
		path     string
		expected any
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

// TestFieldAccessErrorMessage (Test field access error message).
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

// TestParseFieldPathErrors Test the error handling of ParseFieldPath
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

// TestGetNestedFieldErrorCases Tests for GetNestedField errors
func TestGetNestedFieldErrorCases(t *testing.T) {
	tests := []struct {
		name     string
		data     any
		path     string
		expected any
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
			data:     map[string]any{"test": "value"},
			path:     "",
			expected: nil,
			found:    false,
		},
		{
			name:     "指针数据",
			data:     &map[string]any{"test": "value"},
			path:     "test",
			expected: "value",
			found:    true,
		},
		{
			name:     "nil指针",
			data:     (*map[string]any)(nil),
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
			data:     map[string]any{"123": "numeric key"},
			path:     "123",
			expected: "numeric key",
			found:    true,
		},
		{
			name:     "map中不存在的键",
			data:     map[string]any{"existing": "value"},
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

// TestStructFieldAccess tests the field access to the structure field
func TestStructFieldAccess(t *testing.T) {
	type TestStruct struct {
		Name string
		Age  int
	}

	data := map[string]any{
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

	// Test the nonexistent structure field
	result, found = GetNestedField(data, "user.NonExistent")
	if found {
		t.Error("expected not to find field")
	}

	// Test the structure pointer
	ptrData := map[string]any{
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

// Auxiliary function
func createDeepNestedData(depth int) any {
	if depth <= 0 {
		return "deep_value"
	}
	return map[string]any{
		"level": createDeepNestedData(depth - 1),
	}
}

func createLargeArray(size int) []any {
	arr := make([]any, size)
	for i := 0; i < size; i++ {
		arr[i] = i
	}
	return arr
}

func createComplexData() any {
	return map[string]any{
		"data": map[string]any{
			"items": []any{
				map[string]any{
					"metadata": map[string]any{
						"labels": map[string]any{
							"app.kubernetes.io/name": "test-app",
						},
					},
				},
			},
		},
	}
}

func createCircularReference() any {
	data := map[string]any{
		"self": map[string]any{},
	}
	data["self"].(map[string]any)["ref"] = data
	return data
}

func createLargeDataStructure() any {
	data := make(map[string]any)
	for i := 0; i < 100; i++ { // Reduce data volume to avoid test timeouts
		data["key_"+string(rune('0'+i%10))] = map[string]any{
			"nested": map[string]any{
				"value": i,
			},
		}
	}
	return data
}

// TestSetNestedFieldErrors: Tests the error handling of SetNestedField
func TestSetNestedFieldErrors(t *testing.T) {
	tests := []struct {
		name      string
		data      map[string]any
		path      string
		value     any
		expectErr bool
	}{
		{
			name:      "空路径",
			data:      make(map[string]any),
			path:      "",
			value:     "test",
			expectErr: true,
		},
		{
			name:      "复杂路径中间部分包含数组索引",
			data:      make(map[string]any),
			path:      "data[0].field",
			value:     "test",
			expectErr: true,
		},
		{
			name:      "最后部分不是字段",
			data:      make(map[string]any),
			path:      "data.field[0]",
			value:     "test",
			expectErr: true,
		},
		{
			name:      "覆盖非map类型的中间值",
			data:      map[string]any{"existing": "not a map"},
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

// TestValidateFieldPathExtended tests the extension of the ValidateFieldPath function
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

// TestGetFieldPathDepthExtended tests the extension of the GetFieldPathDepth function
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
			expected: 2, // Backtrack to simple calculation: ["field[invalid", "path"]
		},
		{
			name:     "空括号内容",
			path:     "field[].nested",
			expected: 2, // Parsing fails, reverting to simple calculations
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

// TestNormalizeFieldPathExtended tests the extension of the NormalizeFieldPath function
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

// TestGetAllReferencedFieldsExtended tests the extension of the GetAllReferencedFields function
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
			// Since the returned map keys may be in different order, you need to check the length and inclusion relationships
			if len(result) != len(tt.expected) {
				t.Errorf("expected length: %v, got: %v", len(tt.expected), len(result))
				return
			}

			// Check that every desired field is included in the results
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

// TestMapWithIntKeys tests the map access to integer keys
func TestMapWithIntKeys(t *testing.T) {
	data := map[string]any{
		"intMap": map[any]any{
			1:   "value1",
			2:   "value2",
			"3": "value3", // String keys
		},
	}

	// Test to access integer keys via array index (should succeed, since integer keys are attempted)
	result, found := GetNestedField(data, "intMap[1]")
	if !found {
		t.Error("expected to find int key when accessing with index")
	}
	if result != "value1" {
		t.Errorf("expected: value1, got: %v", result)
	}

	// Test access string keys
	result, found = GetNestedField(data, "intMap[\"3\"]")
	if !found {
		t.Error("expected to find string key in map")
	}
	if result != "value3" {
		t.Errorf("expected: value3, got: %v", result)
	}

	// Test keys that don't exist
	result, found = GetNestedField(data, "intMap[\"999\"]")
	if found {
		t.Error("expected not to find non-existent key")
	}
}

// TestArrayAsMapAccess Tests the array as a Map access
func TestArrayAsMapAccess(t *testing.T) {
	data := map[string]any{
		"mixedMap": map[any]any{
			0:   "zero",
			"1": "one",
			2:   "two",
		},
	}

	// Test the numeric keys
	result, found := GetNestedField(data, "mixedMap[0]")
	if !found {
		t.Error("expected to find numeric key 0")
	}
	if result != "zero" {
		t.Errorf("expected: zero, got: %v", result)
	}

	// Test the number key in string form
	result, found = GetNestedField(data, "mixedMap['1']")
	if !found {
		t.Error("expected to find string key '1'")
	}
	if result != "one" {
		t.Errorf("expected: one, got: %v", result)
	}
}

// TestTypedMapMismatchedKey verifies that accessing a typed map with a key
// whose type does not match the map's key type returns not-found instead of
// panicking inside reflect.MapIndex.
func TestTypedMapMismatchedKey(t *testing.T) {
	// JSON object decoded as map[string]any, accessed via an array index.
	data := map[string]any{
		"obj": map[string]any{"name": "alice"},
	}
	if _, found := GetNestedField(data, "obj[0]"); found {
		t.Error("expected not-found for string-key map accessed by index")
	}

	// int-key map accessed via a string key.
	intKey := map[string]any{
		"imap": map[int]string{1: "v1"},
	}
	if _, found := GetNestedField(intKey, `imap["1"]`); found {
		t.Error("expected not-found for int-key map accessed by string key")
	}

	// Positive control: normal string-key access still works.
	if v, found := GetNestedField(data, `obj["name"]`); !found || v != "alice" {
		t.Errorf("expected alice, got %v (found=%v)", v, found)
	}
}

// TestComplexErrorScenarios: Tests complex error scenarios
func TestComplexErrorScenarios(t *testing.T) {
	// Testing the rollback mechanism when parsing fails
	data := map[string]any{
		"simple": map[string]any{
			"field": "value",
		},
	}

	// This path will cause parsing failures, but it should be reverted to simple access
	result, found := GetNestedField(data, "simple.field")
	if !found {
		t.Error("expected fallback to simple access to work")
	}
	if result != "value" {
		t.Errorf("expected: value, got: %v", result)
	}

	// Test the parsing failure of SetNestedField and rollback
	testData := make(map[string]any)
	err := SetNestedField(testData, "simple.field", "test")
	if err != nil {
		t.Errorf("unexpected error in fallback: %v", err)
	}

	// Verification values are correctly set
	if testData["simple"].(map[string]any)["field"] != "test" {
		t.Error("fallback setting failed")
	}
}
