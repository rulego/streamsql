package functions

import (
	"reflect"
	"testing"
)

func TestIsUnnestResult(t *testing.T) {
	// 测试非unnest结果
	normalSlice := []interface{}{"a", "b", "c"}
	if IsUnnestResult(normalSlice) {
		t.Errorf("IsUnnestResult should return false for normal slice")
	}

	// 测试unnest结果
	unnestSlice := []interface{}{
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__": map[string]interface{}{
				"name": "Alice",
				"age":  25,
			},
		},
	}
	if !IsUnnestResult(unnestSlice) {
		t.Errorf("IsUnnestResult should return true for unnest slice")
	}

	// 测试混合结果
	mixedSlice := []interface{}{
		"normal",
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__": map[string]interface{}{
				"name": "Bob",
				"age":  30,
			},
		},
	}
	if !IsUnnestResult(mixedSlice) {
		t.Errorf("IsUnnestResult should return true for mixed slice")
	}

	// 测试非切片类型
	if IsUnnestResult("not a slice") {
		t.Errorf("IsUnnestResult should return false for non-slice")
	}
}

func TestProcessUnnestResult(t *testing.T) {
	// 测试处理普通数组
	normalSlice := []interface{}{"a", "b", "c"}
	result := ProcessUnnestResult(normalSlice)
	expected := []map[string]interface{}{
		{"value": "a"},
		{"value": "b"},
		{"value": "c"},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("ProcessUnnestResult normal slice = %v, want %v", result, expected)
	}

	// 测试处理对象数组
	objectSlice := []interface{}{
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__": map[string]interface{}{
				"name": "Alice",
				"age":  25,
			},
		},
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__": map[string]interface{}{
				"name": "Bob",
				"age":  30,
			},
		},
	}
	result = ProcessUnnestResult(objectSlice)
	expected = []map[string]interface{}{
		{"name": "Alice", "age": 25},
		{"name": "Bob", "age": 30},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("ProcessUnnestResult object slice = %v, want %v", result, expected)
	}

	// 测试混合数组
	mixedSlice := []interface{}{
		"normal",
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__": map[string]interface{}{
				"name": "Charlie",
				"age":  35,
			},
		},
		"another",
	}
	result = ProcessUnnestResult(mixedSlice)
	expected = []map[string]interface{}{
		{"value": "normal"},
		{"name": "Charlie", "age": 35},
		{"value": "another"},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("ProcessUnnestResult mixed slice = %v, want %v", result, expected)
	}

	// 测试非切片类型
	result = ProcessUnnestResult("not a slice")
	if result != nil {
		t.Errorf("ProcessUnnestResult should return nil for non-slice")
	}
}
