package functions

import (
	"reflect"
	"testing"
)

func TestUnnestFunction(t *testing.T) {
	fn := NewUnnestFunction()
	ctx := &FunctionContext{}

	// 测试基本unnest功能
	args := []interface{}{[]interface{}{"a", "b", "c"}}
	result, err := fn.Execute(ctx, args)
	if err != nil {
		t.Errorf("UnnestFunction should not return error: %v", err)
	}
	expected := []interface{}{
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__":          "a",
		},
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__":          "b",
		},
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__":          "c",
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("UnnestFunction = %v, want %v", result, expected)
	}

	// 测试对象数组unnest
	args = []interface{}{
		[]interface{}{
			map[string]interface{}{"name": "Alice", "age": 25},
			map[string]interface{}{"name": "Bob", "age": 30},
		},
	}
	result, err = fn.Execute(ctx, args)
	if err != nil {
		t.Errorf("UnnestFunction should not return error: %v", err)
	}
	expected = []interface{}{
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__":          map[string]interface{}{"name": "Alice", "age": 25},
		},
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__":          map[string]interface{}{"name": "Bob", "age": 30},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("UnnestFunction = %v, want %v", result, expected)
	}

	// 测试空数组
	args = []interface{}{[]interface{}{}}
	result, err = fn.Execute(ctx, args)
	if err != nil {
		t.Errorf("UnnestFunction should not return error for empty array: %v", err)
	}
	// 空数组应该返回带有空标记的结果
	expectedEmpty := []interface{}{
		map[string]interface{}{
			"__unnest_object__": true,
			"__empty_unnest__":  true,
		},
	}
	if !reflect.DeepEqual(result, expectedEmpty) {
		t.Errorf("UnnestFunction empty array = %v, want %v", result, expectedEmpty)
	}

	// 测试nil参数
	args = []interface{}{nil}
	result, err = fn.Execute(ctx, args)
	if err != nil {
		t.Errorf("UnnestFunction should not return error for nil: %v", err)
	}
	// nil应该返回带有空标记的结果
	expectedNil := []interface{}{
		map[string]interface{}{
			"__unnest_object__": true,
			"__empty_unnest__":  true,
		},
	}
	if !reflect.DeepEqual(result, expectedNil) {
		t.Errorf("UnnestFunction nil = %v, want %v", result, expectedNil)
	}

	// 测试错误参数数量
	args = []interface{}{}
	err = fn.Validate(args)
	if err == nil {
		t.Errorf("UnnestFunction should return error for no arguments")
	}

	// 测试非数组参数
	args = []interface{}{"not an array"}
	_, err = fn.Execute(ctx, args)
	if err == nil {
		t.Errorf("UnnestFunction should return error for non-array argument")
	}

	// 测试数组类型
	args = []interface{}{[3]string{"x", "y", "z"}}
	result, err = fn.Execute(ctx, args)
	if err != nil {
		t.Errorf("UnnestFunction should handle arrays: %v", err)
	}
	expected = []interface{}{
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__":          "x",
		},
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__":          "y",
		},
		map[string]interface{}{
			"__unnest_object__": true,
			"__data__":          "z",
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("UnnestFunction array = %v, want %v", result, expected)
	}
}

// TestUnnestFunctionCreation 测试UnnestFunction创建
func TestUnnestFunctionCreation(t *testing.T) {
	fn := NewUnnestFunction()
	if fn == nil {
		t.Error("NewUnnestFunction should not return nil")
	}

	if fn.GetName() != "unnest" {
		t.Errorf("Expected name 'unnest', got %s", fn.GetName())
	}

	// Test argument validation through Validate method
	err := fn.Validate([]interface{}{"test"})
	if err != nil {
		t.Errorf("Validate should accept 1 argument: %v", err)
	}

	err = fn.Validate([]interface{}{})
	if err == nil {
		t.Error("Validate should reject 0 arguments")
	}

	err = fn.Validate([]interface{}{"arg1", "arg2"})
	if err == nil {
		t.Error("Validate should reject 2 arguments")
	}

	if fn.GetType() == "" {
		t.Error("Function type should not be empty")
	}

	if fn.GetCategory() == "" {
		t.Error("Function category should not be empty")
	}

	if fn.GetDescription() == "" {
		t.Error("Function description should not be empty")
	}
}

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
