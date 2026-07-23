package functions

import (
	"reflect"
	"testing"
)

func TestUnnestFunction(t *testing.T) {
	fn := NewUnnestFunction()
	ctx := &FunctionContext{}

	// Test basic unnest functionality
	args := []any{[]any{"a", "b", "c"}}
	result, err := fn.Execute(ctx, args)
	if err != nil {
		t.Errorf("UnnestFunction should not return error: %v", err)
	}
	expected := []any{
		map[string]any{
			"__unnest_object__": true,
			"__data__":          "a",
		},
		map[string]any{
			"__unnest_object__": true,
			"__data__":          "b",
		},
		map[string]any{
			"__unnest_object__": true,
			"__data__":          "c",
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("UnnestFunction = %v, want %v", result, expected)
	}

	// The array of test objects is unnest
	args = []any{
		[]any{
			map[string]any{"name": "Alice", "age": 25},
			map[string]any{"name": "Bob", "age": 30},
		},
	}
	result, err = fn.Execute(ctx, args)
	if err != nil {
		t.Errorf("UnnestFunction should not return error: %v", err)
	}
	expected = []any{
		map[string]any{
			"__unnest_object__": true,
			"__data__":          map[string]any{"name": "Alice", "age": 25},
		},
		map[string]any{
			"__unnest_object__": true,
			"__data__":          map[string]any{"name": "Bob", "age": 30},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("UnnestFunction = %v, want %v", result, expected)
	}

	// Test the empty array
	args = []any{[]any{}}
	result, err = fn.Execute(ctx, args)
	if err != nil {
		t.Errorf("UnnestFunction should not return error for empty array: %v", err)
	}
	// An empty array should return results with an empty flag
	expectedEmpty := []any{
		map[string]any{
			"__unnest_object__": true,
			"__empty_unnest__":  true,
		},
	}
	if !reflect.DeepEqual(result, expectedEmpty) {
		t.Errorf("UnnestFunction empty array = %v, want %v", result, expectedEmpty)
	}

	// Test the nil parameters
	args = []any{nil}
	result, err = fn.Execute(ctx, args)
	if err != nil {
		t.Errorf("UnnestFunction should not return error for nil: %v", err)
	}
	// nil should return results with an empty marker
	expectedNil := []any{
		map[string]any{
			"__unnest_object__": true,
			"__empty_unnest__":  true,
		},
	}
	if !reflect.DeepEqual(result, expectedNil) {
		t.Errorf("UnnestFunction nil = %v, want %v", result, expectedNil)
	}

	// Test the number of error parameters
	args = []any{}
	err = fn.Validate(args)
	if err == nil {
		t.Errorf("UnnestFunction should return error for no arguments")
	}

	// Test non-array parameters
	args = []any{"not an array"}
	_, err = fn.Execute(ctx, args)
	if err == nil {
		t.Errorf("UnnestFunction should return error for non-array argument")
	}

	// Test array types
	args = []any{[3]string{"x", "y", "z"}}
	result, err = fn.Execute(ctx, args)
	if err != nil {
		t.Errorf("UnnestFunction should handle arrays: %v", err)
	}
	expected = []any{
		map[string]any{
			"__unnest_object__": true,
			"__data__":          "x",
		},
		map[string]any{
			"__unnest_object__": true,
			"__data__":          "y",
		},
		map[string]any{
			"__unnest_object__": true,
			"__data__":          "z",
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("UnnestFunction array = %v, want %v", result, expected)
	}
}

// TestUnnestFunctionCreation Tests UnnestFunction creation
func TestUnnestFunctionCreation(t *testing.T) {
	fn := NewUnnestFunction()
	if fn == nil {
		t.Error("NewUnnestFunction should not return nil")
	}

	if fn.GetName() != "unnest" {
		t.Errorf("Expected name 'unnest', got %s", fn.GetName())
	}

	// Test argument validation through Validate method
	err := fn.Validate([]any{"test"})
	if err != nil {
		t.Errorf("Validate should accept 1 argument: %v", err)
	}

	err = fn.Validate([]any{})
	if err == nil {
		t.Error("Validate should reject 0 arguments")
	}

	err = fn.Validate([]any{"arg1", "arg2"})
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
	// Test for non-unnest results
	normalSlice := []any{"a", "b", "c"}
	if IsUnnestResult(normalSlice) {
		t.Errorf("IsUnnestResult should return false for normal slice")
	}

	// Test the unnest results
	unnestSlice := []any{
		map[string]any{
			"__unnest_object__": true,
			"__data__": map[string]any{
				"name": "Alice",
				"age":  25,
			},
		},
	}
	if !IsUnnestResult(unnestSlice) {
		t.Errorf("IsUnnestResult should return true for unnest slice")
	}

	// Test mixed results
	mixedSlice := []any{
		"normal",
		map[string]any{
			"__unnest_object__": true,
			"__data__": map[string]any{
				"name": "Bob",
				"age":  30,
			},
		},
	}
	if !IsUnnestResult(mixedSlice) {
		t.Errorf("IsUnnestResult should return true for mixed slice")
	}

	// Test non-slice types
	if IsUnnestResult("not a slice") {
		t.Errorf("IsUnnestResult should return false for non-slice")
	}
}

func TestProcessUnnestResult(t *testing.T) {
	// Test processing of ordinary arrays
	normalSlice := []any{"a", "b", "c"}
	result := ProcessUnnestResult(normalSlice)
	expected := []map[string]any{
		{"value": "a"},
		{"value": "b"},
		{"value": "c"},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("ProcessUnnestResult normal slice = %v, want %v", result, expected)
	}

	// Test the array of processed objects
	objectSlice := []any{
		map[string]any{
			"__unnest_object__": true,
			"__data__": map[string]any{
				"name": "Alice",
				"age":  25,
			},
		},
		map[string]any{
			"__unnest_object__": true,
			"__data__": map[string]any{
				"name": "Bob",
				"age":  30,
			},
		},
	}
	result = ProcessUnnestResult(objectSlice)
	expected = []map[string]any{
		{"name": "Alice", "age": 25},
		{"name": "Bob", "age": 30},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("ProcessUnnestResult object slice = %v, want %v", result, expected)
	}

	// Test mixed arrays
	mixedSlice := []any{
		"normal",
		map[string]any{
			"__unnest_object__": true,
			"__data__": map[string]any{
				"name": "Charlie",
				"age":  35,
			},
		},
		"another",
	}
	result = ProcessUnnestResult(mixedSlice)
	expected = []map[string]any{
		{"value": "normal"},
		{"name": "Charlie", "age": 35},
		{"value": "another"},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("ProcessUnnestResult mixed slice = %v, want %v", result, expected)
	}

	// Test non-slice types
	result = ProcessUnnestResult("not a slice")
	if result != nil {
		t.Errorf("ProcessUnnestResult should return nil for non-slice")
	}
}
