package functions

import (
	"testing"
)

func TestExprFunction(t *testing.T) {
	fn := NewExprFunction()
	ctx := &FunctionContext{
		Data: map[string]any{
			"x": 10,
			"y": 20,
		},
	}

	// Test the Execute method
	_, err := fn.Execute(ctx, []any{"x + y"})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	// Note: The result here depends on the implementation of the expression evaluator
	// We mainly test whether the function call is successful

	// Test the Validate method
	err = fn.Validate([]any{"test"})
	if err != nil {
		t.Errorf("Validate error: %v", err)
	}

	// Verification of test parameter quantities
	err = fn.Validate([]any{})
	if err == nil {
		t.Errorf("Validate should fail for empty args")
	}

	err = fn.Validate([]any{"arg1", "arg2"})
	if err == nil {
		t.Errorf("Validate should fail for too many args")
	}
}

func TestExprFunctionEdgeCases(t *testing.T) {
	fn := NewExprFunction()
	// The number of validate parameters does not match
	if err := fn.Validate([]any{}); err == nil {
		t.Error("ExprFunction.Validate should fail for empty args")
	}
	if err := fn.Validate([]any{"a", "b"}); err == nil {
		t.Error("ExprFunction.Validate should fail for too many args")
	}
	// Execute empty parameters
	_, err := fn.Execute(nil, []any{})
	if err == nil {
		t.Error("ExprFunction.Execute should fail for empty args")
	}

	// Testing non-string parameters (should now succeed)
	ctx := &FunctionContext{Data: map[string]any{}}
	_, err = fn.Execute(ctx, []any{123})
	if err != nil {
		t.Errorf("ExprFunction.Execute should accept non-string argument: %v", err)
	}

	// Test for invalid expressions
	_, err = fn.Execute(ctx, []any{"invalid expression +++"})
	if err == nil {
		t.Error("ExprFunction.Execute should fail for invalid expression")
	}
}

// TestExprFunctionCreation tests the creation and properties of ExprFunction
func TestExprFunctionCreation(t *testing.T) {
	fn := NewExprFunction()
	if fn == nil {
		t.Error("NewExprFunction should not return nil")
	}

	if fn.GetName() != "expr" {
		t.Errorf("Expected name 'expr', got %s", fn.GetName())
	}

	if fn.GetType() != TypeString {
		t.Errorf("Expected type %s, got %s", TypeString, fn.GetType())
	}

	// BaseFunction doesn't expose GetMinArgs/GetMaxArgs methods
	// We can only test through Validate method
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

	if fn.GetCategory() == "" {
		t.Error("Function category should not be empty")
	}

	if fn.GetDescription() == "" {
		t.Error("Function description should not be empty")
	}
}

// TestExprFunctionWithDifferentExpressions tests expressions of different types
func TestExprFunctionWithDifferentExpressions(t *testing.T) {
	fn := NewExprFunction()
	ctx := &FunctionContext{
		Data: map[string]any{
			"x":      10,
			"y":      5,
			"name":   "John",
			"active": true,
		},
	}

	// Test mathematical expressions
	result, err := fn.Execute(ctx, []any{"x + y"})
	if err != nil {
		t.Errorf("Math expression failed: %v", err)
	}
	if result != 15 {
		t.Errorf("Expected 15, got %v", result)
	}

	// Test the comparison expression
	result, err = fn.Execute(ctx, []any{"x > y"})
	if err != nil {
		t.Errorf("Comparison expression failed: %v", err)
	}
	if result != true {
		t.Errorf("Expected true, got %v", result)
	}

	// Test string expressions
	result, err = fn.Execute(ctx, []any{"name + ' Doe'"})
	if err != nil {
		t.Errorf("String expression failed: %v", err)
	}
	if result != "John Doe" {
		t.Errorf("Expected 'John Doe', got %v", result)
	}

	// Test the Boolean expression
	result, err = fn.Execute(ctx, []any{"active && true"})
	if err != nil {
		t.Errorf("Boolean expression failed: %v", err)
	}
	if result != true {
		t.Errorf("Expected true, got %v", result)
	}

	// Test complex expressions
	result, err = fn.Execute(ctx, []any{"(x + y) * 2"})
	if err != nil {
		t.Errorf("Complex expression failed: %v", err)
	}
	if result != 30 {
		t.Errorf("Expected 30, got %v", result)
	}
}

// TestExprFunctionWithFunctionCalls is a test function call expression
func TestExprFunctionWithFunctionCalls(t *testing.T) {
	fn := NewExprFunction()
	ctx := &FunctionContext{
		Data: map[string]any{
			"text": "Hello World",
			"num":  -42,
		},
	}

	// Testing the abs function call
	result, err := fn.Execute(ctx, []any{"abs(-10)"})
	if err != nil {
		t.Errorf("Function call expression failed: %v", err)
	}
	if result != float64(10) {
		t.Errorf("Expected 10, got %v", result)
	}

	// Test the length function call
	result, err = fn.Execute(ctx, []any{"length(text)"})
	if err != nil {
		t.Errorf("Length function call failed: %v", err)
	}
	if result != 11 {
		t.Errorf("Expected 11, got %v", result)
	}

	// Test composer function calls
	result, err = fn.Execute(ctx, []any{"abs(num) + length(text)"})
	if err != nil {
		t.Errorf("Combined function calls failed: %v", err)
	}
	expected := float64(53) // 42 + 11
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}
