package functions

import (
	"testing"
)

func TestExprFunction(t *testing.T) {
	fn := NewExprFunction()
	ctx := &FunctionContext{
		Data: map[string]interface{}{
			"x": 10,
			"y": 20,
		},
	}

	// 测试Execute方法
	_, err := fn.Execute(ctx, []interface{}{"x + y"})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	// 注意：这里的结果取决于表达式求值器的实现
	// 我们主要测试函数调用是否成功

	// 测试Validate方法
	err = fn.Validate([]interface{}{"test"})
	if err != nil {
		t.Errorf("Validate error: %v", err)
	}

	// 测试参数数量验证
	err = fn.Validate([]interface{}{})
	if err == nil {
		t.Errorf("Validate should fail for empty args")
	}

	err = fn.Validate([]interface{}{"arg1", "arg2"})
	if err == nil {
		t.Errorf("Validate should fail for too many args")
	}
}

func TestExprFunctionEdgeCases(t *testing.T) {
	fn := NewExprFunction()
	// Validate参数数量不符
	if err := fn.Validate([]interface{}{}); err == nil {
		t.Error("ExprFunction.Validate should fail for empty args")
	}
	if err := fn.Validate([]interface{}{"a", "b"}); err == nil {
		t.Error("ExprFunction.Validate should fail for too many args")
	}
	// Execute空参数
	_, err := fn.Execute(nil, []interface{}{})
	if err == nil {
		t.Error("ExprFunction.Execute should fail for empty args")
	}

	// 测试非字符串参数（现在应该成功）
	ctx := &FunctionContext{Data: map[string]interface{}{}}
	_, err = fn.Execute(ctx, []interface{}{123})
	if err != nil {
		t.Errorf("ExprFunction.Execute should accept non-string argument: %v", err)
	}

	// 测试无效表达式
	_, err = fn.Execute(ctx, []interface{}{"invalid expression +++"})
	if err == nil {
		t.Error("ExprFunction.Execute should fail for invalid expression")
	}
}

// TestExprFunctionCreation 测试ExprFunction的创建和属性
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

	if fn.GetCategory() == "" {
		t.Error("Function category should not be empty")
	}

	if fn.GetDescription() == "" {
		t.Error("Function description should not be empty")
	}
}

// TestExprFunctionWithDifferentExpressions 测试不同类型的表达式
func TestExprFunctionWithDifferentExpressions(t *testing.T) {
	fn := NewExprFunction()
	ctx := &FunctionContext{
		Data: map[string]interface{}{
			"x":      10,
			"y":      5,
			"name":   "John",
			"active": true,
		},
	}

	// 测试数学表达式
	result, err := fn.Execute(ctx, []interface{}{"x + y"})
	if err != nil {
		t.Errorf("Math expression failed: %v", err)
	}
	if result != 15 {
		t.Errorf("Expected 15, got %v", result)
	}

	// 测试比较表达式
	result, err = fn.Execute(ctx, []interface{}{"x > y"})
	if err != nil {
		t.Errorf("Comparison expression failed: %v", err)
	}
	if result != true {
		t.Errorf("Expected true, got %v", result)
	}

	// 测试字符串表达式
	result, err = fn.Execute(ctx, []interface{}{"name + ' Doe'"})
	if err != nil {
		t.Errorf("String expression failed: %v", err)
	}
	if result != "John Doe" {
		t.Errorf("Expected 'John Doe', got %v", result)
	}

	// 测试布尔表达式
	result, err = fn.Execute(ctx, []interface{}{"active && true"})
	if err != nil {
		t.Errorf("Boolean expression failed: %v", err)
	}
	if result != true {
		t.Errorf("Expected true, got %v", result)
	}

	// 测试复杂表达式
	result, err = fn.Execute(ctx, []interface{}{"(x + y) * 2"})
	if err != nil {
		t.Errorf("Complex expression failed: %v", err)
	}
	if result != 30 {
		t.Errorf("Expected 30, got %v", result)
	}
}

// TestExprFunctionWithFunctionCalls 测试函数调用表达式
func TestExprFunctionWithFunctionCalls(t *testing.T) {
	fn := NewExprFunction()
	ctx := &FunctionContext{
		Data: map[string]interface{}{
			"text": "Hello World",
			"num":  -42,
		},
	}

	// 测试abs函数调用
	result, err := fn.Execute(ctx, []interface{}{"abs(-10)"})
	if err != nil {
		t.Errorf("Function call expression failed: %v", err)
	}
	if result != float64(10) {
		t.Errorf("Expected 10, got %v", result)
	}

	// 测试length函数调用
	result, err = fn.Execute(ctx, []interface{}{"length(text)"})
	if err != nil {
		t.Errorf("Length function call failed: %v", err)
	}
	if result != 11 {
		t.Errorf("Expected 11, got %v", result)
	}

	// 测试组合函数调用
	result, err = fn.Execute(ctx, []interface{}{"abs(num) + length(text)"})
	if err != nil {
		t.Errorf("Combined function calls failed: %v", err)
	}
	expected := float64(53) // 42 + 11
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}
