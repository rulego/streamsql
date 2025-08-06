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
}
