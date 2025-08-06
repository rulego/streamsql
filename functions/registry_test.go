package functions

import "testing"

func TestRegistryEdgeCases(t *testing.T) {
	reg := NewFunctionRegistry()
	// Unregister未注册函数
	reg.Unregister("not_exist")
	// RegisterCustomFunction空名
	err := RegisterCustomFunction("", TypeCustom, "", "", 0, 0, nil)
	if err == nil {
		t.Error("RegisterCustomFunction should fail for empty name")
	}
	// RegisterCustomFunction重复注册
	f := func(ctx *FunctionContext, args []interface{}) (interface{}, error) { return nil, nil }
	err = RegisterCustomFunction("dup", TypeCustom, "", "", 0, 0, f)
	if err != nil {
		t.Errorf("RegisterCustomFunction failed: %v", err)
	}
	err = RegisterCustomFunction("dup", TypeCustom, "", "", 0, 0, f)
	if err == nil {
		t.Error("RegisterCustomFunction should fail for duplicate name")
	}
}
