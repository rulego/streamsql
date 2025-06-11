package functions

import (
	"reflect"
)

// IsNullFunction 检查是否为NULL
type IsNullFunction struct {
	*BaseFunction
}

func NewIsNullFunction() *IsNullFunction {
	return &IsNullFunction{
		BaseFunction: NewBaseFunction("is_null", TypeString, "类型检查函数", "检查是否为NULL", 1, 1),
	}
}

func (f *IsNullFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *IsNullFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return args[0] == nil, nil
}

// IsNotNullFunction 检查是否不为NULL
type IsNotNullFunction struct {
	*BaseFunction
}

func NewIsNotNullFunction() *IsNotNullFunction {
	return &IsNotNullFunction{
		BaseFunction: NewBaseFunction("is_not_null", TypeString, "类型检查函数", "检查是否不为NULL", 1, 1),
	}
}

func (f *IsNotNullFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *IsNotNullFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return args[0] != nil, nil
}

// IsNumericFunction 检查是否为数字类型
type IsNumericFunction struct {
	*BaseFunction
}

func NewIsNumericFunction() *IsNumericFunction {
	return &IsNumericFunction{
		BaseFunction: NewBaseFunction("is_numeric", TypeString, "类型检查函数", "检查是否为数字类型", 1, 1),
	}
}

func (f *IsNumericFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *IsNumericFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if args[0] == nil {
		return false, nil
	}

	v := reflect.ValueOf(args[0])
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true, nil
	default:
		return false, nil
	}
}

// IsStringFunction 检查是否为字符串类型
type IsStringFunction struct {
	*BaseFunction
}

func NewIsStringFunction() *IsStringFunction {
	return &IsStringFunction{
		BaseFunction: NewBaseFunction("is_string", TypeString, "类型检查函数", "检查是否为字符串类型", 1, 1),
	}
}

func (f *IsStringFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *IsStringFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if args[0] == nil {
		return false, nil
	}

	_, ok := args[0].(string)
	return ok, nil
}

// IsBoolFunction 检查是否为布尔类型
type IsBoolFunction struct {
	*BaseFunction
}

func NewIsBoolFunction() *IsBoolFunction {
	return &IsBoolFunction{
		BaseFunction: NewBaseFunction("is_bool", TypeString, "类型检查函数", "检查是否为布尔类型", 1, 1),
	}
}

func (f *IsBoolFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *IsBoolFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if args[0] == nil {
		return false, nil
	}

	_, ok := args[0].(bool)
	return ok, nil
}

// IsArrayFunction 检查是否为数组类型
type IsArrayFunction struct {
	*BaseFunction
}

func NewIsArrayFunction() *IsArrayFunction {
	return &IsArrayFunction{
		BaseFunction: NewBaseFunction("is_array", TypeString, "类型检查函数", "检查是否为数组类型", 1, 1),
	}
}

func (f *IsArrayFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *IsArrayFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if args[0] == nil {
		return false, nil
	}

	v := reflect.ValueOf(args[0])
	return v.Kind() == reflect.Slice || v.Kind() == reflect.Array, nil
}

// IsObjectFunction 检查是否为对象类型
type IsObjectFunction struct {
	*BaseFunction
}

func NewIsObjectFunction() *IsObjectFunction {
	return &IsObjectFunction{
		BaseFunction: NewBaseFunction("is_object", TypeString, "类型检查函数", "检查是否为对象类型", 1, 1),
	}
}

func (f *IsObjectFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *IsObjectFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if args[0] == nil {
		return false, nil
	}

	v := reflect.ValueOf(args[0])
	return v.Kind() == reflect.Map || v.Kind() == reflect.Struct, nil
}
