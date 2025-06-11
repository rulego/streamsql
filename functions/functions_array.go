package functions

import (
	"fmt"
	"reflect"
)

// ArrayLengthFunction 返回数组长度
type ArrayLengthFunction struct {
	*BaseFunction
}

func NewArrayLengthFunction() *ArrayLengthFunction {
	return &ArrayLengthFunction{
		BaseFunction: NewBaseFunction("array_length", TypeMath, "数组函数", "返回数组长度", 1, 1),
	}
}

func (f *ArrayLengthFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayLengthFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	array := args[0]
	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_length requires array input")
	}
	return v.Len(), nil
}

// ArrayContainsFunction 检查数组是否包含指定值
type ArrayContainsFunction struct {
	*BaseFunction
}

func NewArrayContainsFunction() *ArrayContainsFunction {
	return &ArrayContainsFunction{
		BaseFunction: NewBaseFunction("array_contains", TypeString, "数组函数", "检查数组是否包含指定值", 2, 2),
	}
}

func (f *ArrayContainsFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayContainsFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	array := args[0]
	value := args[1]

	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_contains requires array input")
	}

	for i := 0; i < v.Len(); i++ {
		if reflect.DeepEqual(v.Index(i).Interface(), value) {
			return true, nil
		}
	}
	return false, nil
}

// ArrayPositionFunction 返回值在数组中的位置
type ArrayPositionFunction struct {
	*BaseFunction
}

func NewArrayPositionFunction() *ArrayPositionFunction {
	return &ArrayPositionFunction{
		BaseFunction: NewBaseFunction("array_position", TypeMath, "数组函数", "返回值在数组中的位置", 2, 2),
	}
}

func (f *ArrayPositionFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayPositionFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	array := args[0]
	value := args[1]

	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_position requires array input")
	}

	for i := 0; i < v.Len(); i++ {
		if reflect.DeepEqual(v.Index(i).Interface(), value) {
			return i + 1, nil // 返回1基索引
		}
	}
	return 0, nil // 未找到返回0
}

// ArrayRemoveFunction 从数组中移除指定值
type ArrayRemoveFunction struct {
	*BaseFunction
}

func NewArrayRemoveFunction() *ArrayRemoveFunction {
	return &ArrayRemoveFunction{
		BaseFunction: NewBaseFunction("array_remove", TypeString, "数组函数", "从数组中移除指定值", 2, 2),
	}
}

func (f *ArrayRemoveFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayRemoveFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	array := args[0]
	value := args[1]

	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_remove requires array input")
	}

	var result []interface{}
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()
		if !reflect.DeepEqual(elem, value) {
			result = append(result, elem)
		}
	}
	return result, nil
}

// ArrayDistinctFunction 数组去重
type ArrayDistinctFunction struct {
	*BaseFunction
}

func NewArrayDistinctFunction() *ArrayDistinctFunction {
	return &ArrayDistinctFunction{
		BaseFunction: NewBaseFunction("array_distinct", TypeString, "数组函数", "数组去重", 1, 1),
	}
}

func (f *ArrayDistinctFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayDistinctFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	array := args[0]

	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_distinct requires array input")
	}

	seen := make(map[interface{}]bool)
	var result []interface{}

	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()
		if !seen[elem] {
			seen[elem] = true
			result = append(result, elem)
		}
	}
	return result, nil
}

// ArrayIntersectFunction 数组交集
type ArrayIntersectFunction struct {
	*BaseFunction
}

func NewArrayIntersectFunction() *ArrayIntersectFunction {
	return &ArrayIntersectFunction{
		BaseFunction: NewBaseFunction("array_intersect", TypeString, "数组函数", "数组交集", 2, 2),
	}
}

func (f *ArrayIntersectFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayIntersectFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	array1 := args[0]
	array2 := args[1]

	v1 := reflect.ValueOf(array1)
	v2 := reflect.ValueOf(array2)

	if v1.Kind() != reflect.Slice && v1.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_intersect requires array input for first argument")
	}
	if v2.Kind() != reflect.Slice && v2.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_intersect requires array input for second argument")
	}

	// 创建第二个数组的元素集合
	set2 := make(map[interface{}]bool)
	for i := 0; i < v2.Len(); i++ {
		set2[v2.Index(i).Interface()] = true
	}

	// 找交集
	seen := make(map[interface{}]bool)
	var result []interface{}

	for i := 0; i < v1.Len(); i++ {
		elem := v1.Index(i).Interface()
		if set2[elem] && !seen[elem] {
			seen[elem] = true
			result = append(result, elem)
		}
	}
	return result, nil
}

// ArrayUnionFunction 数组并集
type ArrayUnionFunction struct {
	*BaseFunction
}

func NewArrayUnionFunction() *ArrayUnionFunction {
	return &ArrayUnionFunction{
		BaseFunction: NewBaseFunction("array_union", TypeString, "数组函数", "数组并集", 2, 2),
	}
}

func (f *ArrayUnionFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayUnionFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	array1 := args[0]
	array2 := args[1]

	v1 := reflect.ValueOf(array1)
	v2 := reflect.ValueOf(array2)

	if v1.Kind() != reflect.Slice && v1.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_union requires array input for first argument")
	}
	if v2.Kind() != reflect.Slice && v2.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_union requires array input for second argument")
	}

	seen := make(map[interface{}]bool)
	var result []interface{}

	// 添加第一个数组的元素
	for i := 0; i < v1.Len(); i++ {
		elem := v1.Index(i).Interface()
		if !seen[elem] {
			seen[elem] = true
			result = append(result, elem)
		}
	}

	// 添加第二个数组的元素
	for i := 0; i < v2.Len(); i++ {
		elem := v2.Index(i).Interface()
		if !seen[elem] {
			seen[elem] = true
			result = append(result, elem)
		}
	}
	return result, nil
}

// ArrayExceptFunction 数组差集
type ArrayExceptFunction struct {
	*BaseFunction
}

func NewArrayExceptFunction() *ArrayExceptFunction {
	return &ArrayExceptFunction{
		BaseFunction: NewBaseFunction("array_except", TypeString, "数组函数", "数组差集", 2, 2),
	}
}

func (f *ArrayExceptFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayExceptFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	array1 := args[0]
	array2 := args[1]

	v1 := reflect.ValueOf(array1)
	v2 := reflect.ValueOf(array2)

	if v1.Kind() != reflect.Slice && v1.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_except requires array input for first argument")
	}
	if v2.Kind() != reflect.Slice && v2.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_except requires array input for second argument")
	}

	// 创建第二个数组的元素集合
	set2 := make(map[interface{}]bool)
	for i := 0; i < v2.Len(); i++ {
		set2[v2.Index(i).Interface()] = true
	}

	// 找差集
	seen := make(map[interface{}]bool)
	var result []interface{}

	for i := 0; i < v1.Len(); i++ {
		elem := v1.Index(i).Interface()
		if !set2[elem] && !seen[elem] {
			seen[elem] = true
			result = append(result, elem)
		}
	}
	return result, nil
}
