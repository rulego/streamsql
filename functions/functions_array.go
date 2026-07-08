package functions

import (
	"fmt"
	"reflect"
)

// ArrayLengthFunction returns array length
type ArrayLengthFunction struct {
	*BaseFunction
}

func NewArrayLengthFunction() *ArrayLengthFunction {
	return &ArrayLengthFunction{
		BaseFunction: NewBaseFunction("array_length", TypeMath, "array", "Return array length", 1, 1),
	}
}

func (f *ArrayLengthFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayLengthFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	array := args[0]
	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_length requires array input")
	}
	return v.Len(), nil
}

// ArrayContainsFunction checks if array contains specified value
type ArrayContainsFunction struct {
	*BaseFunction
}

func NewArrayContainsFunction() *ArrayContainsFunction {
	return &ArrayContainsFunction{
		BaseFunction: NewBaseFunction("array_contains", TypeString, "array", "Check if array contains specified value", 2, 2),
	}
}

func (f *ArrayContainsFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayContainsFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// ArrayPositionFunction returns position of value in array
type ArrayPositionFunction struct {
	*BaseFunction
}

func NewArrayPositionFunction() *ArrayPositionFunction {
	return &ArrayPositionFunction{
		BaseFunction: NewBaseFunction("array_position", TypeMath, "array", "Return position of value in array", 2, 2),
	}
}

func (f *ArrayPositionFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayPositionFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	array := args[0]
	value := args[1]

	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_position requires array input")
	}

	for i := 0; i < v.Len(); i++ {
		if reflect.DeepEqual(v.Index(i).Interface(), value) {
			return i + 1, nil // Return 1-based index
		}
	}
	return 0, nil // Return 0 if not found
}

// ArrayRemoveFunction removes specified value from array
type ArrayRemoveFunction struct {
	*BaseFunction
}

func NewArrayRemoveFunction() *ArrayRemoveFunction {
	return &ArrayRemoveFunction{
		BaseFunction: NewBaseFunction("array_remove", TypeString, "数组函数", "从数组中移除指定值", 2, 2),
	}
}

func (f *ArrayRemoveFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayRemoveFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	array := args[0]
	value := args[1]

	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_remove requires array input")
	}

	result := make([]any, 0) // 初始化为空切片而不是nil切片
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()
		if !reflect.DeepEqual(elem, value) {
			result = append(result, elem)
		}
	}
	return result, nil
}

// hashSafeSet is a set of any values that tolerates unhashable elements
// (slices, maps) by falling back to a reflect.DeepEqual linear scan.
type hashSafeSet struct {
	m     map[any]bool
	extra []any
}

func newHashSafeSet() *hashSafeSet {
	return &hashSafeSet{m: make(map[any]bool)}
}

// has reports whether elem is in the set.
func (s *hashSafeSet) has(elem any) bool {
	if elem == nil || reflect.TypeOf(elem).Comparable() {
		return s.m[elem]
	}
	for _, e := range s.extra {
		if reflect.DeepEqual(e, elem) {
			return true
		}
	}
	return false
}

// add inserts elem and reports whether it was newly added.
func (s *hashSafeSet) add(elem any) bool {
	if elem == nil || reflect.TypeOf(elem).Comparable() {
		if s.m[elem] {
			return false
		}
		s.m[elem] = true
		return true
	}
	for _, e := range s.extra {
		if reflect.DeepEqual(e, elem) {
			return false
		}
	}
	s.extra = append(s.extra, elem)
	return true
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

func (f *ArrayDistinctFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayDistinctFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	array := args[0]

	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("array_distinct requires array input")
	}

	seen := newHashSafeSet()
	result := make([]any, 0)

	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()
		if seen.add(elem) {
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

func (f *ArrayIntersectFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayIntersectFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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
	set2 := newHashSafeSet()
	for i := 0; i < v2.Len(); i++ {
		set2.add(v2.Index(i).Interface())
	}

	// 找交集
	seen := newHashSafeSet()
	result := make([]any, 0)

	for i := 0; i < v1.Len(); i++ {
		elem := v1.Index(i).Interface()
		if set2.has(elem) && seen.add(elem) {
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

func (f *ArrayUnionFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayUnionFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

	seen := newHashSafeSet()
	result := make([]any, 0)

	// 添加第一个数组的元素
	for i := 0; i < v1.Len(); i++ {
		elem := v1.Index(i).Interface()
		if seen.add(elem) {
			result = append(result, elem)
		}
	}

	// 添加第二个数组的元素
	for i := 0; i < v2.Len(); i++ {
		elem := v2.Index(i).Interface()
		if seen.add(elem) {
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

func (f *ArrayExceptFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ArrayExceptFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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
	set2 := newHashSafeSet()
	for i := 0; i < v2.Len(); i++ {
		set2.add(v2.Index(i).Interface())
	}

	// 找差集
	seen := newHashSafeSet()
	result := make([]any, 0)

	for i := 0; i < v1.Len(); i++ {
		elem := v1.Index(i).Interface()
		if !set2.has(elem) && seen.add(elem) {
			result = append(result, elem)
		}
	}
	return result, nil
}
