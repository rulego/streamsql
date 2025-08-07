package functions

import (
	"fmt"
	"reflect"

	"github.com/rulego/streamsql/utils/cast"
)

// IfNullFunction returns second argument if first argument is NULL
type IfNullFunction struct {
	*BaseFunction
}

func NewIfNullFunction() *IfNullFunction {
	return &IfNullFunction{
		BaseFunction: NewBaseFunction("if_null", TypeString, "conditional", "Return second argument if first argument is NULL", 2, 2),
	}
}

func (f *IfNullFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *IfNullFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if args[0] == nil {
		return args[1], nil
	}
	return args[0], nil
}

// CoalesceFunction returns first non-NULL value
type CoalesceFunction struct {
	*BaseFunction
}

func NewCoalesceFunction() *CoalesceFunction {
	return &CoalesceFunction{
		BaseFunction: NewBaseFunction("coalesce", TypeString, "conditional", "Return first non-NULL value", 1, -1),
	}
}

func (f *CoalesceFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CoalesceFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	for _, arg := range args {
		if arg != nil {
			return arg, nil
		}
	}
	return nil, nil
}

// NullIfFunction returns NULL if two values are equal
type NullIfFunction struct {
	*BaseFunction
}

func NewNullIfFunction() *NullIfFunction {
	return &NullIfFunction{
		BaseFunction: NewBaseFunction("null_if", TypeString, "conditional", "Return NULL if two values are equal", 2, 2),
	}
}

func (f *NullIfFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *NullIfFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if reflect.DeepEqual(args[0], args[1]) {
		return nil, nil
	}
	return args[0], nil
}

// GreatestFunction returns maximum value
type GreatestFunction struct {
	*BaseFunction
}

func NewGreatestFunction() *GreatestFunction {
	return &GreatestFunction{
		BaseFunction: NewBaseFunction("greatest", TypeMath, "conditional", "Return maximum value", 1, -1),
	}
}

func (f *GreatestFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *GreatestFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, nil
	}

	max := args[0]
	if max == nil {
		return nil, nil
	}

	for i := 1; i < len(args); i++ {
		if args[i] == nil {
			return nil, nil
		}

		// 尝试转换为数字进行比较
		maxVal, err1 := cast.ToFloat64E(max)
		currVal, err2 := cast.ToFloat64E(args[i])

		if err1 == nil && err2 == nil {
			if currVal > maxVal {
				max = args[i]
			}
		} else {
			// 如果不能转换为数字，则按字符串比较
			maxStr := fmt.Sprintf("%v", max)
			currStr := fmt.Sprintf("%v", args[i])
			if currStr > maxStr {
				max = args[i]
			}
		}
	}
	return max, nil
}

// LeastFunction 返回最小值
type LeastFunction struct {
	*BaseFunction
}

func NewLeastFunction() *LeastFunction {
	return &LeastFunction{
		BaseFunction: NewBaseFunction("least", TypeMath, "条件函数", "返回最小值", 1, -1),
	}
}

func (f *LeastFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *LeastFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, nil
	}

	min := args[0]
	if min == nil {
		return nil, nil
	}

	for i := 1; i < len(args); i++ {
		if args[i] == nil {
			return nil, nil
		}

		// 尝试转换为数字进行比较
		minVal, err1 := cast.ToFloat64E(min)
		currVal, err2 := cast.ToFloat64E(args[i])

		if err1 == nil && err2 == nil {
			if currVal < minVal {
				min = args[i]
			}
		} else {
			// 如果不能转换为数字，则按字符串比较
			minStr := fmt.Sprintf("%v", min)
			currStr := fmt.Sprintf("%v", args[i])
			if currStr < minStr {
				min = args[i]
			}
		}
	}
	return min, nil
}

// CaseWhenFunction CASE WHEN表达式
type CaseWhenFunction struct {
	*BaseFunction
}

func NewCaseWhenFunction() *CaseWhenFunction {
	return &CaseWhenFunction{
		BaseFunction: NewBaseFunction("case_when", TypeString, "条件函数", "CASE WHEN表达式", 2, -1),
	}
}

func (f *CaseWhenFunction) Validate(args []interface{}) error {
	if len(args) < 2 {
		return fmt.Errorf("case_when requires at least 2 arguments")
	}

	// 参数必须是偶数个（条件-值对）或奇数个（最后一个是默认值）
	if len(args)%2 == 0 {
		// 偶数个参数，必须都是条件-值对
		for i := 0; i < len(args); i += 2 {
			// 条件应该是布尔值或可以转换为布尔值的表达式
		}
	} else {
		// 奇数个参数，最后一个是默认值
		for i := 0; i < len(args)-1; i += 2 {
			// 条件应该是布尔值或可以转换为布尔值的表达式
		}
	}

	return nil
}

func (f *CaseWhenFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	// 处理条件-值对
	for i := 0; i < len(args)-1; i += 2 {
		condition := args[i]
		value := args[i+1]

		// 将条件转换为布尔值
		condBool, err := cast.ToBoolE(condition)
		if err != nil {
			// 如果无法转换为布尔值，检查是否为非零/非空值
			if condition == nil {
				condBool = false
			} else {
				switch v := condition.(type) {
				case string:
					condBool = v != ""
				case int, int32, int64:
					num, _ := cast.ToInt64E(v)
					condBool = num != 0
				case float32, float64:
					num, _ := cast.ToFloat64E(v)
					condBool = num != 0.0
				default:
					condBool = true
				}
			}
		}

		if condBool {
			return value, nil
		}
	}

	// 如果没有条件匹配，返回默认值（如果有）
	if len(args)%2 == 1 {
		return args[len(args)-1], nil
	}

	// 没有默认值，返回 nil
	return nil, nil
}
