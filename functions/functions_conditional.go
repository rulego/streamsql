package functions

import (
	"fmt"
	"reflect"

	"github.com/rulego/streamsql/utils/cast"
)

// CoalesceFunction 返回第一个非NULL值
type CoalesceFunction struct {
	*BaseFunction
}

func NewCoalesceFunction() *CoalesceFunction {
	return &CoalesceFunction{
		BaseFunction: NewBaseFunction("coalesce", TypeString, "条件函数", "返回第一个非NULL值", 1, -1),
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

// NullIfFunction 如果两个值相等则返回NULL
type NullIfFunction struct {
	*BaseFunction
}

func NewNullIfFunction() *NullIfFunction {
	return &NullIfFunction{
		BaseFunction: NewBaseFunction("nullif", TypeString, "条件函数", "如果两个值相等则返回NULL", 2, 2),
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

// GreatestFunction 返回最大值
type GreatestFunction struct {
	*BaseFunction
}

func NewGreatestFunction() *GreatestFunction {
	return &GreatestFunction{
		BaseFunction: NewBaseFunction("greatest", TypeMath, "条件函数", "返回最大值", 1, -1),
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