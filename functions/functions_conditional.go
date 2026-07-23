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

func (f *IfNullFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *IfNullFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if args[0] == nil {
		// When the first argument is nil, the second parameter is returned
		// If the second parameter is 0, make sure to return float64 type to maintain consistency
		if args[1] != nil {
			// Try converting to float64 to maintain consistency between numeric types
			if val, ok := args[1].(int); ok && val == 0 {
				return 0.0, nil
			}
			if val, ok := args[1].(float32); ok {
				return float64(val), nil
			}
		}
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

func (f *CoalesceFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *CoalesceFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

func (f *NullIfFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *NullIfFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

func (f *GreatestFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *GreatestFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

		// Try to convert them into numbers for comparison
		maxVal, err1 := cast.ToFloat64E(max)
		currVal, err2 := cast.ToFloat64E(args[i])

		if err1 == nil && err2 == nil {
			if currVal > maxVal {
				max = args[i]
			}
		} else {
			// If it cannot be converted to a number, it is compared by string
			maxStr := fmt.Sprintf("%v", max)
			currStr := fmt.Sprintf("%v", args[i])
			if currStr > maxStr {
				max = args[i]
			}
		}
	}
	return max, nil
}

// LeastFunction returns the minimum value
type LeastFunction struct {
	*BaseFunction
}

func NewLeastFunction() *LeastFunction {
	return &LeastFunction{
		BaseFunction: NewBaseFunction("least", TypeMath, "条件函数", "返回最小值", 1, -1),
	}
}

func (f *LeastFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *LeastFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

		// Try to convert them into numbers for comparison
		minVal, err1 := cast.ToFloat64E(min)
		currVal, err2 := cast.ToFloat64E(args[i])

		if err1 == nil && err2 == nil {
			if currVal < minVal {
				min = args[i]
			}
		} else {
			// If it cannot be converted to a number, it is compared by string
			minStr := fmt.Sprintf("%v", min)
			currStr := fmt.Sprintf("%v", args[i])
			if currStr < minStr {
				min = args[i]
			}
		}
	}
	return min, nil
}

// CaseWhenFunction CASE WHEN expression
type CaseWhenFunction struct {
	*BaseFunction
}

func NewCaseWhenFunction() *CaseWhenFunction {
	return &CaseWhenFunction{
		BaseFunction: NewBaseFunction("case_when", TypeString, "条件函数", "CASE WHEN表达式", 2, -1),
	}
}

func (f *CaseWhenFunction) Validate(args []any) error {
	if len(args) < 2 {
		return fmt.Errorf("case_when requires at least 2 arguments")
	}

	// Parameters must be an even number (condition-value pair) or an odd number (the last one is the default)
	if len(args)%2 == 0 {
		// Even parameters must all be conditional-value pairs
		for i := 0; i < len(args); i += 2 {
			// The condition should be a boolean value or an expression that can be converted to a boolean value
		}
	} else {
		// Odd parameters, with the last one being the default value
		for i := 0; i < len(args)-1; i += 2 {
			// The condition should be a boolean value or an expression that can be converted to a boolean value
		}
	}

	return nil
}

func (f *CaseWhenFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	// Handle condition-value pairs
	for i := 0; i < len(args)-1; i += 2 {
		condition := args[i]
		value := args[i+1]

		// Convert the condition into a boolean value
		condBool, err := cast.ToBoolE(condition)
		if err != nil {
			// If it cannot be converted to a boolean value, check whether it is non-zero/non-null
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

	// If no conditional match exists, returns the default value (if any)
	if len(args)%2 == 1 {
		return args[len(args)-1], nil
	}

	// No default value, returns nil
	return nil, nil
}
