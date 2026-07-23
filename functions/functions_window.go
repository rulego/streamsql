package functions

import (
	"fmt"
)

// WindowStartFunction returns window start time
type WindowStartFunction struct {
	*BaseFunction
	windowStart any
}

func NewWindowStartFunction() *WindowStartFunction {
	return &WindowStartFunction{
		BaseFunction: NewBaseFunction("window_start", TypeWindow, "窗口函数", "返回窗口开始时间", 0, 0),
	}
}

func (f *WindowStartFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *WindowStartFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if ctx.WindowInfo != nil {
		return ctx.WindowInfo.WindowStart, nil
	}
	return f.windowStart, nil
}

// Implement AggregatorFunction interface
func (f *WindowStartFunction) New() AggregatorFunction {
	return &WindowStartFunction{
		BaseFunction: f.BaseFunction,
	}
}

func (f *WindowStartFunction) Add(value any) {
	// Window start time usually doesn't need accumulative calculation
	f.windowStart = value
}

func (f *WindowStartFunction) Result() any {
	return f.windowStart
}

func (f *WindowStartFunction) Reset() {
	f.windowStart = nil
}

func (f *WindowStartFunction) Clone() AggregatorFunction {
	return &WindowStartFunction{
		BaseFunction: f.BaseFunction,
		windowStart:  f.windowStart,
	}
}

// WindowEndFunction returns window end time
type WindowEndFunction struct {
	*BaseFunction
	windowEnd any
}

func NewWindowEndFunction() *WindowEndFunction {
	return &WindowEndFunction{
		BaseFunction: NewBaseFunction("window_end", TypeWindow, "窗口函数", "返回窗口结束时间", 0, 0),
	}
}

func (f *WindowEndFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *WindowEndFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if ctx.WindowInfo != nil {
		return ctx.WindowInfo.WindowEnd, nil
	}
	return f.windowEnd, nil
}

// Implement the AggregatorFunction interface
func (f *WindowEndFunction) New() AggregatorFunction {
	return &WindowEndFunction{
		BaseFunction: f.BaseFunction,
	}
}

func (f *WindowEndFunction) Add(value any) {
	// The window end time usually does not need to be cumulatively calculated
	f.windowEnd = value
}

func (f *WindowEndFunction) Result() any {
	return f.windowEnd
}

func (f *WindowEndFunction) Reset() {
	f.windowEnd = nil
}

func (f *WindowEndFunction) Clone() AggregatorFunction {
	return &WindowEndFunction{
		BaseFunction: f.BaseFunction,
		windowEnd:    f.windowEnd,
	}
}

// ExpressionFunction is used to handle custom expressions
type ExpressionFunction struct {
	*BaseFunction
	values []any
}

func NewExpressionFunction() *ExpressionFunction {
	return &ExpressionFunction{
		BaseFunction: NewBaseFunction("expression", TypeCustom, "表达式函数", "处理自定义表达式", 0, -1),
		values:       make([]any, 0),
	}
}

func (f *ExpressionFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ExpressionFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// The specific implementation of expression functions is handled by the expression engine
	if len(args) == 0 {
		return nil, nil
	}
	return args[len(args)-1], nil
}

// Implement the AggregatorFunction interface
func (f *ExpressionFunction) New() AggregatorFunction {
	return &ExpressionFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, 0),
	}
}

func (f *ExpressionFunction) Add(value any) {
	f.values = append(f.values, value)
}

func (f *ExpressionFunction) Result() any {
	// The results of the expression aggregator are handled by the expression engine
	// Here, only the last calculation result is returned
	if len(f.values) == 0 {
		return nil
	}
	return f.values[len(f.values)-1]
}

func (f *ExpressionFunction) Reset() {
	f.values = make([]any, 0)
}

func (f *ExpressionFunction) Clone() AggregatorFunction {
	clone := &ExpressionFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// ExpressionAggregatorFunction Expression aggregator function - used to handle non-aggregated functions in aggregated queries
type ExpressionAggregatorFunction struct {
	*BaseFunction
	lastResult any
}

func NewExpressionAggregatorFunction() *ExpressionAggregatorFunction {
	return &ExpressionAggregatorFunction{
		BaseFunction: NewBaseFunction("expression", TypeCustom, "表达式聚合器", "处理表达式计算", 1, -1),
		lastResult:   nil,
	}
}

func (f *ExpressionAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ExpressionAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// For expression aggregators, the last value is returned directly
	if len(args) > 0 {
		return args[len(args)-1], nil
	}
	return nil, nil
}

// Implement the AggregatorFunction interface
func (f *ExpressionAggregatorFunction) New() AggregatorFunction {
	return &ExpressionAggregatorFunction{
		BaseFunction: f.BaseFunction,
		lastResult:   nil,
	}
}

func (f *ExpressionAggregatorFunction) Add(value any) {
	// For the expression aggregator, save the last calculation result
	// The calculation result of the expression should be the result of each data item
	f.lastResult = value
}

func (f *ExpressionAggregatorFunction) Result() any {
	// For the expression aggregator, return the last calculation result
	// Note: For string functions like CONCAT, each data item will produce a result
	// In window aggregation, we return the result of the last calculation
	return f.lastResult
}

func (f *ExpressionAggregatorFunction) Reset() {
	f.lastResult = nil
}

func (f *ExpressionAggregatorFunction) Clone() AggregatorFunction {
	return &ExpressionAggregatorFunction{
		BaseFunction: f.BaseFunction,
		lastResult:   f.lastResult,
	}
}

// NthValueFunction returns the Nth value in the window
type NthValueFunction struct {
	*BaseFunction
	values []any
	n      int
}

func NewNthValueFunction() *NthValueFunction {
	return &NthValueFunction{
		BaseFunction: NewBaseFunction("nth_value", TypeWindow, "窗口函数", "返回窗口中第N个值", 2, 2),
		values:       make([]any, 0),
		n:            1, // Default is the first value
	}
}

func (f *NthValueFunction) Validate(args []any) error {
	if err := f.ValidateArgCount(args); err != nil {
		return err
	}

	// Verify the value of N
	n := 1
	if nVal, ok := args[1].(int); ok {
		n = nVal
	} else if nVal, ok := args[1].(int64); ok {
		n = int(nVal)
	} else {
		return fmt.Errorf("nth_value n must be an integer")
	}

	if n <= 0 {
		return fmt.Errorf("nth_value n must be positive, got %d", n)
	}

	// Set the value of n
	f.n = n

	return nil
}

func (f *NthValueFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	// Obtain the value of N
	n := 1
	if nVal, ok := args[1].(int); ok {
		n = nVal
	} else if nVal, ok := args[1].(int64); ok {
		n = int(nVal)
	} else {
		return nil, fmt.Errorf("nth_value n must be an integer")
	}

	if n <= 0 {
		return nil, fmt.Errorf("nth_value n must be positive, got %d", n)
	}

	// Returns the Nth value (1-based index)
	if len(f.values) >= n {
		return f.values[n-1], nil
	}

	return nil, nil
}

// Implement the AggregatorFunction interface
func (f *NthValueFunction) New() AggregatorFunction {
	newInstance := &NthValueFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, 0),
		n:            f.n, // Maintain n parameters
	}

	return newInstance
}

func (f *NthValueFunction) Add(value any) {
	f.values = append(f.values, value)
}

func (f *NthValueFunction) Result() any {
	if len(f.values) >= f.n && f.n > 0 {
		return f.values[f.n-1]
	}
	return nil
}

func (f *NthValueFunction) Reset() {
	f.values = make([]any, 0)
}

func (f *NthValueFunction) Clone() AggregatorFunction {
	clone := &NthValueFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, len(f.values)),
		n:            f.n, // Maintain n parameters
	}
	copy(clone.values, f.values)
	return clone
}

// Init implements ParameterizedFunction interface
func (f *NthValueFunction) Init(args []any) error {
	if len(args) < 2 {
		return fmt.Errorf("nth_value requires at least 2 arguments")
	}

	// Parse N parameter
	n := 1
	if nVal, ok := args[1].(int); ok {
		n = nVal
	} else if nVal, ok := args[1].(int64); ok {
		n = int(nVal)
	} else if nVal, ok := args[1].(float64); ok {
		n = int(nVal)
	} else {
		return fmt.Errorf("nth_value n must be an integer, got %T", args[1])
	}

	if n <= 0 {
		return fmt.Errorf("nth_value n must be positive, got %d", n)
	}

	f.n = n
	return nil
}
