package functions

import (
	"fmt"
)

// RowNumberFunction returns row number
type RowNumberFunction struct {
	*BaseFunction
	CurrentRowNumber int64
}

func NewRowNumberFunction() *RowNumberFunction {
	return &RowNumberFunction{
		BaseFunction:     NewBaseFunction("row_number", TypeWindow, "window", "Return current row number", 0, 0),
		CurrentRowNumber: 0,
	}
}

func (f *RowNumberFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *RowNumberFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	f.CurrentRowNumber++
	return f.CurrentRowNumber, nil
}

func (f *RowNumberFunction) Reset() {
	f.CurrentRowNumber = 0
}

// WindowStartFunction returns window start time
type WindowStartFunction struct {
	*BaseFunction
	windowStart interface{}
}

func NewWindowStartFunction() *WindowStartFunction {
	return &WindowStartFunction{
		BaseFunction: NewBaseFunction("window_start", TypeWindow, "窗口函数", "返回窗口开始时间", 0, 0),
	}
}

func (f *WindowStartFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *WindowStartFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
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

func (f *WindowStartFunction) Add(value interface{}) {
	// Window start time usually doesn't need accumulative calculation
	f.windowStart = value
}

func (f *WindowStartFunction) Result() interface{} {
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
	windowEnd interface{}
}

func NewWindowEndFunction() *WindowEndFunction {
	return &WindowEndFunction{
		BaseFunction: NewBaseFunction("window_end", TypeWindow, "窗口函数", "返回窗口结束时间", 0, 0),
	}
}

func (f *WindowEndFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *WindowEndFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if ctx.WindowInfo != nil {
		return ctx.WindowInfo.WindowEnd, nil
	}
	return f.windowEnd, nil
}

// 实现AggregatorFunction接口
func (f *WindowEndFunction) New() AggregatorFunction {
	return &WindowEndFunction{
		BaseFunction: f.BaseFunction,
	}
}

func (f *WindowEndFunction) Add(value interface{}) {
	// 窗口结束时间通常不需要累积计算
	f.windowEnd = value
}

func (f *WindowEndFunction) Result() interface{} {
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

// ExpressionFunction 表达式函数，用于处理自定义表达式
type ExpressionFunction struct {
	*BaseFunction
	values []interface{}
}

func NewExpressionFunction() *ExpressionFunction {
	return &ExpressionFunction{
		BaseFunction: NewBaseFunction("expression", TypeCustom, "表达式函数", "处理自定义表达式", 0, -1),
		values:       make([]interface{}, 0),
	}
}

func (f *ExpressionFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ExpressionFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 表达式函数的具体实现由表达式引擎处理
	if len(args) == 0 {
		return nil, nil
	}
	return args[len(args)-1], nil
}

// 实现AggregatorFunction接口
func (f *ExpressionFunction) New() AggregatorFunction {
	return &ExpressionFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, 0),
	}
}

func (f *ExpressionFunction) Add(value interface{}) {
	f.values = append(f.values, value)
}

func (f *ExpressionFunction) Result() interface{} {
	// 表达式聚合器的结果处理由表达式引擎处理
	// 这里只返回最后一个计算结果
	if len(f.values) == 0 {
		return nil
	}
	return f.values[len(f.values)-1]
}

func (f *ExpressionFunction) Reset() {
	f.values = make([]interface{}, 0)
}

func (f *ExpressionFunction) Clone() AggregatorFunction {
	clone := &ExpressionFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// ExpressionAggregatorFunction 表达式聚合器函数 - 用于处理非聚合函数在聚合查询中的情况
type ExpressionAggregatorFunction struct {
	*BaseFunction
	lastResult interface{}
}

func NewExpressionAggregatorFunction() *ExpressionAggregatorFunction {
	return &ExpressionAggregatorFunction{
		BaseFunction: NewBaseFunction("expression", TypeCustom, "表达式聚合器", "处理表达式计算", 1, -1),
		lastResult:   nil,
	}
}

func (f *ExpressionAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ExpressionAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 对于表达式聚合器，直接返回最后一个值
	if len(args) > 0 {
		return args[len(args)-1], nil
	}
	return nil, nil
}

// 实现AggregatorFunction接口
func (f *ExpressionAggregatorFunction) New() AggregatorFunction {
	return &ExpressionAggregatorFunction{
		BaseFunction: f.BaseFunction,
		lastResult:   nil,
	}
}

func (f *ExpressionAggregatorFunction) Add(value interface{}) {
	// 对于表达式聚合器，保存最后一个计算结果
	// 表达式的计算结果应该是每个数据项的计算结果
	f.lastResult = value
}

func (f *ExpressionAggregatorFunction) Result() interface{} {
	// 对于表达式聚合器，返回最后一个计算结果
	// 注意：对于字符串函数如CONCAT，每个数据项都会产生一个结果
	// 在窗口聚合中，我们返回最后一个计算的结果
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

// LeadFunction 返回当前行之后第N行的值
type LeadFunction struct {
	*BaseFunction
	values       []interface{}
	offset       int
	defaultValue interface{}
	hasDefault   bool
}

func NewLeadFunction() *LeadFunction {
	return &LeadFunction{
		BaseFunction: NewBaseFunction("lead", TypeWindow, "窗口函数", "返回当前行之后第N行的值", 1, 3),
		values:       make([]interface{}, 0),
		offset:       1, // 默认偏移量为1
	}
}

func (f *LeadFunction) Validate(args []interface{}) error {
	if err := f.ValidateArgCount(args); err != nil {
		return err
	}

	// 验证第二个参数（offset）是否为整数
	if len(args) >= 2 {
		if offset, ok := args[1].(int); ok {
			f.offset = offset
		} else {
			return fmt.Errorf("offset must be an integer")
		}
	}

	// 设置默认值
	if len(args) >= 3 {
		f.defaultValue = args[2]
		f.hasDefault = true
	}

	return nil
}

func (f *LeadFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	// 获取偏移量
	if len(args) >= 2 {
		if offset, ok := args[1].(int); ok {
			f.offset = offset
		} else {
			return nil, fmt.Errorf("offset must be an integer")
		}
	}

	// 获取默认值
	if len(args) >= 3 {
		f.defaultValue = args[2]
		f.hasDefault = true
	}

	// Lead函数需要在窗口处理完成后才能确定值
	// 这里返回默认值，实际实现需要在窗口引擎中处理
	if f.hasDefault {
		return f.defaultValue, nil
	}
	return nil, nil
}

// 实现AggregatorFunction接口
func (f *LeadFunction) New() AggregatorFunction {
	return &LeadFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, 0),
		offset:       f.offset,       // 保持offset参数
		defaultValue: f.defaultValue, // 保持默认值
		hasDefault:   f.hasDefault,   // 保持默认值标志
	}
}

func (f *LeadFunction) Add(value interface{}) {
	f.values = append(f.values, value)
}

func (f *LeadFunction) Result() interface{} {
	// LEAD函数在没有指定当前行位置的情况下，返回默认值或nil
	// 这通常用于聚合场景，真正的窗口计算需要在窗口处理器中进行
	if f.hasDefault {
		return f.defaultValue
	}

	return nil
}

func (f *LeadFunction) Reset() {
	f.values = make([]interface{}, 0)
	f.offset = 1
	f.defaultValue = nil
	f.hasDefault = false
}

func (f *LeadFunction) Clone() AggregatorFunction {
	clone := &LeadFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, len(f.values)),
		offset:       f.offset,
		defaultValue: f.defaultValue,
		hasDefault:   f.hasDefault,
	}
	copy(clone.values, f.values)
	return clone
}

// Init implements ParameterizedFunction interface
func (f *LeadFunction) Init(args []interface{}) error {
	if len(args) < 2 {
		// LEAD with default offset = 1
		f.offset = 1
		return nil
	}

	// Parse offset parameter
	offset := 1
	if offsetVal, ok := args[1].(int); ok {
		offset = offsetVal
	} else if offsetVal, ok := args[1].(int64); ok {
		offset = int(offsetVal)
	} else if offsetVal, ok := args[1].(float64); ok {
		offset = int(offsetVal)
	} else {
		return fmt.Errorf("lead offset must be an integer, got %T", args[1])
	}

	if offset < 0 {
		return fmt.Errorf("lead offset must be non-negative, got %d", offset)
	}

	f.offset = offset

	// Parse default value if provided
	if len(args) >= 3 {
		f.defaultValue = args[2]
		f.hasDefault = true
	}

	return nil
}

// NthValueFunction 返回窗口中第N个值
type NthValueFunction struct {
	*BaseFunction
	values []interface{}
	n      int
}

func NewNthValueFunction() *NthValueFunction {
	return &NthValueFunction{
		BaseFunction: NewBaseFunction("nth_value", TypeWindow, "窗口函数", "返回窗口中第N个值", 2, 2),
		values:       make([]interface{}, 0),
		n:            1, // 默认第1个值
	}
}

func (f *NthValueFunction) Validate(args []interface{}) error {
	if err := f.ValidateArgCount(args); err != nil {
		return err
	}

	// 验证N值
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

	// 设置n值
	f.n = n

	return nil
}

func (f *NthValueFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	// 获取N值
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

	// 返回第N个值（1-based索引）
	if len(f.values) >= n {
		return f.values[n-1], nil
	}

	return nil, nil
}

// 实现AggregatorFunction接口
func (f *NthValueFunction) New() AggregatorFunction {
	newInstance := &NthValueFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, 0),
		n:            f.n, // 保持n参数
	}

	return newInstance
}

func (f *NthValueFunction) Add(value interface{}) {
	f.values = append(f.values, value)
}

func (f *NthValueFunction) Result() interface{} {
	if len(f.values) >= f.n && f.n > 0 {
		return f.values[f.n-1]
	}
	return nil
}

func (f *NthValueFunction) Reset() {
	f.values = make([]interface{}, 0)
}

func (f *NthValueFunction) Clone() AggregatorFunction {
	clone := &NthValueFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, len(f.values)),
		n:            f.n, // 保持n参数
	}
	copy(clone.values, f.values)
	return clone
}

// Init implements ParameterizedFunction interface
func (f *NthValueFunction) Init(args []interface{}) error {
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
