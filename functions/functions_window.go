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
		BaseFunction: NewBaseFunction("window_start", TypeWindow, "window", "Return window start time", 0, 0),
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
		BaseFunction: NewBaseFunction("window_end", TypeWindow, "window", "Return window end time", 0, 0),
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

// FirstValueFunction 返回窗口中第一个值
type FirstValueFunction struct {
	*BaseFunction
	firstValue interface{}
	hasValue   bool
}

func NewFirstValueFunction() *FirstValueFunction {
	return &FirstValueFunction{
		BaseFunction: NewBaseFunction("first_value", TypeWindow, "窗口函数", "返回窗口中第一个值", 1, 1),
		hasValue:     false,
	}
}

func (f *FirstValueFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *FirstValueFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}
	return f.firstValue, nil
}

// 实现AggregatorFunction接口
func (f *FirstValueFunction) New() AggregatorFunction {
	return &FirstValueFunction{
		BaseFunction: f.BaseFunction,
		hasValue:     false,
	}
}

func (f *FirstValueFunction) Add(value interface{}) {
	if !f.hasValue {
		f.firstValue = value
		f.hasValue = true
	}
}

func (f *FirstValueFunction) Result() interface{} {
	return f.firstValue
}

func (f *FirstValueFunction) Reset() {
	f.firstValue = nil
	f.hasValue = false
}

func (f *FirstValueFunction) Clone() AggregatorFunction {
	return &FirstValueFunction{
		BaseFunction: f.BaseFunction,
		firstValue:   f.firstValue,
		hasValue:     f.hasValue,
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
		offset:       f.offset,
		defaultValue: f.defaultValue,
		hasDefault:   f.hasDefault,
	}
}

func (f *LeadFunction) Add(value interface{}) {
	f.values = append(f.values, value)
}

func (f *LeadFunction) Result() interface{} {
	// Lead函数的结果需要在所有数据添加完成后计算
	// 如果没有足够的数据，返回默认值
	if len(f.values) == 0 && f.hasDefault {
		return f.defaultValue
	}
	// 这里简化实现，返回nil
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
	return &NthValueFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, 0),
		n:            f.n,
	}
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
		n:            f.n,
	}
	copy(clone.values, f.values)
	return clone
}
