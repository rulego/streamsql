package functions

// RowNumberFunction 行号函数
type RowNumberFunction struct {
	*BaseFunction
	CurrentRowNumber int64
}

func NewRowNumberFunction() *RowNumberFunction {
	return &RowNumberFunction{
		BaseFunction:     NewBaseFunction("row_number", TypeWindow, "窗口函数", "返回当前行号", 0, 0),
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

// WindowStartFunction 窗口开始时间函数
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

// 实现AggregatorFunction接口
func (f *WindowStartFunction) New() AggregatorFunction {
	return &WindowStartFunction{
		BaseFunction: f.BaseFunction,
	}
}

func (f *WindowStartFunction) Add(value interface{}) {
	// 窗口开始时间通常不需要累积计算
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

// WindowEndFunction 窗口结束时间函数
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
