package functions

import (
	"fmt"
	"reflect"
)

// LagFunction LAG函数 - 返回当前行之前的第N行的值
type LagFunction struct {
	*BaseFunction
	PreviousValues []interface{}
	DefaultValue   interface{}
	Offset         int
}

func NewLagFunction() *LagFunction {
	return &LagFunction{
		BaseFunction: NewBaseFunction("lag", TypeAnalytical, "分析函数", "返回前N行的值", 1, 3),
		Offset:       1, // 设置默认偏移量为1
	}
}

func (f *LagFunction) Validate(args []interface{}) error {
	if err := f.ValidateArgCount(args); err != nil {
		return err
	}
	if len(args) >= 2 {
		offset, ok := args[1].(int)
		if !ok {
			return fmt.Errorf("lag function second argument (offset) must be an integer")
		}
		f.Offset = offset
		if f.Offset <= 0 {
			return fmt.Errorf("lag function offset must be a positive integer")
		}
	} else {
		f.Offset = 1 // 默认为1
	}
	if len(args) == 3 {
		f.DefaultValue = args[2]
	} else {
		f.DefaultValue = nil // 默认值为nil
	}
	return nil
}

func (f *LagFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	currentValue := args[0]

	var result interface{}
	if len(f.PreviousValues) < f.Offset {
		result = f.DefaultValue
	} else {
		result = f.PreviousValues[len(f.PreviousValues)-f.Offset]
	}

	// 更新历史值队列
	f.PreviousValues = append(f.PreviousValues, currentValue)
	// 保持队列长度，移除最旧的值
	if len(f.PreviousValues) > f.Offset*2 { // 保留足够的历史数据，可以根据需要调整
		f.PreviousValues = f.PreviousValues[1:]
	}

	return result, nil
}

func (f *LagFunction) Reset() {
	f.PreviousValues = nil
}

// 实现AggregatorFunction接口 - 增量计算支持
func (f *LagFunction) New() AggregatorFunction {
	// 确保Offset有默认值
	offset := f.Offset
	if offset <= 0 {
		offset = 1
	}
	newFunc := &LagFunction{
		BaseFunction:   f.BaseFunction,
		DefaultValue:   f.DefaultValue,
		Offset:         offset,
		PreviousValues: make([]interface{}, 0),
	}
	return newFunc
}

func (f *LagFunction) Add(value interface{}) {
	// 增量添加值，维护历史值队列
	f.PreviousValues = append(f.PreviousValues, value)
	// 保持队列长度
	if len(f.PreviousValues) > f.Offset*2 {
		f.PreviousValues = f.PreviousValues[1:]
	}
}

func (f *LagFunction) Result() interface{} {
	// 检查是否有足够的历史值
	if len(f.PreviousValues) <= f.Offset {
		return f.DefaultValue
	}
	// 返回当前值之前第Offset个值
	// 对于数组[first, second, third]，当前位置是最后一个元素
	// offset=1时返回second（倒数第2个），offset=2时返回first（倒数第3个）
	return f.PreviousValues[len(f.PreviousValues)-f.Offset-1]
}

func (f *LagFunction) Clone() AggregatorFunction {
	clone := &LagFunction{
		BaseFunction:   f.BaseFunction,
		DefaultValue:   f.DefaultValue,
		Offset:         f.Offset,
		PreviousValues: make([]interface{}, len(f.PreviousValues)),
	}
	copy(clone.PreviousValues, f.PreviousValues)
	return clone
}

// LatestFunction 最新值函数 - 返回指定列的最新值
type LatestFunction struct {
	*BaseFunction
	LatestValue interface{}
}

func NewLatestFunction() *LatestFunction {
	return &LatestFunction{
		BaseFunction: NewBaseFunction("latest", TypeAnalytical, "分析函数", "返回最新值", 1, 1),
	}
}

func (f *LatestFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *LatestFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	f.LatestValue = args[0]
	return f.LatestValue, nil
}

func (f *LatestFunction) Reset() {
	f.LatestValue = nil
}

// 实现AggregatorFunction接口 - 增量计算支持
func (f *LatestFunction) New() AggregatorFunction {
	return &LatestFunction{
		BaseFunction: f.BaseFunction,
		LatestValue:  nil,
	}
}

func (f *LatestFunction) Add(value interface{}) {
	f.LatestValue = value
}

func (f *LatestFunction) Result() interface{} {
	return f.LatestValue
}

func (f *LatestFunction) Clone() AggregatorFunction {
	return &LatestFunction{
		BaseFunction: f.BaseFunction,
		LatestValue:  f.LatestValue,
	}
}

// ChangedColFunction 变化列函数 - 返回发生变化的列名
type ChangedColFunction struct {
	*BaseFunction
	PreviousValues map[string]interface{}
}

func NewChangedColFunction() *ChangedColFunction {
	return &ChangedColFunction{
		BaseFunction:   NewBaseFunction("changed_col", TypeAnalytical, "分析函数", "返回变化的列名", 1, 1),
		PreviousValues: make(map[string]interface{}),
	}
}

func (f *ChangedColFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ChangedColFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	currentValue := args[0]
	// 假设currentValue是一个map[string]interface{}，代表当前行数据
	currentMap, ok := currentValue.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("changed_col function expects a map as input")
	}

	changedColumns := []string{}
	for key, val := range currentMap {
		if prevVal, exists := f.PreviousValues[key]; !exists || !valuesEqual(prevVal, val) {
			changedColumns = append(changedColumns, key)
		}
		f.PreviousValues[key] = val // 更新上一行的值
	}

	return changedColumns, nil
}

func (f *ChangedColFunction) Reset() {
	f.PreviousValues = make(map[string]interface{})
}

// 实现AggregatorFunction接口 - 增量计算支持
func (f *ChangedColFunction) New() AggregatorFunction {
	return &ChangedColFunction{
		BaseFunction:   f.BaseFunction,
		PreviousValues: make(map[string]interface{}),
	}
}

func (f *ChangedColFunction) Add(value interface{}) {
	// 对于changed_col函数，每次Add都会更新状态
	currentMap, ok := value.(map[string]interface{})
	if !ok {
		return
	}

	for key, val := range currentMap {
		f.PreviousValues[key] = val
	}
}

func (f *ChangedColFunction) Result() interface{} {
	// 返回所有变化的列名
	changedColumns := make([]string, 0, len(f.PreviousValues))
	for key := range f.PreviousValues {
		changedColumns = append(changedColumns, key)
	}
	return changedColumns
}

func (f *ChangedColFunction) Clone() AggregatorFunction {
	clone := &ChangedColFunction{
		BaseFunction:   f.BaseFunction,
		PreviousValues: make(map[string]interface{}),
	}
	for k, v := range f.PreviousValues {
		clone.PreviousValues[k] = v
	}
	return clone
}

// HadChangedFunction 是否变化函数 - 判断指定列的值是否发生变化
type HadChangedFunction struct {
	*BaseFunction
	PreviousValue interface{}
	IsSet         bool // 标记PreviousValue是否已被设置
}

func NewHadChangedFunction() *HadChangedFunction {
	return &HadChangedFunction{
		BaseFunction: NewBaseFunction("had_changed", TypeAnalytical, "分析函数", "判断值是否变化", 1, 1),
	}
}

func (f *HadChangedFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *HadChangedFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	currentValue := args[0]
	changed := false
	if f.IsSet {
		changed = !valuesEqual(f.PreviousValue, currentValue)
	} else {
		changed = true // 第一次调用，认为发生了变化
	}
	f.PreviousValue = currentValue
	f.IsSet = true
	return changed, nil
}

func (f *HadChangedFunction) Reset() {
	f.PreviousValue = nil
	f.IsSet = false
}

// 实现AggregatorFunction接口 - 增量计算支持
func (f *HadChangedFunction) New() AggregatorFunction {
	return &HadChangedFunction{
		BaseFunction:  f.BaseFunction,
		PreviousValue: nil,
		IsSet:         false,
	}
}

func (f *HadChangedFunction) Add(value interface{}) {
	f.PreviousValue = value
	f.IsSet = true
}

func (f *HadChangedFunction) Result() interface{} {
	// 对于增量计算，返回是否发生了变化
	return f.IsSet
}

func (f *HadChangedFunction) Clone() AggregatorFunction {
	return &HadChangedFunction{
		BaseFunction:  f.BaseFunction,
		PreviousValue: f.PreviousValue,
		IsSet:         f.IsSet,
	}
}

// valuesEqual 比较两个值是否相等，处理不同类型和nil的情况
func valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// 使用reflect.DeepEqual进行深度比较，可以处理复杂类型
	return reflect.DeepEqual(a, b)
}
