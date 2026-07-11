package functions

import (
	"fmt"
	"reflect"
)

// LagFunction LAG函数 - 返回当前行之前的第N行的值
type LagFunction struct {
	*BaseFunction
	PreviousValues []any
	DefaultValue   any
	Offset         int
}

func NewLagFunction() *LagFunction {
	return &LagFunction{
		BaseFunction: NewBaseFunction("lag", TypeAnalytical, "分析函数", "返回前N行的值", 1, 4),
		Offset:       1, // 设置默认偏移量为1
	}
}

func (f *LagFunction) Validate(args []any) error {
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

func (f *LagFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	currentValue := args[0]

	var result any
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
		PreviousValues: make([]any, 0),
	}
	return newFunc
}

func (f *LagFunction) Add(value any) {
	// 增量添加值，维护历史值队列
	f.PreviousValues = append(f.PreviousValues, value)
	// 保持队列长度
	if len(f.PreviousValues) > f.Offset*2 {
		f.PreviousValues = f.PreviousValues[1:]
	}
}

func (f *LagFunction) Result() any {
	// 检查是否有足够的历史值
	if len(f.PreviousValues) <= f.Offset {
		return f.DefaultValue
	}
	// 返回当前值之前第Offset个值
	// 对于数组[first, second, third]，当前位置是最后一个元素third（索引2）
	// offset=1时应该返回second（索引1），计算：len-1-offset = 3-1-1 = 1
	// offset=2时应该返回first（索引0），计算：len-1-offset = 3-1-2 = 0
	// 索引计算：len-1-offset，即从最后一个元素往前数offset个位置
	return f.PreviousValues[len(f.PreviousValues)-1-f.Offset]
}

func (f *LagFunction) Clone() AggregatorFunction {
	clone := &LagFunction{
		BaseFunction:   f.BaseFunction,
		DefaultValue:   f.DefaultValue,
		Offset:         f.Offset,
		PreviousValues: make([]any, len(f.PreviousValues)),
	}
	copy(clone.PreviousValues, f.PreviousValues)
	return clone
}

// LatestFunction 最新值函数 - 返回指定列的最新值
type LatestFunction struct {
	*BaseFunction
	LatestValue any
}

func NewLatestFunction() *LatestFunction {
	return &LatestFunction{
		BaseFunction: NewBaseFunction("latest", TypeAnalytical, "分析函数", "返回最新值", 1, 2),
	}
}

func (f *LatestFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *LatestFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

func (f *LatestFunction) Add(value any) {
	f.LatestValue = value
}

func (f *LatestFunction) Result() any {
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
	PreviousValues map[string]any
}

func NewChangedColFunction() *ChangedColFunction {
	return &ChangedColFunction{
		BaseFunction:   NewBaseFunction("changed_col", TypeAnalytical, "分析函数", "返回变化的列值", 2, 2),
		PreviousValues: make(map[string]any),
	}
}

func (f *ChangedColFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ChangedColFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	currentValue := args[0]
	// 假设currentValue是一个map[string]any，代表当前行数据
	currentMap, ok := currentValue.(map[string]any)
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
	f.PreviousValues = make(map[string]any)
}

// 实现AggregatorFunction接口 - 增量计算支持
func (f *ChangedColFunction) New() AggregatorFunction {
	return &ChangedColFunction{
		BaseFunction:   f.BaseFunction,
		PreviousValues: make(map[string]any),
	}
}

func (f *ChangedColFunction) Add(value any) {
	// 对于changed_col函数，每次Add都会更新状态
	currentMap, ok := value.(map[string]any)
	if !ok {
		return
	}

	for key, val := range currentMap {
		f.PreviousValues[key] = val
	}
}

func (f *ChangedColFunction) Result() any {
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
		PreviousValues: make(map[string]any),
	}
	for k, v := range f.PreviousValues {
		clone.PreviousValues[k] = v
	}
	return clone
}

// HadChangedFunction 是否变化函数 - 判断指定列的值是否发生变化
type HadChangedFunction struct {
	*BaseFunction
	PreviousValue any
	IsSet         bool // 标记PreviousValue是否已被设置
}

func NewHadChangedFunction() *HadChangedFunction {
	return &HadChangedFunction{
		BaseFunction: NewBaseFunction("had_changed", TypeAnalytical, "分析函数", "判断值是否变化", 2, -1),
	}
}

func (f *HadChangedFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *HadChangedFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

func (f *HadChangedFunction) Add(value any) {
	f.PreviousValue = value
	f.IsSet = true
}

func (f *HadChangedFunction) Result() any {
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
func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// 使用reflect.DeepEqual进行深度比较，可以处理复杂类型
	return reflect.DeepEqual(a, b)
}

// ===== 流式状态机实现（走直连路径逐条 Apply）=====
// 与 AggregatorFunction 的批量 Add/Result 不同，这里是跨事件、逐条 Apply 的状态机，
// 由 stream.AnalyticEngine 为每个 PARTITION 各持一份。

// lagState 维护最近 offset 个历史值，Apply 返回前 offset 个值（无则 default/nil）。
type lagState struct {
	history []any
}

func (s *lagState) Apply(args []any) any {
	if len(args) == 0 {
		return nil
	}
	val := args[0]
	offset := 1
	if len(args) >= 2 {
		if n, ok := analyticToInt(args[1]); ok && n > 0 {
			offset = n
		}
	}
	var def any
	hasDef := false
	if len(args) >= 3 {
		def = args[2]
		hasDef = true
	}
	// 第 4 参数 ignoreNull：nil 值跳过，不存入历史（默认 true，与主流分析函数一致）
	ignoreNull := true
	if len(args) >= 4 {
		ignoreNull = AnalyticToBool(args[3])
	}
	var result any
	if len(s.history) >= offset {
		result = s.history[len(s.history)-offset]
	} else if hasDef {
		result = def
	}
	if !(ignoreNull && val == nil) {
		s.history = append(s.history, val)
		if len(s.history) > offset {
			s.history = s.history[len(s.history)-offset:]
		}
	}
	return result
}

func (s *lagState) Reset() { s.history = nil }

// NewState 实现 StatefulAnalytic。
func (f *LagFunction) NewState() AnalyticState { return &lagState{} }

// latestState 维护最新非空值。
type latestState struct {
	latest any
	hasVal bool
}

func (s *latestState) Apply(args []any) any {
	if len(args) > 0 && args[0] != nil {
		s.latest = args[0]
		s.hasVal = true
	}
	if s.hasVal {
		return s.latest
	}
	if len(args) >= 2 {
		return args[1] // default
	}
	return nil
}

func (s *latestState) Reset() { s.latest = nil; s.hasVal = false }

func (f *LatestFunction) NewState() AnalyticState { return &latestState{} }

// hadChangedState 维护各列上次值，Apply 返回是否有变化（首次视为变化。
type hadChangedState struct {
	prev       []any
	first      bool
	prevNamed  map[string]any
	firstNamed bool
}

func (s *hadChangedState) Apply(args []any) any {
	ignoreNull := false
	var values []any
	if len(args) > 0 {
		ignoreNull = AnalyticToBool(args[0])
		values = args[1:]
	}
	if !s.first {
		s.first = true
		s.prev = append([]any(nil), values...)
		return true
	}
	changed := false
	// ignoreNull+nil：不触发变化，且保留旧基准（不把 nil 写入 prev）。
	newPrev := make([]any, len(values))
	for i, v := range values {
		if ignoreNull && v == nil {
			if i < len(s.prev) {
				newPrev[i] = s.prev[i]
			}
			continue
		}
		newPrev[i] = v
		if i >= len(s.prev) || !analyticEqual(s.prev[i], v) {
			changed = true
		}
	}
	s.prev = newPrev
	return changed
}

// ApplyNamed 按列名比较整行（had_changed '*' 用）。列增删/乱序均按名字判定，
// 不受位置影响。ignoreNull+nil 不触发变化且保留旧基准。
func (s *hadChangedState) ApplyNamed(ignoreNull bool, cols map[string]any) any {
	if !s.firstNamed {
		s.firstNamed = true
		s.prevNamed = make(map[string]any, len(cols))
		for k, v := range cols {
			if !(ignoreNull && v == nil) {
				s.prevNamed[k] = v
			}
		}
		return true
	}
	changed := false
	// 新增/变化的列
	for k, v := range cols {
		if ignoreNull && v == nil {
			continue
		}
		if pv, had := s.prevNamed[k]; !had || !analyticEqual(pv, v) {
			changed = true
		}
	}
	// 删除的列（旧基准有、本次无）也算变化
	for k, pv := range s.prevNamed {
		if _, had := cols[k]; !had {
			if !(ignoreNull && pv == nil) {
				changed = true
			}
		}
	}
	// 更新基准：ignoreNull+nil 保留旧值
	next := make(map[string]any, len(cols))
	for k, v := range cols {
		if ignoreNull && v == nil {
			if pv, had := s.prevNamed[k]; had {
				next[k] = pv
			}
			continue
		}
		next[k] = v
	}
	s.prevNamed = next
	return changed
}

func (s *hadChangedState) Reset() {
	s.prev = nil
	s.first = false
	s.prevNamed = nil
	s.firstNamed = false
}

func (f *HadChangedFunction) NewState() AnalyticState { return &hadChangedState{} }
