package functions

import (
	"fmt"
)

// LagFunction LAG函数 - 返回当前行之前的第N行的值
type LagFunction struct {
	*BaseFunction
}

func NewLagFunction() *LagFunction {
	return &LagFunction{
		BaseFunction: NewBaseFunction("lag", TypeAnalytical, "分析函数", "返回前N行的值", 1, 4),
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
		if offset <= 0 {
			return fmt.Errorf("lag function offset must be a positive integer")
		}
	}
	return nil
}

// Execute 标量路径禁用：分析函数需跨行状态，只能作为独立字段/OVER 由状态机求值。
func (f *LagFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

// LatestFunction 最新值函数 - 返回指定列的最新值
type LatestFunction struct {
	*BaseFunction
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
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

// ChangedColFunction 变化列函数 - 返回发生变化的列名
type ChangedColFunction struct {
	*BaseFunction
}

func NewChangedColFunction() *ChangedColFunction {
	return &ChangedColFunction{
		BaseFunction: NewBaseFunction("changed_col", TypeAnalytical, "分析函数", "返回变化的列值", 2, 2),
	}
}

func (f *ChangedColFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ChangedColFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

// HadChangedFunction 是否变化函数 - 判断指定列的值是否发生变化
type HadChangedFunction struct {
	*BaseFunction
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
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
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
