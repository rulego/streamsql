package functions

import "fmt"

// accState 是 acc_sum/acc_max/acc_min/acc_count/acc_avg 的通用累积状态。
// 累积范围为规则生命周期。可选的条件累计：acc_xxx(expr, startExpr, resetExpr)，
// startExpr 命中或已开始才累计，resetExpr 命中则归零并停止（直到再次 start）。
type accState struct {
	kind    string
	sum     float64
	count   int64
	num     float64 // max/min 当前极值
	hasNum  bool
	started bool // 条件累计：是否已进入累计阶段
}

// resetState 归零累加器并退出累计阶段（保留 kind）。
func (s *accState) resetState() {
	s.sum = 0
	s.count = 0
	s.num = 0
	s.hasNum = false
	s.started = false
}

func (s *accState) Apply(args []any) any {
	// 条件累计：args[1]=开始点，args[2]=重置点（可选布尔表达式）。
	hasStart := len(args) >= 2
	hasReset := len(args) >= 3
	if hasReset && AnalyticToBool(args[2]) {
		s.resetState()
		return s.result()
	}
	if hasStart {
		started := AnalyticToBool(args[1])
		if !started && !s.started {
			return s.result() // 未进入累计阶段，不计
		}
		s.started = true
	}
	if len(args) > 0 {
		val := args[0]
		if v, ok := toFloat64Generic(val); ok {
			s.count++
			switch s.kind {
			case "acc_sum", "acc_avg":
				s.sum += v
			case "acc_max":
				if !s.hasNum || v > s.num {
					s.num = v
				}
			case "acc_min":
				if !s.hasNum || v < s.num {
					s.num = v
				}
			}
			s.hasNum = true
		} else if s.kind == "acc_count" && val != nil {
			// acc_count 计数表达式结果（含非数字列）。
			s.count++
		}
	}
	return s.result()
}

func (s *accState) result() any {
	switch s.kind {
	case "acc_sum":
		return s.sum
	case "acc_count":
		return s.count
	case "acc_avg":
		// 空累积返回 nil（与 acc_max/min 一致），避免"无数据"被当成"均值 0"。
		if s.count == 0 {
			return nil
		}
		return s.sum / float64(s.count)
	case "acc_max":
		if s.hasNum {
			return s.num
		}
		return nil
	case "acc_min":
		if s.hasNum {
			return s.num
		}
		return nil
	}
	return nil
}

func (s *accState) Reset() { kind := s.kind; *s = accState{}; s.kind = kind }

// accFunction acc_* 通用函数（TypeAnalytical）。直连路径走 NewState 的 accState。
type accFunction struct {
	*BaseFunction
	kind string
}

func (f *accFunction) Validate(args []any) error { return f.ValidateArgCount(args) }

// Execute 标量路径禁用：分析函数需跨行状态，只能作为独立字段/OVER 由状态机求值。
func (f *accFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

func (f *accFunction) NewState() AnalyticState { return &accState{kind: f.kind} }

func NewAccSumFunction() *accFunction {
	return &accFunction{BaseFunction: NewBaseFunction("acc_sum", TypeAnalytical, "分析函数", "累积求和", 1, 3), kind: "acc_sum"}
}
func NewAccMaxFunction() *accFunction {
	return &accFunction{BaseFunction: NewBaseFunction("acc_max", TypeAnalytical, "分析函数", "累积最大值", 1, 3), kind: "acc_max"}
}
func NewAccMinFunction() *accFunction {
	return &accFunction{BaseFunction: NewBaseFunction("acc_min", TypeAnalytical, "分析函数", "累积最小值", 1, 3), kind: "acc_min"}
}
func NewAccCountFunction() *accFunction {
	return &accFunction{BaseFunction: NewBaseFunction("acc_count", TypeAnalytical, "分析函数", "累积计数", 1, 3), kind: "acc_count"}
}
func NewAccAvgFunction() *accFunction {
	return &accFunction{BaseFunction: NewBaseFunction("acc_avg", TypeAnalytical, "分析函数", "累积平均值", 1, 3), kind: "acc_avg"}
}

// changedColState changed_col 状态：变化时返回新值，未变化返回 nil。
type changedColState struct {
	prev    any
	hasPrev bool
}

func (s *changedColState) Apply(args []any) any {
	ignoreNull := false
	var val any
	if len(args) >= 1 {
		ignoreNull = AnalyticToBool(args[0])
	}
	if len(args) >= 2 {
		val = args[1]
	}
	if ignoreNull && val == nil {
		return nil // null 不触发变化
	}
	var result any
	if !s.hasPrev || !analyticEqual(s.prev, val) {
		result = val // 发生变化，返回新值
	} else {
		result = nil // 未变化
	}
	s.prev = val
	s.hasPrev = true
	return result
}

func (s *changedColState) Reset() { s.prev = nil; s.hasPrev = false }

// NewState 实现 StatefulAnalytic（changed_col 走直连路径状态机）。
func (f *ChangedColFunction) NewState() AnalyticState { return &changedColState{} }

// changedColsState changed_cols(prefix, ignoreNull, expr...) 多列状态：
// 返回 {prefix+列名: 新值} 仅含发生变化的列。
type changedColsState struct {
	prev map[string]any
}

// Apply 单列接口占位；多列函数由引擎调 ApplyColumns。
func (s *changedColsState) Apply(args []any) any { return nil }

// ApplyColumns 比较各列与上次值，返回变化列的 {prefix+列名: 新值}。
func (s *changedColsState) ApplyColumns(prefix string, ignoreNull bool, cols map[string]any) map[string]any {
	if s.prev == nil {
		s.prev = make(map[string]any, len(cols))
	}
	out := make(map[string]any)
	for name, val := range cols {
		if ignoreNull && val == nil {
			continue
		}
		prev, had := s.prev[name]
		if !had || !analyticEqual(prev, val) {
			out[prefix+name] = val
		}
		s.prev[name] = val
	}
	return out
}

func (s *changedColsState) Reset() { s.prev = nil }

// ChangedColsFunction changed_cols(prefix, ignoreNull, expr...) 多列变化检测（仅 SELECT）。
type ChangedColsFunction struct {
	*BaseFunction
}

func NewChangedColsFunction() *ChangedColsFunction {
	return &ChangedColsFunction{BaseFunction: NewBaseFunction("changed_cols", TypeAnalytical, "分析函数", "多列变化值（动态列）", 3, -1)}
}

func (f *ChangedColsFunction) Validate(args []any) error { return f.ValidateArgCount(args) }

// Execute 标量路径禁用：多列分析函数只能作为独立字段由状态机求值。
func (f *ChangedColsFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

func (f *ChangedColsFunction) NewState() AnalyticState { return &changedColsState{} }
