package functions

import "fmt"

// accState is the universal cumulative state of acc_sum/acc_max/acc_min/acc_count/acc_avg.
// The accumulation range is the regular lifecycle. Optional cumulative conditions: acc_xxx(expr, startExpr, resetExpr),
// startExpr Accumulate only after hits or have started; resetExpr hits reset to zero and stop (until start again).
type accState struct {
	kind    string
	sum     float64
	count   int64
	num     float64 // max/min is the current extremum
	hasNum  bool
	started bool // Condition accumulation: Has it entered the accumulation phase?
}

// resetState Zeros the accumulator and exits the accumulation phase (retaining kind).
func (s *accState) resetState() {
	s.sum = 0
	s.count = 0
	s.num = 0
	s.hasNum = false
	s.started = false
}

func (s *accState) Apply(args []any) any {
	// Conditional cumulation: args[1] = start point, args[2] = reset point (optional Boolean expression).
	hasStart := len(args) >= 2
	hasReset := len(args) >= 3
	if hasReset && AnalyticToBool(args[2]) {
		s.resetState()
		return s.result()
	}
	if hasStart {
		started := AnalyticToBool(args[1])
		if !started && !s.started {
			return s.result() // If the counting stage is not reached, it is not counted
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
			// acc_count Results of counting expressions (including non-numeric columns).
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
		// Null cumulative returns nil (consistent with acc_max/min), avoiding "no data" being mistaken for "mean 0".
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

// accFunction acc_* General function (TypeAnalytical). Direct connection paths go through NewState's accState.
type accFunction struct {
	*BaseFunction
	kind string
}

func (f *accFunction) Validate(args []any) error { return f.ValidateArgCount(args) }

// Execute scalar path disabled: The analysis function needs to be cross-line and can only be used as an independent field/OVER evaluated by the state machine.
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

// changedColState changed_col State: Returns a new value when it changes; returns nil when unchanged.
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
		return nil // null does not trigger changes
	}
	var result any
	if !s.hasPrev || !analyticEqual(s.prev, val) {
		result = val // Changes occur, returning new values
	} else {
		result = nil // Not yet changed
	}
	s.prev = val
	s.hasPrev = true
	return result
}

func (s *changedColState) Reset() { s.prev = nil; s.hasPrev = false }

// NewState implements StatefulAnalytic (changed_col takes a direct path to the state machine).
func (f *ChangedColFunction) NewState() AnalyticState { return &changedColState{} }

// changedColsState changed_cols(prefix, ignoreNull, expr...) Multi-column status:
// Returns {prefix+column_name: new value} contains only the columns that have changed.
type changedColsState struct {
	prev map[string]any
}

// Apply: Single-column interface spacehold; Multi-column functions are called ApplyColumns by the engine.
func (s *changedColsState) Apply(args []any) any { return nil }

// ApplyColumns compares each column with the previous value and returns {prefix+ column name: new value} for the changing column.
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

// ChangedColsFunction changed_cols(prefix, ignoreNull, expr...) Multi-column change detection (SELECT only).
type ChangedColsFunction struct {
	*BaseFunction
}

func NewChangedColsFunction() *ChangedColsFunction {
	return &ChangedColsFunction{BaseFunction: NewBaseFunction("changed_cols", TypeAnalytical, "分析函数", "多列变化值（动态列）", 3, -1)}
}

func (f *ChangedColsFunction) Validate(args []any) error { return f.ValidateArgCount(args) }

// Execute Scalar Path Disabled: Multi-column analysis functions can only be evaluated as independent fields by state machine.
func (f *ChangedColsFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

func (f *ChangedColsFunction) NewState() AnalyticState { return &changedColsState{} }
