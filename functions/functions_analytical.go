package functions

import (
	"fmt"
)

// LagFunction LAG function - Returns the value of the Nth row before the current line
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

// Execute scalar path disabled: The analysis function needs to be cross-line and can only be used as an independent field/OVER evaluated by the state machine.
func (f *LagFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

// LatestFunction Latest Value Function - Returns the latest value for the specified column
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

// ChangedColFunction - Returns the column name where the change occurred
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

// HadChangedFunction - Checks whether the value of a specified column has changed
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

// ===== Stream state machine implementation (applying each directly connected path one by one) =====
// Unlike AggregatorFunction's batch Add/Result, here it is a state machine that applies across events and applies one by one,
// By stream.AnalyticEngine holds a copy for each PARTITION.

// lagState maintains the most recent offset historical values; Apply returns the previous offset values (if none, default/nil).
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
	// 4th parameter ignoreNull: nil value skipped, not stored in history (default true, consistent with mainstream analysis functions)
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

// NewState implements StatefulAnalytic.
func (f *LagFunction) NewState() AnalyticState { return &lagState{} }

// latestState maintains the latest non-null values.
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

// hadChangedState maintains the previous values in each column, and Apply returns whether there has been a change (the first time is considered a change).
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
	// ignoreNull+nil: does not trigger changes and retains the old reference (nil is not written to prev).
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

// ApplyNamed compares entire rows by column name (had_changed '*'). Additions, deletions, and disordered order are all determined by name,
// Not affected by location. ignoreNull+nil does not trigger changes and retains the old benchmark.
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
	// Columns that have been added/changed
	for k, v := range cols {
		if ignoreNull && v == nil {
			continue
		}
		if pv, had := s.prevNamed[k]; !had || !analyticEqual(pv, v) {
			changed = true
		}
	}
	// Deleted columns (old benchmarks existed, but not this time) also count as changes
	for k, pv := range s.prevNamed {
		if _, had := cols[k]; !had {
			if !(ignoreNull && pv == nil) {
				changed = true
			}
		}
	}
	// Update benchmark: ignoreNull+nil retains the old value
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
