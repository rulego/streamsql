package functions

import (
	"fmt"
)

// hysteresisState 施密特触发器/死区迟滞状态机。逐条 Apply，返回当前是否处于 on 区。
// enter≥exit：上越限（value≥enter 进 on，value≤exit 回 off）；
// enter<exit：下越限（value≤enter 进 on，value≥exit 回 off）。方向由两阈值大小自动判定。
// on/off 之间的迟滞区保持当前态，挡住边界抖动——这是 changed_col/lag 做不到的核心价值。
type hysteresisState struct {
	on  bool
	has bool
}

func (s *hysteresisState) Apply(args []any) any {
	if len(args) < 3 {
		return s.on
	}
	val, ok := toFloat64Generic(args[0])
	if !ok {
		return s.on // 非数值保持当前态
	}
	enter, ok1 := toFloat64Generic(args[1])
	exit, ok2 := toFloat64Generic(args[2])
	if !ok1 || !ok2 {
		return s.on // 阈值不可解析保持当前态（与 value 一致，避免静默退化成 0）
	}
	if !s.has {
		s.has = true
		if len(args) >= 4 {
			s.on = AnalyticToBool(args[3]) // initial，缺省 off
		}
	}
	if enter >= exit {
		// 上越限
		if s.on {
			if val <= exit {
				s.on = false
			}
		} else if val >= enter {
			s.on = true
		}
	} else {
		// 下越限
		if s.on {
			if val >= exit {
				s.on = false
			}
		} else if val <= enter {
			s.on = true
		}
	}
	return s.on
}

func (s *hysteresisState) Reset() { *s = hysteresisState{} }

// HysteresisFunction hysteresis(value, enter, exit [, initial]) → bool。
// 分析函数（TypeAnalytical）：跨行状态，只能作为字段或带 OVER 由状态机求值。
type HysteresisFunction struct {
	*BaseFunction
}

func NewHysteresisFunction() *HysteresisFunction {
	return &HysteresisFunction{
		BaseFunction: NewBaseFunction("hysteresis", TypeAnalytical, "分析函数", "施密特迟滞/死区（双阈值状态机）", 3, 4),
	}
}

func (f *HysteresisFunction) Validate(args []any) error { return f.ValidateArgCount(args) }

// Execute 标量路径禁用：分析函数需跨行状态，只能作为独立字段/OVER 由状态机求值。
func (f *HysteresisFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

func (f *HysteresisFunction) NewState() AnalyticState { return &hysteresisState{} }

// latchState SR/RS 布尔锁存状态机。set 置位、reset 复位；set 与 reset 同真时由
// setPriority 决定（默认 true=置位优先 SR，传 false=复位优先 RS）。两者皆假则保持。
type latchState struct {
	q bool
}

func (s *latchState) Apply(args []any) any {
	set := len(args) >= 1 && AnalyticToBool(args[0])
	reset := len(args) >= 2 && AnalyticToBool(args[1])
	setPriority := true
	if len(args) >= 3 {
		setPriority = AnalyticToBool(args[2])
	}
	switch {
	case set && reset:
		s.q = setPriority
	case set:
		s.q = true
	case reset:
		s.q = false
	}
	return s.q
}

func (s *latchState) Reset() { *s = latchState{} }

// LatchFunction latch(set, reset [, setPriority]) → bool。SR/RS 锁存。
type LatchFunction struct {
	*BaseFunction
}

func NewLatchFunction() *LatchFunction {
	return &LatchFunction{
		BaseFunction: NewBaseFunction("latch", TypeAnalytical, "分析函数", "SR/RS 布尔锁存", 2, 3),
	}
}

func (f *LatchFunction) Validate(args []any) error { return f.ValidateArgCount(args) }

func (f *LatchFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

func (f *LatchFunction) NewState() AnalyticState { return &latchState{} }
