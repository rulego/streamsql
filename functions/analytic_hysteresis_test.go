package functions

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// 上越限：enter=80 进 on，exit=78 才回 off；中间迟滞区保持——死区核心价值。
func TestHysteresisState_UpperLimit(t *testing.T) {
	s := &hysteresisState{}
	cases := []struct {
		val  any
		want bool
	}{
		{79, false}, // 未达上限
		{80, true},  // 达上限进 on
		{79, true},  // 迟滞区保持 on（无死区会在此刻误翻回 off）
		{78, false}, // 跌破恢复线才 off
		{77, false},
		{78, false}, // 回升未达上限，保持 off
		{80, true},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, s.Apply([]any{c.val, 80, 78}), "val=%v", c.val)
	}
}

// 下越限（低温告警）：enter<exit 自动判定方向，value≤enter 进 on，value≥exit 才 off。
func TestHysteresisState_LowerLimit(t *testing.T) {
	s := &hysteresisState{}
	cases := []struct {
		val  any
		want bool
	}{
		{12, false},
		{10, true},  // 跌入下限进 on
		{12, true},  // 迟滞区保持
		{15, false}, // 升破恢复线才 off
		{16, false},
		{14, false}, // 回落未达下限，保持 off
		{10, true},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, s.Apply([]any{c.val, 10, 15}), "val=%v", c.val)
	}
}

// initial 参数：首条即按初始态，缺省 off。
func TestHysteresisState_Initial(t *testing.T) {
	s := &hysteresisState{}
	assert.Equal(t, true, s.Apply([]any{79, 80, 78, true})) // initial=on，79 未跌破 78 保持 on
	assert.Equal(t, false, s.Apply([]any{70, 80, 78, true}))
}

// 非数值/nil 保持当前态。
func TestHysteresisState_NonNumericKeepsState(t *testing.T) {
	s := &hysteresisState{}
	assert.Equal(t, true, s.Apply([]any{80, 80, 78}))
	assert.Equal(t, true, s.Apply([]any{nil, 80, 78}))
	assert.Equal(t, true, s.Apply([]any{"x", 80, 78}))
}

// 非数值阈值保持态（与 value 非数值一致，不静默退化成 0）。
func TestHysteresisState_NonNumericThresholds(t *testing.T) {
	s := &hysteresisState{}
	assert.Equal(t, true, s.Apply([]any{80, 80, 78}))   // 进 on
	assert.Equal(t, true, s.Apply([]any{79, "x", nil})) // 阈值不可解析，保持 on
}

// initial 仅 seed 初始态，首值仍正常求值（initial=true + 首值已跌破 exit → 立即翻 false）。
func TestHysteresisState_InitialFlipsOnFirstValue(t *testing.T) {
	s := &hysteresisState{}
	assert.Equal(t, false, s.Apply([]any{70, 80, 78, true}))
}

func TestHysteresisState_Reset(t *testing.T) {
	s := &hysteresisState{}
	s.Apply([]any{80, 80, 78})
	s.Reset()
	assert.False(t, s.on)
	assert.False(t, s.has)
}

func TestHysteresisFunction_NewStateIsolation(t *testing.T) {
	f := NewHysteresisFunction()
	a, b := f.NewState(), f.NewState()
	a.Apply([]any{80, 80, 78})
	assert.Equal(t, false, b.Apply([]any{79, 80, 78}), "分区状态相互独立")
}

func TestHysteresisFunction_ScalarDisabled(t *testing.T) {
	f := NewHysteresisFunction()
	_, err := f.Execute(nil, []any{1, 80, 78})
	assert.Error(t, err)
}

// SR 锁存：默认 setPriority=true（置位优先）。
func TestLatchState_SR(t *testing.T) {
	s := &latchState{}
	assert.Equal(t, false, s.Apply([]any{false, false})) // 保持初始 off
	assert.Equal(t, true, s.Apply([]any{true, false}))   // set
	assert.Equal(t, true, s.Apply([]any{false, false}))  // 保持 on
	assert.Equal(t, false, s.Apply([]any{false, true}))  // reset
	assert.Equal(t, true, s.Apply([]any{true, true}))    // 冲突：SR 置位优先
}

// RS 锁存：setPriority=false（复位优先）。
func TestLatchState_RS(t *testing.T) {
	s := &latchState{}
	s.Apply([]any{true, false}) // on
	assert.Equal(t, false, s.Apply([]any{true, true, false}))
}

func TestLatchState_Reset(t *testing.T) {
	s := &latchState{}
	s.Apply([]any{true, false})
	s.Reset()
	assert.False(t, s.q)
}

func TestLatchFunction_NewStateIsolation(t *testing.T) {
	f := NewLatchFunction()
	a, b := f.NewState(), f.NewState()
	a.Apply([]any{true, false})
	assert.Equal(t, false, b.Apply([]any{false, false}))
}

func TestLatchFunction_ScalarDisabled(t *testing.T) {
	f := NewLatchFunction()
	_, err := f.Execute(nil, []any{true, false})
	assert.Error(t, err)
}
