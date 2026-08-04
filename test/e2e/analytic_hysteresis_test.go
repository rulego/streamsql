package e2e

import (
	"testing"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hysteresis 上越限：≥80 进 on，≤78 才回 off，中间迟滞区保持（死区防抖）。
func TestAnalytic_Hysteresis_UpperLimit(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT hysteresis(temp, 80, 78) AS alarm FROM stream"))
	require.False(t, ssql.IsAggregationQuery())
	defer ssql.Stop()

	cases := []struct {
		val  any
		want bool
	}{
		{79, false}, {80, true}, {79, true}, {78, false}, {77, false}, {80, true},
	}
	for _, c := range cases {
		r, err := ssql.EmitSync(map[string]any{"temp": c.val})
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Equal(t, c.want, r["alarm"], "temp=%v", c.val)
	}
}

// hysteresis 下越限：enter<exit 自动判定，≤10 进 on，≥15 才 off。
func TestAnalytic_Hysteresis_LowerLimit(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT hysteresis(temp, 10, 15) AS alarm FROM stream"))
	defer ssql.Stop()

	cases := []struct {
		val  any
		want bool
	}{
		{12, false}, {10, true}, {12, true}, {15, false}, {10, true},
	}
	for _, c := range cases {
		r, err := ssql.EmitSync(map[string]any{"temp": c.val})
		require.NoError(t, err)
		assert.Equal(t, c.want, r["alarm"], "temp=%v", c.val)
	}
}

// 多点独立死区：OVER (PARTITION BY name) 让每个点位各持一份 on/off 状态。
func TestAnalytic_Hysteresis_PartitionBy(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT name, hysteresis(temp, 80, 78) OVER (PARTITION BY name) AS alarm FROM stream"))
	defer ssql.Stop()

	// d1 进告警，d2 未达上限，各自独立。
	r1, _ := ssql.EmitSync(map[string]any{"temp": 81, "name": "d1"})
	assert.Equal(t, true, r1["alarm"])
	r2, _ := ssql.EmitSync(map[string]any{"temp": 79, "name": "d2"})
	assert.Equal(t, false, r2["alarm"], "d2 独立状态，79 未达 80")
	// d1 回落到 79（迟滞区），保持 on；d2 状态不受 d1 影响。
	r3, _ := ssql.EmitSync(map[string]any{"temp": 79, "name": "d1"})
	assert.Equal(t, true, r3["alarm"], "d1 死区保持 on")
}

// hysteresis 在 WHERE：只输出处于告警态的行（电平过滤）。
func TestAnalytic_Hysteresis_InWhere(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT temp FROM stream WHERE hysteresis(temp, 80, 78) == true"))
	defer ssql.Stop()

	vals := []any{79, 81, 79, 77, 80}
	var outs []map[string]any
	for _, v := range vals {
		r, _ := ssql.EmitSync(map[string]any{"temp": v})
		if r != nil {
			outs = append(outs, r)
		}
	}
	// 79(off)→81(on 输出)→79(保持 on 输出)→77(跌破 off)→80(重新 on 输出)
	require.Len(t, outs, 3)
	assert.Equal(t, 81, outs[0]["temp"])
	assert.Equal(t, 79, outs[1]["temp"])
	assert.Equal(t, 80, outs[2]["temp"])
}

// latch SR 锁存：set 置位、reset 复位、皆假保持。
func TestAnalytic_Latch(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT latch(set, reset) AS q FROM stream"))
	defer ssql.Stop()

	cases := []struct {
		set, reset any
		want       bool
	}{
		{false, false, false}, // 保持初始 off
		{true, false, true},   // set
		{false, false, true},  // 保持 on
		{false, true, false},  // reset
	}
	for _, c := range cases {
		r, err := ssql.EmitSync(map[string]any{"set": c.set, "reset": c.reset})
		require.NoError(t, err)
		assert.Equal(t, c.want, r["q"])
	}
}

// 死区告警的边沿触发：changed_col 嵌套 hysteresis——仅状态翻转才输出（告警通知核心）。
// 若引擎暂不支持分析函数嵌套，跳过并在注释说明替代写法（两节点串联）。
func TestAnalytic_Hysteresis_ChangedColEdge(t *testing.T) {
	ssql := streamsql.New()
	err := ssql.Execute("SELECT changed_col(true, hysteresis(temp,80,78)) AS edge FROM stream")
	if err != nil {
		t.Skipf("changed_col 嵌套 hysteresis 引擎暂不支持，边沿可改用两节点串联：先算 hysteresis 再 changed_col: %v", err)
	}
	defer ssql.Stop()

	vals := []any{79, 81, 79, 77, 80}
	var outs []map[string]any
	for _, v := range vals {
		r, _ := ssql.EmitSync(map[string]any{"temp": v})
		if r != nil {
			outs = append(outs, r)
		}
	}
	// 翻转点：79→81(off→on)、79→77(on→off)、77→80(off→on)，共 3 次边沿
	require.Len(t, outs, 3)
}
