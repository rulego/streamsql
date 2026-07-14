package e2e

import (
	"testing"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CDC 场景2：各设备的电流变化后超过阈值（lag 在 WHERE + OVER PARTITION BY）。
// 预期输出 ts5(d1,current=500) 与 ts8(d2,current=600)。
func TestAnalytic_CDC_LagInWhere_PartitionBy(t *testing.T) {
	ssql := streamsql.New()
	err := ssql.Execute("SELECT current, deviceId, ts FROM stream WHERE current > 300 AND lag(current) OVER (PARTITION BY deviceId) < 300")
	require.NoError(t, err)
	require.False(t, ssql.IsAggregationQuery(), "纯分析函数查询应为非聚合（走 streamTransform/EmitSync）")
	defer ssql.Stop()

	inputs := []map[string]any{
		{"current": 300, "ts": 1, "deviceId": 1},
		{"current": 400, "ts": 2, "deviceId": 2},
		{"current": 200, "ts": 3, "deviceId": 1},
		{"current": 200, "ts": 4, "deviceId": 2},
		{"current": 500, "ts": 5, "deviceId": 1},
		{"current": 200, "ts": 6, "deviceId": 2},
		{"current": 400, "ts": 7, "deviceId": 1},
		{"current": 600, "ts": 8, "deviceId": 2},
	}
	var outputs []map[string]any
	for _, in := range inputs {
		r, err := ssql.EmitSync(in)
		require.NoError(t, err)
		if r != nil {
			outputs = append(outputs, r)
		}
	}
	require.Len(t, outputs, 2, "应输出 ts5(d1) 与 ts8(d2)")
	assert.Equal(t, 500, outputs[0]["current"])
	assert.Equal(t, 1, outputs[0]["deviceId"])
	assert.Equal(t, 600, outputs[1]["current"])
	assert.Equal(t, 2, outputs[1]["deviceId"])
}

// lag 在 SELECT（无 OVER）：返回前一个值，首行 nil。
func TestAnalytic_LagInSelect(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT lag(temperature) AS prev_temp FROM stream"))
	require.False(t, ssql.IsAggregationQuery())
	defer ssql.Stop()

	r1, err := ssql.EmitSync(map[string]any{"temperature": 23})
	require.NoError(t, err)
	require.NotNil(t, r1)
	assert.Nil(t, r1["prev_temp"], "首行 lag 无前值")

	r2, _ := ssql.EmitSync(map[string]any{"temperature": 25})
	assert.Equal(t, 23, r2["prev_temp"])

	r3, _ := ssql.EmitSync(map[string]any{"temperature": 27})
	assert.Equal(t, 25, r3["prev_temp"])
}

// had_changed 在 WHERE：只输出变化的行（首次视为变化。
func TestAnalytic_HadChangedInWhere(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT ts, temperature FROM stream WHERE had_changed(true, temperature) == true"))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"ts": 1, "temperature": 23},
		{"ts": 2, "temperature": 23},
		{"ts": 3, "temperature": 25},
		{"ts": 4, "temperature": 25},
		{"ts": 5, "temperature": 27},
	}
	var outs []map[string]any
	for _, in := range inputs {
		r, _ := ssql.EmitSync(in)
		if r != nil {
			outs = append(outs, r)
		}
	}
	require.Len(t, outs, 3, "首次 + 两次变化")
	assert.Equal(t, 1, outs[0]["ts"])
	assert.Equal(t, 3, outs[1]["ts"])
	assert.Equal(t, 5, outs[2]["ts"])
}

// latest：返回最新非空值，nil 不更新状态。
func TestAnalytic_Latest(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT latest(temperature) AS lt FROM stream"))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"temperature": 23})
	assert.Equal(t, 23, r1["lt"])

	r2, _ := ssql.EmitSync(map[string]any{"temperature": 25})
	assert.Equal(t, 25, r2["lt"])

	// nil 不更新状态，仍返回上次非空值 25
	r3, _ := ssql.EmitSync(map[string]any{"temperature": nil})
	assert.Equal(t, 25, r3["lt"])
}

// lag 无 OVER + 普通字段混选。
func TestAnalytic_LagWithPlainField(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT temperature, lag(temperature) AS prev FROM stream"))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"temperature": 10})
	assert.Equal(t, 10, r1["temperature"])
	assert.Nil(t, r1["prev"])

	r2, _ := ssql.EmitSync(map[string]any{"temperature": 20})
	assert.Equal(t, 20, r2["temperature"])
	assert.Equal(t, 10, r2["prev"])
}

// acc_sum：规则生命周期内累积求和。
func TestAnalytic_AccSum(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT acc_sum(value) AS total FROM stream"))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"value": 10})
	assert.Equal(t, 10.0, r1["total"])
	r2, _ := ssql.EmitSync(map[string]any{"value": 20})
	assert.Equal(t, 30.0, r2["total"])
	r3, _ := ssql.EmitSync(map[string]any{"value": 30})
	assert.Equal(t, 60.0, r3["total"])
}

// acc_avg：累积平均值。
func TestAnalytic_AccAvg(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT acc_avg(value) AS avg_v FROM stream"))
	defer ssql.Stop()

	ssql.EmitSync(map[string]any{"value": 10})
	ssql.EmitSync(map[string]any{"value": 20})
	r3, _ := ssql.EmitSync(map[string]any{"value": 30})
	assert.Equal(t, 20.0, r3["avg_v"]) // (10+20+30)/3
}

// acc_max / acc_count：累积极值与计数。
func TestAnalytic_AccMaxCount(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT acc_max(value) AS mx, acc_count(value) AS cnt FROM stream"))
	defer ssql.Stop()

	ssql.EmitSync(map[string]any{"value": 10})
	ssql.EmitSync(map[string]any{"value": 50})
	r3, _ := ssql.EmitSync(map[string]any{"value": 30})
	assert.Equal(t, 50.0, r3["mx"])
	assert.Equal(t, int64(3), r3["cnt"])
}

// changed_col：变化时返回新值，未变化返回 nil。
func TestAnalytic_ChangedCol(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT changed_col(true, temperature) AS chg FROM stream"))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"temperature": 23})
	assert.Equal(t, 23, r1["chg"], "首次视为变化")

	r2, _ := ssql.EmitSync(map[string]any{"temperature": 23})
	assert.Nil(t, r2["chg"], "未变化返回 nil")

	r3, _ := ssql.EmitSync(map[string]any{"temperature": 25})
	assert.Equal(t, 25, r3["chg"])
}

// lag OVER WHEN：满足条件才更新状态，否则复用上次结果。
func TestAnalytic_LagWithWhen(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT lag(value) OVER (WHEN value > 20) AS prev FROM stream"))
	defer ssql.Stop()

	// value=10 不满足 WHEN → lag 状态不更新，prev=nil
	r1, _ := ssql.EmitSync(map[string]any{"value": 10})
	assert.Nil(t, r1["prev"])
	// value=25 满足 → 状态更新，但无前一个有效值，prev=nil
	r2, _ := ssql.EmitSync(map[string]any{"value": 25})
	assert.Nil(t, r2["prev"])
	// value=30 满足 → prev=上一次有效值 25
	r3, _ := ssql.EmitSync(map[string]any{"value": 30})
	assert.Equal(t, 25, r3["prev"])
}

// lag 多偏移：lag(value, 2) 返回前 2 个值。
func TestAnalytic_LagOffset(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT lag(value, 2) AS prev2 FROM stream"))
	defer ssql.Stop()

	ssql.EmitSync(map[string]any{"value": 10})
	r2, _ := ssql.EmitSync(map[string]any{"value": 20})
	assert.Nil(t, r2["prev2"], "仅 2 个值，offset=2 无前值")
	r3, _ := ssql.EmitSync(map[string]any{"value": 30})
	assert.Equal(t, 10, r3["prev2"])
	r4, _ := ssql.EmitSync(map[string]any{"value": 40})
	assert.Equal(t, 20, r4["prev2"])
}

// CDC 场景1：整流变化后超过阈值（无 PARTITION 的 lag 在 WHERE）。
// 预期输出 ts2(400)、ts4(500)。
func TestAnalytic_CDC_LagInWhere_NoPartition(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT concurrency, ts FROM stream WHERE concurrency > 300 AND lag(concurrency) <= 300"))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"concurrency": 100, "ts": 1},
		{"concurrency": 400, "ts": 2},
		{"concurrency": 300, "ts": 3},
		{"concurrency": 500, "ts": 4},
		{"concurrency": 600, "ts": 5},
	}
	var outs []map[string]any
	for _, in := range inputs {
		r, _ := ssql.EmitSync(in)
		if r != nil {
			outs = append(outs, r)
		}
	}
	require.Len(t, outs, 2)
	assert.Equal(t, 400, outs[0]["concurrency"])
	assert.Equal(t, 500, outs[1]["concurrency"])
}

// A1：OVER WHEN 含嵌套函数调用（CDC S2 状态时长的解析，之前 parseOverWhen 截断）。
func TestAnalytic_LagOverWhenNestedFunc(t *testing.T) {
	ssql := streamsql.New()
	err := ssql.Execute("SELECT lag(status) OVER (WHEN had_changed(true, status)) AS prev_status FROM stream")
	require.NoError(t, err, "WHEN 含嵌套函数调用应正确解析（A1）")
	defer ssql.Stop()
	ssql.EmitSync(map[string]any{"status": 1})
}

// A2：had_changed(ignoreNull=true) 遇 nil 不覆盖基准。
func TestAnalytic_HadChangedIgnoreNull(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT had_changed(true, temperature) AS chg FROM stream"))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"temperature": 23})
	assert.Equal(t, true, r1["chg"], "首次视为变化")

	// nil 不触发变化、不更新基准（基准仍为 23）
	r2, _ := ssql.EmitSync(map[string]any{"temperature": nil})
	assert.Equal(t, false, r2["chg"])

	// 与基准 23 相等 → 未变化（A2 修复前因基准被 nil 污染而误报 true）
	r3, _ := ssql.EmitSync(map[string]any{"temperature": 23})
	assert.Equal(t, false, r3["chg"], "nil 后基准保留 23，23==23 未变化")

	r4, _ := ssql.EmitSync(map[string]any{"temperature": 25})
	assert.Equal(t, true, r4["chg"])
}

// A3：lag 第 4 参数 ignoreNull，nil 值跳过不存入历史。
func TestAnalytic_LagIgnoreNull(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT lag(value, 1, -1, true) AS lg FROM stream"))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"value": 10})
	assert.Equal(t, -1.0, r1["lg"], "首次无前值返回 default=-1")

	// nil 被 ignoreNull 跳过，不进历史
	ssql.EmitSync(map[string]any{"value": nil})

	// 上一个有效值是 10
	r3, _ := ssql.EmitSync(map[string]any{"value": 30})
	assert.Equal(t, 10, r3["lg"])
}

// A4：acc_count 计数非数字列。
func TestAnalytic_AccCountNonNumeric(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT acc_count(name) AS cnt FROM stream"))
	defer ssql.Stop()

	ssql.EmitSync(map[string]any{"name": "a"})
	ssql.EmitSync(map[string]any{"name": "b"})
	r3, _ := ssql.EmitSync(map[string]any{"name": "c"})
	assert.Equal(t, int64(3), r3["cnt"], "acc_count 应计数非数字表达式结果")
}

// B2：WHEN 满足→不满足→满足，不满足时复用上次结果。
func TestAnalytic_WhenTransition(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT lag(value) OVER (WHEN value > 20) AS prev FROM stream"))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"value": 25})
	assert.Nil(t, r1["prev"], "首次满足 WHEN，无前有效值")
	r2, _ := ssql.EmitSync(map[string]any{"value": 10})
	assert.Nil(t, r2["prev"], "不满足 WHEN，复用上次结果 nil")
	r3, _ := ssql.EmitSync(map[string]any{"value": 30})
	assert.Equal(t, 25, r3["prev"], "满足 WHEN，上一个有效值 25")
}
