package e2e

import (
	"sort"
	"testing"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 分析函数组合与边界测试：多函数共存、与分区/窗口/条件组合，找潜在 bug。

// 多个分析函数在同一查询共存，各自独立状态。
func TestCombo_MultipleAnalytics_Direct(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(
		`SELECT lag(a) AS p, acc_sum(b) AS s, changed_col(true, c) AS cc FROM stream`))
	defer ssql.Stop()
	in := []map[string]any{{"a": 1, "b": 10, "c": "x"}, {"a": 2, "b": 20, "c": "x"}, {"a": 3, "b": 30, "c": "y"}}
	var got []map[string]any
	for _, r := range in {
		out, err := ssql.EmitSync(copyRow(r))
		require.NoError(t, err)
		got = append(got, out)
	}
	// lag: nil,1,2 ; acc_sum(b): 10,30,60 ; changed_col(c): x(首),nil(未变),y(变→cc 省略 nil)
	assert.Nil(t, got[0]["p"])
	assert.InDelta(t, 10.0, toFloatVal(got[0]["s"]), 0.01)
	assert.Equal(t, "x", got[0]["cc"])
	assert.InDelta(t, 1.0, toFloatVal(got[1]["p"]), 0.01)
	assert.InDelta(t, 30.0, toFloatVal(got[1]["s"]), 0.01)
	_, hasCC1 := got[1]["cc"] // c 未变 → cc 省略
	assert.False(t, hasCC1)
	assert.InDelta(t, 2.0, toFloatVal(got[2]["p"]), 0.01)
	assert.InDelta(t, 60.0, toFloatVal(got[2]["s"]), 0.01)
	assert.Equal(t, "y", got[2]["cc"])
}

// acc_sum + PARTITION BY：每分区各自累加。
func TestCombo_AccSum_Partition(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(
		`SELECT k, acc_sum(v) OVER (PARTITION BY k) AS s FROM stream`))
	defer ssql.Stop()
	in := []map[string]any{{"k": "A", "v": 1}, {"k": "B", "v": 2}, {"k": "A", "v": 3}, {"k": "B", "v": 4}}
	var got []map[string]any
	for _, r := range in {
		out, err := ssql.EmitSync(copyRow(r))
		require.NoError(t, err)
		got = append(got, out)
	}
	// 输入序: A1, B2, A3, B4 → A 累加 1,4；B 累加 2,6（分区独立，互不混入）
	assert.Equal(t, "A", got[0]["k"])
	assert.InDelta(t, 1.0, toFloatVal(got[0]["s"]), 0.01)
	assert.Equal(t, "B", got[1]["k"])
	assert.InDelta(t, 2.0, toFloatVal(got[1]["s"]), 0.01)
	assert.Equal(t, "A", got[2]["k"])
	assert.InDelta(t, 4.0, toFloatVal(got[2]["s"]), 0.01)
	assert.Equal(t, "B", got[3]["k"])
	assert.InDelta(t, 6.0, toFloatVal(got[3]["s"]), 0.01)
}

// 条件 ACC + PARTITION：每分区各自的 start/reset 生命周期。
func TestCombo_CondAcc_Partition(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(
		`SELECT k, acc_count(v, v > 1, v < 0) OVER (PARTITION BY k) AS c FROM stream`))
	defer ssql.Stop()
	// A: 1,2,3,-1,1 → 0,1,2,0,0 ; B: 5,5 → 5>1 计 1,2
	in := []map[string]any{{"k": "A", "v": 1}, {"k": "A", "v": 2}, {"k": "B", "v": 5}, {"k": "A", "v": 3}, {"k": "B", "v": 5}, {"k": "A", "v": -1}}
	var got []map[string]any
	for _, r := range in {
		out, err := ssql.EmitSync(copyRow(r))
		require.NoError(t, err)
		got = append(got, out)
	}
	byK := map[string][]int64{}
	for _, r := range got {
		k, _ := r["k"].(string)
		byK[k] = append(byK[k], toInt64Val(r["c"]))
	}
	// A 依次: 0,1,2,0 (A 的 -1 归零)；B: 1,2
	assert.Equal(t, []int64{0, 1, 2, 0}, byK["A"], "A 分区条件累计")
	assert.Equal(t, []int64{1, 2}, byK["B"], "B 分区条件累计")
}

// lag 进窗口：对窗口聚合输出跨窗口 lag。
func TestCombo_Lag_InWindow(t *testing.T) {
	d := []map[string]any{{"t": 10.0}, {"t": 20.0}, {"t": 30.0}, {"t": 40.0}}
	got := runWindow(t, `SELECT lag(avg(t)) AS p FROM stream GROUP BY CountingWindow(1)`, d)
	// 每事件一窗，avg=t：10,20,30,40；lag 跨窗口: nil,10,20,30
	var vals []float64
	nilCnt := 0
	for _, r := range got {
		if r["p"] == nil {
			nilCnt++
			continue
		}
		vals = append(vals, toFloatVal(r["p"]))
	}
	sort.Float64s(vals)
	assert.Equal(t, 1, nilCnt, "首窗 lag 应为 nil")
	assert.Equal(t, []float64{10, 20, 30}, vals, "lag(avg) 跨窗口非 nil 值: 10,20,30")
}

// had_changed 进窗口：检测窗口聚合输出是否变化。
func TestCombo_HadChanged_InWindow(t *testing.T) {
	d := []map[string]any{{"t": 10.0}, {"t": 10.0}, {"t": 20.0}, {"t": 20.0}}
	got := runWindow(t, `SELECT had_changed(true, avg(t)) AS h FROM stream GROUP BY CountingWindow(2)`, d)
	// 窗口均值: 10,20 → had_changed: true(首),true(变)
	bools := []bool{}
	for _, r := range got {
		b, _ := r["h"].(bool)
		bools = append(bools, b)
	}
	// 两个窗口都 true（首+变）；若串扰/错位会出 false
	require.Len(t, bools, 2, "应 2 个窗口输出")
	for _, b := range bools {
		assert.True(t, b, "had_changed(avg) 首窗与变化窗均应 true")
	}
}

// 窗口内双内联聚合：changed_cols 同时跟踪 avg 和 max。
func TestCombo_Window_TwoAggregates(t *testing.T) {
	d := []map[string]any{{"t": 10.0}, {"t": 20.0}, {"t": 30.0}, {"t": 30.0}}
	got := runWindow(t, `SELECT changed_cols("c_", true, avg(t), max(t)) FROM stream GROUP BY CountingWindow(2)`, d)
	// 窗口1 [10,20]: avg=15,max=20（首→都变）{c_avg:15,c_max:20}
	// 窗口2 [30,30]: avg=30,max=30（都变）{c_avg:30,c_max:30}
	keys := map[string]bool{}
	for _, r := range got {
		for k := range r {
			if k == "c_avg" || k == "c_max" {
				keys[k] = true
			}
		}
	}
	assert.True(t, keys["c_avg"] && keys["c_max"], "双内联聚合应产出 c_avg 与 c_max 列；got=%v", got)
}

// 文档案例 9 镜像：设备级跨窗口变化——changed_col(avg) 按 GROUP BY 分区。
func TestCombo_DocCase_GroupedCrossWindow(t *testing.T) {
	in := []map[string]any{
		{"deviceId": "A", "temp": 10.0}, {"deviceId": "A", "temp": 20.0},
		{"deviceId": "A", "temp": 30.0}, {"deviceId": "A", "temp": 40.0},
		{"deviceId": "B", "temp": 5.0}, {"deviceId": "B", "temp": 5.0},
	}
	got := runWindow(t, `SELECT deviceId, changed_col(true, avg(temp)) AS chg FROM stream GROUP BY deviceId, CountingWindow(2)`, in)
	// A 窗口均值 15,35；B 窗口均值 5。各设备独立：A→{15,35}，B→{5}。
	byDev := map[string][]float64{}
	for _, r := range got {
		k, _ := r["deviceId"].(string)
		if r["chg"] != nil {
			byDev[k] = append(byDev[k], toFloatVal(r["chg"]))
		}
	}
	for _, v := range byDev {
		sort.Float64s(v)
	}
	assert.Equal(t, []float64{15, 35}, byDev["A"], "A 设备跨窗口变化")
	assert.Equal(t, []float64{5}, byDev["B"], "B 设备跨窗口变化")
}

// D1：窗口查询里分析函数引用裸原始列 → 报错（不再静默返回列名）。
func TestCombo_D1_RawColumnInWindowErrors(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	err := ssql.Execute(`SELECT lag(temperature) AS p FROM stream GROUP BY CountingWindow(2)`)
	assert.Error(t, err, "裸原始列进窗口分析函数应报错")
	assert.Contains(t, err.Error(), "raw column")
}

// D9：分析套分析 / 聚合套分析 → 报错；分析套聚合仍允许。
func TestCombo_D9_NestedAnalyticErrors(t *testing.T) {
	for _, sql := range []string{
		`SELECT lag(lag(a)) AS p FROM stream`,
		`SELECT sum(lag(a)) AS s FROM stream GROUP BY CountingWindow(2)`,
	} {
		ssql := streamsql.New()
		err := ssql.Execute(sql)
		assert.Error(t, err, "嵌套分析应报错: %s", sql)
		ssql.Stop()
	}
	// 分析套聚合（窗口内）仍合法。
	ok := streamsql.New()
	defer ok.Stop()
	assert.NoError(t, ok.Execute(`SELECT changed_cols("t", true, avg(temperature)) FROM stream GROUP BY CountingWindow(2)`))
}

// D5：acc_avg 空累积返回 nil（与 acc_max/min 一致），不返回 0.0。
func TestCombo_D5_AccAvgEmptyNil(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT acc_avg(s) AS a FROM stream`))
	defer ssql.Stop()
	// 非数字输入 → count 保持 0 → nil
	r, err := ssql.EmitSync(map[string]any{"s": "not-a-number"})
	require.NoError(t, err)
	assert.Nil(t, r["a"], "空 acc_avg 应返回 nil 而非 0.0")
}

func toInt64Val(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}
