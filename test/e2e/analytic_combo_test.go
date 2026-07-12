package e2e

import (
	"reflect"
	"sort"
	"testing"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 本文件专门覆盖"多特性真实组合"场景：JOIN×分析、聚合间算术、双分析算术、
// LEFT JOIN×COALESCE 空值填充、多分组多指标不变式。期望值均按 SQL 语义推导，
// 用于验证组合路径的正确性；不与单特性文件重复。

// runDirectJoin 在直连路径上跑 JOIN 查询：Execute 后注册表，逐条 EmitSync 收非 nil 行。
func runDirectJoin(t *testing.T, sql, table string, tableRows []map[string]any, inputs []map[string]any) []map[string]any {
	t.Helper()
	ssql := streamsql.New()
	if err := ssql.Execute(sql); err != nil {
		t.Fatalf("Execute %q: %v", sql, err)
	}
	defer ssql.Stop()
	if _, err := ssql.RegisterTable(table, tableRows); err != nil {
		t.Fatalf("RegisterTable %s: %v", table, err)
	}
	var out []map[string]any
	for _, in := range inputs {
		r, err := ssql.EmitSync(copyRow(in))
		if err != nil {
			t.Fatalf("EmitSync %q: %v", sql, err)
		}
		if r != nil {
			out = append(out, r)
		}
	}
	return out
}

// === JOIN × 分析函数（元数据增强后按增强字段分区做 lag/累计）===

// JOIN 增强出 location 后，lag(temp) 按 location 分区取上一条 temp。
// meta: d1→plantA, d2→plantB, d3→plantA。
//
// PARTITION BY 连接字段 m.location 经 resolvePartitionField 走 fieldpath 解析
// （增强行里 location 嵌套在 m 下，裸 row[k] 恒 nil 会退化成全局分区）。
func TestScenario_JoinAnalytic_LagByLocation(t *testing.T) {
	const sql = `SELECT deviceId, m.location AS loc, lag(temp) OVER (PARTITION BY m.location) AS prev
FROM stream JOIN meta m ON deviceId = m.deviceId`
	meta := []map[string]any{
		{"deviceId": "d1", "location": "plantA"},
		{"deviceId": "d2", "location": "plantB"},
		{"deviceId": "d3", "location": "plantA"},
	}
	in := []map[string]any{
		{"deviceId": "d1", "temp": 10}, // plantA 首条 → prev=nil
		{"deviceId": "d2", "temp": 20}, // plantB 首条 → prev=nil
		{"deviceId": "d3", "temp": 30}, // plantA 第2条 → prev=10
		{"deviceId": "d1", "temp": 40}, // plantA 第3条 → prev=30
		{"deviceId": "d2", "temp": 50}, // plantB 第2条 → prev=20
	}
	got := runDirectJoin(t, sql, "meta", meta, in)
	wantPrev := []any{nil, nil, 10.0, 30.0, 20.0}
	wantLoc := []string{"plantA", "plantB", "plantA", "plantA", "plantB"}
	if len(got) != len(wantPrev) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(wantPrev), got)
	}
	for i, r := range got {
		if r["loc"] != wantLoc[i] {
			t.Errorf("row %d loc=%v want %s", i, r["loc"], wantLoc[i])
		}
		if wantPrev[i] == nil {
			if r["prev"] != nil {
				t.Errorf("row %d prev=%v want nil", i, r["prev"])
			}
		} else if toFloatVal(r["prev"]) != wantPrev[i].(float64) {
			t.Errorf("row %d prev=%v want %v", i, r["prev"], wantPrev[i])
		}
	}
}

// JOIN 增强后 acc_sum(temp) 按 location 分区跨行累计（同 LagByLocation，分区键走 fieldpath）。
func TestScenario_JoinAnalytic_AccSumByLocation(t *testing.T) {
	const sql = `SELECT m.location AS loc, acc_sum(temp) OVER (PARTITION BY m.location) AS s
FROM stream JOIN meta m ON deviceId = m.deviceId`
	meta := []map[string]any{
		{"deviceId": "d1", "location": "plantA"},
		{"deviceId": "d2", "location": "plantB"},
		{"deviceId": "d3", "location": "plantA"},
	}
	in := []map[string]any{
		{"deviceId": "d1", "temp": 10}, // plantA → 10
		{"deviceId": "d2", "temp": 20}, // plantB → 20
		{"deviceId": "d3", "temp": 30}, // plantA → 40
		{"deviceId": "d1", "temp": 40}, // plantA → 80
		{"deviceId": "d2", "temp": 50}, // plantB → 70
	}
	got := runDirectJoin(t, sql, "meta", meta, in)
	want := []float64{10, 20, 40, 80, 70}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if toFloatVal(r["s"]) != want[i] {
			t.Errorf("row %d s=%v want %v", i, r["s"], want[i])
		}
	}
}

// === 聚合间算术（窗口聚合结果再做运算）===

// 窗口 max-min 极差。CountingWindow(2) 三窗：[3,7]→4, [1,4]→3, [10,2]→8。
func TestScenario_AggArithmetic_WindowRange(t *testing.T) {
	in := []map[string]any{{"v": 3}, {"v": 7}, {"v": 1}, {"v": 4}, {"v": 10}, {"v": 2}}
	got := runWindow(t, `SELECT max(v) - min(v) AS rng FROM stream GROUP BY CountingWindow(2)`, in)
	if vals := sortedFloatField(got, "rng"); !reflect.DeepEqual(vals, []float64{3, 4, 8}) {
		t.Errorf("window max-min range: got %v, want [3 4 8]", vals)
	}
}

// 手算均值 sum/count 应与内置 avg 一致（浮点输入避免整除）。
// 三窗 [3,7]→5, [1,4]→2.5, [10,2]→6。
func TestScenario_AggArithmetic_ManualAvgMatchesBuiltIn(t *testing.T) {
	in := []map[string]any{{"v": 3.0}, {"v": 7.0}, {"v": 1.0}, {"v": 4.0}, {"v": 10.0}, {"v": 2.0}}
	got := runWindow(t, `SELECT sum(v) / count(*) AS manual, avg(v) AS built FROM stream GROUP BY CountingWindow(2)`, in)
	manual := sortedFloatField(got, "manual")
	built := sortedFloatField(got, "built")
	want := []float64{2.5, 5, 6}
	if !reflect.DeepEqual(manual, want) {
		t.Errorf("manual avg sum/count: got %v, want %v", manual, want)
	}
	if !reflect.DeepEqual(built, want) {
		t.Errorf("built-in avg: got %v, want %v", built, want)
	}
}

// === 双分析函数算术（同一表达式含两个分析调用）===

// 运行期极差 acc_max-acc_min。v:3,1,4,1,5 → acc_max[3,3,4,4,5] acc_min[3,1,1,1,1] → 极差[0,2,3,3,4]。
//
// 同表达式含两个分析调用：splitAnalyticExprMulti 抽全部调用各替换占位
// （__analytic_self__ / __analytic_self_1__），求值期各算各的、注入占位再求 wrapper。
func TestScenario_TwoAnalyticArithmetic_RunningRange(t *testing.T) {
	in := []map[string]any{{"v": 3}, {"v": 1}, {"v": 4}, {"v": 1}, {"v": 5}}
	got := runDirect(t, `SELECT acc_max(v) - acc_min(v) AS rr FROM stream`, in)
	want := []float64{0, 2, 3, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if toFloatVal(r["rr"]) != want[i] {
			t.Errorf("row %d rr=%v want %v", i, r["rr"], want[i])
		}
	}
}

// 三个分析调用 + 混合算术：acc_max + acc_min + acc_sum。
// v:3,1,4,1,5 → max[3,3,4,4,5] min[3,1,1,1,1] sum[3,4,8,9,14] → 三者之和[9,8,13,14,20]。
// 验证同表达式不限于 2 个调用，占位 __analytic_self__/__analytic_self_1__/__analytic_self_2__ 各自回代。
func TestScenario_ThreeAnalyticArithmetic_SumOfRunning(t *testing.T) {
	in := []map[string]any{{"v": 3}, {"v": 1}, {"v": 4}, {"v": 1}, {"v": 5}}
	got := runDirect(t, `SELECT acc_max(v) + acc_min(v) + acc_sum(v) AS s FROM stream`, in)
	want := []float64{9, 8, 13, 14, 20}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if toFloatVal(r["s"]) != want[i] {
			t.Errorf("row %d s=%v want %v", i, r["s"], want[i])
		}
	}
}

// 两个分析调用 + 乘法与标量：acc_max * acc_min。
// v:3,1,4,1,5 → [9,3,4,4,5]。验证算术运算符不限加减，expr bridge 支持的运算符皆可。
func TestScenario_TwoAnalyticArithmetic_Multiply(t *testing.T) {
	in := []map[string]any{{"v": 3}, {"v": 1}, {"v": 4}, {"v": 1}, {"v": 5}}
	got := runDirect(t, `SELECT acc_max(v) * acc_min(v) AS p FROM stream`, in)
	want := []float64{9, 3, 4, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if toFloatVal(r["p"]) != want[i] {
			t.Errorf("row %d p=%v want %v", i, r["p"], want[i])
		}
	}
}

// 复杂优先级表达式：acc_max/10 - acc_min*10 + acc_sum（三个调用 + 乘除加减混合）。
// v:3,1,4,1,5 → max[3,3,4,4,5] min[3,1,1,1,1] sum[3,4,8,9,14]
// row= max/10 - min*10 + sum：-26.7,-5.7,-1.6,-0.6,4.5。验证 */优先于+-、多调用回代正确。
func TestScenario_ThreeAnalyticArithmetic_ComplexPrecedence(t *testing.T) {
	in := []map[string]any{{"v": 3}, {"v": 1}, {"v": 4}, {"v": 1}, {"v": 5}}
	got := runDirect(t, `SELECT acc_max(v)/10 - acc_min(v)*10 + acc_sum(v) AS c FROM stream`, in)
	want := []float64{-26.7, -5.7, -1.6, -0.6, 4.5}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if abs(toFloatVal(r["c"])-want[i]) > 1e-9 {
			t.Errorf("row %d c=%v want %v", i, r["c"], want[i])
		}
	}
}

// === LEFT JOIN × COALESCE（无匹配行空值填充）===

// LEFT JOIN 无匹配时 m.location 为 nil，coalesce 填默认值。
func TestScenario_LeftJoinCoalesce_FillUnknown(t *testing.T) {
	const sql = `SELECT deviceId, coalesce(m.location, 'unknown') AS loc
FROM stream LEFT JOIN meta m ON deviceId = m.deviceId`
	meta := []map[string]any{
		{"deviceId": "d1", "location": "plantA"},
		{"deviceId": "d2", "location": "plantB"},
	}
	in := []map[string]any{
		{"deviceId": "d1"}, // 匹配 → plantA
		{"deviceId": "d9"}, // 无匹配 → unknown
	}
	got := runDirectJoin(t, sql, "meta", meta, in)
	want := []map[string]any{
		{"deviceId": "d1", "loc": "plantA"},
		{"deviceId": "d9", "loc": "unknown"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LEFT JOIN coalesce: got %v, want %v", got, want)
	}
}

// === 多分组多指标不变式（校验指标间内在关系，非逐值）===

// 每窗按 device 分组，断言每组满足：min<=avg<=max 且 sum==count*avg。
// CountingWindow(2) 为全局计数窗口：6 事件 → 3 窗，窗内再按 device 分组。
func TestScenario_MultiGroupMetrics_Invariants(t *testing.T) {
	in := []map[string]any{
		{"device": "A", "v": 1.0}, {"device": "A", "v": 3.0},
		{"device": "B", "v": 2.0}, {"device": "B", "v": 4.0},
		{"device": "A", "v": 5.0}, {"device": "B", "v": 6.0},
	}
	got := runWindow(t,
		`SELECT device, sum(v) AS s, count(*) AS c, avg(v) AS a, min(v) AS mn, max(v) AS mx
FROM stream GROUP BY device, CountingWindow(2)`, in)
	if len(got) == 0 {
		t.Fatal("expected non-empty window output")
	}
	for _, r := range got {
		s := toFloatVal(r["s"])
		c := toFloatVal(r["c"])
		a := toFloatVal(r["a"])
		mn := toFloatVal(r["mn"])
		mx := toFloatVal(r["mx"])
		if mn > a+1e-9 || a > mx+1e-9 {
			t.Errorf("metric order violated min(%v)<=avg(%v)<=max(%v) in %v", mn, a, mx, r)
		}
		if c > 0 && abs(s-c*a) > 1e-6 {
			t.Errorf("sum!=count*avg: sum=%v count=%v avg=%v in %v", s, c, a, r)
		}
	}
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// === 补充：窗口聚合结果用于 HAVING 之外的复合投影 ===

// 同一窗口同时算 sum 与 avg，并对结果做算术（avg*count 应==sum）。
// 校验聚合间一致性：与 ManualAvg 互补，这里断言 sum - avg*count ≈ 0。
func TestScenario_WindowAggConsistency(t *testing.T) {
	in := []map[string]any{{"v": 2.0}, {"v": 4.0}, {"v": 6.0}, {"v": 8.0}}
	got := runWindow(t,
		`SELECT sum(v) AS s, avg(v) AS a, count(*) AS c FROM stream GROUP BY CountingWindow(2)`, in)
	s := sortedFloatField(got, "s")
	a := sortedFloatField(got, "a")
	c := sortedFloatField(got, "c")
	// 两窗 [2,4]→sum6/avg3/count2；[6,8]→sum14/avg7/count2
	wantSum := []float64{6, 14}
	wantAvg := []float64{3, 7}
	wantCnt := []float64{2, 2}
	if !reflect.DeepEqual(s, wantSum) || !reflect.DeepEqual(a, wantAvg) || !reflect.DeepEqual(c, wantCnt) {
		t.Errorf("agg consistency: sum=%v avg=%v count=%v (want sum=%v avg=%v count=%v)", s, a, c, wantSum, wantAvg, wantCnt)
	}
	for i := range s {
		if abs(s[i]-a[i]*c[i]) > 1e-6 {
			t.Errorf("window %d: sum(%v)!=avg(%v)*count(%v)", i, s[i], a[i], c[i])
		}
	}
}

// === 第二批：HAVING 引用未选出聚合 / GROUP BY 表达式 / 多 OVER 分区 / 分析嵌 CASE 或 coalesce / WHERE与分析顺序 ===

// HAVING 引用未在 SELECT 出现的聚合 max(v)。CountingWindow(3) 为按 key 计数窗：
// 每 device 各计满 3 条触发。A=[1,2,1]→c3 max2（max>4 ✗ 过滤）；B=[5,6,1]→c3 max6（>4 ✓）。
// 期望仅 B 一行 c3——证明未选出的 max(v) 被正确补算（否则 nil>4 恒假→空）。
//
// extractHavingAggregates 把 max(v) 注册为隐藏聚合 __having_0__ 让 aggregator 补算，
// HAVING 文本改写为 __having_0__；输出剥离该键。
func TestScenario_Having_NonSelectedAggregate(t *testing.T) {
	in := []map[string]any{
		{"device": "A", "v": 1}, {"device": "B", "v": 5}, {"device": "A", "v": 2},
		{"device": "B", "v": 6}, {"device": "A", "v": 1}, {"device": "B", "v": 1},
	}
	got := runWindow(t,
		`SELECT device, count(*) AS c FROM stream GROUP BY device, CountingWindow(3) HAVING max(v) > 4`, in)
	byDev := map[string]float64{}
	for _, r := range got {
		d, _ := r["device"].(string)
		byDev[d] = toFloatVal(r["c"])
	}
	if len(byDev) != 1 || byDev["B"] != 3 {
		t.Errorf("HAVING max(v)>4 (max 未选出): got %v, want {B:3}", byDev)
	}
}

// GROUP BY 函数表达式 upper(device)。CountingWindow(4) 单窗，upper 后分两组。
// GROUP BY 函数表达式 upper(device)。CountingWindow(2) 为按 key 计数窗：input aa,AA,bb,BB
// → upper 各为 AA,AA,BB,BB。AA 满 2 触发 c2、BB 满 2 触发 c2。期望 {AA:2, BB:2}。
// 若 upper() 未生效（按原始 device 分组），则 4 个不同值各 1 条、无 key 满 2 → 空。
//
// parser 把 upper(device) 读成整体分组键；injectGroupKeyExprs 在 Window.Add 前求值注入；
// projectGroupColumns 把分组键值重命名到输出别名 d。
func TestScenario_GroupBy_FunctionExpression(t *testing.T) {
	in := []map[string]any{
		{"device": "aa"}, {"device": "AA"}, {"device": "bb"}, {"device": "BB"},
	}
	got := runWindow(t,
		`SELECT upper(device) AS d, count(*) AS c FROM stream GROUP BY upper(device), CountingWindow(2)`, in)
	byD := map[string]float64{}
	for _, r := range got {
		d, _ := r["d"].(string)
		byD[d] = toFloatVal(r["c"])
	}
	if len(byD) != 2 || byD["AA"] != 2 || byD["BB"] != 2 {
		t.Errorf("GROUP BY upper(device): got %v, want {AA:2 BB:2}", byD)
	}
}

// GROUP BY 时间函数表达式 hour(timestamp)。hour() 取 "YYYY-MM-DD HH:MM:SS" 串的小时。
// CountingWindow(2) 按 key（小时值）计数：hour10 有 2 条→触发 c2，hour11 有 2 条→触发 c2。
// 期望 {10:2, 11:2}。注：聚合器复合分组键为字符串，故输出 h 列是 "10"/"11"（字符串）。
func TestScenario_GroupBy_HourExpression(t *testing.T) {
	in := []map[string]any{
		{"timestamp": "2026-07-12 10:00:00"},
		{"timestamp": "2026-07-12 10:30:00"}, // hour 10
		{"timestamp": "2026-07-12 11:00:00"}, // hour 11
		{"timestamp": "2026-07-12 11:30:00"}, // hour 11
	}
	got := runWindow(t,
		`SELECT hour(timestamp) AS h, count(*) AS c FROM stream GROUP BY hour(timestamp), CountingWindow(2)`, in)
	byH := map[string]float64{}
	for _, r := range got {
		h, _ := r["h"].(string)
		byH[h] = toFloatVal(r["c"])
	}
	if len(byH) != 2 || byH["10"] != 2 || byH["11"] != 2 {
		t.Errorf("GROUP BY hour(timestamp): got %v, want {10:2 11:2}", byH)
	}
}

// 同一 SELECT 两个分析函数、不同 PARTITION BY，状态机应独立无串扰。
// p1=lag(a) PARTITION BY k1；p2=lag(b) PARTITION BY k2。
func TestScenario_MultiOver_DifferentPartitions(t *testing.T) {
	in := []map[string]any{
		{"k1": 1, "a": 10, "k2": "x", "b": 100},
		{"k1": 2, "a": 20, "k2": "x", "b": 200},
		{"k1": 1, "a": 30, "k2": "y", "b": 300},
		{"k1": 2, "a": 40, "k2": "x", "b": 400},
	}
	got := runDirect(t, `SELECT lag(a) OVER (PARTITION BY k1) AS p1, lag(b) OVER (PARTITION BY k2) AS p2 FROM stream`, in)
	// p1 按 k1：k1=1[行0,2]→nil,10；k1=2[行1,3]→nil,20
	// p2 按 k2：x[行0,1,3]→nil,100,200；y[行2]→nil
	assertNumericField(t, "multi-over p1", got, "p1", []any{nil, nil, 10.0, 20.0})
	assertNumericField(t, "multi-over p2", got, "p2", []any{nil, 100.0, nil, 200.0})
}

// 分析函数嵌在 CASE 里：lag(temp) 解析后回代进 CASE 求值。
// temp 10,25,15,30 → lag[nil,10,25,15] → lag>20 ? up:down → [down,down,up,down]（nil 比较判假→down）。
// 标量套分析：applyWrapper 经 expr bridge→expr 包两级求值，CASE 走 expr 包求值器。
func TestScenario_AnalyticInside_Case(t *testing.T) {
	in := []map[string]any{{"temp": 10}, {"temp": 25}, {"temp": 15}, {"temp": 30}}
	got := runDirect(t, `SELECT CASE WHEN lag(temp) > 20 THEN 'up' ELSE 'down' END AS trend FROM stream`, in)
	want := []string{"down", "down", "up", "down"}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i, r := range got {
		if r["trend"] != want[i] {
			t.Errorf("row %d trend=%v want %s (full %v)", i, r["trend"], want[i], got)
		}
	}
}

// P1 对照组：JOIN + 分析函数，但 PARTITION BY 的是**流自有列** deviceId（行内存在）。
// 与 P1（分区键为连接字段）对照：这里应正确按 deviceId 分区。
func TestScenario_JoinAnalytic_PartitionByStreamColumn(t *testing.T) {
	const sql = `SELECT deviceId, lag(temp) OVER (PARTITION BY deviceId) AS prev
FROM stream JOIN meta m ON deviceId = m.deviceId`
	meta := []map[string]any{
		{"deviceId": "d1", "location": "plantA"},
		{"deviceId": "d2", "location": "plantB"},
	}
	in := []map[string]any{
		{"deviceId": "d1", "temp": 10},
		{"deviceId": "d2", "temp": 20},
		{"deviceId": "d1", "temp": 30},
		{"deviceId": "d2", "temp": 40},
	}
	got := runDirectJoin(t, sql, "meta", meta, in)
	assertNumericField(t, "JOIN+analytic 分区流列", got, "prev", []any{nil, nil, 10.0, 20.0})
}

// coalesce 包分析函数：首行 lag=nil → 填默认 -1。
// temp 10,20,30 → lag[nil,10,20] → coalesce(lag,-1)=[-1,10,20]。
// 标量套分析：applyWrapper 不在 nil 上短路，coalesce(nil,-1)→-1。
func TestScenario_CoalesceWraps_Analytic(t *testing.T) {
	in := []map[string]any{{"temp": 10}, {"temp": 20}, {"temp": 30}}
	got := runDirect(t, `SELECT coalesce(lag(temp), -1) AS safe_prev FROM stream`, in)
	assertNumericField(t, "coalesce(lag,-1)", got, "safe_prev", []any{-1.0, 10.0, 20.0})
}

// WHERE 与分析函数的求值顺序（标准 SQL：WHERE 先过滤，分析只看过滤后的行）。
// temp 10,20,15,30，WHERE temp>12 保留 20,15,30；lag 在过滤后流上 → [nil,20,15]；
// d=temp-lag → [nil,-5,15]。普通 WHERE 场景；CDC 模式（WHERE 引用分析）仍分析在先。
func TestScenario_WhereVsAnalytic_Ordering(t *testing.T) {
	in := []map[string]any{{"temp": 10}, {"temp": 20}, {"temp": 15}, {"temp": 30}}
	got := runDirect(t, `SELECT temp, temp - lag(temp) AS d FROM stream WHERE temp > 12`, in)
	assertNumericField(t, "WHERE先于分析", got, "d", []any{nil, -5.0, 15.0})
}

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
