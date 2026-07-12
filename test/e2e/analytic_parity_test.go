package e2e

import (
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	streamsql "github.com/rulego/streamsql"
)

// 分析函数标准示例集，覆盖变化检测 / lag / latest / 累计 / 条件累计，
// 作为分析函数语法的回归门禁。

func analyticDemo() []map[string]any {
	return []map[string]any{
		{"ts": 1, "temperature": 23, "humidity": 88},
		{"ts": 2, "temperature": 23, "humidity": 88},
		{"ts": 3, "temperature": 23, "humidity": 88},
		{"ts": 4, "temperature": 25, "humidity": 88},
		{"ts": 5, "temperature": 25, "humidity": 90},
		{"ts": 6, "temperature": 25, "humidity": 91},
		{"ts": 7, "temperature": 25, "humidity": 91},
		{"ts": 8, "temperature": 25, "humidity": 91},
	}
}

func runWindow(t *testing.T, sql string, inputs []map[string]any) []map[string]any {
	t.Helper()
	ssql := streamsql.New()
	if err := ssql.Execute(sql); err != nil {
		t.Fatalf("Execute %q: %v", sql, err)
	}
	defer ssql.Stop() // 失败路径兜底（t.Fatalf → Goexit 仍执行）；正常路径已显式 Stop，幂等。
	var mu sync.Mutex
	var out []map[string]any
	ssql.AddSink(func(r []map[string]any) {
		mu.Lock()
		out = append(out, r...)
		mu.Unlock()
	})
	for _, in := range inputs {
		ssql.Emit(copyRow(in))
	}
	time.Sleep(500 * time.Millisecond) // 让窗口触发 + sink 回调执行完毕
	ssql.Stop()                        // 停止并 join sink worker：在跑回调跑完、不再有并发写 out
	mu.Lock()
	result := out
	mu.Unlock()
	return result
}

func runDirect(t *testing.T, sql string, inputs []map[string]any) []map[string]any {
	t.Helper()
	ssql := streamsql.New()
	if err := ssql.Execute(sql); err != nil {
		t.Fatalf("Execute %q: %v", sql, err)
	}
	defer ssql.Stop()
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

func copyRow(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// assertRows 按序断言结果行（直接路径结果有序）。
func assertRows(t *testing.T, sql string, got, want []map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s\n got: %v\nwant: %v", sql, got, want)
	}
}

func TestAnalytic_ChangedCols(t *testing.T) {
	d := analyticDemo()
	// 单列无前缀：首值 + 变化值
	assertRows(t, "E1", runDirect(t, `SELECT changed_cols("", true, temperature) FROM stream`, d),
		[]map[string]any{{"temperature": 23}, {"temperature": 25}})
	// 多列带前缀
	assertRows(t, "E2", runDirect(t, `SELECT changed_cols("c_", true, temperature, humidity) FROM stream`, d),
		[]map[string]any{
			{"c_temperature": 23, "c_humidity": 88},
			{"c_temperature": 25},
			{"c_humidity": 90},
			{"c_humidity": 91},
		})
	// 不忽略 null + *：每行至少输出变化的 ts
	assertRows(t, "E3", runDirect(t, `SELECT changed_cols("c_", false, "*") FROM stream`, d),
		[]map[string]any{
			{"c_ts": 1, "c_temperature": 23, "c_humidity": 88},
			{"c_ts": 2},
			{"c_ts": 3},
			{"c_ts": 4, "c_temperature": 25},
			{"c_ts": 5, "c_humidity": 90},
			{"c_ts": 6, "c_humidity": 91},
			{"c_ts": 7},
			{"c_ts": 8},
		})
}

func TestAnalytic_HadChanged_Where(t *testing.T) {
	d := analyticDemo()
	assertRows(t, "E5", runDirect(t, `SELECT ts, temperature, humidity FROM stream WHERE had_changed(true, temperature, humidity) = true`, d),
		[]map[string]any{
			{"ts": 1, "temperature": 23, "humidity": 88},
			{"ts": 4, "temperature": 25, "humidity": 88},
			{"ts": 5, "temperature": 25, "humidity": 90},
			{"ts": 6, "temperature": 25, "humidity": 91},
		})
	assertRows(t, "E6", runDirect(t, `SELECT ts, temperature, humidity FROM stream WHERE had_changed(true, temperature) = true AND had_changed(true, humidity) = false`, d),
		[]map[string]any{{"ts": 4, "temperature": 25, "humidity": 88}})
}

func TestAnalytic_ChangedCol_AliasAndWhere(t *testing.T) {
	d := analyticDemo()
	// 别名：nil 字段省略、全 nil 行抑制
	assertRows(t, "E7", runDirect(t, `SELECT changed_col(true, temperature) AS myTemp, changed_col(true, humidity) AS myHum FROM stream`, d),
		[]map[string]any{
			{"myTemp": 23, "myHum": 88},
			{"myTemp": 25},
			{"myHum": 90},
			{"myHum": 91},
		})
	// WHERE 比较
	assertRows(t, "E8", runDirect(t, `SELECT ts, temperature, humidity FROM stream WHERE changed_col(true, temperature) > 24`, d),
		[]map[string]any{{"ts": 4, "temperature": 25, "humidity": 88}})
}

func TestAnalytic_LagLatest(t *testing.T) {
	d := analyticDemo()
	// lag：首事件无前值
	got := runDirect(t, `SELECT temperature, lag(temperature) AS prev FROM stream`, d)
	if len(got) != 8 || got[0]["prev"] != nil || got[1]["prev"] != 23 || got[3]["prev"] != 23 {
		t.Errorf("lag unexpected: %v", got)
	}
	// latest：最新非空值
	gotL := runDirect(t, `SELECT latest(temperature) AS lt FROM stream`, d)
	if len(gotL) != 8 || gotL[0]["lt"] != 23 || gotL[3]["lt"] != 25 {
		t.Errorf("latest unexpected: %v", gotL)
	}
}

func TestAnalytic_LagWhenHadChanged(t *testing.T) {
	// lag 示例3：WHEN 内嵌 had_changed + 算术 ts - lag(ts,1,ts,true)
	in := []map[string]any{
		{"ts": 1, "Status": "A", "statusCode": 100},
		{"ts": 5, "Status": "A", "statusCode": 100},
		{"ts": 8, "Status": "B", "statusCode": 200},
		{"ts": 12, "Status": "B", "statusCode": 300},
	}
	// 仅断言可解析执行 + prevStatus 正确（duration 的 WHEN 门控语义见设计文档）
	got := runDirect(t, `SELECT ts, lag(Status) AS prevStatus, ts - lag(ts, 1, ts, true) OVER (WHEN had_changed(true, statusCode)) AS duration FROM stream`, in)
	wantPrev := []any{nil, "A", "A", "B"}
	for i, r := range got {
		if r["prevStatus"] != wantPrev[i] {
			t.Errorf("prevStatus[%d]=%v want %v", i, r["prevStatus"], wantPrev[i])
		}
	}
}

func TestAnalytic_Acc(t *testing.T) {
	v := []map[string]any{{"v": 1}, {"v": 2}, {"v": 3}}
	assertRows(t, "acc_sum", runDirect(t, `SELECT acc_sum(v) AS s FROM stream`, v),
		[]map[string]any{{"s": float64(1)}, {"s": float64(3)}, {"s": float64(6)}})
	assertRows(t, "acc_max", runDirect(t, `SELECT acc_max(v) AS m FROM stream`, v),
		[]map[string]any{{"m": float64(1)}, {"m": float64(2)}, {"m": float64(3)}})
	assertRows(t, "acc_min", runDirect(t, `SELECT acc_min(v) AS m FROM stream`, v),
		[]map[string]any{{"m": float64(1)}, {"m": float64(1)}, {"m": float64(1)}})
	assertRows(t, "acc_count", runDirect(t, `SELECT acc_count(v) AS c FROM stream`, v),
		[]map[string]any{{"c": int64(1)}, {"c": int64(2)}, {"c": int64(3)}})
	assertRows(t, "acc_avg", runDirect(t, `SELECT acc_avg(v) AS a FROM stream`, v),
		[]map[string]any{{"a": float64(1)}, {"a": float64(1.5)}, {"a": float64(2)}})
}

func TestAnalytic_AccConditional(t *testing.T) {
	// acc_count(a, a>1, a<0)：a>1 开始累计，a<0 归零。a:1,2,1,3,-1,1 → 0,1,2,3,0,0
	in := []map[string]any{{"a": 1}, {"a": 2}, {"a": 1}, {"a": 3}, {"a": -1}, {"a": 1}}
	assertRows(t, "条件 acc_count", runDirect(t, `SELECT acc_count(a, a > 1, a < 0) AS c FROM stream`, in),
		[]map[string]any{{"c": int64(0)}, {"c": int64(1)}, {"c": int64(2)}, {"c": int64(3)}, {"c": int64(0)}, {"c": int64(0)}})
}

// sortedFloatField 从结果行提取某字段的数值并排序（窗口 sink 异步无序，按集合比较）。
func sortedFloatField(rows []map[string]any, key string) []float64 {
	var out []float64
	for _, r := range rows {
		out = append(out, toFloatVal(r[key]))
	}
	sort.Float64s(out)
	return out
}

func toFloatVal(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}

// 分组+分析：分析函数按 GROUP BY 键分区，各分组跨窗口状态独立（不串扰）。
func TestAnalytic_Window_GroupPartition(t *testing.T) {
	in := []map[string]any{
		{"deviceId": "A", "temp": 10.0}, {"deviceId": "B", "temp": 5.0},
		{"deviceId": "A", "temp": 20.0}, {"deviceId": "B", "temp": 15.0},
		{"deviceId": "A", "temp": 30.0},
	}
	// CountingWindow(1)：每事件一窗。acc_sum(avg(temp)) 跨窗口累计，按 deviceId 分区。
	got := runWindow(t, `SELECT deviceId, acc_sum(avg(temp)) AS s FROM stream GROUP BY deviceId, CountingWindow(1)`, in)
	byDev := map[string][]float64{}
	for _, r := range got {
		d, _ := r["deviceId"].(string)
		byDev[d] = append(byDev[d], toFloatVal(r["s"]))
	}
	for _, v := range byDev {
		sort.Float64s(v)
	}
	// A: avg=10,20,30 → 累计 10,30,60；B: avg=5,15 → 累计 5,20。分区独立。
	if len(byDev["A"]) != 3 || byDev["A"][0] != 10 || byDev["A"][1] != 30 || byDev["A"][2] != 60 ||
		len(byDev["B"]) != 2 || byDev["B"][0] != 5 || byDev["B"][1] != 20 {
		t.Errorf("分组分区错误: A=%v B=%v (want A=[10,30,60] B=[5,20])", byDev["A"], byDev["B"])
	}
}

// E4：窗口内变化检测——changed_cols 包裹 avg，对窗口输出做变化检测。
func TestAnalytic_Window_ChangedColsOverAgg(t *testing.T) {
	d := analyticDemo()
	// CountingWindow(2) → 窗口均值 23,24,25,25；changed_cols 跨窗口比较：
	// 23(首),24(变),25(变),25(未变→抑制) → tavg:23,24,25
	got := runWindow(t, `SELECT changed_cols("t", true, avg(temperature)) FROM stream GROUP BY CountingWindow(2)`, d)
	vals := sortedFloatField(got, "tavg")
	want := []float64{23, 24, 25}
	if len(vals) != len(want) || (len(vals) == 3 && (vals[0] != 23 || vals[1] != 24 || vals[2] != 25)) {
		t.Errorf("E4 tavg=%v want %v", vals, want)
	}
}

// 窗口内累计——acc_sum 包裹 avg，跨窗口累计求和。
func TestAnalytic_Window_AccSumOverAgg(t *testing.T) {
	d := analyticDemo()
	// 4 窗口 avg=23,24,25,25 → 跨窗口累计求和 23,47,72,97
	got := runWindow(t, `SELECT acc_sum(avg(temperature)) AS s FROM stream GROUP BY CountingWindow(2)`, d)
	vals := sortedFloatField(got, "s")
	want := []float64{23, 47, 72, 97}
	if len(vals) != len(want) {
		t.Errorf("acc_sum(avg) got %d rows, want %d: %v", len(vals), len(want), vals)
		return
	}
	for i := range want {
		if vals[i] != want[i] {
			t.Errorf("acc_sum(avg)=%v want %v", vals, want)
			break
		}
	}
}

// acc_sum 直连 vs 窗口：同样数据，结果不同（累积频率/对象/行数均不同）。
// 直连逐事件累积原始 v；窗口里 acc_sum 必须包裹聚合，对窗口结果跨窗口累积。
func TestAnalytic_AccSum_WindowVsDirect(t *testing.T) {
	v := []map[string]any{{"v": 1}, {"v": 2}, {"v": 3}, {"v": 4}}

	direct := runDirect(t, `SELECT acc_sum(v) AS s FROM stream`, v)
	var directVals []float64
	for _, r := range direct {
		directVals = append(directVals, toFloatVal(r["s"]))
	}

	win := runWindow(t, `SELECT acc_sum(sum(v)) AS s FROM stream GROUP BY CountingWindow(2)`, v)
	winVals := sortedFloatField(win, "s")

	wantDirect := []float64{1, 3, 6, 10}
	wantWin := []float64{3, 10}
	if !reflect.DeepEqual(directVals, wantDirect) {
		t.Errorf("直连 acc_sum(v)=%v want %v", directVals, wantDirect)
	}
	if !reflect.DeepEqual(winVals, wantWin) {
		t.Errorf("窗口 acc_sum(sum(v))=%v want %v", winVals, wantWin)
	}
}

// acc_sum 累积状态按实例隔离：两实例交叉 Emit，各自独立累积、互不影响。
func TestAnalytic_AccSum_InstanceIsolation(t *testing.T) {
	a := streamsql.New()
	defer a.Stop()
	if err := a.Execute("SELECT acc_sum(v) AS s FROM stream"); err != nil {
		t.Fatalf("Execute A: %v", err)
	}
	b := streamsql.New()
	defer b.Stop()
	if err := b.Execute("SELECT acc_sum(v) AS s FROM stream"); err != nil {
		t.Fatalf("Execute B: %v", err)
	}

	emit := func(s *streamsql.Streamsql, v float64) map[string]any {
		r, err := s.EmitSync(map[string]any{"v": v})
		if err != nil {
			t.Fatalf("EmitSync v=%v: %v", v, err)
		}
		if r == nil {
			t.Fatalf("EmitSync v=%v 返回 nil", v)
		}
		return r
	}
	check := func(r map[string]any, want float64) {
		if got := toFloatVal(r["s"]); got != want {
			t.Errorf("s=%v want %v", got, want)
		}
	}

	// 交叉 Emit：若累积状态全局共享会互相污染
	check(emit(a, 1), 1)
	check(emit(b, 10), 10)
	check(emit(a, 2), 3)
	check(emit(b, 20), 30)
	check(emit(a, 3), 6)
}

// === 解析放行但曾运行期静默错值/空的回归（B1-B4 修复）===

// B1: 算术表达式包分析函数——分析结果回代入外层表达式再求值。
func TestRuntimeFix_B1_ArithmeticAroundAnalytic(t *testing.T) {
	d := []map[string]any{
		{"k": "d1", "ts": 1}, {"k": "d1", "ts": 2}, {"k": "d1", "ts": 3}, {"k": "d2", "ts": 10},
	}
	// ts - lag(ts)：d1 → [nil,1,1]；d2 首行无 lag → nil。
	got := runDirect(t, `SELECT ts - lag(ts) OVER (PARTITION BY k) AS d FROM stream`, d)
	assertNumericField(t, "B1 ts-lag", got, "d", []any{nil, 1.0, 1.0, nil})
	// 100 - lag(ts)：d1 → [nil,99,98]；d2 → nil。
	got = runDirect(t, `SELECT 100 - lag(ts) OVER (PARTITION BY k) AS d FROM stream`, d)
	assertNumericField(t, "B1 100-lag", got, "d", []any{nil, 99.0, 98.0, nil})
	// 纯分析字段不受回代影响：lag 仍 [nil,1,2,nil]。
	got = runDirect(t, `SELECT lag(ts) OVER (PARTITION BY k) AS p FROM stream`, d)
	assertNumericField(t, "B1 plain lag", got, "p", []any{nil, 1.0, 2.0, nil})
}

// B2: 裸分析函数作 WHERE 条件——值型分析函数走 nil 判定（变化到 0 也要选中）。
func TestRuntimeFix_B2_BareAnalyticInWhere(t *testing.T) {
	d := []map[string]any{{"temp": 5}, {"temp": 5}, {"temp": 0}, {"temp": 3}}
	// changed_col：变化行（含变化到 0）→ 5,0,3；未变化的第二行被过滤。
	got := runDirect(t, `SELECT temp FROM stream WHERE changed_col(true, temp)`, d)
	assertTempSeq(t, "B2 changed_col", got, []float64{5, 0, 3})
	// 显式 > 0 排除 0 → 5,3（旧行为不变）。
	got = runDirect(t, `SELECT temp FROM stream WHERE changed_col(true, temp) > 0`, d)
	assertTempSeq(t, "B2 changed_col>0", got, []float64{5, 3})
	// had_changed 裸作 WHERE：返回 bool 直判 → 5,0,3。
	got = runDirect(t, `SELECT temp FROM stream WHERE had_changed(true, temp)`, d)
	assertTempSeq(t, "B2 had_changed", got, []float64{5, 0, 3})
}

// B3: 窗口分析函数参数为"聚合+运算"复合表达式——抽内层聚合、留外层运算符。
func TestRuntimeFix_B3_CompositeArgInlineAgg(t *testing.T) {
	d := []map[string]any{{"temp": 23}, {"temp": 25}, {"temp": 25}, {"temp": 30}}
	// CountingWindow(2) → 两窗 avg=24,27.5；avg(temp)+1 → 25,28.5。
	got := runWindow(t, `SELECT changed_col(true, avg(temp) + 1) AS c FROM stream GROUP BY CountingWindow(2)`, d)
	if vals := sortedFloatField(got, "c"); !reflect.DeepEqual(vals, []float64{25, 28.5}) {
		t.Errorf("B3 avg+1: got %v, want [25, 28.5]", vals)
	}
	// 纯 avg(temp) 基线不变 → 24,27.5。
	got = runWindow(t, `SELECT changed_col(true, avg(temp)) AS c FROM stream GROUP BY CountingWindow(2)`, d)
	if vals := sortedFloatField(got, "c"); !reflect.DeepEqual(vals, []float64{24, 27.5}) {
		t.Errorf("B3 avg baseline: got %v, want [24, 27.5]", vals)
	}
}

// B4: 窗口分析函数参数用限定列（表.列）——运行期剥前缀解析到 GROUP BY 键值，不返回字面串。
func TestRuntimeFix_B4_QualifiedColumnArg(t *testing.T) {
	d := []map[string]any{{"k": "d1"}, {"k": "d1"}, {"k": "d2"}, {"k": "d2"}}
	got := runWindow(t, `SELECT changed_col(true, stream.k) AS c FROM stream GROUP BY k, CountingWindow(2)`, d)
	if len(got) == 0 {
		t.Fatalf("B4: expected non-empty output")
	}
	for _, r := range got {
		if s, _ := r["c"].(string); s == "stream.k" {
			t.Errorf("B4: qualified arg leaked literal %q in %v", "stream.k", r)
		}
		if c, _ := r["c"].(string); c != r["k"] {
			t.Errorf("B4: c=%v should equal group key k=%v in %v", r["c"], r["k"], r)
		}
	}
}

// assertNumericField 按序断言字段值：want[i]==nil 期望 nil；否则按浮点比较（容 int/float64）。
func assertNumericField(t *testing.T, label string, got []map[string]any, key string, want []any) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %d rows, want %d (%v)", label, len(got), len(want), got)
		return
	}
	for i, row := range got {
		v := row[key]
		if want[i] == nil {
			if v != nil {
				t.Errorf("%s row %d: want nil, got %v", label, i, v)
			}
			continue
		}
		if toFloatVal(v) != toFloatVal(want[i]) {
			t.Errorf("%s row %d: got %v, want %v", label, i, v, want[i])
		}
	}
}

// assertTempSeq 按序断言 temp 字段的浮点值序列（直连路径有序）。
func assertTempSeq(t *testing.T, label string, got []map[string]any, want []float64) {
	t.Helper()
	vals := make([]float64, 0, len(got))
	for _, r := range got {
		vals = append(vals, toFloatVal(r["temp"]))
	}
	if !reflect.DeepEqual(vals, want) {
		t.Errorf("%s: got %v, want %v", label, vals, want)
	}
}
