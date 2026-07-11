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
	defer ssql.Stop()
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
	time.Sleep(500 * time.Millisecond)
	return out
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
