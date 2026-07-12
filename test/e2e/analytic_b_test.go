package e2e

import (
	"testing"

	streamsql "github.com/rulego/streamsql"
)

// BenchmarkMultiCallAnalytic 衡量同一表达式含 1/2/3 个分析调用的每行开销。
// 走直连路径（EmitSync 同步求值），隔离多调用回代（applyCall×N + wrapper 一次）的成本。
func BenchmarkMultiCallAnalytic(b *testing.B) {
	cases := []struct{ name, sql string }{
		{"1call", `SELECT acc_sum(v) AS r FROM stream`},
		{"2call", `SELECT acc_max(v) - acc_min(v) AS r FROM stream`},
		{"3call", `SELECT acc_max(v) + acc_min(v) + acc_sum(v) AS r FROM stream`},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			ssql := streamsql.New()
			if err := ssql.Execute(c.sql); err != nil {
				b.Fatal(err)
			}
			defer ssql.Stop()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := ssql.EmitSync(map[string]any{"v": i}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGroupByKeyExpr 衡量 GROUP BY 裸列 vs 表达式分组键的每行开销（窗口路径）。
// expr 子项触发 injectGroupKeyExprs 逐行对 hour(ts) 求 expr bridge；bare 不触发。
// 两者差值即表达式分组键注入的开销。CountingWindow(1000) 压低触发频率，凸显逐行成本。
func BenchmarkGroupByKeyExpr(b *testing.B) {
	cases := []struct {
		name, sql string
		row       map[string]any
	}{
		{
			name: "bare",
			sql:  `SELECT device AS d, count(*) AS c FROM stream GROUP BY device, CountingWindow(1000)`,
			row:  map[string]any{"device": "aa"},
		},
		{
			name: "expr",
			sql:  `SELECT hour(ts) AS h, count(*) AS c FROM stream GROUP BY hour(ts), CountingWindow(1000)`,
			row:  map[string]any{"ts": "2026-07-12 10:00:00"},
		},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			ssql := streamsql.New()
			if err := ssql.Execute(c.sql); err != nil {
				b.Fatal(err)
			}
			ssql.AddSink(func(r []map[string]any) {}) // 丢弃窗口输出
			defer ssql.Stop()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ssql.Emit(c.row)
			}
		})
	}
}
