package e2e

import (
	"testing"

	streamsql "github.com/rulego/streamsql"
)

// BenchmarkMultiCallAnalytic measures the overhead per line for the same expression with 1/2/3 analysis calls.
// Taking a direct connection path (EmitSync synchronous evaluation) isolates the cost of multiple calls (applyCall×N + wrapper in one go).
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

// BenchmarkGroupByKeyExpr measures the overhead per row (window path) for GROUP BY bare columns vs. expression group keys.
// The expr subterm triggers injectGroupKeyExprs to query the expr bridge line by line for hour(ts); Bare does not trigger.
// The difference between the two is the overhead of injecting the expression block key. CountingWindow(1000) lowers trigger frequency, highlighting the cost of each line.
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
			ssql.AddSink(func(r []map[string]any) {}) // Discard window output
			defer ssql.Stop()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ssql.Emit(c.row)
			}
		})
	}
}
