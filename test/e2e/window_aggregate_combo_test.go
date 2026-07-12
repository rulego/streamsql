package e2e

import (
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 窗口 × 聚合组合的语义测试。预期值从 SQL 标准语义推导（不从代码反推）。
// 统一用事件时间 + 远未来事件推水位触发窗口（确定性，不依赖真实时钟）。

// asFloat64 把 count/sum/avg 等可能为 int/int64/float64 的聚合结果统一成 float64 比对，
// 避免 count(*) 的具体整数类型耦合。
func asFloat64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	}
	return 0
}

// HAVING 引用「未在 SELECT 出现」的聚合：SELECT 只有 count(*)，HAVING 用 max(v)。
// 窗口 [base,base+1s) 值 10,60 → max=60>50 通过，count=2。
func TestWindowCombo_Having_NonSelectedAggregate(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute(
		`SELECT count(*) AS c FROM stream GROUP BY TumblingWindow('1s') WITH (TIMESTAMP='ts', TIMEUNIT='ms') HAVING max(v) > 50`))
	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	base := time.Now().UnixMilli() - 5000
	ssql.Emit(map[string]any{"ts": base, "v": 10.0})
	ssql.Emit(map[string]any{"ts": base, "v": 60.0})
	ssql.Emit(map[string]any{"ts": base + 2000, "v": 5.0}) // 推水位触发 [base,base+1s)

	select {
	case rows := <-ch:
		require.Len(t, rows, 1)
		assert.Equal(t, 2.0, asFloat64(rows[0]["c"]), "count=2，max=60>50 应通过 HAVING")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: HAVING 引用未选中的 max(v) 应能正常求值触发")
	}
}

// 两个聚合做算术（后聚合回代）：SELECT max(v)-min(v) AS rng, sum(v)。
// 值 10,40,25 → max-min=30，sum=75。
func TestWindowCombo_PostAggArithmetic_TwoAggregates(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute(
		`SELECT max(v)-min(v) AS rng, sum(v) AS total FROM stream GROUP BY TumblingWindow('1s') WITH (TIMESTAMP='ts', TIMEUNIT='ms')`))
	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	base := time.Now().UnixMilli() - 5000
	ssql.Emit(map[string]any{"ts": base, "v": 10.0})
	ssql.Emit(map[string]any{"ts": base, "v": 40.0})
	ssql.Emit(map[string]any{"ts": base, "v": 25.0})
	ssql.Emit(map[string]any{"ts": base + 2000, "v": 1.0})

	select {
	case rows := <-ch:
		require.Len(t, rows, 1)
		assert.Equal(t, 30.0, asFloat64(rows[0]["rng"]), "max-min = 40-10")
		assert.Equal(t, 75.0, asFloat64(rows[0]["total"]), "sum = 10+40+25")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: 两聚合 max-min 算术应回代求值")
	}
}

// 同一窗口多种聚合类型：count/sum/avg/min/max 各自正确。
// 值 10,20,30 → c=3 s=60 a=20 mn=10 mx=30。
func TestWindowCombo_MultipleAggregates(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute(
		`SELECT count(*) AS c, sum(v) AS s, avg(v) AS a, min(v) AS mn, max(v) AS mx FROM stream GROUP BY TumblingWindow('1s') WITH (TIMESTAMP='ts', TIMEUNIT='ms')`))
	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	base := time.Now().UnixMilli() - 5000
	for _, v := range []float64{10.0, 20.0, 30.0} {
		ssql.Emit(map[string]any{"ts": base, "v": v})
	}
	ssql.Emit(map[string]any{"ts": base + 2000, "v": 1.0})

	select {
	case rows := <-ch:
		require.Len(t, rows, 1)
		assert.Equal(t, 3.0, asFloat64(rows[0]["c"]))
		assert.Equal(t, 60.0, asFloat64(rows[0]["s"]))
		assert.Equal(t, 20.0, asFloat64(rows[0]["a"]))
		assert.Equal(t, 10.0, asFloat64(rows[0]["mn"]))
		assert.Equal(t, 30.0, asFloat64(rows[0]["mx"]))
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}

// NULL 在聚合里的处理（SQL 标准）：count(*) 计行，count(v)/avg/sum 忽略 NULL。
// 值 10,nil,30 → c=3 cv=2 a=20 s=40。
func TestWindowCombo_NullInAggregates(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute(
		`SELECT count(*) AS c, count(v) AS cv, avg(v) AS a, sum(v) AS s FROM stream GROUP BY TumblingWindow('1s') WITH (TIMESTAMP='ts', TIMEUNIT='ms')`))
	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	base := time.Now().UnixMilli() - 5000
	ssql.Emit(map[string]any{"ts": base, "v": 10.0})
	ssql.Emit(map[string]any{"ts": base, "v": nil})
	ssql.Emit(map[string]any{"ts": base, "v": 30.0})
	ssql.Emit(map[string]any{"ts": base + 2000, "v": 1.0})

	select {
	case rows := <-ch:
		require.Len(t, rows, 1)
		assert.Equal(t, 3.0, asFloat64(rows[0]["c"]), "count(*) 计所有行")
		assert.Equal(t, 2.0, asFloat64(rows[0]["cv"]), "count(v) 忽略 NULL")
		assert.Equal(t, 20.0, asFloat64(rows[0]["a"]), "avg 忽略 NULL: (10+30)/2")
		assert.Equal(t, 40.0, asFloat64(rows[0]["s"]), "sum 忽略 NULL: 10+30")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}
