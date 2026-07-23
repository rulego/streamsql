package e2e

import (
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Window × Semantic testing of aggregated combinations. Expected values are derived from SQL standard semantics (not backwards from code).
// Unified trigger window using event time + distant future event push-up threshold (deterministic, not dependent on the actual clock).

// asFloat64 unifies the aggregate results of count/sum/avg that may be int/int/int64/float64 into float64 comparison,
// Avoid coupling count(*) into specific integer types.
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

// HAVING references aggregations that do not appear in SELECT: SELECT only count(*), HAVING uses max(v).
// Window [base,base+1s) values 10, 60 → max=60>50 pass, count=2.
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
	ssql.Emit(map[string]any{"ts": base + 2000, "v": 5.0}) // Push the water level to trigger [base,base+1s)

	select {
	case rows := <-ch:
		require.Len(t, rows, 1)
		assert.Equal(t, 2.0, asFloat64(rows[0]["c"]), "count=2，max=60>50 应通过 HAVING")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: HAVING The reference to an unselected max(v) should be able to evaluate and trigger normally")
	}
}

// Arithmetic for two aggregates (post-aggregation backsubstitution): SELECT max(v) - min(v) AS rng, sum(v).
// Values 10, 40, 25 → max-min=30, sum=75.
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
		t.Fatal("timeout: Two aggregates max-min arithmetic should be performed by substitution")
	}
}

// Multiple aggregation types in the same window: count/sum/avg/min/max are each correct.
// Values 10, 20, 30 → c = 3, s = 60, a = 20, mn = 10, mx = 30.
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

// NULL processing in aggregates (SQL standard): count(*) counts rows, count(v)/avg/sum ignores NULL.
// Values 10, nil, 30 → c=3, cv=2, a=20, s=40.
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
