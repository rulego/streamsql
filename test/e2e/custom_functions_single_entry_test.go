package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This document verifies: Scalar, Aggregation, and Analysis are three types of custom functions, each only requiring functions.Register a single entry point,
// This allows end-to-end implementation in the SQL—no need for a aggregator.Register or RegisterAggregatorAdapter to register again.
// Global registry is shared across tests, function names are uniformly prefixed with zz_ and defer unregister for cleanup to avoid contamination.

// ===== Scalar: RegisterCustomFunction Single Entry =====

func TestSingleEntry_ScalarFunction(t *testing.T) {
	require.NoError(t, functions.RegisterCustomFunction(
		"zz_my_double", functions.TypeMath, "test", "value*2", 1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			return cast.ToFloat64(args[0]) * 2, nil
		},
	))
	defer functions.Unregister("zz_my_double")

	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute("SELECT zz_my_double(value) AS d FROM stream"))

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	ssql.Emit(map[string]any{"value": 21.0})

	select {
	case r := <-ch:
		require.Len(t, r, 1)
		assert.Equal(t, 42.0, r[0]["d"])
	case <-time.After(2 * time.Second):
		t.Fatal("Scalar function result timeout")
	}
}

// ===== Aggregation: Implementing functions.AggregatorFunction + only functions.Register =====

// zzMySum fully implements functions.AggregatorFunction, only through functions.Register,
// Not tuned aggregator.Register. Proof aggregation requires only one entry point (the adapter is automatically enabled by the registry).
type zzMySum struct {
	*functions.BaseFunction
	sum float64
	ok  bool
}

func newZzMySum() *zzMySum {
	return &zzMySum{BaseFunction: functions.NewBaseFunction("zz_my_sum", functions.TypeAggregation, "test", "custom sum", 1, -1)}
}

func (f *zzMySum) Validate(args []any) error { return f.ValidateArgCount(args) }
func (f *zzMySum) Execute(ctx *functions.FunctionContext, args []any) (any, error) {
	s := 0.0
	for _, a := range args {
		if v, err := cast.ToFloat64E(a); err == nil {
			s += v
		}
	}
	return s, nil
}
func (f *zzMySum) New() functions.AggregatorFunction { return &zzMySum{BaseFunction: f.BaseFunction} }
func (f *zzMySum) Add(v any) {
	if v == nil {
		return
	}
	if x, err := cast.ToFloat64E(v); err == nil {
		f.sum += x
		f.ok = true
	}
}
func (f *zzMySum) Result() any {
	if !f.ok {
		return nil
	}
	return f.sum
}
func (f *zzMySum) Reset() { f.sum = 0; f.ok = false }
func (f *zzMySum) Clone() functions.AggregatorFunction {
	return &zzMySum{BaseFunction: f.BaseFunction, sum: f.sum, ok: f.ok}
}

func TestSingleEntry_AggregateFunction(t *testing.T) {
	require.NoError(t, functions.Register(newZzMySum()))
	defer functions.Unregister("zz_my_sum")

	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute(
		"SELECT device, zz_my_sum(value) AS s FROM stream GROUP BY device, TumblingWindow('1s')"))
	require.True(t, ssql.IsAggregationQuery(), "TypeAggregation 函数应被识别为聚合查询")

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	ssql.Emit(map[string]any{"device": "d1", "value": 1.0})
	ssql.Emit(map[string]any{"device": "d1", "value": 2.0})
	ssql.Emit(map[string]any{"device": "d1", "value": 3.0})
	time.Sleep(1100 * time.Millisecond)
	ssql.TriggerWindow()

	select {
	case r := <-ch:
		require.Len(t, r, 1)
		assert.Equal(t, "d1", r[0]["device"])
		assert.Equal(t, 6.0, r[0]["s"])
	case <-time.After(2 * time.Second):
		t.Fatal("The aggregate function result times out")
	}
}

// ===== Analysis: Implementing functions.StatefulAnalytic + only functions.Register =====

// zzMyPrev is a custom analysis function for lag semantics, and only through functions.Register.
type zzMyPrev struct {
	*functions.BaseFunction
}

func newZzMyPrev() *zzMyPrev {
	return &zzMyPrev{BaseFunction: functions.NewBaseFunction("zz_my_prev", functions.TypeAnalytical, "test", "previous value", 1, 1)}
}

func (f *zzMyPrev) Validate(args []any) error { return f.ValidateArgCount(args) }
func (f *zzMyPrev) Execute(ctx *functions.FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used with OVER", f.GetName())
}
func (f *zzMyPrev) NewState() functions.AnalyticState { return &zzPrevState{} }

type zzPrevState struct {
	prev any
}

func (s *zzPrevState) Apply(args []any) any {
	cur := args[0]
	result := s.prev
	s.prev = cur
	return result
}
func (s *zzPrevState) Reset() { s.prev = nil }

func TestSingleEntry_AnalyticFunction(t *testing.T) {
	require.NoError(t, functions.Register(newZzMyPrev()))
	defer functions.Unregister("zz_my_prev")

	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute(
		"SELECT value, zz_my_prev(value) OVER (PARTITION BY device) AS prev FROM stream"))
	require.False(t, ssql.IsAggregationQuery(), "纯分析函数查询应为非聚合（走 streamTransform/EmitSync）")

	r1, err := ssql.EmitSync(map[string]any{"device": "d1", "value": 10})
	require.NoError(t, err)
	require.NotNil(t, r1)
	assert.Nil(t, r1["prev"], "首行无前值")

	r2, err := ssql.EmitSync(map[string]any{"device": "d1", "value": 20})
	require.NoError(t, err)
	require.NotNil(t, r2)
	assert.Equal(t, 10, r2["prev"], "第二行应返回上一行的值")
}
