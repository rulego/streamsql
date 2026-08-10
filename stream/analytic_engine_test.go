/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stream

import (
	"fmt"
	"testing"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAnalyticTestEngine builds an AnalyticEngine over a minimal Stream so the
// engine can be driven directly, without going through SQL.
func newAnalyticTestEngine(t *testing.T, cfg types.Config, fields []types.AnalyticField) *AnalyticEngine {
	t.Helper()
	s, err := NewStream(cfg)
	require.NoError(t, err)
	t.Cleanup(s.Stop)

	e, err := NewAnalyticEngine(s, fields)
	require.NoError(t, err)
	return e
}

func TestNewAnalyticEngine_NoFieldsReturnsNil(t *testing.T) {
	e, err := NewAnalyticEngine(nil, nil)
	require.NoError(t, err)
	assert.Nil(t, e)
	assert.False(t, e.HasFields(), "nil engine must report no fields, not panic")
}

func TestNewAnalyticEngine_UnknownFunctionIsRejected(t *testing.T) {
	_, err := NewAnalyticEngine(nil, []types.AnalyticField{{
		FuncName:   "no_such_analytic_fn",
		Expression: "no_such_analytic_fn(v)",
		Alias:      "x",
	}})
	require.Error(t, err, "unknown analytic function must fail at construction, not at first row")
	assert.Contains(t, err.Error(), "no_such_analytic_fn")
}

func TestNewAnalyticEngine_NonAnalyticFunctionIsRejected(t *testing.T) {
	// abs exists but is not a stateful analytic function.
	_, err := NewAnalyticEngine(nil, []types.AnalyticField{{
		FuncName:   "abs",
		Expression: "abs(v)",
		Alias:      "x",
	}})
	require.Error(t, err, "non-analytic function must be rejected")
	assert.Contains(t, err.Error(), "abs")
}

func TestNewAnalyticEngine_BadWhenConditionIsRejected(t *testing.T) {
	_, err := NewAnalyticEngine(nil, []types.AnalyticField{{
		FuncName:   "lag",
		Args:       []string{"v"},
		Expression: "lag(v)",
		Alias:      "prev",
		Over:       &types.OverSpec{When: "v >"}, // truncated expression
	}})
	require.Error(t, err, "an uncompilable WHEN must fail at construction")
}

func TestAnalyticEngine_LagTracksPreviousValue(t *testing.T) {
	e := newAnalyticTestEngine(t, types.Config{}, []types.AnalyticField{{
		FuncName:   "lag",
		Args:       []string{"v"},
		Expression: "lag(v)",
		Alias:      "prev",
	}})
	require.True(t, e.HasFields())

	// First row has no predecessor; subsequent rows see the prior value.
	got := []any{}
	for _, v := range []float64{10, 20, 30} {
		out := e.Evaluate(map[string]any{"v": v})
		got = append(got, out["prev"])
	}

	assert.Nil(t, got[0], "first row has no previous value")
	assert.EqualValues(t, 10, got[1])
	assert.EqualValues(t, 20, got[2])
}

func TestAnalyticEngine_PartitionsKeepStateSeparate(t *testing.T) {
	e := newAnalyticTestEngine(t, types.Config{}, []types.AnalyticField{{
		FuncName:   "lag",
		Args:       []string{"v"},
		Expression: "lag(v)",
		Alias:      "prev",
		Over:       &types.OverSpec{PartitionBy: []string{"deviceId"}},
	}})

	// Interleave two devices: each must see only its own history.
	e.Evaluate(map[string]any{"deviceId": "a", "v": 1.0})
	e.Evaluate(map[string]any{"deviceId": "b", "v": 100.0})
	outA := e.Evaluate(map[string]any{"deviceId": "a", "v": 2.0})
	outB := e.Evaluate(map[string]any{"deviceId": "b", "v": 200.0})

	assert.EqualValues(t, 1, outA["prev"], "partition a must not see b's rows")
	assert.EqualValues(t, 100, outB["prev"], "partition b must not see a's rows")
}

func TestAnalyticEngine_PartitionKeyDoesNotCollideAcrossTypes(t *testing.T) {
	e := newAnalyticTestEngine(t, types.Config{}, []types.AnalyticField{{
		FuncName:   "lag",
		Args:       []string{"v"},
		Expression: "lag(v)",
		Alias:      "prev",
		Over:       &types.OverSpec{PartitionBy: []string{"k"}},
	}})

	// Partition key 1 (int) and "1" (string) must be distinct partitions.
	e.Evaluate(map[string]any{"k": 1, "v": 10.0})
	out := e.Evaluate(map[string]any{"k": "1", "v": 20.0})

	assert.Nil(t, out["prev"],
		`partition 1 (int) and "1" (string) must not share state`)
}

func TestAnalyticEngine_WhenGatesStateUpdate(t *testing.T) {
	e := newAnalyticTestEngine(t, types.Config{}, []types.AnalyticField{{
		FuncName:   "acc_sum",
		Args:       []string{"v"},
		Expression: "acc_sum(v)",
		Alias:      "total",
		Over:       &types.OverSpec{When: "ok == true"},
	}})

	e.Evaluate(map[string]any{"v": 5.0, "ok": true})
	gated := e.Evaluate(map[string]any{"v": 100.0, "ok": false})
	resumed := e.Evaluate(map[string]any{"v": 5.0, "ok": true})

	assert.EqualValues(t, 5, gated["total"],
		"WHEN false must reuse the previous result, not accumulate")
	assert.EqualValues(t, 10, resumed["total"],
		"WHEN true must resume accumulating from the retained state")
}

func TestAnalyticEngine_EvictsLeastRecentlyUsedPartition(t *testing.T) {
	const cap = 2
	e := newAnalyticTestEngine(t, types.Config{AnalyticMaxPartitions: cap},
		[]types.AnalyticField{{
			FuncName:   "acc_sum",
			Args:       []string{"v"},
			Expression: "acc_sum(v)",
			Alias:      "total",
			Over:       &types.OverSpec{PartitionBy: []string{"k"}},
		}})

	// Seed 2 partitions (at cap), then touch "a" so "b" becomes the LRU victim.
	e.Evaluate(map[string]any{"k": "a", "v": 1.0})
	e.Evaluate(map[string]any{"k": "b", "v": 1.0})
	e.Evaluate(map[string]any{"k": "a", "v": 1.0}) // a: 2, a is now MRU
	e.Evaluate(map[string]any{"k": "c", "v": 1.0}) // over cap: evicts b

	// a survived and kept its accumulated state.
	outA := e.Evaluate(map[string]any{"k": "a", "v": 1.0})
	assert.EqualValues(t, 3, outA["total"], "recently used partition must survive eviction")

	// b was evicted, so its accumulator restarts from zero.
	outB := e.Evaluate(map[string]any{"k": "b", "v": 1.0})
	assert.EqualValues(t, 1, outB["total"],
		"evicted partition must restart from 0 (state discarded)")
}

func TestAnalyticEngine_PartitionCountStaysAtCap(t *testing.T) {
	const cap = 8
	s, err := NewStream(types.Config{AnalyticMaxPartitions: cap})
	require.NoError(t, err)
	defer s.Stop()

	e, err := NewAnalyticEngine(s, []types.AnalyticField{{
		FuncName:   "acc_sum",
		Args:       []string{"v"},
		Expression: "acc_sum(v)",
		Alias:      "total",
		Over:       &types.OverSpec{PartitionBy: []string{"k"}},
	}})
	require.NoError(t, err)

	for i := 0; i < 500; i++ {
		e.Evaluate(map[string]any{"k": fmt.Sprintf("k%03d", i), "v": 1.0})
	}

	fe := e.fields[0]
	fe.mu.Lock()
	partitions, lruLen, lastResults := len(fe.partitions), fe.lru.Len(), len(fe.lastResults)
	fe.mu.Unlock()

	assert.Equal(t, cap, partitions, "partition map must stay at the cap")
	assert.Equal(t, cap, lruLen, "LRU list must stay in step with the partition map")
	assert.LessOrEqual(t, lastResults, cap,
		"cached last-results must be evicted with their partition, else they leak")
}

func TestAnalyticEngine_WrapperExpressionSubstitutesResult(t *testing.T) {
	// "v - lag(v)" → wrapper template with the analytic call replaced by a placeholder.
	e := newAnalyticTestEngine(t, types.Config{}, []types.AnalyticField{{
		FuncName:    "lag",
		Args:        []string{"v"},
		Expression:  "lag(v)",
		Alias:       "delta",
		WrapperExpr: "v - " + types.AnalyticSelfToken,
		Calls: []types.AnalyticCall{
			{FuncName: "lag", BareCall: "lag(v)", Args: []string{"v"}},
		},
	}})

	e.Evaluate(map[string]any{"v": 10.0})
	out := e.Evaluate(map[string]any{"v": 25.0})

	assert.EqualValues(t, 15, out["delta"],
		"wrapper expression must be evaluated with the analytic result substituted in")
}

func TestAnalyticEngine_MultipleCallsInOneExpression(t *testing.T) {
	// acc_max(v) - acc_min(v): two independent state machines, one shared partition.
	e := newAnalyticTestEngine(t, types.Config{}, []types.AnalyticField{{
		FuncName:    "acc_max",
		Args:        []string{"v"},
		Expression:  "acc_max(v)",
		Alias:       "spread",
		WrapperExpr: types.AnalyticSelfTokenN(0) + " - " + types.AnalyticSelfTokenN(1),
		Calls: []types.AnalyticCall{
			{FuncName: "acc_max", BareCall: "acc_max(v)", Args: []string{"v"}},
			{FuncName: "acc_min", BareCall: "acc_min(v)", Args: []string{"v"}},
		},
	}})

	for _, v := range []float64{5, 20, 2} {
		e.Evaluate(map[string]any{"v": v})
	}
	out := e.Evaluate(map[string]any{"v": 8.0})

	assert.EqualValues(t, 18, out["spread"],
		"each call must keep its own state (max 20 - min 2)")
}

func TestAnalyticEngine_EvaluatePanicIsContained(t *testing.T) {
	// A wrapper referencing an undefined placeholder makes evaluation fail; the
	// engine must degrade to nil rather than take down the pipeline.
	e := newAnalyticTestEngine(t, types.Config{}, []types.AnalyticField{{
		FuncName:    "lag",
		Args:        []string{"v"},
		Expression:  "lag(v)",
		Alias:       "bad",
		WrapperExpr: "!!!(((",
		Calls: []types.AnalyticCall{
			{FuncName: "lag", BareCall: "lag(v)", Args: []string{"v"}},
		},
	}})

	assert.NotPanics(t, func() {
		e.Evaluate(map[string]any{"v": 1.0})
	}, "a broken analytic field must not panic the pipeline")
}

func TestAnalyticEngine_MissingPartitionFieldUsesOneBucket(t *testing.T) {
	e := newAnalyticTestEngine(t, types.Config{}, []types.AnalyticField{{
		FuncName:   "acc_sum",
		Args:       []string{"v"},
		Expression: "acc_sum(v)",
		Alias:      "total",
		Over:       &types.OverSpec{PartitionBy: []string{"absent"}},
	}})

	e.Evaluate(map[string]any{"v": 1.0})
	out := e.Evaluate(map[string]any{"v": 1.0})

	assert.EqualValues(t, 2, out["total"],
		"rows missing the partition field must share one bucket, not create one each")

	fe := e.fields[0]
	fe.mu.Lock()
	n := len(fe.partitions)
	fe.mu.Unlock()
	assert.Equal(t, 1, n)
}
