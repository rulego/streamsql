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
	"testing"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- literalValue / analyticColName / hasStarArg / expandStarArgs ---

func TestLiteralValue_ScalarForms(t *testing.T) {
	cases := []struct {
		in   string
		want any
	}{
		{"true", true},
		{"false", false},
		{"42", 42},
		{" 42 ", 42},
		{"3.5", 3.5},
		{`"quoted"`, "quoted"},
		{"'single'", "single"},
		{"bare", "bare"}, // unquoted non-numeric falls through unchanged
	}
	for _, c := range cases {
		assert.Equal(t, c.want, literalValue(c.in), "literalValue(%q)", c.in)
	}
}

func TestAnalyticColName_StripsQuotesAndQualifier(t *testing.T) {
	cases := map[string]string{
		"temp":     "temp",
		"  temp  ": "temp",
		"`temp`":   "temp",
		`"temp"`:   "temp",
		"'temp'":   "temp",
		"s.temp":   "temp", // qualified: last segment wins
		"a.b.temp": "temp",
		// Per-segment backticks are not stripped: Trim only removes the outer
		// pair, then the qualifier split leaves the inner one. Pinned as-is
		// because it is the current behavior, not an endorsement of it.
		"`s`.`temp`": "`temp",
	}
	for in, want := range cases {
		assert.Equal(t, want, analyticColName(in), "analyticColName(%q)", in)
	}
}

func TestHasStarArg(t *testing.T) {
	assert.True(t, hasStarArg([]string{"p", "true", "*"}))
	assert.True(t, hasStarArg([]string{" * "}), "surrounding space must not hide the star")
	assert.False(t, hasStarArg([]string{"p", "true", "temp"}))
	assert.False(t, hasStarArg(nil))
}

func TestExpandStarArgs_AppendsRowValuesInKeyOrder(t *testing.T) {
	args := []string{`"p_"`, "true", "*"}
	row := map[string]any{"b": 2, "a": 1, "c": 3}

	// parsed holds the already-evaluated leading args; "*" is skipped and the
	// whole row is appended sorted by key so the order is deterministic.
	out := expandStarArgs(args, row, []any{"p_", true})

	require.Len(t, out, 5, "2 leading args + 3 row values")
	assert.Equal(t, "p_", out[0])
	assert.Equal(t, true, out[1])
	assert.EqualValues(t, 1, out[2], "row values must follow sorted keys: a,b,c")
	assert.EqualValues(t, 2, out[3])
	assert.EqualValues(t, 3, out[4])
}

func TestExpandStarArgs_FallsBackToLiteralsWhenUnparsed(t *testing.T) {
	// parsed is empty (the "*" made evaluation fail), so leading args must be
	// recovered from their literal text.
	out := expandStarArgs([]string{`"p_"`, "true", "*"}, map[string]any{"a": 1}, nil)

	require.Len(t, out, 3)
	assert.Equal(t, "p_", out[0], "quoted literal must be recovered")
	assert.Equal(t, true, out[1], "boolean literal must be recovered")
}

// --- evaluateMultiColumn (changed_cols) ---

func newMultiColEngine(t *testing.T, args []string, over *types.OverSpec, inlineDisplay map[string]string) *AnalyticEngine {
	t.Helper()
	s, err := NewStream(types.Config{})
	require.NoError(t, err)
	t.Cleanup(s.Stop)

	expr := "changed_cols("
	for i, a := range args {
		if i > 0 {
			expr += ","
		}
		expr += a
	}
	expr += ")"

	e, err := NewAnalyticEngine(s, []types.AnalyticField{{
		FuncName:         "changed_cols",
		Args:             args,
		Expression:       expr,
		Alias:            "__mc__",
		MultiColumn:      true,
		Over:             over,
		InlineAggDisplay: inlineDisplay,
	}})
	require.NoError(t, err)
	return e
}

func TestEvaluateMultiColumn_ReportsChangedColumnsWithPrefix(t *testing.T) {
	e := newMultiColEngine(t, []string{`"c_"`, "true", "temp"}, nil, nil)

	first := e.Evaluate(map[string]any{"temp": 20.0})
	cols, ok := first["__mc__"].(map[string]any)
	require.True(t, ok, "a multi-column field must evaluate to a column map")
	assert.Contains(t, cols, "c_temp", "changed columns must carry the prefix")

	// Same value again: nothing changed.
	same := e.Evaluate(map[string]any{"temp": 20.0})
	sameCols, _ := same["__mc__"].(map[string]any)
	assert.Empty(t, sameCols, "an unchanged column must not be reported")

	// New value: reported again.
	changed := e.Evaluate(map[string]any{"temp": 30.0})
	changedCols, _ := changed["__mc__"].(map[string]any)
	assert.Contains(t, changedCols, "c_temp")
	assert.EqualValues(t, 30, changedCols["c_temp"])
}

func TestEvaluateMultiColumn_StarExpandsWholeRow(t *testing.T) {
	e := newMultiColEngine(t, []string{`"c_"`, "true", "*"}, nil, nil)

	out := e.Evaluate(map[string]any{"temp": 20.0, "humidity": 50.0})
	cols, ok := out["__mc__"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, cols, "c_temp", `"*" must expand to every row column`)
	assert.Contains(t, cols, "c_humidity")
}

func TestEvaluateMultiColumn_WhenGatesAndReusesLastResult(t *testing.T) {
	e := newMultiColEngine(t, []string{`"c_"`, "true", "temp"},
		&types.OverSpec{When: "ok == true"}, nil)

	first := e.Evaluate(map[string]any{"temp": 20.0, "ok": true})
	firstCols, _ := first["__mc__"].(map[string]any)
	require.Contains(t, firstCols, "c_temp")

	// WHEN false: the previous result is reused and the state is not updated.
	gated := e.Evaluate(map[string]any{"temp": 999.0, "ok": false})
	gatedCols, _ := gated["__mc__"].(map[string]any)
	assert.Equal(t, firstCols, gatedCols, "WHEN false must reuse the last result")
}

func TestEvaluateMultiColumn_WhenFalseBeforeAnyResultIsEmpty(t *testing.T) {
	e := newMultiColEngine(t, []string{`"c_"`, "true", "temp"},
		&types.OverSpec{When: "ok == true"}, nil)

	out := e.Evaluate(map[string]any{"temp": 20.0, "ok": false})
	cols, ok := out["__mc__"].(map[string]any)
	require.True(t, ok)
	assert.Empty(t, cols, "WHEN false with no prior result must yield an empty column map")
}

func TestEvaluateMultiColumn_InlineAggDisplayRenamesOutput(t *testing.T) {
	// A window query rewrites an inlined aggregate to a hidden key; the output
	// column must use the display name instead.
	e := newMultiColEngine(t, []string{`"t"`, "true", "__winagg_0__"}, nil,
		map[string]string{"__winagg_0__": "avg"})

	out := e.Evaluate(map[string]any{"__winagg_0__": 20.0})
	cols, ok := out["__mc__"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, cols, "tavg", "hidden key must be renamed to prefix+display name")
	assert.NotContains(t, cols, "t__winagg_0__")
}

func TestEvaluateMultiColumn_PartitionsKeepStateSeparate(t *testing.T) {
	e := newMultiColEngine(t, []string{`"c_"`, "true", "temp"},
		&types.OverSpec{PartitionBy: []string{"deviceId"}}, nil)

	e.Evaluate(map[string]any{"deviceId": "a", "temp": 20.0})
	// Device b's first row: changed relative to b's own (empty) history.
	outB := e.Evaluate(map[string]any{"deviceId": "b", "temp": 20.0})
	colsB, _ := outB["__mc__"].(map[string]any)
	assert.Contains(t, colsB, "c_temp",
		"a new partition must not inherit another partition's history")

	// Device a repeating its own value: unchanged.
	outA := e.Evaluate(map[string]any{"deviceId": "a", "temp": 20.0})
	colsA, _ := outA["__mc__"].(map[string]any)
	assert.Empty(t, colsA, "partition a must see its own history")
}
