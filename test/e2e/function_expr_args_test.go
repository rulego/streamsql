package e2e

import (
	"testing"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFunctionExpressionArguments guards the fix for the silent-nil bug where a
// multi-argument function whose argument is an arithmetic/logical expression
// (e.g. round(v/3, 2)) returned nil. parseFunctionArgs now evaluates such
// expression arguments against the row instead of treating them as raw strings.
// Regression for the "unit tests construct the function directly and bypass the
// real SQL argument-resolution path" half-feature pattern.
func TestFunctionExpressionArguments(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		sql  string
		emit float64
		want any
	}{
		{"round_div", "SELECT round(v/3, 2) AS r FROM stream", 25.0, 8.33},      // 25/3=8.333 -> 8.33
		{"round_mul", "SELECT round(v*2, 2) AS r FROM stream", 25.0, 50.0},      // 50.00
		{"round_add", "SELECT round(v+1, 2) AS r FROM stream", 25.0, 26.0},      // 26.00
		{"round_sub", "SELECT round(v-1, 2) AS r FROM stream", 25.0, 24.0},      // 24.00
		{"round_field_still_works", "SELECT round(v, 2) AS r FROM stream", 25.0, 25.0},
		{"round_paren_still_works", "SELECT round((v+1), 2) AS r FROM stream", 25.0, 26.0},
		{"abs_expr", "SELECT abs(v-30) AS a FROM stream", 25.0, 5.0},            // |25-30|=5
		{"power_expr_arg", "SELECT power(v, 2) AS p FROM stream", 3.0, 9.0},     // sanity: known fn, numeric literal arg
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ssql := streamsql.New()
			defer ssql.Stop()
			require.NoError(t, ssql.Execute(c.sql))
			res, err := ssql.EmitSync(map[string]any{"v": c.emit})
			require.NoError(t, err)
			require.NotNil(t, res, "result must not be nil (was the silent-nil bug)")
			// The output key is the alias (last SELECT alias). Pick the single value.
			var got any
			for _, v := range res {
				got = v
			}
			assert.Equal(t, c.want, got, "expression-argument function returned wrong value")
		})
	}
}
