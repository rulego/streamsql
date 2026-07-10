package e2e

import (
	"testing"
	"time"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPerRowWindowFunctionsRejectedAtExecute guards the P0/P1 fix: row_number()
// and lead() are per-row window functions the per-group aggregation model
// cannot evaluate. They must fail at Execute with a clear error — NOT crash on
// the data path (row_number used to panic) and NOT silently return nil (lead
// used to return <nil>). Regression for the "registered-but-unwired window
// function" half-feature that CI missed due to zero e2e coverage.
func TestPerRowWindowFunctionsRejectedAtExecute(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		sql  string
		fn   string
	}{
		{"row_number", "SELECT row_number() AS rn FROM stream GROUP BY TumblingWindow('1s')", "row_number"},
		{"lead", "SELECT lead(temperature) AS ld FROM stream GROUP BY TumblingWindow('1s')", "lead"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ssql := streamsql.New()
			defer ssql.Stop()
			err := ssql.Execute(c.sql)
			require.Error(t, err, "%s() must be rejected at Execute", c.fn)
			assert.Contains(t, err.Error(), c.fn, "error should name the unsupported function")
			assert.Contains(t, err.Error(), "not supported", "error should explain it is not supported")
		})
	}
}

// TestNthValueWindowFunctionWorks verifies nth_value (a per-group window
// function that DOES fit the aggregation model) evaluates correctly end-to-end,
// returning the Nth value added within the window.
func TestNthValueWindowFunctionWorks(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT nth_value(temperature, 1) AS first_temp
		FROM stream
		GROUP BY TumblingWindow('1s')
		WITH (TIMESTAMP='ts', TIMEUNIT='ms')`
	require.NoError(t, ssql.Execute(sql))

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	base := time.Now().UnixMilli() - 5000
	// Two rows in the first [base, base+1s) window, then a far-future row to
	// drive the watermark past it and fire the window.
	ssql.Emit(map[string]any{"ts": base, "temperature": 10.0})
	ssql.Emit(map[string]any{"ts": base + 100, "temperature": 20.0})
	ssql.Emit(map[string]any{"ts": base + 2000, "temperature": 99.0})

	select {
	case rows := <-ch:
		require.NotEmpty(t, rows, "window should fire")
		// nth_value(temperature, 1) returns the first value added in the window.
		// Add-order within a group is not guaranteed under concurrent processing,
		// so accept either emitted value — the point is it returns a real window
		// value (not nil / not a crash), proving the function is wired.
		first := rows[0]["first_temp"]
		assert.Contains(t, []any{float64(10), float64(20)}, first,
			"nth_value(temperature,1) should return a window value, got %v", first)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for nth_value window to fire")
	}
}
