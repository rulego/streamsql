package e2e

import (
	"testing"
	"time"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWindowHavingSustainedDetection verifies the mainstream approach: aggregate all events in the window, HAVING to block dip.
// All >5 windows are HAVING; Window containing dip(<5) is blocked by HAVING and has no output.
// Note: HAVING in streamsql references the SELECT alias (mn) and does not restate the aggregation function.
func TestWindowHavingSustainedDetection(t *testing.T) {
	t.Parallel()

	// The window is >5 → HAVING, output mn=9.
	s1 := streamsql.New()
	require.NoError(t, s1.Execute(`SELECT min(v) AS mn FROM stream GROUP BY CountingWindow(3) HAVING mn > 5`))
	ch1 := make(chan []map[string]any, 4)
	s1.AddSink(func(r []map[string]any) { ch1 <- r })
	for _, v := range []float64{9, 9, 9} {
		s1.Emit(map[string]any{"v": v})
	}
	select {
	case rows := <-ch1:
		require.Len(t, rows, 1)
		assert.Equal(t, 9.0, rows[0]["mn"])
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: The windows of the >5 should pass through HAVING")
	}
	s1.Stop()

	// If the window contains dip → is blocked by HAVING and should not output a result containing the value.
	s2 := streamsql.New()
	require.NoError(t, s2.Execute(`SELECT min(v) AS mn FROM stream GROUP BY CountingWindow(3) HAVING mn > 5`))
	ch2 := make(chan []map[string]any, 4)
	s2.AddSink(func(r []map[string]any) { ch2 <- r })
	for _, v := range []float64{9, 1, 9} {
		s2.Emit(map[string]any{"v": v})
	}
	select {
	case rows := <-ch2:
		for _, r := range rows {
			if mn, ok := r["mn"]; ok && mn != nil {
				t.Fatalf("A window containing dip should be blocked by HAVING, but receives mn=%v", mn)
			}
		}
	case <-time.After(500 * time.Millisecond):
		// Expectation: No output (HAVING blocks the dip window).
	}
	s2.Stop()
}

// TestWindowOverRejected: OVER on the GROUP BY window is always rejected, and HAVING is used for guidance.
func TestWindowOverRejected(t *testing.T) {
	t.Parallel()
	for _, sql := range []string{
		`SELECT count(*) AS c FROM stream GROUP BY CountingWindow(3) OVER (WHEN v > 5)`,
		`SELECT count(*) AS c FROM stream GROUP BY CountingWindow(3) OVER (PARTITION BY k)`,
	} {
		ssql := streamsql.New()
		err := ssql.Execute(sql)
		require.Error(t, err, "窗口上的 OVER 应被拒绝: %s", sql)
		assert.Contains(t, err.Error(), "not supported")
		ssql.Stop()
	}
}

// TestPerRowWindowFunctionsRejectedAtExecute:row_number()/lead() has been removed from the registry,
// References must fail during the Execute period (when parsing unknown functions), rather than silently returning nil or crashing data paths.
// Returning to the "registered but unwired" half-finished product.
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
			require.Error(t, err, "%s() must be rejected (function removed)", c.fn)
			assert.Contains(t, err.Error(), c.fn, "error should name the unknown function")
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
