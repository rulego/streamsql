package e2e

import (
	"testing"
	"time"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWindowHavingSustainedDetection 验证主流写法：窗口聚合全部事件，HAVING 拦 dip。
// 全 >5 的窗口通过 HAVING；含 dip(<5) 的窗口被 HAVING 拦住无输出。
// 注意：streamsql 的 HAVING 引用 SELECT 别名（mn），不复述聚合函数。
func TestWindowHavingSustainedDetection(t *testing.T) {
	t.Parallel()

	// 窗口全 >5 → 通过 HAVING，输出 mn=9。
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
		t.Fatal("timeout: 全 >5 的窗口应通过 HAVING")
	}
	s1.Stop()

	// 窗口含 dip → 被 HAVING 拦住，不应输出含数值的结果。
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
				t.Fatalf("含 dip 的窗口应被 HAVING 拦住，却收到 mn=%v", mn)
			}
		}
	case <-time.After(500 * time.Millisecond):
		// 期望：无输出（HAVING 拦住了 dip 窗口）。
	}
	s2.Stop()
}

// TestWindowOverRejected：GROUP BY 窗口上的 OVER(...) 一律拒绝，引导用 HAVING。
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

// TestPerRowWindowFunctionsRejectedAtExecute：row_number()/lead() 已从注册表移除，
// 引用须在 Execute 期（解析报未知函数）失败，而非静默返回 nil 或崩数据路径。
// 回归"注册但未接线"的半成品。
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
