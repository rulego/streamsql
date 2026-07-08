package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runOrderByWindow runs a windowed SQL query, emits the given rows, and returns
// the largest result batch received by the sink (polled until it has at least
// want rows or ~1s elapses). All ORDER BY ordering happens server-side before
// the sink sees the batch.
func runOrderByWindow(t *testing.T, sql string, emit []map[string]interface{}, want int) []map[string]interface{} {
	t.Helper()
	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute(sql))

	var got []map[string]interface{}
	var mu sync.Mutex
	ssql.AddSink(func(r []map[string]interface{}) {
		mu.Lock()
		defer mu.Unlock()
		if len(r) > len(got) {
			got = r
		}
	})

	for _, d := range emit {
		ssql.Emit(d)
	}

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= want {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(got), want, "expected at least %d rows in window batch, got %d", want, len(got))
	return got
}

func TestIntegration_OrderBy_DescOnAggAlias(t *testing.T) {
	t.Parallel()
	sql := `SELECT deviceId, avg(temperature) AS m
	        FROM stream
	        GROUP BY deviceId, TumblingWindow('100ms')
	        ORDER BY m DESC`
	batch := runOrderByWindow(t, sql, []map[string]interface{}{
		{"deviceId": "d1", "temperature": 30.0},
		{"deviceId": "d2", "temperature": 50.0},
		{"deviceId": "d3", "temperature": 40.0},
	}, 3)

	// Expect d2 (50) > d3 (40) > d1 (30).
	assert.Equal(t, "d2", batch[0]["deviceId"])
	assert.Equal(t, "d3", batch[1]["deviceId"])
	assert.Equal(t, "d1", batch[2]["deviceId"])
}

func TestIntegration_OrderBy_AscOnAggAlias(t *testing.T) {
	t.Parallel()
	sql := `SELECT deviceId, avg(temperature) AS m
	        FROM stream
	        GROUP BY deviceId, TumblingWindow('100ms')
	        ORDER BY m ASC`
	batch := runOrderByWindow(t, sql, []map[string]interface{}{
		{"deviceId": "d1", "temperature": 30.0},
		{"deviceId": "d2", "temperature": 50.0},
		{"deviceId": "d3", "temperature": 40.0},
	}, 3)

	// Ascending: d1 (30) < d3 (40) < d2 (50).
	assert.Equal(t, "d1", batch[0]["deviceId"])
	assert.Equal(t, "d3", batch[1]["deviceId"])
	assert.Equal(t, "d2", batch[2]["deviceId"])
}

func TestIntegration_OrderBy_WithLimitTopN(t *testing.T) {
	t.Parallel()
	sql := `SELECT deviceId, avg(temperature) AS m
	        FROM stream
	        GROUP BY deviceId, TumblingWindow('100ms')
	        ORDER BY m DESC LIMIT 2`
	batch := runOrderByWindow(t, sql, []map[string]interface{}{
		{"deviceId": "d1", "temperature": 30.0},
		{"deviceId": "d2", "temperature": 50.0},
		{"deviceId": "d3", "temperature": 40.0},
	}, 2)

	require.Len(t, batch, 2, "LIMIT 2 should cap the batch at 2 rows")
	// Top-2 by m: d2 (50), d3 (40).
	assert.Equal(t, "d2", batch[0]["deviceId"])
	assert.Equal(t, "d3", batch[1]["deviceId"])
}

func TestIntegration_OrderBy_OnGroupKey(t *testing.T) {
	t.Parallel()
	sql := `SELECT deviceId, avg(temperature) AS m
	        FROM stream
	        GROUP BY deviceId, TumblingWindow('100ms')
	        ORDER BY deviceId ASC`
	batch := runOrderByWindow(t, sql, []map[string]interface{}{
		{"deviceId": "gamma", "temperature": 1.0},
		{"deviceId": "alpha", "temperature": 1.0},
		{"deviceId": "beta", "temperature": 1.0},
	}, 3)

	assert.Equal(t, "alpha", batch[0]["deviceId"])
	assert.Equal(t, "beta", batch[1]["deviceId"])
	assert.Equal(t, "gamma", batch[2]["deviceId"])
}

// TestIntegration_OrderBy_MultiKey: primary m DESC, secondary deviceId ASC.
// Two devices share the same m so the secondary key decides their order.
func TestIntegration_OrderBy_MultiKey(t *testing.T) {
	t.Parallel()
	sql := `SELECT deviceId, avg(temperature) AS m
	        FROM stream
	        GROUP BY deviceId, TumblingWindow('100ms')
	        ORDER BY m DESC, deviceId ASC`
	batch := runOrderByWindow(t, sql, []map[string]interface{}{
		{"deviceId": "zzz", "temperature": 40.0}, // m=40 (tie)
		{"deviceId": "aaa", "temperature": 40.0}, // m=40 (tie)
		{"deviceId": "mid", "temperature": 99.0}, // m=99 (largest)
	}, 3)

	// mid(99) first; then the two m=40 rows by deviceId asc: aaa < zzz.
	assert.Equal(t, "mid", batch[0]["deviceId"])
	assert.Equal(t, "aaa", batch[1]["deviceId"])
	assert.Equal(t, "zzz", batch[2]["deviceId"])
}

// TestIntegration_OrderBy_NonWindowNoCrash: ORDER BY on a non-aggregation query
// must not break per-row processing (ordering a single-row batch is a no-op).
func TestIntegration_OrderBy_NonWindowNoCrash(t *testing.T) {
	t.Parallel()
	sql := `SELECT deviceId, temperature FROM stream WHERE temperature > 20 ORDER BY temperature DESC`
	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute(sql))

	var mu sync.Mutex
	var count int
	ssql.AddSink(func(r []map[string]interface{}) {
		mu.Lock()
		defer mu.Unlock()
		count += len(r)
	})

	ssql.Emit(map[string]interface{}{"deviceId": "d1", "temperature": 30.0})
	ssql.Emit(map[string]interface{}{"deviceId": "d2", "temperature": 10.0}) // filtered out

	time.Sleep(150 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, count, 1, "non-window ORDER BY should still emit passing rows")
}
