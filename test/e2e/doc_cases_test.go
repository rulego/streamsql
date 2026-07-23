package e2e

import (
	"testing"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This document mirrors the SQL from the rulego-doc "Case Collection" to prevent document examples and code from drifting:
// If a streamsql change causes a case SQL to become invalid (parsing failed/output changes), it will be red first.
// Case Document: docs/03.StreamSQL/31. Case Collection/

// 05. Data filtering and conversion: Non-aggregation EmitSync, filtering + arithmetic conversion + CASE hierarchy.
func TestDocCases_FilterTransform(t *testing.T) {
	ssql := streamsql.New()
	const sql = `SELECT deviceId,
       temperature,
       temperature * 1.8 + 32 AS temp_f,
       CASE WHEN temperature > 35 THEN 'CRITICAL'
            WHEN temperature > 30 THEN 'WARNING'
            ELSE 'OK' END AS level
FROM stream
WHERE temperature > 0 AND temperature < 100`
	require.NoError(t, ssql.Execute(sql))
	defer ssql.Stop()

	cases := []struct {
		in     map[string]any
		filter bool // Expected to be filtered by WHERE (EmitSync returns nil)
		level  string
		tempF  float64
	}{
		{map[string]any{"deviceId": "dev-01", "temperature": 28.0}, false, "OK", 82.4},
		{map[string]any{"deviceId": "dev-02", "temperature": 32.0}, false, "WARNING", 89.6},
		{map[string]any{"deviceId": "dev-03", "temperature": 38.0}, false, "CRITICAL", 100.4},
		{map[string]any{"deviceId": "dev-04", "temperature": 999.0}, true, "", 0}, // Filtering beyond boundaries
		{map[string]any{"deviceId": "dev-05", "temperature": nil}, true, "", 0},   // NIL filtration
	}
	for _, c := range cases {
		out, err := ssql.EmitSync(c.in)
		require.NoError(t, err)
		if c.filter {
			assert.Nil(t, out, "deviceId=%v 应被 WHERE 过滤", c.in["deviceId"])
			continue
		}
		require.NotNil(t, out)
		assert.Equal(t, c.level, out["level"])
		assert.InDelta(t, c.tempF, out["temp_f"], 0.01)
	}
}

// 01. Enhanced JOIN metadata in flow tables: After executing, RegisterTable and INNER JOIN are supplemented with attributes, and no match is discarded.
func TestDocCases_JoinEnrichment(t *testing.T) {
	ssql := streamsql.New()
	const sql = `SELECT deviceId, m.location, m.model, temperature
FROM stream JOIN meta m ON deviceId = m.deviceId`
	require.NoError(t, ssql.Execute(sql))
	// RegisterTable must be placed after Execute.
	_, err := ssql.RegisterTable("meta", []map[string]any{
		{"deviceId": "d1", "location": "plantA", "model": "TX-100"},
		{"deviceId": "d2", "location": "plantB", "model": "TX-200"},
	})
	require.NoError(t, err)
	defer ssql.Stop()

	r1, err := ssql.EmitSync(map[string]any{"deviceId": "d1", "temperature": 31.0})
	require.NoError(t, err)
	require.NotNil(t, r1)
	assert.Equal(t, "plantA", r1["location"])
	assert.Equal(t, "TX-100", r1["model"])

	r2, _ := ssql.EmitSync(map[string]any{"deviceId": "d2", "temperature": 27.5})
	require.NotNil(t, r2)
	assert.Equal(t, "plantB", r2["location"])

	// d9 No match → INNER JOIN discard (nil, not error).
	r9, _ := ssql.EmitSync(map[string]any{"deviceId": "d9", "temperature": 40.0})
	assert.Nil(t, r9, "INNER JOIN 无匹配应丢弃")
}

// 03. Change Data Capture Scenario 1: Global lag (no partitioning), the moment when current jumps from ≤300 to >300.
func TestDocCases_CDC_GlobalLag(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT current, ts FROM stream WHERE current > 300 AND lag(current) <= 300`))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"current": 300, "ts": 1},
		{"current": 400, "ts": 2},
		{"current": 200, "ts": 3},
		{"current": 200, "ts": 4},
		{"current": 500, "ts": 5},
		{"current": 200, "ts": 6},
		{"current": 400, "ts": 7},
		{"current": 600, "ts": 8},
	}
	var outs []map[string]any
	for _, in := range inputs {
		if r, _ := ssql.EmitSync(in); r != nil {
			outs = append(outs, r)
		}
	}
	require.Len(t, outs, 3, "全局 lag：ts2/5/7 跨阈值")
	assert.Equal(t, 400, outs[0]["current"])
	assert.Equal(t, 500, outs[1]["current"])
	assert.Equal(t, 400, outs[2]["current"])
}

// 03. Change Data Capture Scenario 3: OVER WHEN Limit the lag range, focusing only on deviceId=1.
func TestDocCases_CDC_WhenLimitedLag(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT current, deviceId, ts FROM stream WHERE current > 300 AND deviceId = 1 AND lag(current) OVER (WHEN deviceId = 1) < 300`))
	defer ssql.Stop()

	inputs := []map[string]any{
		{"current": 300, "deviceId": 1, "ts": 1},
		{"current": 400, "deviceId": 2, "ts": 2},
		{"current": 200, "deviceId": 1, "ts": 3},
		{"current": 200, "deviceId": 2, "ts": 4},
		{"current": 500, "deviceId": 1, "ts": 5},
		{"current": 200, "deviceId": 2, "ts": 6},
		{"current": 400, "deviceId": 1, "ts": 7},
		{"current": 600, "deviceId": 2, "ts": 8},
	}
	var outs []map[string]any
	for _, in := range inputs {
		if r, _ := ssql.EmitSync(in); r != nil {
			outs = append(outs, r)
		}
	}
	require.Len(t, outs, 1, "OVER WHEN 限定 deviceId=1：仅 ts5(d1) 跨阈值")
	assert.Equal(t, 500, outs[0]["current"])
	assert.Equal(t, 1, outs[0]["deviceId"])
}

// Time-based window SQL must at least ensure parsable execution (output behavior is affected by time scheduling and is overlaid separately in window testing).
func TestDocCases_WindowSQL_Parses(t *testing.T) {
	t.Parallel()
	sqls := map[string]string{
		"session":  `SELECT deviceId, COUNT(*) AS msgs, MAX(ts) AS last_ts FROM stream GROUP BY deviceId, SessionWindow('5s')`,
		"sliding":  `SELECT MIN(concurrency) AS mn, COUNT(*) AS c FROM stream GROUP BY SlidingWindow('10s','2s') HAVING mn > 200`,
		"tumbling": `SELECT deviceId, COUNT(*) AS samples, AVG(temperature) AS a FROM stream GROUP BY deviceId, TumblingWindow('1m') WITH (TIMESTAMP='ts', TIMEUNIT='ms')`,
		"global":   `SELECT deviceId, MAX(temperature) AS max_t, COUNT(*) AS samples FROM stream GROUP BY deviceId, GLOBAL WINDOW TRIGGER WHEN MAX(temperature) > 50`,
	}
	for name, sql := range sqls {
		sql := sql
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ssql := streamsql.New()
			defer ssql.Stop()
			assert.NoError(t, ssql.Execute(sql), "案例 SQL %q 应可执行", name)
		})
	}
}
