package e2e

import (
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJoinAggregationGroupByTableField verifies stream-table JOIN enrichment
// feeds the window/aggregator path: GROUP BY a table column (m.location) and
// AVG over a stream column, INNER JOIN. Enrichment must run before Window.Add
// so the table column is visible to the aggregator.
func TestJoinAggregationGroupByTableField(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT m.location, AVG(temp) AS avg_t
		FROM stream JOIN meta m ON deviceId = m.deviceId
		GROUP BY m.location, CountingWindow(4)`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(results []map[string]any) { ch <- results })

	// d1 -> plantA (temp 30,40), d2 -> plantB (temp 20,60); window fires at 4 rows.
	ssql.Emit(map[string]any{"deviceId": "d1", "temp": 30.0})
	ssql.Emit(map[string]any{"deviceId": "d1", "temp": 40.0})
	ssql.Emit(map[string]any{"deviceId": "d2", "temp": 20.0})
	ssql.Emit(map[string]any{"deviceId": "d2", "temp": 60.0})

	// CountingWindow(4) fires one batch with both groups in it.
	avg := make(map[string]float64)
	select {
	case res := <-ch:
		for _, row := range res {
			loc, _ := row["location"].(string)
			avg[loc] = row["avg_t"].(float64)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for aggregation batch; got=%v", avg)
	}
	require.Len(t, avg, 2, "expected plantA and plantB groups, got %v", avg)
	assert.InEpsilon(t, 35.0, avg["plantA"], 0.0001)
	assert.InEpsilon(t, 40.0, avg["plantB"], 0.0001)
}

// TestJoinLeftAggregationNullGroup verifies a LEFT JOIN row with no match
// aggregates into a NULL group (location == nil) instead of being dropped.
func TestJoinLeftAggregationNullGroup(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT m.location, AVG(temp) AS avg_t
		FROM stream LEFT JOIN meta m ON deviceId = m.deviceId
		GROUP BY m.location, CountingWindow(2)`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(results []map[string]any) { ch <- results })

	ssql.Emit(map[string]any{"deviceId": "d1", "temp": 10.0}) // plantA
	ssql.Emit(map[string]any{"deviceId": "d9", "temp": 20.0}) // no match -> NULL group

	select {
	case res := <-ch:
		require.Len(t, res, 2, "expected plantA and NULL groups, got %v", res)
		var nullAvg, plantAAvg *float64
		for _, row := range res {
			v := row["avg_t"].(float64)
			if row["location"] == nil {
				nullAvg = &v
			} else {
				plantAAvg = &v
			}
		}
		require.NotNil(t, nullAvg, "NULL group missing (LEFT unmatched row dropped?)")
		require.NotNil(t, plantAAvg, "plantA group missing")
		assert.InEpsilon(t, 10.0, *plantAAvg, 0.0001)
		assert.InEpsilon(t, 20.0, *nullAvg, 0.0001)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for aggregation batch")
	}
}

// TestJoinInnerAggregationDropsUnmatched verifies an INNER JOIN row with no
// match is dropped before entering the window, so it never contributes to any
// aggregate (no NULL group, not counted).
func TestJoinInnerAggregationDropsUnmatched(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT m.location, COUNT(*) AS cnt
		FROM stream JOIN meta m ON deviceId = m.deviceId
		GROUP BY m.location, CountingWindow(2)`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(results []map[string]any) { ch <- results })

	ssql.Emit(map[string]any{"deviceId": "d1", "temp": 1.0}) // plantA
	ssql.Emit(map[string]any{"deviceId": "d1", "temp": 2.0}) // plantA -> fires at 2 rows
	ssql.Emit(map[string]any{"deviceId": "d9", "temp": 9.0}) // unmatched INNER -> dropped pre-window

	select {
	case res := <-ch:
		require.Len(t, res, 1, "only plantA expected (d9 dropped), got %v", res)
		assert.Equal(t, "plantA", res[0]["location"])
		assert.Equal(t, float64(2), res[0]["cnt"])
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for aggregation batch")
	}
	// d9 must not produce a later batch.
	select {
	case extra := <-ch:
		t.Fatalf("unexpected extra batch (d9 should be dropped): %v", extra)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestJoinAggregationUpsert verifies a table Upsert between emits changes
// subsequent enrichment: after adding d3 metadata, a d3 stream row aggregates
// under the new location.
func TestJoinAggregationUpsert(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT m.location, COUNT(*) AS cnt
		FROM stream JOIN meta m ON deviceId = m.deviceId
		GROUP BY m.location, CountingWindow(1)`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 8)
	ssql.AddSink(func(results []map[string]any) { ch <- results })

	// Before upsert: d3 has no metadata -> INNER drops it.
	ssql.Emit(map[string]any{"deviceId": "d3", "temp": 1.0})
	// Add d3 metadata, then emit d3 again -> now enriched to plantC.
	require.NoError(t, ssql.UpsertTable("meta", map[string]any{"deviceId": "d3", "location": "plantC", "type": "temp"}))
	ssql.Emit(map[string]any{"deviceId": "d3", "temp": 2.0})

	// First emit (d3 unmatched) is dropped; only the post-upsert d3 fires.
	var sawPlantC bool
	timeout := time.After(2 * time.Second)
loop:
	for k := 0; k < 4; k++ {
		select {
		case res := <-ch:
			for _, row := range res {
				if row["location"] == "plantC" {
					sawPlantC = true
					assert.Equal(t, float64(1), row["cnt"])
				}
			}
		case <-timeout:
			break loop
		}
	}
	assert.True(t, sawPlantC, "post-upsert d3 should aggregate under plantC")
}

// TestJoinAggregationCompositeKey verifies a composite-key JOIN (two ON pairs)
// feeds the aggregator: rows match on (deviceId, tenant) and group by m.loc.
func TestJoinAggregationCompositeKey(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT m.loc, COUNT(*) AS cnt
		FROM stream JOIN meta m ON deviceId = m.deviceId AND tenant = m.tenant
		GROUP BY m.loc, CountingWindow(2)`
	require.NoError(t, ssql.Execute(sql))
	// Key auto-derived from ON: (deviceId, tenant).
	_, err := ssql.RegisterTable("meta", []map[string]any{
		{"deviceId": "d1", "tenant": "t1", "loc": "plantA"},
		{"deviceId": "d1", "tenant": "t2", "loc": "plantB"},
	})
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	ssql.Emit(map[string]any{"deviceId": "d1", "tenant": "t1"}) // -> plantA
	ssql.Emit(map[string]any{"deviceId": "d1", "tenant": "t2"}) // -> plantB

	got := make(map[string]float64)
	select {
	case res := <-ch:
		for _, row := range res {
			loc, _ := row["loc"].(string)
			got[loc] = row["cnt"].(float64)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for composite-key aggregation")
	}
	assert.Equal(t, float64(1), got["plantA"], "plantA group count, got=%v", got)
	assert.Equal(t, float64(1), got["plantB"], "plantB group count, got=%v", got)
}

// TestJoinAggregationWhereOnTableField verifies WHERE can reference a joined
// column (m.type) and filters BEFORE windowing, so filtered rows never aggregate.
func TestJoinAggregationWhereOnTableField(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	// deviceMetaRows: d1=plantA/temp, d2=plantB/humid. WHERE m.type='temp' keeps d1 only.
	sql := `SELECT m.location, COUNT(*) AS cnt
		FROM stream JOIN meta m ON deviceId = m.deviceId
		WHERE m.type = 'temp'
		GROUP BY m.location, CountingWindow(3)`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	ssql.Emit(map[string]any{"deviceId": "d1"}) // temp -> kept
	ssql.Emit(map[string]any{"deviceId": "d2"}) // humid -> filtered pre-window
	ssql.Emit(map[string]any{"deviceId": "d1"}) // kept
	ssql.Emit(map[string]any{"deviceId": "d1"}) // kept -> 3rd Add fires

	select {
	case res := <-ch:
		require.Len(t, res, 1, "only plantA expected (d2 filtered), got %v", res)
		assert.Equal(t, "plantA", res[0]["location"])
		assert.Equal(t, float64(3), res[0]["cnt"])
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for WHERE-filtered aggregation")
	}
}

// TestJoinAggregationEventTimeWindow verifies enrichment works on the event-time
// path: the eventTime field survives enrichment (copied top-level) so the
// watermark advances and fires the window with joined group columns.
func TestJoinAggregationEventTimeWindow(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT m.location, COUNT(*) AS cnt
		FROM stream JOIN meta m ON deviceId = m.deviceId
		GROUP BY m.location, SessionWindow('300ms')
		WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='200ms', IDLETIMEOUT='2s')`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 8)
	ssql.AddSink(func(r []map[string]any) {
		defer func() { _ = recover() }()
		ch <- r
	})

	baseTime := time.Now().UnixMilli() - 5000
	// 3 d1 (plantA) rows form one session at base+0..100.
	for i := 0; i < 3; i++ {
		ssql.Emit(map[string]any{"deviceId": "d1", "eventTime": baseTime + int64(i*50)})
		time.Sleep(20 * time.Millisecond)
	}
	// A matched far-future d2 (plantB) row advances the watermark past the plantA
	// session close, firing it. (An unmatched row would be dropped pre-window and
	// never advance the watermark, so it must be a matched row.)
	ssql.Emit(map[string]any{"deviceId": "d2", "eventTime": baseTime + 2000})

	var plantACnt float64
	var saw bool
	timeout := time.After(3 * time.Second)
loop:
	for {
		select {
		case res := <-ch:
			for _, row := range res {
				if row["location"] == "plantA" {
					plantACnt = row["cnt"].(float64)
					if plantACnt == 3 {
						saw = true
						break loop
					}
				}
			}
		case <-timeout:
			break loop
		}
	}
	assert.True(t, saw, "plantA event-time session of 3 should fire; got cnt=%v", plantACnt)
}

// TestJoinAggregationTumblingWindow verifies JOIN + multi-aggregate (AVG + COUNT)
// over a processing-time TumblingWindow — the SQL shape a file/custom-source
// integration test would use.
func TestJoinAggregationTumblingWindow(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT m.location, AVG(temperature) AS avg_temp, COUNT(*) AS cnt
		FROM stream JOIN meta m ON deviceId = m.deviceId
		GROUP BY m.location, TumblingWindow('1s')`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	// d1 -> plantA (30, 40), d2 -> plantB (20); all land in the same 1s window.
	ssql.Emit(map[string]any{"deviceId": "d1", "temperature": 30.0})
	ssql.Emit(map[string]any{"deviceId": "d1", "temperature": 40.0})
	ssql.Emit(map[string]any{"deviceId": "d2", "temperature": 20.0})

	got := make(map[string]float64)
	select {
	case res := <-ch:
		for _, row := range res {
			loc, _ := row["location"].(string)
			if loc == "plantA" {
				got["plantA_avg"] = row["avg_temp"].(float64)
				got["plantA_cnt"] = row["cnt"].(float64)
			} else if loc == "plantB" {
				got["plantB_avg"] = row["avg_temp"].(float64)
				got["plantB_cnt"] = row["cnt"].(float64)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for tumbling window fire")
	}
	assert.InEpsilon(t, 35.0, got["plantA_avg"], 0.0001)
	assert.Equal(t, float64(2), got["plantA_cnt"])
	assert.InEpsilon(t, 20.0, got["plantB_avg"], 0.0001)
	assert.Equal(t, float64(1), got["plantB_cnt"])
}

// TestJoinAggregationGlobalWindow verifies JOIN enrichment reaches the Global
// window path (which groups and aggregates internally): GROUP BY a table column
// under a TRIGGER WHEN threshold.
func TestJoinAggregationGlobalWindow(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT m.location, COUNT(*) AS cnt
		FROM stream JOIN meta m ON deviceId = m.deviceId
		GROUP BY m.location, GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 2`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	ssql.Emit(map[string]any{"deviceId": "d1"}) // plantA
	ssql.Emit(map[string]any{"deviceId": "d1"}) // plantA -> fire (cnt 2, then purge)
	ssql.Emit(map[string]any{"deviceId": "d2"}) // plantB
	ssql.Emit(map[string]any{"deviceId": "d2"}) // plantB -> fire (cnt 2)

	got := make(map[string]float64)
	for k := 0; k < 2; k++ {
		select {
		case res := <-ch:
			for _, row := range res {
				loc, _ := row["location"].(string)
				got[loc] = row["cnt"].(float64)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for global window fire %d; got=%v", k+1, got)
		}
	}
	assert.Equal(t, float64(2), got["plantA"])
	assert.Equal(t, float64(2), got["plantB"])
}

// TestJoinAggregationMultipleTables verifies multiple stream-table JOINs feed
// the aggregator: GROUP BY columns from two different joined tables.
func TestJoinAggregationMultipleTables(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT l.location, s.model, COUNT(*) AS cnt
		FROM stream JOIN locations l ON deviceId = l.deviceId JOIN models s ON deviceId = s.deviceId
		GROUP BY l.location, s.model, CountingWindow(2)`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("locations", []map[string]any{
		{"deviceId": "d1", "location": "plantA"},
	})
	require.NoError(t, err)
	_, err = ssql.RegisterTable("models", []map[string]any{
		{"deviceId": "d1", "model": "MX-1"},
	})
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	ssql.Emit(map[string]any{"deviceId": "d1"})
	ssql.Emit(map[string]any{"deviceId": "d1"})

	select {
	case res := <-ch:
		require.Len(t, res, 1, "expected one group (plantA, MX-1), got %v", res)
		assert.Equal(t, "plantA", res[0]["location"])
		assert.Equal(t, "MX-1", res[0]["model"])
		assert.Equal(t, float64(2), res[0]["cnt"])
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for multi-table aggregation")
	}
}
