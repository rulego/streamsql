package e2e

import (
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJoinAggregationGroupColumnAlias verifies the aggregation path honors a
// SELECT AS alias on a grouped column (AJ1 point 2): "SELECT m.location AS loc"
// emits key "loc", not "m.location" or "location".
func TestJoinAggregationGroupColumnAlias(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT m.location AS loc, COUNT(*) AS cnt
		FROM stream JOIN meta m ON deviceId = m.deviceId
		GROUP BY m.location, CountingWindow(2)`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	ssql.Emit(map[string]any{"deviceId": "d1"})
	ssql.Emit(map[string]any{"deviceId": "d1"})

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		assert.Equal(t, "plantA", res[0]["loc"])        // alias honored
		assert.Equal(t, float64(2), res[0]["cnt"])
		_, hasQualified := res[0]["m.location"]          // qualifier stripped
		assert.False(t, hasQualified)
		_, hasBare := res[0]["location"]                 // bare name not used (alias won)
		assert.False(t, hasBare)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for aggregation batch")
	}
}

// TestJoinColumnCollisionDirect verifies the direct path rejects two joined
// columns that strip to the same output name (a map cannot hold both).
func TestJoinColumnCollisionDirect(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT a.location, b.location
		FROM stream JOIN t1 a ON id = a.id JOIN t2 b ON id = b.id`
	err := ssql.Execute(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous output column")
}

// TestJoinColumnCollisionAggregation verifies the aggregation path rejects two
// GROUP BY columns that strip to the same output name.
func TestJoinColumnCollisionAggregation(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT a.location, b.location, COUNT(*) AS cnt
		FROM stream JOIN t1 a ON id = a.id JOIN t2 b ON id = b.id
		GROUP BY a.location, b.location, CountingWindow(2)`
	err := ssql.Execute(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous output column")
}

// TestJoinColumnCollisionGlobalWindow verifies the global-window path also
// rejects colliding grouped columns at compile time.
func TestJoinColumnCollisionGlobalWindow(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT a.location, b.location, COUNT(*) AS cnt
		FROM stream JOIN t1 a ON id = a.id JOIN t2 b ON id = b.id
		GROUP BY a.location, b.location, GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 2`
	err := ssql.Execute(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous output column")
}

// TestJoinColumnCollisionResolvedByAlias verifies AS aliases let two same-named
// joined columns coexist (no collision, both values emitted).
func TestJoinColumnCollisionResolvedByAlias(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT a.location AS loc_a, b.location AS loc_b
		FROM stream JOIN t1 a ON id = a.id JOIN t2 b ON id = b.id`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("t1", []map[string]any{{"id": "1", "location": "AAA"}})
	require.NoError(t, err)
	_, err = ssql.RegisterTable("t2", []map[string]any{{"id": "1", "location": "BBB"}})
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })
	ssql.Emit(map[string]any{"id": "1"})

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		assert.Equal(t, "AAA", res[0]["loc_a"])
		assert.Equal(t, "BBB", res[0]["loc_b"])
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for direct JOIN result")
	}
}

// runHavingForm runs an aggregation whose HAVING filters by the plantA group,
// using the given HAVING clause. outKey is the result-map key the grouped
// column is emitted under. Returns the surviving group location values.
func runHavingForm(t *testing.T, selectCol, havingCol, outKey string) map[string]bool {
	t.Helper()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT ` + selectCol + `, COUNT(*) AS cnt
		FROM stream JOIN meta m ON deviceId = m.deviceId
		GROUP BY m.location, CountingWindow(4)
		HAVING ` + havingCol + ` = 'plantA'`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	// d1->plantA (x2), d2->plantB (x2); window fires at 4 rows with both groups.
	ssql.Emit(map[string]any{"deviceId": "d1"})
	ssql.Emit(map[string]any{"deviceId": "d1"})
	ssql.Emit(map[string]any{"deviceId": "d2"})
	ssql.Emit(map[string]any{"deviceId": "d2"})

	survived := make(map[string]bool)
	select {
	case res := <-ch:
		for _, row := range res {
			if loc, ok := row[outKey].(string); ok {
				survived[loc] = true
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for HAVING (%s) batch", havingCol)
	}
	return survived
}

// TestJoinAggregationHavingNameForms verifies HAVING can reference a grouped
// column by its output name, the qualified name, or an AS alias.
func TestJoinAggregationHavingNameForms(t *testing.T) {
	t.Parallel()
	// HAVING by stripped output name "location".
	survived := runHavingForm(t, "m.location", "location", "location")
	assert.True(t, survived["plantA"], "HAVING location: plantA should survive")
	assert.False(t, survived["plantB"], "HAVING location: plantB should be filtered")

	// HAVING by qualified name "m.location".
	survived = runHavingForm(t, "m.location", "m.location", "location")
	assert.True(t, survived["plantA"], "HAVING m.location: plantA should survive")
	assert.False(t, survived["plantB"], "HAVING m.location: plantB should be filtered")

	// HAVING by AS alias "loc".
	survived = runHavingForm(t, "m.location AS loc", "loc", "loc")
	assert.True(t, survived["plantA"], "HAVING loc: plantA should survive")
	assert.False(t, survived["plantB"], "HAVING loc: plantB should be filtered")
}

// TestJoinAggregationOrderByNameForms verifies ORDER BY can reference a grouped
// column by its output name and by the qualified name.
func TestJoinAggregationOrderByNameForms(t *testing.T) {
	for _, orderCol := range []string{"location", "m.location"} {
		t.Run(orderCol, func(t *testing.T) {
			ssql := streamsql.New()
			defer ssql.Stop()
			sql := `SELECT m.location, COUNT(*) AS cnt
				FROM stream JOIN meta m ON deviceId = m.deviceId
				GROUP BY m.location, CountingWindow(4)
				ORDER BY ` + orderCol + ` DESC`
			require.NoError(t, ssql.Execute(sql))
			_, err := ssql.RegisterTable("meta", deviceMetaRows())
			require.NoError(t, err)

			ch := make(chan []map[string]any, 4)
			ssql.AddSink(func(r []map[string]any) { ch <- r })

			ssql.Emit(map[string]any{"deviceId": "d1"})
			ssql.Emit(map[string]any{"deviceId": "d1"})
			ssql.Emit(map[string]any{"deviceId": "d2"})
			ssql.Emit(map[string]any{"deviceId": "d2"})

			select {
			case res := <-ch:
				require.Len(t, res, 2)
				// DESC over {"plantA","plantB"} -> plantB first.
				assert.Equal(t, "plantB", res[0]["location"])
				assert.Equal(t, "plantA", res[1]["location"])
				_, hasQualified := res[0]["m.location"] // qualifier stripped on output
				assert.False(t, hasQualified)
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout waiting for ORDER BY %s batch", orderCol)
			}
		})
	}
}

// TestJoinAggregationGlobalWindowAlias verifies the global-window path also
// strips the join alias / honors AS on grouped columns.
func TestJoinAggregationGlobalWindowAlias(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	sql := `SELECT m.location AS site, COUNT(*) AS cnt
		FROM stream JOIN meta m ON deviceId = m.deviceId
		GROUP BY m.location, GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 2`
	require.NoError(t, ssql.Execute(sql))
	_, err := ssql.RegisterTable("meta", deviceMetaRows())
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(r []map[string]any) { ch <- r })

	ssql.Emit(map[string]any{"deviceId": "d1"})
	ssql.Emit(map[string]any{"deviceId": "d1"}) // plantA fires (cnt 2)
	ssql.Emit(map[string]any{"deviceId": "d2"})
	ssql.Emit(map[string]any{"deviceId": "d2"}) // plantB fires (cnt 2)

	got := make(map[string]float64)
	for k := 0; k < 2; k++ {
		select {
		case res := <-ch:
			for _, row := range res {
				if site, ok := row["site"].(string); ok {
					got[site] = row["cnt"].(float64)
				}
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for global window fire %d; got=%v", k+1, got)
		}
	}
	assert.Equal(t, float64(2), got["plantA"])
	assert.Equal(t, float64(2), got["plantB"])
}
