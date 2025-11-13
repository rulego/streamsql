package streamsql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLSessionWindow_SingleKey(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt
        FROM stream
        GROUP BY deviceId, SessionWindow('300ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 4)
	ssql.AddSink(func(results []map[string]interface{}) { ch <- results })

	for i := 0; i < 5; i++ {
		ssql.Emit(map[string]interface{}{"deviceId": "sensor001", "timestamp": time.Now()})
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(600 * time.Millisecond)

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		row := res[0]
		assert.Equal(t, "sensor001", row["deviceId"])
		assert.Equal(t, float64(5), row["cnt"])
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestSQLSessionWindow_GroupedSession_MixedDevices(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               AVG(temperature) as avg_temp
        FROM stream
        GROUP BY deviceId, SessionWindow('200ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 8)
	ssql.AddSink(func(results []map[string]interface{}) { ch <- results })

	// Emit data for two different devices in interleaved pattern
	for i := 0; i < 5; i++ {
		ssql.Emit(map[string]interface{}{"deviceId": "A", "temperature": float64(i), "timestamp": time.Now()})
		ssql.Emit(map[string]interface{}{"deviceId": "B", "temperature": float64(i + 10), "timestamp": time.Now()})
		time.Sleep(30 * time.Millisecond)
	}

	// Wait for session timeout
	time.Sleep(400 * time.Millisecond)

	ids := make(map[string]bool)
	avgTemps := make(map[string]float64)
	for k := 0; k < 2; k++ {
		select {
		case res := <-ch:
			require.Len(t, res, 1)
			id := res[0]["deviceId"].(string)
			avgTemp := res[0]["avg_temp"].(float64)
			ids[id] = true
			avgTemps[id] = avgTemp
		case <-time.After(2 * time.Second):
			t.Fatal("timeout")
		}
	}
	assert.True(t, ids["A"])
	assert.True(t, ids["B"])
	// Verify average temperatures: A should have avg of 0-4 = 2.0, B should have avg of 10-14 = 12.0
	assert.InEpsilon(t, 2.0, avgTemps["A"], 0.1)
	assert.InEpsilon(t, 12.0, avgTemps["B"], 0.1)
}

func TestSQLSessionWindow_MultiKeyGroupedSession(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId, region,
               COUNT(*) as cnt,
               AVG(temperature) as avg_temp,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, region, SessionWindow('200ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 8)
	ssql.AddSink(func(results []map[string]interface{}) { ch <- results })

	// Emit data for 4 different combinations: A|R1, B|R1, A|R2, B|R2
	for i := 0; i < 4; i++ {
		ssql.Emit(map[string]interface{}{"deviceId": "A", "region": "R1", "temperature": float64(i), "timestamp": time.Now()})
		ssql.Emit(map[string]interface{}{"deviceId": "B", "region": "R1", "temperature": float64(i + 10), "timestamp": time.Now()})
		ssql.Emit(map[string]interface{}{"deviceId": "A", "region": "R2", "temperature": float64(i + 20), "timestamp": time.Now()})
		ssql.Emit(map[string]interface{}{"deviceId": "B", "region": "R2", "temperature": float64(i + 30), "timestamp": time.Now()})
		time.Sleep(30 * time.Millisecond)
	}

	// Wait for session timeout
	time.Sleep(400 * time.Millisecond)

	type agg struct {
		cnt float64
		avg float64
		min float64
		max float64
	}
	got := make(map[string]agg)
	for k := 0; k < 4; k++ {
		select {
		case res := <-ch:
			require.Len(t, res, 1)
			id := res[0]["deviceId"].(string)
			region := res[0]["region"].(string)
			cnt := res[0]["cnt"].(float64)
			avg := res[0]["avg_temp"].(float64)
			min := res[0]["min_temp"].(float64)
			max := res[0]["max_temp"].(float64)
			got[id+"|"+region] = agg{cnt: cnt, avg: avg, min: min, max: max}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout")
		}
	}

	// Verify all 4 combinations are present
	require.Contains(t, got, "A|R1")
	require.Contains(t, got, "B|R1")
	require.Contains(t, got, "A|R2")
	require.Contains(t, got, "B|R2")

	// Verify counts: each combination should have 4 records
	assert.Equal(t, float64(4), got["A|R1"].cnt)
	assert.Equal(t, float64(4), got["B|R1"].cnt)
	assert.Equal(t, float64(4), got["A|R2"].cnt)
	assert.Equal(t, float64(4), got["B|R2"].cnt)

	// Verify averages: A|R1: (0+1+2+3)/4 = 1.5, B|R1: (10+11+12+13)/4 = 11.5
	//                  A|R2: (20+21+22+23)/4 = 21.5, B|R2: (30+31+32+33)/4 = 31.5
	assert.InEpsilon(t, 1.5, got["A|R1"].avg, 0.1)
	assert.InEpsilon(t, 11.5, got["B|R1"].avg, 0.1)
	assert.InEpsilon(t, 21.5, got["A|R2"].avg, 0.1)
	assert.InEpsilon(t, 31.5, got["B|R2"].avg, 0.1)

	// Verify minimums: A|R1: 0, B|R1: 10, A|R2: 20, B|R2: 30
	assert.Equal(t, 0.0, got["A|R1"].min)
	assert.Equal(t, 10.0, got["B|R1"].min)
	assert.Equal(t, 20.0, got["A|R2"].min)
	assert.Equal(t, 30.0, got["B|R2"].min)

	// Verify maximums: A|R1: 3, B|R1: 13, A|R2: 23, B|R2: 33
	assert.Equal(t, 3.0, got["A|R1"].max)
	assert.Equal(t, 13.0, got["B|R1"].max)
	assert.Equal(t, 23.0, got["A|R2"].max)
	assert.Equal(t, 33.0, got["B|R2"].max)
}
