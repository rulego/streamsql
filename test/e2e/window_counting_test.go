package streamsql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLCountingWindow_GroupByDevice(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt
        FROM stream
        GROUP BY deviceId, CountingWindow(10)
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 4)
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	for i := 0; i < 30; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": i,
			"timestamp":   time.Now(),
		})
	}

	// Expect 3 batches, each with one row for deviceId=sensor001
	for batch := 0; batch < 3; batch++ {
		select {
		case res := <-ch:
			require.Len(t, res, 1)
			row := res[0]
			assert.Equal(t, "sensor001", row["deviceId"])
			assert.Equal(t, float64(10), row["cnt"])
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for batch %d", batch+1)
		}
	}
}

func TestSQLCountingWindow_GroupedCounting_MixedDevices(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               AVG(temperature) as avg_temp
        FROM stream
        GROUP BY deviceId, CountingWindow(10)
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 8)
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		ch <- results
	})

	for i := 0; i < 10; i++ {
		ssql.Emit(map[string]interface{}{"deviceId": "A", "temperature": i, "timestamp": time.Now()})
		ssql.Emit(map[string]interface{}{"deviceId": "B", "temperature": i, "timestamp": time.Now()})
	}

	ids := make(map[string]bool)
	for k := 0; k < 2; k++ {
		select {
		case res := <-ch:
			require.Len(t, res, 1)
			id := res[0]["deviceId"].(string)
			ids[id] = true
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
		}
	}
	assert.True(t, ids["A"])
	assert.True(t, ids["B"])
}

func TestSQLCountingWindow_MultiKeyGroupedCounting(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId, region,
               COUNT(*) as cnt,
               AVG(temperature) as avg_temp,
               MIN(temperature) as min_temp
        FROM stream
        GROUP BY deviceId, region, CountingWindow(5)
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 8)
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		ch <- results
	})

	for i := 0; i < 5; i++ {
		ssql.Emit(map[string]interface{}{"deviceId": "A", "region": "R1", "temperature": i, "timestamp": time.Now()})
		ssql.Emit(map[string]interface{}{"deviceId": "B", "region": "R1", "temperature": i + 10, "timestamp": time.Now()})
		ssql.Emit(map[string]interface{}{"deviceId": "A", "region": "R2", "temperature": i + 20, "timestamp": time.Now()})
		ssql.Emit(map[string]interface{}{"deviceId": "B", "region": "R2", "temperature": i + 30, "timestamp": time.Now()})
	}

	type agg struct {
		cnt float64
		avg float64
		min float64
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
			got[id+"|"+region] = agg{cnt: cnt, avg: avg, min: min}
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
		}
	}
	// Expect 4 combinations all counted to 5, with known avg/min
	assert.Equal(t, float64(5), got["A|R1"].cnt)
	assert.Equal(t, float64(5), got["B|R1"].cnt)
	assert.Equal(t, float64(5), got["A|R2"].cnt)
	assert.Equal(t, float64(5), got["B|R2"].cnt)

	assert.InEpsilon(t, 2.0, got["A|R1"].avg, 0.0001)
	assert.InEpsilon(t, 12.0, got["B|R1"].avg, 0.0001)
	assert.InEpsilon(t, 22.0, got["A|R2"].avg, 0.0001)
	assert.InEpsilon(t, 32.0, got["B|R2"].avg, 0.0001)

	assert.Equal(t, 0.0, got["A|R1"].min)
	assert.InEpsilon(t, 10.0, got["B|R1"].min, 0.0001)
	assert.InEpsilon(t, 20.0, got["A|R2"].min, 0.0001)
	assert.InEpsilon(t, 30.0, got["B|R2"].min, 0.0001)
}
