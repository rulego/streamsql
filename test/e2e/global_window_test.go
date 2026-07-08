/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package e2e

import (
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGlobalWindow_CountDrivenAlert: COUNT(*) reaches the trigger threshold and
// the global window fires the running aggregate, then purges (next batch starts
// from 0 again). This is the Flink GlobalWindows + CountTrigger pattern.
func TestGlobalWindow_CountDrivenAlert(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId, COUNT(*) AS cnt
        FROM stream
        GROUP BY deviceId, GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 3
    `
	require.NoError(t, ssql.Execute(sql))

	ch := make(chan []map[string]interface{}, 4)
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	for i := 0; i < 6; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId": "sensorA",
			"value":    i,
		})
	}

	// Two fires of 3 rows each (FIRE_AND_PURGE resets the count).
	for batch := 0; batch < 2; batch++ {
		select {
		case res := <-ch:
			require.Len(t, res, 1)
			assert.Equal(t, "sensorA", res[0]["deviceId"])
			assert.Equal(t, float64(3), res[0]["cnt"])
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for fire %d", batch+1)
		}
	}
}

// TestGlobalWindow_FieldDrivenTrigger: a field-value-driven predicate
// (MAX(temp) > 50) fires the instant the running max crosses the threshold.
func TestGlobalWindow_FieldDrivenTrigger(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId, MAX(temp) AS max_temp
        FROM stream
        GROUP BY deviceId, GLOBAL WINDOW TRIGGER WHEN MAX(temp) > 50
    `
	require.NoError(t, ssql.Execute(sql))

	ch := make(chan []map[string]interface{}, 4)
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	// Below threshold: no fire.
	ssql.Emit(map[string]interface{}{"deviceId": "dev1", "temp": 40})
	ssql.Emit(map[string]interface{}{"deviceId": "dev1", "temp": 45})
	// Crosses threshold: fire with max=55.
	ssql.Emit(map[string]interface{}{"deviceId": "dev1", "temp": 55})

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		assert.Equal(t, "dev1", res[0]["deviceId"])
		assert.Equal(t, float64(55), res[0]["max_temp"])
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for field-driven fire")
	}
}

// TestGlobalWindow_NoGroupBy: a global window with no GROUP BY aggregates the
// whole stream into a single implicit group.
func TestGlobalWindow_NoGroupBy(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT COUNT(*) AS total
        FROM stream
        GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 4
    `
	require.NoError(t, ssql.Execute(sql))

	ch := make(chan []map[string]interface{}, 4)
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	for i := 0; i < 4; i++ {
		ssql.Emit(map[string]interface{}{"v": i})
	}

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		assert.Equal(t, float64(4), res[0]["total"])
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ungrouped global window fire")
	}
}

// TestGlobalWindow_MultiGroupIndependentFire: groups fire independently as each
// crosses the trigger threshold.
func TestGlobalWindow_MultiGroupIndependentFire(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId, COUNT(*) AS cnt
        FROM stream
        GROUP BY deviceId, GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 2
    `
	require.NoError(t, ssql.Execute(sql))

	ch := make(chan []map[string]interface{}, 8)
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	// Interleave; each device hits cnt=2 on its own.
	ssql.Emit(map[string]interface{}{"deviceId": "x"})
	ssql.Emit(map[string]interface{}{"deviceId": "y"})
	ssql.Emit(map[string]interface{}{"deviceId": "x"}) // x fires
	ssql.Emit(map[string]interface{}{"deviceId": "y"}) // y fires

	fired := map[string]int{}
	for i := 0; i < 2; i++ {
		select {
		case res := <-ch:
			require.Len(t, res, 1)
			fired[res[0]["deviceId"].(string)] = int(res[0]["cnt"].(float64))
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for fire %d", i+1)
		}
	}
	assert.Equal(t, 2, fired["x"], "device x should fire at cnt=2")
	assert.Equal(t, 2, fired["y"], "device y should fire at cnt=2")
}

// TestGlobalWindow_WithStateTTL: WITH(STATETTL=...) config parses and the stream
// runs end-to-end; the STATETTL reap is covered by unit tests, here we only
// verify the SQL compiles and the window emits normally.
func TestGlobalWindow_WithStateTTL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId, COUNT(*) AS cnt
        FROM stream
        GROUP BY deviceId, GLOBAL WINDOW TRIGGER WHEN COUNT(*) >= 2
        WITH(STATETTL='1h', IDLETIMEOUT='60s')
    `
	require.NoError(t, ssql.Execute(sql))

	ch := make(chan []map[string]interface{}, 4)
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	ssql.Emit(map[string]interface{}{"deviceId": "d"})
	ssql.Emit(map[string]interface{}{"deviceId": "d"})

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		assert.Equal(t, float64(2), res[0]["cnt"])
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for fire with WITH(STATETTL=...) config")
	}
}

// TestGlobalWindow_MissingTriggerRejected: a GLOBAL WINDOW without TRIGGER WHEN
// is a NeverTrigger and must be rejected at Execute time.
func TestGlobalWindow_MissingTriggerRejected(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `SELECT deviceId, COUNT(*) AS cnt FROM stream GROUP BY deviceId, GLOBAL WINDOW`
	err := ssql.Execute(sql)
	require.Error(t, err, "GLOBAL WINDOW without TRIGGER WHEN should be rejected")
}
