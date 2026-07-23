package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLSessionWindow_ProcessingTime Session window for testing processing time
// When verification does not use the WITH clause, the session window operates based on processing time (system clock).
func TestSQLSessionWindow_ProcessingTime(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt
        FROM stream
        GROUP BY deviceId, SessionWindow('300ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		ch <- results
	})

	// Use processing time: Send data without timestamp fields
	// The session window divides sessions based on the processing time (system clock) of data arrival
	for i := 0; i < 5; i++ {
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond) // If the data interval is less than the session timeout (300ms), it belongs to the same session
	}

	// Waiting for session timeout (processing time session window triggered by system clock)
	time.Sleep(600 * time.Millisecond)

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		row := res[0]
		assert.Equal(t, "sensor001", row["deviceId"])
		assert.Equal(t, float64(5), row["cnt"])
		t.Logf("Processing time session window successfully triggered, data volume: %.0f", row["cnt"])
	case <-time.After(2 * time.Second):
		t.Fatal("The processing time session window should be triggered")
	}
}

func TestSQLSessionWindow_GroupedSession_MixedDevices(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               AVG(temperature) as avg_temp
        FROM stream
        GROUP BY deviceId, SessionWindow('200ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 8)
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		ch <- results
	})

	// Emit data for two different devices in interleaved pattern
	for i := 0; i < 5; i++ {
		ssql.Emit(map[string]any{"deviceId": "A", "temperature": float64(i), "timestamp": time.Now()})
		ssql.Emit(map[string]any{"deviceId": "B", "temperature": float64(i + 10), "timestamp": time.Now()})
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
	t.Parallel()
	ssql := streamsql.New()
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

	ch := make(chan []map[string]any, 8)
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		ch <- results
	})

	// Emit data for 4 different combinations: A|R1, B|R1, A|R2, B|R2
	for i := 0; i < 4; i++ {
		ssql.Emit(map[string]any{"deviceId": "A", "region": "R1", "temperature": float64(i), "timestamp": time.Now()})
		ssql.Emit(map[string]any{"deviceId": "B", "region": "R1", "temperature": float64(i + 10), "timestamp": time.Now()})
		ssql.Emit(map[string]any{"deviceId": "A", "region": "R2", "temperature": float64(i + 20), "timestamp": time.Now()})
		ssql.Emit(map[string]any{"deviceId": "B", "region": "R2", "temperature": float64(i + 30), "timestamp": time.Now()})
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

// TestSQLSessionWindow_EventTimeWithWithClause Test session windows that use the WITH clause to specify event time
func TestSQLSessionWindow_EventTimeWithWithClause(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt
        FROM stream
        GROUP BY deviceId, SessionWindow('300ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='200ms', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		ch <- results
	})

	// Using event time: Send data with event timestamps
	baseTime := time.Now().UnixMilli() - 5000 // 5 seconds ago as the reference time
	for i := 0; i < 5; i++ {
		eventTime := baseTime + int64(i*50) // One data sheet every 50ms
		ssql.Emit(map[string]any{
			"deviceId":  "sensor001",
			"eventTime": eventTime, // Event time field
		})
		time.Sleep(20 * time.Millisecond) // Processing time intervals are smaller
	}

	// Send data on an event that lasts longer than the session ends, and push the watermark
	// Session end time = baseTime + 200 + 300 = baseTime + 500
	// The event time > baseTime + 500 + maxOutOfOrderness(200) = baseTime + 700 data is required
	// Use different device IDs to avoid affecting the count of the current session
	ssql.Emit(map[string]any{
		"deviceId":  "sensor002",     // Using different device IDs does not affect sensor001's session
		"eventTime": baseTime + 2000, // Push the watermark
	})

	// Waiting for session timeout (event time session window triggered by watermark)
	time.Sleep(1 * time.Second)

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		row := res[0]
		assert.Equal(t, "sensor001", row["deviceId"])
		assert.Equal(t, float64(5), row["cnt"])
		t.Logf("Event time session window successfully triggered, data amount: %.0f", row["cnt"])
	case <-time.After(2 * time.Second):
		t.Fatal("The event time session window should be triggered")
	}
}

// TestSQLSessionWindow_ProcessingTimeWithoutWithClause When testing without the WITH clause, processing time is used by default
func TestSQLSessionWindow_ProcessingTimeWithoutWithClause(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt
        FROM stream
        GROUP BY deviceId, SessionWindow('300ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 4)
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		ch <- results
	})

	// Do not use the event time field; the processing time should be used
	for i := 0; i < 5; i++ {
		ssql.Emit(map[string]any{
			"deviceId": "sensor001",
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Waiting for session timeout (processing time session window based on system clock)
	time.Sleep(600 * time.Millisecond)

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		row := res[0]
		assert.Equal(t, "sensor001", row["deviceId"])
		assert.Equal(t, float64(5), row["cnt"])
		t.Logf("Processing time session window successfully triggered, data volume: %.0f", row["cnt"])
	case <-time.After(2 * time.Second):
		t.Fatal("The processing time session window should be triggered")
	}
}

// TestSQLSessionWindow_EventTimeWindowAlignment Test event time session window
func TestSQLSessionWindow_EventTimeWindowAlignment(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, SessionWindow('500ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='200ms', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 10)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// Using event time: Send data and validate session windows triggered based on event time
	baseTime := time.Now().UnixMilli() - 5000
	sessionTimeoutMs := int64(500)

	// Phase One: Send continuous data (event interval less than sessionTimeout)
	// These data should belong to the same session
	t.Log("Phase One: Sending Continuous Data (Same Session)")
	for i := 0; i < 5; i++ {
		eventTime := baseTime + int64(i*100) // One piece every 100ms, timeout less than 500ms
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Wait a while and let Watermark advance
	// End time of the first session = baseTime + 400 + 500 = baseTime + 900
	// Send event time > baseTime + 900 + maxOutOfOrderness(200) = baseTime + 1100 data
	// Only then can watermark > = baseTime + 900 trigger the first session
	time.Sleep(500 * time.Millisecond)

	// Send data to advance the watermark
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"eventTime":   baseTime + int64(1500),
		"temperature": 50.0,
	})
	time.Sleep(500 * time.Millisecond)

	// Stage Two: Send data with longer intervals (event intervals greater than sessionTimeout)
	// This should trigger a new session
	t.Log("Phase Two: Sending Data with Longer Intervals (New Sessions)")
	eventTime := baseTime + int64(2000) // 2-second interval, with a timeout greater than 500ms
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"eventTime":   eventTime,
		"temperature": 100.0,
	})

	// Continue sending continuous data (second session)
	for i := 0; i < 3; i++ {
		eventTime := baseTime + int64(2000+i*100)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(100 + i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Push the watermark to trigger a session
	// Session end time = baseTime + 400 + 500 = baseTime + 900
	// Send event time > baseTime + 900 + maxOutOfOrderness(200) = baseTime + 1100 data
	// Only then can watermark > = baseTime + 900
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"eventTime":   baseTime + int64(5000),
		"temperature": 200.0,
	})

	// Keep sending more data to ensure Watermark advances
	for i := 0; i < 5; i++ {
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   baseTime + int64(5000+i*200),
			"temperature": float64(200 + i),
		})
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for session trigger (watermark update interval 200ms, requires sufficient time)
	time.Sleep(3 * time.Second)

	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-ch:
			// Continue collecting results
		case <-timeout:
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]any, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	if windowResultsLen == 0 {
		t.Log("⚠ If the session window is not triggered, it may watermark not advanced to a sufficient position")
		t.Log("Note: The session window needs to be triggered when watermark > = session_end")
		t.Log("Session end time = last data time + timeout timeout")
	}
	require.Greater(t, windowResultsLen, 0, "应该至少触发一个会话窗口")

	// Validate the session window
	for i, window := range windowResultsCopy {
		if len(window) > 0 {
			row := window[0]
			start := row["start"].(int64)
			end := row["end"].(int64)
			cnt := row["cnt"].(float64)

			// Verify that the session window has data
			assert.Greater(t, cnt, 0.0, "会话窗口 %d 应该包含数据", i+1)

			// Verify that the time range of the session window is reasonable
			assert.Greater(t, end, start, "会话窗口 %d 的结束时间应该大于开始时间", i+1)

			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)
			sessionDurationMs := endMs - startMs

			assert.GreaterOrEqual(t, sessionDurationMs, sessionTimeoutMs,
				"会话窗口 %d 的持续时间应该至少等于会话超时时间", i+1)

			t.Logf("Session window %d: [%d, %d), cnt=%.0f, duration=%dms", i+1, startMs, endMs, cnt, sessionDurationMs)
		}
	}

	t.Logf("A total of %d session windows were triggered", windowResultsLen)
}

// TestSQLSessionWindow_WatermarkTriggerTiming Test the timing of triggering the session window Watermark
func TestSQLSessionWindow_WatermarkTriggerTiming(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, SessionWindow('500ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='200ms', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 10)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	baseTime := time.Now().UnixMilli() - 5000
	maxOutOfOrdernessMs := int64(200)
	sessionTimeoutMs := int64(500)

	// Send data and create sessions
	// The first data point: baseTime
	// Subsequent data: baseTime +100, baseTime +200, baseTime +300, baseTime +400
	// The session end time should be baseTime + 400 + 500 = baseTime + 900
	// When watermark > = baseTime + 900, the session should be triggered
	t.Log("Send data to create a session")
	for i := 0; i < 5; i++ {
		eventTime := baseTime + int64(i*100)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Calculate the end time of the session
	sessionEndTime := baseTime + int64(400) + sessionTimeoutMs // Last data time + timeout

	// Send data where the event time is exactly equal to sessionEndTime
	// watermark = maxEventTime - maxOutOfOrderness = sessionEndTime - 200
	// At this time, the watermark < sessionEndTime, and the session should not be triggered
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"eventTime":   sessionEndTime,
		"temperature": 100.0,
	})

	// Waiting for the watermark update
	time.Sleep(500 * time.Millisecond)

	// Send data with an event time exceeding sessionEndTime, advancing the watermark
	// watermark = maxEventTime - maxOutOfOrderness = (sessionEndTime + 500) - 200 = sessionEndTime + 300
	// At this point, watermark > = sessionEndTime, and the session should be triggered
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"eventTime":   sessionEndTime + 1000,
		"temperature": 200.0,
	})

	// Keep sending more data to ensure Watermark advances
	for i := 0; i < 3; i++ {
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   sessionEndTime + int64(1000+i*200),
			"temperature": float64(200 + i),
		})
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for session trigger (watermark update interval 200ms, requires sufficient time)
	time.Sleep(3 * time.Second)

	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-ch:
			// Continue collecting results
		case <-timeout:
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]any, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	if windowResultsLen == 0 {
		t.Log("⚠ If the session window is not triggered, it may watermark not advanced to a sufficient position")
		t.Log("Note: The session window needs to be triggered when watermark > = session_end")
		t.Log("Session end time = last data time + timeout timeout")
	}
	require.Greater(t, windowResultsLen, 0, "应该至少触发一个会话窗口")

	// Verify the timing of the session window trigger
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			row := firstWindow[0]
			start := row["start"].(int64)
			end := row["end"].(int64)
			cnt := row["cnt"].(float64)

			// The validation session window contains data
			assert.Greater(t, cnt, 0.0, "会话窗口应该包含数据")

			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)
			sessionDurationMs := endMs - startMs

			assert.GreaterOrEqual(t, sessionDurationMs, sessionTimeoutMs,
				"会话窗口的持续时间应该至少等于会话超时时间")

			t.Logf("✓ The session window is correctly triggered when watermark > = session_end")
			t.Logf("Session window: [%d, %d), cnt=%.0f, triggered maxEventTime >=%d",
				start, end, cnt, end+maxOutOfOrdernessMs)
		}
	}
}

// TestSQLSessionWindow_IdleSourceMechanism Test the Idle Source mechanism of the session window
func TestSQLSessionWindow_IdleSourceMechanism(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, SessionWindow('500ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='200ms', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 10)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	baseTime := time.Now().UnixMilli() - 5000

	// Send data and create sessions
	t.Log("Send data and create sessions")
	for i := 0; i < 5; i++ {
		eventTime := baseTime + int64(i*100)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Stop sending data and wait for the Idle Source mechanism to trigger
	t.Log("Stop sending data and wait for the Idle Source mechanism to trigger (IdleTimeout=2s)")
	time.Sleep(3 * time.Second)

	// Collect window results
	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-ch:
			// Continue collecting results
		case <-timeout:
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]any, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个会话窗口（即使数据源空闲）")

	if windowResultsLen > 0 {
		t.Logf("✓ The Idle Source session window mechanism worked properly, triggering %d sessions", windowResultsLen)
		for i, window := range windowResultsCopy {
			if len(window) > 0 {
				row := window[0]
				start := row["start"].(int64)
				end := row["end"].(int64)
				cnt := row["cnt"].(float64)
				t.Logf("Conversation %d: [%d, %d), cnt=%.0f", i+1, start, end, cnt)
			}
		}
	}
}
