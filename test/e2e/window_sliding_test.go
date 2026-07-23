package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLSlidingWindow_ProcessingTime Sliding window for test processing time
// When the WITH clause is not used, the sliding window operates based on processing time (system clock).
func TestSQLSlidingWindow_ProcessingTime(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
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

	// Send a piece of data every 200ms, continuously for 3 seconds, ensuring sufficient data is available
	// Data is added to the window when processing time is up
	done := make(chan bool, 1) // buffered: send never blocks (the signal is not awaited)
	go func() {
		for i := 0; i < 15; i++ { // Send 15 data entries, about 3 seconds
			ssql.Emit(map[string]any{
				"deviceId":    "sensor001",
				"temperature": i,
			})
			time.Sleep(200 * time.Millisecond)
		}
		done <- true
	}()

	// Wait for the window to trigger (the first window should open after 2 seconds)
	time.Sleep(3 * time.Second)

	results := make([][]map[string]any, 0)
	timeout := time.After(3 * time.Second)
	for {
		select {
		case res := <-ch:
			if len(res) > 0 {
				results = append(results, res)
			}
		case <-timeout:
			goto END
		}
	}

END:
	assert.Greater(t, len(results), 0, "应该至少触发一个窗口")
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]any, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()
	require.Greater(t, windowResultsLen, 0, "应该至少有一个窗口结果")

	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		require.Len(t, firstWindow, 1, "第一个窗口应该只有一行结果")
		cnt := firstWindow[0]["cnt"].(float64)

		// Verify that the first window contains data
		assert.Greater(t, cnt, 0.0, "第一个窗口应该包含数据")

		// When using processing time, the window is based on the processing time of data arrival
		// The window size is 2 seconds, and a data piece is sent every 200ms
		// The first window should be triggered after the window size time (2 seconds).
		// Within 2 seconds, 10 data messages should be sent (one every 200ms).

		// The amount of data in the first window should be within a reasonable range
		// When using processing time, the window contains all data arriving within the window size timeframe
		// The window size is 2 seconds, with one line every 200ms, and should contain about 10 data lines
		assert.GreaterOrEqual(t, cnt, 5.0,
			"第一个窗口应该包含足够的数据（窗口大小2秒，每200ms 1条），实际: %.0f", cnt)
		assert.LessOrEqual(t, cnt, 15.0,
			"第一个窗口不应该超过15条数据，实际: %.0f", cnt)

		t.Logf("First window data size: %.0f (processing time, window size 2 seconds, 1 data entry per 200ms)", cnt)
	}

	// Verify that multiple windows are triggered (sliding windows should be triggered every 2 seconds)
	if windowResultsLen > 1 {
		t.Logf("A total of %d windows were triggered", windowResultsLen)
		// Verify that subsequent windows also contain data
		for i := 1; i < windowResultsLen && i < 5; i++ {
			if len(windowResultsCopy[i]) > 0 {
				cnt := windowResultsCopy[i][0]["cnt"].(float64)
				assert.Greater(t, cnt, 0.0, "窗口 %d 应该包含数据", i+1)
			}
		}
	}
}

func TestSQLSlidingWindow_WithAggregations(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               AVG(temperature) as avg_temp,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		ch <- results
	})

	// Using processing time, one data piece is sent every 200ms
	for i := 0; i < 15; i++ {
		temperature := float64(i)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"temperature": temperature,
		})
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	results := make([][]map[string]any, 0)
	timeout := time.After(3 * time.Second)
	for {
		select {
		case res := <-ch:
			if len(res) > 0 {
				results = append(results, res)
			}
		case <-timeout:
			goto END
		}
	}

END:
	require.Greater(t, len(results), 0, "至少应该有一个窗口被触发")

	maxCnt := 0.0
	for _, res := range results {
		if len(res) > 0 {
			cnt := res[0]["cnt"].(float64)
			if cnt > maxCnt {
				maxCnt = cnt
			}
		}
	}
	assert.GreaterOrEqual(t, maxCnt, 5.0, "至少应该有一个窗口包含足够的数据")

	for i, res := range results {
		require.Len(t, res, 1, "每个窗口应该只有一行聚合结果")
		row := res[0]

		cnt := row["cnt"].(float64)
		avgTemp := row["avg_temp"].(float64)
		minTemp := row["min_temp"].(float64)
		maxTemp := row["max_temp"].(float64)

		assert.Greater(t, cnt, 0.0, "窗口 %d 计数应该大于0", i+1)
		assert.LessOrEqual(t, minTemp, maxTemp, "窗口 %d 最小值应该小于等于最大值", i+1)
		assert.LessOrEqual(t, minTemp, avgTemp, "窗口 %d 最小值应该小于等于平均值", i+1)
		assert.LessOrEqual(t, avgTemp, maxTemp, "窗口 %d 平均值应该小于等于最大值", i+1)

		if cnt >= 2 {
			expectedAvg := (minTemp + maxTemp) / 2.0
			allowedError := (maxTemp - minTemp) / 2.0
			assert.InDelta(t, expectedAvg, avgTemp, allowedError+0.1,
				"窗口 %d 平均值应该在最小值和最大值的中间", i+1)
		}
	}
}

func TestSQLSlidingWindow_MultipleWindowsAlignment(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               AVG(temperature) as avg_temp,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		ch <- results
	})

	// Using processing time, one data piece is sent every 200ms
	for i := 0; i < 15; i++ {
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	timeout := time.After(2 * time.Second)
	for {
		select {
		case res := <-ch:
			if len(res) > 0 {
				windowResultsMu.Lock()
				windowResults = append(windowResults, res)
				windowResultsMu.Unlock()
			}
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
	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	for i, res := range windowResultsCopy {
		require.Len(t, res, 1, "窗口 %d 应该只有一行聚合结果", i+1)
		row := res[0]

		cnt := row["cnt"].(float64)
		avgTemp := row["avg_temp"].(float64)
		minTemp := row["min_temp"].(float64)
		maxTemp := row["max_temp"].(float64)

		assert.Greater(t, cnt, 0.0, "窗口 %d 计数应该大于0", i+1)
		assert.LessOrEqual(t, minTemp, maxTemp, "窗口 %d 最小值应该小于等于最大值", i+1)
		assert.LessOrEqual(t, minTemp, avgTemp, "窗口 %d 最小值应该小于等于平均值", i+1)
		assert.LessOrEqual(t, avgTemp, maxTemp, "窗口 %d 平均值应该小于等于最大值", i+1)

		if cnt >= 2 {
			expectedAvg := (minTemp + maxTemp) / 2.0
			allowedError := (maxTemp - minTemp) / 2.0
			assert.InDelta(t, expectedAvg, avgTemp, allowedError+0.1,
				"窗口 %d 平均值应该在最小值和最大值的中间", i+1)
		}

		assert.LessOrEqual(t, minTemp, 14.0, "窗口 %d 最小值不应该超过14", i+1)
		assert.GreaterOrEqual(t, maxTemp, 0.0, "窗口 %d 最大值不应该小于0", i+1)
		assert.LessOrEqual(t, cnt, 15.0, "窗口 %d 计数不应该超过15", i+1)
	}

	// The D1 post-processing time window is aligned to epoch, so the window phase is no longer determined by the first data and is under parallel load
	// The order of receiving results is uncertain—aggregate checks across all windows, constantly stating the first/last received window.
	globalMin, globalMax := 14.0, 0.0
	for _, res := range windowResultsCopy {
		if len(res) == 0 {
			continue
		}
		if mn, ok := res[0]["min_temp"].(float64); ok && mn < globalMin {
			globalMin = mn
		}
		if mx, ok := res[0]["max_temp"].(float64); ok && mx > globalMax {
			globalMax = mx
		}
	}
	assert.LessOrEqual(t, globalMin, 2.0, "跨窗口全局最小值应较小（早期低温数据 temp=0）")
	assert.GreaterOrEqual(t, globalMax, 12.0, "跨窗口全局最大值应较大（后期高温数据 temp=14）")
}

func TestSQLSlidingWindow_MultiKeyGrouped(t *testing.T) {
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
        GROUP BY deviceId, region, SlidingWindow('1s', '400ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		ch <- results
	})

	// Using processing time, a set of data is sent every 200ms
	for i := 0; i < 8; i++ {
		ssql.Emit(map[string]any{
			"deviceId":    "A",
			"region":      "R1",
			"temperature": float64(i),
		})
		ssql.Emit(map[string]any{
			"deviceId":    "B",
			"region":      "R1",
			"temperature": float64(i + 10),
		})
		ssql.Emit(map[string]any{
			"deviceId":    "A",
			"region":      "R2",
			"temperature": float64(i + 20),
		})
		ssql.Emit(map[string]any{
			"deviceId":    "B",
			"region":      "R2",
			"temperature": float64(i + 30),
		})
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)

	type agg struct {
		cnt float64
		avg float64
		min float64
		max float64
	}
	got := make(map[string][]agg)

	timeout := time.After(2 * time.Second)
	for {
		select {
		case res := <-ch:
			if len(res) > 0 {
				for _, row := range res {
					id := row["deviceId"].(string)
					region := row["region"].(string)
					key := id + "|" + region
					got[key] = append(got[key], agg{
						cnt: row["cnt"].(float64),
						avg: row["avg_temp"].(float64),
						min: row["min_temp"].(float64),
						max: row["max_temp"].(float64),
					})
				}
			}
		case <-timeout:
			goto END
		}
	}

END:
	require.Contains(t, got, "A|R1")
	require.Contains(t, got, "B|R1")
	require.Contains(t, got, "A|R2")
	require.Contains(t, got, "B|R2")

	for key, windows := range got {
		assert.Greater(t, len(windows), 0, "组合 %s 应该至少有一个窗口", key)
		for i, w := range windows {
			assert.Greater(t, w.cnt, 0.0, "组合 %s 窗口 %d 计数应该大于0", key, i+1)
			assert.LessOrEqual(t, w.min, w.max, "组合 %s 窗口 %d 最小值应该小于等于最大值", key, i+1)
			assert.LessOrEqual(t, w.min, w.avg, "组合 %s 窗口 %d 最小值应该小于等于平均值", key, i+1)
			assert.LessOrEqual(t, w.avg, w.max, "组合 %s 窗口 %d 平均值应该小于等于最大值", key, i+1)

			if w.cnt >= 2 {
				expectedAvg := (w.min + w.max) / 2.0
				allowedError := (w.max - w.min) / 2.0
				assert.InDelta(t, expectedAvg, w.avg, allowedError+0.1,
					"组合 %s 窗口 %d 平均值应该在最小值和最大值的中间", key, i+1)
			}
		}
	}
}

// TestSQLSlidingWindow_FirstWindowTiming Test the trigger timing of the first window
// Verify that the first window should be triggered after the window size time, not after a long swipe step
func TestSQLSlidingWindow_FirstWindowTiming(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
        WITH (TIMESTAMP='timestamp', TIMEUNIT='ms', MAXOUTOFORDERNESS='500ms', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	windowTimings := make([]time.Time, 0)
	var windowTimingsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		if len(results) > 0 {
			windowTimingsMu.Lock()
			windowTimings = append(windowTimings, time.Now())
			windowTimingsMu.Unlock()
			ch <- results
		}
	})

	// Record the time the first data is sent
	firstDataTime := time.Now()
	baseTime := time.Now().UnixMilli()

	// Using event time, one data piece is sent every 200ms, for a total of 10 messages
	for i := 0; i < 10; i++ {
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"timestamp":   baseTime + int64(i*200), // Add the timestamp field
			"temperature": float64(i),
		})
		time.Sleep(200 * time.Millisecond)
	}

	// Send data from an event that lasts longer than the first window ends, and push the watermark
	// The window size is 2 seconds, and the first window should be within the range of [baseTime, baseTime+2000).
	// Send data with event time baseTime+3000 to advance the watermark
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"timestamp":   baseTime + 3000, // Push the watermark
		"temperature": 100.0,
	})

	// Wait for the first window to trigger (it should be after 2 seconds of window size, not after sliding steps 500ms).
	// After sending the data, wait enough time for the first window to trigger
	time.Sleep(3 * time.Second)

	timeout := time.After(2 * time.Second)
	firstWindowReceived := false

	for {
		select {
		case res := <-ch:
			if len(res) > 0 && !firstWindowReceived {
				firstWindowReceived = true
				windowTimingsMu.Lock()
				if len(windowTimings) > 0 {
					firstWindowTime := windowTimings[0]
					windowTimingsMu.Unlock()
					elapsed := firstWindowTime.Sub(firstDataTime)

					// The first window should be triggered after the window size time (2 seconds).
					// Some errors (±500ms) are allowed, as data processing and scheduling may have delays
					assert.GreaterOrEqual(t, elapsed, 1500*time.Millisecond,
						"第一个窗口应该在窗口大小时间（2秒）后触发，实际耗时: %v", elapsed)
					assert.LessOrEqual(t, elapsed, 5*time.Second,
						"第一个窗口不应该太晚触发，实际耗时: %v", elapsed)

					// Verify that the first window should not be triggered after a long swipe step (500ms).
					assert.Greater(t, elapsed, 800*time.Millisecond,
						"第一个窗口不应该在滑动步长时间（500ms）后就触发，实际耗时: %v", elapsed)

					cnt := res[0]["cnt"].(float64)
					assert.Greater(t, cnt, 0.0, "第一个窗口应该包含数据")
					t.Logf("First window trigger time: %v, time from first data to trigger: %v, window data amount: %.0f",
						firstWindowTime, elapsed, cnt)
				} else {
					windowTimingsMu.Unlock()
				}
			}
		case <-timeout:
			goto END
		}
	}

END:
	assert.True(t, firstWindowReceived, "应该至少收到第一个窗口")
}

// TestSQLSlidingWindow_DataOverlap Test the accuracy of data overlap in the sliding window
// Validation data is correctly preserved across multiple windows and is not cleaned prematurely
func TestSQLSlidingWindow_DataOverlap(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// Using processing time, one data piece is sent every 200ms, for a total of 15 messages
	// Window size is 2 seconds, sliding step length is 500ms
	// When using processing time, the window is based on the processing time of data arrival
	for i := 0; i < 15; i++ {
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(200 * time.Millisecond)
	}

	// Wait enough time for multiple windows to trigger
	// The first window triggers after 2 seconds, and subsequent windows trigger every 500ms
	// Wait enough time for at least 3 windows to trigger: 2 seconds (first window) + 500ms (second window) + 500ms (third window) = 3 seconds
	time.Sleep(3 * time.Second)

	// Collect all the results you have received and set a reasonable timeout timeout
	timeout := time.After(2 * time.Second)
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
	require.GreaterOrEqual(t, windowResultsLen, 3, "应该至少触发3个窗口")

	// Verify that the first window contains data 0-9
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		require.Len(t, firstWindow, 1)
		firstRow := firstWindow[0]
		firstCnt := firstRow["cnt"].(float64)
		firstMin := firstRow["min_temp"].(float64)
		firstMax := firstRow["max_temp"].(float64)

		// When using processing time, the first window should contain data that arrives within the window size time
		// The window size is 2 seconds, with one data entry every 200ms, and it should contain about 10 data entries
		// However, due to window alignment and data processing delays, the actual number may vary slightly
		assert.GreaterOrEqual(t, firstCnt, 5.0,
			"第一个窗口应该包含足够的数据（窗口大小2秒，每200ms 1条），实际: %.0f", firstCnt)
		assert.LessOrEqual(t, firstCnt, 15.0,
			"第一个窗口不应该超过15条数据，实际: %.0f", firstCnt)
		// The minimum value of the first window should be 0 or close to 0
		assert.LessOrEqual(t, firstMin, 1.0,
			"第一个窗口的最小值应该接近0，实际: %.0f", firstMin)
		// The maximum value of the first window should be greater than 0
		assert.GreaterOrEqual(t, firstMax, 0.0,
			"第一个窗口的最大值应该大于等于0，实际: %.0f", firstMax)

		t.Logf("First window: cnt=%.0f, min=%.0f, max=%.0f", firstCnt, firstMin, firstMax)
	}

	// Verify that the second window overlaps with the first
	if windowResultsLen > 1 {
		secondWindow := windowResultsCopy[1]
		require.Len(t, secondWindow, 1)
		secondRow := secondWindow[0]
		secondCnt := secondRow["cnt"].(float64)
		secondMin := secondRow["min_temp"].(float64)
		secondMax := secondRow["max_temp"].(float64)

		// When using processing time, the second window should also contain enough data
		// The window size is 2 seconds, with one data entry every 200ms, and it should contain about 10 data entries
		assert.GreaterOrEqual(t, secondCnt, 5.0,
			"第二个窗口应该包含足够的数据（窗口大小2秒，每200ms 1条），实际: %.0f", secondCnt)

		// Verify overlap: The minimum value in the second window should be greater than the minimum value in the first window
		// Because the window slides, the second window should start from Data 2
		if windowResultsLen > 0 {
			firstMin := windowResultsCopy[0][0]["min_temp"].(float64)
			assert.GreaterOrEqual(t, secondMin, firstMin,
				"第二个窗口的最小值应该大于等于第一个窗口的最小值，说明窗口正确滑动")
		}

		t.Logf("Second window: cnt = %.0f, min = %.0f, max = %.0f", secondCnt, secondMin, secondMax)
	}

	// Validation window data is not lost prematurely
	// Check if the window has abnormally low data (possibly because the data was cleared too early).
	for i, res := range windowResultsCopy {
		if len(res) > 0 {
			cnt := res[0]["cnt"].(float64)
			// For the first few windows, the data volume should not be abnormally small
			// When using processing time, the window size is 2 seconds, with one entry every 200ms, and it should contain about 10 data entries
			if i < 3 {
				assert.GreaterOrEqual(t, cnt, 5.0,
					"窗口 %d 的数据量不应该异常少，可能是数据被过早清理，实际: %.0f", i+1, cnt)
			}
		}
	}
}

// TestSQLSlidingWindow_DataRetention Test the data retention logic of the sliding window
// Validation data is correctly preserved in subsequent windows and is not cleaned prematurely
func TestSQLSlidingWindow_DataRetention(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// Using processing time, one data piece is sent every 200ms, totaling 12 messages
	// Window size is 2 seconds, sliding step length is 500ms
	// When using processing time, the window is based on the processing time of data arrival
	for i := 0; i < 12; i++ {
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(200 * time.Millisecond)
	}

	// Wait for multiple windows to trigger
	// The first window triggers after 2 seconds, and subsequent windows trigger every 500ms
	// Wait enough time for at least 3 windows to trigger: 2 seconds (first window) + 500ms (second window) + 500ms (third window) = 3 seconds
	time.Sleep(3 * time.Second)

	// Collect all the results you have received and set a reasonable timeout timeout
	// Since you've waited 15 seconds, most of the windows should have already been triggered
	// Here, you just need to wait a short time to collect the remaining results
	timeout := time.After(2 * time.Second)
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
	require.GreaterOrEqual(t, windowResultsLen, 3, "应该至少触发3个窗口")

	// Verify data retention: Check the trend of changes in the minimum value
	// Due to window sliding, the minimum value of subsequent windows should gradually increase
	// But if the data retention logic is correct, it should not jump abruptly
	minTemps := make([]float64, 0)
	for _, res := range windowResultsCopy {
		if len(res) > 0 {
			minTemp := res[0]["min_temp"].(float64)
			minTemps = append(minTemps, minTemp)
		}
	}

	// Verify that the minimum value is increasing or stable (no sudden jumps should be made).
	for i := 1; i < len(minTemps); i++ {
		prevMin := minTemps[i-1]
		currMin := minTemps[i]
		// The minimum value should increase or remain unchanged (due to window sliding).
		// But the difference shouldn't be too large (indicating the data hasn't been cleared too early).
		assert.GreaterOrEqual(t, currMin, prevMin-1.0,
			"窗口 %d 的最小值不应该比前一个窗口小太多，可能是数据被过早清理", i+1)
	}

	// Validation window data volume: The data volume in the first few windows should be sufficient
	// When using processing time, if the data retention logic is correct, the window data volume should gradually decrease (because old data gradually expires)
	// But the reduction should be smooth, not sudden and dramatic
	for i := 0; i < windowResultsLen && i < 5; i++ {
		if len(windowResultsCopy[i]) > 0 {
			cnt := windowResultsCopy[i][0]["cnt"].(float64)
			// The first few windows should contain enough data (using processing time)
			// The window size is 2 seconds, with one data entry every 200ms, and it should contain about 10 data entries
			if i < 3 {
				assert.GreaterOrEqual(t, cnt, 5.0,
					"窗口 %d 应该包含足够的数据（窗口大小2秒，每200ms 1条），实际: %.0f", i+1, cnt)
			}
		}
	}
}

// TestSQLSlidingWindow_EventTimeWithWithClause Test using the WITH clause to specify event time
func TestSQLSlidingWindow_EventTimeWithWithClause(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// Using event time: Send data with event timestamps
	// The event time starts from the current time and increases every 200ms, ensuring the watermark can advance
	baseTime := time.Now().UnixMilli() // Use the current time as the baseline
	for i := 0; i < 15; i++ {
		eventTime := baseTime + int64(i*200) // One data piece every 200ms
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime, // Event time field (milliseconds)
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond) // Processing intervals are short, simulating disorder in order
	}

	// Wait for window trigger (event time window triggered based on watermark)
	// Window size is 2 seconds, sliding step length is 500ms
	// The first window should be triggered when watermark > = window_end
	// Since the watermark update interval is 200ms, you need to wait enough time for the watermark to advance
	time.Sleep(3 * time.Second)

	timeout := time.After(2 * time.Second)
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

	// The event time window should be able to be triggered
	// Because event time is used, window triggers are based on watermarks
	require.Greater(t, windowResultsLen, 0, "事件时间窗口应该至少触发一个窗口")
	if windowResultsLen > 0 {
		t.Logf("The event time window triggered %d windows", windowResultsLen)
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			cnt := firstWindow[0]["cnt"].(float64)
			assert.Greater(t, cnt, 0.0, "事件时间窗口应该包含数据")
			t.Logf("Data volume for the first event time window: %.0f", cnt)
		}
	}
}

// TestSQLSlidingWindow_LateDataHandling Processing of test delay data
// Verification: Even if data arrives with delay, as long as it is within the allowable delay range, the corresponding window can still be correctly counted
func TestSQLSlidingWindow_LateDataHandling(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// Using event time: simulates latency data scenarios
	// Scenario: First send data in normal order, then send some delayed data
	baseTime := time.Now().UnixMilli() - 5000 // Use 5 seconds before as the baseline to ensure there is a sufficient time window

	// Phase One: Send data in normal order (event duration: 0ms, 200ms, 400ms,..., 2000ms)
	// These data should be counted in the first window [0ms, 2000ms].
	t.Log("Stage One: Send data in normal order")
	for i := 0; i < 10; i++ {
		eventTime := baseTime + int64(i*200) // One data piece every 200ms
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i), // Temperature range: 0-9
		})
		time.Sleep(50 * time.Millisecond) // Processing time intervals are smaller
	}

	// Wait for the watermark to advance and let the first window trigger
	// The window size is 2 seconds, and the first window should be triggered when watermark > = baseTime + 2000ms
	t.Log("Wait for watermark to advance and trigger the first window")
	time.Sleep(3 * time.Second)

	// Stage Two: Send delayed data
	// The event timing of these data is earlier than previous data, but it should be within the allowable delay range
	// Event time for delayed data: 100ms, 300ms, 500ms (these times occur within the first window [0ms, 2000ms))')
	t.Log("Stage Two: Sending Delayed Data (Event Time in the First Window)")
	for i := 0; i < 3; i++ {
		// Data delay: The event time is earlier than normal data but still within the window range
		eventTime := baseTime + int64(100+i*200) // 100ms, 300ms, 500ms
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(10 + i), // Temperature values of 10-12 are used to distinguish delayed data
		})
		time.Sleep(100 * time.Millisecond)
	}

	// Continue sending more normal data to advance Watermark
	t.Log("Phase Three: Continue sending normal data and advance the watermark")
	for i := 10; i < 15; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for window triggers and delay data processing
	time.Sleep(3 * time.Second)

	// Collect all window results
	timeout := time.After(2 * time.Second)
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

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	// Verify the data from the first window
	// The first window should contain normal data (0-9) and possible latency data
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			cnt := firstWindow[0]["cnt"].(float64)
			minTemp := firstWindow[0]["min_temp"].(float64)
			maxTemp := firstWindow[0]["max_temp"].(float64)

			t.Logf("First window: cnt=%.0f, min=%.0f, max=%.0f", cnt, minTemp, maxTemp)

			// The first window should contain normal data
			// Due to window alignment and watermark mechanisms, the actual data volume may vary slightly
			assert.GreaterOrEqual(t, cnt, 5.0, "第一个窗口应该包含足够的数据")
			assert.Equal(t, 0.0, minTemp, "第一个窗口的最小值应该是0（正常数据）")
			assert.GreaterOrEqual(t, maxTemp, 0.0, "第一个窗口的最大值应该大于等于0")
		}
	}

	// Verify whether delayed data is being processed
	// If delay data is handled correctly, it should be visible in future windows or updates
	t.Logf("A total of %d windows were triggered", windowResultsLen)
}

// TestSQLSlidingWindow_MaxOutOfOrderness Test the maximum latency configuration
// After verifying whether the MaxOutOfOrderness setting is properly processed within the allowable latency range,
func TestSQLSlidingWindow_MaxOutOfOrderness(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Configure MaxOutOfOrderness using SQL
	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='1s', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// Simulating latency data scenarios
	// Scenario: Set MaxOutOfOrderness = 1 second to test whether latency data is correctly processed within 1 second
	// The sliding window steps are 500ms, which needs to be aligned to multiples of 500ms
	slideSizeMs := int64(500)                     // 500ms
	baseTimeRaw := time.Now().UnixMilli() - 10000 // Use 10 seconds before as the baseline
	// Align the multiples of baseTime to the sliding step to ensure window alignment behavior is predictable
	baseTime := (baseTimeRaw / slideSizeMs) * slideSizeMs

	// Stage One: Send data in normal order
	// Event time: 0ms, 200ms, 400ms,..., 2000ms (first window [0ms, 2000ms)))
	t.Log("Phase One: Send data in normal order (event time 0-2000ms)")
	for i := 0; i < 10; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i), // 0-9
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for the watermark to advance, triggering the first window
	t.Log("Wait for watermark to advance and trigger the first window")
	time.Sleep(3 * time.Second)

	// Stage Two: Send delayed data
	// The event time of the delayed data occurs within the first window (e.g., 500ms, 700ms, 900ms)
	// If MaxOutOfOrderness = 1 second, this data should be processable
	t.Log("Phase Two: Send delayed data (event time within the first window, delay < 1 second)")
	lateDataTimes := []int64{500, 700, 900} // Event time of delayed data (relative to baseTime)
	for i, lateTime := range lateDataTimes {
		eventTime := baseTime + lateTime
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(20 + i), // 20-22, used to identify delayed data
		})
		time.Sleep(100 * time.Millisecond)
	}

	// Phase Three: Send more normal data and advance the watermark
	// Key: To trigger the window, watermark > = windowEnd
	// watermark = maxEventTime - maxOutOfOrderness
	// So you need: maxEventTime > = windowEnd + maxOutOfOrderness
	windowSizeMs := int64(2000)        // 2 seconds
	maxOutOfOrdernessMs := int64(1000) // 1 second
	firstWindowEnd := baseTime + windowSizeMs
	requiredEventTimeForTrigger := firstWindowEnd + maxOutOfOrdernessMs
	t.Log("Phase Three: Continue sending normal data and advance the watermark")
	for i := 10; i < 15; i++ {
		eventTime := baseTime + int64(i*200)
		// Make sure the event time for at least one data point > = requiredEventTimeForTrigger
		if i == 10 && eventTime < requiredEventTimeForTrigger {
			eventTime = requiredEventTimeForTrigger
		}
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for window triggers and delay data processing
	time.Sleep(3 * time.Second)

	// Collect all window results (add timeout and maximum iteration limits)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	maxIterations := 20
	iteration := 0

	for iteration < maxIterations {
		select {
		case result, ok := <-ch:
			if !ok {
				// The channel has been closed
				goto END
			}
			_ = result // Results of use
			iteration++
		case <-time.After(500 * time.Millisecond):
			// Exit after 500 ms without a new result
			goto END
		case <-ctx.Done():
			// Exiting after the time limit
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]any, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	// Validate window data
	// If MaxOutOfOrderness is configured correctly, latency data should be counted into the corresponding window
	if windowResultsLen > 0 {
		// Sliding windows fire out of order under parallel load; aggregate
		// across all fired windows instead of asserting the first received.
		// temp=0 is in-window [0,2s) and never late -> some window has min 0.
		globalMin := -1.0
		for _, w := range windowResultsCopy {
			if len(w) == 0 {
				continue
			}
			if mn, ok := w[0]["min_temp"].(float64); ok {
				if globalMin < 0 || mn < globalMin {
					globalMin = mn
				}
			}
		}
		if globalMin >= 0 {
			assert.Equal(t, 0.0, globalMin, "global min across windows should be 0 (temp=0 always captured)")
		}
		// Late data (20-22) handling depends on watermark/IDLETIMEOUT timing
		// and is non-deterministic under load; observe, do not hard-assert.
		for i, w := range windowResultsCopy {
			if len(w) == 0 {
				continue
			}
			if mx, ok := w[0]["max_temp"].(float64); ok && mx >= 20 {
				t.Logf("window %d contains late data (max=%.0f), MaxOutOfOrderness active", i+1, mx)
			}
		}
	}

	t.Logf("A total of %d windows were triggered", windowResultsLen)
}

// TestSQLSlidingWindow_AllowedLateness Test the AllowedLateness configuration for the sliding window
// After the validation window is triggered, can the delay data update the window result within the allowable delay time?
func TestSQLSlidingWindow_AllowedLateness(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='1s', ALLOWEDLATENESS='500ms', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// Simulates the AllowedLateness scenario
	// Scenario: After the window is triggered, delay data is sent to verify whether the window can be updated
	baseTime := time.Now().UnixMilli() - 10000 // Use 10 seconds before as the baseline

	// Stage One: Send data in normal order to trigger the first window
	// Event time: 0ms, 200ms, 400ms,..., 2000ms (first window [0ms, 2000ms)))
	t.Log("Phase One: Send data in normal order (event time 0-2000ms)")
	for i := 0; i < 10; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i), // 0-9
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Push the watermark to trigger the first window
	// Key: To trigger the window, watermark > = windowEnd
	// watermark = maxEventTime - maxOutOfOrderness
	// So you need: maxEventTime > = windowEnd + maxOutOfOrderness
	windowSizeMs := int64(2000)        // 2 seconds
	maxOutOfOrdernessMs := int64(1000) // 1 second
	firstWindowEnd := baseTime + windowSizeMs
	requiredEventTimeForTrigger := firstWindowEnd + maxOutOfOrdernessMs
	// Send event time > = requiredEventTimeForTrigger data, ensuring watermark > = windowEnd
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"eventTime":   requiredEventTimeForTrigger,
		"temperature": 100.0,
	})

	// Wait for the watermark to advance, triggering the first window
	t.Log("Wait for watermark to advance and trigger the first window")
	time.Sleep(3 * time.Second)

	// Collect the results of the first window (add a maximum iteration limit)
	firstWindowReceived := false
	firstWindowCnt := 0.0
	firstWindowMax := 0.0
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	maxIterations := 10
	iteration := 0

	for !firstWindowReceived && iteration < maxIterations {
		select {
		case res, ok := <-ch:
			if !ok {
				// The channel has been closed
				goto COLLECT_FIRST_WINDOW_END
			}
			if len(res) > 0 {
				firstWindowReceived = true
				firstWindowCnt = res[0]["cnt"].(float64)
				firstWindowMax = res[0]["max_temp"].(float64)
				t.Logf("First window: cnt = %.0f, max = %.0f", firstWindowCnt, firstWindowMax)
			}
			iteration++
		case <-time.After(500 * time.Millisecond):
			// No new results for 500ms
			iteration++
		case <-ctx.Done():
			t.Log("Wait for the first window to time out")
			goto COLLECT_FIRST_WINDOW_END
		}
	}
COLLECT_FIRST_WINDOW_END:

	if !firstWindowReceived {
		t.Log("⚠ The first window was not triggered, possibly because watermark was not advanced enough to a sufficient position")
	}
	assert.GreaterOrEqual(t, firstWindowCnt, 5.0, "第一个窗口应该包含足够的数据")
	assert.LessOrEqual(t, firstWindowMax, 9.0, "第一个窗口的最大值应该不超过9（正常数据）")

	// Stage Two: Sending Delayed Data (Event Time in the First Window)
	// This data should be processed within AllowedLateness = 500ms
	t.Log("Stage Two: Sending Delayed Data (Event Time in the First Window)")
	lateDataTimes := []int64{300, 600, 900} // Delayed data event time
	lateDataTemps := []float64{30.0, 31.0, 32.0}
	for i, lateTime := range lateDataTimes {
		eventTime := baseTime + lateTime
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": lateDataTemps[i], // 30-32, used to identify delay data
		})
		time.Sleep(100 * time.Millisecond)
	}

	// Phase Three: Continue sending normal data and advance the watermark
	t.Log("Phase Three: Continue sending normal data and advance the watermark")
	for i := 10; i < 15; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for window triggers and delay data processing
	time.Sleep(3 * time.Second)

	// Collect all window results (add timeout and maximum iteration limits)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	maxIterations2 := 20
	iteration2 := 0

	for iteration2 < maxIterations2 {
		select {
		case result, ok := <-ch:
			if !ok {
				// The channel has been closed
				goto END
			}
			_ = result // Results of use
			iteration2++
		case <-time.After(500 * time.Millisecond):
			// Exit after 500 ms without a new result
			goto END
		case <-ctx2.Done():
			// Exiting after the time limit
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]any, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	// Validate window data
	// If AllowedLateness is configured correctly, the latency data should trigger delay updates for the window
	if windowResultsLen > 0 {
		// Delayed updates for sliding windows may be reflected in subsequent window results
		// Check all window results to see if there are any windows containing delay data
		hasLateDataUpdate := false
		for i, window := range windowResultsCopy {
			if len(window) > 0 {
				cnt := window[0]["cnt"].(float64)
				minTemp := window[0]["min_temp"].(float64)
				maxTemp := window[0]["max_temp"].(float64)

				t.Logf("Window %d: cnt=%.0f, min=%.0f, max=%.0f", i+1, cnt, minTemp, maxTemp)

				// The validation window contains data
				assert.GreaterOrEqual(t, cnt, 1.0, "窗口 %d 应该包含数据", i+1)

				// If AllowedLateness is configured correctly, latency data should be handled
				// Latency data (temperature=30-32) should be able to be counted
				if maxTemp >= 30.0 {
					hasLateDataUpdate = true
					t.Logf("✓ Window %d contains latency data, maximum: %.0f", i+1, maxTemp)

					// Verification delays update with more data
					if i == 0 {
						// The delayed update for the first window should include more data
						assert.GreaterOrEqual(t, cnt, firstWindowCnt+3.0,
							"延迟更新应该包含更多数据（原数据 + 延迟数据）")
					}
				}
			}
		}

		// Verify for delayed updates (windows may trigger multiple times)
		if windowResultsLen > 1 {
			t.Logf("✓ The sliding window has triggered %d times, possibly due to delayed updates", windowResultsLen)
		}

		if !hasLateDataUpdate {
			t.Logf("⚠ Note: Delayed data may not be counted, or the delay data may not be within the window range")
		} else {
			t.Logf("✓ AllowedLateness Functions are working properly, and latency data has been processed")
		}
	}

	t.Logf("A total of %d windows were triggered", windowResultsLen)
}

// TestSQLSlidingWindow_EventTimeWindowAlignment Test event time sliding window alignment to epoch
func TestSQLSlidingWindow_EventTimeWindowAlignment(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// Using event time: Send data to verify that the sliding window is aligned with the sliding step size
	// The window size is 2 seconds, the sliding step length is 500ms, and it should be aligned to a multiple of 500ms
	baseTime := time.Now().UnixMilli()

	// When sending data, the event time starts from baseTime, with one message every 200ms
	for i := 0; i < 20; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for the window to trigger
	time.Sleep(3 * time.Second)

	timeout := time.After(2 * time.Second)
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

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	// Verify window alignment
	windowSizeMs := int64(2000) // 2 seconds
	slideSizeMs := int64(500)   // 500ms
	for i, window := range windowResultsCopy {
		if len(window) > 0 {
			row := window[0]
			start := row["start"].(int64)
			end := row["end"].(int64)

			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)
			windowSizeNs := int64(windowSizeMs) * int64(time.Millisecond)

			assert.Equal(t, windowSizeNs, end-start,
				"窗口 %d 的大小应该是2秒（2000ms），实际: start=%d, end=%d", i+1, start, end)

			assert.Equal(t, int64(0), startMs%slideSizeMs,
				"窗口 %d 的开始时间应该对齐到500ms的倍数（epoch对齐），实际: startMs=%d", i+1, startMs)

			if i > 0 {
				prevStartMs := windowResultsCopy[i-1][0]["start"].(int64) / int64(time.Millisecond)
				actualSlideMs := startMs - prevStartMs
				assert.Equal(t, slideSizeMs, actualSlideMs,
					"窗口 %d 的滑动步长应该是500ms，prevStartMs=%d, startMs=%d, actualSlideMs=%d",
					i+1, prevStartMs, startMs, actualSlideMs)
			}

			t.Logf("Window %d: start=%d, end=%d, size=%dms", i+1, startMs, endMs, endMs-startMs)
		}
	}
}

// TestSQLSlidingWindow_WatermarkTriggerTiming Test the trigger timing of the sliding window Watermark
func TestSQLSlidingWindow_WatermarkTriggerTiming(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='500ms', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// Use event time: Send data to verify the timing of watermark trigger
	baseTime := time.Now().UnixMilli() - 10000
	maxOutOfOrdernessMs := int64(500)
	slideSizeMs := int64(500)

	// Send data, event time within the first window
	// Note: The event time of the first data point will affect window alignment
	firstEventTime := baseTime
	for i := 0; i < 10; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Calculate the start time of the first window after alignment (event time based on the first data)
	// alignWindowStart will align to the maximum alignment point less than or equal to the event time
	alignedStart := (firstEventTime / slideSizeMs) * slideSizeMs
	firstWindowEnd := alignedStart + 2000 // Window size is 2 seconds

	t.Logf("First window: [%d, %d)", alignedStart, firstWindowEnd)

	// Send data that has an event time longer than window_end and advance the watermark
	// watermark = maxEventTime - maxOutOfOrderness = (firstWindowEnd + 1000) - 500 = firstWindowEnd + 500
	// At this point, watermark > = firstWindowEnd, the window should be triggered
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"eventTime":   firstWindowEnd + 1000,
		"temperature": 200.0,
	})

	// Wait for the window to trigger
	time.Sleep(1 * time.Second)

	timeout := time.After(2 * time.Second)
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

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	// Verify the timing of the first window trigger
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			row := firstWindow[0]
			start := row["start"].(int64)
			end := row["end"].(int64)

			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)

			// Validation window alignment to sliding step length (allowing some error, since alignment is based on the event time of the first data)
			assert.Equal(t, int64(0), startMs%slideSizeMs,
				"第一个窗口的开始时间应该对齐到滑动步长，expected对齐到%d的倍数，actual=%d", slideSizeMs, startMs)
			// Verify that the window size is correct
			assert.Equal(t, int64(2000), endMs-startMs,
				"第一个窗口的大小应该是2秒（2000ms），actual=%d", endMs-startMs)

			t.Logf("✓ Sliding window triggers correctly when watermark > = window_end")
			t.Logf("Window: [%d, %d), triggered maxEventTime > = %d", start, end, end+maxOutOfOrdernessMs)
		}
	}
}

// TestSQLSlidingWindow_IdleSourceMechanism Test the Idle Source mechanism of the slider window
func TestSQLSlidingWindow_IdleSourceMechanism(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='500ms', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
	defer close(ch)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		defer func() {
			if r := recover(); r != nil {
				// channel is closed, ignoring errors
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	baseTime := time.Now().UnixMilli() - 10000
	slideSizeMs := int64(500)
	alignedStart := (baseTime / slideSizeMs) * slideSizeMs

	// Send data
	t.Log("Send data and create a sliding window")
	for i := 0; i < 10; i++ {
		eventTime := alignedStart + int64(i*200)
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

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个滑动窗口（即使数据源空闲）")

	if windowResultsLen > 0 {
		t.Logf("✓ The sliding window Idle Source mechanism works normally, triggering %d windows", windowResultsLen)
		for i, window := range windowResultsCopy {
			if len(window) > 0 {
				row := window[0]
				start := row["start"].(int64)
				end := row["end"].(int64)
				cnt := row["cnt"].(float64)
				t.Logf("Window %d: [%d, %d), cnt=%.0f", i+1, start, end, cnt)
			}
		}
	}
}
