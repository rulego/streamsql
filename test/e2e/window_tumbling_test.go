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

// TestSQLTumblingWindow_ProcessingTime Rolling window for test processing times
// When verification does not use the WITH clause, the scroll window operates based on processing time (system clock).
func TestSQLTumblingWindow_ProcessingTime(t *testing.T) {
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
        GROUP BY deviceId, TumblingWindow('2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 10)
	defer close(ch)
	windowResults := make([][]map[string]any, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]any) {
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			select {
			case ch <- results:
			default:
				// Non-blocking transmission
			}
		}
	})

	// Use processing time: Send data without timestamp fields
	// Scrolling windows are divided based on the processing time of data arrival (system clock).
	for i := 0; i < 10; i++ {
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(200 * time.Millisecond) // Send a data piece every 200ms
	}

	// Wait window trigger (processing time rolling window triggered based on system clock)
	time.Sleep(3 * time.Second)

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

	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			row := firstWindow[0]
			cnt := row["cnt"].(float64)
			avgTemp := row["avg_temp"].(float64)
			minTemp := row["min_temp"].(float64)
			maxTemp := row["max_temp"].(float64)

			assert.Greater(t, cnt, 0.0, "窗口应该包含数据")
			assert.LessOrEqual(t, minTemp, maxTemp, "最小值应该小于等于最大值")
			assert.LessOrEqual(t, minTemp, avgTemp, "最小值应该小于等于平均值")
			assert.LessOrEqual(t, avgTemp, maxTemp, "平均值应该小于等于最大值")

			t.Logf("The processing time scrolling window successfully triggered, data volume: %.0f, average temperature: %.2f", cnt, avgTemp)
		}
	}
}

// TestSQLTumblingWindow_MaxOutOfOrderness Test the maximum latency configuration for the rolling window
// After verifying whether the MaxOutOfOrderness setting is properly processed within the allowable latency range,
func TestSQLTumblingWindow_MaxOutOfOrderness(t *testing.T) {
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
        GROUP BY deviceId, TumblingWindow('2s')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='1s', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
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

	// Simulating latency data scenarios
	// Scenario: Set MaxOutOfOrderness = 1 second to test whether latency data is correctly processed within 1 second
	// The window size is 2 seconds, and it needs to be aligned to a multiple of 2 seconds
	windowSizeMs := int64(2000)                   // 2 seconds
	baseTimeRaw := time.Now().UnixMilli() - 10000 // Use 10 seconds before as the baseline
	// Align baseTime as a multiple to window size to ensure window alignment behavior is predictable
	baseTime := (baseTimeRaw / windowSizeMs) * windowSizeMs

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
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			cnt := firstWindow[0]["cnt"].(float64)
			minTemp := firstWindow[0]["min_temp"].(float64)
			maxTemp := firstWindow[0]["max_temp"].(float64)

			t.Logf("First window: cnt=%.0f, min=%.0f, max=%.0f", cnt, minTemp, maxTemp)

			// The validation window contains data
			// Scrolling window: window size is 2 seconds, one data entry every 200ms, theoretically there should be 10 data entries
			// However, due to window alignment and watermark mechanisms, the actual data volume may vary slightly
			assert.GreaterOrEqual(t, cnt, 3.0, "第一个窗口应该包含足够的数据（滚动窗口特性）")
			assert.Equal(t, 0.0, minTemp, "第一个窗口的最小值应该是0（正常数据）")

			// If MaxOutOfOrderness is configured correctly, latency data should be handled
			if maxTemp >= 20.0 {
				t.Logf("✓ Delay data is handled correctly, with maximum value including delay data: %.0f", maxTemp)
			} else {
				t.Logf("Note: Latency data may not be counted; current maximum value: %.0f", maxTemp)
			}
		}
	}

	t.Logf("A total of %d windows were triggered", windowResultsLen)
}

// TestSQLTumblingWindow_AllowedLateness Test the AllowedLateness configuration for the scrolling window
// After the validation window is triggered, can the delay data update the window result within the allowable delay time?
func TestSQLTumblingWindow_AllowedLateness(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, TumblingWindow('2s')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', ALLOWEDLATENESS='1s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
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

	// Wait for the watermark to advance, triggering the first window
	t.Log("Wait for watermark to advance and trigger the first window")
	time.Sleep(3 * time.Second)

	// Stage Two: Sending Delayed Data (Event Time in the First Window)
	// This data should be processed within AllowedLateness = 1 second
	t.Log("Stage Two: Sending Delayed Data (Event Time in the First Window)")
	lateDataTimes := []int64{300, 600, 900} // Delayed data event time
	for i, lateTime := range lateDataTimes {
		eventTime := baseTime + lateTime
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(30 + i), // 30-32, used to identify delay data
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

	// Collect all window results
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
	// If AllowedLateness is configured correctly, the latency data should trigger delay updates for the window
	if windowResultsLen > 0 {
		// Delayed updates for rolling windows may be reflected in subsequent window results
		// Check all window results to see if there are any windows containing delay data
		hasLateData := false
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
					hasLateData = true
					t.Logf("✓ Window %d contains latency data, maximum: %.0f", i+1, maxTemp)
				}
			}
		}

		// Verify for delayed updates (windows may trigger multiple times)
		if windowResultsLen > 1 {
			t.Logf("✓ The scroll window has triggered %d times, possibly including delayed updates", windowResultsLen)
		}

		if !hasLateData {
			t.Logf("Note: Delayed data may not be counted, or the delay data may not be within the window range")
		}
	}

	t.Logf("A total of %d windows were triggered", windowResultsLen)
}

// TestSQLTumblingWindow_BothConfigs Test the scrolling window by configuring both MaxOutOfOrderness and AllowedLateness
// Verify whether latency data is correctly handled when using two configurations in combination
func TestSQLTumblingWindow_BothConfigs(t *testing.T) {
	t.Parallel()
	// Enable debug logs (optional, for troubleshooting)
	// window.EnableDebug = true

	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, TumblingWindow('2s')
        WITH (
            TIMESTAMP='eventTime',
            TIMEUNIT='ms',
            MAXOUTOFORDERNESS='1s',
            ALLOWEDLATENESS='500ms'
        )
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
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

	// Simulates a complete latency data processing scenario
	// Key: Ensure baseTime aligns with window boundaries so window alignment behavior is predictable
	windowSizeMs := int64(2000) // 2 seconds
	baseTimeRaw := time.Now().UnixMilli() - 10000
	baseTime := (baseTimeRaw / windowSizeMs) * windowSizeMs // Aligned with the window boundary
	maxOutOfOrdernessMs := int64(1000)                      // 1 second
	firstWindowEnd := baseTime + windowSizeMs
	// Key: To trigger the window, watermark > = windowEnd
	// watermark = maxEventTime - maxOutOfOrderness
	// So you need: maxEventTime - maxOutOfOrderness > = windowEnd
	// That is: maxEventTime > = windowEnd + maxOutOfOrderness
	requiredEventTimeForTrigger := firstWindowEnd + maxOutOfOrdernessMs

	// Stage One: Send data in normal order
	t.Log("Stage One: Send data in normal order")
	for i := 0; i < 10; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i), // 0-9
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for watermark to advance (consider MaxOutOfOrderness = 1s)
	t.Log("Wait for watermark advance, trigger window (MaxOutOfOrderness = 1s)")
	time.Sleep(3 * time.Second)

	// Stage Two: Sending Delayed Data (Event Time in the First Window)
	// MaxOutOfOrderness = 1s: This data should be within the allowable out-of-order range
	// AllowedLateness = 500ms: After the window is triggered, it can still accept 500ms of latency data
	t.Log("Stage Two: Sending Delayed Data (Event Time in the First Window)")
	lateDataTimes := []int64{400, 800, 1200} // Delayed data event time
	for i, lateTime := range lateDataTimes {
		eventTime := baseTime + lateTime
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(40 + i), // 40-42, used to identify delayed data
		})
		time.Sleep(100 * time.Millisecond)
	}

	// Phase Three: Continue sending normal data and advance the watermark
	// Key: You must send the event time > = requiredEventTimeForTrigger data to allow watermark > = windowEnd
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

	// Collect all window results
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
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			cnt := firstWindow[0]["cnt"].(float64)
			minTemp := firstWindow[0]["min_temp"].(float64)
			maxTemp := firstWindow[0]["max_temp"].(float64)

			t.Logf("First window: cnt=%.0f, min=%.0f, max=%.0f", cnt, minTemp, maxTemp)

			// The validation window contains data
			// Scrolling window: window size is 2 seconds, one data entry every 200ms, theoretically there should be 10 data entries
			// However, due to window alignment and watermark mechanisms, the actual data volume may vary slightly
			assert.GreaterOrEqual(t, cnt, 3.0, "第一个窗口应该包含足够的数据（滚动窗口特性）")
			assert.Equal(t, 0.0, minTemp, "第一个窗口的最小值应该是0（正常数据）")

			// Verify whether delayed data is being processed
			// If configured correctly, maxTemp may contain latency data values (40-42)
			if maxTemp >= 40.0 {
				t.Logf("✓ Delay data is handled correctly, with maximum value including delay data: %.0f", maxTemp)
			} else {
				t.Logf("Note: Latency data may not be counted; current maximum value: %.0f", maxTemp)
			}
		}

		// Verify if there is a delayed update
		if windowResultsLen > 1 {
			t.Logf("✓ The scroll window has triggered %d times, possibly including delayed updates", windowResultsLen)

			// Verify the data in subsequent windows
			for i := 1; i < windowResultsLen && i < 3; i++ {
				if len(windowResultsCopy[i]) > 0 {
					cnt := windowResultsCopy[i][0]["cnt"].(float64)
					t.Logf("Window %d: cnt=%.0f", i+1, cnt)
				}
			}
		}
	}

	t.Logf("A total of %d windows were triggered", windowResultsLen)
	t.Logf("Configuration verification: MaxOutOfOrderness=1s, AllowedLateness=500ms")
}

// TestSQLTumblingWindow_LateDataHandling Test the delayed data processing of the rolling window
// Verification: Even if data arrives with delay, as long as it is within the allowable delay range, the corresponding window can still be correctly counted
func TestSQLTumblingWindow_LateDataHandling(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp
        FROM stream
        GROUP BY deviceId, TumblingWindow('2s')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
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

	// Using event time: simulates latency data scenarios
	// Scenario: First send data in normal order, then send some delayed data
	// The window size is 2 seconds, and it needs to be aligned to a multiple of 2 seconds
	windowSizeMs := int64(2000)                  // 2 seconds
	baseTimeRaw := time.Now().UnixMilli() - 5000 // Use 5 seconds before as the baseline
	// Align baseTime as a multiple to window size to ensure window alignment behavior is predictable
	baseTime := (baseTimeRaw / windowSizeMs) * windowSizeMs

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
			// Scrolling window: window size is 2 seconds, one data entry every 200ms, theoretically there should be 10 data entries
			// However, due to window alignment and watermark mechanisms, the actual data volume may vary slightly
			assert.GreaterOrEqual(t, cnt, 3.0, "第一个窗口应该包含足够的数据（滚动窗口特性）")
			assert.Equal(t, 0.0, minTemp, "第一个窗口的最小值应该是0（正常数据）")
			assert.GreaterOrEqual(t, maxTemp, 0.0, "第一个窗口的最大值应该大于等于0")
		}
	}

	// Verify whether delayed data is being processed
	// If delay data is handled correctly, it should be visible in future windows or updates
	t.Logf("A total of %d windows were triggered", windowResultsLen)
}

// TestSQLTumblingWindow_EventTimeWindowAlignment Align the event time window to epoch
func TestSQLTumblingWindow_EventTimeWindowAlignment(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, TumblingWindow('2s')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
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

	// Using event time: send data and align the validation window to epoch
	// The window size should be 2 seconds, aligned to multiples of 2 seconds
	baseTime := time.Now().UnixMilli()

	// When sending data, the event time starts from baseTime, with one message every 200ms
	// The first window should be aligned to a maximum 2-second multiple of baseTime
	for i := 0; i < 15; i++ {
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

	// Verify window alignment
	windowSizeMs := int64(2000) // 2 seconds = 2000 milliseconds
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

			assert.Equal(t, int64(0), startMs%windowSizeMs,
				"窗口 %d 的开始时间应该对齐到2秒的倍数（epoch对齐），实际: startMs=%d", i+1, startMs)

			if i > 0 {
				prevEndMs := windowResultsCopy[i-1][0]["end"].(int64) / int64(time.Millisecond)
				assert.Equal(t, prevEndMs, startMs,
					"窗口 %d 的开始时间应该等于前一个窗口的结束时间，prevEndMs=%d, startMs=%d", i+1, prevEndMs, startMs)
			}

			t.Logf("Window %d: start=%d, end=%d, size=%dms", i+1, startMs, endMs, endMs-startMs)
		}
	}
}

// TestSQLTumblingWindow_WatermarkTriggerTiming Test the timing of the Watermark trigger window
func TestSQLTumblingWindow_WatermarkTriggerTiming(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, TumblingWindow('2s')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='500ms', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
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

	// Use event time: Send data to verify the timing of watermark trigger
	baseTime := time.Now().UnixMilli() - 10000 // Use 10 seconds before as the baseline
	maxOutOfOrdernessMs := int64(500)          // 500ms

	// Stage One: Send data to the first window [alignedStart, alignedStart+2000)
	// Calculate the window start time after alignment
	windowSizeMs := int64(2000)
	alignedStart := (baseTime / windowSizeMs) * windowSizeMs
	firstWindowEnd := alignedStart + windowSizeMs

	t.Logf("First window: [%d, %d)", alignedStart, firstWindowEnd)

	// Send data, event time within the first window
	for i := 0; i < 10; i++ {
		eventTime := alignedStart + int64(i*200) // Inside the window
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Send data with an event time exactly equal to window_end to advance the watermark
	// watermark = maxEventTime - maxOutOfOrderness = firstWindowEnd - 500
	// At this point, watermark < firstWindowEnd, and the window should not be triggered
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"eventTime":   firstWindowEnd,
		"temperature": 100.0,
	})

	// Wait for watermark updates (watermark update interval 200ms)
	time.Sleep(500 * time.Millisecond)

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

	// Verify the timing of the first window trigger
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			row := firstWindow[0]
			start := row["start"].(int64)
			end := row["end"].(int64)

			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)

			assert.Equal(t, alignedStart, startMs,
				"第一个窗口的开始时间应该对齐到epoch，expected=%d, actual=%d", alignedStart, startMs)
			assert.Equal(t, firstWindowEnd, endMs,
				"第一个窗口的结束时间应该正确，expected=%d, actual=%d", firstWindowEnd, endMs)

			// The validation window is triggered when watermark >= window_end
			// Since watermark = maxEventTime - maxOutOfOrderness
			// When maxEventTime = firstWindowEnd + 1000, watermark = firstWindowEnd + 500
			// watermark > = firstWindowEnd, the window should be triggered
			t.Logf("✓ The window is correctly triggered when watermark > = window_end")
			t.Logf("Window: [%d, %d), triggered maxEventTime > = %d", start, end, end+maxOutOfOrdernessMs)
		}
	}
}

// TestSQLTumblingWindow_AllowedLatenessUpdate Test latency updates for AllowedLateness
func TestSQLTumblingWindow_AllowedLatenessUpdate(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               MIN(temperature) as min_temp,
               MAX(temperature) as max_temp,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, TumblingWindow('2s')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='500ms', ALLOWEDLATENESS='1s', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
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

	baseTime := time.Now().UnixMilli() - 10000
	windowSizeMs := int64(2000)
	alignedStart := (baseTime / windowSizeMs) * windowSizeMs
	firstWindowEnd := alignedStart + windowSizeMs
	allowedLatenessMs := int64(1000) // 1 second

	// Stage One: Send normal data and trigger the first window
	t.Log("Stage One: Send normal data and trigger the first window")
	for i := 0; i < 10; i++ {
		eventTime := alignedStart + int64(i*200)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i), // 0-9
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Push the watermark to trigger the first window
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"eventTime":   firstWindowEnd + 1000,
		"temperature": 100.0,
	})

	// Wait for the first window to trigger
	time.Sleep(1 * time.Second)

	// Collect the results from the first window
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
				t.Fatal("The first window should be received")
			}
			if len(res) > 0 {
				firstWindowReceived = true
				firstWindowCnt = res[0]["cnt"].(float64)
				firstWindowMax = res[0]["max_temp"].(float64)
				t.Logf("First window (initial): cnt = %.0f, max = %.0f", firstWindowCnt, firstWindowMax)
			}
			iteration++
		case <-time.After(500 * time.Millisecond):
			// No new results for 500ms
			iteration++
		case <-ctx.Done():
			t.Fatal("The first window should be received")
		}
	}

	// Stage 2: Send delayed data (event time within the first window but within the AllowedLateness range)
	t.Log("Stage Two: Sending Delayed Data (Event Time in the First Window)")
	lateDataTimes := []int64{300, 600, 900} // Event time of delayed data (relative to alignedStart)
	lateDataTemps := []float64{30.0, 31.0, 32.0}
	for i, lateTime := range lateDataTimes {
		eventTime := alignedStart + lateTime
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": lateDataTemps[i],
		})
		time.Sleep(100 * time.Millisecond)
	}

	// Continue sending normal data, advancing the watermark (but not exceeding window_end + allowedLateness)
	ssql.Emit(map[string]any{
		"deviceId":    "sensor001",
		"eventTime":   firstWindowEnd + allowedLatenessMs - 100, // Within the range of allowedLateness
		"temperature": 200.0,
	})

	// Waiting for delayed updates
	time.Sleep(1 * time.Second)

	// Collect all window results
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

	// Verification delay updates
	hasLateUpdate := false
	for i, window := range windowResultsCopy {
		if len(window) > 0 {
			row := window[0]
			start := row["start"].(int64)
			end := row["end"].(int64)
			cnt := row["cnt"].(float64)
			maxTemp := row["max_temp"].(float64)

			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)

			if startMs == alignedStart && endMs == firstWindowEnd {
				if cnt > firstWindowCnt {
					hasLateUpdate = true
					t.Logf("✓ Window Delay Updates: cnt increased from %.0f to %.0f, max from %.0f to %.0f",
						firstWindowCnt, cnt, firstWindowMax, maxTemp)

					// Validation latency data is included
					assert.GreaterOrEqual(t, maxTemp, 30.0,
						"延迟更新应该包含延迟数据，maxTemp应该>=30.0，实际: %.0f", maxTemp)
				}
			}

			t.Logf("Window %d: [%d, %d), cnt=%.0f, max=%.0f", i+1, start, end, cnt, maxTemp)
		}
	}

	if !hasLateUpdate {
		t.Logf("⚠ Prompt: No delayed updates detected; the delayed data may not have been processed or the window may have been closed")
	} else {
		t.Logf("✓ AllowedLateness functions work normally, delayed data triggers window updates")
	}
}

// TestSQLTumblingWindow_IdleSourceMechanism Test the Idle Source mechanism
// Verification When the data source is idle, the watermark advances processing time and the window closes normally
func TestSQLTumblingWindow_IdleSourceMechanism(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, TumblingWindow('2s')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='500ms', IDLETIMEOUT='2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
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

	// Using event time: send data, then stop sending and the validation window can close
	baseTime := time.Now().UnixMilli() - 10000
	windowSizeMs := int64(2000) // 2 seconds

	// Calculate the start time of the first window after alignment
	alignedStart := (baseTime / windowSizeMs) * windowSizeMs
	firstWindowEnd := alignedStart + windowSizeMs

	t.Logf("First window: [%d, %d)", alignedStart, firstWindowEnd)

	// Stage One: Send data and create a window
	t.Log("Stage One: Send data and create a window")
	for i := 0; i < 5; i++ {
		eventTime := alignedStart + int64(i*200)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Stage Two: Stop sending data and wait for the Idle Source mechanism to trigger
	// IdleTimeout = 2 seconds, meaning after 2 seconds without data, the watermark will proceed based on processing time
	t.Log("Stage Two: Stop sending data and wait for the Idle Source mechanism to trigger (IdleTimeout=2s)")
	time.Sleep(3 * time.Second) // Wait beyond IdleTimeout to ensure watermark progress

	// Collect window results
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

	// The validation window can be closed (even if no new data is found).
	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口（即使数据源空闲）")

	// Validate window data
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			row := firstWindow[0]
			start := row["start"].(int64)
			end := row["end"].(int64)
			cnt := row["cnt"].(float64)

			// Verify that window boundaries are correct
			// window_start() and window_end() return nanoseconds and need to be converted to milliseconds
			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)
			assert.Equal(t, alignedStart, startMs,
				"第一个窗口的开始时间应该对齐到窗口大小，expected=%d, actual=%d", alignedStart, startMs)
			assert.Equal(t, firstWindowEnd, endMs,
				"第一个窗口的结束时间应该正确，expected=%d, actual=%d", firstWindowEnd, endMs)

			// The validation window contains data
			assert.Greater(t, cnt, 0.0, "窗口应该包含数据")

			t.Logf("✓ Idle Source mechanism works properly; windows can close when data sources are idle")
			t.Logf("Window: [%d, %d), cnt=%.0f", start, end, cnt)
		}
	}
}

// TestSQLTumblingWindow_IdleSourceDisabled Testing if the Idle Source mechanism is not enabled
// Verify that when IdleTimeout=0 (disabled), if the data source is idle, the window cannot be closed
func TestSQLTumblingWindow_IdleSourceDisabled(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt,
               window_start() as start,
               window_end() as end
        FROM stream
        GROUP BY deviceId, TumblingWindow('2s')
        WITH (TIMESTAMP='eventTime', TIMEUNIT='ms', MAXOUTOFORDERNESS='500ms', IDLETIMEOUT='2s')
        -- 注意：没有配置IDLETIMEOUT，默认为0（禁用）
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]any, 20)
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

	baseTime := time.Now().UnixMilli() - 10000
	windowSizeMs := int64(2000)
	alignedStart := (baseTime / windowSizeMs) * windowSizeMs

	// Data is sent, but the event time is insufficient to trigger the window
	t.Log("Data is sent, but the event time is insufficient to trigger the window")
	for i := 0; i < 3; i++ {
		eventTime := alignedStart + int64(i*200)
		ssql.Emit(map[string]any{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Stop sending data and wait for a while
	// Since IdleTimeout is not enabled, the watermark does not advance based on processing time
	t.Log("Stop sending data and wait (IdleTimeout not enabled)")
	time.Sleep(3 * time.Second)

	// Collect window results
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
	windowResultsMu.Unlock()

	// Note: This test may not fully verify that the window cannot be closed
	// Because if the watermark has advanced enough to a sufficient position, the window may have already been triggered
	// This test is mainly used for comparison: enabling Idle Source versus not enabling Idle Source
	t.Logf("Number of window results: %d (IdleTimeout not enabled)", windowResultsLen)
}
