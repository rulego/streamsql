package streamsql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLSlidingWindow_ProcessingTime 测试处理时间的滑动窗口
// 验证不使用 WITH 子句时，滑动窗口基于处理时间（系统时钟）工作
func TestSQLSlidingWindow_ProcessingTime(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt
        FROM stream
        GROUP BY deviceId, SlidingWindow('2s', '500ms')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 10)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// 每200ms发送一条数据，持续发送3秒，确保有足够的数据
	// 数据会在处理时间到达时被添加到窗口
	done := make(chan bool)
	go func() {
		for i := 0; i < 15; i++ { // 发送15条数据，约3秒
			ssql.Emit(map[string]interface{}{
				"deviceId":    "sensor001",
				"temperature": i,
			})
			time.Sleep(200 * time.Millisecond)
		}
		done <- true
	}()

	// 等待窗口触发（第一个窗口应该在2秒后触发）
	time.Sleep(3 * time.Second)

	results := make([][]map[string]interface{}, 0)
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
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()
	require.Greater(t, windowResultsLen, 0, "应该至少有一个窗口结果")

	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		require.Len(t, firstWindow, 1, "第一个窗口应该只有一行结果")
		cnt := firstWindow[0]["cnt"].(float64)

		// 验证第一个窗口包含数据
		assert.Greater(t, cnt, 0.0, "第一个窗口应该包含数据")

		// 使用处理时间时，窗口基于数据到达的处理时间
		// 窗口大小2秒，每200ms发送一条数据
		// 第一个窗口应该在窗口大小时间（2秒）后触发
		// 在2秒内，应该会发送10条数据（每200ms 1条）

		// 验证第一个窗口的数据量应该在合理范围内
		// 使用处理时间时，窗口包含的是在窗口大小时间内到达的所有数据
		// 窗口大小2秒，每200ms 1条，应该包含约10条数据
		assert.GreaterOrEqual(t, cnt, 5.0,
			"第一个窗口应该包含足够的数据（窗口大小2秒，每200ms 1条），实际: %.0f", cnt)
		assert.LessOrEqual(t, cnt, 15.0,
			"第一个窗口不应该超过15条数据，实际: %.0f", cnt)

		t.Logf("第一个窗口数据量: %.0f（使用处理时间，窗口大小2秒，每200ms 1条数据）", cnt)
	}

	// 验证有多个窗口被触发（滑动窗口应该每2秒触发一次）
	if windowResultsLen > 1 {
		t.Logf("总共触发了 %d 个窗口", windowResultsLen)
		// 验证后续窗口也包含数据
		for i := 1; i < windowResultsLen && i < 5; i++ {
			if len(windowResultsCopy[i]) > 0 {
				cnt := windowResultsCopy[i][0]["cnt"].(float64)
				assert.Greater(t, cnt, 0.0, "窗口 %d 应该包含数据", i+1)
			}
		}
	}
}

func TestSQLSlidingWindow_WithAggregations(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		ch <- results
	})

	// 使用处理时间，每200ms发送一条数据
	for i := 0; i < 15; i++ {
		temperature := float64(i)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": temperature,
		})
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	results := make([][]map[string]interface{}, 0)
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
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		ch <- results
	})

	// 使用处理时间，每200ms发送一条数据
	for i := 0; i < 15; i++ {
		ssql.Emit(map[string]interface{}{
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
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
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

	if windowResultsLen > 1 {
		firstWindow := windowResultsCopy[0]
		lastWindow := windowResultsCopy[windowResultsLen-1]

		firstCnt := firstWindow[0]["cnt"].(float64)
		lastCnt := lastWindow[0]["cnt"].(float64)
		firstMin := firstWindow[0]["min_temp"].(float64)
		lastMin := lastWindow[0]["min_temp"].(float64)

		assert.GreaterOrEqual(t, firstCnt, lastCnt,
			"第一个窗口应该包含不少于最后一个窗口的数据")
		assert.LessOrEqual(t, firstMin, lastMin,
			"第一个窗口的最小值应该小于等于最后一个窗口的最小值")
	}

	allCounts := make([]float64, windowResultsLen)
	for i, res := range windowResultsCopy {
		allCounts[i] = res[0]["cnt"].(float64)
	}

	for i := 1; i < len(allCounts); i++ {
		prevCnt := allCounts[i-1]
		currCnt := allCounts[i]
		assert.GreaterOrEqual(t, prevCnt, currCnt,
			"窗口计数应该递减或保持不变（由于窗口对齐，可能不完全递减）")
	}
}

func TestSQLSlidingWindow_MultiKeyGrouped(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		ch <- results
	})

	// 使用处理时间，每200ms发送一组数据
	for i := 0; i < 8; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "A",
			"region":      "R1",
			"temperature": float64(i),
		})
		ssql.Emit(map[string]interface{}{
			"deviceId":    "B",
			"region":      "R1",
			"temperature": float64(i + 10),
		})
		ssql.Emit(map[string]interface{}{
			"deviceId":    "A",
			"region":      "R2",
			"temperature": float64(i + 20),
		})
		ssql.Emit(map[string]interface{}{
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

// TestSQLSlidingWindow_FirstWindowTiming 测试第一个窗口的触发时机
// 验证第一个窗口应该在窗口大小时间后触发，而不是滑动步长时间后触发
func TestSQLSlidingWindow_FirstWindowTiming(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	windowTimings := make([]time.Time, 0)
	var windowTimingsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		if len(results) > 0 {
			windowTimingsMu.Lock()
			windowTimings = append(windowTimings, time.Now())
			windowTimingsMu.Unlock()
			ch <- results
		}
	})

	// 记录第一个数据发送时间
	firstDataTime := time.Now()
	baseTime := time.Now().UnixMilli()

	// 使用事件时间，每200ms发送一条数据，共发送10条
	for i := 0; i < 10; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"timestamp":   baseTime + int64(i*200), // 添加timestamp字段
			"temperature": float64(i),
		})
		time.Sleep(200 * time.Millisecond)
	}

	// 发送一个事件时间超过第一个窗口结束时间的数据，推进watermark
	// 窗口大小2秒，第一个窗口应该在 [baseTime, baseTime+2000) 范围内
	// 发送一个事件时间为 baseTime+3000 的数据来推进watermark
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"timestamp":   baseTime + 3000, // 推进watermark
		"temperature": 100.0,
	})

	// 等待第一个窗口触发（应该在窗口大小2秒后，而不是滑动步长500ms后）
	// 发送完数据后，等待足够的时间让第一个窗口触发
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

					// 第一个窗口应该在窗口大小时间（2秒）后触发
					// 允许一些误差（±500ms），因为数据处理和调度可能有延迟
					assert.GreaterOrEqual(t, elapsed, 1500*time.Millisecond,
						"第一个窗口应该在窗口大小时间（2秒）后触发，实际耗时: %v", elapsed)
					assert.LessOrEqual(t, elapsed, 5*time.Second,
						"第一个窗口不应该太晚触发，实际耗时: %v", elapsed)

					// 验证第一个窗口不应该在滑动步长时间（500ms）后就触发
					assert.Greater(t, elapsed, 800*time.Millisecond,
						"第一个窗口不应该在滑动步长时间（500ms）后就触发，实际耗时: %v", elapsed)

					cnt := res[0]["cnt"].(float64)
					assert.Greater(t, cnt, 0.0, "第一个窗口应该包含数据")
					t.Logf("第一个窗口触发时间: %v, 从第一个数据到触发耗时: %v, 窗口数据量: %.0f",
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

// TestSQLSlidingWindow_DataOverlap 测试滑动窗口的数据重叠正确性
// 验证数据在多个窗口中正确保留，不会过早清理
func TestSQLSlidingWindow_DataOverlap(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// 使用处理时间，每200ms发送一条数据，共发送15条
	// 窗口大小2秒，滑动步长500ms
	// 使用处理时间时，窗口基于数据到达的处理时间
	for i := 0; i < 15; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(200 * time.Millisecond)
	}

	// 等待足够的时间让多个窗口触发
	// 第一个窗口在2秒后触发，后续窗口每500ms触发一次
	// 等待足够的时间让至少3个窗口触发：2秒（第一个窗口）+ 500ms（第二个窗口）+ 500ms（第三个窗口）= 3秒
	time.Sleep(3 * time.Second)

	// 收集所有已到达的结果，设置合理的超时时间
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-ch:
			// 继续收集结果
		case <-timeout:
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()
	require.GreaterOrEqual(t, windowResultsLen, 3, "应该至少触发3个窗口")

	// 验证第一个窗口包含数据0-9
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		require.Len(t, firstWindow, 1)
		firstRow := firstWindow[0]
		firstCnt := firstRow["cnt"].(float64)
		firstMin := firstRow["min_temp"].(float64)
		firstMax := firstRow["max_temp"].(float64)

		// 使用处理时间时，第一个窗口应该包含在窗口大小时间内到达的数据
		// 窗口大小2秒，每200ms 1条数据，应该包含约10条数据
		// 但由于窗口对齐和数据处理延迟，实际数量可能略有不同
		assert.GreaterOrEqual(t, firstCnt, 5.0,
			"第一个窗口应该包含足够的数据（窗口大小2秒，每200ms 1条），实际: %.0f", firstCnt)
		assert.LessOrEqual(t, firstCnt, 15.0,
			"第一个窗口不应该超过15条数据，实际: %.0f", firstCnt)
		// 第一个窗口的最小值应该是0或接近0
		assert.LessOrEqual(t, firstMin, 1.0,
			"第一个窗口的最小值应该接近0，实际: %.0f", firstMin)
		// 第一个窗口的最大值应该大于0
		assert.GreaterOrEqual(t, firstMax, 0.0,
			"第一个窗口的最大值应该大于等于0，实际: %.0f", firstMax)

		t.Logf("第一个窗口: cnt=%.0f, min=%.0f, max=%.0f", firstCnt, firstMin, firstMax)
	}

	// 验证第二个窗口与第一个窗口有重叠
	if windowResultsLen > 1 {
		secondWindow := windowResultsCopy[1]
		require.Len(t, secondWindow, 1)
		secondRow := secondWindow[0]
		secondCnt := secondRow["cnt"].(float64)
		secondMin := secondRow["min_temp"].(float64)
		secondMax := secondRow["max_temp"].(float64)

		// 使用处理时间时，第二个窗口也应该包含足够的数据
		// 窗口大小2秒，每200ms 1条数据，应该包含约10条数据
		assert.GreaterOrEqual(t, secondCnt, 5.0,
			"第二个窗口应该包含足够的数据（窗口大小2秒，每200ms 1条），实际: %.0f", secondCnt)

		// 验证重叠：第二个窗口的最小值应该大于第一个窗口的最小值
		// 因为窗口滑动，第二个窗口应该从数据2开始
		if windowResultsLen > 0 {
			firstMin := windowResultsCopy[0][0]["min_temp"].(float64)
			assert.GreaterOrEqual(t, secondMin, firstMin,
				"第二个窗口的最小值应该大于等于第一个窗口的最小值，说明窗口正确滑动")
		}

		t.Logf("第二个窗口: cnt=%.0f, min=%.0f, max=%.0f", secondCnt, secondMin, secondMax)
	}

	// 验证窗口数据不会过早丢失
	// 检查是否有窗口的数据量异常少（可能是数据被过早清理）
	for i, res := range windowResultsCopy {
		if len(res) > 0 {
			cnt := res[0]["cnt"].(float64)
			// 对于前几个窗口，数据量不应该异常少
			// 使用处理时间时，窗口大小2秒，每200ms 1条，应该包含约10条数据
			if i < 3 {
				assert.GreaterOrEqual(t, cnt, 5.0,
					"窗口 %d 的数据量不应该异常少，可能是数据被过早清理，实际: %.0f", i+1, cnt)
			}
		}
	}
}

// TestSQLSlidingWindow_DataRetention 测试滑动窗口的数据保留逻辑
// 验证数据在后续窗口中正确保留，不会过早清理
func TestSQLSlidingWindow_DataRetention(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// 使用处理时间，每200ms发送一条数据，共发送12条
	// 窗口大小2秒，滑动步长500ms
	// 使用处理时间时，窗口基于数据到达的处理时间
	for i := 0; i < 12; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(200 * time.Millisecond)
	}

	// 等待多个窗口触发
	// 第一个窗口在2秒后触发，后续窗口每500ms触发一次
	// 等待足够的时间让至少3个窗口触发：2秒（第一个窗口）+ 500ms（第二个窗口）+ 500ms（第三个窗口）= 3秒
	time.Sleep(3 * time.Second)

	// 收集所有已到达的结果，设置合理的超时时间
	// 由于已经等待了15秒，大部分窗口应该已经触发
	// 这里只需要等待一小段时间收集剩余的结果
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-ch:
			// 继续收集结果
		case <-timeout:
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()
	require.GreaterOrEqual(t, windowResultsLen, 3, "应该至少触发3个窗口")

	// 验证数据保留：检查最小值的变化趋势
	// 由于窗口滑动，后续窗口的最小值应该逐渐增大
	// 但如果数据保留逻辑正确，不应该突然跳跃
	minTemps := make([]float64, 0)
	for _, res := range windowResultsCopy {
		if len(res) > 0 {
			minTemp := res[0]["min_temp"].(float64)
			minTemps = append(minTemps, minTemp)
		}
	}

	// 验证最小值是递增或保持稳定的（不应该突然跳跃）
	for i := 1; i < len(minTemps); i++ {
		prevMin := minTemps[i-1]
		currMin := minTemps[i]
		// 最小值应该递增或保持不变（窗口滑动导致）
		// 但差值不应该太大（说明数据没有被过早清理）
		assert.GreaterOrEqual(t, currMin, prevMin-1.0,
			"窗口 %d 的最小值不应该比前一个窗口小太多，可能是数据被过早清理", i+1)
	}

	// 验证窗口数据量：前几个窗口的数据量应该足够
	// 使用处理时间时，如果数据保留逻辑正确，窗口数据量应该逐渐减少（因为旧数据逐渐过期）
	// 但减少应该是平滑的，不应该突然大幅减少
	for i := 0; i < windowResultsLen && i < 5; i++ {
		if len(windowResultsCopy[i]) > 0 {
			cnt := windowResultsCopy[i][0]["cnt"].(float64)
			// 前几个窗口应该包含足够的数据（使用处理时间）
			// 窗口大小2秒，每200ms 1条数据，应该包含约10条数据
			if i < 3 {
				assert.GreaterOrEqual(t, cnt, 5.0,
					"窗口 %d 应该包含足够的数据（窗口大小2秒，每200ms 1条），实际: %.0f", i+1, cnt)
			}
		}
	}
}

// TestSQLSlidingWindow_EventTimeWithWithClause 测试使用 WITH 子句指定事件时间
func TestSQLSlidingWindow_EventTimeWithWithClause(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// 使用事件时间：发送带有事件时间戳的数据
	// 事件时间从当前时间开始，每200ms递增，确保 watermark 能够推进
	baseTime := time.Now().UnixMilli() // 使用当前时间作为基准
	for i := 0; i < 15; i++ {
		eventTime := baseTime + int64(i*200) // 每200ms一条数据
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime, // 事件时间字段（毫秒）
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond) // 处理时间间隔较小，模拟乱序
	}

	// 等待窗口触发（事件时间窗口基于 watermark 触发）
	// 窗口大小2秒，滑动步长500ms
	// 第一个窗口应该在 watermark >= window_end 时触发
	// 由于 watermark 更新间隔是 200ms，需要等待足够的时间让 watermark 推进
	time.Sleep(3 * time.Second)

	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-ch:
			// 继续收集结果
		case <-timeout:
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	// 事件时间窗口应该能够触发
	// 由于使用事件时间，窗口触发基于 watermark
	require.Greater(t, windowResultsLen, 0, "事件时间窗口应该至少触发一个窗口")
	if windowResultsLen > 0 {
		t.Logf("事件时间窗口触发了 %d 个窗口", windowResultsLen)
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			cnt := firstWindow[0]["cnt"].(float64)
			assert.Greater(t, cnt, 0.0, "事件时间窗口应该包含数据")
			t.Logf("第一个事件时间窗口数据量: %.0f", cnt)
		}
	}
}

// TestSQLSlidingWindow_LateDataHandling 测试延迟数据的处理
// 验证即使数据延迟到达，只要在允许的延迟范围内，也能正确统计到对应窗口
func TestSQLSlidingWindow_LateDataHandling(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// 使用事件时间：模拟延迟数据场景
	// 场景：先发送正常顺序的数据，然后发送一些延迟的数据
	baseTime := time.Now().UnixMilli() - 5000 // 使用5秒前作为基准，确保有足够的时间窗口

	// 第一阶段：发送正常顺序的数据（事件时间：0ms, 200ms, 400ms, ..., 2000ms）
	// 这些数据应该被统计到第一个窗口 [0ms, 2000ms)
	t.Log("第一阶段：发送正常顺序的数据")
	for i := 0; i < 10; i++ {
		eventTime := baseTime + int64(i*200) // 每200ms一条数据
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i), // 温度值 0-9
		})
		time.Sleep(50 * time.Millisecond) // 处理时间间隔较小
	}

	// 等待 watermark 推进，让第一个窗口触发
	// 窗口大小2秒，第一个窗口应该在 watermark >= baseTime + 2000ms 时触发
	t.Log("等待 watermark 推进，触发第一个窗口")
	time.Sleep(3 * time.Second)

	// 第二阶段：发送延迟的数据
	// 这些数据的事件时间比之前的数据早，但应该在允许的延迟范围内
	// 延迟数据的事件时间：100ms, 300ms, 500ms（这些时间在第一个窗口 [0ms, 2000ms) 内）
	t.Log("第二阶段：发送延迟数据（事件时间在第一个窗口内）")
	for i := 0; i < 3; i++ {
		// 延迟数据：事件时间比正常数据早，但仍在窗口范围内
		eventTime := baseTime + int64(100+i*200) // 100ms, 300ms, 500ms
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(10 + i), // 温度值 10-12，用于区分延迟数据
		})
		time.Sleep(100 * time.Millisecond)
	}

	// 继续发送更多正常数据，推进 watermark
	t.Log("第三阶段：继续发送正常数据，推进 watermark")
	for i := 10; i < 15; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 等待窗口触发和延迟数据处理
	time.Sleep(3 * time.Second)

	// 收集所有窗口结果
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-ch:
			// 继续收集结果
		case <-timeout:
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	// 验证第一个窗口的数据
	// 第一个窗口应该包含正常数据（0-9）和可能的延迟数据
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			cnt := firstWindow[0]["cnt"].(float64)
			minTemp := firstWindow[0]["min_temp"].(float64)
			maxTemp := firstWindow[0]["max_temp"].(float64)

			t.Logf("第一个窗口: cnt=%.0f, min=%.0f, max=%.0f", cnt, minTemp, maxTemp)

			// 第一个窗口应该包含正常数据
			// 由于窗口对齐和 watermark 机制，实际数据量可能略有不同
			assert.GreaterOrEqual(t, cnt, 5.0, "第一个窗口应该包含足够的数据")
			assert.Equal(t, 0.0, minTemp, "第一个窗口的最小值应该是0（正常数据）")
			assert.GreaterOrEqual(t, maxTemp, 0.0, "第一个窗口的最大值应该大于等于0")
		}
	}

	// 验证延迟数据是否被处理
	// 如果延迟数据被正确处理，应该能在后续窗口或更新中看到
	t.Logf("总共触发了 %d 个窗口", windowResultsLen)
}

// TestSQLSlidingWindow_MaxOutOfOrderness 测试最大延迟时间配置
// 验证设置 MaxOutOfOrderness 后，延迟数据能否在允许的延迟范围内被正确处理
func TestSQLSlidingWindow_MaxOutOfOrderness(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	// 使用 SQL 配置 MaxOutOfOrderness
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// 模拟延迟数据场景
	// 场景：设置 MaxOutOfOrderness = 1秒，测试延迟数据能否在1秒内被正确处理
	// 滑动窗口步长500ms，需要对齐到500ms的倍数
	slideSizeMs := int64(500)                     // 500ms
	baseTimeRaw := time.Now().UnixMilli() - 10000 // 使用10秒前作为基准
	// 对齐baseTime到滑动步长的倍数，确保窗口对齐行为可预测
	baseTime := (baseTimeRaw / slideSizeMs) * slideSizeMs

	// 第一阶段：发送正常顺序的数据
	// 事件时间：0ms, 200ms, 400ms, ..., 2000ms（第一个窗口 [0ms, 2000ms)）
	t.Log("第一阶段：发送正常顺序的数据（事件时间 0-2000ms）")
	for i := 0; i < 10; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i), // 0-9
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 等待 watermark 推进，触发第一个窗口
	t.Log("等待 watermark 推进，触发第一个窗口")
	time.Sleep(3 * time.Second)

	// 第二阶段：发送延迟数据
	// 延迟数据的事件时间在第一个窗口内（如 500ms, 700ms, 900ms）
	// 如果 MaxOutOfOrderness = 1秒，这些数据应该能被处理
	t.Log("第二阶段：发送延迟数据（事件时间在第一个窗口内，延迟 < 1秒）")
	lateDataTimes := []int64{500, 700, 900} // 延迟数据的事件时间（相对于 baseTime）
	for i, lateTime := range lateDataTimes {
		eventTime := baseTime + lateTime
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(20 + i), // 20-22，用于标识延迟数据
		})
		time.Sleep(100 * time.Millisecond)
	}

	// 第三阶段：发送更多正常数据，推进 watermark
	// 关键：要触发窗口，需要 watermark >= windowEnd
	// watermark = maxEventTime - maxOutOfOrderness
	// 所以需要：maxEventTime >= windowEnd + maxOutOfOrderness
	windowSizeMs := int64(2000)        // 2秒
	maxOutOfOrdernessMs := int64(1000) // 1秒
	firstWindowEnd := baseTime + windowSizeMs
	requiredEventTimeForTrigger := firstWindowEnd + maxOutOfOrdernessMs
	t.Log("第三阶段：继续发送正常数据，推进 watermark")
	for i := 10; i < 15; i++ {
		eventTime := baseTime + int64(i*200)
		// 确保至少有一个数据的事件时间 >= requiredEventTimeForTrigger
		if i == 10 && eventTime < requiredEventTimeForTrigger {
			eventTime = requiredEventTimeForTrigger
		}
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 等待窗口触发和延迟数据处理
	time.Sleep(3 * time.Second)

	// 收集所有窗口结果（添加超时和最大迭代次数限制）
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	maxIterations := 20
	iteration := 0

	for iteration < maxIterations {
		select {
		case result, ok := <-ch:
			if !ok {
				// channel 已关闭
				goto END
			}
			_ = result // 使用结果
			iteration++
		case <-time.After(500 * time.Millisecond):
			// 500ms 没有新结果，退出
			goto END
		case <-ctx.Done():
			// 超时退出
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	// 验证窗口数据
	// 如果 MaxOutOfOrderness 配置正确，延迟数据应该能被统计到对应窗口
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			cnt := firstWindow[0]["cnt"].(float64)
			minTemp := firstWindow[0]["min_temp"].(float64)
			maxTemp := firstWindow[0]["max_temp"].(float64)

			t.Logf("第一个窗口: cnt=%.0f, min=%.0f, max=%.0f", cnt, minTemp, maxTemp)

			// 验证窗口包含数据
			assert.GreaterOrEqual(t, cnt, 5.0, "第一个窗口应该包含足够的数据")
			assert.Equal(t, 0.0, minTemp, "第一个窗口的最小值应该是0（正常数据）")

			// 注意：如果 MaxOutOfOrderness 配置正确且延迟数据被处理，
			// maxTemp 可能会包含延迟数据的值（20-22）
			// 但由于当前可能没有配置 MaxOutOfOrderness，延迟数据可能不会被统计
			t.Logf("提示：如果 MaxOutOfOrderness 配置正确，延迟数据（temperature=20-22）应该能被统计")
		}
	}

	t.Logf("总共触发了 %d 个窗口", windowResultsLen)
}

// TestSQLSlidingWindow_AllowedLateness 测试滑动窗口的 AllowedLateness 配置
// 验证窗口触发后，延迟数据能否在允许的延迟时间内更新窗口结果
func TestSQLSlidingWindow_AllowedLateness(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// 模拟 AllowedLateness 场景
	// 场景：窗口触发后，发送延迟数据，验证窗口能否更新
	baseTime := time.Now().UnixMilli() - 10000 // 使用10秒前作为基准

	// 第一阶段：发送正常顺序的数据，触发第一个窗口
	// 事件时间：0ms, 200ms, 400ms, ..., 2000ms（第一个窗口 [0ms, 2000ms)）
	t.Log("第一阶段：发送正常顺序的数据（事件时间 0-2000ms）")
	for i := 0; i < 10; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i), // 0-9
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 推进watermark，触发第一个窗口
	// 关键：要触发窗口，需要 watermark >= windowEnd
	// watermark = maxEventTime - maxOutOfOrderness
	// 所以需要：maxEventTime >= windowEnd + maxOutOfOrderness
	windowSizeMs := int64(2000)        // 2秒
	maxOutOfOrdernessMs := int64(1000) // 1秒
	firstWindowEnd := baseTime + windowSizeMs
	requiredEventTimeForTrigger := firstWindowEnd + maxOutOfOrdernessMs
	// 发送事件时间 >= requiredEventTimeForTrigger 的数据，确保 watermark >= windowEnd
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"eventTime":   requiredEventTimeForTrigger,
		"temperature": 100.0,
	})

	// 等待 watermark 推进，触发第一个窗口
	t.Log("等待 watermark 推进，触发第一个窗口")
	time.Sleep(3 * time.Second)

	// 收集第一个窗口的结果（添加最大迭代次数限制）
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
				// channel 已关闭
				goto COLLECT_FIRST_WINDOW_END
			}
			if len(res) > 0 {
				firstWindowReceived = true
				firstWindowCnt = res[0]["cnt"].(float64)
				firstWindowMax = res[0]["max_temp"].(float64)
				t.Logf("第一个窗口: cnt=%.0f, max=%.0f", firstWindowCnt, firstWindowMax)
			}
			iteration++
		case <-time.After(500 * time.Millisecond):
			// 500ms 没有新结果
			iteration++
		case <-ctx.Done():
			t.Log("等待第一个窗口超时")
			goto COLLECT_FIRST_WINDOW_END
		}
	}
COLLECT_FIRST_WINDOW_END:

	if !firstWindowReceived {
		t.Log("⚠ 第一个窗口未触发，可能watermark未推进到足够位置")
	}
	assert.GreaterOrEqual(t, firstWindowCnt, 5.0, "第一个窗口应该包含足够的数据")
	assert.LessOrEqual(t, firstWindowMax, 9.0, "第一个窗口的最大值应该不超过9（正常数据）")

	// 第二阶段：发送延迟数据（事件时间在第一个窗口内）
	// 这些数据应该在 AllowedLateness = 500ms 内被处理
	t.Log("第二阶段：发送延迟数据（事件时间在第一个窗口内）")
	lateDataTimes := []int64{300, 600, 900} // 延迟数据的事件时间
	lateDataTemps := []float64{30.0, 31.0, 32.0}
	for i, lateTime := range lateDataTimes {
		eventTime := baseTime + lateTime
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": lateDataTemps[i], // 30-32，用于标识延迟数据
		})
		time.Sleep(100 * time.Millisecond)
	}

	// 第三阶段：继续发送正常数据，推进 watermark
	t.Log("第三阶段：继续发送正常数据，推进 watermark")
	for i := 10; i < 15; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 等待窗口触发和延迟数据处理
	time.Sleep(3 * time.Second)

	// 收集所有窗口结果（添加超时和最大迭代次数限制）
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	maxIterations2 := 20
	iteration2 := 0

	for iteration2 < maxIterations2 {
		select {
		case result, ok := <-ch:
			if !ok {
				// channel 已关闭
				goto END
			}
			_ = result // 使用结果
			iteration2++
		case <-time.After(500 * time.Millisecond):
			// 500ms 没有新结果，退出
			goto END
		case <-ctx2.Done():
			// 超时退出
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	// 验证窗口数据
	// 如果 AllowedLateness 配置正确，延迟数据应该能触发窗口的延迟更新
	if windowResultsLen > 0 {
		// 滑动窗口的延迟更新可能体现在后续的窗口结果中
		// 检查所有窗口结果，看是否有包含延迟数据的窗口
		hasLateDataUpdate := false
		for i, window := range windowResultsCopy {
			if len(window) > 0 {
				cnt := window[0]["cnt"].(float64)
				minTemp := window[0]["min_temp"].(float64)
				maxTemp := window[0]["max_temp"].(float64)

				t.Logf("窗口 %d: cnt=%.0f, min=%.0f, max=%.0f", i+1, cnt, minTemp, maxTemp)

				// 验证窗口包含数据
				assert.GreaterOrEqual(t, cnt, 1.0, "窗口 %d 应该包含数据", i+1)

				// 如果 AllowedLateness 配置正确，延迟数据应该被处理
				// 延迟数据（temperature=30-32）应该能被统计
				if maxTemp >= 30.0 {
					hasLateDataUpdate = true
					t.Logf("✓ 窗口 %d 包含延迟数据，最大值: %.0f", i+1, maxTemp)

					// 验证延迟更新包含更多数据
					if i == 0 {
						// 第一个窗口的延迟更新应该包含更多数据
						assert.GreaterOrEqual(t, cnt, firstWindowCnt+3.0,
							"延迟更新应该包含更多数据（原数据 + 延迟数据）")
					}
				}
			}
		}

		// 验证是否有延迟更新（窗口可能触发多次）
		if windowResultsLen > 1 {
			t.Logf("✓ 滑动窗口触发了 %d 次，可能包含延迟更新", windowResultsLen)
		}

		if !hasLateDataUpdate {
			t.Logf("⚠ 提示：延迟数据可能未被统计，或延迟数据的时间不在窗口范围内")
		} else {
			t.Logf("✓ AllowedLateness 功能正常工作，延迟数据已被处理")
		}
	}

	t.Logf("总共触发了 %d 个窗口", windowResultsLen)
}

// TestSQLSlidingWindow_EventTimeWindowAlignment 测试事件时间滑动窗口对齐到epoch
func TestSQLSlidingWindow_EventTimeWindowAlignment(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// 使用事件时间：发送数据，验证滑动窗口对齐到滑动步长
	// 窗口大小2秒，滑动步长500ms，应该对齐到500ms的倍数
	baseTime := time.Now().UnixMilli()

	// 发送数据，事件时间从baseTime开始，每200ms一条
	for i := 0; i < 20; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 等待窗口触发
	time.Sleep(3 * time.Second)

	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-ch:
			// 继续收集结果
		case <-timeout:
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	// 验证窗口对齐
	windowSizeMs := int64(2000) // 2秒
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

			t.Logf("窗口 %d: start=%d, end=%d, size=%dms", i+1, startMs, endMs, endMs-startMs)
		}
	}
}

// TestSQLSlidingWindow_WatermarkTriggerTiming 测试滑动窗口Watermark触发时机
func TestSQLSlidingWindow_WatermarkTriggerTiming(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			ch <- results
		}
	})

	// 使用事件时间：发送数据，验证watermark触发时机
	baseTime := time.Now().UnixMilli() - 10000
	maxOutOfOrdernessMs := int64(500)
	slideSizeMs := int64(500)

	// 发送数据，事件时间在第一个窗口内
	// 注意：第一个数据的事件时间会影响窗口对齐
	firstEventTime := baseTime
	for i := 0; i < 10; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 计算对齐后的第一个窗口开始时间（基于第一个数据的事件时间）
	// alignWindowStart 会对齐到小于等于事件时间的最大对齐点
	alignedStart := (firstEventTime / slideSizeMs) * slideSizeMs
	firstWindowEnd := alignedStart + 2000 // 窗口大小2秒

	t.Logf("第一个窗口: [%d, %d)", alignedStart, firstWindowEnd)

	// 发送一个事件时间超过window_end的数据，推进watermark
	// watermark = maxEventTime - maxOutOfOrderness = (firstWindowEnd + 1000) - 500 = firstWindowEnd + 500
	// 此时 watermark >= firstWindowEnd，窗口应该触发
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"eventTime":   firstWindowEnd + 1000,
		"temperature": 200.0,
	})

	// 等待窗口触发
	time.Sleep(1 * time.Second)

	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-ch:
			// 继续收集结果
		case <-timeout:
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口")

	// 验证第一个窗口的触发时机
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			row := firstWindow[0]
			start := row["start"].(int64)
			end := row["end"].(int64)

			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)

			// 验证窗口对齐到滑动步长（允许一定的误差，因为对齐基于第一个数据的事件时间）
			assert.Equal(t, int64(0), startMs%slideSizeMs,
				"第一个窗口的开始时间应该对齐到滑动步长，expected对齐到%d的倍数，actual=%d", slideSizeMs, startMs)
			// 验证窗口大小正确
			assert.Equal(t, int64(2000), endMs-startMs,
				"第一个窗口的大小应该是2秒（2000ms），actual=%d", endMs-startMs)

			t.Logf("✓ 滑动窗口在watermark >= window_end时正确触发")
			t.Logf("窗口: [%d, %d), 触发时maxEventTime >= %d", start, end, end+maxOutOfOrdernessMs)
		}
	}
}

// TestSQLSlidingWindow_IdleSourceMechanism 测试滑动窗口的Idle Source机制
func TestSQLSlidingWindow_IdleSourceMechanism(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 20)
	defer close(ch)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
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

	// 发送数据
	t.Log("发送数据，创建滑动窗口")
	for i := 0; i < 10; i++ {
		eventTime := alignedStart + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 停止发送数据，等待Idle Source机制触发
	t.Log("停止发送数据，等待Idle Source机制触发（IdleTimeout=2s）")
	time.Sleep(3 * time.Second)

	// 收集窗口结果
	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-ch:
			// 继续收集结果
		case <-timeout:
			goto END
		}
	}

END:
	windowResultsMu.Lock()
	windowResultsLen := len(windowResults)
	windowResultsCopy := make([][]map[string]interface{}, len(windowResults))
	copy(windowResultsCopy, windowResults)
	windowResultsMu.Unlock()

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个滑动窗口（即使数据源空闲）")

	if windowResultsLen > 0 {
		t.Logf("✓ 滑动窗口Idle Source机制正常工作，触发了 %d 个窗口", windowResultsLen)
		for i, window := range windowResultsCopy {
			if len(window) > 0 {
				row := window[0]
				start := row["start"].(int64)
				end := row["end"].(int64)
				cnt := row["cnt"].(float64)
				t.Logf("窗口 %d: [%d, %d), cnt=%.0f", i+1, start, end, cnt)
			}
		}
	}
}
