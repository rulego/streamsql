package streamsql

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLSlidingWindow_Basic(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	sql := `
        SELECT deviceId,
               COUNT(*) as cnt
        FROM stream
        GROUP BY deviceId, SlidingWindow('10s', '2s')
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

	// 每秒发送一条数据，持续发送15秒，确保有足够的数据
	// 数据会在处理时间到达时被添加到窗口
	done := make(chan bool)
	go func() {
		for i := 0; i < 15; i++ { // 发送15条数据，约15秒
			ssql.Emit(map[string]interface{}{
				"deviceId":    "sensor001",
				"temperature": i,
			})
			time.Sleep(1 * time.Second)
		}
		done <- true
	}()

	// 等待窗口触发（第一个窗口应该在10秒后触发）
	time.Sleep(12 * time.Second)

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
		// 窗口大小10秒，每秒发送一条数据
		// 第一个窗口应该在窗口大小时间（10秒）后触发
		// 在10秒内，应该会发送10条数据（每秒1条）
		// 但由于窗口对齐到滑动步长（2秒），实际窗口范围可能略有不同

		// 验证第一个窗口的数据量应该在合理范围内
		// 使用处理时间时，窗口包含的是在窗口大小时间内到达的所有数据
		// 窗口大小10秒，每秒1条，应该包含接近10条数据
		assert.GreaterOrEqual(t, cnt, 5.0,
			"第一个窗口应该包含足够的数据（窗口大小10秒，每秒1条），实际: %.0f", cnt)
		assert.LessOrEqual(t, cnt, 15.0,
			"第一个窗口不应该超过15条数据，实际: %.0f", cnt)

		t.Logf("第一个窗口数据量: %.0f（使用处理时间，窗口大小10秒，每秒1条数据）", cnt)
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
        GROUP BY deviceId, SlidingWindow('10s', '2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 20)
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	// 使用处理时间，每秒发送一条数据
	for i := 0; i < 15; i++ {
		temperature := float64(i)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": temperature,
		})
		time.Sleep(1 * time.Second)
	}

	time.Sleep(5 * time.Second)

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
	assert.GreaterOrEqual(t, maxCnt, 8.0, "至少应该有一个窗口包含接近10条数据")

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
        GROUP BY deviceId, SlidingWindow('10s', '2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 20)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	// 使用处理时间，每秒发送一条数据
	for i := 0; i < 15; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(1 * time.Second)
	}

	time.Sleep(8 * time.Second)

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
        GROUP BY deviceId, region, SlidingWindow('5s', '2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 20)
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	// 使用处理时间，每秒发送一组数据
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
		time.Sleep(1 * time.Second)
	}

	time.Sleep(3 * time.Second)

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
        GROUP BY deviceId, SlidingWindow('10s', '2s')
        WITH (TIMESTAMP='timestamp')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 20)
	windowTimings := make([]time.Time, 0)
	var windowTimingsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		if len(results) > 0 {
			windowTimingsMu.Lock()
			windowTimings = append(windowTimings, time.Now())
			windowTimingsMu.Unlock()
			ch <- results
		}
	})

	// 记录第一个数据发送时间
	firstDataTime := time.Now()

	// 使用处理时间，每秒发送一条数据，共发送10条
	for i := 0; i < 10; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(1 * time.Second)
	}

	// 等待第一个窗口触发（应该在窗口大小10秒后，而不是滑动步长2秒后）
	timeout := time.After(12 * time.Second)
	firstWindowReceived := false

	for {
		select {
		case res := <-ch:
			if len(res) > 0 && !firstWindowReceived {
				firstWindowReceived = true
				windowTimingsMu.Lock()
				firstWindowTime := windowTimings[0]
				windowTimingsMu.Unlock()
				elapsed := firstWindowTime.Sub(firstDataTime)

				// 第一个窗口应该在窗口大小时间（10秒）后触发
				// 允许一些误差（±1秒），因为数据处理和调度可能有延迟
				assert.GreaterOrEqual(t, elapsed, 9*time.Second,
					"第一个窗口应该在窗口大小时间（10秒）后触发，实际耗时: %v", elapsed)
				assert.LessOrEqual(t, elapsed, 12*time.Second,
					"第一个窗口不应该太晚触发，实际耗时: %v", elapsed)

				// 验证第一个窗口不应该在滑动步长时间（2秒）后就触发
				assert.Greater(t, elapsed, 3*time.Second,
					"第一个窗口不应该在滑动步长时间（2秒）后就触发，实际耗时: %v", elapsed)

				cnt := res[0]["cnt"].(float64)
				assert.Greater(t, cnt, 0.0, "第一个窗口应该包含数据")
				t.Logf("第一个窗口触发时间: %v, 从第一个数据到触发耗时: %v, 窗口数据量: %.0f",
					firstWindowTime, elapsed, cnt)
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
        GROUP BY deviceId, SlidingWindow('10s', '2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 20)
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

	// 使用处理时间，每秒发送一条数据，共发送15条
	// 窗口大小10秒，滑动步长2秒
	// 使用处理时间时，窗口基于数据到达的处理时间
	for i := 0; i < 15; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(1 * time.Second)
	}

	// 等待足够的时间让多个窗口触发
	// 第一个窗口在10秒后触发，后续窗口每2秒触发一次
	// 等待足够的时间让至少3个窗口触发：10秒（第一个窗口）+ 2秒（第二个窗口）+ 2秒（第三个窗口）= 14秒
	time.Sleep(15 * time.Second)

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
		// 窗口大小10秒，每秒1条数据，应该包含约10条数据
		// 但由于窗口对齐和数据处理延迟，实际数量可能略有不同
		assert.GreaterOrEqual(t, firstCnt, 5.0,
			"第一个窗口应该包含足够的数据（窗口大小10秒，每秒1条），实际: %.0f", firstCnt)
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
		// 窗口大小10秒，每秒1条数据，应该包含约10条数据
		assert.GreaterOrEqual(t, secondCnt, 5.0,
			"第二个窗口应该包含足够的数据（窗口大小10秒，每秒1条），实际: %.0f", secondCnt)

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
			// 使用处理时间时，窗口大小10秒，每秒1条，应该包含约10条数据
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
        GROUP BY deviceId, SlidingWindow('10s', '2s')
    `
	err := ssql.Execute(sql)
	require.NoError(t, err)

	ch := make(chan []map[string]interface{}, 20)
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

	// 使用处理时间，每秒发送一条数据，共发送12条
	// 窗口大小10秒，滑动步长2秒
	// 使用处理时间时，窗口基于数据到达的处理时间
	for i := 0; i < 12; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(1 * time.Second)
	}

	// 等待多个窗口触发
	// 第一个窗口在10秒后触发，后续窗口每2秒触发一次
	// 等待足够的时间让至少3个窗口触发：10秒（第一个窗口）+ 2秒（第二个窗口）+ 2秒（第三个窗口）= 14秒
	time.Sleep(15 * time.Second)

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
			// 窗口大小10秒，每秒1条数据，应该包含约10条数据
			if i < 3 {
				assert.GreaterOrEqual(t, cnt, 5.0,
					"窗口 %d 应该包含足够的数据（窗口大小10秒，每秒1条），实际: %.0f", i+1, cnt)
			}
		}
	}
}
