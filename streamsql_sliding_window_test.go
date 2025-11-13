package streamsql

import (
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
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 12; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": i,
			"timestamp":   baseTime.Add(time.Duration(i) * time.Second),
		})
		time.Sleep(10 * time.Millisecond)
	}

	results := make([][]map[string]interface{}, 0)
	timeout := time.After(15 * time.Second)
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
	assert.Greater(t, len(results), 0)
	if len(results) > 0 {
		firstWindow := results[0]
		require.Len(t, firstWindow, 1)
		cnt := firstWindow[0]["cnt"].(float64)
		assert.Greater(t, cnt, 0.0)
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

	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 15; i++ {
		timestamp := baseTime.Add(time.Duration(i) * time.Second)
		temperature := float64(i)

		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": temperature,
			"timestamp":   timestamp,
		})
		time.Sleep(10 * time.Millisecond)
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
	ssql.AddSink(func(results []map[string]interface{}) {
		ch <- results
	})

	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 15; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": float64(i),
			"timestamp":   baseTime.Add(time.Duration(i) * time.Second),
		})
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(8 * time.Second)

	timeout := time.After(2 * time.Second)
	for {
		select {
		case res := <-ch:
			if len(res) > 0 {
				windowResults = append(windowResults, res)
			}
		case <-timeout:
			goto END
		}
	}

END:
	require.Greater(t, len(windowResults), 0, "应该至少触发一个窗口")

	for i, res := range windowResults {
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

	if len(windowResults) > 1 {
		firstWindow := windowResults[0]
		lastWindow := windowResults[len(windowResults)-1]

		firstCnt := firstWindow[0]["cnt"].(float64)
		lastCnt := lastWindow[0]["cnt"].(float64)
		firstMin := firstWindow[0]["min_temp"].(float64)
		lastMin := lastWindow[0]["min_temp"].(float64)

		assert.GreaterOrEqual(t, firstCnt, lastCnt,
			"第一个窗口应该包含不少于最后一个窗口的数据")
		assert.LessOrEqual(t, firstMin, lastMin,
			"第一个窗口的最小值应该小于等于最后一个窗口的最小值")
	}

	allCounts := make([]float64, len(windowResults))
	for i, res := range windowResults {
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

	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 8; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "A",
			"region":      "R1",
			"temperature": float64(i),
			"timestamp":   baseTime.Add(time.Duration(i) * time.Second),
		})
		ssql.Emit(map[string]interface{}{
			"deviceId":    "B",
			"region":      "R1",
			"temperature": float64(i + 10),
			"timestamp":   baseTime.Add(time.Duration(i) * time.Second),
		})
		ssql.Emit(map[string]interface{}{
			"deviceId":    "A",
			"region":      "R2",
			"temperature": float64(i + 20),
			"timestamp":   baseTime.Add(time.Duration(i) * time.Second),
		})
		ssql.Emit(map[string]interface{}{
			"deviceId":    "B",
			"region":      "R2",
			"temperature": float64(i + 30),
			"timestamp":   baseTime.Add(time.Duration(i) * time.Second),
		})
		time.Sleep(10 * time.Millisecond)
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
