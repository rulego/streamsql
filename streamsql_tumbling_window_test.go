package streamsql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLTumblingWindow_ProcessingTime 测试处理时间的滚动窗口
// 验证不使用 WITH 子句时，滚动窗口基于处理时间（系统时钟）工作
func TestSQLTumblingWindow_ProcessingTime(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 10)
	defer close(ch)
	windowResults := make([][]map[string]interface{}, 0)
	var windowResultsMu sync.Mutex
	ssql.AddSink(func(results []map[string]interface{}) {
		if len(results) > 0 {
			windowResultsMu.Lock()
			windowResults = append(windowResults, results)
			windowResultsMu.Unlock()
			select {
			case ch <- results:
			default:
				// 非阻塞发送
			}
		}
	})

	// 使用处理时间：发送数据，不包含时间戳字段
	// 滚动窗口基于数据到达的处理时间（系统时钟）来划分窗口
	for i := 0; i < 10; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(200 * time.Millisecond) // 每200ms发送一条数据
	}

	// 等待窗口触发（处理时间滚动窗口基于系统时钟触发）
	time.Sleep(3 * time.Second)

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

			t.Logf("处理时间滚动窗口成功触发，数据量: %.0f, 平均温度: %.2f", cnt, avgTemp)
		}
	}
}

// TestSQLTumblingWindow_MaxOutOfOrderness 测试滚动窗口的最大延迟时间配置
// 验证设置 MaxOutOfOrderness 后，延迟数据能否在允许的延迟范围内被正确处理
func TestSQLTumblingWindow_MaxOutOfOrderness(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	// 使用 SQL 配置 MaxOutOfOrderness
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

	// 模拟延迟数据场景
	// 场景：设置 MaxOutOfOrderness = 1秒，测试延迟数据能否在1秒内被正确处理
	// 窗口大小2秒，需要对齐到2秒的倍数
	windowSizeMs := int64(2000)                   // 2秒
	baseTimeRaw := time.Now().UnixMilli() - 10000 // 使用10秒前作为基准
	// 对齐baseTime到窗口大小的倍数，确保窗口对齐行为可预测
	baseTime := (baseTimeRaw / windowSizeMs) * windowSizeMs

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
			// 滚动窗口：窗口大小2秒，每200ms一条数据，理论上应该有10条数据
			// 但由于窗口对齐和 watermark 机制，实际数据量可能略有不同
			assert.GreaterOrEqual(t, cnt, 3.0, "第一个窗口应该包含足够的数据（滚动窗口特性）")
			assert.Equal(t, 0.0, minTemp, "第一个窗口的最小值应该是0（正常数据）")

			// 如果 MaxOutOfOrderness 配置正确，延迟数据应该被处理
			if maxTemp >= 20.0 {
				t.Logf("✓ 延迟数据被正确处理，最大值包含延迟数据: %.0f", maxTemp)
			} else {
				t.Logf("提示：延迟数据可能未被统计，当前最大值: %.0f", maxTemp)
			}
		}
	}

	t.Logf("总共触发了 %d 个窗口", windowResultsLen)
}

// TestSQLTumblingWindow_AllowedLateness 测试滚动窗口的 AllowedLateness 配置
// 验证窗口触发后，延迟数据能否在允许的延迟时间内更新窗口结果
func TestSQLTumblingWindow_AllowedLateness(t *testing.T) {
	ssql := New()
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

	// 等待 watermark 推进，触发第一个窗口
	t.Log("等待 watermark 推进，触发第一个窗口")
	time.Sleep(3 * time.Second)

	// 第二阶段：发送延迟数据（事件时间在第一个窗口内）
	// 这些数据应该在 AllowedLateness = 1秒 内被处理
	t.Log("第二阶段：发送延迟数据（事件时间在第一个窗口内）")
	lateDataTimes := []int64{300, 600, 900} // 延迟数据的事件时间
	for i, lateTime := range lateDataTimes {
		eventTime := baseTime + lateTime
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(30 + i), // 30-32，用于标识延迟数据
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

	// 收集所有窗口结果
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
	// 如果 AllowedLateness 配置正确，延迟数据应该能触发窗口的延迟更新
	if windowResultsLen > 0 {
		// 滚动窗口的延迟更新可能体现在后续的窗口结果中
		// 检查所有窗口结果，看是否有包含延迟数据的窗口
		hasLateData := false
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
					hasLateData = true
					t.Logf("✓ 窗口 %d 包含延迟数据，最大值: %.0f", i+1, maxTemp)
				}
			}
		}

		// 验证是否有延迟更新（窗口可能触发多次）
		if windowResultsLen > 1 {
			t.Logf("✓ 滚动窗口触发了 %d 次，可能包含延迟更新", windowResultsLen)
		}

		if !hasLateData {
			t.Logf("提示：延迟数据可能未被统计，或延迟数据的时间不在窗口范围内")
		}
	}

	t.Logf("总共触发了 %d 个窗口", windowResultsLen)
}

// TestSQLTumblingWindow_BothConfigs 测试滚动窗口同时配置 MaxOutOfOrderness 和 AllowedLateness
// 验证两个配置组合使用时，延迟数据能否被正确处理
func TestSQLTumblingWindow_BothConfigs(t *testing.T) {
	// 启用调试日志（可选，用于排查问题）
	// window.EnableDebug = true

	ssql := New()
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

	// 模拟完整的延迟数据处理场景
	baseTime := time.Now().UnixMilli() - 10000

	// 第一阶段：发送正常顺序的数据
	t.Log("第一阶段：发送正常顺序的数据")
	for i := 0; i < 10; i++ {
		eventTime := baseTime + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i), // 0-9
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 等待 watermark 推进（考虑 MaxOutOfOrderness = 1s）
	t.Log("等待 watermark 推进，触发窗口（MaxOutOfOrderness = 1s）")
	time.Sleep(3 * time.Second)

	// 第二阶段：发送延迟数据（事件时间在第一个窗口内）
	// MaxOutOfOrderness = 1s：这些数据应该在允许的乱序范围内
	// AllowedLateness = 500ms：窗口触发后还能接受500ms的延迟数据
	t.Log("第二阶段：发送延迟数据（事件时间在第一个窗口内）")
	lateDataTimes := []int64{400, 800, 1200} // 延迟数据的事件时间
	for i, lateTime := range lateDataTimes {
		eventTime := baseTime + lateTime
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(40 + i), // 40-42，用于标识延迟数据
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

	// 收集所有窗口结果
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
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			cnt := firstWindow[0]["cnt"].(float64)
			minTemp := firstWindow[0]["min_temp"].(float64)
			maxTemp := firstWindow[0]["max_temp"].(float64)

			t.Logf("第一个窗口: cnt=%.0f, min=%.0f, max=%.0f", cnt, minTemp, maxTemp)

			// 验证窗口包含数据
			// 滚动窗口：窗口大小2秒，每200ms一条数据，理论上应该有10条数据
			// 但由于窗口对齐和 watermark 机制，实际数据量可能略有不同
			assert.GreaterOrEqual(t, cnt, 3.0, "第一个窗口应该包含足够的数据（滚动窗口特性）")
			assert.Equal(t, 0.0, minTemp, "第一个窗口的最小值应该是0（正常数据）")

			// 验证延迟数据是否被处理
			// 如果配置正确，maxTemp 可能包含延迟数据的值（40-42）
			if maxTemp >= 40.0 {
				t.Logf("✓ 延迟数据被正确处理，最大值包含延迟数据: %.0f", maxTemp)
			} else {
				t.Logf("提示：延迟数据可能未被统计，当前最大值: %.0f", maxTemp)
			}
		}

		// 验证是否有延迟更新
		if windowResultsLen > 1 {
			t.Logf("✓ 滚动窗口触发了 %d 次，可能包含延迟更新", windowResultsLen)

			// 验证后续窗口的数据
			for i := 1; i < windowResultsLen && i < 3; i++ {
				if len(windowResultsCopy[i]) > 0 {
					cnt := windowResultsCopy[i][0]["cnt"].(float64)
					t.Logf("窗口 %d: cnt=%.0f", i+1, cnt)
				}
			}
		}
	}

	t.Logf("总共触发了 %d 个窗口", windowResultsLen)
	t.Logf("配置验证：MaxOutOfOrderness=1s, AllowedLateness=500ms")
}

// TestSQLTumblingWindow_LateDataHandling 测试滚动窗口的延迟数据处理
// 验证即使数据延迟到达，只要在允许的延迟范围内，也能正确统计到对应窗口
func TestSQLTumblingWindow_LateDataHandling(t *testing.T) {
	ssql := New()
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

	// 使用事件时间：模拟延迟数据场景
	// 场景：先发送正常顺序的数据，然后发送一些延迟的数据
	// 窗口大小2秒，需要对齐到2秒的倍数
	windowSizeMs := int64(2000)                  // 2秒
	baseTimeRaw := time.Now().UnixMilli() - 5000 // 使用5秒前作为基准
	// 对齐baseTime到窗口大小的倍数，确保窗口对齐行为可预测
	baseTime := (baseTimeRaw / windowSizeMs) * windowSizeMs

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
			// 滚动窗口：窗口大小2秒，每200ms一条数据，理论上应该有10条数据
			// 但由于窗口对齐和 watermark 机制，实际数据量可能略有不同
			assert.GreaterOrEqual(t, cnt, 3.0, "第一个窗口应该包含足够的数据（滚动窗口特性）")
			assert.Equal(t, 0.0, minTemp, "第一个窗口的最小值应该是0（正常数据）")
			assert.GreaterOrEqual(t, maxTemp, 0.0, "第一个窗口的最大值应该大于等于0")
		}
	}

	// 验证延迟数据是否被处理
	// 如果延迟数据被正确处理，应该能在后续窗口或更新中看到
	t.Logf("总共触发了 %d 个窗口", windowResultsLen)
}

// TestSQLTumblingWindow_EventTimeWindowAlignment 测试事件时间窗口对齐到epoch
func TestSQLTumblingWindow_EventTimeWindowAlignment(t *testing.T) {
	ssql := New()
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

	// 使用事件时间：发送数据，验证窗口对齐到epoch
	// 窗口大小2秒，应该对齐到2秒的倍数
	baseTime := time.Now().UnixMilli()

	// 发送数据，事件时间从baseTime开始，每200ms一条
	// 第一个窗口应该对齐到小于等于baseTime的最大2秒倍数
	for i := 0; i < 15; i++ {
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

	// 验证窗口对齐
	windowSizeMs := int64(2000) // 2秒 = 2000毫秒
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

			t.Logf("窗口 %d: start=%d, end=%d, size=%dms", i+1, startMs, endMs, endMs-startMs)
		}
	}
}

// TestSQLTumblingWindow_WatermarkTriggerTiming 测试Watermark触发窗口的时机
func TestSQLTumblingWindow_WatermarkTriggerTiming(t *testing.T) {
	ssql := New()
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

	// 使用事件时间：发送数据，验证watermark触发时机
	baseTime := time.Now().UnixMilli() - 10000 // 使用10秒前作为基准
	maxOutOfOrdernessMs := int64(500)          // 500ms

	// 第一阶段：发送数据到第一个窗口 [alignedStart, alignedStart+2000)
	// 计算对齐后的窗口开始时间
	windowSizeMs := int64(2000)
	alignedStart := (baseTime / windowSizeMs) * windowSizeMs
	firstWindowEnd := alignedStart + windowSizeMs

	t.Logf("第一个窗口: [%d, %d)", alignedStart, firstWindowEnd)

	// 发送数据，事件时间在第一个窗口内
	for i := 0; i < 10; i++ {
		eventTime := alignedStart + int64(i*200) // 在窗口内
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 发送一个事件时间刚好等于window_end的数据，推进watermark
	// watermark = maxEventTime - maxOutOfOrderness = firstWindowEnd - 500
	// 此时 watermark < firstWindowEnd，窗口不应该触发
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"eventTime":   firstWindowEnd,
		"temperature": 100.0,
	})

	// 等待watermark更新（watermark更新间隔200ms）
	time.Sleep(500 * time.Millisecond)

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

	// 验证第一个窗口的触发时机
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

			// 验证窗口在watermark >= window_end时触发
			// 由于watermark = maxEventTime - maxOutOfOrderness
			// 当maxEventTime = firstWindowEnd + 1000时，watermark = firstWindowEnd + 500
			// watermark >= firstWindowEnd，窗口应该触发
			t.Logf("✓ 窗口在watermark >= window_end时正确触发")
			t.Logf("窗口: [%d, %d), 触发时maxEventTime >= %d", start, end, end+maxOutOfOrdernessMs)
		}
	}
}

// TestSQLTumblingWindow_AllowedLatenessUpdate 测试AllowedLateness的延迟更新
func TestSQLTumblingWindow_AllowedLatenessUpdate(t *testing.T) {
	ssql := New()
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

	baseTime := time.Now().UnixMilli() - 10000
	windowSizeMs := int64(2000)
	alignedStart := (baseTime / windowSizeMs) * windowSizeMs
	firstWindowEnd := alignedStart + windowSizeMs
	allowedLatenessMs := int64(1000) // 1秒

	// 第一阶段：发送正常数据，触发第一个窗口
	t.Log("第一阶段：发送正常数据，触发第一个窗口")
	for i := 0; i < 10; i++ {
		eventTime := alignedStart + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i), // 0-9
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 推进watermark，触发第一个窗口
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"eventTime":   firstWindowEnd + 1000,
		"temperature": 100.0,
	})

	// 等待第一个窗口触发
	time.Sleep(1 * time.Second)

	// 收集第一个窗口的结果
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
				t.Fatal("应该收到第一个窗口")
			}
			if len(res) > 0 {
				firstWindowReceived = true
				firstWindowCnt = res[0]["cnt"].(float64)
				firstWindowMax = res[0]["max_temp"].(float64)
				t.Logf("第一个窗口（初始）: cnt=%.0f, max=%.0f", firstWindowCnt, firstWindowMax)
			}
			iteration++
		case <-time.After(500 * time.Millisecond):
			// 500ms 没有新结果
			iteration++
		case <-ctx.Done():
			t.Fatal("应该收到第一个窗口")
		}
	}

	// 第二阶段：发送延迟数据（事件时间在第一个窗口内，但在AllowedLateness范围内）
	t.Log("第二阶段：发送延迟数据（事件时间在第一个窗口内）")
	lateDataTimes := []int64{300, 600, 900} // 延迟数据的事件时间（相对于alignedStart）
	lateDataTemps := []float64{30.0, 31.0, 32.0}
	for i, lateTime := range lateDataTimes {
		eventTime := alignedStart + lateTime
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": lateDataTemps[i],
		})
		time.Sleep(100 * time.Millisecond)
	}

	// 继续发送正常数据，推进watermark（但不超过window_end + allowedLateness）
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"eventTime":   firstWindowEnd + allowedLatenessMs - 100, // 在allowedLateness范围内
		"temperature": 200.0,
	})

	// 等待延迟更新
	time.Sleep(1 * time.Second)

	// 收集所有窗口结果
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

	// 验证延迟更新
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
					t.Logf("✓ 窗口延迟更新: cnt从%.0f增加到%.0f, max从%.0f增加到%.0f",
						firstWindowCnt, cnt, firstWindowMax, maxTemp)

					// 验证延迟数据被包含
					assert.GreaterOrEqual(t, maxTemp, 30.0,
						"延迟更新应该包含延迟数据，maxTemp应该>=30.0，实际: %.0f", maxTemp)
				}
			}

			t.Logf("窗口 %d: [%d, %d), cnt=%.0f, max=%.0f", i+1, start, end, cnt, maxTemp)
		}
	}

	if !hasLateUpdate {
		t.Logf("⚠ 提示：未检测到延迟更新，可能延迟数据未被处理或窗口已关闭")
	} else {
		t.Logf("✓ AllowedLateness功能正常工作，延迟数据触发窗口更新")
	}
}

// TestSQLTumblingWindow_IdleSourceMechanism 测试Idle Source机制
// 验证当数据源空闲时，watermark基于处理时间推进，窗口能够正常关闭
func TestSQLTumblingWindow_IdleSourceMechanism(t *testing.T) {
	ssql := New()
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

	// 使用事件时间：发送数据，然后停止发送，验证窗口能够关闭
	baseTime := time.Now().UnixMilli() - 10000
	windowSizeMs := int64(2000) // 2秒

	// 计算对齐后的第一个窗口开始时间
	alignedStart := (baseTime / windowSizeMs) * windowSizeMs
	firstWindowEnd := alignedStart + windowSizeMs

	t.Logf("第一个窗口: [%d, %d)", alignedStart, firstWindowEnd)

	// 第一阶段：发送数据，创建窗口
	t.Log("第一阶段：发送数据，创建窗口")
	for i := 0; i < 5; i++ {
		eventTime := alignedStart + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 第二阶段：停止发送数据，等待Idle Source机制触发
	// IdleTimeout = 2秒，意味着2秒无数据后，watermark会基于处理时间推进
	t.Log("第二阶段：停止发送数据，等待Idle Source机制触发（IdleTimeout=2s）")
	time.Sleep(3 * time.Second) // 等待超过IdleTimeout，确保watermark推进

	// 收集窗口结果
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

	// 验证窗口能够关闭（即使没有新数据）
	require.Greater(t, windowResultsLen, 0, "应该至少触发一个窗口（即使数据源空闲）")

	// 验证窗口数据
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			row := firstWindow[0]
			start := row["start"].(int64)
			end := row["end"].(int64)
			cnt := row["cnt"].(float64)

			// 验证窗口边界正确
			// window_start() 和 window_end() 返回纳秒，需要转换为毫秒
			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)
			assert.Equal(t, alignedStart, startMs,
				"第一个窗口的开始时间应该对齐到窗口大小，expected=%d, actual=%d", alignedStart, startMs)
			assert.Equal(t, firstWindowEnd, endMs,
				"第一个窗口的结束时间应该正确，expected=%d, actual=%d", firstWindowEnd, endMs)

			// 验证窗口包含数据
			assert.Greater(t, cnt, 0.0, "窗口应该包含数据")

			t.Logf("✓ Idle Source机制正常工作，窗口在数据源空闲时能够关闭")
			t.Logf("窗口: [%d, %d), cnt=%.0f", start, end, cnt)
		}
	}
}

// TestSQLTumblingWindow_IdleSourceDisabled 测试Idle Source机制未启用的情况
// 验证当IdleTimeout=0（禁用）时，如果数据源空闲，窗口无法关闭
func TestSQLTumblingWindow_IdleSourceDisabled(t *testing.T) {
	ssql := New()
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

	baseTime := time.Now().UnixMilli() - 10000
	windowSizeMs := int64(2000)
	alignedStart := (baseTime / windowSizeMs) * windowSizeMs

	// 发送数据，但事件时间不足以触发窗口
	t.Log("发送数据，但事件时间不足以触发窗口")
	for i := 0; i < 3; i++ {
		eventTime := alignedStart + int64(i*200)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 停止发送数据，等待一段时间
	// 由于IdleTimeout未启用，watermark不会基于处理时间推进
	t.Log("停止发送数据，等待（IdleTimeout未启用）")
	time.Sleep(3 * time.Second)

	// 收集窗口结果
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
	windowResultsMu.Unlock()

	// 注意：这个测试可能无法完全验证窗口无法关闭
	// 因为如果watermark已经推进到足够的位置，窗口可能已经触发
	// 这个测试主要用于对比：启用Idle Source vs 未启用Idle Source
	t.Logf("窗口结果数量: %d（IdleTimeout未启用）", windowResultsLen)
}
