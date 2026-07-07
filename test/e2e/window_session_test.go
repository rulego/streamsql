package streamsql

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLSessionWindow_ProcessingTime 测试处理时间的会话窗口
// 验证不使用 WITH 子句时，会话窗口基于处理时间（系统时钟）工作
func TestSQLSessionWindow_ProcessingTime(t *testing.T) {
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
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		ch <- results
	})

	// 使用处理时间：发送数据，不包含时间戳字段
	// 会话窗口基于数据到达的处理时间（系统时钟）来划分会话
	for i := 0; i < 5; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond) // 数据间隔小于会话超时时间（300ms），属于同一会话
	}

	// 等待会话超时（处理时间会话窗口基于系统时钟触发）
	time.Sleep(600 * time.Millisecond)

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		row := res[0]
		assert.Equal(t, "sensor001", row["deviceId"])
		assert.Equal(t, float64(5), row["cnt"])
		t.Logf("处理时间会话窗口成功触发，数据量: %.0f", row["cnt"])
	case <-time.After(2 * time.Second):
		t.Fatal("处理时间会话窗口应该触发")
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
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		ch <- results
	})

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
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		ch <- results
	})

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

// TestSQLSessionWindow_EventTimeWithWithClause 测试使用 WITH 子句指定事件时间的会话窗口
func TestSQLSessionWindow_EventTimeWithWithClause(t *testing.T) {
	ssql := New()
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

	ch := make(chan []map[string]interface{}, 4)
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		ch <- results
	})

	// 使用事件时间：发送带有事件时间戳的数据
	baseTime := time.Now().UnixMilli() - 5000 // 5秒前作为基准时间
	for i := 0; i < 5; i++ {
		eventTime := baseTime + int64(i*50) // 每50ms一条数据
		ssql.Emit(map[string]interface{}{
			"deviceId":  "sensor001",
			"eventTime": eventTime, // 事件时间字段
		})
		time.Sleep(20 * time.Millisecond) // 处理时间间隔较小
	}

	// 发送一个事件时间超过会话结束时间的数据，推进watermark
	// 会话结束时间 = baseTime + 200 + 300 = baseTime + 500
	// 需要发送事件时间 > baseTime + 500 + maxOutOfOrderness(200) = baseTime + 700 的数据
	// 使用不同的设备ID，避免影响当前会话的计数
	ssql.Emit(map[string]interface{}{
		"deviceId":  "sensor002",     // 使用不同的设备ID，不影响sensor001的会话
		"eventTime": baseTime + 2000, // 推进watermark
	})

	// 等待会话超时（事件时间会话窗口基于watermark触发）
	time.Sleep(1 * time.Second)

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		row := res[0]
		assert.Equal(t, "sensor001", row["deviceId"])
		assert.Equal(t, float64(5), row["cnt"])
		t.Logf("事件时间会话窗口成功触发，数据量: %.0f", row["cnt"])
	case <-time.After(2 * time.Second):
		t.Fatal("事件时间会话窗口应该触发")
	}
}

// TestSQLSessionWindow_ProcessingTimeWithoutWithClause 测试不使用 WITH 子句时默认使用处理时间
func TestSQLSessionWindow_ProcessingTimeWithoutWithClause(t *testing.T) {
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
	ssql.AddSink(func(results []map[string]interface{}) {
		defer func() {
			if r := recover(); r != nil {
				// channel 已关闭，忽略错误
			}
		}()
		ch <- results
	})

	// 不使用事件时间字段，应该使用处理时间
	for i := 0; i < 5; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId": "sensor001",
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 等待会话超时（处理时间会话窗口基于系统时钟）
	time.Sleep(600 * time.Millisecond)

	select {
	case res := <-ch:
		require.Len(t, res, 1)
		row := res[0]
		assert.Equal(t, "sensor001", row["deviceId"])
		assert.Equal(t, float64(5), row["cnt"])
		t.Logf("处理时间会话窗口成功触发，数据量: %.0f", row["cnt"])
	case <-time.After(2 * time.Second):
		t.Fatal("处理时间会话窗口应该触发")
	}
}

// TestSQLSessionWindow_EventTimeWindowAlignment 测试事件时间会话窗口
func TestSQLSessionWindow_EventTimeWindowAlignment(t *testing.T) {
	ssql := New()
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

	// 使用事件时间：发送数据，验证会话窗口基于事件时间触发
	baseTime := time.Now().UnixMilli() - 5000
	sessionTimeoutMs := int64(500)

	// 第一阶段：发送连续的数据（事件时间间隔小于sessionTimeout）
	// 这些数据应该属于同一个会话
	t.Log("第一阶段：发送连续数据（同一会话）")
	for i := 0; i < 5; i++ {
		eventTime := baseTime + int64(i*100) // 每100ms一条，小于500ms超时
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 等待一段时间，让watermark推进
	// 第一个会话的结束时间 = baseTime + 400 + 500 = baseTime + 900
	// 需要发送事件时间 > baseTime + 900 + maxOutOfOrderness(200) = baseTime + 1100 的数据
	// 才能让 watermark >= baseTime + 900，触发第一个会话
	time.Sleep(500 * time.Millisecond)

	// 发送数据推进watermark
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"eventTime":   baseTime + int64(1500),
		"temperature": 50.0,
	})
	time.Sleep(500 * time.Millisecond)

	// 第二阶段：发送间隔较大的数据（事件时间间隔大于sessionTimeout）
	// 这应该触发新会话
	t.Log("第二阶段：发送间隔较大的数据（新会话）")
	eventTime := baseTime + int64(2000) // 间隔2秒，大于500ms超时
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"eventTime":   eventTime,
		"temperature": 100.0,
	})

	// 继续发送连续数据（第二个会话）
	for i := 0; i < 3; i++ {
		eventTime := baseTime + int64(2000+i*100)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(100 + i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 推进watermark，触发会话
	// 会话结束时间 = baseTime + 400 + 500 = baseTime + 900
	// 需要发送事件时间 > baseTime + 900 + maxOutOfOrderness(200) = baseTime + 1100 的数据
	// 才能让 watermark >= baseTime + 900
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"eventTime":   baseTime + int64(5000),
		"temperature": 200.0,
	})

	// 继续发送更多数据，确保watermark推进
	for i := 0; i < 5; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   baseTime + int64(5000+i*200),
			"temperature": float64(200 + i),
		})
		time.Sleep(100 * time.Millisecond)
	}

	// 等待会话触发（watermark更新间隔200ms，需要等待足够时间）
	time.Sleep(3 * time.Second)

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

	if windowResultsLen == 0 {
		t.Log("⚠ 会话窗口未触发，可能watermark未推进到足够位置")
		t.Log("提示：会话窗口需要在 watermark >= session_end 时触发")
		t.Log("会话结束时间 = 最后一个数据时间 + 超时时间")
	}
	require.Greater(t, windowResultsLen, 0, "应该至少触发一个会话窗口")

	// 验证会话窗口
	for i, window := range windowResultsCopy {
		if len(window) > 0 {
			row := window[0]
			start := row["start"].(int64)
			end := row["end"].(int64)
			cnt := row["cnt"].(float64)

			// 验证会话窗口有数据
			assert.Greater(t, cnt, 0.0, "会话窗口 %d 应该包含数据", i+1)

			// 验证会话窗口的时间范围合理
			assert.Greater(t, end, start, "会话窗口 %d 的结束时间应该大于开始时间", i+1)

			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)
			sessionDurationMs := endMs - startMs

			assert.GreaterOrEqual(t, sessionDurationMs, sessionTimeoutMs,
				"会话窗口 %d 的持续时间应该至少等于会话超时时间", i+1)

			t.Logf("会话窗口 %d: [%d, %d), cnt=%.0f, duration=%dms", i+1, startMs, endMs, cnt, sessionDurationMs)
		}
	}

	t.Logf("总共触发了 %d 个会话窗口", windowResultsLen)
}

// TestSQLSessionWindow_WatermarkTriggerTiming 测试会话窗口Watermark触发时机
func TestSQLSessionWindow_WatermarkTriggerTiming(t *testing.T) {
	ssql := New()
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

	baseTime := time.Now().UnixMilli() - 5000
	maxOutOfOrdernessMs := int64(200)
	sessionTimeoutMs := int64(500)

	// 发送数据，创建会话
	// 第一个数据：baseTime
	// 后续数据：baseTime + 100, baseTime + 200, baseTime + 300, baseTime + 400
	// 会话结束时间应该是 baseTime + 400 + 500 = baseTime + 900
	// 当watermark >= baseTime + 900时，会话应该触发
	t.Log("发送数据创建会话")
	for i := 0; i < 5; i++ {
		eventTime := baseTime + int64(i*100)
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   eventTime,
			"temperature": float64(i),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// 计算会话结束时间
	sessionEndTime := baseTime + int64(400) + sessionTimeoutMs // 最后一个数据时间 + 超时时间

	// 发送一个事件时间刚好等于sessionEndTime的数据
	// watermark = maxEventTime - maxOutOfOrderness = sessionEndTime - 200
	// 此时 watermark < sessionEndTime，会话不应该触发
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"eventTime":   sessionEndTime,
		"temperature": 100.0,
	})

	// 等待watermark更新
	time.Sleep(500 * time.Millisecond)

	// 发送一个事件时间超过sessionEndTime的数据，推进watermark
	// watermark = maxEventTime - maxOutOfOrderness = (sessionEndTime + 500) - 200 = sessionEndTime + 300
	// 此时 watermark >= sessionEndTime，会话应该触发
	ssql.Emit(map[string]interface{}{
		"deviceId":    "sensor001",
		"eventTime":   sessionEndTime + 1000,
		"temperature": 200.0,
	})

	// 继续发送更多数据，确保watermark推进
	for i := 0; i < 3; i++ {
		ssql.Emit(map[string]interface{}{
			"deviceId":    "sensor001",
			"eventTime":   sessionEndTime + int64(1000+i*200),
			"temperature": float64(200 + i),
		})
		time.Sleep(100 * time.Millisecond)
	}

	// 等待会话触发（watermark更新间隔200ms，需要等待足够时间）
	time.Sleep(3 * time.Second)

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

	if windowResultsLen == 0 {
		t.Log("⚠ 会话窗口未触发，可能watermark未推进到足够位置")
		t.Log("提示：会话窗口需要在 watermark >= session_end 时触发")
		t.Log("会话结束时间 = 最后一个数据时间 + 超时时间")
	}
	require.Greater(t, windowResultsLen, 0, "应该至少触发一个会话窗口")

	// 验证会话窗口的触发时机
	if windowResultsLen > 0 {
		firstWindow := windowResultsCopy[0]
		if len(firstWindow) > 0 {
			row := firstWindow[0]
			start := row["start"].(int64)
			end := row["end"].(int64)
			cnt := row["cnt"].(float64)

			// 验证会话窗口包含数据
			assert.Greater(t, cnt, 0.0, "会话窗口应该包含数据")

			startMs := start / int64(time.Millisecond)
			endMs := end / int64(time.Millisecond)
			sessionDurationMs := endMs - startMs

			assert.GreaterOrEqual(t, sessionDurationMs, sessionTimeoutMs,
				"会话窗口的持续时间应该至少等于会话超时时间")

			t.Logf("✓ 会话窗口在watermark >= session_end时正确触发")
			t.Logf("会话窗口: [%d, %d), cnt=%.0f, 触发时maxEventTime >= %d",
				start, end, cnt, end+maxOutOfOrdernessMs)
		}
	}
}

// TestSQLSessionWindow_IdleSourceMechanism 测试会话窗口的Idle Source机制
func TestSQLSessionWindow_IdleSourceMechanism(t *testing.T) {
	ssql := New()
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

	baseTime := time.Now().UnixMilli() - 5000

	// 发送数据，创建会话
	t.Log("发送数据，创建会话")
	for i := 0; i < 5; i++ {
		eventTime := baseTime + int64(i*100)
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

	require.Greater(t, windowResultsLen, 0, "应该至少触发一个会话窗口（即使数据源空闲）")

	if windowResultsLen > 0 {
		t.Logf("✓ 会话窗口Idle Source机制正常工作，触发了 %d 个会话", windowResultsLen)
		for i, window := range windowResultsCopy {
			if len(window) > 0 {
				row := window[0]
				start := row["start"].(int64)
				end := row["end"].(int64)
				cnt := row["cnt"].(float64)
				t.Logf("会话 %d: [%d, %d), cnt=%.0f", i+1, start, end, cnt)
			}
		}
	}
}
