package stream

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamProcess(t *testing.T) {
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: map[string]interface{}{"size": 500 * time.Millisecond}, // 减少窗口大小以更快触发
		},
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.Avg,
			"humidity":    aggregator.Sum,
		},
		NeedWindow: true,
	}

	strm, err := NewStream(config)
	require.NoError(t, err)

	err = strm.RegisterFilter("device == 'aa' && temperature > 10")
	require.NoError(t, err)

	// 添加 Sink 函数来捕获结果
	resultChan := make(chan interface{}, 1) // 添加缓冲
	strm.AddSink(func(result interface{}) {
		select {
		case resultChan <- result:
		default:
			// 防止阻塞
		}
	})

	strm.Start()

	// 准备测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0, "humidity": 60},
		map[string]interface{}{"device": "aa", "temperature": 30.0, "humidity": 55},
		map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70},
	}

	for _, data := range testData {
		strm.Emit(data)
	}

	// 等待窗口关闭并触发结果
	time.Sleep(700 * time.Millisecond) // 等待窗口关闭

	// 等待结果，并设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan: // 从sink的channel读取
		cancel()
	case <-ctx.Done():
		t.Fatal("No results received within 3 seconds")
	}

	// 预期结果：只有 device='aa' 且 temperature>10 的数据会被聚合
	expected := map[string]interface{}{
		"device":      "aa",
		"temperature": 27.5,  // (25+30)/2
		"humidity":    115.0, // 60+55
	}

	// 验证结果
	t.Logf("Received result: %+v (type: %T)", actual, actual)
	if actual == nil {
		t.Fatal("Received nil result")
	}
	assert.IsType(t, []map[string]interface{}{}, actual)
	t.Logf("Type assertion successful")
	resultMap := actual.([]map[string]interface{})
	t.Logf("Result map length: %d", len(resultMap))
	if len(resultMap) > 0 {
		t.Logf("First result: %+v", resultMap[0])

		// 检查temperature字段
		if tempAvg, ok := resultMap[0]["temperature"]; ok {
			t.Logf("temperature: %+v (type: %T)", tempAvg, tempAvg)
			assert.InEpsilon(t, expected["temperature"].(float64), tempAvg.(float64), 0.0001)
		} else {
			t.Fatal("temperature field not found in result")
		}

		// 检查humidity字段
		if humSum, ok := resultMap[0]["humidity"]; ok {
			t.Logf("humidity: %+v (type: %T)", humSum, humSum)
			assert.InDelta(t, expected["humidity"].(float64), humSum.(float64), 0.0001)
		} else {
			t.Fatal("humidity field not found in result")
		}
	} else {
		t.Fatal("No results in result map")
	}
}

// 不设置过滤器
func TestStreamWithoutFilter(t *testing.T) {
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:   "sliding",
			Params: map[string]interface{}{"size": 2 * time.Second, "slide": 1 * time.Second},
		},
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.Max,
			"humidity":    aggregator.Min,
		},
		NeedWindow: true,
	}

	strm, err := NewStream(config)
	require.NoError(t, err)

	strm.Start()

	testData := []interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0, "humidity": 60},
		map[string]interface{}{"device": "aa", "temperature": 30.0, "humidity": 55},
		map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70},
	}

	for _, data := range testData {
		strm.Emit(data)
	}

	// 捕获结果
	resultChan := make(chan interface{})
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})
	// 等待 3 秒触发窗口
	time.Sleep(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		cancel()
	case <-ctx.Done():
		t.Fatal("Timeout waiting for results")
	}

	expected := []map[string]interface{}{
		{
			"device":      "aa",
			"temperature": 30.0,
			"humidity":    55.0,
		},
		{
			"device":      "bb",
			"temperature": 22.0,
			"humidity":    70.0,
		},
	}

	assert.IsType(t, []map[string]interface{}{}, actual)
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok)

	assert.Len(t, resultSlice, 2)
	for _, expectedResult := range expected {
		found := false
		for _, resultMap := range resultSlice {
			if resultMap["device"] == expectedResult["device"] {
				assert.InEpsilon(t, expectedResult["temperature"].(float64), resultMap["temperature"].(float64), 0.0001)
				assert.InEpsilon(t, expectedResult["humidity"].(float64), resultMap["humidity"].(float64), 0.0001)
				found = true
				break
			}
		}
		assert.True(t, found, fmt.Sprintf("Expected result for device %v not found", expectedResult["device"]))
	}
}

func TestIncompleteStreamProcess(t *testing.T) {
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: map[string]interface{}{"size": 500 * time.Millisecond}, // 减少窗口大小
		},
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.Avg,
			"humidity":    aggregator.Sum,
		},
		NeedWindow: true,
	}

	strm, err := NewStream(config)
	require.NoError(t, err)

	err = strm.RegisterFilter("device == 'aa' ")
	require.NoError(t, err)

	// 添加 Sink 函数来捕获结果
	resultChan := make(chan interface{}, 1) // 添加缓冲
	strm.AddSink(func(result interface{}) {
		select {
		case resultChan <- result:
		default:
			// 防止阻塞
		}
	})

	strm.Start()

	// 准备测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0},
		map[string]interface{}{"device": "aa", "humidity": 60},
		map[string]interface{}{"device": "aa", "temperature": 30.0},
		map[string]interface{}{"device": "aa", "humidity": 55},
		map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70},
	}

	for _, data := range testData {
		strm.Emit(data)
	}

	// 等待窗口关闭并触发结果
	time.Sleep(700 * time.Millisecond) // 等待窗口关闭

	// 等待结果，并设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan: // 从sink的channel读取
		cancel()
	case <-ctx.Done():
		t.Fatal("No results received within 3 seconds")
	}

	// 预期结果：只有 device='aa' 且 temperature>10 的数据会被聚合
	expected := map[string]interface{}{
		"device":      "aa",
		"temperature": 27.5,  // (25+30)/2
		"humidity":    115.0, // 60+55
	}

	// 验证结果
	t.Logf("Received result: %+v (type: %T)", actual, actual)
	if actual == nil {
		t.Fatal("Received nil result")
	}
	assert.IsType(t, []map[string]interface{}{}, actual)
	t.Logf("Type assertion successful")
	resultMap := actual.([]map[string]interface{})
	t.Logf("Result map length: %d", len(resultMap))
	if len(resultMap) > 0 {
		t.Logf("First result: %+v", resultMap[0])

		// 检查temperature字段
		if tempAvg, ok := resultMap[0]["temperature"]; ok {
			t.Logf("temperature: %+v (type: %T)", tempAvg, tempAvg)
			assert.InEpsilon(t, expected["temperature"].(float64), tempAvg.(float64), 0.0001)
		} else {
			t.Fatal("temperature field not found in result")
		}

		// 检查humidity字段
		if humSum, ok := resultMap[0]["humidity"]; ok {
			t.Logf("humidity: %+v (type: %T)", humSum, humSum)
			assert.InDelta(t, expected["humidity"].(float64), humSum.(float64), 0.0001)
		} else {
			t.Fatal("humidity field not found in result")
		}
	} else {
		t.Fatal("No results in result map")
	}
}

func TestWindowSlotAgg(t *testing.T) {
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:   "sliding",
			Params: map[string]interface{}{"size": 2 * time.Second, "slide": 1 * time.Second},
			TsProp: "ts",
		},
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.Max,
			"humidity":    aggregator.Min,
			"start":       aggregator.WindowStart,
			"end":         aggregator.WindowEnd,
		},
		NeedWindow: true,
	}

	strm, err := NewStream(config)
	require.NoError(t, err)

	strm.Start()
	// Add data every 500ms
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	testData := []interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0, "humidity": 60, "ts": baseTime},
		map[string]interface{}{"device": "aa", "temperature": 30.0, "humidity": 55, "ts": baseTime.Add(1 * time.Second)},
		map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70, "ts": baseTime},
	}

	for _, data := range testData {
		strm.Emit(data)
	}

	// 捕获结果
	resultChan := make(chan interface{})
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})
	// 等待 3 秒触发窗口
	time.Sleep(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		cancel()
	case <-ctx.Done():
		t.Fatal("Timeout waiting for results")
	}

	expected := []map[string]interface{}{
		{
			"device":      "aa",
			"temperature": 30.0,
			"humidity":    55.0,
			"start":       baseTime.UnixNano(),
			"end":         baseTime.Add(2 * time.Second).UnixNano(),
		},
		{
			"device":      "bb",
			"temperature": 22.0,
			"humidity":    70.0,
			"start":       baseTime.UnixNano(),
			"end":         baseTime.Add(2 * time.Second).UnixNano(),
		},
	}

	assert.IsType(t, []map[string]interface{}{}, actual)
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok)

	assert.Len(t, resultSlice, 2)
	for _, expectedResult := range expected {
		found := false
		for _, resultMap := range resultSlice {
			if resultMap["device"] == expectedResult["device"] {
				assert.InEpsilon(t, expectedResult["temperature"].(float64), resultMap["temperature"].(float64), 0.0001)
				assert.InEpsilon(t, expectedResult["humidity"].(float64), resultMap["humidity"].(float64), 0.0001)
				assert.Equal(t, expectedResult["start"].(int64), resultMap["start"].(int64))
				assert.Equal(t, expectedResult["end"].(int64), resultMap["end"].(int64))
				found = true
				break
			}
		}
		assert.True(t, found, fmt.Sprintf("Expected result for device %v not found", expectedResult["device"]))
	}
}

// TestPersistenceManagerBasic 测试持久化管理器基本功能
func TestPersistenceManagerBasic(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()

	// 创建持久化管理器
	pm := NewPersistenceManagerWithConfig(tmpDir, 1024, 100*time.Millisecond)

	// 启动管理器
	err := pm.Start()
	require.NoError(t, err)
	defer pm.Stop()

	// 测试数据持久化
	testData := []interface{}{
		map[string]interface{}{"id": 1, "value": "test1"},
		map[string]interface{}{"id": 2, "value": "test2"},
		map[string]interface{}{"id": 3, "value": "test3"},
	}

	// 写入数据
	for _, data := range testData {
		err := pm.PersistData(data)
		require.NoError(t, err)
	}

	// 等待数据写入
	time.Sleep(200 * time.Millisecond)

	// 读取持久化数据
	loadedData, err := pm.LoadPersistedData()
	require.NoError(t, err)

	// 验证数据
	assert.GreaterOrEqual(t, len(loadedData), len(testData))
}

// TestPersistenceManagerFileRotation 测试文件轮转功能
func TestPersistenceManagerFileRotation(t *testing.T) {
	tmpDir := t.TempDir()

	// 创建小文件大小的持久化管理器以触发文件轮转
	pm := NewPersistenceManagerWithConfig(tmpDir, 100, 50*time.Millisecond)

	err := pm.Start()
	require.NoError(t, err)
	defer pm.Stop()

	// 写入大量数据以触发文件轮转
	for i := 0; i < 50; i++ {
		data := map[string]interface{}{
			"id":    i,
			"value": fmt.Sprintf("test_data_with_long_content_%d", i),
		}
		err := pm.PersistData(data)
		require.NoError(t, err)
	}

	// 等待数据写入和文件轮转
	time.Sleep(200 * time.Millisecond)

	// 验证创建了多个文件
	files, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	assert.Greater(t, len(files), 1, "应该创建多个持久化文件")
}

// TestStreamWithPersistenceStrategy 测试流处理器的持久化策略
func TestStreamWithPersistenceStrategy(t *testing.T) {
	tmpDir := t.TempDir()

	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: map[string]interface{}{"size": 100 * time.Millisecond},
		},
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.Avg,
		},
		NeedWindow: true,
	}

	// 创建带持久化策略的流处理器，使用小缓冲区以触发持久化
	stream, err := NewStreamWithLossPolicyAndPersistence(config,
		2, 2, 2, // 小缓冲区
		"persist", 100*time.Millisecond,
		tmpDir, 1024, 50*time.Millisecond)
	require.NoError(t, err)
	defer stream.Stop()

	// 添加结果收集器
	var results []interface{}
	var resultMutex sync.Mutex
	stream.AddSink(func(result interface{}) {
		resultMutex.Lock()
		defer resultMutex.Unlock()
		results = append(results, result)
	})

	stream.Start()

	// 快速添加大量数据以触发持久化
	for i := 0; i < 20; i++ {
		data := map[string]interface{}{
			"device":      fmt.Sprintf("device_%d", i%3),
			"temperature": float64(20 + i),
			"timestamp":   time.Now(),
		}
		stream.Emit(data)
	}

	// 等待处理完成
	time.Sleep(300 * time.Millisecond)

	// 验证持久化文件已创建
	files, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	if len(files) > 0 {
		t.Logf("创建了 %d 个持久化文件", len(files))
	}

	// 验证可以加载持久化数据
	if stream.persistenceManager != nil {
		loadedData, err := stream.persistenceManager.LoadPersistedData()
		require.NoError(t, err)
		t.Logf("加载了 %d 条持久化数据", len(loadedData))
	}
}

// TestStreamPersistenceRecovery 测试持久化数据恢复功能
func TestStreamPersistenceRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: map[string]interface{}{"size": 500 * time.Millisecond},
		},
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.Sum,
		},
		NeedWindow: true,
	}

	// 第一阶段：创建流并持久化数据
	stream1, err := NewStreamWithLossPolicyAndPersistence(config,
		1, 1, 1, // 极小缓冲区强制持久化
		"persist", 50*time.Millisecond,
		tmpDir, 512, 30*time.Millisecond)
	require.NoError(t, err)

	stream1.Start()

	// 添加测试数据
	testData := []map[string]interface{}{
		{"device": "sensor1", "temperature": 25.0},
		{"device": "sensor2", "temperature": 30.0},
		{"device": "sensor1", "temperature": 27.0},
	}

	for _, data := range testData {
		stream1.Emit(data)
	}

	// 等待数据持久化
	time.Sleep(200 * time.Millisecond)
	stream1.Stop()

	// 第二阶段：创建新流并恢复数据
	stream2, err := NewStreamWithLossPolicyAndPersistence(config,
		10, 10, 10,
		"persist", 100*time.Millisecond,
		tmpDir, 1024, 100*time.Millisecond)
	require.NoError(t, err)
	defer stream2.Stop()

	// 恢复持久化数据
	err = stream2.LoadAndReprocessPersistedData()
	require.NoError(t, err)

	// 验证数据恢复成功
	if stream2.persistenceManager != nil {
		stats := stream2.persistenceManager.GetStats()
		t.Logf("持久化统计: %+v", stats)
	}
}

// TestPersistenceManagerConcurrency 测试持久化管理器并发安全性
func TestPersistenceManagerConcurrency(t *testing.T) {
	tmpDir := t.TempDir()

	pm := NewPersistenceManagerWithConfig(tmpDir, 2048, 100*time.Millisecond)

	err := pm.Start()
	require.NoError(t, err)
	defer pm.Stop()

	// 并发写入数据
	const numGoroutines = 10
	const dataPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < dataPerGoroutine; j++ {
				data := map[string]interface{}{
					"goroutine": goroutineID,
					"sequence":  j,
					"value":     fmt.Sprintf("data_%d_%d", goroutineID, j),
				}
				err := pm.PersistData(data)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// 等待所有数据写入
	time.Sleep(300 * time.Millisecond)

	// 验证数据完整性
	loadedData, err := pm.LoadPersistedData()
	require.NoError(t, err)

	// 应该至少有部分数据被持久化
	assert.Greater(t, len(loadedData), 0)
	t.Logf("并发测试: 持久化了 %d 条数据", len(loadedData))
}

// TestPersistenceManagerStats 测试持久化统计功能
func TestPersistenceManagerStats(t *testing.T) {
	tmpDir := t.TempDir()

	pm := NewPersistenceManagerWithConfig(tmpDir, 1024, 50*time.Millisecond)

	err := pm.Start()
	require.NoError(t, err)
	defer pm.Stop()

	// 写入一些数据
	for i := 0; i < 10; i++ {
		data := map[string]interface{}{"index": i, "data": "test"}
		err := pm.PersistData(data)
		require.NoError(t, err)
	}

	// 等待数据处理
	time.Sleep(200 * time.Millisecond)

	// 获取统计信息
	stats := pm.GetStats()
	require.NotNil(t, stats)

	// 验证统计信息包含预期字段
	assert.Contains(t, stats, "data_dir")
	assert.Contains(t, stats, "max_file_size")
	assert.Contains(t, stats, "flush_interval")
	assert.Contains(t, stats, "running")
	assert.Equal(t, tmpDir, stats["data_dir"])
	assert.Equal(t, int64(1024), stats["max_file_size"])
	assert.Equal(t, true, stats["running"])

	t.Logf("持久化统计信息: %+v", stats)
}

// TestStreamPersistencePerformance 测试持久化性能
func TestStreamPersistencePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过性能测试 (使用 -short 标志)")
	}

	tmpDir := t.TempDir()

	config := types.Config{
		GroupFields: []string{"type"},
		SelectFields: map[string]aggregator.AggregateType{
			"value": aggregator.Count,
		},
		NeedWindow: false, // 无窗口，直接处理
	}

	// 创建高性能持久化配置
	stream, err := NewStreamWithLossPolicyAndPersistence(config,
		1000, 1000, 100,
		"persist", 1*time.Second,
		tmpDir, 10*1024*1024, 500*time.Millisecond) // 10MB文件，500ms刷新
	require.NoError(t, err)
	defer stream.Stop()

	var processedCount int64
	stream.AddSink(func(result interface{}) {
		atomic.AddInt64(&processedCount, 1)
	})

	stream.Start()

	// 性能测试：快速添加大量数据
	const numData = 10000
	start := time.Now()

	for i := 0; i < numData; i++ {
		data := map[string]interface{}{
			"type":  fmt.Sprintf("type_%d", i%10),
			"value": i,
			"data":  fmt.Sprintf("performance_test_data_%d", i),
		}
		stream.Emit(data)
	}

	elapsed := time.Since(start)

	// 等待处理完成
	time.Sleep(2 * time.Second)

	processed := atomic.LoadInt64(&processedCount)

	t.Logf("性能测试结果:")
	t.Logf("- 数据量: %d", numData)
	t.Logf("- 耗时: %v", elapsed)
	t.Logf("- 吞吐量: %.2f ops/sec", float64(numData)/elapsed.Seconds())
	t.Logf("- 处理结果数: %d", processed)

	// 验证持久化文件
	files, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	t.Logf("- 持久化文件数: %d", len(files))

	// 基本性能要求（可根据实际情况调整）
	assert.Less(t, elapsed, 10*time.Second, "持久化处理耗时应在合理范围内")
}

// TestStreamsqlPersistenceConfigPassing 测试Streamsql持久化配置的传递
func TestStreamsqlPersistenceConfigPassing(t *testing.T) {
	tmpDir := t.TempDir()

	// 测试自定义持久化配置是否正确传递
	config := types.Config{
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.Count,
		},
		NeedWindow: false,
	}

	// 创建带自定义持久化配置的流
	stream, err := NewStreamWithLossPolicyAndPersistence(config,
		100, 100, 10,
		"persist", 1*time.Second,
		tmpDir, 2048, 200*time.Millisecond) // 自定义配置：2KB文件，200ms刷新
	require.NoError(t, err)
	defer stream.Stop()

	// 验证持久化管理器配置
	require.NotNil(t, stream.persistenceManager)

	stats := stream.persistenceManager.GetStats()
	require.NotNil(t, stats)

	// 验证配置是否正确传递
	assert.Equal(t, tmpDir, stats["data_dir"])
	assert.Equal(t, int64(2048), stats["max_file_size"])
	assert.Contains(t, stats["flush_interval"], "200ms")

	t.Logf("持久化配置验证通过: %+v", stats)
}

func TestSelectStarWithExpressionFields(t *testing.T) {
	config := types.Config{
		NeedWindow:   false,
		SimpleFields: []string{"*"}, // SELECT *
		FieldExpressions: map[string]types.FieldExpression{
			"name": {
				Expression: "UPPER(name)",
				Fields:     []string{"name"},
			},
			"full_info": {
				Expression: "CONCAT(name, ' - ', status)",
				Fields:     []string{"name", "status"},
			},
		},
	}

	stream, err := NewStream(config)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Stop()

	// 收集结果 - 使用sync.Mutex防止数据竞争
	var mu sync.Mutex
	var results []interface{}
	stream.AddSink(func(result interface{}) {
		mu.Lock()
		defer mu.Unlock()
		results = append(results, result)
	})

	stream.Start()

	// 添加测试数据
	testData := map[string]interface{}{
		"name":   "john",
		"status": "active",
		"age":    25,
	}

	stream.Emit(testData)

	// 等待处理完成
	time.Sleep(100 * time.Millisecond)

	// 验证结果 - 使用互斥锁保护读取
	mu.Lock()
	resultsLen := len(results)
	var resultData map[string]interface{}
	if resultsLen > 0 {
		resultData = results[0].([]map[string]interface{})[0]
	}
	mu.Unlock()

	if resultsLen != 1 {
		t.Fatalf("Expected 1 result, got %d", resultsLen)
	}

	// 验证表达式字段的结果没有被覆盖
	if resultData["name"] != "JOHN" {
		t.Errorf("Expected name to be 'JOHN' (uppercase), got %v", resultData["name"])
	}

	if resultData["full_info"] != "john - active" {
		t.Errorf("Expected full_info to be 'john - active', got %v", resultData["full_info"])
	}

	// 验证原始字段仍然存在
	if resultData["status"] != "active" {
		t.Errorf("Expected status to be 'active', got %v", resultData["status"])
	}

	if resultData["age"] != 25 {
		t.Errorf("Expected age to be 25, got %v", resultData["age"])
	}
}

func TestSelectStarWithExpressionFieldsOverride(t *testing.T) {
	// 测试表达式字段名与原始字段名相同的情况
	config := types.Config{
		NeedWindow:   false,
		SimpleFields: []string{"*"}, // SELECT *
		FieldExpressions: map[string]types.FieldExpression{
			"name": {
				Expression: "UPPER(name)",
				Fields:     []string{"name"},
			},
			"age": {
				Expression: "age * 2",
				Fields:     []string{"age"},
			},
		},
	}

	stream, err := NewStream(config)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Stop()

	// 收集结果 - 使用sync.Mutex防止数据竞争
	var mu sync.Mutex
	var results []interface{}
	stream.AddSink(func(result interface{}) {
		mu.Lock()
		defer mu.Unlock()
		results = append(results, result)
	})

	stream.Start()

	// 添加测试数据
	testData := map[string]interface{}{
		"name":   "alice",
		"age":    30,
		"status": "active",
	}

	stream.Emit(testData)

	// 等待处理完成
	time.Sleep(100 * time.Millisecond)

	// 验证结果 - 使用互斥锁保护读取
	mu.Lock()
	resultsLen := len(results)
	var resultData map[string]interface{}
	if resultsLen > 0 {
		resultData = results[0].([]map[string]interface{})[0]
	}
	mu.Unlock()

	if resultsLen != 1 {
		t.Fatalf("Expected 1 result, got %d", resultsLen)
	}

	// 验证表达式字段的结果覆盖了原始字段
	if resultData["name"] != "ALICE" {
		t.Errorf("Expected name to be 'ALICE' (expression result), got %v", resultData["name"])
	}

	// 检查age表达式的结果（可能是int或float64类型）
	ageResult := resultData["age"]
	if ageResult != 60 && ageResult != 60.0 {
		t.Errorf("Expected age to be 60 (expression result), got %v (type: %T)", resultData["age"], resultData["age"])
	}

	// 验证没有表达式的字段保持原值
	if resultData["status"] != "active" {
		t.Errorf("Expected status to be 'active', got %v", resultData["status"])
	}
}

func TestSelectStarWithoutExpressionFields(t *testing.T) {
	// 测试没有表达式字段时SELECT *的行为
	config := types.Config{
		NeedWindow:   false,
		SimpleFields: []string{"*"}, // SELECT *
	}

	stream, err := NewStream(config)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}
	defer stream.Stop()

	// 收集结果 - 使用sync.Mutex防止数据竞争
	var mu sync.Mutex
	var results []interface{}
	stream.AddSink(func(result interface{}) {
		mu.Lock()
		defer mu.Unlock()
		results = append(results, result)
	})

	stream.Start()

	// 添加测试数据
	testData := map[string]interface{}{
		"name":   "bob",
		"age":    35,
		"status": "inactive",
	}

	stream.Emit(testData)

	// 等待处理完成
	time.Sleep(100 * time.Millisecond)

	// 验证结果 - 使用互斥锁保护读取
	mu.Lock()
	resultsLen := len(results)
	var resultData map[string]interface{}
	if resultsLen > 0 {
		resultData = results[0].([]map[string]interface{})[0]
	}
	mu.Unlock()

	if resultsLen != 1 {
		t.Fatalf("Expected 1 result, got %d", resultsLen)
	}

	// 验证所有原始字段都被保留
	if resultData["name"] != "bob" {
		t.Errorf("Expected name to be 'bob', got %v", resultData["name"])
	}

	if resultData["age"] != 35 {
		t.Errorf("Expected age to be 35, got %v", resultData["age"])
	}

	if resultData["status"] != "inactive" {
		t.Errorf("Expected status to be 'inactive', got %v", resultData["status"])
	}
}
