package stream

import (
	"context"
	"fmt"
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
			Params: map[string]interface{}{"size": time.Second},
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
	resultChan := make(chan interface{})
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})

	strm.Start()

	// 准备测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0, "humidity": 60},
		map[string]interface{}{"device": "aa", "temperature": 30.0, "humidity": 55},
		map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70},
	}

	for _, data := range testData {
		strm.AddData(data)
	}

	// 等待结果，并设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-strm.GetResultsChan():
		cancel()
	case <-ctx.Done():
		t.Fatal("No results received within 5 seconds")
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
		strm.AddData(data)
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
			Params: map[string]interface{}{"size": time.Second},
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
	resultChan := make(chan interface{})
	strm.AddSink(func(result interface{}) {
		resultChan <- result
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
		strm.AddData(data)
	}

	// 等待结果，并设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-strm.GetResultsChan():
		cancel()
	case <-ctx.Done():
		t.Fatal("No results received within 5 seconds")
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
		strm.AddData(data)
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
