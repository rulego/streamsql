package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamProcess(t *testing.T) {
	config := Config{
		WindowConfig: WindowConfig{
			Type:   "tumbling",
			Params: map[string]interface{}{"size": time.Second},
		},
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"age":   aggregator.Avg,
			"score": aggregator.Sum,
		},
	}

	strm, err := NewStream(config)
	require.NoError(t, err)

	err = strm.RegisterFilter("device == 'aa' && age > 10")
	require.NoError(t, err)

	// 添加 Sink 函数来捕获结果
	resultChan := make(chan interface{})
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})

	strm.Start()

	// 准备测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "age": 15.0, "score": 100},
		map[string]interface{}{"device": "aa", "age": 20.0, "score": 200},
		map[string]interface{}{"device": "bb", "age": 25.0, "score": 300},
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

	// 预期结果：只有 device='aa' 且 age>10 的数据会被聚合
	expected := map[string]interface{}{
		"device":    "aa",
		"age_avg":   17.5,  // (15+20)/2
		"score_sum": 300.0, // 100+200
	}

	// 验证结果
	assert.IsType(t, []map[string]interface{}{}, actual)
	resultMap := actual.([]map[string]interface{})
	assert.InEpsilon(t, expected["age_avg"].(float64), resultMap[0]["age_avg"].(float64), 0.0001)
	assert.InDelta(t, expected["score_sum"].(float64), resultMap[0]["score_sum"].(float64), 0.0001)
}

// 不设置过滤器
func TestStreamWithoutFilter(t *testing.T) {
	config := Config{
		WindowConfig: WindowConfig{
			Type:   "sliding",
			Params: map[string]interface{}{"size": 2 * time.Second, "slide": 1 * time.Second},
		},
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"age":   aggregator.Max,
			"score": aggregator.Min,
		},
	}

	strm, err := NewStream(config)
	require.NoError(t, err)

	strm.Start()

	testData := []interface{}{
		map[string]interface{}{"device": "aa", "age": 5.0, "score": 100},
		map[string]interface{}{"device": "aa", "age": 10.0, "score": 200},
		map[string]interface{}{"device": "bb", "age": 3.0, "score": 300},
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
			"device":    "aa",
			"age_max":   10.0,
			"score_min": 100.0,
		},
		{
			"device":    "bb",
			"age_max":   3.0,
			"score_min": 300.0,
		},
	}

	assert.IsType(t, []map[string]interface{}{}, actual)
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok)

	assert.Len(t, resultSlice, 2)
	for _, expectedResult := range expected {
		found := false
		for _, resultMap := range resultSlice {
			//if resultMap, ok := result.(map[string]interface{}); ok {
			if resultMap["device"] == expectedResult["device"] {
				assert.InEpsilon(t, expectedResult["age_max"].(float64), resultMap["age_max"].(float64), 0.0001)
				assert.InEpsilon(t, expectedResult["score_min"].(float64), resultMap["score_min"].(float64), 0.0001)
				found = true
				break
			}
			//}
		}
		assert.True(t, found, fmt.Sprintf("Expected result for device %v not found", expectedResult["device"]))
	}
}

func TestIncompleteStreamProcess(t *testing.T) {
	config := Config{
		WindowConfig: WindowConfig{
			Type:   "tumbling",
			Params: map[string]interface{}{"size": time.Second},
		},
		GroupFields: []string{"device"},
		SelectFields: map[string]aggregator.AggregateType{
			"age":   aggregator.Avg,
			"score": aggregator.Sum,
		},
	}

	strm, err := NewStream(config)
	require.NoError(t, err)

	err = strm.RegisterFilter("device == 'aa' && age > 10")
	require.NoError(t, err)

	// 添加 Sink 函数来捕获结果
	resultChan := make(chan interface{})
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})

	strm.Start()

	// 准备测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "age": 15.0},
		map[string]interface{}{"device": "aa", "score": 100},
		map[string]interface{}{"device": "aa", "age": 20.0},
		map[string]interface{}{"device": "aa", "score": 200},
		map[string]interface{}{"device": "bb", "age": 25.0, "score": 300},
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

	// 预期结果：只有 device='aa' 且 age>10 的数据会被聚合
	expected := map[string]interface{}{
		"device":    "aa",
		"age_avg":   17.5,  // (15+20)/2
		"score_sum": 300.0, // 100+200
	}

	// 验证结果
	assert.IsType(t, []map[string]interface{}{}, actual)
	resultMap := actual.([]map[string]interface{})
	assert.InEpsilon(t, expected["age_avg"].(float64), resultMap[0]["age_avg"].(float64), 0.0001)
	assert.InDelta(t, expected["score_sum"].(float64), resultMap[0]["score_sum"].(float64), 0.0001)
}
