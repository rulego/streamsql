package stream

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamBasicFunctionality 测试Stream基本功能
func TestStreamBasicFunctionality(t *testing.T) {
	tests := []struct {
		name           string
		config         types.Config
		filter         string
		testData       []map[string]interface{}
		expectedDevice string
		expectedTemp   float64
		expectedHum    float64
	}{
		{
			name: "带过滤器的窗口聚合",
			config: types.Config{
				WindowConfig: types.WindowConfig{
					Type:   "tumbling",
					Params: map[string]interface{}{"size": 500 * time.Millisecond},
				},
				GroupFields: []string{"device"},
				SelectFields: map[string]aggregator.AggregateType{
					"temperature": aggregator.Avg,
					"humidity":    aggregator.Sum,
				},
				NeedWindow: true,
			},
			filter: "device == 'aa' && temperature > 10",
			testData: []map[string]interface{}{
				map[string]interface{}{"device": "aa", "temperature": 25.0, "humidity": 60},
				map[string]interface{}{"device": "aa", "temperature": 30.0, "humidity": 55},
				map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70},
			},
			expectedDevice: "aa",
			expectedTemp:   27.5,
			expectedHum:    115.0,
		},
		{
			name: "不完整数据处理",
			config: types.Config{
				WindowConfig: types.WindowConfig{
					Type:   "tumbling",
					Params: map[string]interface{}{"size": 500 * time.Millisecond},
				},
				GroupFields: []string{"device"},
				SelectFields: map[string]aggregator.AggregateType{
					"temperature": aggregator.Avg,
					"humidity":    aggregator.Sum,
				},
				NeedWindow: true,
			},
			filter: "device == 'aa'",
			testData: []map[string]interface{}{
				map[string]interface{}{"device": "aa", "temperature": 25.0},
				map[string]interface{}{"device": "aa", "humidity": 60},
				map[string]interface{}{"device": "aa", "temperature": 30.0},
				map[string]interface{}{"device": "aa", "humidity": 55},
				map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70},
			},
			expectedDevice: "aa",
			expectedTemp:   27.5,
			expectedHum:    115.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strm, err := NewStream(tt.config)
			require.NoError(t, err)
			defer strm.Stop()

			if tt.filter != "" {
				err = strm.RegisterFilter(tt.filter)
				require.NoError(t, err)
			}

			// 添加 Sink 函数来捕获结果
			resultChan := make(chan interface{}, 1)
			strm.AddSink(func(result []map[string]interface{}) {
				select {
				case resultChan <- result:
				default:
					// 防止阻塞
				}
			})

			strm.Start()

			// 发送测试数据
			for _, data := range tt.testData {
				strm.Emit(data)
			}

			// 等待窗口关闭并触发结果
			time.Sleep(700 * time.Millisecond)

			// 等待结果
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			var actual interface{}
			select {
			case actual = <-resultChan:
				cancel()
			case <-ctx.Done():
				t.Fatal("No results received within 3 seconds")
			}

			// 验证结果
			require.NotNil(t, actual)
			assert.IsType(t, []map[string]interface{}{}, actual)
			resultMap := actual.([]map[string]interface{})
			require.Greater(t, len(resultMap), 0)

			firstResult := resultMap[0]
			assert.Equal(t, tt.expectedDevice, firstResult["device"])
			assert.InEpsilon(t, tt.expectedTemp, firstResult["temperature"].(float64), 0.0001)
			assert.InDelta(t, tt.expectedHum, firstResult["humidity"].(float64), 0.0001)
		})
	}
}

// TestStreamWithoutFilter 测试无过滤器的流处理
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
	defer strm.Stop()

	strm.Start()

	testData := []map[string]interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0, "humidity": 60},
		map[string]interface{}{"device": "aa", "temperature": 30.0, "humidity": 55},
		map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70},
	}

	for _, data := range testData {
		strm.Emit(data)
	}

	// 捕获结果
	resultChan := make(chan interface{})
	strm.AddSink(func(result []map[string]interface{}) {
		resultChan <- result
	})

	// 等待窗口触发
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
		{"device": "aa", "temperature": 30.0, "humidity": 55.0},
		{"device": "bb", "temperature": 22.0, "humidity": 70.0},
	}

	assert.IsType(t, []map[string]interface{}{}, actual)
	resultSlice := actual.([]map[string]interface{})
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
		assert.True(t, found, "Expected result for device %v not found", expectedResult["device"])
	}
}

// TestStreamRefactoring 测试重构后的Stream功能
func TestStreamRefactoring(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		SelectFields: map[string]aggregator.AggregateType{
			"count": aggregator.Count,
		},
		GroupFields: []string{"category"},
		NeedWindow:  false,
	}

	stream, err := NewStream(config)
	require.NoError(t, err)
	defer stream.Stop()

	testData := map[string]interface{}{
		"name":     "test",
		"age":      25,
		"category": "A",
	}

	// 启动Stream
	stream.Start()

	// 发送测试数据
	stream.Emit(testData)

	// 等待处理完成
	time.Sleep(100 * time.Millisecond)

	// 获取统计信息
	stats := stream.GetStats()
	assert.Equal(t, int64(1), stats[InputCount])
}

// TestStreamFactory 测试StreamFactory功能
func TestStreamFactory(t *testing.T) {
	factory := NewStreamFactory()
	require.NotNil(t, factory)

	config := types.Config{
		SimpleFields: []string{"value"},
		NeedWindow:   false,
	}

	// 测试不同的创建方法
	tests := []struct {
		name   string
		create func() (*Stream, error)
	}{
		{"CreateStream", func() (*Stream, error) { return factory.CreateStream(config) }},
		{"CreateHighPerformanceStream", func() (*Stream, error) { return factory.CreateHighPerformanceStream(config) }},
		{"CreateLowLatencyStream", func() (*Stream, error) { return factory.CreateLowLatencyStream(config) }},
		{"CreateZeroDataLossStream", func() (*Stream, error) { return factory.CreateZeroDataLossStream(config) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, err := tt.create()
			require.NoError(t, err)
			require.NotNil(t, stream)
			defer stream.Stop()
		})
	}
}

// TestMigratedFunctions 测试迁移后的函数
func TestMigratedFunctions(t *testing.T) {
	// 测试性能评估函数
	tests := []struct {
		name      string
		dataUsage float64
		dropRate  float64
		expected  string
	}{
		{"Critical", 50.0, 60.0, PerformanceLevelCritical},
		{"Warning", 50.0, 30.0, PerformanceLevelWarning},
		{"HighLoad", 95.0, 5.0, PerformanceLevelHighLoad},
		{"ModerateLoad", 75.0, 5.0, PerformanceLevelModerateLoad},
		{"Optimal", 50.0, 5.0, PerformanceLevelOptimal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AssessPerformanceLevel(tt.dataUsage, tt.dropRate)
			assert.Equal(t, tt.expected, result)
		})
	}
}
