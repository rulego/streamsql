package stream

import (
	"fmt"
	"testing"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWindowSlotAggregation 测试窗口时间槽聚合
func TestWindowSlotAggregation(t *testing.T) {
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
	defer strm.Stop()

	strm.Start()

	// 使用固定时间戳的测试数据
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)
	testData := []map[string]interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0, "humidity": 60, "ts": baseTime},
		map[string]interface{}{"device": "aa", "temperature": 30.0, "humidity": 55, "ts": baseTime.Add(1 * time.Second)},
		map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70, "ts": baseTime},
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

	select {
	case actual := <-resultChan:
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
		resultSlice := actual.([]map[string]interface{})
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
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for results")
	}
}

// TestWindowTypes 测试不同类型的窗口
func TestWindowTypes(t *testing.T) {
	tests := []struct {
		name         string
		windowType   string
		windowParams map[string]interface{}
		expectError  bool
	}{
		{
			name:       "Tumbling Window",
			windowType: "tumbling",
			windowParams: map[string]interface{}{
				"size": "5s",
			},
			expectError: false,
		},
		{
			name:       "Sliding Window",
			windowType: "sliding",
			windowParams: map[string]interface{}{
				"size":  "10s",
				"slide": "5s",
			},
			expectError: false,
		},
		{
			name:       "Session Window",
			windowType: "session",
			windowParams: map[string]interface{}{
				"timeout": "30s",
			},
			expectError: false,
		},
		{
			name:         "Invalid Window Type",
			windowType:   "invalid_window_type",
			windowParams: map[string]interface{}{"size": "5s"},
			expectError:  true,
		},
		{
			name:         "Missing Size Parameter",
			windowType:   "tumbling",
			windowParams: map[string]interface{}{},
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.Config{
				NeedWindow: true,
				WindowConfig: types.WindowConfig{
					Type:   tt.windowType,
					Params: tt.windowParams,
				},
				SelectFields: map[string]aggregator.AggregateType{
					"value": aggregator.Count,
				},
				PerformanceConfig: types.DefaultPerformanceConfig(),
			}

			stream, err := NewStream(config)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, stream)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, stream)
				if stream != nil {
					defer stream.Stop()
					assert.NotNil(t, stream.Window)
				}
			}
		})
	}
}

// TestAggregationTypes 测试不同的聚合类型
func TestAggregationTypes(t *testing.T) {
	tests := []struct {
		name     string
		aggType  aggregator.AggregateType
		testData []float64
		expected float64
	}{
		{"Sum", aggregator.Sum, []float64{1, 2, 3, 4, 5}, 15.0},
		{"Avg", aggregator.Avg, []float64{2, 4, 6, 8}, 5.0},
		{"Min", aggregator.Min, []float64{5, 2, 8, 1, 9}, 1.0},
		{"Max", aggregator.Max, []float64{5, 2, 8, 1, 9}, 9.0},
		{"Count", aggregator.Count, []float64{1, 2, 3}, 3.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.Config{
				WindowConfig: types.WindowConfig{
					Type:   "tumbling",
					Params: map[string]interface{}{"size": 500 * time.Millisecond},
				},
				GroupFields: []string{"group"},
				SelectFields: map[string]aggregator.AggregateType{
					"value": tt.aggType,
				},
				NeedWindow: true,
			}

			stream, err := NewStream(config)
			require.NoError(t, err)
			defer stream.Stop()

			resultChan := make(chan interface{}, 1)
			stream.AddSink(func(result []map[string]interface{}) {
				select {
				case resultChan <- result:
				default:
				}
			})

			stream.Start()

			// 发送测试数据
			for _, value := range tt.testData {
				data := map[string]interface{}{
					"group": "test",
					"value": value,
				}
				stream.Emit(data)
			}

			// 等待窗口关闭
			time.Sleep(700 * time.Millisecond)

			select {
			case result := <-resultChan:
				resultSlice := result.([]map[string]interface{})
				require.Len(t, resultSlice, 1)
				actual := resultSlice[0]["value"].(float64)
				assert.InEpsilon(t, tt.expected, actual, 0.0001)
			case <-time.After(3 * time.Second):
				t.Fatal("Timeout waiting for aggregation result")
			}
		})
	}
}
