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

// TestWindowSlotAggregation: Aggregation of test window time slots
func TestWindowSlotAggregation(t *testing.T) {
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:   "sliding",
			Params: []any{2 * time.Second, 1 * time.Second},
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

	// Test data using fixed timestamps
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)
	testData := []map[string]any{
		map[string]any{"device": "aa", "temperature": 25.0, "humidity": 60, "ts": baseTime},
		map[string]any{"device": "aa", "temperature": 30.0, "humidity": 55, "ts": baseTime.Add(1 * time.Second)},
		map[string]any{"device": "bb", "temperature": 22.0, "humidity": 70, "ts": baseTime},
	}

	for _, data := range testData {
		strm.Emit(data)
	}

	// Capture the results
	resultChan := make(chan any)
	strm.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Wait for the window to trigger
	time.Sleep(3 * time.Second)

	select {
	case actual := <-resultChan:
		expected := []map[string]any{
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

		assert.IsType(t, []map[string]any{}, actual)
		resultSlice := actual.([]map[string]any)
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

// TestWindowTypes tests different types of windows
func TestWindowTypes(t *testing.T) {
	tests := []struct {
		name         string
		windowType   string
		windowParams []any
		expectError  bool
	}{
		{
			name:         "Tumbling Window",
			windowType:   "tumbling",
			windowParams: []any{5 * time.Second},
			expectError:  false,
		},
		{
			name:         "Sliding Window",
			windowType:   "sliding",
			windowParams: []any{10 * time.Second, 5 * time.Second},
			expectError:  false,
		},
		{
			name:         "Session Window",
			windowType:   "session",
			windowParams: []any{30 * time.Second},
			expectError:  false,
		},
		{
			name:         "Invalid Window Type",
			windowType:   "invalid_window_type",
			windowParams: []any{5 * time.Second},
			expectError:  true,
		},
		{
			name:         "Missing Size Parameter",
			windowType:   "tumbling",
			windowParams: []any{},
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

// TestAggregationTypes tests different aggregation types
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
					Params: []any{500 * time.Millisecond},
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

			resultChan := make(chan any, 1)
			stream.AddSink(func(result []map[string]any) {
				select {
				case resultChan <- result:
				default:
				}
			})

			stream.Start()

			// Send test data
			for _, value := range tt.testData {
				data := map[string]any{
					"group": "test",
					"value": value,
				}
				stream.Emit(data)
			}

			// Wait for the window to close
			time.Sleep(700 * time.Millisecond)

			select {
			case result := <-resultChan:
				resultSlice := result.([]map[string]any)
				require.Len(t, resultSlice, 1)
				actual := resultSlice[0]["value"].(float64)
				assert.InEpsilon(t, tt.expected, actual, 0.0001)
			case <-time.After(3 * time.Second):
				t.Fatal("Timeout waiting for aggregation result")
			}
		})
	}
}
