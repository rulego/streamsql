package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFunctionIntegrationNonAggregation tests the integration of non-aggregation functions in SQL
func TestFunctionIntegrationNonAggregation(t *testing.T) {
	t.Parallel()
	t.Run("MathFunctions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test multiple mathematical functions: abs, sqrt, round
		rsql := "SELECT device, abs(temperature) as abs_temp, sqrt(humidity) as sqrt_humidity, round(temperature) as rounded_temp FROM stream"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := map[string]any{
			"device":      "test-device",
			"temperature": -25.5,
			"humidity":    64.0,
		}
		strm.Emit(testData)

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "test-device", item["device"])
			// Verify abs(-25.5) = 25.5
			assert.InEpsilon(t, 25.5, item["abs_temp"], 0.001)
			// Validation sqrt(64) = 8
			assert.InEpsilon(t, 8.0, item["sqrt_humidity"], 0.001)
			// Verify round(-25.5) = -26
			assert.InEpsilon(t, -26.0, item["rounded_temp"], 0.001)
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("StringFunctions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test string functions: upper, lower, concat, length
		rsql := "SELECT upper(device) as upper_device, lower(location) as lower_location, concat(device, '-', location) as combined, length(device) as device_len FROM stream"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := map[string]any{
			"device":   "sensor01",
			"location": "ROOM_A",
		}
		strm.Emit(testData)

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "SENSOR01", item["upper_device"])
			assert.Equal(t, "room_a", item["lower_location"])
			assert.Equal(t, "sensor01-ROOM_A", item["combined"])
			assert.Equal(t, 8, item["device_len"])
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("ConversionFunctions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test conversion function: cast
		rsql := "SELECT device, cast(temperature, 'int') as temp_int, cast(humidity, 'string') as humidity_str FROM stream"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := map[string]any{
			"device":      "test-device",
			"temperature": 25.7,
			"humidity":    65.0,
		}
		strm.Emit(testData)

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "test-device", item["device"])
			assert.Equal(t, int(25), item["temp_int"])
			assert.Equal(t, "65", item["humidity_str"])
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("DateTimeFunctions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test date time function: now, year, month, day
		rsql := "SELECT device, now() as current_time, year(timestamp) as ts_year, month(timestamp) as ts_month FROM stream"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		//testTime := time.Date(2025, 4, 15, 10, 30, 0, 0, time.UTC)
		testData := map[string]any{
			"device":    "test-device",
			"timestamp": "2025-08-25",
		}
		strm.Emit(testData)

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "test-device", item["device"])
			assert.NotNil(t, item["current_time"])
			assert.Equal(t, 2025, item["ts_year"])
			assert.Equal(t, 8, item["ts_month"])
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("JSONFunctions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test JSON function: json_extract, json_valid
		rsql := "SELECT device, json_extract(metadata, '$.type') as device_type, json_valid(metadata) as is_valid_json FROM stream"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := map[string]any{
			"device":   "test-device",
			"metadata": `{"type": "temperature_sensor", "version": "1.0"}`,
		}
		strm.Emit(testData)

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "test-device", item["device"])
			assert.Equal(t, "temperature_sensor", item["device_type"])
			assert.Equal(t, true, item["is_valid_json"])
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("JSONExtractMapSupport", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test json_extract with map input
		rsql := "SELECT device, json_extract(properties, '$.color') as device_color FROM stream"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data with map
		testData := map[string]any{
			"device": "test-device-map",
			"properties": map[string]any{
				"color":  "red",
				"weight": 10,
			},
		}
		strm.Emit(testData)

		// Wait for result
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "test-device-map", item["device"])
			assert.Equal(t, "red", item["device_color"])
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("JSONExtractArrayAndNested", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test json_extract with array and nested structures
		rsql := "SELECT device, json_extract(tags, '$[0]') as first_tag, json_extract(data, '$.users[0].name') as first_user_name FROM stream"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data with complex structures
		testData := map[string]any{
			"device": "complex-device",
			"tags":   []any{"tag1", "tag2"},
			"data": map[string]any{
				"users": []any{
					map[string]any{"name": "Alice", "age": 30},
					map[string]any{"name": "Bob", "age": 25},
				},
			},
		}
		strm.Emit(testData)

		// Wait for result
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "complex-device", item["device"])
			assert.Equal(t, "tag1", item["first_tag"])
			assert.Equal(t, "Alice", item["first_user_name"])
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("JSONExtractWithAggregation", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test json_extract nested in aggregation function
		// json_extract returns any, usually need cast to number for aggregation like sum/avg
		// specific logic depends on whether aggregator handles string/interface conversion
		// Here we assume json_extract returns float64 for numbers (from Unmarshal) or use cast
		rsql := "SELECT count(json_extract(tags, '$[0]')) as tag_count, sum(cast(json_extract(data, '$.value'), 'float')) as total_value FROM stream GROUP BY device, TumblingWindow('1s')"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		testData := []map[string]any{
			{
				"device": "device1",
				"tags":   []any{"tag1", "tag2"},
				"data":   map[string]any{"value": 10},
			},
			{
				"device": "device1",
				"tags":   []any{"tag3"},
				"data":   map[string]any{"value": 20},
			},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "device1", item["device"])
			assert.Equal(t, float64(2), item["tag_count"])
			assert.Equal(t, float64(30), item["total_value"])
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})
}

// TestFunctionIntegrationAggregation: Integrate the test aggregation function in SQL
func TestFunctionIntegrationAggregation(t *testing.T) {
	t.Parallel()
	t.Run("BasicAggregationFunctions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test basic aggregate functions: sum, avg, min, max, count
		rsql := "SELECT device, sum(temperature) as total_temp, avg(temperature) as avg_temp, min(temperature) as min_temp, max(temperature) as max_temp, count(temperature) as temp_count FROM stream GROUP BY device, TumblingWindow('1s')"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := []map[string]any{
			{"device": "sensor1", "temperature": 20.0},
			{"device": "sensor1", "temperature": 25.0},
			{"device": "sensor1", "temperature": 30.0},
			{"device": "sensor2", "temperature": 15.0},
			{"device": "sensor2", "temperature": 18.0},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// Wait for the window to initialize
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 2)

			// Verify the aggregation results of sensor1
			for _, item := range resultSlice {
				device := item["device"].(string)
				if device == "sensor1" {
					assert.InEpsilon(t, 75.0, item["total_temp"].(float64), 0.001)
					assert.InEpsilon(t, 25.0, item["avg_temp"].(float64), 0.001)
					assert.InEpsilon(t, 20.0, item["min_temp"].(float64), 0.001)
					assert.InEpsilon(t, 30.0, item["max_temp"].(float64), 0.001)
					assert.Equal(t, 3.0, item["temp_count"].(float64))
				} else if device == "sensor2" {
					assert.InEpsilon(t, 33.0, item["total_temp"].(float64), 0.001)
					assert.InEpsilon(t, 16.5, item["avg_temp"].(float64), 0.001)
					assert.InEpsilon(t, 15.0, item["min_temp"].(float64), 0.001)
					assert.InEpsilon(t, 18.0, item["max_temp"].(float64), 0.001)
					assert.Equal(t, 2.0, item["temp_count"].(float64))
				}
			}
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("StatisticalAggregationFunctions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test statistical aggregate functions: stddev, median, percentile
		rsql := "SELECT device, stddev(temperature) as temp_stddev, median(temperature) as temp_median FROM stream GROUP BY device, TumblingWindow('1s')"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := []map[string]any{
			{"device": "sensor1", "temperature": 10.0},
			{"device": "sensor1", "temperature": 20.0},
			{"device": "sensor1", "temperature": 30.0},
			{"device": "sensor1", "temperature": 40.0},
			{"device": "sensor1", "temperature": 50.0},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// Wait for the window to initialize
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "sensor1", item["device"])
			// The standard deviation should be about 15.81
			assert.InEpsilon(t, 15.81, item["temp_stddev"].(float64), 0.1)
			// The median should be 30.0
			assert.InEpsilon(t, 30.0, item["temp_median"].(float64), 0.001)
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("CollectionAggregationFunctions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test the aggregate functions of the set: collect, first_value, last_value
		rsql := "SELECT device, collect(temperature) as temp_array, first_value(temperature) as first_temp, last_value(temperature) as last_temp FROM stream GROUP BY device, TumblingWindow('1s')"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := []map[string]any{
			{"device": "sensor1", "temperature": 20.0},
			{"device": "sensor1", "temperature": 25.0},
			{"device": "sensor1", "temperature": 30.0},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// Wait for the window to initialize
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "sensor1", item["device"])

			// Verify the array returned by the collect function
			tempArray, ok := item["temp_array"].([]any)
			assert.True(t, ok)
			assert.Len(t, tempArray, 3)
			assert.Contains(t, tempArray, 20.0)
			assert.Contains(t, tempArray, 25.0)
			assert.Contains(t, tempArray, 30.0)

			// Verify first_value and last_value
			assert.Equal(t, 20.0, item["first_temp"])
			assert.Equal(t, 30.0, item["last_temp"])
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})
}

// TestFunctionIntegrationMixed tests the mixed function scenario
func TestFunctionIntegrationMixed(t *testing.T) {
	t.Parallel()
	t.Run("AggregationWithNonAggregationFunctions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test a mix of aggregate and non-aggregate functions
		rsql := "SELECT device, upper(device) as device_upper, avg(temperature) as avg_temp, round(avg(temperature), 2) as rounded_avg FROM stream GROUP BY device, TumblingWindow('1s')"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := []map[string]any{
			{"device": "sensor1", "temperature": 20.567},
			{"device": "sensor1", "temperature": 25.234},
			{"device": "sensor1", "temperature": 30.123},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// Wait for the window to initialize
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			assert.Equal(t, "sensor1", item["device"])
			assert.Equal(t, "SENSOR1", item["device_upper"])

			// Verify the average
			if avgTemp, exists := item["avg_temp"]; exists && avgTemp != nil {
				assert.InEpsilon(t, 25.308, avgTemp.(float64), 0.001)
			} else {
				t.Errorf("avg_temp is missing or nil: %v", avgTemp)
			}

			// Verify the rounded average
			if roundedAvg, exists := item["rounded_avg"]; exists {
				if roundedAvg == nil {
					t.Errorf("rounded_avg exists but is nil - this indicates the round(avg()) expression failed")
				} else if val, ok := roundedAvg.(float64); ok {
					// The verification results are within a reasonable range
					assert.True(t, val >= 25.0 && val <= 25.5, "rounded_avg should be between 25.0 and 25.5, got %v", val)
				} else {
					t.Errorf("rounded_avg is not a float64: %v (type: %T)", roundedAvg, roundedAvg)
				}
			} else {
				t.Errorf("rounded_avg field is missing from result")
			}
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("NestedFunctionCalls", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test nested function calls
		rsql := "SELECT device, upper(concat(device, '_', cast(round(temperature, 0), 'string'))) as device_temp_label FROM stream"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := map[string]any{
			"device":      "sensor1",
			"temperature": 25.7,
		}
		strm.Emit(testData)

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "sensor1", item["device"])
			// round(25.7, 0) = 26, cast(26, 'string') = "26", concat("sensor1", "_", "26") = "sensor1_26", upper("sensor1_26") = "SENSOR1_26"
			assert.Equal(t, "SENSOR1_26", item["device_temp_label"])
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("WindowFunctionsWithAggregation", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// The test window function is combined with the aggregation function
		rsql := "SELECT device, avg(temperature) as avg_temp, window_start() as start_time, window_end() as end_time FROM stream GROUP BY device, TumblingWindow('1s')"
		err := ssql.Execute(rsql)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := []map[string]any{
			{"device": "sensor1", "temperature": 20.0},
			{"device": "sensor1", "temperature": 30.0},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// Wait for the window to initialize
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "sensor1", item["device"])
			assert.InEpsilon(t, 25.0, item["avg_temp"].(float64), 0.001)
			assert.NotNil(t, item["start_time"])
			assert.NotNil(t, item["end_time"])
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})
}

// TestNestedFunctionSupport tests nested function support
func TestNestedFunctionSupport(t *testing.T) {
	t.Parallel()
	t.Run("NormalFunctionNestingAggregation", func(t *testing.T) {
		// Test ordinary functions nested aggregate functions: round(avg(temperature, 2)
		ssql := streamsql.New()
		defer ssql.Stop()

		// Execute queries containing round(avg(temperature, 2).
		query := "SELECT device, round(avg(temperature), 2) as rounded_avg FROM stream GROUP BY device, TumblingWindow('1s')"
		err := ssql.Execute(query)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := []map[string]any{
			{"device": "sensor1", "temperature": 20.567},
			{"device": "sensor1", "temperature": 25.234},
			{"device": "sensor1", "temperature": 30.123},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// Wait for the window to initialize
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "sensor1", item["device"])

			// Verify the rounded average
			if roundedAvg, exists := item["rounded_avg"]; exists {
				if roundedAvg == nil {
					t.Errorf("rounded_avg exists but is nil - this indicates the round(avg()) expression failed")
				} else if val, ok := roundedAvg.(float64); ok {
					// The average should be (20.567 + 25.234 + 30.123) / 3 = 25.308
					// round(25.308, 2) = 25.31
					assert.InEpsilon(t, 25.31, val, 0.01)
				} else {
					t.Errorf("rounded_avg is not a float64: %v (type: %T)", roundedAvg, roundedAvg)
				}
			} else {
				t.Errorf("rounded_avg field is missing from result")
			}
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("AggregationNestingNormalFunction", func(t *testing.T) {
		// Test the nested common function of the aggregate function: avg(round(temperature, 2))
		ssql := streamsql.New()
		defer ssql.Stop()

		// Execute queries containing avg(round(temperature, 2)).
		query := "SELECT device, avg(round(temperature, 2)) as avg_rounded FROM stream GROUP BY device, TumblingWindow('1s')"

		err := ssql.Execute(query)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := []map[string]any{
			{"device": "sensor1", "temperature": 20.567}, // round(20.567, 2) = 20.57
			{"device": "sensor1", "temperature": 25.234}, // round(25.234, 2) = 25.23
			{"device": "sensor1", "temperature": 30.123}, // round(30.123, 2) = 30.12
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// Wait for the window to initialize
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			assert.Equal(t, "sensor1", item["device"])

			// Verify the result of the aggregation function nesting ordinary functions
			if avgRounded, exists := item["avg_rounded"]; exists {
				if avgRounded == nil {
					t.Errorf("avg_rounded exists but is nil - this indicates the avg(round()) expression failed")
				} else if val, ok := avgRounded.(float64); ok {
					// Expected value: avg(20.57, 25.23, 30.12) = (20.57 + 25.23 + 30.12) / 3 = 25.31
					assert.InEpsilon(t, 25.31, val, 0.01)
				} else {
					t.Errorf("avg_rounded is not a float64: %v (type: %T)", avgRounded, avgRounded)
				}
			} else {
				t.Errorf("avg_rounded field is missing from result")
			}
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("ComplexNestedFunctions", func(t *testing.T) {
		// Testing more complex nested functions: round(avg(abs(temperature)), 1)
		ssql := streamsql.New()
		defer ssql.Stop()

		// Execute a query containing round(avg(abs(temperature)), 1).
		query := "SELECT device, round(avg(abs(temperature)), 1) as complex_result FROM stream GROUP BY device, TumblingWindow('1s')"
		err := ssql.Execute(query)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data (including negative numbers)
		testData := []map[string]any{
			{"device": "sensor1", "temperature": -20.567}, // abs(-20.567) = 20.567
			{"device": "sensor1", "temperature": 25.234},  // abs(25.234) = 25.234
			{"device": "sensor1", "temperature": -30.123}, // abs(-30.123) = 30.123
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// Wait for the window to initialize
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			assert.Equal(t, "sensor1", item["device"])

			// Verify the results of complex nested functions
			if complexResult, exists := item["complex_result"]; exists {
				if complexResult == nil {
					t.Errorf("complex_result exists but is nil - this indicates the round(avg(abs())) expression failed")
				} else if val, ok := complexResult.(float64); ok {
					// Expected value: avg(20.567, 25.234, 30.123) = 25.308, round(25.308, 1) = 25.3
					assert.InEpsilon(t, 25.3, val, 0.01)
				} else {
					t.Errorf("complex_result is not a float64: %v (type: %T)", complexResult, complexResult)
				}
			} else {
				t.Errorf("complex_result field is missing from result")
			}
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})
}

// TestNestedFunctionExecutionOrder tests the execution order of nested functions and combinations of functions of different types
func TestNestedFunctionExecutionOrder(t *testing.T) {
	t.Parallel()

	// Test 1: String functions nested mathematical functions
	t.Run("StringFunctionNestingMathFunction", func(t *testing.T) {
		// Test upper(concat("temp_", round(temperature, 1)))
		ssql := streamsql.New()
		defer ssql.Stop()

		query := "SELECT device, upper(concat('temp_', round(temperature, 1))) as formatted_temp FROM stream"
		err := ssql.Execute(query)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		strm.Emit(map[string]any{"device": "sensor1", "temperature": 25.67})

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			// Verification execution order: round(25.67, 1) -> 25.7, concat('temp_', '25.7') -> 'temp_25.7', upper('temp_25.7') -> 'TEMP_25.7'
			assert.Equal(t, "TEMP_25.7", item["formatted_temp"])
		case <-ctx.Done():
			t.Fatal("Test timeout")
		}
	})

	// Test 2: Mathematical Functions Nested String Functions
	t.Run("MathFunctionNestingStringFunction", func(t *testing.T) {
		// Test round(len(upper(device)), 0)
		ssql := streamsql.New()
		defer ssql.Stop()

		query := "SELECT device, round(len(upper(device)), 0) as device_length FROM stream"

		err := ssql.Execute(query)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		strm.Emit(map[string]any{"device": "sensor1"})

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			// Verification execution sequence: upper('sensor1') -> 'SENSOR1', len('SENSOR1') - > 7, round(7, 0) - > 7
			assert.Equal(t, float64(7), item["device_length"])
		case <-ctx.Done():
			t.Fatal("Test timeout")
		}
	})

	// Test 3: Multi-layer nested functions (3 layers)
	t.Run("ThreeLevelNestedFunctions", func(t *testing.T) {
		// Test abs(round(sqrt(temperature, 2))
		ssql := streamsql.New()
		defer ssql.Stop()

		query := "SELECT device, abs(round(sqrt(temperature), 2)) as processed_temp FROM stream"

		err := ssql.Execute(query)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		strm.Emit(map[string]any{"device": "sensor1", "temperature": 16.0})

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]
			// Verification execution order: sqrt(16) - > 4, round(4, 2) - > 4.00, abs(4.00) -> 4.00
			assert.Equal(t, float64(4), item["processed_temp"])
		case <-ctx.Done():
			t.Fatal("Test timeout")
		}
	})

	// Test 6: Complex nested aggregation functions – Should cause errors
	t.Run("ComplexAggregationNesting", func(t *testing.T) {
		// Test max(round(avg(temperature, 1)) - This is a nested aggregation function and should throw an error
		ssql := streamsql.New()
		defer ssql.Stop()

		query := "SELECT device, max(round(avg(temperature), 1)) as max_rounded_avg FROM stream GROUP BY device, TumblingWindow('1s')"
		err := ssql.Execute(query)
		// A nested aggregation function error should be returned
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "aggregate function calls cannot be nested")
	})

	// Test 7: Other types of nested aggregation function checks
	t.Run("NestedAggregationDetection", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test sum(count(*)) - Nested aggregator functions
		query1 := "SELECT sum(count(*)) as nested_agg FROM stream GROUP BY device, TumblingWindow('1s')"
		err1 := ssql.Execute(query1)
		assert.NotNil(t, err1)
		assert.Contains(t, err1.Error(), "aggregate function calls cannot be nested")

		// Testing avg(min(temperature)) - Nested aggregation functions
		query2 := "SELECT avg(min(temperature)) as nested_agg FROM stream GROUP BY device, TumblingWindow('1s')"
		err2 := ssql.Execute(query2)
		assert.NotNil(t, err2)
		assert.Contains(t, err2.Error(), "aggregate function calls cannot be nested")

		// Test round(avg(temperature), 1) - normal function nested aggregation function should be normal
		query3 := "SELECT round(avg(temperature), 1) as normal_nesting FROM stream GROUP BY device, TumblingWindow('1s')"
		err3 := ssql.Execute(query3)
		assert.Nil(t, err3) // This nesting should be allowed
	})

	// Test 7: Nested date-time function
	t.Run("DateTimeFunctionNesting", func(t *testing.T) {
		// Test year(date_add(created_at, 1, 'years'))
		ssql := streamsql.New()
		defer ssql.Stop()

		query := "SELECT device, year(date_add(created_at, 1, 'years')) as next_year FROM stream"
		err := ssql.Execute(query)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		strm.Emit(map[string]any{"device": "sensor1", "created_at": "2023-12-25 15:30:45"})

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			// Verification execution sequence: date_add ('2023-12-25 15:30:45', 1, 'years') -> '2024-12-25 15:30:45', year('2024-12-25 15:30:45') - > 2024
			assert.Equal(t, 2024, item["next_year"])
		case <-ctx.Done():
			t.Fatal("Test timeout")
		}
	})

	// Test 8: Incorrect execution order of nested functions
	t.Run("ErrorHandlingInNestedFunctions", func(t *testing.T) {
		// Test sqrt(len(invalid_field)) - should handle errors
		ssql := streamsql.New()
		defer ssql.Stop()

		query := "SELECT device, sqrt(len(invalid_field)) as error_result FROM stream"
		err := ssql.Execute(query)
		assert.Nil(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data (excluding invalid_field)
		strm.Emit(map[string]any{"device": "sensor1", "temperature": 25.0})

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			// Validation error handling: invalid_field does not exist, it should return nil or the default value
			_, exists := item["error_result"]
			assert.True(t, exists)
		case <-ctx.Done():
			t.Fatal("Test timeout")
		}
	})
}

// flattenUnnestRows expands batch results that may contain unnest results into multiple rows for easier assertions
// Compatible with two forms:
// 1) Current implementation: returns a single line, where the alias field is []any (needs to be expanded on the test side)
// 2) Future implementation: The engine returns multiple lines directly (at this point, it returns as is).
func flattenUnnestRows(result []map[string]any, alias string) []map[string]any {
	// If there are already multiple lines, just return directly
	if len(result) > 1 {
		return result
	}
	if len(result) == 0 {
		return result
	}

	// Forms: [{ alias: []any{...},...}]
	if v, ok := result[0][alias]; ok {
		if functions.IsUnnestResult(v) {
			// Use ProcessUnnestResultWithFieldName to reserve field names and merge other fields
			expandedRows := functions.ProcessUnnestResultWithFieldName(v, alias)
			if len(expandedRows) == 0 {
				return result
			}

			// Merge other fields into each row
			results := make([]map[string]any, len(expandedRows))
			for i, unnestRow := range expandedRows {
				newRow := make(map[string]any, len(result[0])+len(unnestRow))
				// Copy other fields of the original row (except the unnest field)
				for k, v := range result[0] {
					if k != alias {
						newRow[k] = v
					}
				}
				// Add the field expanded by unnest
				for k, v := range unnestRow {
					newRow[k] = v
				}
				results[i] = newRow
			}
			return results
		}
	}

	return result
}

// TestUnnestFunctionIntegration verifies whether unnest(array) is expanding the array into multiple rows as expected
// This use case is integrated into the full SQL execution path:
// - Syntax: unnest(array)
// - Description: Expand the array into multiple rows
// - Example: SELECT unnest(tags) as tag FROM stream
func TestUnnestFunctionIntegration(t *testing.T) {
	t.Parallel()
	t.Run("PrimitiveArray", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		sql := "SELECT unnest(tags) as tag FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Input as a regular string array
		input := map[string]any{
			"tags": []string{"a", "b", "c"},
		}
		strm.Emit(input)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case raw := <-resultChan:
			batch, ok := raw.([]map[string]any)
			require.True(t, ok)
			// Standardized into multi-row forms according to these two forms
			rows := flattenUnnestRows(batch, "tag")
			require.Len(t, rows, 3)

			expected := []string{"a", "b", "c"}
			for i, exp := range expected {
				row := rows[i]
				// Compatible with two types of field names: Engine direct expansion may use alias (tags), function-side expansion uses default fields (values)
				var got any
				if v, ok := row["tag"]; ok {
					got = v
				} else if v, ok := row["value"]; ok {
					got = v
				} else {
					t.Fatalf("row %d does not contain expected field 'tag' or 'value': %v", i, row)
				}
				assert.Equal(t, exp, got)
			}
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("CombinedColumns", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Test the combination column: SELECT id, unnest(tags) as tag FROM events
		sql := "SELECT id, unnest(tags) as tag FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Enter an array containing id fields and tags
		input := map[string]any{
			"id":   100,
			"tags": []string{"a", "b", "c"},
		}
		strm.Emit(input)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case raw := <-resultChan:
			batch, ok := raw.([]map[string]any)
			require.True(t, ok)
			// Unnest results
			rows := flattenUnnestRows(batch, "tag")
			require.Len(t, rows, 3)

			// Verify that each row contains id and tag fields
			expectedTags := []string{"a", "b", "c"}
			for i, expectedTag := range expectedTags {
				row := rows[i]

				// The verification id field remains unchanged
				assert.Equal(t, 100, row["id"], "row %d should have id=100", i)

				// Verify the tag field
				var gotTag any
				if v, ok := row["tag"]; ok {
					gotTag = v
				} else if v, ok := row["value"]; ok {
					gotTag = v
				} else {
					t.Fatalf("row %d does not contain expected field 'tag' or 'value': %v", i, row)
				}
				assert.Equal(t, expectedTag, gotTag, "row %d should have tag=%s", i, expectedTag)
			}
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})
	t.Run("ObjectArray", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		sql := "SELECT unnest(props) as prop FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Input as an array of objects
		input := map[string]any{
			"props": []map[string]any{
				{"k": "x", "v": 1},
				{"k": "y", "v": 2},
			},
		}
		strm.Emit(input)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case raw := <-resultChan:
			batch, ok := raw.([]map[string]any)
			require.True(t, ok)

			rows := flattenUnnestRows(batch, "prop")
			require.Len(t, rows, 2)

			// Verify that each row contains fields within the object
			assert.Equal(t, "x", firstOf(rows[0], "k", "prop", "k"))
			assert.Equal(t, 1, firstOf(rows[0], "v", "prop", "v"))
			assert.Equal(t, "y", firstOf(rows[1], "k", "prop", "k"))
			assert.Equal(t, 2, firstOf(rows[1], "v", "prop", "v"))
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("EmptyArray", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		sql := "SELECT unnest(tags) as tag FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Empty array
		input := map[string]any{
			"tags": []string{},
		}
		strm.Emit(input)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case raw := <-resultChan:
			batch, ok := raw.([]map[string]any)
			require.True(t, ok)

			rows := flattenUnnestRows(batch, "tag")
			assert.Len(t, rows, 0)
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})

	t.Run("NilArray", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		sql := "SELECT unnest(tags) as tag FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// nil value
		input := map[string]any{
			"tags": nil,
		}
		strm.Emit(input)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case raw := <-resultChan:
			batch, ok := raw.([]map[string]any)
			require.True(t, ok)

			rows := flattenUnnestRows(batch, "tag")
			assert.Len(t, rows, 0)
		case <-ctx.Done():
			t.Fatal("The test timed out and no results were received")
		}
	})
}

// firstOf assists in reading field values from rows, compatible with prop as an object
// Prioritize values by top-level field; if none exist, try to obtain them from nested objects (such as prop[k]).
func firstOf(row map[string]any, topLevelKey string, nestedObjKey string, nestedField string) any {
	if v, ok := row[topLevelKey]; ok {
		return v
	}
	if m, ok := row[nestedObjKey].(map[string]any); ok {
		return m[nestedField]
	}
	return nil
}
