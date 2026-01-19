package streamsql

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/streamsql/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFunctionIntegrationNonAggregation 测试非聚合函数在SQL中的集成
func TestFunctionIntegrationNonAggregation(t *testing.T) {
	t.Run("MathFunctions", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试多个数学函数：abs, sqrt, round
		rsql := "SELECT device, abs(temperature) as abs_temp, sqrt(humidity) as sqrt_humidity, round(temperature) as rounded_temp FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := map[string]interface{}{
			"device":      "test-device",
			"temperature": -25.5,
			"humidity":    64.0,
		}
		strm.Emit(testData)

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "test-device", item["device"])
			// 验证 abs(-25.5) = 25.5
			assert.InEpsilon(t, 25.5, item["abs_temp"], 0.001)
			// 验证 sqrt(64) = 8
			assert.InEpsilon(t, 8.0, item["sqrt_humidity"], 0.001)
			// 验证 round(-25.5) = -26
			assert.InEpsilon(t, -26.0, item["rounded_temp"], 0.001)
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("StringFunctions", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试字符串函数：upper, lower, concat, length
		rsql := "SELECT upper(device) as upper_device, lower(location) as lower_location, concat(device, '-', location) as combined, length(device) as device_len FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := map[string]interface{}{
			"device":   "sensor01",
			"location": "ROOM_A",
		}
		strm.Emit(testData)

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "SENSOR01", item["upper_device"])
			assert.Equal(t, "room_a", item["lower_location"])
			assert.Equal(t, "sensor01-ROOM_A", item["combined"])
			assert.Equal(t, 8, item["device_len"])
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("ConversionFunctions", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试转换函数：cast
		rsql := "SELECT device, cast(temperature, 'int') as temp_int, cast(humidity, 'string') as humidity_str FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := map[string]interface{}{
			"device":      "test-device",
			"temperature": 25.7,
			"humidity":    65.0,
		}
		strm.Emit(testData)

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "test-device", item["device"])
			assert.Equal(t, int32(25), item["temp_int"])
			assert.Equal(t, "65", item["humidity_str"])
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("DateTimeFunctions", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试日期时间函数：now, year, month, day
		rsql := "SELECT device, now() as current_time, year(timestamp) as ts_year, month(timestamp) as ts_month FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		//testTime := time.Date(2025, 4, 15, 10, 30, 0, 0, time.UTC)
		testData := map[string]interface{}{
			"device":    "test-device",
			"timestamp": "2025-08-25",
		}
		strm.Emit(testData)

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "test-device", item["device"])
			assert.NotNil(t, item["current_time"])
			assert.Equal(t, 2025, item["ts_year"])
			assert.Equal(t, 8, item["ts_month"])
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("JSONFunctions", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试JSON函数：json_extract, json_valid
		rsql := "SELECT device, json_extract(metadata, '$.type') as device_type, json_valid(metadata) as is_valid_json FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := map[string]interface{}{
			"device":   "test-device",
			"metadata": `{"type": "temperature_sensor", "version": "1.0"}`,
		}
		strm.Emit(testData)

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "test-device", item["device"])
			assert.Equal(t, "temperature_sensor", item["device_type"])
			assert.Equal(t, true, item["is_valid_json"])
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("JSONExtractMapSupport", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// Test json_extract with map input
		rsql := "SELECT device, json_extract(properties, '$.color') as device_color FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// Add test data with map
		testData := map[string]interface{}{
			"device": "test-device-map",
			"properties": map[string]interface{}{
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
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "test-device-map", item["device"])
			assert.Equal(t, "red", item["device_color"])
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("JSONExtractArrayAndNested", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// Test json_extract with array and nested structures
		rsql := "SELECT device, json_extract(tags, '$[0]') as first_tag, json_extract(data, '$.users[0].name') as first_user_name FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// Add test data with complex structures
		testData := map[string]interface{}{
			"device": "complex-device",
			"tags":   []interface{}{"tag1", "tag2"},
			"data": map[string]interface{}{
				"users": []interface{}{
					map[string]interface{}{"name": "Alice", "age": 30},
					map[string]interface{}{"name": "Bob", "age": 25},
				},
			},
		}
		strm.Emit(testData)

		// Wait for result
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "complex-device", item["device"])
			assert.Equal(t, "tag1", item["first_tag"])
			assert.Equal(t, "Alice", item["first_user_name"])
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("JSONExtractWithAggregation", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// Test json_extract nested in aggregation function
		// json_extract returns interface{}, usually need cast to number for aggregation like sum/avg
		// specific logic depends on whether aggregator handles string/interface conversion
		// Here we assume json_extract returns float64 for numbers (from Unmarshal) or use cast
		rsql := "SELECT count(json_extract(tags, '$[0]')) as tag_count, sum(cast(json_extract(data, '$.value'), 'float')) as total_value FROM stream GROUP BY device, TumblingWindow('1s')"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		testData := []map[string]interface{}{
			{
				"device": "device1",
				"tags":   []interface{}{"tag1", "tag2"},
				"data":   map[string]interface{}{"value": 10},
			},
			{
				"device": "device1",
				"tags":   []interface{}{"tag3"},
				"data":   map[string]interface{}{"value": 20},
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
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "device1", item["device"])
			assert.Equal(t, float64(2), item["tag_count"])
			assert.Equal(t, float64(30), item["total_value"])
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})
}

// TestFunctionIntegrationAggregation 测试聚合函数在SQL中的集成
func TestFunctionIntegrationAggregation(t *testing.T) {
	t.Run("BasicAggregationFunctions", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试基本聚合函数：sum, avg, min, max, count
		rsql := "SELECT device, sum(temperature) as total_temp, avg(temperature) as avg_temp, min(temperature) as min_temp, max(temperature) as max_temp, count(temperature) as temp_count FROM stream GROUP BY device, TumblingWindow('1s')"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []map[string]interface{}{
			{"device": "sensor1", "temperature": 20.0},
			{"device": "sensor1", "temperature": 25.0},
			{"device": "sensor1", "temperature": 30.0},
			{"device": "sensor2", "temperature": 15.0},
			{"device": "sensor2", "temperature": 18.0},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// 等待窗口初始化
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 2)

			// 验证sensor1的聚合结果
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
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("StatisticalAggregationFunctions", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试统计聚合函数：stddev, median, percentile
		rsql := "SELECT device, stddev(temperature) as temp_stddev, median(temperature) as temp_median FROM stream GROUP BY device, TumblingWindow('1s')"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []map[string]interface{}{
			{"device": "sensor1", "temperature": 10.0},
			{"device": "sensor1", "temperature": 20.0},
			{"device": "sensor1", "temperature": 30.0},
			{"device": "sensor1", "temperature": 40.0},
			{"device": "sensor1", "temperature": 50.0},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// 等待窗口初始化
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "sensor1", item["device"])
			// 标准差应该约为15.81
			assert.InEpsilon(t, 15.81, item["temp_stddev"].(float64), 0.1)
			// 中位数应该为30.0
			assert.InEpsilon(t, 30.0, item["temp_median"].(float64), 0.001)
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("CollectionAggregationFunctions", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试集合聚合函数：collect, first_value, last_value
		rsql := "SELECT device, collect(temperature) as temp_array, first_value(temperature) as first_temp, last_value(temperature) as last_temp FROM stream GROUP BY device, TumblingWindow('1s')"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []map[string]interface{}{
			{"device": "sensor1", "temperature": 20.0},
			{"device": "sensor1", "temperature": 25.0},
			{"device": "sensor1", "temperature": 30.0},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// 等待窗口初始化
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "sensor1", item["device"])

			// 验证collect函数返回的数组
			tempArray, ok := item["temp_array"].([]interface{})
			assert.True(t, ok)
			assert.Len(t, tempArray, 3)
			assert.Contains(t, tempArray, 20.0)
			assert.Contains(t, tempArray, 25.0)
			assert.Contains(t, tempArray, 30.0)

			// 验证first_value和last_value
			assert.Equal(t, 20.0, item["first_temp"])
			assert.Equal(t, 30.0, item["last_temp"])
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})
}

// TestFunctionIntegrationMixed 测试混合函数场景
func TestFunctionIntegrationMixed(t *testing.T) {
	t.Run("AggregationWithNonAggregationFunctions", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试聚合函数与非聚合函数混合使用
		rsql := "SELECT device, upper(device) as device_upper, avg(temperature) as avg_temp, round(avg(temperature), 2) as rounded_avg FROM stream GROUP BY device, TumblingWindow('1s')"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []map[string]interface{}{
			{"device": "sensor1", "temperature": 20.567},
			{"device": "sensor1", "temperature": 25.234},
			{"device": "sensor1", "temperature": 30.123},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// 等待窗口初始化
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			assert.Equal(t, "sensor1", item["device"])
			assert.Equal(t, "SENSOR1", item["device_upper"])

			// 验证平均值
			if avgTemp, exists := item["avg_temp"]; exists && avgTemp != nil {
				assert.InEpsilon(t, 25.308, avgTemp.(float64), 0.001)
			} else {
				t.Errorf("avg_temp is missing or nil: %v", avgTemp)
			}

			// 验证四舍五入的平均值
			if roundedAvg, exists := item["rounded_avg"]; exists {
				if roundedAvg == nil {
					t.Errorf("rounded_avg exists but is nil - this indicates the round(avg()) expression failed")
				} else if val, ok := roundedAvg.(float64); ok {
					// 验证结果在合理范围内
					assert.True(t, val >= 25.0 && val <= 25.5, "rounded_avg should be between 25.0 and 25.5, got %v", val)
				} else {
					t.Errorf("rounded_avg is not a float64: %v (type: %T)", roundedAvg, roundedAvg)
				}
			} else {
				t.Errorf("rounded_avg field is missing from result")
			}
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("NestedFunctionCalls", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试嵌套函数调用
		rsql := "SELECT device, upper(concat(device, '_', cast(round(temperature, 0), 'string'))) as device_temp_label FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := map[string]interface{}{
			"device":      "sensor1",
			"temperature": 25.7,
		}
		strm.Emit(testData)

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "sensor1", item["device"])
			// round(25.7, 0) = 26, cast(26, 'string') = "26", concat("sensor1", "_", "26") = "sensor1_26", upper("sensor1_26") = "SENSOR1_26"
			assert.Equal(t, "SENSOR1_26", item["device_temp_label"])
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("WindowFunctionsWithAggregation", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试窗口函数与聚合函数结合
		rsql := "SELECT device, avg(temperature) as avg_temp, window_start() as start_time, window_end() as end_time FROM stream GROUP BY device, TumblingWindow('1s')"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []map[string]interface{}{
			{"device": "sensor1", "temperature": 20.0},
			{"device": "sensor1", "temperature": 30.0},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// 等待窗口初始化
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "sensor1", item["device"])
			assert.InEpsilon(t, 25.0, item["avg_temp"].(float64), 0.001)
			assert.NotNil(t, item["start_time"])
			assert.NotNil(t, item["end_time"])
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})
}

// TestNestedFunctionSupport 测试嵌套函数支持
func TestNestedFunctionSupport(t *testing.T) {
	t.Run("NormalFunctionNestingAggregation", func(t *testing.T) {
		// 测试普通函数嵌套聚合函数：round(avg(temperature), 2)
		streamsql := New()
		defer streamsql.Stop()

		// 执行包含 round(avg(temperature), 2) 的查询
		query := "SELECT device, round(avg(temperature), 2) as rounded_avg FROM stream GROUP BY device, TumblingWindow('1s')"
		err := streamsql.Execute(query)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []map[string]interface{}{
			{"device": "sensor1", "temperature": 20.567},
			{"device": "sensor1", "temperature": 25.234},
			{"device": "sensor1", "temperature": 30.123},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// 等待窗口初始化
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]
			assert.Equal(t, "sensor1", item["device"])

			// 验证四舍五入的平均值
			if roundedAvg, exists := item["rounded_avg"]; exists {
				if roundedAvg == nil {
					t.Errorf("rounded_avg exists but is nil - this indicates the round(avg()) expression failed")
				} else if val, ok := roundedAvg.(float64); ok {
					// 平均值应该是 (20.567 + 25.234 + 30.123) / 3 = 25.308
					// round(25.308, 2) = 25.31
					assert.InEpsilon(t, 25.31, val, 0.01)
				} else {
					t.Errorf("rounded_avg is not a float64: %v (type: %T)", roundedAvg, roundedAvg)
				}
			} else {
				t.Errorf("rounded_avg field is missing from result")
			}
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("AggregationNestingNormalFunction", func(t *testing.T) {
		// 测试聚合函数嵌套普通函数：avg(round(temperature, 2))
		streamsql := New()
		defer streamsql.Stop()

		// 执行包含 avg(round(temperature, 2)) 的查询
		query := "SELECT device, avg(round(temperature, 2)) as avg_rounded FROM stream GROUP BY device, TumblingWindow('1s')"

		err := streamsql.Execute(query)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []map[string]interface{}{
			{"device": "sensor1", "temperature": 20.567}, // round(20.567, 2) = 20.57
			{"device": "sensor1", "temperature": 25.234}, // round(25.234, 2) = 25.23
			{"device": "sensor1", "temperature": 30.123}, // round(30.123, 2) = 30.12
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// 等待窗口初始化
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			assert.Equal(t, "sensor1", item["device"])

			// 验证聚合函数嵌套普通函数的结果
			if avgRounded, exists := item["avg_rounded"]; exists {
				if avgRounded == nil {
					t.Errorf("avg_rounded exists but is nil - this indicates the avg(round()) expression failed")
				} else if val, ok := avgRounded.(float64); ok {
					// 期望值：avg(20.57, 25.23, 30.12) = (20.57 + 25.23 + 30.12) / 3 = 25.31
					assert.InEpsilon(t, 25.31, val, 0.01)
				} else {
					t.Errorf("avg_rounded is not a float64: %v (type: %T)", avgRounded, avgRounded)
				}
			} else {
				t.Errorf("avg_rounded field is missing from result")
			}
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("ComplexNestedFunctions", func(t *testing.T) {
		// 测试更复杂的嵌套函数：round(avg(abs(temperature)), 1)
		streamsql := New()
		defer streamsql.Stop()

		// 执行包含 round(avg(abs(temperature)), 1) 的查询
		query := "SELECT device, round(avg(abs(temperature)), 1) as complex_result FROM stream GROUP BY device, TumblingWindow('1s')"
		err := streamsql.Execute(query)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据（包含负数）
		testData := []map[string]interface{}{
			{"device": "sensor1", "temperature": -20.567}, // abs(-20.567) = 20.567
			{"device": "sensor1", "temperature": 25.234},  // abs(25.234) = 25.234
			{"device": "sensor1", "temperature": -30.123}, // abs(-30.123) = 30.123
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		// 等待窗口初始化
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			assert.Equal(t, "sensor1", item["device"])

			// 验证复杂嵌套函数的结果
			if complexResult, exists := item["complex_result"]; exists {
				if complexResult == nil {
					t.Errorf("complex_result exists but is nil - this indicates the round(avg(abs())) expression failed")
				} else if val, ok := complexResult.(float64); ok {
					// 期望值：avg(20.567, 25.234, 30.123) = 25.308, round(25.308, 1) = 25.3
					assert.InEpsilon(t, 25.3, val, 0.01)
				} else {
					t.Errorf("complex_result is not a float64: %v (type: %T)", complexResult, complexResult)
				}
			} else {
				t.Errorf("complex_result field is missing from result")
			}
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})
}

// TestNestedFunctionExecutionOrder 测试嵌套函数的执行顺序和不同类型函数的组合
func TestNestedFunctionExecutionOrder(t *testing.T) {

	// 测试1: 字符串函数嵌套数学函数
	t.Run("StringFunctionNestingMathFunction", func(t *testing.T) {
		// 测试 upper(concat("temp_", round(temperature, 1)))
		streamsql := New()
		defer streamsql.Stop()

		query := "SELECT device, upper(concat('temp_', round(temperature, 1))) as formatted_temp FROM stream"
		err := streamsql.Execute(query)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		strm.Emit(map[string]interface{}{"device": "sensor1", "temperature": 25.67})

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			// 验证执行顺序：round(25.67, 1) -> 25.7, concat('temp_', '25.7') -> 'temp_25.7', upper('temp_25.7') -> 'TEMP_25.7'
			assert.Equal(t, "TEMP_25.7", item["formatted_temp"])
		case <-ctx.Done():
			t.Fatal("测试超时")
		}
	})

	// 测试2: 数学函数嵌套字符串函数
	t.Run("MathFunctionNestingStringFunction", func(t *testing.T) {
		// 测试 round(len(upper(device)), 0)
		streamsql := New()
		defer streamsql.Stop()

		query := "SELECT device, round(len(upper(device)), 0) as device_length FROM stream"

		err := streamsql.Execute(query)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		strm.Emit(map[string]interface{}{"device": "sensor1"})

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			// 验证执行顺序：upper('sensor1') -> 'SENSOR1', len('SENSOR1') -> 7, round(7, 0) -> 7
			assert.Equal(t, float64(7), item["device_length"])
		case <-ctx.Done():
			t.Fatal("测试超时")
		}
	})

	// 测试3: 多层嵌套函数（3层）
	t.Run("ThreeLevelNestedFunctions", func(t *testing.T) {
		// 测试 abs(round(sqrt(temperature), 2))
		streamsql := New()
		defer streamsql.Stop()

		query := "SELECT device, abs(round(sqrt(temperature), 2)) as processed_temp FROM stream"

		err := streamsql.Execute(query)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		strm.Emit(map[string]interface{}{"device": "sensor1", "temperature": 16.0})

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]
			// 验证执行顺序：sqrt(16) -> 4, round(4, 2) -> 4.00, abs(4.00) -> 4.00
			assert.Equal(t, float64(4), item["processed_temp"])
		case <-ctx.Done():
			t.Fatal("测试超时")
		}
	})

	// 测试6: 复杂的聚合函数嵌套 - 应该报错
	t.Run("ComplexAggregationNesting", func(t *testing.T) {
		// 测试 max(round(avg(temperature), 1)) - 这是嵌套聚合函数，应该报错
		streamsql := New()
		defer streamsql.Stop()

		query := "SELECT device, max(round(avg(temperature), 1)) as max_rounded_avg FROM stream GROUP BY device, TumblingWindow('1s')"
		err := streamsql.Execute(query)
		// 应该返回嵌套聚合函数错误
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "aggregate function calls cannot be nested")
	})

	// 测试7: 其他类型的嵌套聚合函数检测
	t.Run("NestedAggregationDetection", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		// 测试 sum(count(*)) - 聚合函数嵌套聚合函数
		query1 := "SELECT sum(count(*)) as nested_agg FROM stream GROUP BY device, TumblingWindow('1s')"
		err1 := streamsql.Execute(query1)
		assert.NotNil(t, err1)
		assert.Contains(t, err1.Error(), "aggregate function calls cannot be nested")

		// 测试 avg(min(temperature)) - 聚合函数嵌套聚合函数
		query2 := "SELECT avg(min(temperature)) as nested_agg FROM stream GROUP BY device, TumblingWindow('1s')"
		err2 := streamsql.Execute(query2)
		assert.NotNil(t, err2)
		assert.Contains(t, err2.Error(), "aggregate function calls cannot be nested")

		// 测试 round(avg(temperature), 1) - 正常函数嵌套聚合函数，应该正常
		query3 := "SELECT round(avg(temperature), 1) as normal_nesting FROM stream GROUP BY device, TumblingWindow('1s')"
		err3 := streamsql.Execute(query3)
		assert.Nil(t, err3) // 这种嵌套应该是允许的
	})

	// 测试7: 日期时间函数嵌套
	t.Run("DateTimeFunctionNesting", func(t *testing.T) {
		// 测试 year(date_add(created_at, 1, 'years'))
		streamsql := New()
		defer streamsql.Stop()

		query := "SELECT device, year(date_add(created_at, 1, 'years')) as next_year FROM stream"
		err := streamsql.Execute(query)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		strm.Emit(map[string]interface{}{"device": "sensor1", "created_at": "2023-12-25 15:30:45"})

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			// 验证执行顺序：date_add('2023-12-25 15:30:45', 1, 'years') -> '2024-12-25 15:30:45', year('2024-12-25 15:30:45') -> 2024
			assert.Equal(t, 2024, item["next_year"])
		case <-ctx.Done():
			t.Fatal("测试超时")
		}
	})

	// 测试8: 错误的嵌套函数执行顺序
	t.Run("ErrorHandlingInNestedFunctions", func(t *testing.T) {
		// 测试 sqrt(len(invalid_field)) - 应该处理错误
		streamsql := New()
		defer streamsql.Stop()

		query := "SELECT device, sqrt(len(invalid_field)) as error_result FROM stream"
		err := streamsql.Execute(query)
		assert.Nil(t, err)

		strm := streamsql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据（不包含invalid_field）
		strm.Emit(map[string]interface{}{"device": "sensor1", "temperature": 25.0})

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok)
			assert.Len(t, resultSlice, 1)

			item := resultSlice[0]

			// 验证错误处理：invalid_field不存在，应该返回nil或默认值
			_, exists := item["error_result"]
			assert.True(t, exists)
		case <-ctx.Done():
			t.Fatal("测试超时")
		}
	})
}

// flattenUnnestRows 将可能包含 unnest 结果的批次结果展开为多行，便于断言
// 兼容两种形态：
// 1) 当前实现：返回单行，其中 alias 字段为 []interface{}（需要在测试侧展开）
// 2) 未来实现：引擎直接返回多行（此时原样返回）
func flattenUnnestRows(result []map[string]interface{}, alias string) []map[string]interface{} {
	// 如果已经是多行，直接返回
	if len(result) > 1 {
		return result
	}
	if len(result) == 0 {
		return result
	}

	// 形如：[{ alias: []interface{}{...} , ...}]
	if v, ok := result[0][alias]; ok {
		if functions.IsUnnestResult(v) {
			// 使用ProcessUnnestResultWithFieldName保留字段名，并合并其他字段
			expandedRows := functions.ProcessUnnestResultWithFieldName(v, alias)
			if len(expandedRows) == 0 {
				return result
			}

			// 将其他字段合并到每一行中
			results := make([]map[string]interface{}, len(expandedRows))
			for i, unnestRow := range expandedRows {
				newRow := make(map[string]interface{}, len(result[0])+len(unnestRow))
				// 复制原始行的其他字段（除了unnest字段）
				for k, v := range result[0] {
					if k != alias {
						newRow[k] = v
					}
				}
				// 添加unnest展开的字段
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

// TestUnnestFunctionIntegration 验证 unnest(array) 是否按预期将数组展开为多行
// 该用例集成到完整 SQL 执行路径：
// - 语法: unnest(array)
// - 描述: 将数组展开为多行
// - 示例: SELECT unnest(tags) as tag FROM stream
func TestUnnestFunctionIntegration(t *testing.T) {
	t.Run("PrimitiveArray", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		sql := "SELECT unnest(tags) as tag FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		strm := ssql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 输入为普通字符串数组
		input := map[string]interface{}{
			"tags": []string{"a", "b", "c"},
		}
		strm.Emit(input)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case raw := <-resultChan:
			batch, ok := raw.([]map[string]interface{})
			require.True(t, ok)
			// 按两种形态规范化为多行
			rows := flattenUnnestRows(batch, "tag")
			require.Len(t, rows, 3)

			expected := []string{"a", "b", "c"}
			for i, exp := range expected {
				row := rows[i]
				// 兼容两种字段命名：引擎直接展开可能使用别名(tag)，函数侧展开为默认字段(value)
				var got interface{}
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
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("CombinedColumns", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		// 测试组合列：SELECT id,unnest(tags) as tag FROM events
		sql := "SELECT id, unnest(tags) as tag FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		strm := ssql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 输入包含id字段和tags数组
		input := map[string]interface{}{
			"id":   100,
			"tags": []string{"a", "b", "c"},
		}
		strm.Emit(input)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case raw := <-resultChan:
			batch, ok := raw.([]map[string]interface{})
			require.True(t, ok)
			// 展开unnest结果
			rows := flattenUnnestRows(batch, "tag")
			require.Len(t, rows, 3)

			// 验证每行都包含id字段和tag字段
			expectedTags := []string{"a", "b", "c"}
			for i, expectedTag := range expectedTags {
				row := rows[i]

				// 验证id字段保持不变
				assert.Equal(t, 100, row["id"], "row %d should have id=100", i)

				// 验证tag字段
				var gotTag interface{}
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
			t.Fatal("测试超时，未收到结果")
		}
	})
	t.Run("ObjectArray", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		sql := "SELECT unnest(props) as prop FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		strm := ssql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 输入为对象数组
		input := map[string]interface{}{
			"props": []map[string]interface{}{
				{"k": "x", "v": 1},
				{"k": "y", "v": 2},
			},
		}
		strm.Emit(input)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case raw := <-resultChan:
			batch, ok := raw.([]map[string]interface{})
			require.True(t, ok)

			rows := flattenUnnestRows(batch, "prop")
			require.Len(t, rows, 2)

			// 校验每一行包含对象内的字段
			assert.Equal(t, "x", firstOf(rows[0], "k", "prop", "k"))
			assert.Equal(t, 1, firstOf(rows[0], "v", "prop", "v"))
			assert.Equal(t, "y", firstOf(rows[1], "k", "prop", "k"))
			assert.Equal(t, 2, firstOf(rows[1], "v", "prop", "v"))
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("EmptyArray", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		sql := "SELECT unnest(tags) as tag FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		strm := ssql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 空数组
		input := map[string]interface{}{
			"tags": []string{},
		}
		strm.Emit(input)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case raw := <-resultChan:
			batch, ok := raw.([]map[string]interface{})
			require.True(t, ok)

			rows := flattenUnnestRows(batch, "tag")
			assert.Len(t, rows, 0)
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	t.Run("NilArray", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		sql := "SELECT unnest(tags) as tag FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		strm := ssql.stream
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// nil 值
		input := map[string]interface{}{
			"tags": nil,
		}
		strm.Emit(input)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case raw := <-resultChan:
			batch, ok := raw.([]map[string]interface{})
			require.True(t, ok)

			rows := flattenUnnestRows(batch, "tag")
			assert.Len(t, rows, 0)
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})
}

// firstOf 辅助从行中读取字段值，兼容 prop 为对象的形态
// 优先按 top-level 字段取值，若不存在则尝试从嵌套对象（如 prop[k]）获取
func firstOf(row map[string]interface{}, topLevelKey string, nestedObjKey string, nestedField string) interface{} {
	if v, ok := row[topLevelKey]; ok {
		return v
	}
	if m, ok := row[nestedObjKey].(map[string]interface{}); ok {
		return m[nestedField]
	}
	return nil
}
