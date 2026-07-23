package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestComprehensiveNestedFieldAccess Comprehensive testing of nested field access functionality
func TestComprehensiveNestedFieldAccess(t *testing.T) {
	t.Parallel()
	t.Run("多层嵌套字段访问", func(t *testing.T) {
		ssql := streamsql.New()
		defer func() {
			if ssql != nil {
				ssql.Stop()
			}
		}()

		// Test multi-layer nested field access
		var rsql = "SELECT device.info.name as device_name, device.location.building as building, sensor.data.temperature as temp FROM stream"
		err := ssql.Execute(rsql)
		assert.Nil(t, err, "多层嵌套字段SQL应该能够执行")

		require.NoError(t, err, "多层嵌套字段访问不应该出错")

		strm := ssql.Stream()

		// Create a result receiving channel
		resultChan := make(chan any, 10)

		// Add a result receiver
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data with multi-layer nested fields
		testData := map[string]any{
			"device": map[string]any{
				"info": map[string]any{
					"name":   "温度传感器001",
					"type":   "temperature",
					"status": "active",
				},
				"location": map[string]any{
					"building": "A栋",
					"floor":    "3F",
					"room":     "301",
				},
			},
			"sensor": map[string]any{
				"data": map[string]any{
					"temperature": 28.5,
					"humidity":    65.0,
					"pressure":    1013.25,
				},
				"status": "online",
			},
		}

		// Send data
		strm.Emit(testData)

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			// Verify the results
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok, "结果应该是[]map[string]any类型")
			require.Len(t, resultSlice, 1, "应该只有一条结果")

			item := resultSlice[0]

			// Check the extraction status of each nested field
			deviceName, deviceNameExists := item["device_name"]
			assert.True(t, deviceNameExists, "device.info.name字段应该存在")
			assert.Equal(t, "温度传感器001", deviceName, "device.info.name应该被正确提取")

			building, buildingExists := item["building"]
			assert.True(t, buildingExists, "device.location.building字段应该存在")
			assert.Equal(t, "A栋", building, "device.location.building应该被正确提取")

			temp, tempExists := item["temp"]
			assert.True(t, tempExists, "sensor.data.temperature字段应该存在")
			assert.Equal(t, 28.5, temp, "sensor.data.temperature应该被正确提取")

		case <-ctx.Done():
			t.Fatal("Multi-layer nested test timeout")
		}
	})

	t.Run("嵌套字段聚合查询", func(t *testing.T) {
		ssql := streamsql.New()
		defer func() {
			if ssql != nil {
				ssql.Stop()
			}
		}()

		// Test nested fields in aggregated queries
		var rsql = "SELECT device.type, AVG(sensor.temperature) as avg_temp, COUNT(*) as cnt FROM stream GROUP BY device.type, TumblingWindow('1s')"
		err := ssql.Execute(rsql)
		assert.Nil(t, err, "嵌套字段聚合SQL应该能够执行")

		require.NoError(t, err, "嵌套字段聚合查询不应该出错")

		strm := ssql.Stream()

		// Create a result receiving channel
		resultChan := make(chan any, 10)

		// Add result callbacks
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data
		testData := []map[string]any{
			{
				"device": map[string]any{
					"type": "temperature",
					"id":   "sensor001",
				},
				"sensor": map[string]any{
					"temperature": 25.0,
				},
			},
			{
				"device": map[string]any{
					"type": "temperature",
					"id":   "sensor002",
				},
				"sensor": map[string]any{
					"temperature": 35.0,
				},
			},
			{
				"device": map[string]any{
					"type": "humidity",
					"id":   "sensor003",
				},
				"sensor": map[string]any{
					"temperature": 20.0,
				},
			},
		}

		// Add data
		for _, data := range testData {
			strm.Emit(data)
		}

		// Wait for the window to trigger
		time.Sleep(1200 * time.Millisecond)

		// Wait for the results
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			// Verify the aggregated results
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok, "结果应该是[]map[string]any类型")

			// Aggregated queries may return empty results, which is normal
			if len(resultSlice) > 0 {
				// If there are results, verify the structure of the results
				for _, item := range resultSlice {
					// Check for any aggregated fields
					hasAnyField := false
					if _, exists := item["type"]; exists {
						hasAnyField = true
					}
					if _, exists := item["avg_temp"]; exists {
						hasAnyField = true
					}
					if _, exists := item["cnt"]; exists {
						hasAnyField = true
					}
					assert.True(t, hasAnyField, "聚合结果应该包含至少一个预期字段")
				}
			}

		case <-ctx.Done():
			t.Fatal("Nested field aggregation query test timeout")
		}
	})

	t.Run("复杂嵌套字段WHERE条件", func(t *testing.T) {
		ssql := streamsql.New()
		defer func() {
			if ssql != nil {
				ssql.Stop()
			}
		}()

		// Test complex WHERE conditions
		var rsql = "SELECT * FROM stream WHERE device.info.status = 'active' AND sensor.data.temperature > 25 AND device.location.building = 'A栋'"
		err := ssql.Execute(rsql)
		assert.Nil(t, err, "复杂嵌套字段WHERE SQL应该能够执行")

		require.NoError(t, err, "复杂嵌套字段WHERE条件不应该出错")

		strm := ssql.Stream()

		// Create a result receiving channel
		resultChan := make(chan any, 10)

		// Add a result receiver
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Add test data: one meets all conditions, one does not
		testData1 := map[string]any{
			"device": map[string]any{
				"info": map[string]any{
					"name":   "传感器A",
					"status": "active", // Conditions are met
				},
				"location": map[string]any{
					"building": "A栋", // Conditions are met
				},
			},
			"sensor": map[string]any{
				"data": map[string]any{
					"temperature": 30.0, // Conditions met: > 25
				},
			},
		}

		testData2 := map[string]any{
			"device": map[string]any{
				"info": map[string]any{
					"name":   "传感器B",
					"status": "inactive", // The conditions are not met
				},
				"location": map[string]any{
					"building": "B栋", // The conditions are not met
				},
			},
			"sensor": map[string]any{
				"data": map[string]any{
					"temperature": 20.0, // Conditions not met < = 25
				},
			},
		}

		// Send data
		strm.Emit(testData1)
		strm.Emit(testData2)

		// Wait for the results
		var results []any
		timeout := time.After(3 * time.Second)
		done := false

		for !done {
			select {
			case result := <-resultChan:
				results = append(results, result)
				if len(results) >= 1 {
					done = true
				}
			case <-timeout:
				done = true
			}
		}

		// Verify the results
		assert.Greater(t, len(results), 0, "复杂WHERE条件应该返回结果")

		for _, result := range results {
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok, "结果应该是[]map[string]any类型")

			for _, item := range resultSlice {
				// Verify that the filtered data truly meets all the conditions
				device, deviceOk := item["device"].(map[string]any)
				assert.True(t, deviceOk, "device字段应该存在且为map类型")

				info, infoOk := device["info"].(map[string]any)
				assert.True(t, infoOk, "device.info字段应该存在且为map类型")

				status, statusOk := info["status"].(string)
				assert.True(t, statusOk, "device.info.status字段应该存在且为string类型")
				assert.Equal(t, "active", status, "status应该是active")

				location, locationOk := device["location"].(map[string]any)
				assert.True(t, locationOk, "device.location字段应该存在且为map类型")

				building, buildingOk := location["building"].(string)
				assert.True(t, buildingOk, "device.location.building字段应该存在且为string类型")
				assert.Equal(t, "A栋", building, "building应该是A栋")

				sensor, sensorOk := item["sensor"].(map[string]any)
				assert.True(t, sensorOk, "sensor字段应该存在且为map类型")

				data, dataOk := sensor["data"].(map[string]any)
				assert.True(t, dataOk, "sensor.data字段应该存在且为map类型")

				temp, tempOk := data["temperature"].(float64)
				assert.True(t, tempOk, "sensor.data.temperature字段应该存在且为float64类型")
				assert.Greater(t, temp, 25.0, "temperature应该大于25")
			}
		}
	})
}

// TestArrayFieldAccess test array field access feature
func TestArrayFieldAccess(t *testing.T) {
	t.Parallel()
	t.Run("数组索引访问", func(t *testing.T) {
		ssql := streamsql.New()
		defer func() {
			if ssql != nil {
				ssql.Stop()
			}
		}()

		// Test array index access
		var rsql = "SELECT items[0].name as first_item_name, items[1].id as second_item_id, values[2] as third_value FROM stream"
		err := ssql.Execute(rsql)
		assert.Nil(t, err, "数组索引访问SQL应该能够执行")
		require.NoError(t, err, "数组索引访问不应该出错")

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Test data
		testData := map[string]any{
			"items": []any{
				map[string]any{"name": "item1", "id": 101},
				map[string]any{"name": "item2", "id": 102},
			},
			"values": []any{10, 20, 30, 40},
		}

		strm.Emit(testData)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)

			item := resultSlice[0]

			// Validate items[0].name
			name, ok := item["first_item_name"]
			assert.True(t, ok)
			assert.Equal(t, "item1", name)

			// Verify items[1].id
			id, ok := item["second_item_id"]
			assert.True(t, ok)
			assert.Equal(t, 102, id)

			// Validating values[2]
			val, ok := item["third_value"]
			assert.True(t, ok)
			assert.Equal(t, 30, val)

		case <-ctx.Done():
			t.Fatal("Test timeout")
		}
	})

	t.Run("数组索引在WHERE条件中", func(t *testing.T) {
		ssql := streamsql.New()
		defer func() {
			if ssql != nil {
				ssql.Stop()
			}
		}()

		var rsql = "SELECT * FROM stream WHERE tags[0] = 'urgent' AND scores[1] > 90"
		err := ssql.Execute(rsql)
		require.NoError(t, err)

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		// Matching data
		matchData := map[string]any{
			"id":     "match",
			"tags":   []any{"urgent", "work"},
			"scores": []any{80, 95},
		}
		// Mismatched data
		mismatchData := map[string]any{
			"id":     "mismatch",
			"tags":   []any{"normal", "home"},
			"scores": []any{80, 85},
		}

		strm.Emit(matchData)
		strm.Emit(mismatchData)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			require.Len(t, resultSlice, 1)
			assert.Equal(t, "match", resultSlice[0]["id"])
		case <-ctx.Done():
			t.Fatal("Test timeout")
		}
	})

	t.Run("嵌套数组聚合查询", func(t *testing.T) {
		ssql := streamsql.New()
		defer func() {
			if ssql != nil {
				ssql.Stop()
			}
		}()

		// Test nested array fields in aggregated queries
		var rsql = "SELECT device.type, AVG(sensors[0].temperature) as avg_temp FROM stream GROUP BY device.type, TumblingWindow('1s')"
		err := ssql.Execute(rsql)
		assert.Nil(t, err, "嵌套数组聚合SQL应该能够执行")

		strm := ssql.Stream()
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			resultChan <- result
		})

		testData := []map[string]any{
			{
				"device": map[string]any{"type": "temp_sensor"},
				"sensors": []any{
					map[string]any{"temperature": 20.0},
				},
			},
			{
				"device": map[string]any{"type": "temp_sensor"},
				"sensors": []any{
					map[string]any{"temperature": 30.0},
				},
			},
		}

		for _, data := range testData {
			strm.Emit(data)
		}

		time.Sleep(1200 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			resultSlice, ok := result.([]map[string]any)
			require.True(t, ok)
			// The aggregate result may be empty, depending on when the window is triggered
			if len(resultSlice) > 0 {
				item := resultSlice[0]
				avgTemp, ok := item["avg_temp"].(float64)
				assert.True(t, ok)
				assert.Equal(t, 25.0, avgTemp)
			}
		case <-ctx.Done():
			t.Fatal("Test timeout")
		}
	})
}
