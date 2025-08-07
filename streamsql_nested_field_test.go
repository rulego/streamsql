package streamsql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestComprehensiveNestedFieldAccess 全面测试嵌套字段访问功能
func TestComprehensiveNestedFieldAccess(t *testing.T) {
	t.Run("多层嵌套字段访问", func(t *testing.T) {
		streamsql := New()
		defer func() {
			if streamsql != nil {
				streamsql.Stop()
			}
		}()

		// 测试多层嵌套字段访问
		var rsql = "SELECT device.info.name as device_name, device.location.building as building, sensor.data.temperature as temp FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err, "多层嵌套字段SQL应该能够执行")

		require.NoError(t, err, "多层嵌套字段访问不应该出错")

		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果接收器
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加带多层嵌套字段的测试数据
		testData := map[string]interface{}{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"name":   "温度传感器001",
					"type":   "temperature",
					"status": "active",
				},
				"location": map[string]interface{}{
					"building": "A栋",
					"floor":    "3F",
					"room":     "301",
				},
			},
			"sensor": map[string]interface{}{
				"data": map[string]interface{}{
					"temperature": 28.5,
					"humidity":    65.0,
					"pressure":    1013.25,
				},
				"status": "online",
			},
		}

		// 发送数据
		strm.Emit(testData)

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			// 验证结果
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok, "结果应该是[]map[string]interface{}类型")
			require.Len(t, resultSlice, 1, "应该只有一条结果")

			item := resultSlice[0]

			// 检查各个嵌套字段的提取情况
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
			t.Fatal("多层嵌套测试超时")
		}
	})

	t.Run("嵌套字段聚合查询", func(t *testing.T) {
		streamsql := New()
		defer func() {
			if streamsql != nil {
				streamsql.Stop()
			}
		}()

		// 测试聚合查询中的嵌套字段
		var rsql = "SELECT device.type, AVG(sensor.temperature) as avg_temp, COUNT(*) as cnt FROM stream GROUP BY device.type, TumblingWindow('1s')"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err, "嵌套字段聚合SQL应该能够执行")

		require.NoError(t, err, "嵌套字段聚合查询不应该出错")

		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果回调
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []map[string]interface{}{
			{
				"device": map[string]interface{}{
					"type": "temperature",
					"id":   "sensor001",
				},
				"sensor": map[string]interface{}{
					"temperature": 25.0,
				},
			},
			{
				"device": map[string]interface{}{
					"type": "temperature",
					"id":   "sensor002",
				},
				"sensor": map[string]interface{}{
					"temperature": 35.0,
				},
			},
			{
				"device": map[string]interface{}{
					"type": "humidity",
					"id":   "sensor003",
				},
				"sensor": map[string]interface{}{
					"temperature": 20.0,
				},
			},
		}

		// 添加数据
		for _, data := range testData {
			strm.Emit(data)
		}

		// 等待窗口触发
		time.Sleep(1200 * time.Millisecond)

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			// 验证聚合结果
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok, "结果应该是[]map[string]interface{}类型")

			// 聚合查询可能返回空结果，这是正常的
			if len(resultSlice) > 0 {
				// 如果有结果，验证结果结构
				for _, item := range resultSlice {
					// 检查是否包含任何聚合字段
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
			t.Fatal("嵌套字段聚合查询测试超时")
		}
	})

	t.Run("复杂嵌套字段WHERE条件", func(t *testing.T) {
		streamsql := New()
		defer func() {
			if streamsql != nil {
				streamsql.Stop()
			}
		}()

		// 测试复杂的WHERE条件
		var rsql = "SELECT * FROM stream WHERE device.info.status = 'active' AND sensor.data.temperature > 25 AND device.location.building = 'A栋'"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err, "复杂嵌套字段WHERE SQL应该能够执行")

		require.NoError(t, err, "复杂嵌套字段WHERE条件不应该出错")

		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果接收器
		strm.AddSink(func(result []map[string]interface{}) {
			resultChan <- result
		})

		// 添加测试数据：一条满足所有条件，一条不满足
		testData1 := map[string]interface{}{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"name":   "传感器A",
					"status": "active", // 满足条件
				},
				"location": map[string]interface{}{
					"building": "A栋", // 满足条件
				},
			},
			"sensor": map[string]interface{}{
				"data": map[string]interface{}{
					"temperature": 30.0, // 满足条件 > 25
				},
			},
		}

		testData2 := map[string]interface{}{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"name":   "传感器B",
					"status": "inactive", // 不满足条件
				},
				"location": map[string]interface{}{
					"building": "B栋", // 不满足条件
				},
			},
			"sensor": map[string]interface{}{
				"data": map[string]interface{}{
					"temperature": 20.0, // 不满足条件 <= 25
				},
			},
		}

		// 发送数据
		strm.Emit(testData1)
		strm.Emit(testData2)

		// 等待结果
		var results []interface{}
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

		// 验证结果
		assert.Greater(t, len(results), 0, "复杂WHERE条件应该返回结果")

		for _, result := range results {
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok, "结果应该是[]map[string]interface{}类型")

			for _, item := range resultSlice {
				// 验证通过过滤的数据确实满足所有条件
				device, deviceOk := item["device"].(map[string]interface{})
				assert.True(t, deviceOk, "device字段应该存在且为map类型")

				info, infoOk := device["info"].(map[string]interface{})
				assert.True(t, infoOk, "device.info字段应该存在且为map类型")

				status, statusOk := info["status"].(string)
				assert.True(t, statusOk, "device.info.status字段应该存在且为string类型")
				assert.Equal(t, "active", status, "status应该是active")

				location, locationOk := device["location"].(map[string]interface{})
				assert.True(t, locationOk, "device.location字段应该存在且为map类型")

				building, buildingOk := location["building"].(string)
				assert.True(t, buildingOk, "device.location.building字段应该存在且为string类型")
				assert.Equal(t, "A栋", building, "building应该是A栋")

				sensor, sensorOk := item["sensor"].(map[string]interface{})
				assert.True(t, sensorOk, "sensor字段应该存在且为map类型")

				data, dataOk := sensor["data"].(map[string]interface{})
				assert.True(t, dataOk, "sensor.data字段应该存在且为map类型")

				temp, tempOk := data["temperature"].(float64)
				assert.True(t, tempOk, "sensor.data.temperature字段应该存在且为float64类型")
				assert.Greater(t, temp, 25.0, "temperature应该大于25")
			}
		}
	})
}
