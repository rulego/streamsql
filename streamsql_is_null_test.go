package streamsql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsNullOperatorInSQL 测试IS NULL和IS NOT NULL语法功能
func TestIsNullOperatorInSQL(t *testing.T) {
	testCases := []struct {
		name     string
		sql      string
		testData []map[string]interface{}
		expected []map[string]interface{}
	}{
		{
			name: "IS NULL测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value IS NULL",
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor4", "value": nil},
			},
		},
		{
			name: "IS NOT NULL测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value IS NOT NULL",
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor3", "value": 30.0},
			},
		},
		{
			name: "嵌套字段IS NULL测试",
			sql:  "SELECT deviceId, device.location FROM stream WHERE device.location IS NULL",
			testData: []map[string]interface{}{
				{
					"deviceId": "sensor1",
					"device": map[string]interface{}{
						"location": "warehouse-A",
					},
				},
				{
					"deviceId": "sensor2",
					"device": map[string]interface{}{
						"location": nil,
					},
				},
				{
					"deviceId": "sensor3",
					"device":   map[string]interface{}{},
				},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor2", "device.location": nil},
				{"deviceId": "sensor3", "device.location": nil}, // 字段不存在也被认为是null
			},
		},
		{
			name: "组合条件 - IS NULL AND其他条件",
			sql:  "SELECT deviceId, value, status FROM stream WHERE value IS NULL AND status = 'active'",
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor2", "value": nil, "status": "active"},
				{"deviceId": "sensor3", "value": nil, "status": "inactive"},
				{"deviceId": "sensor4", "value": 30.0, "status": "active"},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor2", "value": nil, "status": "active"},
			},
		},
		{
			name: "组合条件 - IS NOT NULL OR其他条件",
			sql:  "SELECT deviceId, value, status FROM stream WHERE value IS NOT NULL OR status = 'error'",
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor2", "value": nil, "status": "active"},
				{"deviceId": "sensor3", "value": nil, "status": "error"},
				{"deviceId": "sensor4", "value": 30.0, "status": "inactive"},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor3", "value": nil, "status": "error"},
				{"deviceId": "sensor4", "value": 30.0, "status": "inactive"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建StreamSQL实例
			ssql := New()
			defer ssql.Stop()

			// 执行SQL
			err := ssql.Execute(tc.sql)
			require.NoError(t, err)

			// 收集结果
			var results []map[string]interface{}
			resultChan := make(chan interface{}, 10)

			ssql.Stream().AddSink(func(result interface{}) {
				resultChan <- result
			})

			// 使用一个done channel来同步
			done := make(chan bool, 1)

			// 添加测试数据
			for _, data := range tc.testData {
				ssql.Stream().Emit(data)
			}

			// 在另一个goroutine中收集结果
			go func() {
				defer func() { done <- true }()
				// 等待一段时间收集结果
				timeout := time.After(300 * time.Millisecond)
				for {
					select {
					case result := <-resultChan:
						if resultSlice, ok := result.([]map[string]interface{}); ok {
							results = append(results, resultSlice...)
						}
					case <-timeout:
						return
					}
				}
			}()

			// 等待收集完成
			<-done

			// 验证结果数量
			assert.Len(t, results, len(tc.expected), "结果数量应该匹配")

			// 验证结果内容（不依赖顺序）
			expectedDeviceIds := make([]string, len(tc.expected))
			for i, exp := range tc.expected {
				expectedDeviceIds[i] = exp["deviceId"].(string)
			}

			actualDeviceIds := make([]string, len(results))
			for i, result := range results {
				actualDeviceIds[i] = result["deviceId"].(string)
			}

			// 验证每个期望的设备ID都在结果中
			for _, expectedId := range expectedDeviceIds {
				assert.Contains(t, actualDeviceIds, expectedId, "结果应该包含设备ID %s", expectedId)
			}

			// 验证每个结果的字段值
			for _, result := range results {
				deviceId := result["deviceId"].(string)
				// 找到对应的期望结果
				var expectedResult map[string]interface{}
				for _, exp := range tc.expected {
					if exp["deviceId"].(string) == deviceId {
						expectedResult = exp
						break
					}
				}

				if expectedResult != nil {
					for key, expectedValue := range expectedResult {
						actualValue := result[key]
						assert.Equal(t, expectedValue, actualValue,
							"设备 %s 的字段 %s 值应该匹配: 期望 %v, 实际 %v", deviceId, key, expectedValue, actualValue)
					}
				}
			}
		})
	}
}

// TestIsNullInAggregation 测试聚合查询中的IS NULL
func TestIsNullInAggregation(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	// 聚合查询：统计非空值的数量
	sql := `SELECT deviceType, 
	               COUNT(*) as total_count,
	               COUNT(value) as non_null_count
	        FROM stream 
	        WHERE value IS NOT NULL
	        GROUP BY deviceType, TumblingWindow('2s')`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	// 收集结果
	resultChan := make(chan interface{}, 10)
	ssql.Stream().AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []map[string]interface{}{
		{"deviceType": "temperature", "value": 25.5},
		{"deviceType": "temperature", "value": nil},
		{"deviceType": "temperature", "value": 27.0},
		{"deviceType": "humidity", "value": 60.0},
		{"deviceType": "humidity", "value": nil},
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// 等待窗口触发
	time.Sleep(3 * time.Second)

	// 验证结果
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")

		// 应该有temperature和humidity两种类型的结果
		assert.GreaterOrEqual(t, len(resultSlice), 1, "应该至少有一个聚合结果")

		for _, item := range resultSlice {
			deviceType := item["deviceType"]
			totalCount, _ := item["total_count"].(float64)
			nonNullCount, _ := item["non_null_count"].(float64)

			if deviceType == "temperature" {
				// temperature有2个非空值（25.5, 27.0）
				assert.Equal(t, 2.0, totalCount, "temperature总数应该是2")
				assert.Equal(t, 2.0, nonNullCount, "temperature非空数应该是2")
			} else if deviceType == "humidity" {
				// humidity有1个非空值（60.0）
				assert.Equal(t, 1.0, totalCount, "humidity总数应该是1")
				assert.Equal(t, 1.0, nonNullCount, "humidity非空数应该是1")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("测试超时，未收到聚合结果")
	}
}

// TestIsNullInHaving 测试HAVING子句中真正的IS NULL功能
func TestIsNullInHaving(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	// 测试HAVING子句中的IS NULL：只返回平均值为NULL的设备类型
	sql := `SELECT deviceType, 
	               COUNT(*) as total_count,
	               AVG(value) as avg_value
	        FROM stream 
	        GROUP BY deviceType, TumblingWindow('2s')
	        HAVING avg_value IS NULL`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	resultChan := make(chan interface{}, 10)
	ssql.Stream().AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加测试数据：只给pressure设备类型添加null值，这样它的平均值会是null
	testData := []map[string]interface{}{
		{"deviceType": "temperature", "value": 25.0},
		{"deviceType": "temperature", "value": 27.0}, // temperature有值，平均值不为null
		{"deviceType": "humidity", "value": 60.0},    // humidity有值，平均值不为null
		{"deviceType": "pressure", "value": nil},     // pressure只有null值
		{"deviceType": "pressure", "value": nil},     // pressure再次null值，平均值会是null
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// 等待窗口触发
	time.Sleep(3 * time.Second)

	// 验证结果
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")

		// 应该只有pressure类型的结果（平均值为null）
		assert.Len(t, resultSlice, 1, "应该只有一个结果")

		if len(resultSlice) > 0 {
			item := resultSlice[0]
			assert.Equal(t, "pressure", item["deviceType"], "应该是pressure类型")

			// 验证avg_value确实为null
			avgValue := item["avg_value"]
			assert.Nil(t, avgValue, "pressure的平均值应该是null")

			// 验证total_count
			totalCount, ok := item["total_count"].(float64)
			assert.True(t, ok, "total_count应该是float64类型")
			assert.Equal(t, 2.0, totalCount, "pressure应该有2条记录")
		}

	case <-time.After(5 * time.Second):
		t.Fatal("测试超时，未收到聚合结果")
	}
}

// TestIsNullInHavingWithIsNotNull 测试HAVING子句中的IS NOT NULL功能
func TestIsNullInHavingWithIsNotNull(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	// 测试HAVING子句中的IS NOT NULL：只返回平均值不为NULL的设备类型
	sql := `SELECT deviceType, 
	               COUNT(*) as total_count,
	               AVG(value) as avg_value
	        FROM stream 
	        GROUP BY deviceType, TumblingWindow('2s')
	        HAVING avg_value IS NOT NULL`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	resultChan := make(chan interface{}, 10)
	ssql.Stream().AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []map[string]interface{}{
		{"deviceType": "temperature", "value": 25.0},
		{"deviceType": "temperature", "value": 27.0}, // temperature有值，平均值不为null
		{"deviceType": "humidity", "value": 60.0},    // humidity有值，平均值不为null
		{"deviceType": "pressure", "value": nil},     // pressure只有null值，平均值会是null
		{"deviceType": "pressure", "value": nil},
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// 等待窗口触发
	time.Sleep(3 * time.Second)

	// 验证结果
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")

		// 应该有temperature和humidity两种类型的结果（平均值不为null）
		assert.Len(t, resultSlice, 2, "应该有两个结果")

		foundTypes := make([]string, 0)
		for _, item := range resultSlice {
			deviceType, ok := item["deviceType"].(string)
			require.True(t, ok, "deviceType应该是string类型")

			// 验证avg_value不为null
			avgValue := item["avg_value"]
			assert.NotNil(t, avgValue, fmt.Sprintf("%s的平均值应该不为null", deviceType))

			foundTypes = append(foundTypes, deviceType)
		}

		// 验证包含temperature和humidity，不包含pressure
		assert.Contains(t, foundTypes, "temperature", "结果应该包含temperature")
		assert.Contains(t, foundTypes, "humidity", "结果应该包含humidity")
		assert.NotContains(t, foundTypes, "pressure", "结果不应该包含pressure")

	case <-time.After(5 * time.Second):
		t.Fatal("测试超时，未收到聚合结果")
	}
}

// TestIsNullWithOtherOperators 测试IS NULL与其他操作符的组合
func TestIsNullWithOtherOperators(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	// 测试复杂的WHERE条件
	sql := `SELECT deviceId, value, status, location 
	        FROM stream 
	        WHERE (value IS NOT NULL AND value > 20) OR 
	              (status IS NULL AND location LIKE 'warehouse%')`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	resultChan := make(chan interface{}, 10)
	ssql.Stream().AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []map[string]interface{}{
		{"deviceId": "sensor1", "value": 25.0, "status": "active", "location": "warehouse-A"},  // 满足第一个条件
		{"deviceId": "sensor2", "value": 15.0, "status": "active", "location": "warehouse-B"},  // 不满足条件
		{"deviceId": "sensor3", "value": nil, "status": nil, "location": "warehouse-C"},        // 满足第二个条件
		{"deviceId": "sensor4", "value": nil, "status": "inactive", "location": "warehouse-D"}, // 不满足条件
		{"deviceId": "sensor5", "value": 30.0, "status": nil, "location": "office-A"},          // 满足第一个条件
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// 使用超时方式安全收集结果
	var results []map[string]interface{}
	timeout := time.After(500 * time.Millisecond)

collecting:
	for {
		select {
		case result := <-resultChan:
			if resultSlice, ok := result.([]map[string]interface{}); ok {
				results = append(results, resultSlice...)
			}
		case <-timeout:
			break collecting
		}
	}

	// 验证结果：应该有sensor1, sensor3, sensor5
	assert.Len(t, results, 3, "应该有3个结果")

	expectedDeviceIds := []string{"sensor1", "sensor3", "sensor5"}
	actualDeviceIds := make([]string, len(results))
	for i, result := range results {
		actualDeviceIds[i] = result["deviceId"].(string)
	}

	for _, expectedId := range expectedDeviceIds {
		assert.Contains(t, actualDeviceIds, expectedId, "结果应该包含设备ID %s", expectedId)
	}
}

// TestCaseWhenWithIsNull 测试CASE WHEN表达式中使用IS NULL和IS NOT NULL
func TestCaseWhenWithIsNull(t *testing.T) {
	testCases := []struct {
		name     string
		sql      string
		testData []map[string]interface{}
		expected []map[string]interface{}
	}{
		{
			name: "CASE WHEN IS NULL基本测试",
			sql: `SELECT deviceId, 
			             CASE WHEN status IS NULL THEN 0 
			                  WHEN status IS NOT NULL THEN 1 
			                  ELSE 2 END as status_flag
			      FROM stream`,
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "status": "active"},
				{"deviceId": "sensor2", "status": nil},
				{"deviceId": "sensor3", "status": "inactive"},
				{"deviceId": "sensor4"}, // 没有status字段
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor1", "status_flag": 1.0},
				{"deviceId": "sensor2", "status_flag": 0.0},
				{"deviceId": "sensor3", "status_flag": 1.0},
				{"deviceId": "sensor4", "status_flag": 0.0},
			},
		},
		{
			name: "CASE WHEN IS NOT NULL复杂条件测试",
			sql: `SELECT deviceId, 
			             CASE WHEN temperature IS NOT NULL AND temperature > 25 THEN 2 
			                  WHEN temperature IS NOT NULL AND temperature <= 25 THEN 1 
			                  WHEN temperature IS NULL THEN 0 
			                  ELSE 3 END as temp_level
			      FROM stream`,
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "temperature": 30.0},
				{"deviceId": "sensor2", "temperature": 20.0},
				{"deviceId": "sensor3", "temperature": nil},
				{"deviceId": "sensor4"}, // 没有temperature字段
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor1", "temp_level": 2.0},
				{"deviceId": "sensor2", "temp_level": 1.0},
				{"deviceId": "sensor3", "temp_level": 0.0},
				{"deviceId": "sensor4", "temp_level": 0.0},
			},
		},
		{
			name: "多个CASE WHEN IS NULL条件测试",
			sql: `SELECT deviceId, 
			             CASE WHEN status IS NULL AND temperature IS NULL THEN 0 
			                  WHEN status IS NULL AND temperature IS NOT NULL THEN 1 
			                  WHEN status IS NOT NULL AND temperature IS NULL THEN 2 
			                  WHEN status IS NOT NULL AND temperature IS NOT NULL THEN 3 
			                  ELSE 4 END as combined_flag
			      FROM stream`,
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "status": "active", "temperature": 25.0},
				{"deviceId": "sensor2", "status": "active", "temperature": nil},
				{"deviceId": "sensor3", "status": nil, "temperature": 30.0},
				{"deviceId": "sensor4", "status": nil, "temperature": nil},
				{"deviceId": "sensor5"}, // 两个字段都不存在
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor1", "combined_flag": 3.0},
				{"deviceId": "sensor2", "combined_flag": 2.0},
				{"deviceId": "sensor3", "combined_flag": 1.0},
				{"deviceId": "sensor4", "combined_flag": 0.0},
				{"deviceId": "sensor5", "combined_flag": 0.0},
			},
		},
		{
			name: "CASE WHEN IS NULL与聚合函数结合测试",
			sql: `SELECT deviceType,
			             COUNT(*) as total_count,
			             SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_count,
			             SUM(CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END) as non_null_count
			      FROM stream 
			      GROUP BY deviceType, TumblingWindow('2s')`,
			testData: []map[string]interface{}{
				{"deviceType": "temperature", "value": 25.0},
				{"deviceType": "temperature", "value": nil},
				{"deviceType": "temperature", "value": 27.0},
				{"deviceType": "humidity", "value": 60.0},
				{"deviceType": "humidity", "value": nil},
				{"deviceType": "humidity", "value": nil},
			},
			expected: []map[string]interface{}{
				{
					"deviceType":     "temperature",
					"total_count":    3.0,
					"null_count":     1.0,
					"non_null_count": 2.0,
				},
				{
					"deviceType":     "humidity",
					"total_count":    3.0,
					"null_count":     2.0,
					"non_null_count": 1.0,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建StreamSQL实例
			ssql := New()
			defer ssql.Stop()

			// 执行SQL
			err := ssql.Execute(tc.sql)
			require.NoError(t, err)

			// 收集结果
			var results []map[string]interface{}
			resultChan := make(chan interface{}, 10)

			ssql.Stream().AddSink(func(result interface{}) {
				resultChan <- result
			})

			// 添加测试数据
			for _, data := range tc.testData {
				ssql.Stream().Emit(data)
			}

			// 使用超时方式安全收集结果
			var timeout time.Duration
			if tc.name == "CASE WHEN IS NULL与聚合函数结合测试" {
				timeout = 4 * time.Second // 聚合查询需要更长时间
			} else {
				timeout = 500 * time.Millisecond
			}

			timeoutChan := time.After(timeout)

		collecting:
			for {
				select {
				case result := <-resultChan:
					if resultSlice, ok := result.([]map[string]interface{}); ok {
						results = append(results, resultSlice...)
					}
				case <-timeoutChan:
					break collecting
				}
			}

			// 验证结果数量
			assert.Len(t, results, len(tc.expected), "结果数量应该匹配")

			// 对于聚合查询，验证逻辑略有不同
			if tc.name == "CASE WHEN IS NULL与聚合函数结合测试" {
				// 验证每个deviceType的结果
				for _, expectedResult := range tc.expected {
					expectedDeviceType := expectedResult["deviceType"].(string)

					// 在结果中找到对应的deviceType
					var actualResult map[string]interface{}
					for _, result := range results {
						if result["deviceType"].(string) == expectedDeviceType {
							actualResult = result
							break
						}
					}

					require.NotNil(t, actualResult, "应该找到设备类型 %s 的结果", expectedDeviceType)

					// 验证各个统计值
					assert.Equal(t, expectedResult["total_count"], actualResult["total_count"],
						"设备类型 %s 的total_count应该匹配", expectedDeviceType)
					assert.Equal(t, expectedResult["null_count"], actualResult["null_count"],
						"设备类型 %s 的null_count应该匹配", expectedDeviceType)
					assert.Equal(t, expectedResult["non_null_count"], actualResult["non_null_count"],
						"设备类型 %s 的non_null_count应该匹配", expectedDeviceType)
				}
			} else {
				// 验证普通查询的结果
				expectedDeviceIds := make([]string, len(tc.expected))
				for i, exp := range tc.expected {
					expectedDeviceIds[i] = exp["deviceId"].(string)
				}

				actualDeviceIds := make([]string, len(results))
				for i, result := range results {
					actualDeviceIds[i] = result["deviceId"].(string)
				}

				// 验证每个期望的设备ID都在结果中
				for _, expectedId := range expectedDeviceIds {
					assert.Contains(t, actualDeviceIds, expectedId, "结果应该包含设备ID %s", expectedId)
				}

				// 验证每个结果的字段值
				for _, result := range results {
					deviceId := result["deviceId"].(string)
					// 找到对应的期望结果
					var expectedResult map[string]interface{}
					for _, exp := range tc.expected {
						if exp["deviceId"].(string) == deviceId {
							expectedResult = exp
							break
						}
					}

					if expectedResult != nil {
						for key, expectedValue := range expectedResult {
							if key != "deviceId" { // deviceId已经验证过了
								actualValue := result[key]
								assert.Equal(t, expectedValue, actualValue,
									"设备 %s 的字段 %s 值应该匹配: 期望 %v, 实际 %v", deviceId, key, expectedValue, actualValue)
							}
						}
					}
				}
			}
		})
	}
}

// TestNullComparisons 测试 = nil、!= nil、= null、!= null 等语法
func TestNullComparisons(t *testing.T) {
	testCases := []struct {
		name     string
		sql      string
		testData []map[string]interface{}
		expected []map[string]interface{}
	}{
		{
			name: "fieldName = nil 测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value = nil",
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor4", "value": nil},
			},
		},
		{
			name: "fieldName != nil 测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value != nil",
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor3", "value": 30.0},
			},
		},
		{
			name: "fieldName = null 测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value = null",
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor4", "value": nil},
			},
		},
		{
			name: "fieldName != null 测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value != null",
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor3", "value": 30.0},
			},
		},
		{
			name: "嵌套字段 = nil 测试",
			sql:  "SELECT deviceId, device.location FROM stream WHERE device.location = nil",
			testData: []map[string]interface{}{
				{
					"deviceId": "sensor1",
					"device": map[string]interface{}{
						"location": "warehouse-A",
					},
				},
				{
					"deviceId": "sensor2",
					"device": map[string]interface{}{
						"location": nil,
					},
				},
				{
					"deviceId": "sensor3",
					"device":   map[string]interface{}{},
				},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor2", "device.location": nil},
				{"deviceId": "sensor3", "device.location": nil}, // 字段不存在也被认为是null
			},
		},
		{
			name: "嵌套字段 != nil 测试",
			sql:  "SELECT deviceId, device.location FROM stream WHERE device.location != nil",
			testData: []map[string]interface{}{
				{
					"deviceId": "sensor1",
					"device": map[string]interface{}{
						"location": "warehouse-A",
					},
				},
				{
					"deviceId": "sensor2",
					"device": map[string]interface{}{
						"location": nil,
					},
				},
				{
					"deviceId": "sensor3",
					"device":   map[string]interface{}{},
				},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor1", "device.location": "warehouse-A"},
			},
		},
		{
			name: "组合条件 - != nil AND 其他条件",
			sql:  "SELECT deviceId, value, status FROM stream WHERE value != nil AND value > 20",
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor2", "value": nil, "status": "active"},
				{"deviceId": "sensor3", "value": 15.0, "status": "inactive"},
				{"deviceId": "sensor4", "value": 30.0, "status": "active"},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor4", "value": 30.0, "status": "active"},
			},
		},
		{
			name: "组合条件 - = nil OR 其他条件",
			sql:  "SELECT deviceId, value, status FROM stream WHERE value = nil OR status = 'error'",
			testData: []map[string]interface{}{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor2", "value": nil, "status": "active"},
				{"deviceId": "sensor3", "value": 30.0, "status": "error"},
				{"deviceId": "sensor4", "value": nil, "status": "inactive"},
			},
			expected: []map[string]interface{}{
				{"deviceId": "sensor2", "value": nil, "status": "active"},
				{"deviceId": "sensor3", "value": 30.0, "status": "error"},
				{"deviceId": "sensor4", "value": nil, "status": "inactive"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建StreamSQL实例
			ssql := New()
			defer ssql.Stop()

			// 执行SQL
			err := ssql.Execute(tc.sql)
			require.NoError(t, err)

			// 收集结果
			var results []map[string]interface{}
			resultChan := make(chan interface{}, 10)

			ssql.Stream().AddSink(func(result interface{}) {
				resultChan <- result
			})

			// 添加测试数据
			for _, data := range tc.testData {
				ssql.Stream().Emit(data)
			}

			// 使用超时方式安全收集结果
			timeout := time.After(500 * time.Millisecond)

		collecting:
			for {
				select {
				case result := <-resultChan:
					if resultSlice, ok := result.([]map[string]interface{}); ok {
						results = append(results, resultSlice...)
					}
				case <-timeout:
					break collecting
				}
			}

			// 验证结果数量
			assert.Len(t, results, len(tc.expected), "结果数量应该匹配")

			// 验证结果内容（不依赖顺序）
			expectedDeviceIds := make([]string, len(tc.expected))
			for i, exp := range tc.expected {
				expectedDeviceIds[i] = exp["deviceId"].(string)
			}

			actualDeviceIds := make([]string, len(results))
			for i, result := range results {
				actualDeviceIds[i] = result["deviceId"].(string)
			}

			// 验证每个期望的设备ID都在结果中
			for _, expectedId := range expectedDeviceIds {
				assert.Contains(t, actualDeviceIds, expectedId, "结果应该包含设备ID %s", expectedId)
			}

			// 验证每个结果的字段值
			for _, result := range results {
				deviceId := result["deviceId"].(string)
				// 找到对应的期望结果
				var expectedResult map[string]interface{}
				for _, exp := range tc.expected {
					if exp["deviceId"].(string) == deviceId {
						expectedResult = exp
						break
					}
				}

				if expectedResult != nil {
					for key, expectedValue := range expectedResult {
						actualValue := result[key]
						assert.Equal(t, expectedValue, actualValue,
							"设备 %s 的字段 %s 值应该匹配: 期望 %v, 实际 %v", deviceId, key, expectedValue, actualValue)
					}
				}
			}
		})
	}
}

// TestNullComparisonInAggregation 测试聚合查询中的 = nil 和 != nil
func TestNullComparisonInAggregation(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	// 聚合查询：统计非空值的数量
	sql := `SELECT deviceType, 
	               COUNT(*) as total_count,
	               COUNT(value) as non_null_count
	        FROM stream 
	        WHERE value != nil
	        GROUP BY deviceType, TumblingWindow('2s')`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	// 收集结果
	resultChan := make(chan interface{}, 10)
	ssql.Stream().AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []map[string]interface{}{
		{"deviceType": "temperature", "value": 25.5},
		{"deviceType": "temperature", "value": nil},
		{"deviceType": "temperature", "value": 27.0},
		{"deviceType": "humidity", "value": 60.0},
		{"deviceType": "humidity", "value": nil},
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// 等待窗口触发
	time.Sleep(3 * time.Second)

	// 验证结果
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")

		// 应该有temperature和humidity两种类型的结果
		assert.GreaterOrEqual(t, len(resultSlice), 1, "应该至少有一个聚合结果")

		for _, item := range resultSlice {
			deviceType := item["deviceType"]
			totalCount, _ := item["total_count"].(float64)
			nonNullCount, _ := item["non_null_count"].(float64)

			if deviceType == "temperature" {
				// temperature有2个非空值（25.5, 27.0）
				assert.Equal(t, 2.0, totalCount, "temperature总数应该是2")
				assert.Equal(t, 2.0, nonNullCount, "temperature非空数应该是2")
			} else if deviceType == "humidity" {
				// humidity有1个非空值（60.0）
				assert.Equal(t, 1.0, totalCount, "humidity总数应该是1")
				assert.Equal(t, 1.0, nonNullCount, "humidity非空数应该是1")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("测试超时，未收到聚合结果")
	}
}

// TestMixedNullComparisons 测试混合使用 IS NULL、= nil、= null、!= null 等语法
func TestMixedNullComparisons(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	// 测试混合null比较语法
	sql := `SELECT deviceId, value, status, priority 
	        FROM stream 
	        WHERE (value IS NOT NULL AND value > 20) OR 
	              (status = nil AND priority != null)`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	resultChan := make(chan interface{}, 10)
	ssql.Stream().AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []map[string]interface{}{
		{"deviceId": "sensor1", "value": 25.0, "status": "active", "priority": "high"}, // 满足第一个条件
		{"deviceId": "sensor2", "value": 15.0, "status": "active", "priority": "low"},  // 不满足条件
		{"deviceId": "sensor3", "value": nil, "status": nil, "priority": "medium"},     // 满足第二个条件
		{"deviceId": "sensor4", "value": nil, "status": nil, "priority": nil},          // 不满足条件
		{"deviceId": "sensor5", "value": 30.0, "status": "inactive", "priority": nil},  // 满足第一个条件
		{"deviceId": "sensor6", "value": 10.0, "status": nil, "priority": "urgent"},    // 满足第二个条件
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// 使用超时方式安全收集结果
	var results []map[string]interface{}
	timeout := time.After(500 * time.Millisecond)

collecting:
	for {
		select {
		case result := <-resultChan:
			if resultSlice, ok := result.([]map[string]interface{}); ok {
				results = append(results, resultSlice...)
			}
		case <-timeout:
			break collecting
		}
	}

	// 验证结果：应该有sensor1, sensor3, sensor5, sensor6
	assert.Len(t, results, 4, "应该有4个结果")

	expectedDeviceIds := []string{"sensor1", "sensor3", "sensor5", "sensor6"}
	actualDeviceIds := make([]string, len(results))
	for i, result := range results {
		actualDeviceIds[i] = result["deviceId"].(string)
	}

	for _, expectedId := range expectedDeviceIds {
		assert.Contains(t, actualDeviceIds, expectedId, "结果应该包含设备ID %s", expectedId)
	}
}
