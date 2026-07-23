package e2e

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsNullOperatorInSQL tests the syntax functions of IS NULL and IS NOT NULL
func TestIsNullOperatorInSQL(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		sql      string
		testData []map[string]any
		expected []map[string]any
	}{
		{
			name: "IS NULL测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value IS NULL",
			testData: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]any{
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor4", "value": nil},
			},
		},
		{
			name: "IS NOT NULL测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value IS NOT NULL",
			testData: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor3", "value": 30.0},
			},
		},
		{
			name: "嵌套字段IS NULL测试",
			sql:  "SELECT deviceId, device.location FROM stream WHERE device.location IS NULL",
			testData: []map[string]any{
				{
					"deviceId": "sensor1",
					"device": map[string]any{
						"location": "warehouse-A",
					},
				},
				{
					"deviceId": "sensor2",
					"device": map[string]any{
						"location": nil,
					},
				},
				{
					"deviceId": "sensor3",
					"device":   map[string]any{},
				},
			},
			expected: []map[string]any{
				{"deviceId": "sensor2", "device.location": nil},
				{"deviceId": "sensor3", "device.location": nil}, // Fields that do not exist are also considered null
			},
		},
		{
			name: "组合条件 - IS NULL AND其他条件",
			sql:  "SELECT deviceId, value, status FROM stream WHERE value IS NULL AND status = 'active'",
			testData: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor2", "value": nil, "status": "active"},
				{"deviceId": "sensor3", "value": nil, "status": "inactive"},
				{"deviceId": "sensor4", "value": 30.0, "status": "active"},
			},
			expected: []map[string]any{
				{"deviceId": "sensor2", "value": nil, "status": "active"},
			},
		},
		{
			name: "组合条件 - IS NOT NULL OR其他条件",
			sql:  "SELECT deviceId, value, status FROM stream WHERE value IS NOT NULL OR status = 'error'",
			testData: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor2", "value": nil, "status": "active"},
				{"deviceId": "sensor3", "value": nil, "status": "error"},
				{"deviceId": "sensor4", "value": 30.0, "status": "inactive"},
			},
			expected: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor3", "value": nil, "status": "error"},
				{"deviceId": "sensor4", "value": 30.0, "status": "inactive"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a StreamSQL instance
			ssql := streamsql.New()
			defer ssql.Stop()

			// Execute SQL
			err := ssql.Execute(tc.sql)
			require.NoError(t, err)

			// Collect the results
			var results []map[string]any
			resultChan := make(chan any, 10)
			resultsMutex := sync.Mutex{}

			ssql.Stream().AddSink(func(result []map[string]any) {
				resultChan <- result
			})

			// Add test data
			for _, data := range tc.testData {
				ssql.Stream().Emit(data)
			}

			// Use shorter timeouts to avoid long waits in CI environments
			timeout := time.After(500 * time.Millisecond)

		collecting:
			for {
				select {
				case result := <-resultChan:
					resultsMutex.Lock()
					if resultSlice, ok := result.([]map[string]any); ok {
						results = append(results, resultSlice...)
					}
					resultsMutex.Unlock()
				case <-timeout:
					break collecting
				}
			}

			// Verification of the number of results
			assert.Len(t, results, len(tc.expected), "结果数量应该匹配")

			// Verification Result Content (Independent of Order)
			expectedDeviceIds := make([]string, len(tc.expected))
			for i, exp := range tc.expected {
				expectedDeviceIds[i] = exp["deviceId"].(string)
			}

			resultsMutex.Lock()
			actualDeviceIds := make([]string, len(results))
			for i, result := range results {
				actualDeviceIds[i] = result["deviceId"].(string)
			}
			resultsMutex.Unlock()

			// Verify the ID of each desired device in the results
			for _, expectedId := range expectedDeviceIds {
				assert.Contains(t, actualDeviceIds, expectedId, "结果应该包含设备ID %s", expectedId)
			}

			// Verify the field values for each result
			resultsMutex.Lock()
			for _, result := range results {
				deviceId := result["deviceId"].(string)
				// Find the corresponding desired outcome
				var expectedResult map[string]any
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
			resultsMutex.Unlock()
		})
	}
}

// TestIsNullInAggregation: Tests the IS NULL in aggregated queries
func TestIsNullInAggregation(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Aggregate query: Counts the number of non-null values
	sql := `SELECT deviceType, 
	               COUNT(*) as total_count,
	               COUNT(value) as non_null_count
	        FROM stream 
	        WHERE value IS NOT NULL
	        GROUP BY deviceType, TumblingWindow('2s')`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	// Collect the results
	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := []map[string]any{
		{"deviceType": "temperature", "value": 25.5},
		{"deviceType": "temperature", "value": nil},
		{"deviceType": "temperature", "value": 27.0},
		{"deviceType": "humidity", "value": 60.0},
		{"deviceType": "humidity", "value": nil},
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// Wait for the window to trigger
	time.Sleep(3 * time.Second)

	// Verify the results
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		require.True(t, ok, "结果应该是[]map[string]any类型")

		// There should be two types of results: temperature and humidity
		assert.GreaterOrEqual(t, len(resultSlice), 1, "应该至少有一个聚合结果")

		for _, item := range resultSlice {
			deviceType := item["deviceType"]
			totalCount, _ := item["total_count"].(float64)
			nonNullCount, _ := item["non_null_count"].(float64)

			if deviceType == "temperature" {
				// Temperature has 2 non-null values (25.5, 27.0)
				assert.Equal(t, 2.0, totalCount, "temperature总数应该是2")
				assert.Equal(t, 2.0, nonNullCount, "temperature非空数应该是2")
			} else if deviceType == "humidity" {
				// Humidity has 1 non-null value (60.0)
				assert.Equal(t, 1.0, totalCount, "humidity总数应该是1")
				assert.Equal(t, 1.0, nonNullCount, "humidity非空数应该是1")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("The test timed out, and no aggregated results were received")
	}
}

// TestIsNullInHaving tests the true IS NULL functionality in the HAVING clause
func TestIsNullInHaving(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Test IS NULL in the HAVING clause: only returns the device type with an average NULL
	sql := `SELECT deviceType, 
	               COUNT(*) as total_count,
	               AVG(value) as avg_value
	        FROM stream 
	        GROUP BY deviceType, TumblingWindow('2s')
	        HAVING avg_value IS NULL`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data: only add null values to the pressure device type, so its average value will be null
	testData := []map[string]any{
		{"deviceType": "temperature", "value": 25.0},
		{"deviceType": "temperature", "value": 27.0}, // temperature is valued, and the average value is not null
		{"deviceType": "humidity", "value": 60.0},    // humidity has a value, and the average value is not null
		{"deviceType": "pressure", "value": nil},     // Pressure only has a null value
		{"deviceType": "pressure", "value": nil},     // pressure is null again, and the average value will be null
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// Wait for the window to trigger
	time.Sleep(3 * time.Second)

	// Verify the results
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		require.True(t, ok, "结果应该是[]map[string]any类型")

		// There should only be results of pressure type (average value null)
		assert.Len(t, resultSlice, 1, "应该只有一个结果")

		if len(resultSlice) > 0 {
			item := resultSlice[0]
			assert.Equal(t, "pressure", item["deviceType"], "应该是pressure类型")

			// Verify that avg_value is indeed null
			avgValue := item["avg_value"]
			assert.Nil(t, avgValue, "pressure的平均值应该是null")

			// Verification total_count
			totalCount, ok := item["total_count"].(float64)
			assert.True(t, ok, "total_count应该是float64类型")
			assert.Equal(t, 2.0, totalCount, "pressure应该有2条记录")
		}

	case <-time.After(5 * time.Second):
		t.Fatal("The test timed out, and no aggregated results were received")
	}
}

// TestIsNullInHavingWithIsNotNull tests the IS NOT NULL function in the HAVING clause
func TestIsNullInHavingWithIsNotNull(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Test IS NOT NULL in the HAVING clause: only returns the device type with an average value that is not NULL
	sql := `SELECT deviceType, 
	               COUNT(*) as total_count,
	               AVG(value) as avg_value
	        FROM stream 
	        GROUP BY deviceType, TumblingWindow('2s')
	        HAVING avg_value IS NOT NULL`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := []map[string]any{
		{"deviceType": "temperature", "value": 25.0},
		{"deviceType": "temperature", "value": 27.0}, // temperature is valued, and the average value is not null
		{"deviceType": "humidity", "value": 60.0},    // humidity has a value, and the average value is not null
		{"deviceType": "pressure", "value": nil},     // Pressure is only a null value, and the average value will be null
		{"deviceType": "pressure", "value": nil},
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// Wait for the window to trigger
	time.Sleep(3 * time.Second)

	// Verify the results
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		require.True(t, ok, "结果应该是[]map[string]any类型")

		// There should be two types of results: temperature and humidity (the average is not null).
		assert.Len(t, resultSlice, 2, "应该有两个结果")

		foundTypes := make([]string, 0)
		for _, item := range resultSlice {
			deviceType, ok := item["deviceType"].(string)
			require.True(t, ok, "deviceType应该是string类型")

			// Verify that avg_value is not null
			avgValue := item["avg_value"]
			assert.NotNil(t, avgValue, fmt.Sprintf("%s的平均值应该不为null", deviceType))

			foundTypes = append(foundTypes, deviceType)
		}

		// Verification includes temperature and humidity, but excludes pressure
		assert.Contains(t, foundTypes, "temperature", "结果应该包含temperature")
		assert.Contains(t, foundTypes, "humidity", "结果应该包含humidity")
		assert.NotContains(t, foundTypes, "pressure", "结果不应该包含pressure")

	case <-time.After(5 * time.Second):
		t.Fatal("The test timed out, and no aggregated results were received")
	}
}

// TestIsNullWithOtherOperators tests the combination of IS NULL and other operators
func TestIsNullWithOtherOperators(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Test complex WHERE conditions
	sql := `SELECT deviceId, value, status, location 
	        FROM stream 
	        WHERE (value IS NOT NULL AND value > 20) OR 
	              (status IS NULL AND location LIKE 'warehouse%')`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := []map[string]any{
		{"deviceId": "sensor1", "value": 25.0, "status": "active", "location": "warehouse-A"},  // The first condition is met
		{"deviceId": "sensor2", "value": 15.0, "status": "active", "location": "warehouse-B"},  // The conditions are not met
		{"deviceId": "sensor3", "value": nil, "status": nil, "location": "warehouse-C"},        // The second condition must be met
		{"deviceId": "sensor4", "value": nil, "status": "inactive", "location": "warehouse-D"}, // The conditions are not met
		{"deviceId": "sensor5", "value": 30.0, "status": nil, "location": "office-A"},          // The first condition is met
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// Safely collect results using timeout methods
	var results []map[string]any
	timeout := time.After(2 * time.Second)

collecting:
	for {
		select {
		case result := <-resultChan:
			if resultSlice, ok := result.([]map[string]any); ok {
				results = append(results, resultSlice...)
			}
		case <-timeout:
			break collecting
		}
	}

	// Verification result: There should be sensor1, sensor3, sensor5
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

// TestCaseWhenWithIsNull tests IS NULL and IS NOT NULL in a CASE WHEN expression
func TestCaseWhenWithIsNull(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		sql      string
		testData []map[string]any
		expected []map[string]any
	}{
		{
			name: "CASE WHEN IS NULL基本测试",
			sql: `SELECT deviceId, 
			             CASE WHEN status IS NULL THEN 0 
			                  WHEN status IS NOT NULL THEN 1 
			                  ELSE 2 END as status_flag
			      FROM stream`,
			testData: []map[string]any{
				{"deviceId": "sensor1", "status": "active"},
				{"deviceId": "sensor2", "status": nil},
				{"deviceId": "sensor3", "status": "inactive"},
				{"deviceId": "sensor4"}, // There is no status field
			},
			expected: []map[string]any{
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
			testData: []map[string]any{
				{"deviceId": "sensor1", "temperature": 30.0},
				{"deviceId": "sensor2", "temperature": 20.0},
				{"deviceId": "sensor3", "temperature": nil},
				{"deviceId": "sensor4"}, // There is no temperature field
			},
			expected: []map[string]any{
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
			testData: []map[string]any{
				{"deviceId": "sensor1", "status": "active", "temperature": 25.0},
				{"deviceId": "sensor2", "status": "active", "temperature": nil},
				{"deviceId": "sensor3", "status": nil, "temperature": 30.0},
				{"deviceId": "sensor4", "status": nil, "temperature": nil},
				{"deviceId": "sensor5"}, // Neither field exists
			},
			expected: []map[string]any{
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
			testData: []map[string]any{
				{"deviceType": "temperature", "value": 25.0},
				{"deviceType": "temperature", "value": nil},
				{"deviceType": "temperature", "value": 27.0},
				{"deviceType": "humidity", "value": 60.0},
				{"deviceType": "humidity", "value": nil},
				{"deviceType": "humidity", "value": nil},
			},
			expected: []map[string]any{
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
			// Create a StreamSQL instance
			ssql := streamsql.New()
			defer ssql.Stop()

			// Execute SQL
			err := ssql.Execute(tc.sql)
			require.NoError(t, err)

			// Collect the results
			var results []map[string]any
			resultChan := make(chan any, 10)

			ssql.Stream().AddSink(func(result []map[string]any) {
				resultChan <- result
			})

			// Add test data
			for _, data := range tc.testData {
				ssql.Stream().Emit(data)
			}

			// Safely collect results using timeout methods
			var timeout time.Duration
			if tc.name == "CASE WHEN IS NULL与聚合函数结合测试" {
				timeout = 4 * time.Second // Aggregate queries take longer
			} else {
				timeout = 500 * time.Millisecond
			}

			timeoutChan := time.After(timeout)

		collecting:
			for {
				select {
				case result := <-resultChan:
					if resultSlice, ok := result.([]map[string]any); ok {
						results = append(results, resultSlice...)
					}
				case <-timeoutChan:
					break collecting
				}
			}

			// Verification of the number of results
			assert.Len(t, results, len(tc.expected), "结果数量应该匹配")

			// For aggregated queries, the validation logic is slightly different
			if tc.name == "CASE WHEN IS NULL与聚合函数结合测试" {
				// Verify the results of each deviceType
				for _, expectedResult := range tc.expected {
					expectedDeviceType := expectedResult["deviceType"].(string)

					// Find the corresponding deviceType in the results
					var actualResult map[string]any
					for _, result := range results {
						if result["deviceType"].(string) == expectedDeviceType {
							actualResult = result
							break
						}
					}

					require.NotNil(t, actualResult, "应该找到设备类型 %s 的结果", expectedDeviceType)

					// Verify each statistic
					assert.Equal(t, expectedResult["total_count"], actualResult["total_count"],
						"设备类型 %s 的total_count应该匹配", expectedDeviceType)
					assert.Equal(t, expectedResult["null_count"], actualResult["null_count"],
						"设备类型 %s 的null_count应该匹配", expectedDeviceType)
					assert.Equal(t, expectedResult["non_null_count"], actualResult["non_null_count"],
						"设备类型 %s 的non_null_count应该匹配", expectedDeviceType)
				}
			} else {
				// Verify the results of ordinary queries
				expectedDeviceIds := make([]string, len(tc.expected))
				for i, exp := range tc.expected {
					expectedDeviceIds[i] = exp["deviceId"].(string)
				}

				actualDeviceIds := make([]string, len(results))
				for i, result := range results {
					actualDeviceIds[i] = result["deviceId"].(string)
				}

				// Verify the ID of each desired device in the results
				for _, expectedId := range expectedDeviceIds {
					assert.Contains(t, actualDeviceIds, expectedId, "结果应该包含设备ID %s", expectedId)
				}

				// Verify the field values for each result
				for _, result := range results {
					deviceId := result["deviceId"].(string)
					// Find the corresponding desired outcome
					var expectedResult map[string]any
					for _, exp := range tc.expected {
						if exp["deviceId"].(string) == deviceId {
							expectedResult = exp
							break
						}
					}

					if expectedResult != nil {
						for key, expectedValue := range expectedResult {
							if key != "deviceId" { // deviceId has already been verified
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

// TestNullComparisons Syntax such as test = nil,!= nil, = null,!= null, etc
func TestNullComparisons(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		sql      string
		testData []map[string]any
		expected []map[string]any
	}{
		{
			name: "fieldName = nil 测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value = nil",
			testData: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]any{
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor4", "value": nil},
			},
		},
		{
			name: "fieldName != nil 测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value != nil",
			testData: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor3", "value": 30.0},
			},
		},
		{
			name: "fieldName = null 测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value = null",
			testData: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]any{
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor4", "value": nil},
			},
		},
		{
			name: "fieldName != null 测试",
			sql:  "SELECT deviceId, value FROM stream WHERE value != null",
			testData: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor2", "value": nil},
				{"deviceId": "sensor3", "value": 30.0},
				{"deviceId": "sensor4", "value": nil},
			},
			expected: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5},
				{"deviceId": "sensor3", "value": 30.0},
			},
		},
		{
			name: "嵌套字段 = nil 测试",
			sql:  "SELECT deviceId, device.location FROM stream WHERE device.location = nil",
			testData: []map[string]any{
				{
					"deviceId": "sensor1",
					"device": map[string]any{
						"location": "warehouse-A",
					},
				},
				{
					"deviceId": "sensor2",
					"device": map[string]any{
						"location": nil,
					},
				},
				{
					"deviceId": "sensor3",
					"device":   map[string]any{},
				},
			},
			expected: []map[string]any{
				{"deviceId": "sensor2", "device.location": nil},
				{"deviceId": "sensor3", "device.location": nil}, // Fields that do not exist are also considered null
			},
		},
		{
			name: "嵌套字段 != nil 测试",
			sql:  "SELECT deviceId, device.location FROM stream WHERE device.location != nil",
			testData: []map[string]any{
				{
					"deviceId": "sensor1",
					"device": map[string]any{
						"location": "warehouse-A",
					},
				},
				{
					"deviceId": "sensor2",
					"device": map[string]any{
						"location": nil,
					},
				},
				{
					"deviceId": "sensor3",
					"device":   map[string]any{},
				},
			},
			expected: []map[string]any{
				{"deviceId": "sensor1", "device.location": "warehouse-A"},
			},
		},
		{
			name: "组合条件 - != nil AND 其他条件",
			sql:  "SELECT deviceId, value, status FROM stream WHERE value != nil AND value > 20",
			testData: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor2", "value": nil, "status": "active"},
				{"deviceId": "sensor3", "value": 15.0, "status": "inactive"},
				{"deviceId": "sensor4", "value": 30.0, "status": "active"},
			},
			expected: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor4", "value": 30.0, "status": "active"},
			},
		},
		{
			name: "组合条件 - = nil OR 其他条件",
			sql:  "SELECT deviceId, value, status FROM stream WHERE value = nil OR status = 'error'",
			testData: []map[string]any{
				{"deviceId": "sensor1", "value": 25.5, "status": "active"},
				{"deviceId": "sensor2", "value": nil, "status": "active"},
				{"deviceId": "sensor3", "value": 30.0, "status": "error"},
				{"deviceId": "sensor4", "value": nil, "status": "inactive"},
			},
			expected: []map[string]any{
				{"deviceId": "sensor2", "value": nil, "status": "active"},
				{"deviceId": "sensor3", "value": 30.0, "status": "error"},
				{"deviceId": "sensor4", "value": nil, "status": "inactive"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a StreamSQL instance
			ssql := streamsql.New()
			defer ssql.Stop()

			// Execute SQL
			err := ssql.Execute(tc.sql)
			require.NoError(t, err)

			// Collect the results
			var results []map[string]any
			resultChan := make(chan any, 10)

			ssql.Stream().AddSink(func(result []map[string]any) {
				resultChan <- result
			})

			// Add test data
			for _, data := range tc.testData {
				ssql.Stream().Emit(data)
			}

			// Safely collect results using timeout methods
			timeout := time.After(500 * time.Millisecond)

		collecting:
			for {
				select {
				case result := <-resultChan:
					if resultSlice, ok := result.([]map[string]any); ok {
						results = append(results, resultSlice...)
					}
				case <-timeout:
					break collecting
				}
			}

			// Verification of the number of results
			assert.Len(t, results, len(tc.expected), "结果数量应该匹配")

			// Verification Result Content (Independent of Order)
			expectedDeviceIds := make([]string, len(tc.expected))
			for i, exp := range tc.expected {
				expectedDeviceIds[i] = exp["deviceId"].(string)
			}

			actualDeviceIds := make([]string, len(results))
			for i, result := range results {
				actualDeviceIds[i] = result["deviceId"].(string)
			}

			// Verify the ID of each desired device in the results
			for _, expectedId := range expectedDeviceIds {
				assert.Contains(t, actualDeviceIds, expectedId, "结果应该包含设备ID %s", expectedId)
			}

			// Verify the field values for each result
			for _, result := range results {
				deviceId := result["deviceId"].(string)
				// Find the corresponding desired outcome
				var expectedResult map[string]any
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

// TestNullComparisonInAggregation tests the aggregated queries for = nil and!= nil
func TestNullComparisonInAggregation(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Aggregate query: Counts the number of non-null values
	sql := `SELECT deviceType, 
	               COUNT(*) as total_count,
	               COUNT(value) as non_null_count
	        FROM stream 
	        WHERE value != nil
	        GROUP BY deviceType, TumblingWindow('2s')`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	// Collect the results
	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := []map[string]any{
		{"deviceType": "temperature", "value": 25.5},
		{"deviceType": "temperature", "value": nil},
		{"deviceType": "temperature", "value": 27.0},
		{"deviceType": "humidity", "value": 60.0},
		{"deviceType": "humidity", "value": nil},
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// Wait for the window to trigger
	time.Sleep(3 * time.Second)

	// Verify the results
	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		require.True(t, ok, "结果应该是[]map[string]any类型")

		// There should be two types of results: temperature and humidity
		assert.GreaterOrEqual(t, len(resultSlice), 1, "应该至少有一个聚合结果")

		for _, item := range resultSlice {
			deviceType := item["deviceType"]
			totalCount, _ := item["total_count"].(float64)
			nonNullCount, _ := item["non_null_count"].(float64)

			if deviceType == "temperature" {
				// Temperature has 2 non-null values (25.5, 27.0)
				assert.Equal(t, 2.0, totalCount, "temperature总数应该是2")
				assert.Equal(t, 2.0, nonNullCount, "temperature非空数应该是2")
			} else if deviceType == "humidity" {
				// Humidity has 1 non-null value (60.0)
				assert.Equal(t, 1.0, totalCount, "humidity总数应该是1")
				assert.Equal(t, 1.0, nonNullCount, "humidity非空数应该是1")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("The test timed out, and no aggregated results were received")
	}
}

// TestMixedNullComparisons tests using syntax such as IS NULL, = nil, = null,!= null, etc
func TestMixedNullComparisons(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Test mixed null comparative syntax
	sql := `SELECT deviceId, value, status, priority 
	        FROM stream 
	        WHERE (value IS NOT NULL AND value > 20) OR 
	              (status = nil AND priority != null)`

	err := ssql.Execute(sql)
	require.NoError(t, err)

	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := []map[string]any{
		{"deviceId": "sensor1", "value": 25.0, "status": "active", "priority": "high"}, // The first condition is met
		{"deviceId": "sensor2", "value": 15.0, "status": "active", "priority": "low"},  // The conditions are not met
		{"deviceId": "sensor3", "value": nil, "status": nil, "priority": "medium"},     // The second condition must be met
		{"deviceId": "sensor4", "value": nil, "status": nil, "priority": nil},          // The conditions are not met
		{"deviceId": "sensor5", "value": 30.0, "status": "inactive", "priority": nil},  // The first condition is met
		{"deviceId": "sensor6", "value": 10.0, "status": nil, "priority": "urgent"},    // The second condition must be met
	}

	for _, data := range testData {
		ssql.Stream().Emit(data)
	}

	// Safely collect results using timeout methods
	var results []map[string]any
	timeout := time.After(2 * time.Second)

collecting:
	for {
		select {
		case result := <-resultChan:
			if resultSlice, ok := result.([]map[string]any); ok {
				results = append(results, resultSlice...)
			}
		case <-timeout:
			break collecting
		}
	}

	// Verification result: There should be sensor1, sensor3, sensor5, sensor6
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
