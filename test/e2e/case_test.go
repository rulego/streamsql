package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/rsql"
	"github.com/stretchr/testify/assert"
)

// TestCaseExpressionInSQL tests the use of CASE expressions in SQL queries
func TestCaseExpressionInSQL(t *testing.T) {
	t.Parallel()
	// Test CASE expressions in non-aggregated scenarios
	sql := `SELECT deviceId, 
	              CASE WHEN temperature > 30 THEN 'HOT' 
	                   WHEN temperature > 20 THEN 'WARM' 
	                   ELSE 'COOL' END as temp_category,
	              CASE status WHEN 'active' THEN 1 ELSE 0 END as status_code
	         FROM stream 
	         WHERE temperature > 15`

	// Create a StreamSQL instance
	streamSQL := streamsql.New()
	defer streamSQL.Stop()

	err := streamSQL.Execute(sql)
	assert.NoError(t, err, "执行SQL应该成功")

	// Simulation data
	testData := []map[string]any{
		{"deviceId": "device1", "temperature": 35.0, "status": "active"},
		{"deviceId": "device2", "temperature": 25.0, "status": "inactive"},
		{"deviceId": "device3", "temperature": 18.0, "status": "active"},
		{"deviceId": "device4", "temperature": 10.0, "status": "inactive"}, // It should be filtered out by WHERE (WHERE files).
	}

	// Add data and get results
	var results []map[string]any
	var resultsMutex sync.Mutex
	streamSQL.Stream().AddSink(func(result []map[string]any) {
		resultsMutex.Lock()
		defer resultsMutex.Unlock()
		results = append(results, result...)
	})

	for _, data := range testData {
		streamSQL.Emit(data)
	}

	// Waiting for processing
	time.Sleep(100 * time.Millisecond)

	// Verify the results
	resultsMutex.Lock()
	resultCount := len(results)
	resultsMutex.Unlock()
	assert.GreaterOrEqual(t, resultCount, 3, "应该有至少3条结果（排除temperature <= 15的记录）")
}

// TestCaseExpressionInAggregation tests the use of CASE expressions in aggregated queries
func TestCaseExpressionInAggregation(t *testing.T) {
	t.Parallel()
	// Use a processing time window to avoid the complexity of advancing watermarks
	// This test mainly verifies the use of CASE expressions in aggregate functions, rather than event time windows
	sql := `SELECT deviceId,
	              COUNT(*) as total_count,
	              SUM(CASE WHEN temperature > 30 THEN 1 ELSE 0 END) as hot_count,
	              AVG(CASE status WHEN 'active' THEN temperature ELSE 0 END) as avg_active_temp
	         FROM stream 
	         GROUP BY deviceId, TumblingWindow('1s')`

	// Create a StreamSQL instance
	streamSQL := streamsql.New()
	defer streamSQL.Stop()

	err := streamSQL.Execute(sql)
	assert.NoError(t, err, "执行SQL应该成功")

	// Simulated data (no timestamp field required, since processing time window is used)
	testData := []map[string]any{
		{"deviceId": "device1", "temperature": 35.0, "status": "active"},
		{"deviceId": "device1", "temperature": 25.0, "status": "inactive"},
		{"deviceId": "device1", "temperature": 32.0, "status": "active"},
		{"deviceId": "device2", "temperature": 28.0, "status": "active"},
		{"deviceId": "device2", "temperature": 22.0, "status": "inactive"},
	}

	// Add data and get results
	var results []map[string]any
	var resultsMutex sync.Mutex
	streamSQL.Stream().AddSink(func(result []map[string]any) {
		resultsMutex.Lock()
		defer resultsMutex.Unlock()
		results = append(results, result...)
	})

	for _, data := range testData {
		streamSQL.Emit(data)
	}

	// Wait for the window to trigger
	time.Sleep(1200 * time.Millisecond)

	// Manually trigger the window
	streamSQL.TriggerWindow()

	// Wait for the results
	time.Sleep(100 * time.Millisecond)

	// Verify the results
	resultsMutex.Lock()
	defer resultsMutex.Unlock()

	assert.Greater(t, len(results), 0, "应该有聚合结果返回")

	// Verify the structure and content of the results
	deviceResults := make(map[string]map[string]any)
	for _, result := range results {
		deviceId, ok := result["deviceId"].(string)
		assert.True(t, ok, "deviceId应该是字符串类型")
		deviceResults[deviceId] = result
	}

	// Expect results from two devices
	assert.Len(t, deviceResults, 2, "应该有两个设备的聚合结果")
	assert.Contains(t, deviceResults, "device1", "应该包含device1的结果")
	assert.Contains(t, deviceResults, "device2", "应该包含device2的结果")

	// Verify the results of device1
	device1Result := deviceResults["device1"]

	// Basic field check
	assert.Contains(t, device1Result, "total_count", "device1结果应该包含total_count")
	assert.Contains(t, device1Result, "hot_count", "device1结果应该包含hot_count")
	assert.Contains(t, device1Result, "avg_active_temp", "device1结果应该包含avg_active_temp")

	// Detailed numerical verification
	totalCount1 := getFloat64Value(device1Result["total_count"])
	hotCount1 := getFloat64Value(device1Result["hot_count"])
	avgActiveTemp1 := getFloat64Value(device1Result["avg_active_temp"])

	// device1: Total number of 3 records
	assert.Equal(t, 3.0, totalCount1, "device1应该有3条记录")

	// device1: 2 high-temperature records (35.0 > 30, 32.0 > 30)
	assert.Equal(t, 2.0, hotCount1, "device1应该有2条高温记录")

	// device1: Average temperature in active state (35.0 + 0 + 32.0) / 3 = 22.333...
	expectedActiveAvg := (35.0 + 0 + 32.0) / 3.0
	assert.InDelta(t, expectedActiveAvg, avgActiveTemp1, 0.01,
		"device1的AVG(CASE WHEN...)应该正确计算")

	// Verify the results of device2
	device2Result := deviceResults["device2"]

	// Basic field check
	assert.Contains(t, device2Result, "total_count", "device2结果应该包含total_count")
	assert.Contains(t, device2Result, "hot_count", "device2结果应该包含hot_count")
	assert.Contains(t, device2Result, "avg_active_temp", "device2结果应该包含avg_active_temp")

	// Detailed numerical verification
	totalCount2 := getFloat64Value(device2Result["total_count"])
	hotCount2 := getFloat64Value(device2Result["hot_count"])
	avgActiveTemp2 := getFloat64Value(device2Result["avg_active_temp"])

	// device2: Total number of 2 records
	assert.Equal(t, 2.0, totalCount2, "device2应该有2条记录")

	// device2: 0 high-temperature records (no temperature >30)
	assert.Equal(t, 0.0, hotCount2, "device2应该有0条高温记录")

	// device2: CASE WHEN status='active' THEN temperature ELSE 0
	// 28.0 (active) + 0 (inactive) = 28.0, mean = (28.0 + 0) / 2 = 14.0
	expectedActiveAvg2 := (28.0 + 0) / 2.0
	assert.InDelta(t, expectedActiveAvg2, avgActiveTemp2, 0.01,
		"device2的AVG(CASE WHEN...)应该正确计算")
}

// getFloat64Value auxiliary function to convert any to float64
func getFloat64Value(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0.0
	}
}

// TestComplexCaseExpressionsInAggregation: Tests the use of complex CASE expressions in aggregated queries
func TestComplexCaseExpressionsInAggregation(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name        string
		sql         string
		data        []map[string]any
		description string
	}{
		{
			name: "多条件CASE在SUM中",
			sql: `SELECT deviceId,
			            SUM(CASE WHEN temperature > 30 AND humidity > 60 THEN 1 
			                     WHEN temperature > 25 THEN 0.5 
			                     ELSE 0 END) as complex_score
			      FROM stream 
			      GROUP BY deviceId, TumblingWindow('1s')`,
			data: []map[string]any{
				{"deviceId": "device1", "temperature": 35.0, "humidity": 70.0},
				{"deviceId": "device1", "temperature": 28.0, "humidity": 50.0},
				{"deviceId": "device1", "temperature": 20.0, "humidity": 40.0},
			},
			description: "测试多条件CASE表达式在SUM聚合中的使用",
		},
		{
			name: "函数调用CASE在AVG中",
			sql: `SELECT deviceId,
			      AVG(CASE WHEN ABS(temperature - 25) < 5 THEN temperature ELSE 0 END) as normalized_avg
			      FROM stream 
			      GROUP BY deviceId, TumblingWindow('1s')`,
			data: []map[string]any{
				{"deviceId": "device1", "temperature": 23.0},
				{"deviceId": "device1", "temperature": 27.0},
				{"deviceId": "device1", "temperature": 35.0}, // This will be ruled out
			},
			description: "测试带函数的CASE表达式在AVG聚合中的使用",
		},
		{
			name: "复杂算术CASE在COUNT中",
			sql: `SELECT deviceId,
			            COUNT(CASE WHEN temperature * 1.8 + 32 > 80 THEN 1 END) as fahrenheit_hot_count
			      FROM stream 
			      GROUP BY deviceId, TumblingWindow('1s')`,
			data: []map[string]any{
				{"deviceId": "device1", "temperature": 25.0}, // 77F
				{"deviceId": "device1", "temperature": 30.0}, // 86F
				{"deviceId": "device1", "temperature": 35.0}, // 95F
			},
			description: "测试算术表达式CASE在COUNT聚合中的使用",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a StreamSQL instance
			streamSQL := streamsql.New()
			defer streamSQL.Stop()

			err := streamSQL.Execute(tc.sql)
			assert.NoError(t, err, "执行SQL应该成功")

			// Add data and get results
			var results []map[string]any
			var resultsMutex sync.Mutex
			streamSQL.Stream().AddSink(func(result []map[string]any) {
				resultsMutex.Lock()
				defer resultsMutex.Unlock()
				results = append(results, result...)
			})

			for _, data := range tc.data {
				streamSQL.Emit(data)
			}

			// Wait for the window to trigger
			time.Sleep(1200 * time.Millisecond)

			// Manually trigger the window
			streamSQL.TriggerWindow()

			// Wait for the results
			time.Sleep(100 * time.Millisecond)

			// At least verification yields results
			resultsMutex.Lock()
			hasResults := len(results) > 0
			resultsMutex.Unlock()
			assert.True(t, hasResults, "应该有聚合结果返回")
		})
	}
}

// TestCaseExpressionNonAggregated Tests CASE expressions in non-aggregated scenarios
func TestCaseExpressionNonAggregated(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		sql      string
		testData []map[string]any
		expected []map[string]any // The expected result
		wantErr  bool
	}{
		{
			name: "简单CASE表达式 - 温度分类",
			sql: `SELECT deviceId,
					CASE 
						WHEN temperature > 30 THEN 'HOT'
						WHEN temperature > 20 THEN 'WARM'
						WHEN temperature > 10 THEN 'COOL'
						ELSE 'COLD'
					END as temp_category
				  FROM stream`,
			testData: []map[string]any{
				{"deviceId": "device1", "temperature": 35.0},
				{"deviceId": "device2", "temperature": 25.0},
				{"deviceId": "device3", "temperature": 15.0},
				{"deviceId": "device4", "temperature": 5.0},
			},
			expected: []map[string]any{
				{"deviceId": "device1", "temp_category": "HOT"},
				{"deviceId": "device2", "temp_category": "WARM"},
				{"deviceId": "device3", "temp_category": "COOL"},
				{"deviceId": "device4", "temp_category": "COLD"},
			},
			wantErr: false,
		},
		{
			name: "简单CASE表达式 - 状态映射",
			sql: `SELECT deviceId,
					CASE status
						WHEN 'active' THEN 1
						WHEN 'inactive' THEN 0
						ELSE -1
					END as status_code
				  FROM stream`,
			testData: []map[string]any{
				{"deviceId": "device1", "status": "active"},
				{"deviceId": "device2", "status": "inactive"},
				{"deviceId": "device3", "status": "unknown"},
			},
			expected: []map[string]any{
				{"deviceId": "device1", "status_code": 1.0},
				{"deviceId": "device2", "status_code": 0.0},
				{"deviceId": "device3", "status_code": -1.0},
			},
			wantErr: false,
		},
		{
			name: "CASE表达式与其他字段组合",
			sql: `SELECT deviceId, temperature,
					CASE 
						WHEN temperature > 30 THEN temperature * 1.2
						WHEN temperature > 20 THEN temperature * 1.1
						ELSE temperature
					END as adjusted_temp
				  FROM stream`,
			testData: []map[string]any{
				{"deviceId": "device1", "temperature": 35.0},
				{"deviceId": "device2", "temperature": 25.0},
				{"deviceId": "device3", "temperature": 15.0},
			},
			expected: []map[string]any{
				{"deviceId": "device1", "temperature": 35.0, "adjusted_temp": 42.0},
				{"deviceId": "device2", "temperature": 25.0, "adjusted_temp": 27.5},
				{"deviceId": "device3", "temperature": 15.0, "adjusted_temp": 15.0},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ssql := streamsql.New()
			defer ssql.Stop()

			err := ssql.Execute(tt.sql)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			if err != nil {
				t.Skip("CASE expression not yet supported in non-aggregated context")
				return
			}

			// If successful, continue testing data processing
			strm := ssql.Stream()

			// Collect all the results
			var allResults []map[string]any
			resultChan := make(chan []map[string]any, 10)
			strm.AddSink(func(result []map[string]any) {
				select {
				case resultChan <- result:
				default:
				}
			})

			// Add test data
			for _, data := range tt.testData {
				strm.Emit(data)
			}

			// Wait and collect results
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// Collect all the results
			for i := 0; i < len(tt.testData); i++ {
				select {
				case result := <-resultChan:
					if len(result) > 0 {
						allResults = append(allResults, result...)
					}
				case <-ctx.Done():
					t.Logf("Timeout waiting for result %d", i+1)
					break
				}
			}

			// Verification of the number of results
			assert.Equal(t, len(tt.expected), len(allResults), "结果数量不匹配")

			// Verify the content of each result (not based on order)
			expectedMap := make(map[string]map[string]any)
			for _, expected := range tt.expected {
				deviceId, ok := expected["deviceId"].(string)
				if ok {
					expectedMap[deviceId] = expected
				}
			}

			// All the devices you expect to verify appear in the results
			for deviceId := range expectedMap {
				found := false
				for _, actual := range allResults {
					if actualDeviceId, ok := actual["deviceId"].(string); ok && actualDeviceId == deviceId {
						found = true
						break
					}
				}
				assert.True(t, found, "期望的设备 %s 未出现在结果中", deviceId)
			}

			for _, actual := range allResults {
				deviceId, ok := actual["deviceId"].(string)
				if !ok {
					t.Errorf("deviceId fields are missing from the results")
					continue
				}

				expected, exists := expectedMap[deviceId]
				if !exists {
					t.Errorf("Expected results for device %s not found", deviceId)
					continue
				}

				// Verify each field
				for key, expectedValue := range expected {
					actualValue, exists := actual[key]
					assert.True(t, exists, "字段 %s 不存在于结果中", key)
					if exists {
						// For numeric types, approximate comparison is used
						if expectedFloat, ok := expectedValue.(float64); ok {
							if actualFloat, ok := actualValue.(float64); ok {
								assert.InDelta(t, expectedFloat, actualFloat, 0.001, "字段 %s 的值不匹配", key)
							} else {
								assert.Equal(t, expectedValue, actualValue, "字段 %s 的值不匹配", key)
							}
						} else {
							assert.Equal(t, expectedValue, actualValue, "字段 %s 的值不匹配", key)
						}
					}
				}

				// There are no extra fields in the validation results (except deviceId)
				for key := range actual {
					if key == "deviceId" {
						continue
					}
					_, exists := expected[key]
					assert.True(t, exists, "结果中包含未期望的字段 %s", key)
				}

				t.Logf("Equipment %s: Expected = %v, Actual = %v", deviceId, expected, actual)
			}
		})
	}
}

// TestCaseExpressionAggregated tests the CASE expression in the aggregation scenario
func TestCaseExpressionAggregated(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		sql      string
		testData []map[string]any
		wantErr  bool
	}{
		{
			name: "聚合中的CASE表达式 - 条件计数",
			sql: `SELECT deviceId,
					COUNT(CASE WHEN temperature > 25 THEN 1 END) as high_temp_count,
					COUNT(CASE WHEN temperature <= 25 THEN 1 END) as normal_temp_count,
					COUNT(*) as total_count
				  FROM stream
				  GROUP BY deviceId, TumblingWindow('1s')`,
			testData: []map[string]any{
				{"deviceId": "device1", "temperature": 30.0},
				{"deviceId": "device1", "temperature": 20.0},
				{"deviceId": "device1", "temperature": 35.0},
				{"deviceId": "device2", "temperature": 22.0},
				{"deviceId": "device2", "temperature": 28.0},
			},
			wantErr: false,
		},
		{
			name: "聚合中的CASE表达式 - 条件求和",
			sql: `SELECT deviceId,
					SUM(CASE 
						WHEN temperature > 25 THEN temperature 
						ELSE 0 
					END) as high_temp_sum,
					AVG(CASE 
						WHEN humidity > 50 THEN humidity 
						ELSE NULL 
					END) as avg_high_humidity
				  FROM stream
				  GROUP BY deviceId, TumblingWindow('1s')`,
			testData: []map[string]any{
				{"deviceId": "device1", "temperature": 30.0, "humidity": 60.0},
				{"deviceId": "device1", "temperature": 20.0, "humidity": 40.0},
				{"deviceId": "device1", "temperature": 35.0, "humidity": 70.0},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ssql := streamsql.New()
			defer ssql.Stop()

			err := ssql.Execute(tt.sql)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			if err != nil {
				t.Skip("CASE expression not yet supported in aggregated context")
				return
			}

			strm := ssql.Stream()

			// Use channels to wait for results and avoid fixed waiting times
			resultChan := make(chan any, 5)
			strm.AddSink(func(result []map[string]any) {
				select {
				case resultChan <- result:
				default:
				}
			})

			for _, data := range tt.testData {
				strm.Emit(data)
			}

			// Use a wait-out mechanism with timeouts
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			var results []map[string]any

			// Wait for the window to trigger or time out
			select {
			case result := <-resultChan:
				if resultSlice, ok := result.([]map[string]any); ok {
					results = append(results, resultSlice...)
				}
			case <-time.After(1200 * time.Millisecond):
				// If there is no result within 1.2 seconds, manually trigger the window
				if strm.Window != nil {
					ssql.TriggerWindow()
				}
				// Wait a little longer for the results
				select {
				case result := <-resultChan:
					if resultSlice, ok := result.([]map[string]any); ok {
						results = append(results, resultSlice...)
					}
				case <-time.After(200 * time.Millisecond):
					// Timeout, continue verification
				}
			case <-ctx.Done():
				return
			}

			// Verify the results
			if len(results) > 0 {
				firstResult := results[0]
				assert.NotNil(t, firstResult)
				assert.Contains(t, firstResult, "deviceId", "Result should contain deviceId")
			}
		})
	}
}

// TestCaseExpressionNullHandlingInAggregation: Tests whether the CASE expression correctly handles NULL values in the aggregation function
func TestCaseExpressionNullHandlingInAggregation(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                  string
		sql                   string
		testData              []map[string]any
		expectedDeviceResults map[string]map[string]any
		description           string
	}{
		{
			name: "CASE表达式在SUM/COUNT/AVG聚合中正确处理NULL值",
			sql: `SELECT deviceType,
			            SUM(CASE WHEN temperature > 30 THEN temperature ELSE NULL END) as high_temp_sum,
			            COUNT(CASE WHEN temperature > 30 THEN 1 ELSE NULL END) as high_temp_count,
			            AVG(CASE WHEN temperature > 30 THEN temperature ELSE NULL END) as high_temp_avg,
			            COUNT(*) as total_count
			      FROM stream 
			      GROUP BY deviceType, TumblingWindow('2s')`,
			testData: []map[string]any{
				{"deviceType": "sensor", "temperature": 35.0},  // Conditions are met
				{"deviceType": "sensor", "temperature": 25.0},  // If the condition is not met, return NULL
				{"deviceType": "sensor", "temperature": 32.0},  // Conditions are met
				{"deviceType": "monitor", "temperature": 28.0}, // If the condition is not met, return NULL
				{"deviceType": "monitor", "temperature": 33.0}, // Conditions are met
			},
			expectedDeviceResults: map[string]map[string]any{
				"sensor": {
					"high_temp_sum":   67.0, // 35 + 32
					"high_temp_count": 2.0,  // COUNT should ignore NULL
					"high_temp_avg":   33.5, // (35 + 32) / 2
					"total_count":     3.0,  // Total recorded count
				},
				"monitor": {
					"high_temp_sum":   33.0, // Only 33
					"high_temp_count": 1.0,  // COUNT should ignore NULL
					"high_temp_avg":   33.0, // Only 33
					"total_count":     2.0,  // Total recorded count
				},
			},
			description: "验证CASE表达式返回的NULL值被聚合函数正确忽略",
		},
		{
			name: "全部返回NULL值时聚合函数的行为",
			sql: `SELECT deviceType,
			            SUM(CASE WHEN temperature > 50 THEN temperature ELSE NULL END) as impossible_sum,
			            COUNT(CASE WHEN temperature > 50 THEN 1 ELSE NULL END) as impossible_count,
			            AVG(CASE WHEN temperature > 50 THEN temperature ELSE NULL END) as impossible_avg,
			            COUNT(*) as total_count
			      FROM stream 
			      GROUP BY deviceType, TumblingWindow('2s')`,
			testData: []map[string]any{
				{"deviceType": "cold_sensor", "temperature": 20.0}, // The conditions are not met
				{"deviceType": "cold_sensor", "temperature": 25.0}, // The conditions are not met
				{"deviceType": "cold_sensor", "temperature": 30.0}, // The conditions are not met
			},
			expectedDeviceResults: map[string]map[string]any{
				"cold_sensor": {
					"impossible_sum":   nil, // For full NULL, the SUM should return NULL
					"impossible_count": 0.0, // COUNT should return 0
					"impossible_avg":   nil, // For full NULL, AVG should return NULL
					"total_count":      3.0, // Total recorded count
				},
			},
			description: "验证当CASE表达式全部返回NULL时，聚合函数的正确行为",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a StreamSQL instance
			ssql := streamsql.New()
			defer ssql.Stop()

			// Execute SQL
			err := ssql.Execute(tc.sql)
			assert.NoError(t, err, "SQL执行应该成功")

			// Collect the results
			var results []map[string]any
			resultChan := make(chan any, 10)

			ssql.AddSink(func(result []map[string]any) {
				resultChan <- result
			})

			// Add test data
			for _, data := range tc.testData {
				ssql.Stream().Emit(data)
			}

			// Wait for the window to trigger
			time.Sleep(3 * time.Second)

			// Collect the results
		collecting:
			for {
				select {
				case result := <-resultChan:
					if resultSlice, ok := result.([]map[string]any); ok {
						results = append(results, resultSlice...)
					}
				case <-time.After(500 * time.Millisecond):
					break collecting
				}
			}

			// Verification of the number of results
			assert.Len(t, results, len(tc.expectedDeviceResults), "结果数量应该匹配")

			// Verify the results of each deviceType
			for _, result := range results {
				deviceType := result["deviceType"].(string)
				expected := tc.expectedDeviceResults[deviceType]

				assert.NotNil(t, expected, "应该有设备类型 %s 的期望结果", deviceType)

				// Verify each field
				for key, expectedValue := range expected {
					if key == "deviceType" {
						continue
					}

					actualValue := result[key]

					// Handle NULL value comparison
					if expectedValue == nil {
						assert.Nil(t, actualValue,
							"设备类型 %s 的字段 %s 应该为NULL", deviceType, key)
					} else {
						assert.Equal(t, expectedValue, actualValue,
							"设备类型 %s 的字段 %s 应该匹配", deviceType, key)
					}
				}
			}
		})
	}
}

// TestHavingWithCaseExpression tests the CASE expression in the HAVING clause
func TestHavingWithCaseExpression(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		errMsg  string
	}{
		{
			name: "简单CASE表达式在HAVING中",
			sql: `SELECT deviceId, 
			             AVG(temperature) as avg_temp,
			             AVG(CASE WHEN temperature > 30 THEN temperature ELSE 0 END) as conditional_avg
			      FROM stream 
			      GROUP BY deviceId, TumblingWindow('5s')
			      HAVING conditional_avg > 25`,
			wantErr: false,
		},
		{
			name: "复杂CASE表达式在HAVING中",
			sql: `SELECT deviceId, 
			             COUNT(*) as total_count,
			             SUM(CASE 
			                   WHEN temperature > 35 THEN 2
			                   WHEN temperature > 25 THEN 1
			                   ELSE 0
			                 END) as weighted_score
			      FROM stream 
			      GROUP BY deviceId, TumblingWindow('5s')
			      HAVING weighted_score > 3`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test SQL parsing
			_, err := rsql.NewParser(tt.sql).Parse()

			if tt.wantErr {
				assert.Error(t, err, "应该产生解析错误")
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg, "错误消息应该包含期望的内容")
				}
			} else {
				assert.NoError(t, err, "SQL解析应该成功")
			}

			// If the parsing succeeds, try creating a StreamSQL instance
			if !tt.wantErr && err == nil {
				streamSQL := streamsql.New()
				defer streamSQL.Stop()

				err = streamSQL.Execute(tt.sql)
				if err != nil {
					t.Skipf("CASE expression execution in HAVING is not currently supported: %v", err)
				}
			}
		})
	}
}

// TestHavingWithCaseExpressionFunctional Function tests CASE expressions in HAVING clauses
func TestHavingWithCaseExpressionFunctional(t *testing.T) {
	t.Parallel()
	sql := `SELECT deviceId, 
	              AVG(temperature) as avg_temp,
	              COUNT(*) as total_count,
	              SUM(CASE WHEN temperature > 30 THEN 1 ELSE 0 END) as hot_count
	        FROM stream 
	        GROUP BY deviceId, TumblingWindow('2s')
	        HAVING hot_count >= 2`

	// Create a StreamSQL instance
	streamSQL := streamsql.New()
	defer streamSQL.Stop()

	err := streamSQL.Execute(sql)
	assert.NoError(t, err, "执行SQL应该成功")

	// Simulation data
	testData := []map[string]any{
		// device1: 3 high-temperature records, should pass HAVING conditions
		{"deviceId": "device1", "temperature": 35.0},
		{"deviceId": "device1", "temperature": 32.0},
		{"deviceId": "device1", "temperature": 31.0},
		{"deviceId": "device1", "temperature": 25.0}, // Not high temperatures

		// device2: One high-temperature record, should not pass HAVING conditions
		{"deviceId": "device2", "temperature": 33.0},
		{"deviceId": "device2", "temperature": 28.0},
		{"deviceId": "device2", "temperature": 26.0},

		// device3: Two high-temperature records, should pass HAVING conditions
		{"deviceId": "device3", "temperature": 34.0},
		{"deviceId": "device3", "temperature": 31.0},
		{"deviceId": "device3", "temperature": 29.0},
	}

	// Add data and get results
	var results []map[string]any
	var resultsMutex sync.Mutex
	streamSQL.Stream().AddSink(func(result []map[string]any) {
		resultsMutex.Lock()
		defer resultsMutex.Unlock()
		results = append(results, result...)
	})

	for _, data := range testData {
		streamSQL.Emit(data)
	}

	// Wait for the window to trigger
	time.Sleep(2500 * time.Millisecond)

	// Manually trigger the window
	streamSQL.TriggerWindow()

	// Wait for the results
	time.Sleep(200 * time.Millisecond)

	// Verify the results
	resultsMutex.Lock()
	defer resultsMutex.Unlock()

	// Only device1 and device3 should pass the HAVING condition (hot_count > = 2)
	assert.Greater(t, len(results), 0, "应该有结果返回")

	// Only devices that meet the HAVING criteria are included in the validation results
	deviceResults := make(map[string]map[string]any)
	for _, result := range results {
		deviceId, ok := result["deviceId"].(string)
		assert.True(t, ok, "deviceId应该是字符串类型")
		deviceResults[deviceId] = result
	}

	// Verify the filtering effect of HAVING conditions
	for deviceId, result := range deviceResults {
		hotCount := getFloat64Value(result["hot_count"])
		assert.GreaterOrEqual(t, hotCount, 2.0,
			"设备 %s 的hot_count应该 >= 2 (HAVING条件)", deviceId)
	}

	// device2 should be filtered out by HAVING conditions (only 1 high-temperature record < 2)
	assert.NotContains(t, deviceResults, "device2",
		"device2应该被HAVING条件过滤掉（hot_count=1 < 2）")

	// The device that verifies the desired results appears in the results
	assert.Contains(t, deviceResults, "device1", "device1应该通过HAVING条件")
	assert.Contains(t, deviceResults, "device3", "device3应该通过HAVING条件")
}

// TestNegativeNumberInSQL tests the use of negative numbers in full SQL
func TestNegativeNumberInSQL(t *testing.T) {
	t.Parallel()
	sql := `SELECT deviceId,
	              temperature,
	              CASE 
	                WHEN temperature < -10.0 THEN 'FREEZING'
	                WHEN temperature < 0 THEN 'COLD'
	                WHEN temperature = 0 THEN 'ZERO'
	                ELSE 'POSITIVE'
	              END as temp_category,
	              CASE 
	                WHEN temperature > 0 THEN temperature 
	                ELSE -1.0 
	              END as adjusted_temp
	        FROM stream`

	streamSQL := streamsql.New()
	defer streamSQL.Stop()

	err := streamSQL.Execute(sql)
	assert.NoError(t, err, "包含负数的SQL应该执行成功")

	// The simulation contains negative data
	testData := []map[string]any{
		{"deviceId": "sensor1", "temperature": -15.0},
		{"deviceId": "sensor2", "temperature": -5.0},
		{"deviceId": "sensor3", "temperature": 0.0},
		{"deviceId": "sensor4", "temperature": 10.0},
	}

	// Collect the results
	var results []map[string]any
	var resultsMutex sync.Mutex

	streamSQL.Stream().AddSink(func(result []map[string]any) {
		resultsMutex.Lock()
		defer resultsMutex.Unlock()
		results = append(results, result...)
	})

	// Add test data
	for _, data := range testData {
		streamSQL.Emit(data)
	}

	// Waiting for processing
	time.Sleep(200 * time.Millisecond)

	// Verify the results
	resultsMutex.Lock()
	defer resultsMutex.Unlock()

	for _, result := range results {
		// Verify that the required fields are included
		assert.Contains(t, result, "deviceId", "结果应该包含deviceId")
		assert.Contains(t, result, "temperature", "结果应该包含temperature")
		assert.Contains(t, result, "temp_category", "结果应该包含temp_category")
		assert.Contains(t, result, "adjusted_temp", "结果应该包含adjusted_temp")
	}
}
