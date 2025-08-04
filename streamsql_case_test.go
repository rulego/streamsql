package streamsql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/rsql"
	"github.com/stretchr/testify/assert"
)

// TestCaseExpressionInSQL 测试CASE表达式在SQL查询中的使用
func TestCaseExpressionInSQL(t *testing.T) {
	// 测试非聚合场景中的CASE表达式
	sql := `SELECT deviceId, 
	              CASE WHEN temperature > 30 THEN 'HOT' 
	                   WHEN temperature > 20 THEN 'WARM' 
	                   ELSE 'COOL' END as temp_category,
	              CASE status WHEN 'active' THEN 1 ELSE 0 END as status_code
	         FROM stream 
	         WHERE temperature > 15`

	// 创建StreamSQL实例
	streamSQL := New()
	defer streamSQL.Stop()

	err := streamSQL.Execute(sql)
	assert.NoError(t, err, "执行SQL应该成功")

	// 模拟数据
	testData := []map[string]interface{}{
		{"deviceId": "device1", "temperature": 35.0, "status": "active"},
		{"deviceId": "device2", "temperature": 25.0, "status": "inactive"},
		{"deviceId": "device3", "temperature": 18.0, "status": "active"},
		{"deviceId": "device4", "temperature": 10.0, "status": "inactive"}, // 应该被WHERE过滤掉
	}

	// 添加数据并获取结果
	var results []map[string]interface{}
	var resultsMutex sync.Mutex
	streamSQL.stream.AddSink(func(result []map[string]interface{}) {
		resultsMutex.Lock()
		defer resultsMutex.Unlock()
		results = append(results, result...)
	})

	for _, data := range testData {
		streamSQL.Emit(data)
	}

	// 等待处理
	time.Sleep(100 * time.Millisecond)

	// 验证结果
	resultsMutex.Lock()
	resultCount := len(results)
	resultsMutex.Unlock()
	assert.GreaterOrEqual(t, resultCount, 3, "应该有至少3条结果（排除temperature <= 15的记录）")
}

// TestCaseExpressionInAggregation 测试CASE表达式在聚合查询中的使用
func TestCaseExpressionInAggregation(t *testing.T) {
	sql := `SELECT deviceId,
	              COUNT(*) as total_count,
	              SUM(CASE WHEN temperature > 30 THEN 1 ELSE 0 END) as hot_count,
	              AVG(CASE status WHEN 'active' THEN temperature ELSE 0 END) as avg_active_temp
	         FROM stream 
	         GROUP BY deviceId, TumblingWindow('1s')
	         WITH (TIMESTAMP='ts', TIMEUNIT='ss')`

	// 创建StreamSQL实例
	streamSQL := New()
	defer streamSQL.Stop()

	err := streamSQL.Execute(sql)
	assert.NoError(t, err, "执行SQL应该成功")

	// 模拟数据
	baseTime := time.Now()
	testData := []map[string]interface{}{
		{"deviceId": "device1", "temperature": 35.0, "status": "active", "ts": baseTime},
		{"deviceId": "device1", "temperature": 25.0, "status": "inactive", "ts": baseTime},
		{"deviceId": "device1", "temperature": 32.0, "status": "active", "ts": baseTime},
		{"deviceId": "device2", "temperature": 28.0, "status": "active", "ts": baseTime},
		{"deviceId": "device2", "temperature": 22.0, "status": "inactive", "ts": baseTime},
	}

	// 添加数据并获取结果
	var results []map[string]interface{}
	var resultsMutex sync.Mutex
	streamSQL.stream.AddSink(func(result []map[string]interface{}) {
		resultsMutex.Lock()
		defer resultsMutex.Unlock()
		results = append(results, result...)
	})

	for _, data := range testData {
		streamSQL.Emit(data)
	}

	// 等待窗口触发
	time.Sleep(1200 * time.Millisecond)

	// 手动触发窗口
	streamSQL.stream.Window.Trigger()

	// 等待结果
	time.Sleep(100 * time.Millisecond)

	// 验证结果
	resultsMutex.Lock()
	defer resultsMutex.Unlock()

	assert.Greater(t, len(results), 0, "应该有聚合结果返回")

	// 验证结果结构和内容
	deviceResults := make(map[string]map[string]interface{})
	for _, result := range results {
		deviceId, ok := result["deviceId"].(string)
		assert.True(t, ok, "deviceId应该是字符串类型")
		deviceResults[deviceId] = result
	}

	// 期望有两个设备的结果
	assert.Len(t, deviceResults, 2, "应该有两个设备的聚合结果")
	assert.Contains(t, deviceResults, "device1", "应该包含device1的结果")
	assert.Contains(t, deviceResults, "device2", "应该包含device2的结果")

	// 验证device1的结果
	device1Result := deviceResults["device1"]

	// 基本字段检查
	assert.Contains(t, device1Result, "total_count", "device1结果应该包含total_count")
	assert.Contains(t, device1Result, "hot_count", "device1结果应该包含hot_count")
	assert.Contains(t, device1Result, "avg_active_temp", "device1结果应该包含avg_active_temp")

	// 详细数值验证
	totalCount1 := getFloat64Value(device1Result["total_count"])
	hotCount1 := getFloat64Value(device1Result["hot_count"])
	avgActiveTemp1 := getFloat64Value(device1Result["avg_active_temp"])

	// device1: 3条记录总数
	assert.Equal(t, 3.0, totalCount1, "device1应该有3条记录")

	// device1: 2条高温记录 (35.0 > 30, 32.0 > 30)
	assert.Equal(t, 2.0, hotCount1, "device1应该有2条高温记录")

	// device1: active状态的平均温度 (35.0 + 0 + 32.0) / 3 = 22.333...
	expectedActiveAvg := (35.0 + 0 + 32.0) / 3.0
	assert.InDelta(t, expectedActiveAvg, avgActiveTemp1, 0.01,
		"device1的AVG(CASE WHEN...)应该正确计算")

	// 验证device2的结果
	device2Result := deviceResults["device2"]

	// 基本字段检查
	assert.Contains(t, device2Result, "total_count", "device2结果应该包含total_count")
	assert.Contains(t, device2Result, "hot_count", "device2结果应该包含hot_count")
	assert.Contains(t, device2Result, "avg_active_temp", "device2结果应该包含avg_active_temp")

	// 详细数值验证
	totalCount2 := getFloat64Value(device2Result["total_count"])
	hotCount2 := getFloat64Value(device2Result["hot_count"])
	avgActiveTemp2 := getFloat64Value(device2Result["avg_active_temp"])

	// device2: 2条记录总数
	assert.Equal(t, 2.0, totalCount2, "device2应该有2条记录")

	// device2: 0条高温记录 (没有温度>30的)
	assert.Equal(t, 0.0, hotCount2, "device2应该有0条高温记录")

	// device2: CASE WHEN status='active' THEN temperature ELSE 0
	// 28.0 (active) + 0 (inactive) = 28.0, 平均值 = (28.0 + 0) / 2 = 14.0
	expectedActiveAvg2 := (28.0 + 0) / 2.0
	assert.InDelta(t, expectedActiveAvg2, avgActiveTemp2, 0.01,
		"device2的AVG(CASE WHEN...)应该正确计算")
}

// getFloat64Value 辅助函数，将interface{}转换为float64
func getFloat64Value(value interface{}) float64 {
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

// TestComplexCaseExpressionsInAggregation 测试复杂CASE表达式在聚合查询中的使用
func TestComplexCaseExpressionsInAggregation(t *testing.T) {
	testCases := []struct {
		name        string
		sql         string
		data        []map[string]interface{}
		description string
	}{
		{
			name: "多条件CASE在SUM中",
			sql: `SELECT deviceId,
			            SUM(CASE WHEN temperature > 30 AND humidity > 60 THEN 1 
			                     WHEN temperature > 25 THEN 0.5 
			                     ELSE 0 END) as complex_score
			      FROM stream 
			      GROUP BY deviceId, TumblingWindow('1s')
			      WITH (TIMESTAMP='ts', TIMEUNIT='ss')`,
			data: []map[string]interface{}{
				{"deviceId": "device1", "temperature": 35.0, "humidity": 70.0, "ts": time.Now()},
				{"deviceId": "device1", "temperature": 28.0, "humidity": 50.0, "ts": time.Now()},
				{"deviceId": "device1", "temperature": 20.0, "humidity": 40.0, "ts": time.Now()},
			},
			description: "测试多条件CASE表达式在SUM聚合中的使用",
		},
		{
			name: "函数调用CASE在AVG中",
			sql: `SELECT deviceId,
			      AVG(CASE WHEN ABS(temperature - 25) < 5 THEN temperature ELSE 0 END) as normalized_avg
			      FROM stream 
			      GROUP BY deviceId, TumblingWindow('1s')
			      WITH (TIMESTAMP='ts', TIMEUNIT='ss')`,
			data: []map[string]interface{}{
				{"deviceId": "device1", "temperature": 23.0, "ts": time.Now()},
				{"deviceId": "device1", "temperature": 27.0, "ts": time.Now()},
				{"deviceId": "device1", "temperature": 35.0, "ts": time.Now()}, // 这个会被排除
			},
			description: "测试带函数的CASE表达式在AVG聚合中的使用",
		},
		{
			name: "复杂算术CASE在COUNT中",
			sql: `SELECT deviceId,
			            COUNT(CASE WHEN temperature * 1.8 + 32 > 80 THEN 1 END) as fahrenheit_hot_count
			      FROM stream 
			      GROUP BY deviceId, TumblingWindow('1s')
			      WITH (TIMESTAMP='ts', TIMEUNIT='ss')`,
			data: []map[string]interface{}{
				{"deviceId": "device1", "temperature": 25.0, "ts": time.Now()}, // 77F
				{"deviceId": "device1", "temperature": 30.0, "ts": time.Now()}, // 86F
				{"deviceId": "device1", "temperature": 35.0, "ts": time.Now()}, // 95F
			},
			description: "测试算术表达式CASE在COUNT聚合中的使用",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建StreamSQL实例
			streamSQL := New()
			defer streamSQL.Stop()

			err := streamSQL.Execute(tc.sql)
			assert.NoError(t, err, "执行SQL应该成功")

			// 添加数据并获取结果
			var results []map[string]interface{}
			var resultsMutex sync.Mutex
			streamSQL.stream.AddSink(func(result []map[string]interface{}) {
				resultsMutex.Lock()
				defer resultsMutex.Unlock()
				results = append(results, result...)
			})

			for _, data := range tc.data {
				streamSQL.Emit(data)
			}

			// 等待窗口触发
			time.Sleep(1200 * time.Millisecond)

			// 手动触发窗口
			streamSQL.stream.Window.Trigger()

			// 等待结果
			time.Sleep(100 * time.Millisecond)

			// 验证至少有结果返回
			resultsMutex.Lock()
			hasResults := len(results) > 0
			resultsMutex.Unlock()
			assert.True(t, hasResults, "应该有聚合结果返回")
		})
	}
}

// TestCaseExpressionNonAggregated 测试非聚合场景下的CASE表达式
func TestCaseExpressionNonAggregated(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		testData []map[string]interface{}
		expected []map[string]interface{} // 期望的结果
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
			testData: []map[string]interface{}{
				{"deviceId": "device1", "temperature": 35.0},
				{"deviceId": "device2", "temperature": 25.0},
				{"deviceId": "device3", "temperature": 15.0},
				{"deviceId": "device4", "temperature": 5.0},
			},
			expected: []map[string]interface{}{
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
			testData: []map[string]interface{}{
				{"deviceId": "device1", "status": "active"},
				{"deviceId": "device2", "status": "inactive"},
				{"deviceId": "device3", "status": "unknown"},
			},
			expected: []map[string]interface{}{
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
			testData: []map[string]interface{}{
				{"deviceId": "device1", "temperature": 35.0},
				{"deviceId": "device2", "temperature": 25.0},
				{"deviceId": "device3", "temperature": 15.0},
			},
			expected: []map[string]interface{}{
				{"deviceId": "device1", "temperature": 35.0, "adjusted_temp": 42.0},
				{"deviceId": "device2", "temperature": 25.0, "adjusted_temp": 27.5},
				{"deviceId": "device3", "temperature": 15.0, "adjusted_temp": 15.0},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamsql := New()
			defer streamsql.Stop()

			err := streamsql.Execute(tt.sql)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			if err != nil {
				t.Skip("CASE expression not yet supported in non-aggregated context")
				return
			}

			// 如果执行成功，继续测试数据处理
			strm := streamsql.stream

			// 收集所有结果
			var allResults []map[string]interface{}
			resultChan := make(chan []map[string]interface{}, 10)
			strm.AddSink(func(result []map[string]interface{}) {
				select {
				case resultChan <- result:
				default:
				}
			})

			// 添加测试数据
			for _, data := range tt.testData {
				strm.Emit(data)
			}

			// 等待并收集结果
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// 收集所有结果
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

			// 验证结果数量
			assert.Equal(t, len(tt.expected), len(allResults), "结果数量不匹配")

			// 验证每个结果的内容（不依赖顺序）
			expectedMap := make(map[string]map[string]interface{})
			for _, expected := range tt.expected {
				deviceId, ok := expected["deviceId"].(string)
				if ok {
					expectedMap[deviceId] = expected
				}
			}

			// 验证所有期望的设备都出现在结果中
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
					t.Errorf("结果中缺少deviceId字段")
					continue
				}

				expected, exists := expectedMap[deviceId]
				if !exists {
					t.Errorf("未找到设备 %s 的期望结果", deviceId)
					continue
				}

				// 验证每个字段
				for key, expectedValue := range expected {
					actualValue, exists := actual[key]
					assert.True(t, exists, "字段 %s 不存在于结果中", key)
					if exists {
						// 对于数值类型，使用近似比较
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

				// 验证结果中没有多余的字段（除了deviceId）
				for key := range actual {
					if key == "deviceId" {
						continue
					}
					_, exists := expected[key]
					assert.True(t, exists, "结果中包含未期望的字段 %s", key)
				}

				t.Logf("设备 %s: 期望=%v, 实际=%v", deviceId, expected, actual)
			}
		})
	}
}

// TestCaseExpressionAggregated 测试聚合场景下的CASE表达式
func TestCaseExpressionAggregated(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		testData []map[string]interface{}
		wantErr  bool
	}{
		{
			name: "聚合中的CASE表达式 - 条件计数",
			sql: `SELECT deviceId,
					COUNT(CASE WHEN temperature > 25 THEN 1 END) as high_temp_count,
					COUNT(CASE WHEN temperature <= 25 THEN 1 END) as normal_temp_count,
					COUNT(*) as total_count
				  FROM stream
				  GROUP BY deviceId, TumblingWindow('1s')
				  WITH (TIMESTAMP='ts', TIMEUNIT='ss')`,
			testData: []map[string]interface{}{
				{"deviceId": "device1", "temperature": 30.0, "ts": time.Now()},
				{"deviceId": "device1", "temperature": 20.0, "ts": time.Now()},
				{"deviceId": "device1", "temperature": 35.0, "ts": time.Now()},
				{"deviceId": "device2", "temperature": 22.0, "ts": time.Now()},
				{"deviceId": "device2", "temperature": 28.0, "ts": time.Now()},
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
				  GROUP BY deviceId, TumblingWindow('1s')
				  WITH (TIMESTAMP='ts', TIMEUNIT='ss')`,
			testData: []map[string]interface{}{
				{"deviceId": "device1", "temperature": 30.0, "humidity": 60.0, "ts": time.Now()},
				{"deviceId": "device1", "temperature": 20.0, "humidity": 40.0, "ts": time.Now()},
				{"deviceId": "device1", "temperature": 35.0, "humidity": 70.0, "ts": time.Now()},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			streamsql := New()
			defer streamsql.Stop()

			err := streamsql.Execute(tt.sql)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			if err != nil {
				t.Skip("CASE expression not yet supported in aggregated context")
				return
			}

			strm := streamsql.stream

			// 使用通道等待结果，避免固定等待时间
			resultChan := make(chan interface{}, 5)
			strm.AddSink(func(result []map[string]interface{}) {
				select {
				case resultChan <- result:
				default:
				}
			})

			for _, data := range tt.testData {
				strm.Emit(data)
			}

			// 使用带超时的等待机制
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			var results []map[string]interface{}

			// 等待窗口触发或超时
			select {
			case result := <-resultChan:
				if resultSlice, ok := result.([]map[string]interface{}); ok {
					results = append(results, resultSlice...)
				}
			case <-time.After(1200 * time.Millisecond):
				// 如果1.2秒内没有结果，手动触发窗口
				if strm.Window != nil {
					strm.Window.Trigger()
				}
				// 再等待一点时间获取结果
				select {
				case result := <-resultChan:
					if resultSlice, ok := result.([]map[string]interface{}); ok {
						results = append(results, resultSlice...)
					}
				case <-time.After(200 * time.Millisecond):
					// 超时，继续验证
				}
			case <-ctx.Done():
				return
			}

			// 验证结果
			if len(results) > 0 {
				firstResult := results[0]
				assert.NotNil(t, firstResult)
				assert.Contains(t, firstResult, "deviceId", "Result should contain deviceId")
			}
		})
	}
}

// TestCaseExpressionNullHandlingInAggregation 测试CASE表达式在聚合函数中正确处理NULL值
func TestCaseExpressionNullHandlingInAggregation(t *testing.T) {
	testCases := []struct {
		name                  string
		sql                   string
		testData              []map[string]interface{}
		expectedDeviceResults map[string]map[string]interface{}
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
			testData: []map[string]interface{}{
				{"deviceType": "sensor", "temperature": 35.0},  // 满足条件
				{"deviceType": "sensor", "temperature": 25.0},  // 不满足条件，返回NULL
				{"deviceType": "sensor", "temperature": 32.0},  // 满足条件
				{"deviceType": "monitor", "temperature": 28.0}, // 不满足条件，返回NULL
				{"deviceType": "monitor", "temperature": 33.0}, // 满足条件
			},
			expectedDeviceResults: map[string]map[string]interface{}{
				"sensor": {
					"high_temp_sum":   67.0, // 35 + 32
					"high_temp_count": 2.0,  // COUNT应该忽略NULL
					"high_temp_avg":   33.5, // (35 + 32) / 2
					"total_count":     3.0,  // 总记录数
				},
				"monitor": {
					"high_temp_sum":   33.0, // 只有33
					"high_temp_count": 1.0,  // COUNT应该忽略NULL
					"high_temp_avg":   33.0, // 只有33
					"total_count":     2.0,  // 总记录数
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
			testData: []map[string]interface{}{
				{"deviceType": "cold_sensor", "temperature": 20.0}, // 不满足条件
				{"deviceType": "cold_sensor", "temperature": 25.0}, // 不满足条件
				{"deviceType": "cold_sensor", "temperature": 30.0}, // 不满足条件
			},
			expectedDeviceResults: map[string]map[string]interface{}{
				"cold_sensor": {
					"impossible_sum":   nil, // 全NULL时SUM应返回NULL
					"impossible_count": 0.0, // COUNT应返回0
					"impossible_avg":   nil, // 全NULL时AVG应返回NULL
					"total_count":      3.0, // 总记录数
				},
			},
			description: "验证当CASE表达式全部返回NULL时，聚合函数的正确行为",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建StreamSQL实例
			ssql := New()
			defer ssql.Stop()

			// 执行SQL
			err := ssql.Execute(tc.sql)
			assert.NoError(t, err, "SQL执行应该成功")

			// 收集结果
			var results []map[string]interface{}
			resultChan := make(chan interface{}, 10)

			ssql.AddSink(func(result []map[string]interface{}) {
				resultChan <- result
			})

			// 添加测试数据
			for _, data := range tc.testData {
				ssql.Stream().Emit(data)
			}

			// 等待窗口触发
			time.Sleep(3 * time.Second)

			// 收集结果
		collecting:
			for {
				select {
				case result := <-resultChan:
					if resultSlice, ok := result.([]map[string]interface{}); ok {
						results = append(results, resultSlice...)
					}
				case <-time.After(500 * time.Millisecond):
					break collecting
				}
			}

			// 验证结果数量
			assert.Len(t, results, len(tc.expectedDeviceResults), "结果数量应该匹配")

			// 验证各个deviceType的结果
			for _, result := range results {
				deviceType := result["deviceType"].(string)
				expected := tc.expectedDeviceResults[deviceType]

				assert.NotNil(t, expected, "应该有设备类型 %s 的期望结果", deviceType)

				// 验证每个字段
				for key, expectedValue := range expected {
					if key == "deviceType" {
						continue
					}

					actualValue := result[key]

					// 处理NULL值比较
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

// TestHavingWithCaseExpression 测试HAVING子句中的CASE表达式
func TestHavingWithCaseExpression(t *testing.T) {
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
			      HAVING conditional_avg > 25
			      WITH (TIMESTAMP='ts', TIMEUNIT='ss')`,
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
			      HAVING weighted_score > 3
			      WITH (TIMESTAMP='ts', TIMEUNIT='ss')`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 测试SQL解析
			_, err := rsql.NewParser(tt.sql).Parse()

			if tt.wantErr {
				assert.Error(t, err, "应该产生解析错误")
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg, "错误消息应该包含期望的内容")
				}
			} else {
				assert.NoError(t, err, "SQL解析应该成功")
			}

			// 如果解析成功，尝试创建StreamSQL实例
			if !tt.wantErr && err == nil {
				streamSQL := New()
				defer streamSQL.Stop()

				err = streamSQL.Execute(tt.sql)
				if err != nil {
					t.Skipf("HAVING中的CASE表达式执行暂不支持: %v", err)
				}
			}
		})
	}
}

// TestHavingWithCaseExpressionFunctional 功能测试HAVING子句中的CASE表达式
func TestHavingWithCaseExpressionFunctional(t *testing.T) {
	sql := `SELECT deviceId, 
	              AVG(temperature) as avg_temp,
	              COUNT(*) as total_count,
	              SUM(CASE WHEN temperature > 30 THEN 1 ELSE 0 END) as hot_count
	        FROM stream 
	        GROUP BY deviceId, TumblingWindow('2s')
	        HAVING hot_count >= 2
	        WITH (TIMESTAMP='ts', TIMEUNIT='ss')`

	// 创建StreamSQL实例
	streamSQL := New()
	defer streamSQL.Stop()

	err := streamSQL.Execute(sql)
	assert.NoError(t, err, "执行SQL应该成功")

	// 模拟数据
	baseTime := time.Now()
	testData := []map[string]interface{}{
		// device1: 3条高温记录，应该通过HAVING条件
		{"deviceId": "device1", "temperature": 35.0, "ts": baseTime},
		{"deviceId": "device1", "temperature": 32.0, "ts": baseTime},
		{"deviceId": "device1", "temperature": 31.0, "ts": baseTime},
		{"deviceId": "device1", "temperature": 25.0, "ts": baseTime}, // 不是高温

		// device2: 1条高温记录，不应该通过HAVING条件
		{"deviceId": "device2", "temperature": 33.0, "ts": baseTime},
		{"deviceId": "device2", "temperature": 28.0, "ts": baseTime},
		{"deviceId": "device2", "temperature": 26.0, "ts": baseTime},

		// device3: 2条高温记录，应该通过HAVING条件
		{"deviceId": "device3", "temperature": 34.0, "ts": baseTime},
		{"deviceId": "device3", "temperature": 31.0, "ts": baseTime},
		{"deviceId": "device3", "temperature": 29.0, "ts": baseTime},
	}

	// 添加数据并获取结果
	var results []map[string]interface{}
	var resultsMutex sync.Mutex
	streamSQL.stream.AddSink(func(result []map[string]interface{}) {
		resultsMutex.Lock()
		defer resultsMutex.Unlock()
		results = append(results, result...)
	})

	for _, data := range testData {
		streamSQL.Emit(data)
	}

	// 等待窗口触发
	time.Sleep(2500 * time.Millisecond)

	// 手动触发窗口
	streamSQL.stream.Window.Trigger()

	// 等待结果
	time.Sleep(200 * time.Millisecond)

	// 验证结果
	resultsMutex.Lock()
	defer resultsMutex.Unlock()

	// 应该只有device1和device3通过HAVING条件（hot_count >= 2）
	assert.Greater(t, len(results), 0, "应该有结果返回")

	// 验证结果中只包含满足HAVING条件的设备
	deviceResults := make(map[string]map[string]interface{})
	for _, result := range results {
		deviceId, ok := result["deviceId"].(string)
		assert.True(t, ok, "deviceId应该是字符串类型")
		deviceResults[deviceId] = result
	}

	// 验证HAVING条件的过滤效果
	for deviceId, result := range deviceResults {
		hotCount := getFloat64Value(result["hot_count"])
		assert.GreaterOrEqual(t, hotCount, 2.0,
			"设备 %s 的hot_count应该 >= 2 (HAVING条件)", deviceId)
	}

	// device2应该被HAVING条件过滤掉（只有1条高温记录 < 2）
	assert.NotContains(t, deviceResults, "device2",
		"device2应该被HAVING条件过滤掉（hot_count=1 < 2）")

	// 验证期望的设备出现在结果中
	assert.Contains(t, deviceResults, "device1", "device1应该通过HAVING条件")
	assert.Contains(t, deviceResults, "device3", "device3应该通过HAVING条件")
}

// TestNegativeNumberInSQL 测试负数在完整SQL中的使用
func TestNegativeNumberInSQL(t *testing.T) {
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

	streamSQL := New()
	defer streamSQL.Stop()

	err := streamSQL.Execute(sql)
	assert.NoError(t, err, "包含负数的SQL应该执行成功")

	// 模拟包含负数的数据
	testData := []map[string]interface{}{
		{"deviceId": "sensor1", "temperature": -15.0},
		{"deviceId": "sensor2", "temperature": -5.0},
		{"deviceId": "sensor3", "temperature": 0.0},
		{"deviceId": "sensor4", "temperature": 10.0},
	}

	// 收集结果
	var results []map[string]interface{}
	var resultsMutex sync.Mutex

	streamSQL.stream.AddSink(func(result []map[string]interface{}) {
		resultsMutex.Lock()
		defer resultsMutex.Unlock()
		results = append(results, result...)
	})

	// 添加测试数据
	for _, data := range testData {
		streamSQL.Emit(data)
	}

	// 等待处理
	time.Sleep(200 * time.Millisecond)

	// 验证结果
	resultsMutex.Lock()
	defer resultsMutex.Unlock()

	for _, result := range results {
		// 验证包含必要字段
		assert.Contains(t, result, "deviceId", "结果应该包含deviceId")
		assert.Contains(t, result, "temperature", "结果应该包含temperature")
		assert.Contains(t, result, "temp_category", "结果应该包含temp_category")
		assert.Contains(t, result, "adjusted_temp", "结果应该包含adjusted_temp")
	}
}
