package streamsql

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/stretchr/testify/assert"
)

// testCase 定义测试用例结构
type testCase struct {
	name        string
	sql         string
	testData    []map[string]interface{}
	expectedLen int
	validator   func(t *testing.T, results []map[string]interface{})
}

// executeTestCase 执行单个测试用例的通用逻辑
func executeTestCase(t *testing.T, streamsql *Streamsql, tc testCase) {
	t.Run(tc.name, func(t *testing.T) {
		// 为每个测试用例创建新的Streamsql实例
		ssql := New()
		defer ssql.Stop()

		err := ssql.Execute(tc.sql)
		assert.Nil(t, err)
		strm := ssql.stream

		// 创建结果接收通道和互斥锁保护并发访问
		resultChan := make(chan interface{}, 10)
		var results []map[string]interface{}
		var resultsMutex sync.Mutex

		strm.AddSink(func(result []map[string]interface{}) {
			select {
			case resultChan <- result:
			default:
				// 通道满时丢弃结果，避免阻塞
			}
		})

		// 添加测试数据
		for _, data := range tc.testData {
			strm.Emit(data)
		}

		// 等待数据处理
		time.Sleep(200 * time.Millisecond)

		// 收集所有结果
		timeout := time.After(2 * time.Second)
		for {
			resultsMutex.Lock()
			currentLen := len(results)
			resultsMutex.Unlock()

			if currentLen >= tc.expectedLen {
				break
			}

			select {
			case result := <-resultChan:
				resultsMutex.Lock()
				if resultSlice, ok := result.([]map[string]interface{}); ok {
					results = append(results, resultSlice...)
				} else if resultMap, ok := result.(map[string]interface{}); ok {
					results = append(results, resultMap)
				}
				resultsMutex.Unlock()
			case <-timeout:
				goto checkResults
			}
		}

	checkResults:
		// 验证结果长度（使用互斥锁保护）
		resultsMutex.Lock()
		finalResults := make([]map[string]interface{}, len(results))
		copy(finalResults, results)
		resultsMutex.Unlock()

		assert.Equal(t, tc.expectedLen, len(finalResults))
		// 执行自定义验证
		if tc.validator != nil {
			tc.validator(t, finalResults)
		}
	})
}

// executeAggregationTestCase 执行聚合函数测试用例的通用逻辑
func executeAggregationTestCase(t *testing.T, streamsql *Streamsql, tc testCase) {
	t.Run(tc.name, func(t *testing.T) {
		// 为每个测试用例创建新的Streamsql实例
		ssql := New()
		defer ssql.Stop()

		err := ssql.Execute(tc.sql)
		assert.Nil(t, err)
		strm := ssql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			select {
			case resultChan <- result:
			default:
				// 通道满时丢弃结果，避免阻塞
			}
		})

		// 添加测试数据
		for _, data := range tc.testData {
			strm.Emit(data)
		}

		// 等待窗口触发
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()
		time.Sleep(500 * time.Millisecond)

		// 验证结果
		select {
		case result := <-resultChan:
			if tc.validator != nil {
				tc.validator(t, result.([]map[string]interface{}))
			}
		case <-time.After(3 * time.Second):
			t.Fatal("测试超时")
		}
	})
}

// executeFunctionTestCase 执行函数测试用例的通用逻辑
func executeFunctionTestCase(t *testing.T, streamsql *Streamsql, tc testCase) {
	t.Run(tc.name, func(t *testing.T) {
		// 为每个测试用例创建新的Streamsql实例
		ssql := New()
		defer ssql.Stop()

		err := ssql.Execute(tc.sql)
		assert.Nil(t, err)
		strm := ssql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)
		strm.AddSink(func(result []map[string]interface{}) {
			select {
			case resultChan <- result:
			default:
				// 通道满时丢弃结果，避免阻塞
			}
		})

		// 添加测试数据
		for _, data := range tc.testData {
			strm.Emit(data)
		}

		time.Sleep(200 * time.Millisecond)

		// 验证结果
		select {
		case result := <-resultChan:
			if tc.validator != nil {
				tc.validator(t, result.([]map[string]interface{}))
			}
		case <-time.After(2 * time.Second):
			t.Fatal("测试超时")
		}
	})
}

// TestQuotedIdentifiersAndStringLiterals 测试反引号标识符和字符串常量支持
func TestQuotedIdentifiersAndStringLiterals(t *testing.T) {
	// 注册测试函数（因为有测试用例使用自定义函数）
	registerTestFunctions(t)
	defer unregisterTestFunctions()

	streamsql := New()
	defer streamsql.Stop()

	// 通用测试数据
	standardTestData := []map[string]interface{}{
		{"deviceId": "sensor001", "deviceType": "temperature"},
		{"deviceId": "device002", "deviceType": "humidity"},
		{"deviceId": "sensor003", "deviceType": "pressure"},
	}

	// 定义测试用例
	testCases := []testCase{
		{
			name:        "反引号标识符支持",
			sql:         "SELECT `deviceId`, `deviceType` FROM stream WHERE `deviceId` LIKE 'sensor%'",
			testData:    standardTestData,
			expectedLen: 2,
			validator: func(t *testing.T, results []map[string]interface{}) {
				for _, result := range results {
					deviceId := result["deviceId"].(string)
					assert.True(t, deviceId == "sensor001" || deviceId == "sensor003")
				}
			},
		},
		{
			name:        "单引号字符串常量支持",
			sql:         "SELECT deviceId, deviceType, 'constant_value' as test FROM stream WHERE deviceId = 'sensor001'",
			testData:    standardTestData,
			expectedLen: 1,
			validator: func(t *testing.T, results []map[string]interface{}) {
				if len(results) > 0 {
					resultMap := results[0]
					assert.Equal(t, "sensor001", resultMap["deviceId"])
					assert.Equal(t, "temperature", resultMap["deviceType"])
					assert.Equal(t, "constant_value", resultMap["test"])
				}
			},
		},
		{
			name:        "双引号字符串常量支持",
			sql:         `SELECT deviceId, deviceType, "another_constant" as test FROM stream WHERE deviceType = "temperature"`,
			testData:    standardTestData,
			expectedLen: 1,
			validator: func(t *testing.T, results []map[string]interface{}) {
				if len(results) > 0 {
					resultMap := results[0]
					assert.Equal(t, "sensor001", resultMap["deviceId"])
					assert.Equal(t, "temperature", resultMap["deviceType"])
					assert.Equal(t, "another_constant", resultMap["test"])
				}
			},
		},
		{
			name:        "混合使用反引号标识符和字符串常量",
			sql:         "SELECT `deviceId`, `deviceType`, 'mixed_test' as test_field,'normal' FROM stream WHERE `deviceId` = 'sensor001'",
			testData:    standardTestData,
			expectedLen: 1,
			validator: func(t *testing.T, results []map[string]interface{}) {
				for _, result := range results {
					deviceId := result["deviceId"].(string)
					assert.True(t, deviceId == "sensor001")
					assert.Equal(t, "mixed_test", result["test_field"])
					assert.Equal(t, "normal", result["normal"])
					assert.Nil(t, result["'normal'"])
				}
			},
		},
		{
			name:        "字符串常量一致性验证",
			sql:         `SELECT 'single_quote' as test1, "double_quote" as test2 FROM stream LIMIT 1`,
			testData:    []map[string]interface{}{{"deviceId": "test001", "deviceType": "test"}},
			expectedLen: 1,
			validator: func(t *testing.T, results []map[string]interface{}) {
				if len(results) > 0 {
					resultMap := results[0]
					assert.Equal(t, "single_quote", resultMap["test1"])
					assert.Equal(t, "double_quote", resultMap["test2"])
				}
			},
		},
	}

	// 执行所有测试用例
	for _, tc := range testCases {
		executeTestCase(t, streamsql, tc)
	}
}

// TestStringConstantExpressions 测试字符串常量表达式
func TestStringConstantExpressions(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 通用测试数据
	testData := []map[string]interface{}{
		{"deviceId": "sensor001", "deviceType": "temperature"},
		{"deviceId": "device002", "deviceType": "humidity"},
		{"deviceId": "sensor003", "deviceType": "pressure"},
	}

	// 字符串常量验证函数
	stringConstantValidator := func(expectedValue string) func(t *testing.T, results []map[string]interface{}) {
		return func(t *testing.T, results []map[string]interface{}) {
			for _, result := range results {
				deviceId := result["deviceId"].(string)
				assert.True(t, deviceId == "sensor001" || deviceId == "sensor003")
				assert.Equal(t, expectedValue, result["test"])
			}
		}
	}

	testCases := []testCase{
		{
			name:        "单引号字符串常量作为表达式字段",
			sql:         "SELECT deviceId, deviceType, 'aa' as test FROM stream WHERE deviceId LIKE 'sensor%'",
			testData:    testData,
			expectedLen: 2,
			validator:   stringConstantValidator("aa"),
		},
		{
			name:        "双引号字符串常量作为表达式字段",
			sql:         `SELECT deviceId, deviceType, "aa" as test FROM stream WHERE deviceId LIKE 'sensor%'`,
			testData:    testData,
			expectedLen: 2,
			validator:   stringConstantValidator("aa"),
		},
	}

	// 执行所有测试用例
	for _, tc := range testCases {
		executeTestCase(t, streamsql, tc)
	}
}

// TestAggregationWithQuotedIdentifiers 测试聚合函数与反引号标识符的结合使用
func TestAggregationWithQuotedIdentifiers(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 聚合测试数据
	aggregationTestData := []map[string]interface{}{
		{"deviceId": "sensor001", "temperature": 25.5},
		{"deviceId": "sensor001", "temperature": 26.0},
		{"deviceId": "sensor002", "temperature": 30.0},
	}

	// 聚合结果验证函数
	aggregationValidator := func(t *testing.T, results []map[string]interface{}) {
		resultSlice := results
		assert.Len(t, resultSlice, 2) // 应该有两个设备的聚合结果

		for _, item := range resultSlice {
			if item["deviceId"] == "sensor001" {
				assert.Equal(t, 25.75, item["avg_temp"]) // (25.5 + 26.0) / 2 = 25.75
				assert.Equal(t, float64(2), item["device_count"])
			} else if item["deviceId"] == "sensor002" {
				assert.Equal(t, 30.0, item["avg_temp"])
				assert.Equal(t, float64(1), item["device_count"])
			}
		}
	}

	testCases := []testCase{
		{
			name:      "聚合函数与字段组合",
			sql:       "SELECT deviceId, AVG(temperature) as avg_temp, COUNT(deviceId) as device_count FROM stream GROUP BY deviceId, TumblingWindow('1s')",
			testData:  aggregationTestData,
			validator: aggregationValidator,
		},
	}

	// 执行所有聚合测试用例
	for _, tc := range testCases {
		executeAggregationTestCase(t, streamsql, tc)
	}
}

// TestCustomFunctionWithQuotedIdentifiers 测试自定义函数与反引号标识符和字符串常量的参数传递
func TestCustomFunctionWithQuotedIdentifiers(t *testing.T) {
	// 注册测试函数
	registerTestFunctions(t)
	defer unregisterTestFunctions()

	streamsql := New()
	defer streamsql.Stop()

	testCases := []testCase{
		{
			name:     "函数参数：字段值vs字符串常量",
			sql:      "SELECT deviceId, func01(temperature) as squared_temp, func02('temperature') as string_length FROM stream WHERE deviceId = 'sensor001'",
			testData: []map[string]interface{}{{"deviceId": "sensor001", "temperature": 5.0}, {"deviceId": "sensor002", "temperature": 10.0}},
			validator: func(t *testing.T, results []map[string]interface{}) {
				resultSlice := results
				assert.Len(t, resultSlice, 1)
				item := resultSlice[0]
				assert.Equal(t, "sensor001", item["deviceId"])
				assert.Equal(t, 25.0, item["squared_temp"]) // func01(5.0) = 25.0
				assert.Equal(t, 11, item["string_length"])  // func02('temperature') = 11
			},
		},
		{
			name:     "反引号标识符作为函数参数",
			sql:      "SELECT deviceId, func01(temperature) as squared_temp, get_type(deviceId) as device_type FROM stream WHERE deviceId = 'sensor001'",
			testData: []map[string]interface{}{{"deviceId": "sensor001", "temperature": 6.0}, {"deviceId": "sensor002", "temperature": 8.0}},
			validator: func(t *testing.T, results []map[string]interface{}) {
				resultSlice := results
				assert.Len(t, resultSlice, 1)
				item := resultSlice[0]
				assert.Equal(t, "sensor001", item["deviceId"])
				assert.Equal(t, 36.0, item["squared_temp"]) // func01(6.0) = 36.0
				assert.Contains(t, item["device_type"], "sensor001")
			},
		},
		{
			name:     "混合使用字段值和字符串常量",
			sql:      `SELECT deviceId, func01(temperature) as field_result, func02("constant_string") as const_result, get_type('literal') as literal_type FROM stream LIMIT 1`,
			testData: []map[string]interface{}{{"deviceId": "test001", "temperature": 7.0}},
			validator: func(t *testing.T, results []map[string]interface{}) {
				resultSlice := results
				assert.Len(t, resultSlice, 1)
				item := resultSlice[0]
				assert.Equal(t, "test001", item["deviceId"])
				assert.Equal(t, 49.0, item["field_result"]) // func01(7.0) = 49.0
				assert.Equal(t, 15, item["const_result"])   // func02("constant_string") = 15
				assert.Contains(t, item["literal_type"], "literal")
			},
		},
	}

	// 执行所有函数测试用例
	for _, tc := range testCases {
		executeFunctionTestCase(t, streamsql, tc)
	}
}

// registerTestFunctions 注册测试用的自定义函数
func registerTestFunctions(t *testing.T) {
	// 注册测试函数：接收字段值并返回其平方
	err := functions.RegisterCustomFunction(
		"func01",
		functions.TypeMath,
		"测试函数",
		"计算数值的平方",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			val := cast.ToFloat64(args[0])
			return val * val, nil
		},
	)
	assert.NoError(t, err)

	// 注册测试函数：接收字符串并返回其长度
	err = functions.RegisterCustomFunction(
		"func02",
		functions.TypeString,
		"测试函数",
		"计算字符串长度",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			str := cast.ToString(args[0])
			return len(str), nil
		},
	)
	assert.NoError(t, err)

	// 注册测试函数：接收参数并返回其类型信息
	err = functions.RegisterCustomFunction(
		"get_type",
		functions.TypeCustom,
		"测试函数",
		"获取参数类型",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			return fmt.Sprintf("%T:%v", args[0], args[0]), nil
		},
	)
	assert.NoError(t, err)
}

// unregisterTestFunctions 注销测试用的自定义函数
func unregisterTestFunctions() {
	functions.Unregister("func01")
	functions.Unregister("func02")
	functions.Unregister("get_type")
}
