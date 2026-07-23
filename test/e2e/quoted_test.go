package e2e

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/stretchr/testify/assert"
)

// testCase defines the test case structure
type testCase struct {
	name        string
	sql         string
	testData    []map[string]any
	expectedLen int
	validator   func(t *testing.T, results []map[string]any)
}

// executeTestCase executes the general logic for a single test case
func executeTestCase(t *testing.T, ssql *streamsql.Streamsql, tc testCase) {
	t.Run(tc.name, func(t *testing.T) {
		// Create a new StreamSQL instance for each test case
		ssql := streamsql.New()
		defer ssql.Stop()

		err := ssql.Execute(tc.sql)
		assert.Nil(t, err)
		strm := ssql.Stream()

		// Create result receiving channels and mutex protection for concurrent access
		resultChan := make(chan any, 10)
		var results []map[string]any
		var resultsMutex sync.Mutex

		strm.AddSink(func(result []map[string]any) {
			select {
			case resultChan <- result:
			default:
				// Discard results when the channel is full, avoiding blockage
			}
		})

		// Add test data
		for _, data := range tc.testData {
			strm.Emit(data)
		}

		// Waiting for data processing
		time.Sleep(200 * time.Millisecond)

		// Collect all the results
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
				if resultSlice, ok := result.([]map[string]any); ok {
					results = append(results, resultSlice...)
				} else if resultMap, ok := result.(map[string]any); ok {
					results = append(results, resultMap)
				}
				resultsMutex.Unlock()
			case <-timeout:
				goto checkResults
			}
		}

	checkResults:
		// Verification result length (using mutex protection)
		resultsMutex.Lock()
		finalResults := make([]map[string]any, len(results))
		copy(finalResults, results)
		resultsMutex.Unlock()

		assert.Equal(t, tc.expectedLen, len(finalResults))
		// Perform custom validation
		if tc.validator != nil {
			tc.validator(t, finalResults)
		}
	})
}

// executeAggregationTestCase executes the general logic for aggregation function test cases
func executeAggregationTestCase(t *testing.T, ssql *streamsql.Streamsql, tc testCase) {
	t.Run(tc.name, func(t *testing.T) {
		// Create a new StreamSQL instance for each test case
		ssql := streamsql.New()
		defer ssql.Stop()

		err := ssql.Execute(tc.sql)
		assert.Nil(t, err)
		strm := ssql.Stream()

		// Create a result receiving channel
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			select {
			case resultChan <- result:
			default:
				// Discard results when the channel is full, avoiding blockage
			}
		})

		// Add test data
		for _, data := range tc.testData {
			strm.Emit(data)
		}

		// Wait for the window to trigger
		time.Sleep(1 * time.Second)
		strm.Window.Trigger()
		time.Sleep(500 * time.Millisecond)

		// Verify the results
		select {
		case result := <-resultChan:
			if tc.validator != nil {
				tc.validator(t, result.([]map[string]any))
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Test timeout")
		}
	})
}

// executeFunctionTestCase executes the general logic of the function test case
func executeFunctionTestCase(t *testing.T, ssql *streamsql.Streamsql, tc testCase) {
	t.Run(tc.name, func(t *testing.T) {
		// Create a new StreamSQL instance for each test case
		ssql := streamsql.New()
		defer ssql.Stop()

		err := ssql.Execute(tc.sql)
		assert.Nil(t, err)
		strm := ssql.Stream()

		// Create a result receiving channel
		resultChan := make(chan any, 10)
		strm.AddSink(func(result []map[string]any) {
			select {
			case resultChan <- result:
			default:
				// Discard results when the channel is full, avoiding blockage
			}
		})

		// Add test data
		for _, data := range tc.testData {
			strm.Emit(data)
		}

		time.Sleep(200 * time.Millisecond)

		// Verify the results
		select {
		case result := <-resultChan:
			if tc.validator != nil {
				tc.validator(t, result.([]map[string]any))
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Test timeout")
		}
	})
}

// This file tests serial execution (without t.Parallel): registers custom functions (func01/func02/get_type, etc.) with the global function registry,
// There are duplicate registrations with custom_functions and others, and parallelism will cause a "already registered" conflict.

// TestQuotedIdentifiersAndStringLiterals tests backquoted identifiers and string constant support
func TestQuotedIdentifiersAndStringLiterals(t *testing.T) {
	// Register test functions (because there are test cases using custom functions)
	registerTestFunctions(t)
	defer unregisterTestFunctions()

	ssql := streamsql.New()
	defer ssql.Stop()

	// General test data
	standardTestData := []map[string]any{
		{"deviceId": "sensor001", "deviceType": "temperature"},
		{"deviceId": "device002", "deviceType": "humidity"},
		{"deviceId": "sensor003", "deviceType": "pressure"},
	}

	// Define test cases
	testCases := []testCase{
		{
			name:        "反引号标识符支持",
			sql:         "SELECT `deviceId`, `deviceType` FROM stream WHERE `deviceId` LIKE 'sensor%'",
			testData:    standardTestData,
			expectedLen: 2,
			validator: func(t *testing.T, results []map[string]any) {
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
			validator: func(t *testing.T, results []map[string]any) {
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
			validator: func(t *testing.T, results []map[string]any) {
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
			validator: func(t *testing.T, results []map[string]any) {
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
			testData:    []map[string]any{{"deviceId": "test001", "deviceType": "test"}},
			expectedLen: 1,
			validator: func(t *testing.T, results []map[string]any) {
				if len(results) > 0 {
					resultMap := results[0]
					assert.Equal(t, "single_quote", resultMap["test1"])
					assert.Equal(t, "double_quote", resultMap["test2"])
				}
			},
		},
	}

	// Execute all test cases
	for _, tc := range testCases {
		executeTestCase(t, ssql, tc)
	}
}

// TestStringConstantExpressions tests string constant expressions
func TestStringConstantExpressions(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()

	// General test data
	testData := []map[string]any{
		{"deviceId": "sensor001", "deviceType": "temperature"},
		{"deviceId": "device002", "deviceType": "humidity"},
		{"deviceId": "sensor003", "deviceType": "pressure"},
	}

	// String constant verification function
	stringConstantValidator := func(expectedValue string) func(t *testing.T, results []map[string]any) {
		return func(t *testing.T, results []map[string]any) {
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

	// Execute all test cases
	for _, tc := range testCases {
		executeTestCase(t, ssql, tc)
	}
}

// TestAggregationWithQuotedIdentifiers tests the combination of aggregation functions and backquoted identifiers
func TestAggregationWithQuotedIdentifiers(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()

	// Aggregate test data
	aggregationTestData := []map[string]any{
		{"deviceId": "sensor001", "temperature": 25.5},
		{"deviceId": "sensor001", "temperature": 26.0},
		{"deviceId": "sensor002", "temperature": 30.0},
	}

	// Aggregate result verification function
	aggregationValidator := func(t *testing.T, results []map[string]any) {
		resultSlice := results
		assert.Len(t, resultSlice, 2) // There should be an aggregated result of two devices

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

	// Execute all aggregated test cases
	for _, tc := range testCases {
		executeAggregationTestCase(t, ssql, tc)
	}
}

// TestCustomFunctionWithQuotedIdentifiers Tests the parameter passing between the custom function and the backquote identifier and string constants
func TestCustomFunctionWithQuotedIdentifiers(t *testing.T) {
	// Register the test function
	registerTestFunctions(t)
	defer unregisterTestFunctions()

	ssql := streamsql.New()
	defer ssql.Stop()

	testCases := []testCase{
		{
			name:     "函数参数：字段值vs字符串常量",
			sql:      "SELECT deviceId, func01(temperature) as squared_temp, func02('temperature') as string_length FROM stream WHERE deviceId = 'sensor001'",
			testData: []map[string]any{{"deviceId": "sensor001", "temperature": 5.0}, {"deviceId": "sensor002", "temperature": 10.0}},
			validator: func(t *testing.T, results []map[string]any) {
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
			testData: []map[string]any{{"deviceId": "sensor001", "temperature": 6.0}, {"deviceId": "sensor002", "temperature": 8.0}},
			validator: func(t *testing.T, results []map[string]any) {
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
			testData: []map[string]any{{"deviceId": "test001", "temperature": 7.0}},
			validator: func(t *testing.T, results []map[string]any) {
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

	// Execute all function test cases
	for _, tc := range testCases {
		executeFunctionTestCase(t, ssql, tc)
	}
}

// registerTestFunctions: A custom function used for registering tests
func registerTestFunctions(t *testing.T) {
	// Register test function: Receives field values and returns their square
	err := functions.RegisterCustomFunction(
		"func01",
		functions.TypeMath,
		"测试函数",
		"计算数值的平方",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			val := cast.ToFloat64(args[0])
			return val * val, nil
		},
	)
	assert.NoError(t, err)

	// Register test function: Receive the string and return its length
	err = functions.RegisterCustomFunction(
		"func02",
		functions.TypeString,
		"测试函数",
		"计算字符串长度",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			str := cast.ToString(args[0])
			return len(str), nil
		},
	)
	assert.NoError(t, err)

	// Register test function: Receives parameters and returns their type information
	err = functions.RegisterCustomFunction(
		"get_type",
		functions.TypeCustom,
		"测试函数",
		"获取参数类型",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			return fmt.Sprintf("%T:%v", args[0], args[0]), nil
		},
	)
	assert.NoError(t, err)
}

// unregisterTestFunctions: A custom function used to log out of testing
func unregisterTestFunctions() {
	functions.Unregister("func01")
	functions.Unregister("func02")
	functions.Unregister("get_type")
}
