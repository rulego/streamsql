package e2e

import (
	"fmt"
	"github.com/rulego/streamsql/utils/cast"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/functions"
	"github.com/stretchr/testify/assert"
)

// This file tests serial execution (without t.Parallel): registering a custom function in the global function registry,
// Duplicate registrations with custom_functions/quoted names and parallel will cause "already registered" conflicts.

// TestPluginStyleCustomFunctions Tests plugin-style custom functions
func TestPluginStyleCustomFunctions(t *testing.T) {

	// Dynamically registering new functions (registering at runtime, no need to modify SQL parsing code)

	// 1. Register a string handler function (should handle it directly, no window needed)
	err := functions.RegisterCustomFunction(
		"mask_phone", // A brand new function name
		functions.TypeString,
		"数据脱敏",
		"手机号脱敏",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			phone := cast.ToString(args[0])
			if len(phone) != 11 {
				return phone, nil
			}
			return phone[:3] + "****" + phone[7:], nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("mask_phone")

	// 2. Register conversion functions (should be handled directly)
	err = functions.RegisterCustomFunction(
		"format_id",
		functions.TypeConversion,
		"格式化",
		"格式化ID",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			id := cast.ToString(args[0])
			return "ID_" + id, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("format_id")

	// 3. Register Math Functions (for window aggregation)
	err = functions.RegisterCustomFunction(
		"calculate_commission",
		functions.TypeMath,
		"业务计算",
		"计算销售佣金",
		2, 2,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			sales := cast.ToFloat64(args[0])
			rate := cast.ToFloat64(args[1])
			return sales * rate / 100, nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("calculate_commission")

	// Test 1: Pure string function (no window required)
	testStringFunctionsOnly(t)

	// Test 2: Conversion function (no window required)
	testConversionFunctionsOnly(t)

	// Test 3: Using Mathematical Functions in Aggregation (Window Required)
	testMathFunctionsInAggregate(t)

}

func testStringFunctionsOnly(t *testing.T) {

	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
		SELECT 
			employee_id,
			mask_phone(phone) as masked_phone
		FROM stream
	`

	err := ssql.Execute(sql)
	assert.NoError(t, err)

	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := map[string]any{
		"employee_id": "E001",
		"phone":       "13812345678",
	}

	ssql.Emit(testData)
	time.Sleep(300 * time.Millisecond)

	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "E001", item["employee_id"])
		assert.Equal(t, "138****5678", item["masked_phone"]) // The desensitized phone number

	case <-time.After(2 * time.Second):
		t.Fatal("String function test timeout")
	}
}

func testConversionFunctionsOnly(t *testing.T) {

	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
		SELECT 
			user_id,
			format_id(user_id) as formatted_id
		FROM stream
	`

	err := ssql.Execute(sql)
	assert.NoError(t, err)

	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := map[string]any{
		"user_id": "12345",
	}

	ssql.Emit(testData)
	time.Sleep(300 * time.Millisecond)

	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "12345", item["user_id"])
		assert.Equal(t, "ID_12345", item["formatted_id"])

		fmt.Printf("  📊 转换函数结果: %v\n", item)
	case <-time.After(2 * time.Second):
		t.Fatal("Conversion function test timeout")
	}
}

func testMathFunctionsInAggregate(t *testing.T) {

	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `
		SELECT 
			department,
			AVG(calculate_commission(sales, commission_rate)) as avg_commission
		FROM stream 
		GROUP BY department, TumblingWindow('1s')
	`

	err := ssql.Execute(sql)
	assert.NoError(t, err)

	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := []map[string]any{
		{
			"department":      "sales",
			"sales":           8000.0,
			"commission_rate": 3.0,
		},
		{
			"department":      "sales",
			"sales":           12000.0,
			"commission_rate": 4.0,
		},
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(1 * time.Second)
	ssql.TriggerWindow()
	time.Sleep(500 * time.Millisecond)

	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "sales", item["department"])

		// Verify aggregated calculation results
		avgCommission, ok := item["avg_commission"].(float64)
		assert.True(t, ok)
		expectedAvg := (8000*3/100 + 12000*4/100) / 2 // (240 + 480) / 2 = 360
		assert.InEpsilon(t, expectedAvg, avgCommission, 0.01)

	case <-time.After(3 * time.Second):
		t.Fatal("Aggregate math function test timed out")
	}
}

// TestRuntimeFunctionManagement Manages the runtime function for testing
func TestRuntimeFunctionManagement(t *testing.T) {
	// Dynamic registration function
	err := functions.RegisterCustomFunction(
		"temp_function",
		functions.TypeString, // String types are used for direct processing
		"临时函数",
		"临时测试函数",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			val := cast.ToString(args[0])
			return "TEMP_" + val, nil
		},
	)
	assert.NoError(t, err)

	// The verification function has been registered
	fn, exists := functions.Get("temp_function")
	assert.True(t, exists)
	assert.Equal(t, "temp_function", fn.GetName())

	// Used in SQL
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `SELECT temp_function(value) as result FROM stream`
	err = ssql.Execute(sql)
	assert.NoError(t, err)

	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	ssql.Emit(map[string]any{"value": "test"})
	time.Sleep(300 * time.Millisecond)

	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)
		assert.Equal(t, "TEMP_test", resultSlice[0]["result"])
	case <-time.After(2 * time.Second):
		t.Fatal("Runtime function management tests timed out")
	}

	// Runtime logout function
	success := functions.Unregister("temp_function")
	assert.True(t, success)

	// The validation function has been logged off
	_, exists = functions.Get("temp_function")
	assert.False(t, exists)
}

// TestFunctionPluginDiscovery Discovers the mechanism of the test function plugin
func TestFunctionPluginDiscovery(t *testing.T) {
	// Register functions of different types
	functions.RegisterCustomFunction("plugin_math", functions.TypeMath, "插件", "数学插件", 1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			return args[0], nil
		})

	functions.RegisterCustomFunction("plugin_string", functions.TypeString, "插件", "字符串插件", 1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			return args[0], nil
		})

	defer functions.Unregister("plugin_math")
	defer functions.Unregister("plugin_string")

	// Testing functions by type
	mathFunctions := functions.GetByType(functions.TypeMath)
	assert.Greater(t, len(mathFunctions), 0)

	// Verification of newly registered functions is discovered
	found := false
	for _, fn := range mathFunctions {
		if fn.GetName() == "plugin_math" {
			found = true
			break
		}
	}
	assert.True(t, found, "新注册的数学函数应该被发现")

	// Testing the full function reveals this
	allFunctions := functions.ListAll()
	assert.Contains(t, allFunctions, "plugin_math")
	assert.Contains(t, allFunctions, "plugin_string")

}

// TestCompleteSQLIntegration tests complete SQL integration
func TestCompleteSQLIntegration(t *testing.T) {
	// Register a completely new business function
	err := functions.RegisterCustomFunction(
		"business_metric",
		functions.TypeString,
		"业务指标",
		"计算业务指标",
		2, 2,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			category := cast.ToString(args[0])
			value := cast.ToFloat64(args[1])

			var multiplier float64
			switch category {
			case "premium":
				multiplier = 1.5
			case "standard":
				multiplier = 1.0
			default:
				multiplier = 0.8
			}

			return fmt.Sprintf("%s:%.2f", category, value*multiplier), nil
		},
	)
	assert.NoError(t, err)
	defer functions.Unregister("business_metric")

	ssql := streamsql.New()
	defer ssql.Stop()

	// Use brand-new functions in SQL
	sql := `
		SELECT 
			customer_id,
			business_metric(tier, amount) as metric
		FROM stream
	`

	err = ssql.Execute(sql)
	assert.NoError(t, err)

	resultChan := make(chan any, 10)
	ssql.Stream().AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	testData := map[string]any{
		"customer_id": "C001",
		"tier":        "premium",
		"amount":      100.0,
	}

	ssql.Emit(testData)
	time.Sleep(300 * time.Millisecond)

	select {
	case result := <-resultChan:
		resultSlice, ok := result.([]map[string]any)
		assert.True(t, ok)
		assert.Len(t, resultSlice, 1)

		item := resultSlice[0]
		assert.Equal(t, "C001", item["customer_id"])
		assert.Equal(t, "premium:150.00", item["metric"])

	case <-time.After(2 * time.Second):
		t.Fatal("Full SQL integration test timeout")
	}
}
