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

// 本文件测试串行执行（不加 t.Parallel）：向全局 function registry 注册自定义函数，
// 与 custom_functions/quoted 等存在重名注册，并行会导致 "already registered" 冲突。

// TestPluginStyleCustomFunctions 测试插件式自定义函数
func TestPluginStyleCustomFunctions(t *testing.T) {

	// 动态注册新函数（运行时注册，无需修改SQL解析代码）

	// 1. 注册字符串处理函数（应该直接处理，不需要窗口）
	err := functions.RegisterCustomFunction(
		"mask_phone", // 全新的函数名
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

	// 2. 注册转换函数（应该直接处理）
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

	// 3. 注册数学函数（用于窗口聚合）
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

	// 测试1：纯字符串函数（不需要窗口）
	testStringFunctionsOnly(t)

	// 测试2：转换函数（不需要窗口）
	testConversionFunctionsOnly(t)

	// 测试3：数学函数在聚合中使用（需要窗口）
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

	// 添加测试数据
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
		assert.Equal(t, "138****5678", item["masked_phone"]) // 脱敏后的手机号

	case <-time.After(2 * time.Second):
		t.Fatal("字符串函数测试超时")
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

	// 添加测试数据
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
		t.Fatal("转换函数测试超时")
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

	// 添加测试数据
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

		// 验证聚合计算结果
		avgCommission, ok := item["avg_commission"].(float64)
		assert.True(t, ok)
		expectedAvg := (8000*3/100 + 12000*4/100) / 2 // (240 + 480) / 2 = 360
		assert.InEpsilon(t, expectedAvg, avgCommission, 0.01)

	case <-time.After(3 * time.Second):
		t.Fatal("聚合数学函数测试超时")
	}
}

// TestRuntimeFunctionManagement 测试运行时函数管理
func TestRuntimeFunctionManagement(t *testing.T) {
	// 动态注册函数
	err := functions.RegisterCustomFunction(
		"temp_function",
		functions.TypeString, // 使用字符串类型以便直接处理
		"临时函数",
		"临时测试函数",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			val := cast.ToString(args[0])
			return "TEMP_" + val, nil
		},
	)
	assert.NoError(t, err)

	// 验证函数已注册
	fn, exists := functions.Get("temp_function")
	assert.True(t, exists)
	assert.Equal(t, "temp_function", fn.GetName())

	// 在SQL中使用
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
		t.Fatal("运行时函数管理测试超时")
	}

	// 运行时注销函数
	success := functions.Unregister("temp_function")
	assert.True(t, success)

	// 验证函数已注销
	_, exists = functions.Get("temp_function")
	assert.False(t, exists)
}

// TestFunctionPluginDiscovery 测试函数插件发现机制
func TestFunctionPluginDiscovery(t *testing.T) {
	// 注册不同类型的函数
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

	// 测试按类型发现函数
	mathFunctions := functions.GetByType(functions.TypeMath)
	assert.Greater(t, len(mathFunctions), 0)

	// 验证新注册的函数被发现
	found := false
	for _, fn := range mathFunctions {
		if fn.GetName() == "plugin_math" {
			found = true
			break
		}
	}
	assert.True(t, found, "新注册的数学函数应该被发现")

	// 测试全量函数发现
	allFunctions := functions.ListAll()
	assert.Contains(t, allFunctions, "plugin_math")
	assert.Contains(t, allFunctions, "plugin_string")

}

// TestCompleteSQLIntegration 测试完整的SQL集成
func TestCompleteSQLIntegration(t *testing.T) {
	// 注册完全新的业务函数
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

	// 使用全新的函数在SQL中
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
		t.Fatal("完整SQL集成测试超时")
	}
}
