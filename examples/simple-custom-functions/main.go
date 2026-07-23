package main

import (
	"fmt"
	"github.com/rulego/streamsql/utils/cast"
	"math"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/functions"
)

func main() {
	fmt.Println("🚀 StreamSQL 简单自定义函数演示")
	fmt.Println("=================================")

	// Register some simple custom functions
	registerSimpleFunctions()

	// Demonstrating the use of functions in SQL
	demonstrateFunctions()

	fmt.Println("\n✅ 演示完成！")
}

// Register simple custom functions
func registerSimpleFunctions() {
	fmt.Println("\n📋 注册自定义函数...")

	// 1. Mathematical function: square
	err := functions.RegisterCustomFunction(
		"square",
		functions.TypeMath,
		"数学函数",
		"计算平方",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			val := cast.ToFloat64(args[0])
			return val * val, nil
		},
	)
	if err != nil {
		fmt.Printf("❌ 注册square函数失败: %v\n", err)
	} else {
		fmt.Println("  ✓ 注册数学函数: square")
	}

	// 2. Fahrenheit to Celsius function
	err = functions.RegisterCustomFunction(
		"f_to_c",
		functions.TypeConversion,
		"温度转换",
		"华氏度转摄氏度",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			fahrenheit := cast.ToFloat64(args[0])
			celsius := (fahrenheit - 32) * 5 / 9
			return celsius, nil
		},
	)
	if err != nil {
		fmt.Printf("❌ 注册f_to_c函数失败: %v\n", err)
	} else {
		fmt.Println("  ✓ 注册转换函数: f_to_c")
	}

	// 3. Circle Area Calculation Function
	err = functions.RegisterCustomFunction(
		"circle_area",
		functions.TypeMath,
		"几何计算",
		"计算圆的面积",
		1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			radius := cast.ToFloat64(args[0])
			if radius < 0 {
				return nil, fmt.Errorf("半径必须为正数")
			}
			area := math.Pi * radius * radius
			return area, nil
		},
	)
	if err != nil {
		fmt.Printf("❌ 注册circle_area函数失败: %v\n", err)
	} else {
		fmt.Println("  ✓ 注册几何函数: circle_area")
	}
}

// Demonstrate the use of custom functions
func demonstrateFunctions() {
	fmt.Println("\n🎯 演示自定义函数在SQL中的使用")
	fmt.Println("================================")

	// Create a StreamSQL instance
	ssql := streamsql.New()
	defer ssql.Stop()

	// 1. Test simple queries (without using windows)
	testSimpleQuery(ssql)

	// 2. Test aggregation queries (using the window)
	testAggregateQuery(ssql)
}

func testSimpleQuery(ssql *streamsql.Streamsql) {
	fmt.Println("\n📝 测试简单查询...")

	sql := `
		SELECT 
			device,
			square(value) as squared_value,
			f_to_c(temperature) as celsius,
			circle_area(radius) as area
		FROM stream
	`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// Add a result listener
	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 简单查询结果: %v\n", result)
	})

	// Add test data
	testData := []map[string]any{
		{
			"device":      "sensor1",
			"value":       5.0,
			"temperature": 68.0, // The degree of the Fahrenheit degree
			"radius":      3.0,
		},
		{
			"device":      "sensor2",
			"value":       10.0,
			"temperature": 86.0, // The degree of the Fahrenheit degree
			"radius":      2.5,
		},
	}

	for _, data := range testData {
		ssql.Emit(data)
		time.Sleep(200 * time.Millisecond) // A slight delay
	}

	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 简单查询测试完成")
}

func testAggregateQuery(ssql *streamsql.Streamsql) {
	fmt.Println("\n📈 测试聚合查询...")

	sql := `
		SELECT 
			device,
			AVG(square(value)) as avg_squared,
			AVG(f_to_c(temperature)) as avg_celsius,
			MAX(circle_area(radius)) as max_area
		FROM stream 
		GROUP BY device, TumblingWindow('1s')
	`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// Add a result listener
	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 聚合查询结果: %v\n", result)
	})

	// Add test data
	testData := []map[string]any{
		{
			"device":      "sensor1",
			"value":       3.0,
			"temperature": 32.0, // 0°C
			"radius":      1.0,
		},
		{
			"device":      "sensor1",
			"value":       4.0,
			"temperature": 212.0, // 100°C
			"radius":      2.0,
		},
		{
			"device":      "sensor2",
			"value":       5.0,
			"temperature": 68.0, // 20°C
			"radius":      1.5,
		},
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	// Wait for the window to trigger
	time.Sleep(1 * time.Second)
	ssql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)

	fmt.Println("  ✅ 聚合查询测试完成")

	// Showcase function management features
	showFunctionManagement()
}

func showFunctionManagement() {
	fmt.Println("\n🔧 函数管理功能演示")
	fmt.Println("==================")

	// List all mathematical functions
	fmt.Println("\n📊 数学函数:")
	mathFunctions := functions.GetByType(functions.TypeMath)
	for _, fn := range mathFunctions {
		fmt.Printf("  • %s - %s\n", fn.GetName(), fn.GetDescription())
	}

	// List all string functions
	fmt.Println("\n📝 字符串函数:")
	stringFunctions := functions.GetByType(functions.TypeString)
	for _, fn := range stringFunctions {
		fmt.Printf("  • %s - %s\n", fn.GetName(), fn.GetDescription())
	}

	// Check whether a specific function exists
	fmt.Println("\n🔍 函数查找:")
	if fn, exists := functions.Get("square"); exists {
		fmt.Printf("  ✓ 找到函数: %s (%s)\n", fn.GetName(), fn.GetDescription())
	}

	if fn, exists := functions.Get("f_to_c"); exists {
		fmt.Printf("  ✓ 找到函数: %s (%s)\n", fn.GetName(), fn.GetDescription())
	}

	// Count the number of functions
	allFunctions := functions.ListAll()
	fmt.Printf("\n📈 统计信息:\n")
	fmt.Printf("  • 总函数数量: %d\n", len(allFunctions))

	// Statistics by type
	typeCount := make(map[functions.FunctionType]int)
	for _, fn := range allFunctions {
		typeCount[fn.GetType()]++
	}

	for fnType, count := range typeCount {
		fmt.Printf("  • %s: %d个\n", fnType, count)
	}
}
