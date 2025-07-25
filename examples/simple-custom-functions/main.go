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

	// 注册一些简单的自定义函数
	registerSimpleFunctions()

	// 演示函数在SQL中的使用
	demonstrateFunctions()

	fmt.Println("\n✅ 演示完成！")
}

// 注册简单的自定义函数
func registerSimpleFunctions() {
	fmt.Println("\n📋 注册自定义函数...")

	// 1. 数学函数：平方
	err := functions.RegisterCustomFunction(
		"square",
		functions.TypeMath,
		"数学函数",
		"计算平方",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			val := cast.ToFloat64(args[0])
			return val * val, nil
		},
	)
	if err != nil {
		fmt.Printf("❌ 注册square函数失败: %v\n", err)
	} else {
		fmt.Println("  ✓ 注册数学函数: square")
	}

	// 2. 华氏度转摄氏度函数
	err = functions.RegisterCustomFunction(
		"f_to_c",
		functions.TypeConversion,
		"温度转换",
		"华氏度转摄氏度",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
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

	// 3. 圆面积计算函数
	err = functions.RegisterCustomFunction(
		"circle_area",
		functions.TypeMath,
		"几何计算",
		"计算圆的面积",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
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

// 演示自定义函数的使用
func demonstrateFunctions() {
	fmt.Println("\n🎯 演示自定义函数在SQL中的使用")
	fmt.Println("================================")

	// 创建StreamSQL实例
	ssql := streamsql.New()
	defer ssql.Stop()

	// 1. 测试简单查询（不使用窗口）
	testSimpleQuery(ssql)

	// 2. 测试聚合查询（使用窗口）
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

	// 添加结果监听器
	ssql.AddSink(func(result interface{}) {
		fmt.Printf("  📊 简单查询结果: %v\n", result)
	})

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{
			"device":      "sensor1",
			"value":       5.0,
			"temperature": 68.0, // 华氏度
			"radius":      3.0,
		},
		map[string]interface{}{
			"device":      "sensor2",
			"value":       10.0,
			"temperature": 86.0, // 华氏度
			"radius":      2.5,
		},
	}

	for _, data := range testData {
		ssql.Emit(data)
		time.Sleep(200 * time.Millisecond) // 稍微延迟
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

	// 添加结果监听器
	ssql.AddSink(func(result interface{}) {
		fmt.Printf("  📊 聚合查询结果: %v\n", result)
	})

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{
			"device":      "sensor1",
			"value":       3.0,
			"temperature": 32.0, // 0°C
			"radius":      1.0,
		},
		map[string]interface{}{
			"device":      "sensor1",
			"value":       4.0,
			"temperature": 212.0, // 100°C
			"radius":      2.0,
		},
		map[string]interface{}{
			"device":      "sensor2",
			"value":       5.0,
			"temperature": 68.0, // 20°C
			"radius":      1.5,
		},
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	// 等待窗口触发
	time.Sleep(1 * time.Second)
	ssql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)

	fmt.Println("  ✅ 聚合查询测试完成")

	// 展示函数管理功能
	showFunctionManagement()
}

func showFunctionManagement() {
	fmt.Println("\n🔧 函数管理功能演示")
	fmt.Println("==================")

	// 列出所有数学函数
	fmt.Println("\n📊 数学函数:")
	mathFunctions := functions.GetByType(functions.TypeMath)
	for _, fn := range mathFunctions {
		fmt.Printf("  • %s - %s\n", fn.GetName(), fn.GetDescription())
	}

	// 列出所有字符串函数
	fmt.Println("\n📝 字符串函数:")
	stringFunctions := functions.GetByType(functions.TypeString)
	for _, fn := range stringFunctions {
		fmt.Printf("  • %s - %s\n", fn.GetName(), fn.GetDescription())
	}

	// 检查特定函数是否存在
	fmt.Println("\n🔍 函数查找:")
	if fn, exists := functions.Get("square"); exists {
		fmt.Printf("  ✓ 找到函数: %s (%s)\n", fn.GetName(), fn.GetDescription())
	}

	if fn, exists := functions.Get("f_to_c"); exists {
		fmt.Printf("  ✓ 找到函数: %s (%s)\n", fn.GetName(), fn.GetDescription())
	}

	// 统计函数数量
	allFunctions := functions.ListAll()
	fmt.Printf("\n📈 统计信息:\n")
	fmt.Printf("  • 总函数数量: %d\n", len(allFunctions))

	// 按类型统计
	typeCount := make(map[functions.FunctionType]int)
	for _, fn := range allFunctions {
		typeCount[fn.GetType()]++
	}

	for fnType, count := range typeCount {
		fmt.Printf("  • %s: %d个\n", fnType, count)
	}
}
