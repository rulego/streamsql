package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/cast"
)

func main() {
	fmt.Println("🚀 StreamSQL 综合测试演示")
	fmt.Println("=============================")

	// Register custom functions
	registerCustomFunctions()

	// Run various test scenarios
	runAllTests()

	fmt.Println("\n✅ 所有测试完成！")
}

// Register custom functions
func registerCustomFunctions() {
	fmt.Println("\n📋 注册自定义函数...")

	// Mathematical function: square
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

	// Fahrenheit to Celsius function
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

	// Circle area calculation function
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

// Run all tests
func runAllTests() {
	// Test 1: Basic data filtering
	testBasicFiltering()

	// Test 2: Aggregate analysis
	testAggregation()

	// Test 3: Sliding windows
	testSlidingWindow()

	// Test 4: Nested field access
	testNestedFields()

	// Test 5: Custom functions
	testCustomFunctions()

	// Test 6: Complex queries
	testComplexQuery()
}

// Test 1: Basic data filtering
func testBasicFiltering() {
	fmt.Println("\n🔍 测试1：基础数据过滤")
	fmt.Println("========================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// Filter data with temperatures above 25 degrees
	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 25"

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// Add a result processing function
	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 高温告警: %v\n", result)
	})

	// Send test data
	testData := []map[string]any{
		{"deviceId": "sensor001", "temperature": 23.5}, // No alarms will be triggered
		{"deviceId": "sensor002", "temperature": 28.3}, // An alarm will be triggered
		{"deviceId": "sensor003", "temperature": 31.2}, // An alarm will be triggered
		{"deviceId": "sensor004", "temperature": 22.1}, // No alarms will be triggered
	}

	for _, data := range testData {
		ssql.Emit(data)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 基础过滤测试完成")
}

// Test 2: Aggregate analysis
func testAggregation() {
	fmt.Println("\n📈 测试2：聚合分析")
	fmt.Println("==================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// Calculate the average temperature of each device every 2 seconds
	sql := `SELECT deviceId, 
                   AVG(temperature) as avg_temp,
                   COUNT(*) as sample_count,
                   MAX(temperature) as max_temp,
                   MIN(temperature) as min_temp
            FROM stream 
            GROUP BY deviceId, TumblingWindow('2s')`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// Process the aggregated results
	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 聚合结果: %v\n", result)
	})

	// Simulating sensor data streams
	devices := []string{"sensor001", "sensor002", "sensor003"}
	for i := 0; i < 8; i++ {
		for _, device := range devices {
			data := map[string]any{
				"deviceId":    device,
				"temperature": 20.0 + rand.Float64()*15, // Random temperature of 20-35 degrees
				"timestamp":   time.Now(),
			}
			ssql.Emit(data)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Wait for the window to trigger
	time.Sleep(2 * time.Second)
	ssql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 聚合分析测试完成")
}

// Test 3: Sliding windows
func testSlidingWindow() {
	fmt.Println("\n🔄 测试3：滑动窗口")
	fmt.Println("==================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// Swipe the window every 6 seconds, then every 2 seconds
	sql := `SELECT deviceId,
                   AVG(temperature) as avg_temp,
                   MAX(temperature) as max_temp,
                   MIN(temperature) as min_temp,
                   COUNT(*) as count
            FROM stream 
            WHERE temperature > 0
            GROUP BY deviceId, SlidingWindow('6s', '2s')`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 滑动窗口分析: %v\n", result)
	})

	// Continuously transmitting data
	for i := 0; i < 10; i++ {
		data := map[string]any{
			"deviceId":    "sensor001",
			"temperature": 20.0 + rand.Float64()*10,
			"timestamp":   time.Now(),
		}
		ssql.Emit(data)
		time.Sleep(800 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)
	fmt.Println("  ✅ 滑动窗口测试完成")
}

// Test 4: Nested field access
func testNestedFields() {
	fmt.Println("\n🔧 测试4：嵌套字段访问")
	fmt.Println("=======================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// SQL queries accessing nested fields
	sql := `SELECT device.info.name as device_name,
                   device.location.building as building,
                   sensor.temperature as temp,
                   UPPER(device.info.type) as device_type
            FROM stream 
            WHERE sensor.temperature > 25 AND device.info.status = 'active'`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 嵌套字段结果: %v\n", result)
	})

	// Send nested structure data
	complexData := []map[string]any{
		{
			"device": map[string]any{
				"info": map[string]any{
					"name":   "温度传感器001",
					"type":   "temperature",
					"status": "active",
				},
				"location": map[string]any{
					"building": "A栋",
					"floor":    "3F",
				},
			},
			"sensor": map[string]any{
				"temperature": 28.5,
				"humidity":    65.0,
			},
		},
		{
			"device": map[string]any{
				"info": map[string]any{
					"name":   "湿度传感器002",
					"type":   "humidity",
					"status": "inactive", // It won't match
				},
				"location": map[string]any{
					"building": "B栋",
					"floor":    "2F",
				},
			},
			"sensor": map[string]any{
				"temperature": 30.0,
				"humidity":    70.0,
			},
		},
	}

	for _, data := range complexData {
		ssql.Emit(data)
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 嵌套字段测试完成")
}

// Test 5: Custom functions
func testCustomFunctions() {
	fmt.Println("\n🎯 测试5：自定义函数")
	fmt.Println("====================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// SQL queries using custom functions
	sql := `SELECT 
				device,
				square(value) as squared_value,
				f_to_c(temperature) as celsius,
				circle_area(radius) as area
			FROM stream
			WHERE value > 0`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 自定义函数结果: %v\n", result)
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
		{
			"device":      "sensor3",
			"value":       0.0, // It will not match the WHERE condition
			"temperature": 32.0,
			"radius":      1.0,
		},
	}

	for _, data := range testData {
		ssql.Emit(data)
		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 自定义函数测试完成")
}

// Test 6: Complex queries
func testComplexQuery() {
	fmt.Println("\n🔬 测试6：复杂查询")
	fmt.Println("==================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// Complex aggregated queries, combined with custom functions and nested fields
	sql := `SELECT 
				device.location as location,
				AVG(square(sensor.temperature)) as avg_temp_squared,
				MAX(f_to_c(sensor.temperature)) as max_celsius,
				COUNT(*) as sample_count,
				SUM(circle_area(device.radius)) as total_area
			FROM stream 
			WHERE sensor.temperature > 20 AND device.status = 'online'
			GROUP BY device.location, TumblingWindow('3s')`

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  📊 复杂查询结果: %v\n", result)
	})

	// Send complex test data
	locations := []string{"room-A", "room-B", "room-C"}
	for i := 0; i < 12; i++ {
		location := locations[i%len(locations)]
		data := map[string]any{
			"device": map[string]any{
				"location": location,
				"status":   "online",
				"radius":   1.0 + rand.Float64()*2.0, // Random radius of 1-3
			},
			"sensor": map[string]any{
				"temperature": 25.0 + rand.Float64()*10.0, // 25-35 degrees
				"humidity":    50.0 + rand.Float64()*30.0, // 50-80%
			},
			"timestamp": time.Now(),
		}
		ssql.Emit(data)
		time.Sleep(300 * time.Millisecond)
	}

	// Wait for the window to trigger
	time.Sleep(3 * time.Second)
	ssql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 复杂查询测试完成")
}
