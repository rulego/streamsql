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

	// 注册自定义函数
	registerCustomFunctions()

	// 运行各种测试场景
	runAllTests()

	fmt.Println("\n✅ 所有测试完成！")
}

// 注册自定义函数
func registerCustomFunctions() {
	fmt.Println("\n📋 注册自定义函数...")

	// 数学函数：平方
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

	// 华氏度转摄氏度函数
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

	// 圆面积计算函数
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

// 运行所有测试
func runAllTests() {
	// 测试1：基础数据过滤
	testBasicFiltering()

	// 测试2：聚合分析
	testAggregation()

	// 测试3：滑动窗口
	testSlidingWindow()

	// 测试4：嵌套字段访问
	testNestedFields()

	// 测试5：自定义函数
	testCustomFunctions()

	// 测试6：复杂查询
	testComplexQuery()
}

// 测试1：基础数据过滤
func testBasicFiltering() {
	fmt.Println("\n🔍 测试1：基础数据过滤")
	fmt.Println("========================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 过滤温度大于25度的数据
	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 25"

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// 添加结果处理函数
	ssql.AddSink(func(result []map[string]interface{}) {
		fmt.Printf("  📊 高温告警: %v\n", result)
	})

	// 发送测试数据
	testData := []map[string]interface{}{
		{"deviceId": "sensor001", "temperature": 23.5}, // 不会触发告警
		{"deviceId": "sensor002", "temperature": 28.3}, // 会触发告警
		{"deviceId": "sensor003", "temperature": 31.2}, // 会触发告警
		{"deviceId": "sensor004", "temperature": 22.1}, // 不会触发告警
	}

	for _, data := range testData {
		ssql.Emit(data)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 基础过滤测试完成")
}

// 测试2：聚合分析
func testAggregation() {
	fmt.Println("\n📈 测试2：聚合分析")
	fmt.Println("==================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 每2秒计算一次各设备的平均温度
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

	// 处理聚合结果
	ssql.AddSink(func(result []map[string]interface{}) {
		fmt.Printf("  📊 聚合结果: %v\n", result)
	})

	// 模拟传感器数据流
	devices := []string{"sensor001", "sensor002", "sensor003"}
	for i := 0; i < 8; i++ {
		for _, device := range devices {
			data := map[string]interface{}{
				"deviceId":    device,
				"temperature": 20.0 + rand.Float64()*15, // 20-35度随机温度
				"timestamp":   time.Now(),
			}
			ssql.Emit(data)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// 等待窗口触发
	time.Sleep(2 * time.Second)
	ssql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 聚合分析测试完成")
}

// 测试3：滑动窗口
func testSlidingWindow() {
	fmt.Println("\n🔄 测试3：滑动窗口")
	fmt.Println("==================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 6秒滑动窗口，每2秒滑动一次
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

	ssql.AddSink(func(result []map[string]interface{}) {
		fmt.Printf("  📊 滑动窗口分析: %v\n", result)
	})

	// 持续发送数据
	for i := 0; i < 10; i++ {
		data := map[string]interface{}{
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

// 测试4：嵌套字段访问
func testNestedFields() {
	fmt.Println("\n🔧 测试4：嵌套字段访问")
	fmt.Println("=======================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 访问嵌套字段的SQL查询
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

	ssql.AddSink(func(result []map[string]interface{}) {
		fmt.Printf("  📊 嵌套字段结果: %v\n", result)
	})

	// 发送嵌套结构数据
	complexData := []map[string]interface{}{
		{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"name":   "温度传感器001",
					"type":   "temperature",
					"status": "active",
				},
				"location": map[string]interface{}{
					"building": "A栋",
					"floor":    "3F",
				},
			},
			"sensor": map[string]interface{}{
				"temperature": 28.5,
				"humidity":    65.0,
			},
		},
		{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"name":   "湿度传感器002",
					"type":   "humidity",
					"status": "inactive", // 不会匹配
				},
				"location": map[string]interface{}{
					"building": "B栋",
					"floor":    "2F",
				},
			},
			"sensor": map[string]interface{}{
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

// 测试5：自定义函数
func testCustomFunctions() {
	fmt.Println("\n🎯 测试5：自定义函数")
	fmt.Println("====================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 使用自定义函数的SQL查询
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

	ssql.AddSink(func(result []map[string]interface{}) {
		fmt.Printf("  📊 自定义函数结果: %v\n", result)
	})

	// 添加测试数据
	testData := []map[string]interface{}{
		{
			"device":      "sensor1",
			"value":       5.0,
			"temperature": 68.0, // 华氏度
			"radius":      3.0,
		},
		{
			"device":      "sensor2",
			"value":       10.0,
			"temperature": 86.0, // 华氏度
			"radius":      2.5,
		},
		{
			"device":      "sensor3",
			"value":       0.0, // 不会匹配WHERE条件
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

// 测试6：复杂查询
func testComplexQuery() {
	fmt.Println("\n🔬 测试6：复杂查询")
	fmt.Println("==================")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 复杂的聚合查询，结合自定义函数和嵌套字段
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

	ssql.AddSink(func(result []map[string]interface{}) {
		fmt.Printf("  📊 复杂查询结果: %v\n", result)
	})

	// 发送复杂测试数据
	locations := []string{"room-A", "room-B", "room-C"}
	for i := 0; i < 12; i++ {
		location := locations[i%len(locations)]
		data := map[string]interface{}{
			"device": map[string]interface{}{
				"location": location,
				"status":   "online",
				"radius":   1.0 + rand.Float64()*2.0, // 1-3的随机半径
			},
			"sensor": map[string]interface{}{
				"temperature": 25.0 + rand.Float64()*10.0, // 25-35度
				"humidity":    50.0 + rand.Float64()*30.0, // 50-80%
			},
			"timestamp": time.Now(),
		}
		ssql.Emit(data)
		time.Sleep(300 * time.Millisecond)
	}

	// 等待窗口触发
	time.Sleep(3 * time.Second)
	ssql.Stream().Window.Trigger()
	time.Sleep(500 * time.Millisecond)
	fmt.Println("  ✅ 复杂查询测试完成")
}
