package main

import (
	"fmt"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/cast"
)

func main() {
	fmt.Println("=== StreamSQL 高级函数示例 ===")

	// 1. 注册自定义函数：温度华氏度转摄氏度
	err := functions.RegisterCustomFunction("fahrenheit_to_celsius", functions.TypeCustom, "温度转换", "华氏度转摄氏度", 1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			fahrenheit, err := cast.ToFloat64E(args[0])
			if err != nil {
				return nil, err
			}
			celsius := (fahrenheit - 32) * 5 / 9
			return celsius, nil
		})
	if err != nil {
		panic(fmt.Sprintf("注册自定义函数失败: %v", err))
	}
	fmt.Println("✓ 注册自定义函数：fahrenheit_to_celsius")

	// 2. 创建 StreamSQL 实例
	ssql := streamsql.New()
	defer ssql.Stop()

	// 3. 定义包含高级函数的 SQL
	sql := `
		SELECT 
			device,
			AVG(abs(temperature - 20)) as avg_deviation,
			AVG(fahrenheit_to_celsius(temperature)) as avg_celsius,
			MAX(sqrt(humidity)) as max_sqrt_humidity
		FROM stream 
		GROUP BY device, TumblingWindow('2s') 
		WITH (TIMESTAMP='ts', TIMEUNIT='ss')
	`

	// 4. 执行 SQL
	err = ssql.Execute(sql)
	if err != nil {
		panic(fmt.Sprintf("执行SQL失败: %v", err))
	}
	fmt.Println("✓ SQL执行成功")

	// 5. 添加结果监听器
	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("📊 聚合结果: %v\n", result)
	})

	// 6. 模拟传感器数据
	baseTime := time.Now()
	sensorData := []map[string]any{
		{"device": "sensor1", "temperature": 68.0, "humidity": 25.0, "ts": baseTime.UnixMicro()},       // 20°C, 湿度25%
		{"device": "sensor1", "temperature": 86.0, "humidity": 36.0, "ts": baseTime.Unix()},            // 30°C, 湿度36%
		{"device": "sensor2", "temperature": 32.0, "humidity": 49.0, "ts": baseTime.Unix()},            // 0°C, 湿度49%
		{"device": "sensor2", "temperature": 104.0, "humidity": 64.0, "ts": baseTime.Unix()},           // 40°C, 湿度64%
		{"device": "temperature_probe", "temperature": 212.0, "humidity": 81.0, "ts": baseTime.Unix()}, // 100°C, 湿度81%
	}

	fmt.Println("\n🌡️ 发送传感器数据:")
	for _, data := range sensorData {
		fmt.Printf("   设备: %s, 温度: %.1f°F, 湿度: %.1f%%\n",
			data["device"], data["temperature"], data["humidity"])
		ssql.Emit(data)
	}

	// 7. 等待处理完成
	fmt.Println("\n⏳ 等待窗口处理...")
	time.Sleep(3 * time.Second)

	// 8. 演示内置函数
	fmt.Println("\n🔧 内置函数演示:")

	// 数学函数
	fmt.Printf("   abs(-15.5) = %.1f\n", callFunction("abs", -15.5))
	fmt.Printf("   sqrt(16) = %.1f\n", callFunction("sqrt", 16.0))

	// 字符串函数
	fmt.Printf("   concat('Hello', ' ', 'World') = %s\n", callFunction("concat", "Hello", " ", "World"))
	fmt.Printf("   upper('streamsql') = %s\n", callFunction("upper", "streamsql"))
	fmt.Printf("   length('StreamSQL') = %d\n", callFunction("length", "StreamSQL"))

	// 转换函数
	fmt.Printf("   hex2dec('ff') = %d\n", callFunction("hex2dec", "ff"))
	fmt.Printf("   dec2hex(255) = %s\n", callFunction("dec2hex", 255))

	// 时间函数
	fmt.Printf("   now() = %v\n", callFunction("now"))

	// 9. 显示已注册的函数
	fmt.Println("\n📋 已注册的函数:")
	allFunctions := functions.ListAll()
	for name, fn := range allFunctions {
		fmt.Printf("   %s (%s): %s\n", name, fn.GetType(), fn.GetDescription())
	}

	fmt.Println("\n✅ 示例完成!")
}

// 辅助函数：调用函数并返回结果
func callFunction(name string, args ...any) any {
	ctx := &functions.FunctionContext{
		Data: make(map[string]any),
	}

	result, err := functions.Execute(name, ctx, args)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}
	return result
}
