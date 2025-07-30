package main

import (
	"fmt"
	"time"

	"github.com/rulego/streamsql"
)

// main 演示PrintTable方法的使用
func main() {
	fmt.Println("=== StreamSQL PrintTable 示例 ===")

	// 创建StreamSQL实例
	ssql := streamsql.New()

	// 示例1: 聚合查询 - 按设备分组统计温度
	fmt.Println("\n示例1: 聚合查询结果")
	err := ssql.Execute("SELECT device, AVG(temperature) as avg_temp, MAX(temperature) as max_temp FROM stream GROUP BY device, TumblingWindow('3s')")
	if err != nil {
		fmt.Printf("执行SQL失败: %v\n", err)
		return
	}

	// 使用PrintTable方法以表格形式输出结果
	ssql.PrintTable()

	// 发送测试数据
	testData := []map[string]interface{}{
		{"device": "sensor1", "temperature": 25.5, "timestamp": time.Now()},
		{"device": "sensor1", "temperature": 26.0, "timestamp": time.Now()},
		{"device": "sensor2", "temperature": 23.8, "timestamp": time.Now()},
		{"device": "sensor2", "temperature": 24.2, "timestamp": time.Now()},
		{"device": "sensor1", "temperature": 27.1, "timestamp": time.Now()},
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	// 等待窗口触发
	time.Sleep(4 * time.Second)

	// 示例2: 非聚合查询
	fmt.Println("\n示例2: 非聚合查询结果")
	ssql2 := streamsql.New()
	err = ssql2.Execute("SELECT device, temperature, temperature * 1.8 + 32 as fahrenheit FROM stream WHERE temperature > 24")
	if err != nil {
		fmt.Printf("执行SQL失败: %v\n", err)
		return
	}

	ssql2.PrintTable()

	// 发送测试数据
	for _, data := range testData {
		ssql2.Emit(data)
	}

	// 等待处理完成
	time.Sleep(1 * time.Second)

	// 示例3: 对比原始Print方法
	fmt.Println("\n示例3: 原始Print方法输出对比")
	ssql3 := streamsql.New()
	err = ssql3.Execute("SELECT device, COUNT(*) as count FROM stream GROUP BY device, TumblingWindow('2s')")
	if err != nil {
		fmt.Printf("执行SQL失败: %v\n", err)
		return
	}

	fmt.Println("原始PrintTable方法:")
	ssql3.PrintTable()

	// 发送数据
	for i := 0; i < 3; i++ {
		ssql3.Emit(map[string]interface{}{"device": "test_device", "value": i})
	}

	time.Sleep(3 * time.Second)
	fmt.Println("\n=== 示例结束 ===")
}