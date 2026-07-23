package main

import (
	"fmt"
	"time"

	"github.com/rulego/streamsql"
)

// main demonstrates the use of the PrintTable method
func main() {
	fmt.Println("=== StreamSQL PrintTable 示例 ===")

	// Create a StreamSQL instance
	ssql := streamsql.New()

	// Example 1: Aggregated query - grouping temperature statistics by device
	fmt.Println("\n示例1: 聚合查询结果")
	err := ssql.Execute("SELECT device, AVG(temperature) as avg_temp, MAX(temperature) as max_temp FROM stream GROUP BY device, TumblingWindow('3s')")
	if err != nil {
		fmt.Printf("执行SQL失败: %v\n", err)
		return
	}

	// Use the PrintTable method to output results in table format
	ssql.PrintTable()

	// Send test data
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 25.5, "timestamp": time.Now()},
		{"device": "sensor1", "temperature": 26.0, "timestamp": time.Now()},
		{"device": "sensor2", "temperature": 23.8, "timestamp": time.Now()},
		{"device": "sensor2", "temperature": 24.2, "timestamp": time.Now()},
		{"device": "sensor1", "temperature": 27.1, "timestamp": time.Now()},
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	// Wait for the window to trigger
	time.Sleep(4 * time.Second)

	// Example 2: Non-aggregated queries
	fmt.Println("\n示例2: 非聚合查询结果")
	ssql2 := streamsql.New()
	err = ssql2.Execute("SELECT device, temperature, temperature * 1.8 + 32 as fahrenheit FROM stream WHERE temperature > 24")
	if err != nil {
		fmt.Printf("执行SQL失败: %v\n", err)
		return
	}

	ssql2.PrintTable()

	// Send test data
	for _, data := range testData {
		ssql2.Emit(data)
	}

	// Wait for processing to complete
	time.Sleep(1 * time.Second)

	// Example 3: Compare with the original print method
	fmt.Println("\n示例3: 原始Print方法输出对比")
	ssql3 := streamsql.New()
	err = ssql3.Execute("SELECT device, COUNT(*) as count FROM stream GROUP BY device, TumblingWindow('2s')")
	if err != nil {
		fmt.Printf("执行SQL失败: %v\n", err)
		return
	}

	fmt.Println("原始PrintTable方法:")
	ssql3.PrintTable()

	// Send data
	for i := 0; i < 3; i++ {
		ssql3.Emit(map[string]any{"device": "test_device", "value": i})
	}

	time.Sleep(3 * time.Second)
	fmt.Println("\n=== 示例结束 ===")
}
