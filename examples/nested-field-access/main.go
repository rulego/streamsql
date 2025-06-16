package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/rulego/streamsql"
)

func main() {
	fmt.Println("🔍 StreamSQL 嵌套字段访问功能演示")
	fmt.Println("=====================================")

	// 创建 StreamSQL 实例
	ssql := streamsql.New()
	defer ssql.Stop()

	// 演示1: 基本嵌套字段查询
	fmt.Println("\n📊 演示1: 基本嵌套字段查询")
	demonstrateBasicNestedQuery(ssql)

	// 演示2: 嵌套字段聚合查询
	fmt.Println("\n📈 演示2: 嵌套字段聚合查询")
	demonstrateNestedAggregation(ssql)

	fmt.Println("\n✅ 演示完成!")
}

// 演示基本嵌套字段查询
func demonstrateBasicNestedQuery(ssql *streamsql.Streamsql) {
	// SQL查询：提取设备信息和传感器数据
	rsql := `SELECT device.info.name as device_name, 
                    device.location,
                    sensor.temperature,
                    sensor.humidity
             FROM stream`

	err := ssql.Execute(rsql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// 准备测试数据
	testData := []map[string]interface{}{
		{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"name":  "temperature-sensor-001",
					"type":  "temperature",
					"model": "TempSense-Pro",
				},
				"location": "智能温室-A区",
				"status":   "online",
			},
			"sensor": map[string]interface{}{
				"temperature": 24.5,
				"humidity":    62.3,
			},
			"timestamp": time.Now().Unix(),
		},
		{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"name":  "humidity-sensor-002",
					"type":  "humidity",
					"model": "HumiSense-X1",
				},
				"location": "智能温室-B区",
				"status":   "online",
			},
			"sensor": map[string]interface{}{
				"temperature": 26.8,
				"humidity":    58.7,
			},
			"timestamp": time.Now().Unix(),
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// 设置结果回调
	ssql.Stream().AddSink(func(result interface{}) {
		defer wg.Done()

		fmt.Println("  📋 查询结果:")
		if resultSlice, ok := result.([]map[string]interface{}); ok {
			for i, item := range resultSlice {
				fmt.Printf("    记录 %d:\n", i+1)
				fmt.Printf("      设备名称: %v\n", item["device_name"])
				fmt.Printf("      设备位置: %v\n", item["device.location"])
				fmt.Printf("      温度: %v°C\n", item["sensor.temperature"])
				fmt.Printf("      湿度: %v%%\n", item["sensor.humidity"])
				fmt.Println()
			}
		}
	})

	// 添加测试数据
	for _, data := range testData {
		ssql.Stream().AddData(data)
	}

	// 等待结果
	wg.Wait()
}

// 演示嵌套字段聚合查询
func demonstrateNestedAggregation(ssql *streamsql.Streamsql) {
	// SQL查询：按设备位置统计平均温度和湿度
	rsql := `SELECT device.location,
                    device.info.type,
                    AVG(sensor.temperature) as avg_temp,
                    AVG(sensor.humidity) as avg_humidity,
                    COUNT(*) as sensor_count,
                    window_start() as window_start,
                    window_end() as window_end
             FROM stream 
             GROUP BY device.location, device.info.type, TumblingWindow('3s')
             WITH (TIMESTAMP='timestamp', TIMEUNIT='ss')`

	err := ssql.Execute(rsql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 设置结果回调
	ssql.Stream().AddSink(func(result interface{}) {
		fmt.Println("  📊 聚合结果:")
		if resultSlice, ok := result.([]map[string]interface{}); ok {
			for i, item := range resultSlice {
				fmt.Printf("    聚合组 %d:\n", i+1)
				fmt.Printf("      设备位置: %v\n", item["device.location"])
				fmt.Printf("      传感器类型: %v\n", item["device.info.type"])
				fmt.Printf("      平均温度: %.2f°C\n", item["avg_temp"])
				fmt.Printf("      平均湿度: %.2f%%\n", item["avg_humidity"])
				fmt.Printf("      传感器数量: %v\n", item["sensor_count"])
				fmt.Printf("      窗口开始: %v\n", formatTime(item["window_start"]))
				fmt.Printf("      窗口结束: %v\n", formatTime(item["window_end"]))
				fmt.Println()
			}
		}
	})

	// 数据生成器：模拟多个区域的传感器数据
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		locations := []string{"智能温室-A区", "智能温室-B区", "智能温室-C区"}
		sensorTypes := []string{"temperature", "humidity", "combo"}

		for {
			select {
			case <-ticker.C:
				// 每500ms生成3条不同位置的传感器数据
				for i := 0; i < 3; i++ {
					location := locations[rand.Intn(len(locations))]
					sensorType := sensorTypes[rand.Intn(len(sensorTypes))]

					data := map[string]interface{}{
						"device": map[string]interface{}{
							"info": map[string]interface{}{
								"name":  fmt.Sprintf("%s-sensor-%03d", sensorType, rand.Intn(999)+1),
								"type":  sensorType,
								"model": fmt.Sprintf("Model-%s", sensorType),
							},
							"location": location,
							"status":   "online",
						},
						"sensor": map[string]interface{}{
							"temperature": 20.0 + rand.Float64()*15, // 20-35度
							"humidity":    45.0 + rand.Float64()*30, // 45-75%
						},
						"timestamp": time.Now().Unix(),
					}

					ssql.Stream().AddData(data)
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	// 等待聚合结果
	wg.Wait()
}

// 格式化时间戳
func formatTime(timestamp interface{}) string {
	if ts, ok := timestamp.(int64); ok {
		return time.Unix(ts, 0).Format("15:04:05")
	}
	return "N/A"
}
