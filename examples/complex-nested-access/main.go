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
	fmt.Println("🔧 StreamSQL 复杂嵌套字段访问功能演示")
	fmt.Println("=======================================")

	// Create a StreamSQL instance
	ssql := streamsql.New()
	defer ssql.Stop()

	// Demo 1: Array index access
	fmt.Println("\n📊 演示1: 数组索引访问")
	demonstrateArrayAccess(ssql)

	// Demo 2: Map key access
	fmt.Println("\n🗝️ 演示2: Map键访问")
	demonstrateMapKeyAccess(ssql)

	// Demo 3: Hybrid Complex Access
	fmt.Println("\n🔄 演示3: 混合复杂访问")
	demonstrateComplexMixedAccess(ssql)

	// Demo 4: Negative Index Access
	fmt.Println("\n⬅️ 演示4: 负数索引访问")
	demonstrateNegativeIndexAccess(ssql)

	// Demo 5: Array Index Aggregation Computation
	fmt.Println("\n📈 演示5: 数组索引聚合计算")
	demonstrateArrayIndexAggregation(ssql)

	fmt.Println("\n✅ 演示完成!")
}

// Demonstration of array index access
func demonstrateArrayAccess(ssql *streamsql.Streamsql) {
	// SQL query: extracting specific elements from an array
	rsql := `SELECT device, 
                    sensors[0].temperature as first_sensor_temp,
                    sensors[1].humidity as second_sensor_humidity,
                    data[2] as third_data_item
             FROM stream`

	err := ssql.Execute(rsql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// Prepare test data
	testData := []map[string]any{
		{
			"device": "工业传感器-001",
			"sensors": []any{
				map[string]any{"temperature": 25.5, "humidity": 60.2},
				map[string]any{"temperature": 26.8, "humidity": 58.7},
				map[string]any{"temperature": 24.1, "humidity": 62.1},
			},
			"data":      []any{"status_ok", "battery_95%", "signal_strong", "location_A1"},
			"timestamp": time.Now().Unix(),
		},
		{
			"device": "环境监测器-002",
			"sensors": []any{
				map[string]any{"temperature": 22.3, "humidity": 65.8},
				map[string]any{"temperature": 23.1, "humidity": 63.2},
			},
			"data":      []any{"status_warning", "battery_78%", "signal_weak"},
			"timestamp": time.Now().Unix(),
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Set result callback
	ssql.AddSink(func(result []map[string]any) {
		defer wg.Done()

		fmt.Println("  📋 数组索引访问结果:")
		for i, item := range result {
			fmt.Printf("    记录 %d:\n", i+1)
			fmt.Printf("      设备: %v\n", item["device"])
			fmt.Printf("      第一个传感器温度: %v°C\n", item["first_sensor_temp"])
			fmt.Printf("      第二个传感器湿度: %v%%\n", item["second_sensor_humidity"])
			fmt.Printf("      第三个数据项: %v\n", item["third_data_item"])
			fmt.Println()
		}
	})

	// Add test data
	for _, data := range testData {
		ssql.Emit(data)
	}

	// Wait for the results
	wg.Wait()
}

// Demonstration of Map key access
func demonstrateMapKeyAccess(ssql *streamsql.Streamsql) {
	// SQL query: Accessing Map data using string keys
	rsql := `SELECT device_id,
                    config['host'] as server_host,
                    config["port"] as server_port,
                    settings['enable_ssl'] as ssl_enabled,
                    metadata["version"] as app_version
             FROM stream`

	err := ssql.Execute(rsql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// Prepare test data
	testData := []map[string]any{
		{
			"device_id": "gateway-001",
			"config": map[string]any{
				"host":     "192.168.1.100",
				"port":     8080,
				"protocol": "https",
			},
			"settings": map[string]any{
				"enable_ssl":  true,
				"timeout":     30,
				"max_retries": 3,
			},
			"metadata": map[string]any{
				"version":    "v2.1.3",
				"build_date": "2023-12-01",
				"vendor":     "TechCorp",
			},
		},
		{
			"device_id": "gateway-002",
			"config": map[string]any{
				"host":     "192.168.1.101",
				"port":     8443,
				"protocol": "https",
			},
			"settings": map[string]any{
				"enable_ssl":  false,
				"timeout":     60,
				"max_retries": 5,
			},
			"metadata": map[string]any{
				"version":    "v2.0.8",
				"build_date": "2023-11-15",
				"vendor":     "TechCorp",
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Set result callback
	ssql.AddSink(func(result []map[string]any) {
		defer wg.Done()

		fmt.Println("  🗝️ Map键访问结果:")
		resultSlice := result
		for i, item := range resultSlice {
			fmt.Printf("    记录 %d:\n", i+1)
			fmt.Printf("      设备ID: %v\n", item["device_id"])
			fmt.Printf("      服务器主机: %v\n", item["server_host"])
			fmt.Printf("      服务器端口: %v\n", item["server_port"])
			fmt.Printf("      SSL启用: %v\n", item["ssl_enabled"])
			fmt.Printf("      应用版本: %v\n", item["app_version"])
			fmt.Println()
		}
	})

	// Add test data
	for _, data := range testData {
		ssql.Emit(data)
	}

	// Wait for the results
	wg.Wait()
}

// Demonstration of hybrid complex access
func demonstrateComplexMixedAccess(ssql *streamsql.Streamsql) {
	// SQL queries: Combining array indexes, Map keys, and nested field access
	rsql := `SELECT building,
                    floors[0].rooms[2]['name'] as first_floor_room3_name,
                    floors[1].sensors[0].readings['temperature'] as second_floor_first_sensor_temp,
                    metadata.building_info['architect'] as building_architect,
                    alerts[-1].message as latest_alert
             FROM stream`

	err := ssql.Execute(rsql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// Prepare complex nested data
	testData := map[string]any{
		"building": "智能大厦A座",
		"floors": []any{
			// The first level
			map[string]any{
				"floor_number": 1,
				"rooms": []any{
					map[string]any{"name": "大厅", "type": "public"},
					map[string]any{"name": "接待室", "type": "office"},
					map[string]any{"name": "会议室A", "type": "meeting"},
					map[string]any{"name": "休息区", "type": "lounge"},
				},
			},
			// The second floor
			map[string]any{
				"floor_number": 2,
				"sensors": []any{
					map[string]any{
						"id": "sensor-201",
						"readings": map[string]any{
							"temperature": 23.5,
							"humidity":    58.2,
							"co2":         420,
						},
					},
					map[string]any{
						"id": "sensor-202",
						"readings": map[string]any{
							"temperature": 24.1,
							"humidity":    60.8,
							"co2":         380,
						},
					},
				},
			},
		},
		"metadata": map[string]any{
			"building_info": map[string]any{
				"architect":    "张建筑师",
				"year_built":   2020,
				"total_floors": 25,
			},
			"owner": "科技园管委会",
		},
		"alerts": []any{
			map[string]any{"level": "info", "message": "系统启动完成"},
			map[string]any{"level": "warning", "message": "传感器信号弱"},
			map[string]any{"level": "info", "message": "定期维护提醒"},
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Set result callback
	ssql.AddSink(func(result []map[string]any) {
		defer wg.Done()

		fmt.Println("  🔄 混合复杂访问结果:")
		resultSlice := result
		for i, item := range resultSlice {
			fmt.Printf("    记录 %d:\n", i+1)
			fmt.Printf("      建筑: %v\n", item["building"])
			fmt.Printf("      一层第3个房间: %v\n", item["first_floor_room3_name"])
			fmt.Printf("      二层第1个传感器温度: %v°C\n", item["second_floor_first_sensor_temp"])
			fmt.Printf("      建筑师: %v\n", item["building_architect"])
			fmt.Printf("      最新警报: %v\n", item["latest_alert"])
			fmt.Println()
		}
	})

	// Add data
	ssql.Emit(testData)

	// Wait for the results
	wg.Wait()
}

// Demonstrating negative index access
func demonstrateNegativeIndexAccess(ssql *streamsql.Streamsql) {
	// SQL query: Uses negative indexes to access the last element of an array
	rsql := `SELECT device_name,
                    readings[-1] as latest_reading,
                    history[-2] as second_last_event,
                    tags[-1] as last_tag
             FROM stream`

	err := ssql.Execute(rsql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	// Prepare test data
	testData := []map[string]any{
		{
			"device_name": "温度监测器-Alpha",
			"readings":    []any{18.5, 19.2, 20.1, 21.3, 22.8, 23.5},                    // [-1] = 23.5
			"history":     []any{"boot", "calibration", "running", "alert", "resolved"}, // [-2] = "alert"
			"tags":        []any{"indoor", "critical", "monitored"},                     // [-1] = "monitored"
		},
		{
			"device_name": "湿度传感器-Beta",
			"readings":    []any{45.2, 47.8, 52.1, 48.9},        // [-1] = 48.9
			"history":     []any{"init", "testing", "deployed"}, // [-2] = "testing"
			"tags":        []any{"outdoor", "backup"},           // [-1] = "backup"
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Set result callback
	ssql.AddSink(func(result []map[string]any) {
		defer wg.Done()

		fmt.Println("  ⬅️ 负数索引访问结果:")
		resultSlice := result
		for i, item := range resultSlice {
			fmt.Printf("    记录 %d:\n", i+1)
			fmt.Printf("      设备名称: %v\n", item["device_name"])
			fmt.Printf("      最新读数: %v\n", item["latest_reading"])
			fmt.Printf("      倒数第二个事件: %v\n", item["second_last_event"])
			fmt.Printf("      最后一个标签: %v\n", item["last_tag"])
			fmt.Println()
		}
	})

	// Add test data
	for _, data := range testData {
		ssql.Emit(data)
	}

	// Wait for the results
	wg.Wait()
}

// Demonstration of array index aggregation calculation
func demonstrateArrayIndexAggregation(ssql *streamsql.Streamsql) {
	// SQL Query: Aggregates data at specific locations in an array for calculation
	rsql := `SELECT location,
                    AVG(sensors[0].temperature) as avg_first_sensor_temp,
                    MAX(sensors[1].humidity) as max_second_sensor_humidity,
                    COUNT(*) as device_count
             FROM stream 
             GROUP BY location, TumblingWindow('2s')
             WITH (TIMESTAMP='timestamp', TIMEUNIT='ss')`

	err := ssql.Execute(rsql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	var resultCount int
	var wg sync.WaitGroup
	wg.Add(1)

	// Set result callback
	ssql.AddSink(func(result []map[string]any) {
		defer wg.Done()

		fmt.Println("  📈 数组索引聚合计算结果:")
		resultSlice := result
		for i, item := range resultSlice {
			resultCount++
			fmt.Printf("    聚合结果 %d:\n", i+1)
			fmt.Printf("      位置: %v\n", item["location"])
			fmt.Printf("      第一个传感器平均温度: %.2f°C\n", item["avg_first_sensor_temp"])
			fmt.Printf("      第二个传感器最大湿度: %.1f%%\n", item["max_second_sensor_humidity"])
			fmt.Printf("      设备数量: %v\n", item["device_count"])
			fmt.Println()
		}
	})

	// Generate simulated data
	locations := []string{"车间A", "车间B", "车间C"}

	go func() {
		for i := 0; i < 12; i++ {
			location := locations[rand.Intn(len(locations))]

			data := map[string]any{
				"device_id": fmt.Sprintf("device-%03d", i+1),
				"location":  location,
				"sensors": []any{
					map[string]any{
						"temperature": 20.0 + rand.Float64()*10.0, // 20-30°C
						"humidity":    50.0 + rand.Float64()*20.0, // 50-70%
					},
					map[string]any{
						"temperature": 18.0 + rand.Float64()*12.0, // 18-30°C
						"humidity":    45.0 + rand.Float64()*25.0, // 45-70%
					},
				},
				"timestamp": time.Now().Unix(),
			}

			ssql.Emit(data)
			time.Sleep(200 * time.Millisecond) // Send a data piece every 200ms
		}
	}()

	// Waiting for the aggregated results
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		fmt.Println("    ⏰ 聚合计算超时")
	case <-func() chan struct{} {
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		return done
	}():
		fmt.Printf("    ✅ 聚合计算完成，共生成 %d 个窗口结果\n", resultCount)
	}
}
