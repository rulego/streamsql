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
	fmt.Println("🔧 StreamSQL 嵌套字段访问功能完整演示")
	fmt.Println("=========================================")

	// 创建 StreamSQL 实例
	ssql := streamsql.New()
	defer ssql.Stop()

	// 基础功能演示
	fmt.Println("\n📊 第一部分：基础嵌套字段访问")
	demonstrateBasicNestedAccess(ssql)

	// 基础聚合演示
	fmt.Println("\n📈 第二部分：嵌套字段聚合")
	demonstrateNestedAggregation(ssql)

	// 复杂功能演示
	fmt.Println("\n🔧 第三部分：复杂嵌套字段访问")

	// 演示1: 数组索引访问
	fmt.Println("\n📊 演示1: 数组索引访问")
	demonstrateArrayAccess(ssql)

	// 演示2: Map键访问
	fmt.Println("\n🗝️ 演示2: Map键访问")
	demonstrateMapKeyAccess(ssql)

	// 演示3: 混合复杂访问
	fmt.Println("\n🔄 演示3: 混合复杂访问")
	demonstrateComplexMixedAccess(ssql)

	// 演示4: 负数索引访问
	fmt.Println("\n⬅️ 演示4: 负数索引访问")
	demonstrateNegativeIndexAccess(ssql)

	// 演示5: 数组索引聚合计算
	fmt.Println("\n📈 演示5: 数组索引聚合计算")
	demonstrateArrayIndexAggregation(ssql)

	fmt.Println("\n✅ 完整演示完成!")
}

// 演示基础嵌套字段访问
func demonstrateBasicNestedAccess(ssql *streamsql.Streamsql) {
	// SQL查询使用基础嵌套字段
	rsql := `SELECT device.info.name as device_name, 
                    device.location,
                    sensor.temperature,
                    sensor.humidity
             FROM stream 
             WHERE device.location = 'room-A' 
               AND sensor.temperature > 20`

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
					"name": "温度传感器-001",
					"type": "temperature",
				},
				"location": "room-A",
			},
			"sensor": map[string]interface{}{
				"temperature": 25.5,
				"humidity":    60.2,
			},
			"timestamp": time.Now().Unix(),
		},
		{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"name": "温度传感器-002",
					"type": "temperature",
				},
				"location": "room-B", // 不匹配条件
			},
			"sensor": map[string]interface{}{
				"temperature": 30.0,
				"humidity":    55.8,
			},
			"timestamp": time.Now().Unix(),
		},
		{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"name": "温度传感器-003",
					"type": "temperature",
				},
				"location": "room-A",
			},
			"sensor": map[string]interface{}{
				"temperature": 15.0, // 不匹配条件
				"humidity":    65.3,
			},
			"timestamp": time.Now().Unix(),
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// 设置结果回调
	ssql.AddSink(func(result interface{}) {
		defer wg.Done()

		fmt.Println("  📋 基础嵌套字段访问结果:")
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
		ssql.Emit(data)
	}

	// 等待结果
	wg.Wait()
}

// 演示嵌套字段聚合
func demonstrateNestedAggregation(ssql *streamsql.Streamsql) {
	// SQL查询：嵌套字段聚合
	rsql := `SELECT device.location, 
                    AVG(sensor.temperature) as avg_temp,
                    MAX(sensor.humidity) as max_humidity,
                    COUNT(*) as sensor_count
             FROM stream 
             GROUP BY device.location, TumblingWindow('2s')
             WITH (TIMESTAMP='timestamp', TIMEUNIT='ss')`

	err := ssql.Execute(rsql)
	if err != nil {
		fmt.Printf("❌ SQL执行失败: %v\n", err)
		return
	}

	var resultCount int
	var wg sync.WaitGroup
	wg.Add(1)

	// 设置结果回调
	ssql.AddSink(func(result interface{}) {
		defer wg.Done()

		fmt.Println("  📈 嵌套字段聚合结果:")
		if resultSlice, ok := result.([]map[string]interface{}); ok {
			for i, item := range resultSlice {
				resultCount++
				fmt.Printf("    聚合结果 %d:\n", i+1)
				fmt.Printf("      位置: %v\n", item["device.location"])
				fmt.Printf("      平均温度: %.2f°C\n", item["avg_temp"])
				fmt.Printf("      最大湿度: %.1f%%\n", item["max_humidity"])
				fmt.Printf("      传感器数量: %v\n", item["sensor_count"])
				fmt.Println()
			}
		}
	})

	// 生成模拟数据
	locations := []string{"智能温室-A区", "智能温室-B区", "智能温室-C区"}

	go func() {
		for i := 0; i < 9; i++ {
			location := locations[rand.Intn(len(locations))]

			data := map[string]interface{}{
				"device": map[string]interface{}{
					"info": map[string]interface{}{
						"name": fmt.Sprintf("sensor-%03d", i+1),
						"type": "environment",
					},
					"location": location,
				},
				"sensor": map[string]interface{}{
					"temperature": 18.0 + rand.Float64()*15.0, // 18-33°C
					"humidity":    40.0 + rand.Float64()*30.0, // 40-70%
				},
				"timestamp": time.Now().Unix(),
			}

			ssql.Emit(data)
			time.Sleep(300 * time.Millisecond) // 每300ms发送一条数据
		}
	}()

	// 等待聚合结果
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
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

// 演示数组索引访问
func demonstrateArrayAccess(ssql *streamsql.Streamsql) {
	// SQL查询：提取数组中的特定元素
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

	// 准备测试数据
	testData := []map[string]interface{}{
		{
			"device": "工业传感器-001",
			"sensors": []interface{}{
				map[string]interface{}{"temperature": 25.5, "humidity": 60.2},
				map[string]interface{}{"temperature": 26.8, "humidity": 58.7},
				map[string]interface{}{"temperature": 24.1, "humidity": 62.1},
			},
			"data":      []interface{}{"status_ok", "battery_95%", "signal_strong", "location_A1"},
			"timestamp": time.Now().Unix(),
		},
		{
			"device": "环境监测器-002",
			"sensors": []interface{}{
				map[string]interface{}{"temperature": 22.3, "humidity": 65.8},
				map[string]interface{}{"temperature": 23.1, "humidity": 63.2},
			},
			"data":      []interface{}{"status_warning", "battery_78%", "signal_weak"},
			"timestamp": time.Now().Unix(),
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// 设置结果回调
	ssql.AddSink(func(result interface{}) {
		defer wg.Done()

		fmt.Println("  📋 数组索引访问结果:")
		if resultSlice, ok := result.([]map[string]interface{}); ok {
			for i, item := range resultSlice {
				fmt.Printf("    记录 %d:\n", i+1)
				fmt.Printf("      设备: %v\n", item["device"])
				fmt.Printf("      第一个传感器温度: %v°C\n", item["first_sensor_temp"])
				fmt.Printf("      第二个传感器湿度: %v%%\n", item["second_sensor_humidity"])
				fmt.Printf("      第三个数据项: %v\n", item["third_data_item"])
				fmt.Println()
			}
		}
	})

	// 添加测试数据
	for _, data := range testData {
		ssql.Emit(data)
	}

	// 等待结果
	wg.Wait()
}

// 演示Map键访问
func demonstrateMapKeyAccess(ssql *streamsql.Streamsql) {
	// SQL查询：使用字符串键访问Map数据
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

	// 准备测试数据
	testData := []map[string]interface{}{
		{
			"device_id": "gateway-001",
			"config": map[string]interface{}{
				"host":     "192.168.1.100",
				"port":     8080,
				"protocol": "https",
			},
			"settings": map[string]interface{}{
				"enable_ssl":  true,
				"timeout":     30,
				"max_retries": 3,
			},
			"metadata": map[string]interface{}{
				"version":    "v2.1.3",
				"build_date": "2023-12-01",
				"vendor":     "TechCorp",
			},
		},
		{
			"device_id": "gateway-002",
			"config": map[string]interface{}{
				"host":     "192.168.1.101",
				"port":     8443,
				"protocol": "https",
			},
			"settings": map[string]interface{}{
				"enable_ssl":  false,
				"timeout":     60,
				"max_retries": 5,
			},
			"metadata": map[string]interface{}{
				"version":    "v2.0.8",
				"build_date": "2023-11-15",
				"vendor":     "TechCorp",
			},
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// 设置结果回调
	ssql.AddSink(func(result interface{}) {
		defer wg.Done()

		fmt.Println("  🗝️ Map键访问结果:")
		if resultSlice, ok := result.([]map[string]interface{}); ok {
			for i, item := range resultSlice {
				fmt.Printf("    记录 %d:\n", i+1)
				fmt.Printf("      设备ID: %v\n", item["device_id"])
				fmt.Printf("      服务器主机: %v\n", item["server_host"])
				fmt.Printf("      服务器端口: %v\n", item["server_port"])
				fmt.Printf("      SSL启用: %v\n", item["ssl_enabled"])
				fmt.Printf("      应用版本: %v\n", item["app_version"])
				fmt.Println()
			}
		}
	})

	// 添加测试数据
	for _, data := range testData {
		ssql.Emit(data)
	}

	// 等待结果
	wg.Wait()
}

// 演示混合复杂访问
func demonstrateComplexMixedAccess(ssql *streamsql.Streamsql) {
	// SQL查询：混合使用数组索引、Map键和嵌套字段访问
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

	// 准备复杂嵌套数据
	testData := map[string]interface{}{
		"building": "智能大厦A座",
		"floors": []interface{}{
			// 第一层
			map[string]interface{}{
				"floor_number": 1,
				"rooms": []interface{}{
					map[string]interface{}{"name": "大厅", "type": "public"},
					map[string]interface{}{"name": "接待室", "type": "office"},
					map[string]interface{}{"name": "会议室A", "type": "meeting"},
					map[string]interface{}{"name": "休息区", "type": "lounge"},
				},
			},
			// 第二层
			map[string]interface{}{
				"floor_number": 2,
				"sensors": []interface{}{
					map[string]interface{}{
						"id": "sensor-201",
						"readings": map[string]interface{}{
							"temperature": 23.5,
							"humidity":    58.2,
							"co2":         420,
						},
					},
					map[string]interface{}{
						"id": "sensor-202",
						"readings": map[string]interface{}{
							"temperature": 24.1,
							"humidity":    60.8,
							"co2":         380,
						},
					},
				},
			},
		},
		"metadata": map[string]interface{}{
			"building_info": map[string]interface{}{
				"architect":    "张建筑师",
				"year_built":   2020,
				"total_floors": 25,
			},
			"owner": "科技园管委会",
		},
		"alerts": []interface{}{
			map[string]interface{}{"level": "info", "message": "系统启动完成"},
			map[string]interface{}{"level": "warning", "message": "传感器信号弱"},
			map[string]interface{}{"level": "info", "message": "定期维护提醒"},
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// 设置结果回调
	ssql.AddSink(func(result interface{}) {
		defer wg.Done()

		fmt.Println("  🔄 混合复杂访问结果:")
		if resultSlice, ok := result.([]map[string]interface{}); ok {
			for i, item := range resultSlice {
				fmt.Printf("    记录 %d:\n", i+1)
				fmt.Printf("      建筑: %v\n", item["building"])
				fmt.Printf("      一层第3个房间: %v\n", item["first_floor_room3_name"])
				fmt.Printf("      二层第1个传感器温度: %v°C\n", item["second_floor_first_sensor_temp"])
				fmt.Printf("      建筑师: %v\n", item["building_architect"])
				fmt.Printf("      最新警报: %v\n", item["latest_alert"])
				fmt.Println()
			}
		}
	})

	// 添加数据
	ssql.Emit(testData)

	// 等待结果
	wg.Wait()
}

// 演示负数索引访问
func demonstrateNegativeIndexAccess(ssql *streamsql.Streamsql) {
	// SQL查询：使用负数索引访问数组末尾元素
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

	// 准备测试数据
	testData := []map[string]interface{}{
		{
			"device_name": "温度监测器-Alpha",
			"readings":    []interface{}{18.5, 19.2, 20.1, 21.3, 22.8, 23.5},                    // [-1] = 23.5
			"history":     []interface{}{"boot", "calibration", "running", "alert", "resolved"}, // [-2] = "alert"
			"tags":        []interface{}{"indoor", "critical", "monitored"},                     // [-1] = "monitored"
		},
		{
			"device_name": "湿度传感器-Beta",
			"readings":    []interface{}{45.2, 47.8, 52.1, 48.9},        // [-1] = 48.9
			"history":     []interface{}{"init", "testing", "deployed"}, // [-2] = "testing"
			"tags":        []interface{}{"outdoor", "backup"},           // [-1] = "backup"
		},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// 设置结果回调
	ssql.AddSink(func(result interface{}) {
		defer wg.Done()

		fmt.Println("  ⬅️ 负数索引访问结果:")
		if resultSlice, ok := result.([]map[string]interface{}); ok {
			for i, item := range resultSlice {
				fmt.Printf("    记录 %d:\n", i+1)
				fmt.Printf("      设备名称: %v\n", item["device_name"])
				fmt.Printf("      最新读数: %v\n", item["latest_reading"])
				fmt.Printf("      倒数第二个事件: %v\n", item["second_last_event"])
				fmt.Printf("      最后一个标签: %v\n", item["last_tag"])
				fmt.Println()
			}
		}
	})

	// 添加测试数据
	for _, data := range testData {
		ssql.Emit(data)
	}

	// 等待结果
	wg.Wait()
}

// 演示数组索引聚合计算
func demonstrateArrayIndexAggregation(ssql *streamsql.Streamsql) {
	// SQL查询：对数组中特定位置的数据进行聚合计算
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

	// 设置结果回调
	ssql.AddSink(func(result interface{}) {
		defer wg.Done()

		fmt.Println("  📈 数组索引聚合计算结果:")
		if resultSlice, ok := result.([]map[string]interface{}); ok {
			for i, item := range resultSlice {
				resultCount++
				fmt.Printf("    聚合结果 %d:\n", i+1)
				fmt.Printf("      位置: %v\n", item["location"])
				fmt.Printf("      第一个传感器平均温度: %.2f°C\n", item["avg_first_sensor_temp"])
				fmt.Printf("      第二个传感器最大湿度: %.1f%%\n", item["max_second_sensor_humidity"])
				fmt.Printf("      设备数量: %v\n", item["device_count"])
				fmt.Println()
			}
		}
	})

	// 生成模拟数据
	locations := []string{"车间A", "车间B", "车间C"}

	go func() {
		for i := 0; i < 12; i++ {
			location := locations[rand.Intn(len(locations))]

			data := map[string]interface{}{
				"device_id": fmt.Sprintf("device-%03d", i+1),
				"location":  location,
				"sensors": []interface{}{
					map[string]interface{}{
						"temperature": 20.0 + rand.Float64()*10.0, // 20-30°C
						"humidity":    50.0 + rand.Float64()*20.0, // 50-70%
					},
					map[string]interface{}{
						"temperature": 18.0 + rand.Float64()*12.0, // 18-30°C
						"humidity":    45.0 + rand.Float64()*25.0, // 45-70%
					},
				},
				"timestamp": time.Now().Unix(),
			}

			ssql.Emit(data)
			time.Sleep(200 * time.Millisecond) // 每200ms发送一条数据
		}
	}()

	// 等待聚合结果
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
