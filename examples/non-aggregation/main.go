package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/rulego/streamsql"
)

// 非聚合场景使用示例
// 展示StreamSQL在实时数据转换、过滤、清洗等场景中的应用
func main() {
	fmt.Println("=== StreamSQL 非聚合场景演示 ===")

	// 场景1: 实时数据清洗和标准化
	fmt.Println("\n1. 实时数据清洗和标准化")
	demonstrateDataCleaning()

	// 场景2: 数据富化和计算字段
	fmt.Println("\n2. 数据富化和计算字段")
	demonstrateDataEnrichment()

	// 场景3: 实时告警和事件过滤
	fmt.Println("\n3. 实时告警和事件过滤")
	demonstrateRealTimeAlerting()

	// 场景4: 数据格式转换
	fmt.Println("\n4. 数据格式转换")
	demonstrateDataFormatConversion()

	// 场景5: 基于条件的数据路由
	fmt.Println("\n5. 基于条件的数据路由")
	demonstrateDataRouting()

	// 场景6: 嵌套字段处理
	fmt.Println("\n6. 嵌套字段处理")
	demonstrateNestedFieldProcessing()

	fmt.Println("\n=== 演示完成 ===")
}

// 场景1: 实时数据清洗和标准化
func demonstrateDataCleaning() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// 清洗和标准化SQL
	rsql := `SELECT deviceId,
	                UPPER(TRIM(deviceType)) as device_type,
	                ROUND(temperature, 2) as temperature,
	                COALESCE(location, 'unknown') as location,
	                CASE WHEN status = 1 THEN 'active'
	                     WHEN status = 0 THEN 'inactive'
	                     ELSE 'unknown' END as status_text
	         FROM stream 
	         WHERE deviceId != '' AND temperature > -999`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	// 结果处理
	ssql.AddSink(func(result interface{}) {
		fmt.Printf("  清洗后数据: %+v\n", result)
	})

	// 模拟脏数据输入
	dirtyData := []map[string]interface{}{
		{"deviceId": "sensor001", "deviceType": " temperature ", "temperature": 25.456789, "location": "room1", "status": 1},
		{"deviceId": "sensor002", "deviceType": "humidity", "temperature": 60.123, "location": nil, "status": 0},
		{"deviceId": "", "deviceType": "pressure", "temperature": nil, "location": "room2", "status": 2}, // 应被过滤
		{"deviceId": "sensor003", "deviceType": "TEMPERATURE", "temperature": 22.7, "location": "room3", "status": 1},
	}

	for _, data := range dirtyData {
		ssql.Emit(data)
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
}

// 场景2: 数据富化和计算字段
func demonstrateDataEnrichment() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// 数据富化SQL
	rsql := `SELECT *,
	                temperature * 1.8 + 32 as temp_fahrenheit,
	                CASE WHEN temperature > 30 THEN 'hot'
	                     WHEN temperature < 15 THEN 'cold'
	                     ELSE 'normal' END as temp_category,
	                CONCAT(location, '-', deviceId) as full_identifier,
	                NOW() as processed_timestamp,
	                ROUND(humidity / 100.0, 4) as humidity_ratio
	         FROM stream`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result interface{}) {
		fmt.Printf("  富化后数据: %+v\n", result)
	})

	// 原始数据
	rawData := []map[string]interface{}{
		{"deviceId": "sensor001", "temperature": 32.5, "humidity": 65, "location": "greenhouse"},
		{"deviceId": "sensor002", "temperature": 12.0, "humidity": 45, "location": "warehouse"},
		{"deviceId": "sensor003", "temperature": 22.8, "humidity": 70, "location": "office"},
	}

	for _, data := range rawData {
		ssql.Emit(data)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
}

// 场景3: 实时告警和事件过滤
func demonstrateRealTimeAlerting() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// 告警过滤SQL
	rsql := `SELECT deviceId,
	                temperature,
	                humidity,
	                location,
	                'CRITICAL' as alert_level,
	                CASE WHEN temperature > 40 THEN 'High Temperature Alert'
	                     WHEN temperature < 5 THEN 'Low Temperature Alert'
	                     WHEN humidity > 90 THEN 'High Humidity Alert'
	                     WHEN humidity < 20 THEN 'Low Humidity Alert'
	                     ELSE 'Unknown Alert' END as alert_message,
	                NOW() as alert_time
	         FROM stream 
	         WHERE temperature > 40 OR temperature < 5 OR humidity > 90 OR humidity < 20`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result interface{}) {
		fmt.Printf("  🚨 告警事件: %+v\n", result)
	})

	// 模拟传感器数据（包含异常值）
	sensorData := []map[string]interface{}{
		{"deviceId": "sensor001", "temperature": 25.0, "humidity": 60, "location": "room1"}, // 正常
		{"deviceId": "sensor002", "temperature": 45.0, "humidity": 50, "location": "room2"}, // 高温告警
		{"deviceId": "sensor003", "temperature": 20.0, "humidity": 95, "location": "room3"}, // 高湿度告警
		{"deviceId": "sensor004", "temperature": 2.0, "humidity": 30, "location": "room4"},  // 低温告警
		{"deviceId": "sensor005", "temperature": 22.0, "humidity": 15, "location": "room5"}, // 低湿度告警
		{"deviceId": "sensor006", "temperature": 24.0, "humidity": 55, "location": "room6"}, // 正常
	}

	for _, data := range sensorData {
		ssql.Emit(data)
		time.Sleep(150 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
}

// 场景4: 数据格式转换
func demonstrateDataFormatConversion() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// 格式转换SQL
	rsql := `SELECT deviceId,
	                CONCAT('{"device_id":"', deviceId, '","metrics":{"temp":', 
	                       CAST(temperature AS STRING), ',"hum":', 
	                       CAST(humidity AS STRING), '},"location":"', 
	                       location, '","timestamp":', 
	                       CAST(NOW() AS STRING), '}') as json_format,
	                CONCAT(deviceId, '|', location, '|', 
	                       CAST(temperature AS STRING), '|', 
	                       CAST(humidity AS STRING)) as csv_format
	         FROM stream`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result interface{}) {
		fmt.Printf("  格式转换结果: %+v\n", result)
	})

	// 输入数据
	inputData := []map[string]interface{}{
		{"deviceId": "sensor001", "temperature": 25.5, "humidity": 60, "location": "warehouse-A"},
		{"deviceId": "sensor002", "temperature": 22.0, "humidity": 55, "location": "warehouse-B"},
	}

	for _, data := range inputData {
		ssql.Emit(data)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
}

// 场景5: 基于条件的数据路由
func demonstrateDataRouting() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// 数据路由SQL
	rsql := `SELECT *,
	                CASE WHEN deviceType = 'temperature' AND temperature > 30 THEN 'high_temp_topic'
	                     WHEN deviceType = 'humidity' AND humidity > 80 THEN 'high_humidity_topic'
	                     WHEN deviceType = 'pressure' THEN 'pressure_topic'
	                     ELSE 'default_topic' END as routing_topic,
	                CASE WHEN temperature > 35 OR humidity > 85 THEN 'urgent'
	                     WHEN temperature > 25 OR humidity > 70 THEN 'normal'
	                     ELSE 'low' END as priority
	         FROM stream`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result interface{}) {
		fmt.Printf("  路由结果: %+v\n", result)
	})

	// 不同类型的设备数据
	deviceData := []map[string]interface{}{
		{"deviceId": "temp001", "deviceType": "temperature", "temperature": 35.0, "humidity": 60},
		{"deviceId": "hum001", "deviceType": "humidity", "temperature": 25.0, "humidity": 85},
		{"deviceId": "press001", "deviceType": "pressure", "temperature": 22.0, "pressure": 1013.25},
		{"deviceId": "temp002", "deviceType": "temperature", "temperature": 20.0, "humidity": 50},
	}

	for _, data := range deviceData {
		ssql.Emit(data)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
}

// 场景6: 嵌套字段处理
func demonstrateNestedFieldProcessing() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// 嵌套字段处理SQL
	rsql := `SELECT device.info.id as device_id,
	                device.info.name as device_name,
	                device.location.building as building,
	                device.location.room as room,
	                metrics.temperature as temp,
	                metrics.humidity as humidity,
	                CONCAT(device.location.building, '-', device.location.room, '-', device.info.id) as full_path,
	                CASE WHEN metrics.temperature > device.config.max_temp THEN 'OVER_LIMIT'
	                     ELSE 'NORMAL' END as temp_status
	         FROM stream 
	         WHERE device.info.type = 'sensor'`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result interface{}) {
		fmt.Printf("  嵌套字段处理结果: %+v\n", result)
	})

	// 嵌套结构数据
	nestedData := []map[string]interface{}{
		{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"id":   "sensor001",
					"name": "Temperature Sensor 1",
					"type": "sensor",
				},
				"location": map[string]interface{}{
					"building": "Building-A",
					"room":     "Room-101",
				},
				"config": map[string]interface{}{
					"max_temp": 30.0,
					"min_temp": 10.0,
				},
			},
			"metrics": map[string]interface{}{
				"temperature": 32.5,
				"humidity":    65,
			},
		},
		{
			"device": map[string]interface{}{
				"info": map[string]interface{}{
					"id":   "sensor002",
					"name": "Humidity Sensor 1",
					"type": "sensor",
				},
				"location": map[string]interface{}{
					"building": "Building-B",
					"room":     "Room-201",
				},
				"config": map[string]interface{}{
					"max_temp": 25.0,
					"min_temp": 15.0,
				},
			},
			"metrics": map[string]interface{}{
				"temperature": 22.0,
				"humidity":    70,
			},
		},
	}

	for _, data := range nestedData {
		ssql.Emit(data)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
}

// 生成随机测试数据的辅助函数
func generateRandomSensorData(deviceId string) map[string]interface{} {
	return map[string]interface{}{
		"deviceId":    deviceId,
		"temperature": 15.0 + rand.Float64()*25.0, // 15-40度
		"humidity":    30.0 + rand.Float64()*40.0, // 30-70%
		"location":    fmt.Sprintf("room%d", rand.Intn(10)+1),
		"timestamp":   time.Now().Unix(),
	}
}
