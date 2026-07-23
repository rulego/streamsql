package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/rulego/streamsql"
)

// Example of use in non-aggregated scenarios
// Showcasing the application of StreamSQL in real-time data transformation, filtering, cleaning, and other scenarios
func main() {
	fmt.Println("=== StreamSQL 非聚合场景演示 ===")

	// Scenario 1: Real-time data cleaning and standardization
	fmt.Println("\n1. 实时数据清洗和标准化")
	demonstrateDataCleaning()

	// Scenario 2: Data Richening and Computation Fields
	fmt.Println("\n2. 数据富化和计算字段")
	demonstrateDataEnrichment()

	// Scenario 3: Real-time alerts and event filtering
	fmt.Println("\n3. 实时告警和事件过滤")
	demonstrateRealTimeAlerting()

	// Scenario 4: Data format conversion
	fmt.Println("\n4. 数据格式转换")
	demonstrateDataFormatConversion()

	// Scenario 5: Condition-based data routing
	fmt.Println("\n5. 基于条件的数据路由")
	demonstrateDataRouting()

	// Scenario 6: Nested Field Handling
	fmt.Println("\n6. 嵌套字段处理")
	demonstrateNestedFieldProcessing()

	fmt.Println("\n=== 演示完成 ===")
}

// Scenario 1: Real-time data cleaning and standardization
func demonstrateDataCleaning() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// Cleaning and standardizing SQL
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

	// Result processing
	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  清洗后数据: %+v\n", result)
	})

	// Simulating dirty data input
	dirtyData := []map[string]any{
		{"deviceId": "sensor001", "deviceType": " temperature ", "temperature": 25.456789, "location": "room1", "status": 1},
		{"deviceId": "sensor002", "deviceType": "humidity", "temperature": 60.123, "location": nil, "status": 0},
		{"deviceId": "", "deviceType": "pressure", "temperature": nil, "location": "room2", "status": 2}, // It should be filtered
		{"deviceId": "sensor003", "deviceType": "TEMPERATURE", "temperature": 22.7, "location": "room3", "status": 1},
	}

	for _, data := range dirtyData {
		ssql.Emit(data)
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
}

// Scenario 2: Data Richening and Computation Fields
func demonstrateDataEnrichment() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// Data Rich SQL
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

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  富化后数据: %+v\n", result)
	})

	// Raw data
	rawData := []map[string]any{
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

// Scenario 3: Real-time alerts and event filtering
func demonstrateRealTimeAlerting() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// Alert filtering SQL
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

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  🚨 告警事件: %+v\n", result)
	})

	// Simulated sensor data (including outliers)
	sensorData := []map[string]any{
		{"deviceId": "sensor001", "temperature": 25.0, "humidity": 60, "location": "room1"}, // Normal
		{"deviceId": "sensor002", "temperature": 45.0, "humidity": 50, "location": "room2"}, // High temperature alert
		{"deviceId": "sensor003", "temperature": 20.0, "humidity": 95, "location": "room3"}, // High humidity alarm
		{"deviceId": "sensor004", "temperature": 2.0, "humidity": 30, "location": "room4"},  // Low temperature alert
		{"deviceId": "sensor005", "temperature": 22.0, "humidity": 15, "location": "room5"}, // Low humidity alarm
		{"deviceId": "sensor006", "temperature": 24.0, "humidity": 55, "location": "room6"}, // Normal
	}

	for _, data := range sensorData {
		ssql.Emit(data)
		time.Sleep(150 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
}

// Scenario 4: Data format conversion
func demonstrateDataFormatConversion() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// Format conversion SQL
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

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  格式转换结果: %+v\n", result)
	})

	// Enter data
	inputData := []map[string]any{
		{"deviceId": "sensor001", "temperature": 25.5, "humidity": 60, "location": "warehouse-A"},
		{"deviceId": "sensor002", "temperature": 22.0, "humidity": 55, "location": "warehouse-B"},
	}

	for _, data := range inputData {
		ssql.Emit(data)
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
}

// Scenario 5: Condition-based data routing
func demonstrateDataRouting() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// Data routing SQL
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

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  路由结果: %+v\n", result)
	})

	// Data from different types of devices
	deviceData := []map[string]any{
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

// Scenario 6: Nested Field Handling
func demonstrateNestedFieldProcessing() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// Nested fields handle SQL
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

	ssql.AddSink(func(result []map[string]any) {
		fmt.Printf("  嵌套字段处理结果: %+v\n", result)
	})

	// Nested structural data
	nestedData := []map[string]any{
		{
			"device": map[string]any{
				"info": map[string]any{
					"id":   "sensor001",
					"name": "Temperature Sensor 1",
					"type": "sensor",
				},
				"location": map[string]any{
					"building": "Building-A",
					"room":     "Room-101",
				},
				"config": map[string]any{
					"max_temp": 30.0,
					"min_temp": 10.0,
				},
			},
			"metrics": map[string]any{
				"temperature": 32.5,
				"humidity":    65,
			},
		},
		{
			"device": map[string]any{
				"info": map[string]any{
					"id":   "sensor002",
					"name": "Humidity Sensor 1",
					"type": "sensor",
				},
				"location": map[string]any{
					"building": "Building-B",
					"room":     "Room-201",
				},
				"config": map[string]any{
					"max_temp": 25.0,
					"min_temp": 15.0,
				},
			},
			"metrics": map[string]any{
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

// Auxiliary functions that generate random test data
func generateRandomSensorData(deviceId string) map[string]any {
	return map[string]any{
		"deviceId":    deviceId,
		"temperature": 15.0 + rand.Float64()*25.0, // 15-40 degrees
		"humidity":    30.0 + rand.Float64()*40.0, // 30-70%
		"location":    fmt.Sprintf("room%d", rand.Intn(10)+1),
		"timestamp":   time.Now().Unix(),
	}
}
