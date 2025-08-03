package main

import (
	"fmt"
	"time"

	"github.com/rulego/streamsql"
)

func main() {
	fmt.Println("=== StreamSQL Null 比较语法演示 ===")
	fmt.Println()

	demo1() // fieldName = nil 语法
	demo2() // fieldName != nil 语法
	demo3() // fieldName = null 和 != null 语法
	demo4() // 混合语法演示
	demo5() // 嵌套字段 null 比较
}

func demo1() {
	fmt.Println("1. fieldName = nil 语法演示")
	fmt.Println("-------------------------------------------")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 使用 = nil 语法查找空值
	rsql := `SELECT deviceId, value, status 
	         FROM stream 
	         WHERE value = nil`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result []map[string]interface{}) {
		for _, data := range result {
			fmt.Printf("发现空值数据: %+v\n", data)
		}
	})

	testData := []map[string]interface{}{
		{"deviceId": "sensor1", "value": 25.5, "status": "active"},
		{"deviceId": "sensor2", "value": nil, "status": "active"}, // 符合条件
		{"deviceId": "sensor3", "value": 30.0, "status": "inactive"},
		{"deviceId": "sensor4", "value": nil, "status": "error"}, // 符合条件
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(300 * time.Millisecond)
	fmt.Println()
}

func demo2() {
	fmt.Println("2. fieldName != nil 语法演示")
	fmt.Println("-------------------------------------------")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 使用 != nil 语法查找非空值
	rsql := `SELECT deviceId, value, status 
	         FROM stream 
	         WHERE value != nil AND value > 20`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result []map[string]interface{}) {
		for _, data := range result {
			fmt.Printf("发现有效数据: %+v\n", data)
		}
	})

	testData := []map[string]interface{}{
		{"deviceId": "sensor1", "value": 25.5, "status": "active"},   // 符合条件
		{"deviceId": "sensor2", "value": nil, "status": "active"},    // 不符合（空值）
		{"deviceId": "sensor3", "value": 15.0, "status": "inactive"}, // 不符合（值<=20）
		{"deviceId": "sensor4", "value": 30.0, "status": "error"},    // 符合条件
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(300 * time.Millisecond)
	fmt.Println()
}

func demo3() {
	fmt.Println("3. fieldName = null 和 != null 语法演示")
	fmt.Println("-------------------------------------------")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 使用 = null 和 != null 语法
	rsql := `SELECT deviceId, value, status 
	         FROM stream 
	         WHERE status != null OR value = null`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result []map[string]interface{}) {
		for _, data := range result {
			status := data["status"]
			value := data["value"]
			if status != nil {
				fmt.Printf("状态非空的数据: %+v\n", data)
			} else if value == nil {
				fmt.Printf("值为空的数据: %+v\n", data)
			}
		}
	})

	testData := []map[string]interface{}{
		{"deviceId": "sensor1", "value": 25.5, "status": "active"},   // 符合（status不为null）
		{"deviceId": "sensor2", "value": nil, "status": nil},         // 符合（value为null）
		{"deviceId": "sensor3", "value": 30.0, "status": "inactive"}, // 符合（status不为null）
		{"deviceId": "sensor4", "value": nil, "status": "error"},     // 符合（两个条件都满足）
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(300 * time.Millisecond)
	fmt.Println()
}

func demo4() {
	fmt.Println("4. 混合 null 比较语法演示")
	fmt.Println("-------------------------------------------")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 混合使用 IS NULL、= nil、!= null 等语法
	rsql := `SELECT deviceId, value, status, priority 
	         FROM stream 
	         WHERE (value IS NOT NULL AND value > 20) OR 
	               (status = nil AND priority != null)`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result []map[string]interface{}) {
		for _, data := range result {
			value := data["value"]
			status := data["status"]
			priority := data["priority"]

			if value != nil && value.(float64) > 20 {
				fmt.Printf("高值数据 (value > 20): %+v\n", data)
			} else if status == nil && priority != nil {
				fmt.Printf("状态异常但有优先级的数据: %+v\n", data)
			}
		}
	})

	testData := []map[string]interface{}{
		{"deviceId": "sensor1", "value": 25.0, "status": "active", "priority": "high"}, // 符合第一个条件
		{"deviceId": "sensor2", "value": 15.0, "status": "active", "priority": "low"},  // 不符合
		{"deviceId": "sensor3", "value": nil, "status": nil, "priority": "medium"},     // 符合第二个条件
		{"deviceId": "sensor4", "value": nil, "status": nil, "priority": nil},          // 不符合
		{"deviceId": "sensor5", "value": 30.0, "status": "inactive", "priority": nil},  // 符合第一个条件
		{"deviceId": "sensor6", "value": 10.0, "status": nil, "priority": "urgent"},    // 符合第二个条件
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(300 * time.Millisecond)
	fmt.Println()
}

func demo5() {
	fmt.Println("5. 嵌套字段 null 比较演示")
	fmt.Println("-------------------------------------------")

	ssql := streamsql.New()
	defer ssql.Stop()

	// 嵌套字段的 null 比较
	rsql := `SELECT deviceId, device.location 
	         FROM stream 
	         WHERE device.location != nil`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result []map[string]interface{}) {
		for _, data := range result {
			fmt.Printf("有位置信息的设备: %+v\n", data)
		}
	})

	testData := []map[string]interface{}{
		{
			"deviceId": "sensor1",
			"device": map[string]interface{}{
				"location": "warehouse-A",
			},
		}, // 符合条件
		{
			"deviceId": "sensor2",
			"device": map[string]interface{}{
				"location": nil,
			},
		}, // 不符合（location为nil）
		{
			"deviceId": "sensor3",
			"device":   map[string]interface{}{},
		}, // 不符合（location字段不存在）
		{
			"deviceId": "sensor4",
			"device": map[string]interface{}{
				"location": "office-B",
			},
		}, // 符合条件
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	time.Sleep(300 * time.Millisecond)
	fmt.Println()

	fmt.Println("=== Null 比较语法演示完成 ===")
	fmt.Println()
	fmt.Println("支持的 null 比较语法:")
	fmt.Println("- fieldName IS NULL")
	fmt.Println("- fieldName IS NOT NULL")
	fmt.Println("- fieldName = nil")
	fmt.Println("- fieldName != nil")
	fmt.Println("- fieldName = null")
	fmt.Println("- fieldName != null")
	fmt.Println("- device.field = nil (嵌套字段)")
	fmt.Println("- device.field != nil (嵌套字段)")
}
