package main

import (
	"fmt"
	"time"

	"github.com/rulego/streamsql"
)

func main() {
	fmt.Println("=== StreamSQL Null 比较语法演示 ===")
	fmt.Println()

	demo1() // fieldName = nil syntax
	demo2() // fieldName!= nil syntax
	demo3() // fieldName = null and!= null syntax
	demo4() // Mixed grammar demonstration
	demo5() // Nested field null comparison
}

func demo1() {
	fmt.Println("1. fieldName = nil 语法演示")
	fmt.Println("-------------------------------------------")

	ssql := streamsql.New()
	defer ssql.Stop()

	// Use the = nil syntax to find null values
	rsql := `SELECT deviceId, value, status 
	         FROM stream 
	         WHERE value = nil`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result []map[string]any) {
		for _, data := range result {
			fmt.Printf("发现空值数据: %+v\n", data)
		}
	})

	testData := []map[string]any{
		{"deviceId": "sensor1", "value": 25.5, "status": "active"},
		{"deviceId": "sensor2", "value": nil, "status": "active"}, // Meet the requirements
		{"deviceId": "sensor3", "value": 30.0, "status": "inactive"},
		{"deviceId": "sensor4", "value": nil, "status": "error"}, // Meet the requirements
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

	// Use the!= nil syntax to find non-null values
	rsql := `SELECT deviceId, value, status 
	         FROM stream 
	         WHERE value != nil AND value > 20`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result []map[string]any) {
		for _, data := range result {
			fmt.Printf("发现有效数据: %+v\n", data)
		}
	})

	testData := []map[string]any{
		{"deviceId": "sensor1", "value": 25.5, "status": "active"},   // Meet the requirements
		{"deviceId": "sensor2", "value": nil, "status": "active"},    // Nonconforming (null)
		{"deviceId": "sensor3", "value": 15.0, "status": "inactive"}, // Not compliant (value <=20)
		{"deviceId": "sensor4", "value": 30.0, "status": "error"},    // Meet the requirements
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

	// Use the = null and!= null syntax
	rsql := `SELECT deviceId, value, status 
	         FROM stream 
	         WHERE status != null OR value = null`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result []map[string]any) {
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

	testData := []map[string]any{
		{"deviceId": "sensor1", "value": 25.5, "status": "active"},   // Compliance (status not null)
		{"deviceId": "sensor2", "value": nil, "status": nil},         // Compliant (value is null)
		{"deviceId": "sensor3", "value": 30.0, "status": "inactive"}, // Compliance (status not null)
		{"deviceId": "sensor4", "value": nil, "status": "error"},     // Meets (both conditions met)
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

	// Syntax such as IS NULL, = nil,!= null is used together
	rsql := `SELECT deviceId, value, status, priority 
	         FROM stream 
	         WHERE (value IS NOT NULL AND value > 20) OR 
	               (status = nil AND priority != null)`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result []map[string]any) {
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

	testData := []map[string]any{
		{"deviceId": "sensor1", "value": 25.0, "status": "active", "priority": "high"}, // Meet the first condition
		{"deviceId": "sensor2", "value": 15.0, "status": "active", "priority": "low"},  // It does not fit
		{"deviceId": "sensor3", "value": nil, "status": nil, "priority": "medium"},     // Meet the second condition
		{"deviceId": "sensor4", "value": nil, "status": nil, "priority": nil},          // It does not fit
		{"deviceId": "sensor5", "value": 30.0, "status": "inactive", "priority": nil},  // Meet the first condition
		{"deviceId": "sensor6", "value": 10.0, "status": nil, "priority": "urgent"},    // Meet the second condition
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

	// null comparison of nested fields
	rsql := `SELECT deviceId, device.location 
	         FROM stream 
	         WHERE device.location != nil`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(result []map[string]any) {
		for _, data := range result {
			fmt.Printf("有位置信息的设备: %+v\n", data)
		}
	})

	testData := []map[string]any{
		{
			"deviceId": "sensor1",
			"device": map[string]any{
				"location": "warehouse-A",
			},
		}, // Meet the requirements
		{
			"deviceId": "sensor2",
			"device": map[string]any{
				"location": nil,
			},
		}, // Not compliant (location is nil)
		{
			"deviceId": "sensor3",
			"device":   map[string]any{},
		}, // Does not meet (location field does not exist)
		{
			"deviceId": "sensor4",
			"device": map[string]any{
				"location": "office-B",
			},
		}, // Meet the requirements
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
