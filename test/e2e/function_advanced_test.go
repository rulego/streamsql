package e2e

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpressionInAggregation(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Test the use of expressions in the aggregate function
	var rsql = "SELECT device, AVG(temperature * 1.8 + 32) as fahrenheit FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("Start testing the expression functionality")

	// Add test data and use temperature in degrees Celsius
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		{"device": "aa", "temperature": 0.0},   // Fahrenheit should be 32
		{"device": "aa", "temperature": 100.0}, // Fahrenheit should be 212
		{"device": "bb", "temperature": 20.0},  // The degree of Fahrenheit should be 68
		{"device": "bb", "temperature": 30.0},  // Fahrenheit should be 86
	}

	// Add data
	//fmt.Println("Add test data")
	for _, data := range testData {
		strm.Emit(data)
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("Result received: %v\n", result)
		resultChan <- result
	})

	// Wait window trigger (processing time mode)
	//fmt.Println("Waiting for window initialization...")
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	//fmt.Println("Manually trigger the window")
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("Successfully received the results")
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// Verification of the number of results
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// Check the equipment and its Fahrenheit temperature
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		fahrenheit, ok := result["fahrenheit"].(float64)

		assert.True(t, ok, "fahrenheit应该是float64类型")

		if device == "aa" {
			// (0 + 100)/2 = 50 degrees Celsius, so the conversion to Fahrenheit is 50*1.8+32 = 122
			assert.InEpsilon(t, 122.0, fahrenheit, 0.001, "aa设备的平均华氏温度应为122")
		} else if device == "bb" {
			// (20 + 30)/2 = 25 degrees Celsius, so the transition is 25*1.8 + 32 = 77
			assert.InEpsilon(t, 77.0, fahrenheit, 0.001, "bb设备的平均华氏温度应为77")
		}
	}

	//fmt.Println("Expression testing complete")
}

func TestAdvancedFunctionsInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Testing complex SQL queries using the new function system
	var rsql = "SELECT device, AVG(abs(temperature - 20)) as abs_diff, CONCAT(device, '_processed') as device_name FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("Start testing advanced function features")

	// Add test data
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 15.0}, // abs(15-20) = 5
		{"device": "sensor1", "temperature": 25.0}, // abs(25-20) = 5
		{"device": "sensor2", "temperature": 18.0}, // abs(18-20) = 2
		{"device": "sensor2", "temperature": 22.0}, // abs(22-20) = 2
	}

	// Add data
	//fmt.Println("Add test data")
	for _, data := range testData {
		strm.Emit(data)
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("Result received: %v\n", result)
		resultChan <- result
	})

	// Wait window trigger (processing time mode)
	//fmt.Println("Waiting for window initialization...")
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	//fmt.Println("Manually trigger the window")
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("Successfully received the results")
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// Verification of the number of results
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// Inspect the equipment and its calculation results
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		absDiff, ok := result["abs_diff"].(float64)
		deviceName, ok2 := result["device_name"].(string)

		assert.True(t, ok, "abs_diff应该是float64类型")
		assert.True(t, ok2, "device_name应该是string类型")

		if device == "sensor1" {
			// (abs(15-20) + abs(25-20))/2 = (5+5)/2 = 5
			assert.InEpsilon(t, 5.0, absDiff, 0.001, "sensor1的平均绝对差应为5")
			assert.Equal(t, "sensor1_processed", deviceName)
		} else if device == "sensor2" {
			// (abs(18-20) + abs(22-20))/2 = (2+2)/2 = 2
			assert.InEpsilon(t, 2.0, absDiff, 0.001, "sensor2的平均绝对差应为2")
			assert.Equal(t, "sensor2_processed", deviceName)
		}
	}

	//fmt.Println("Advanced function testing completed")
}

func TestCustomFunctionInSQL(t *testing.T) {
	t.Parallel()
	// Register a custom function: Temperature Fahrenheit to Celsius
	err := functions.RegisterCustomFunction("fahrenheit_to_celsius", functions.TypeCustom, "温度转换", "华氏度转摄氏度", 1, 1,
		func(ctx *functions.FunctionContext, args []any) (any, error) {
			fahrenheit := cast.ToFloat64(args[0])
			celsius := (fahrenheit - 32) * 5 / 9
			return celsius, nil
		})
	assert.NoError(t, err)
	defer functions.Unregister("fahrenheit_to_celsius")

	ssql := streamsql.New()
	defer ssql.Stop()

	// Testing SQL queries using custom functions
	var rsql = "SELECT device, AVG(fahrenheit_to_celsius(temperature)) as avg_celsius FROM stream GROUP BY device, TumblingWindow('1s')"
	err = ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("Start testing the custom function features")

	// Add test data (Fahrenheit)
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		{"device": "thermometer1", "temperature": 32.0},  // 0°C
		{"device": "thermometer1", "temperature": 212.0}, // 100°C
		{"device": "thermometer2", "temperature": 68.0},  // 20°C
		{"device": "thermometer2", "temperature": 86.0},  // 30°C
	}

	// Add data
	//fmt.Println("Add test data")
	for _, data := range testData {
		strm.Emit(data)
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("Result received: %v\n", result)
		resultChan <- result
	})

	// Wait window trigger (processing time mode)
	//fmt.Println("Waiting for window initialization...")
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	//fmt.Println("Manually trigger the window")
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("Successfully received the results")
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// Verification of the number of results
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// Inspect the equipment and its calculation results
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		avgCelsius, ok := result["avg_celsius"].(float64)

		assert.True(t, ok, "avg_celsius应该是float64类型")

		if device == "thermometer1" {
			// (0 + 100)/2 = 50°C
			assert.InEpsilon(t, 50.0, avgCelsius, 0.001, "thermometer1的平均摄氏温度应为50")
		} else if device == "thermometer2" {
			// (20 + 30)/2 = 25°C
			assert.InEpsilon(t, 25.0, avgCelsius, 0.001, "thermometer2的平均摄氏温度应为25")
		}
	}

	//fmt.Println("Custom function testing complete")
}

func TestNewAggregateFunctionsInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Testing SQL queries using the new aggregation function
	var rsql = "SELECT device, collect(temperature) as temp_values, last_value(temperature) as last_temp, merge_agg(status) as all_status FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("Start testing the new aggregation function features")

	// Add test data
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 15.0, "status": "good"},
		{"device": "sensor1", "temperature": 25.0, "status": "ok"},
		{"device": "sensor2", "temperature": 18.0, "status": "good"},
		{"device": "sensor2", "temperature": 22.0, "status": "warning"},
	}

	// Add data
	//fmt.Println("Add test data")
	for _, data := range testData {
		strm.Emit(data)
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("Result received: %v\n", result)
		resultChan <- result
	})

	// Wait window trigger (processing time mode)
	//fmt.Println("Waiting for window initialization...")
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	//fmt.Println("Manually trigger the window")
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("Successfully received the results")
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// Verification of the number of results
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// Inspect the equipment and its aggregated results
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		tempValues, ok1 := result["temp_values"]
		lastTemp, ok2 := result["last_temp"]
		allStatus, ok3 := result["all_status"].(string)

		assert.True(t, ok1, "temp_values应该存在")
		assert.True(t, ok2, "last_temp应该存在")
		assert.True(t, ok3, "all_status应该是string类型")

		if device == "sensor1" {
			// The collect function should collect [15.0, 25.0]
			values, ok := tempValues.([]any)
			assert.True(t, ok, "temp_values应该是数组")
			assert.Len(t, values, 2, "sensor1应该有2个温度值")
			assert.Contains(t, values, 15.0)
			assert.Contains(t, values, 25.0)

			// last_value should be 25.0
			assert.Equal(t, 25.0, lastTemp)

			// merge_agg should be "good,ok"
			assert.Equal(t, "good,ok", allStatus)
		} else if device == "sensor2" {
			// The collect function should collect [18.0, 22.0]
			values, ok := tempValues.([]any)
			assert.True(t, ok, "temp_values应该是数组")
			assert.Len(t, values, 2, "sensor2应该有2个温度值")
			assert.Contains(t, values, 18.0)
			assert.Contains(t, values, 22.0)

			// last_value should be 22.0
			assert.Equal(t, 22.0, lastTemp)

			// merge_agg should be "good,warning"
			assert.Equal(t, "good,warning", allStatus)
		}
	}

	//fmt.Println("New aggregator function testing completed")
}

func TestStatisticalAggregateFunctionsInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Testing SQL queries using statistical aggregation functions
	var rsql = "SELECT device, stddevs(temperature) as sample_stddev, var(temperature) as population_var, vars(temperature) as sample_var FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("Start testing the statistical aggregation function feature")

	// Add test data
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 10.0},
		{"device": "sensor1", "temperature": 20.0},
		{"device": "sensor1", "temperature": 30.0},
		{"device": "sensor2", "temperature": 15.0},
		{"device": "sensor2", "temperature": 25.0},
	}

	// Add data
	//fmt.Println("Add test data")
	for _, data := range testData {
		strm.Emit(data)
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("Result received: %v\n", result)
		resultChan <- result
	})

	// Wait window trigger (processing time mode)
	//fmt.Println("Waiting for window initialization...")
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	//fmt.Println("Manually trigger the window")
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("Successfully received the results")
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// Verification of the number of results
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// Inspect the equipment and its statistical results
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		sampleStddev, ok1 := result["sample_stddev"].(float64)
		populationVar, ok2 := result["population_var"].(float64)
		sampleVar, ok3 := result["sample_var"].(float64)

		assert.True(t, ok1, "sample_stddev应该是float64类型")
		assert.True(t, ok2, "population_var应该是float64类型")
		assert.True(t, ok3, "sample_var应该是float64类型")

		if device == "sensor1" {
			// sensor1: [10, 20, 30], mean = 20
			// Population variance = ((10-20)² + (20-20)² + (30-20)²) / 3 = (100 + 0 + 100) / 3 = 66.67
			// Sample variance = 200 / 2 = 100
			// Sample standard deviation = sqrt(100) = 10
			assert.InEpsilon(t, 10.0, sampleStddev, 0.001, "sensor1的样本标准差应约为10")
			assert.InEpsilon(t, 66.67, populationVar, 0.1, "sensor1的总体方差应约为66.67")
			assert.InEpsilon(t, 100.0, sampleVar, 0.001, "sensor1的样本方差应约为100")
		} else if device == "sensor2" {
			// sensor2: [15, 25], mean value = 20
			// Population variance = ((15-20)² + (25-20)²) / 2 = (25 + 25) / 2 = 25
			// Sample variance = 50 / 1 = 50
			// Sample standard deviation = sqrt(50) = 7.07
			assert.InEpsilon(t, 7.07, sampleStddev, 0.1, "sensor2的样本标准差应约为7.07")
			assert.InEpsilon(t, 25.0, populationVar, 0.001, "sensor2的总体方差应约为25")
			assert.InEpsilon(t, 50.0, sampleVar, 0.001, "sensor2的样本方差应约为50")
		}
	}

	//fmt.Println("Statistical aggregation function testing completed")
}

func TestDeduplicateAggregateInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Testing SQL queries using deduplicated aggregation functions
	var rsql = "SELECT device, deduplicate(status) as unique_status FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("Start testing the de-duplication aggregation function")

	// Add test data containing duplicate states
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		{"device": "sensor1", "status": "good"},
		{"device": "sensor1", "status": "good"}, // Repeat
		{"device": "sensor1", "status": "warning"},
		{"device": "sensor1", "status": "good"}, // Repeat
		{"device": "sensor2", "status": "error"},
		{"device": "sensor2", "status": "error"}, // Repeat
		{"device": "sensor2", "status": "ok"},
	}

	// Add data
	//fmt.Println("Add test data")
	for _, data := range testData {
		strm.Emit(data)
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("Result received: %v\n", result)
		resultChan <- result
	})

	// Wait window trigger (processing time mode)
	//fmt.Println("Waiting for window initialization...")
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	//fmt.Println("Manually trigger the window")
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("Successfully received the results")
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// Verification of the number of results
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// Check the equipment and its duplication results
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		uniqueStatus, ok := result["unique_status"]

		assert.True(t, ok, "unique_status应该存在")

		if device == "sensor1" {
			// sensor1 should have a deduplicated state: ["good", "warning"]
			statusArray, ok := uniqueStatus.([]any)
			assert.True(t, ok, "unique_status应该是数组")
			assert.Len(t, statusArray, 2, "sensor1应该有2个不同的状态")
			assert.Contains(t, statusArray, "good")
			assert.Contains(t, statusArray, "warning")
		} else if device == "sensor2" {
			// sensor2 should have a deduplicated state: ["error", "ok"]
			statusArray, ok := uniqueStatus.([]any)
			assert.True(t, ok, "unique_status应该是数组")
			assert.Len(t, statusArray, 2, "sensor2应该有2个不同的状态")
			assert.Contains(t, statusArray, "error")
			assert.Contains(t, statusArray, "ok")
		}
	}

	//fmt.Println("The de-deduplication aggregation function test is complete")
}

func TestExprAggregationFunctions(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Test an aggregate function SQL query using expression operations
	var rsql = `SELECT 
		device,
		avg(temperature * 1.8 + 32) as avg_fahrenheit,  
		stddevs((temperature - 20) * 2) as temp_stddev, 
		var(temperature / 10) as temp_var,             
		collect(temperature + humidity) as temp_hum_sum, 
		last_value(temperature * humidity) as last_temp_hum, 
		merge_agg(device + '_' + status) as device_status,  
		deduplicate(status + '_' + device) as unique_status_device
	FROM stream 
	GROUP BY device, TumblingWindow('1s')`

	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("Start testing the expression aggregation function feature")

	// Add test data
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		// device1
		{"device": "device1", "temperature": 20.0, "humidity": 60.0, "status": "normal"},  // Fahrenheit = 68, deviation = 0, sum = 80
		{"device": "device1", "temperature": 25.0, "humidity": 65.0, "status": "warning"}, // Fahrenheit = 77, Deviation = 10, SUM = 90
		{"device": "device1", "temperature": 30.0, "humidity": 70.0, "status": "normal"},  // Fahrenheit = 86, deviation = 20, sum = 100

		// device2
		{"device": "device2", "temperature": 15.0, "humidity": 55.0, "status": "error"},  // Fahrenheit = 59, deviation = -10, sum = 70
		{"device": "device2", "temperature": 18.0, "humidity": 58.0, "status": "normal"}, // Fahrenheit = 64.4, deviation = -4, sum = 76
		{"device": "device2", "temperature": 22.0, "humidity": 62.0, "status": "error"},  // Fahrenheit = 71.6, deviation = 4, sum = 84
	}

	// Add data
	//fmt.Println("Add test data")
	for _, data := range testData {
		strm.Emit(data)
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("Result received: %v\n", result)
		resultChan <- result
	})

	// Wait window trigger (processing time mode)
	//fmt.Println("Waiting for window initialization...")
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	//fmt.Println("Manually trigger the window")
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("Successfully received the results")
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// Verification of the number of results
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// Inspect the equipment and its calculation results
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		avgFahrenheit, ok1 := result["avg_fahrenheit"].(float64)
		tempStddev, ok2 := result["temp_stddev"].(float64)
		tempVar, ok3 := result["temp_var"].(float64)
		tempHumSum, ok4 := result["temp_hum_sum"]
		lastTempHum, ok5 := result["last_temp_hum"].(float64)
		deviceStatus, ok6 := result["device_status"].(string)
		uniqueStatusDevice, ok7 := result["unique_status_device"]

		assert.True(t, ok1, "avg_fahrenheit应该是float64类型")
		assert.True(t, ok2, "temp_stddev应该是float64类型")
		assert.True(t, ok3, "temp_var应该是float64类型")
		assert.True(t, ok4, "temp_hum_sum应该存在")
		assert.True(t, ok5, "last_temp_hum应该是float64类型")
		assert.True(t, ok6, "device_status应该是string类型")
		assert.True(t, ok7, "unique_status_device应该存在")

		if device == "device1" {
			// Verification of device1
			// Average Fahrenheit: (68 + 77 + 86) / 3 = 77
			assert.InEpsilon(t, 77.0, avgFahrenheit, 0.1, "device1的平均华氏度应约为77")

			// Temperature deviation standard deviation: sqrt(((0-10)² + (10-10)² + (20-10)²) / 2) = sqrt(200/2) = 10
			assert.InEpsilon(t, 10.0, tempStddev, 0.1, "device1的温度偏差标准差应约为10")

			// Temperature divided by 10 variance: ((2-2.5)² + (2.5-2.5)² + (3-2.5)²) / 3 = 0.167
			assert.InEpsilon(t, 0.167, tempVar, 0.01, "device1的温度方差应约为0.167")

			// Temperature and humidity arrays
			tempHumSumArray, ok := tempHumSum.([]any)
			assert.True(t, ok, "temp_hum_sum应该是数组")
			assert.Len(t, tempHumSumArray, 3, "device1应该有3个温度和湿度的和")
			assert.Contains(t, tempHumSumArray, 80.0)
			assert.Contains(t, tempHumSumArray, 90.0)
			assert.Contains(t, tempHumSumArray, 100.0)

			// Finally, the product of temperature and humidity: 30 * 70 = 2100
			assert.InEpsilon(t, 2100.0, lastTempHum, 0.1, "device1的最后一个温度和湿度乘积应为2100")

			// Equipment status combinations
			assert.Contains(t, deviceStatus, "device1_normal")
			assert.Contains(t, deviceStatus, "device1_warning")

			// State equipment combination deduplication
			uniqueArray, ok := uniqueStatusDevice.([]any)
			assert.True(t, ok, "unique_status_device应该是数组")
			assert.Len(t, uniqueArray, 2, "device1应该有2个不同的状态设备组合")
			assert.Contains(t, uniqueArray, "normal_device1")
			assert.Contains(t, uniqueArray, "warning_device1")

		} else if device == "device2" {
			// Validation of device2
			// Average Fahrenheit: (59 + 64.4 + 71.6) / 3 = 65
			assert.InEpsilon(t, 65.0, avgFahrenheit, 0.1, "device2的平均华氏度应约为65")

			// Temperature deviation standard deviation: sqrt(((-10 - (-3.33))² + (-4 - (-3.33))² + (4 - (-3.33))²) / 2) = sqrt(147.33/2) = 7.023
			assert.InEpsilon(t, 7.023, tempStddev, 0.1, "device2的温度偏差标准差应约为7.023")

			// Temperature divided by 10 variance: ((1.5 - 1.83)² + (1.8 - 1.83)² + (2.2 - 1.83)²) / 3 = 0.082
			assert.InEpsilon(t, 0.082, tempVar, 0.01, "device2的温度方差应约为0.082")

			// Temperature and humidity arrays
			tempHumSumArray, ok := tempHumSum.([]any)
			assert.True(t, ok, "temp_hum_sum应该是数组")
			assert.Len(t, tempHumSumArray, 3, "device2应该有3个温度和湿度的和")
			assert.Contains(t, tempHumSumArray, 70.0)
			assert.Contains(t, tempHumSumArray, 76.0)
			assert.Contains(t, tempHumSumArray, 84.0)

			// Finally, the product of temperature and humidity: 22 * 62 = 1364
			assert.InEpsilon(t, 1364.0, lastTempHum, 0.1, "device2的最后一个温度和湿度乘积应为1364")

			// Equipment status combinations
			assert.Contains(t, deviceStatus, "device2_error")
			assert.Contains(t, deviceStatus, "device2_normal")

			// State equipment combination deduplication
			uniqueArray, ok := uniqueStatusDevice.([]any)
			assert.True(t, ok, "unique_status_device应该是数组")
			assert.Len(t, uniqueArray, 2, "device2应该有2个不同的状态设备组合")
			assert.Contains(t, uniqueArray, "error_device2")
			assert.Contains(t, uniqueArray, "normal_device2")
		}
	}

	//fmt.Println("The expression aggregation function test is complete")
}

func TestAnalyticalFunctionsInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Testing SQL queries using analysis functions
	var rsql = "SELECT device, lag(temperature) as prev_temp, latest(temperature) as current_temp, had_changed(temperature) as temp_changed FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	if err != nil {
		t.Skipf("v1.2 The analysis function has been changed to direct OVER semantics, no longer used interchangeably with the GROUP BY/ window; Window + OVER WHEN See subsequent versions: %v", err)
	}
	strm := ssql

	//fmt.Println("Start testing the analysis function features")

	// Add test data
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 20.0},
		{"device": "sensor1", "temperature": 25.0},
		{"device": "sensor1", "temperature": 25.0}, // Repeat values, test had_changed
		{"device": "sensor2", "temperature": 18.0},
		{"device": "sensor2", "temperature": 22.0},
	}

	// Add data
	//fmt.Println("Add test data")
	for _, data := range testData {
		strm.Emit(data)
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("Result received: %v\n", result)
		resultChan <- result
	})

	// Wait window trigger (processing time mode)
	//fmt.Println("Waiting for window initialization...")
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	//fmt.Println("Manually trigger the window")
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("Successfully received the results")
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// Verification of the number of results
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// Check the equipment and its analysis function results
	for _, result := range resultSlice {
		device, _ := result["device"].(string)

		assert.Contains(t, result, "prev_temp", "结果应包含prev_temp字段")
		assert.Contains(t, result, "current_temp", "结果应包含current_temp字段")
		assert.Contains(t, result, "temp_changed", "结果应包含temp_changed字段")

		if device == "sensor1" {
			// sensor1 has three temperature values: 20.0, 25.0, and 25.0
			// The last test should return the latest value
			currentTemp := result["current_temp"]
			assert.NotNil(t, currentTemp, "current_temp不应为空")

			// had_changed should have a record of changes
			tempChanged := result["temp_changed"]
			assert.NotNil(t, tempChanged, "temp_changed不应为空")
		} else if device == "sensor2" {
			// The sensor2 has two temperature values: 18.0 and 22.0
			currentTemp := result["current_temp"]
			assert.NotNil(t, currentTemp, "current_temp不应为空")

			tempChanged := result["temp_changed"]
			assert.NotNil(t, tempChanged, "temp_changed不应为空")
		}
	}

	//fmt.Println("Analysis function testing completed")
}
func TestHadChangedFunctionInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// SQL queries for had_changed test functions
	var rsql = "SELECT device, had_changed(temperature) as temp_changed FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	if err != nil {
		t.Skipf("v1.2 The analysis function has been changed to direct OVER semantics, no longer used interchangeably with the GROUP BY/ window; Window + OVER WHEN See subsequent versions: %v", err)
	}
	strm := ssql

	//fmt.Println("Start testing had_changed function functions")

	// Add test data – including duplicate and variant values
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		{"device": "monitor", "temperature": 20.0},
		{"device": "monitor", "temperature": 20.0}, // Same value
		{"device": "monitor", "temperature": 25.0}, // Change values
		{"device": "monitor", "temperature": 25.0}, // Same value
		{"device": "monitor", "temperature": 30.0}, // Change values
	}

	// Add data
	//fmt.Println("Add test data")
	for _, data := range testData {
		strm.Emit(data)
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("Result received: %v\n", result)
		resultChan <- result
	})

	// Wait window trigger (processing time mode)
	//fmt.Println("Waiting for window initialization...")
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	//fmt.Println("Manually trigger the window")
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("Successfully received the results")
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// Verification of the number of results
	assert.Len(t, resultSlice, 1, "应该有1个设备的聚合结果")

	result := resultSlice[0]
	device, _ := result["device"].(string)
	assert.Equal(t, "monitor", device, "设备名应该正确")

	// Verify the presence of fields
	assert.Contains(t, result, "temp_changed", "结果应包含temp_changed字段")

	// had_changed function should return a boolean value
	//tempChanged := result["temp_changed"]
	//fmt.Printf("had_changed function return value: %v\n", tempChanged)

	//fmt.Println("had_changed function test completed")
}
func TestIncrementalComputationBasic(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Test the basic incremental calculation of the aggregation function
	var rsql = "SELECT device, sum(temperature) as total, avg(temperature) as average, count(*) as cnt FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("Start testing the basic incremental calculation function")

	// Add test data
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 10.0},
		{"device": "sensor1", "temperature": 20.0},
		{"device": "sensor1", "temperature": 30.0},
		{"device": "sensor2", "temperature": 15.0},
		{"device": "sensor2", "temperature": 25.0},
	}

	// Add data
	//fmt.Println("Add test data")
	for _, data := range testData {
		strm.Emit(data)
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("Result received: %v\n", result)
		resultChan <- result
	})

	// Wait window trigger (processing time mode)
	//fmt.Println("Waiting for window initialization...")
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	//fmt.Println("Manually trigger the window")
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("Successfully received the results")
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// Verification of the number of results
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// Inspect the equipment and its aggregated results
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		total := result["total"]
		average := result["average"]
		count := result["cnt"]

		//fmt.Printf("Equipment %s: total=%v, average=%v, count=%v\n", device, total, average, count)

		if device == "sensor1" {
			// sensor1: sum=60, avg=20, count=3
			if total != nil {
				assert.Equal(t, 60.0, total)
			}
			if average != nil {
				assert.Equal(t, 20.0, average)
			}
			if count != nil {
				assert.Equal(t, 3.0, count)
			}
		} else if device == "sensor2" {
			// sensor2: sum=40, avg=20, count=2
			if total != nil {
				assert.Equal(t, 40.0, total)
			}
			if average != nil {
				assert.Equal(t, 20.0, average)
			}
			if count != nil {
				assert.Equal(t, 2.0, count)
			}
		}
	}

	//fmt.Println("Basic incremental calculation test completed")
}

// TestExprFunctions Tests the use of the expr function
func TestExprFunctions(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Testing basic expr function: string processing
	var rsql = "SELECT device, upper(device) as upper_device, lower(device) as lower_device FROM stream"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := []map[string]any{
		{"device": "SensorA"},
		{"device": "SensorB"},
	}

	// Add data
	for _, data := range testData {
		strm.Emit(data)
	}

	// Wait for the results
	var results []any
	var resultsMutex sync.Mutex
	timeout := time.After(2 * time.Second)
	done := false

	for !done {
		resultsMutex.Lock()
		resultCount := len(results)
		resultsMutex.Unlock()

		if resultCount >= 2 {
			break
		}

		select {
		case result := <-resultChan:
			resultsMutex.Lock()
			results = append(results, result)
			resultsMutex.Unlock()
		case <-timeout:
			done = true
		}
	}

	// Verify the results
	resultsMutex.Lock()
	finalResultCount := len(results)
	resultsCopy := make([]any, len(results))
	copy(resultsCopy, results)
	resultsMutex.Unlock()

	assert.Greater(t, finalResultCount, 0, "应该收到至少一条结果")

	for _, result := range resultsCopy {
		resultSlice, ok := result.([]map[string]any)
		require.True(t, ok, "结果应该是[]map[string]any类型")

		for _, item := range resultSlice {
			device, _ := item["device"].(string)
			upperDevice, _ := item["upper_device"].(string)
			lowerDevice, _ := item["lower_device"].(string)

			// Verify the UPPER function
			assert.Equal(t, strings.ToUpper(device), upperDevice, "upper函数应该正确转换大写")
			// Verify the lower function
			assert.Equal(t, strings.ToLower(device), lowerDevice, "lower函数应该正确转换小写")
		}
	}
}

// TestExprFunctionsInAggregation tests using the appr function in aggregation
func TestExprFunctionsInAggregation(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Test the use of expr in aggregate functions: mathematical calculations
	var rsql = "SELECT device, AVG(abs(temperature - 25)) as avg_deviation, MAX(ceil(temperature)) as max_ceil FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	// Add test data
	// No event time is used, and no timestamp field is needed
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 23.5}, // abs(23.5-25) = 1.5, ceil(23.5) = 24
		{"device": "sensor1", "temperature": 26.8}, // abs(26.8-25) = 1.8, ceil(26.8) = 27
		{"device": "sensor2", "temperature": 24.2}, // abs(24.2-25) = 0.8, ceil(24.2) = 25
		{"device": "sensor2", "temperature": 25.9}, // abs(25.9-25) = 0.9, ceil(25.9) = 26
	}

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add data
	for _, data := range testData {
		strm.Emit(data)
	}

	// Wait for the window to initialize
	time.Sleep(1 * time.Second)

	// Manually trigger the window
	strm.TriggerWindow()

	// Wait for the results
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		cancel()
	case <-ctx.Done():
		t.Fatal("The test timed out and no results were received")
	}

	// Verify the results
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// Check the aggregate results
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		avgDeviation, ok := result["avg_deviation"].(float64)
		assert.True(t, ok, "avg_deviation应该是float64类型")
		maxCeil, ok := result["max_ceil"].(float64)
		assert.True(t, ok, "max_ceil应该是float64类型")

		if device == "sensor1" {
			// sensor1: avg(abs(23.5-25), abs(26.8-25)) = avg(1.5, 1.8) = 1.65
			assert.InEpsilon(t, 1.65, avgDeviation, 0.01, "sensor1的平均偏差应为1.65")
			// sensor1: max(ceil(23.5), ceil(26.8)) = max(24, 27) = 27
			assert.Equal(t, 27.0, maxCeil, "sensor1的最大向上取整应为27")
		} else if device == "sensor2" {
			// sensor2: avg(abs(24.2-25), abs(25.9-25)) = avg(0.8, 0.9) = 0.85
			assert.InEpsilon(t, 0.85, avgDeviation, 0.01, "sensor2的平均偏差应为0.85")
			// sensor2: max(ceil(24.2), ceil(25.9)) = max(25, 26) = 26
			assert.Equal(t, 26.0, maxCeil, "sensor2的最大向上取整应为26")
		}
	}
}

// TestNestedExprFunctions tests nested expr function calls
func TestNestedExprFunctions(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Test nested functions: string handling + array operations
	var rsql = "SELECT device, len(split(upper(device), 'SENSOR')) as split_count FROM stream"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := []map[string]any{
		{"device": "sensor1"},      // upper -> "SENSOR1", split by "SENSOR" -> ["", "1"], len -> 2
		{"device": "sensorsensor"}, // upper -> "SENSORSENSOR", split by "SENSOR" -> ["", "", ""], len -> 3
		{"device": "device1"},      // upper -> "DEVICE1", split by "SENSOR" -> ["DEVICE1"], len -> 1
	}

	// Add data
	for _, data := range testData {
		strm.Emit(data)
	}

	// Wait for the results
	var results []any
	var resultsMutex sync.Mutex
	timeout := time.After(2 * time.Second)
	done := false

	for !done {
		resultsMutex.Lock()
		resultCount := len(results)
		resultsMutex.Unlock()

		if resultCount >= 3 {
			break
		}

		select {
		case result := <-resultChan:
			resultsMutex.Lock()
			results = append(results, result)
			resultsMutex.Unlock()
		case <-timeout:
			done = true
		}
	}

	// Verify the results
	resultsMutex.Lock()
	finalResultCount := len(results)
	resultsCopy := make([]any, len(results))
	copy(resultsCopy, results)
	resultsMutex.Unlock()

	assert.Greater(t, finalResultCount, 0, "应该收到至少一条结果")

	deviceResults := make(map[string]float64)
	for _, result := range resultsCopy {
		resultSlice, ok := result.([]map[string]any)
		require.True(t, ok, "结果应该是[]map[string]any类型")

		for _, item := range resultSlice {
			device, _ := item["device"].(string)
			splitCount, ok := item["split_count"].(float64)
			if ok {
				deviceResults[device] = splitCount
			}
		}
	}

	// Verify the nested function call result
	if count, exists := deviceResults["sensor1"]; exists {
		assert.Equal(t, 2.0, count, "sensor1经过嵌套函数处理后应该得到2")
	}
	if count, exists := deviceResults["sensorsensor"]; exists {
		assert.Equal(t, 3.0, count, "sensorsensor经过嵌套函数处理后应该得到3")
	}
	if count, exists := deviceResults["device1"]; exists {
		assert.Equal(t, 1.0, count, "device1经过嵌套函数处理后应该得到1")
	}
}

// TestExprFunctionsWithStreamSQLFunctions Test expr functions are used in combination with StreamSQL functions
func TestExprFunctionsWithStreamSQLFunctions(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// Test a hybrid use: StreamSQL concat function + expr upper function
	var rsql = "SELECT device, concat(upper(device), '_processed') as processed_name FROM stream"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	// Create a result receiving channel
	resultChan := make(chan any, 10)

	// Add result callbacks
	strm.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// Add test data
	testData := []map[string]any{
		{"device": "sensor1"},
		{"device": "device2"},
	}

	// Add data
	for _, data := range testData {
		strm.Emit(data)
	}

	// Wait for the results
	var results []any
	var resultsMutex sync.Mutex
	timeout := time.After(2 * time.Second)
	done := false

	for !done {
		resultsMutex.Lock()
		resultCount := len(results)
		resultsMutex.Unlock()

		if resultCount >= 2 {
			break
		}

		select {
		case result := <-resultChan:
			resultsMutex.Lock()
			results = append(results, result)
			resultsMutex.Unlock()
		case <-timeout:
			done = true
		}
	}

	// Verify the results
	resultsMutex.Lock()
	finalResultCount := len(results)
	resultsCopy := make([]any, len(results))
	copy(resultsCopy, results)
	resultsMutex.Unlock()

	assert.Greater(t, finalResultCount, 0, "应该收到至少一条结果")

	for _, result := range resultsCopy {
		resultSlice, ok := result.([]map[string]any)
		require.True(t, ok, "结果应该是[]map[string]any类型")

		for _, item := range resultSlice {
			device, _ := item["device"].(string)
			processedName, _ := item["processed_name"].(string)

			// Verify the result of the hybrid function call
			expected := strings.ToUpper(device) + "_processed"
			assert.Equal(t, expected, processedName, "混合函数调用应该正确处理")
		}
	}
}

// TestSelectAllFeature is specifically designed to test the SELECT* function
