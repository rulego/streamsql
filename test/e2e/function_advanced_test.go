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

	// 测试在聚合函数中使用表达式
	var rsql = "SELECT device, AVG(temperature * 1.8 + 32) as fahrenheit FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("开始测试表达式功能")

	// 添加测试数据，温度使用摄氏度
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		{"device": "aa", "temperature": 0.0},   // 华氏度应为 32
		{"device": "aa", "temperature": 100.0}, // 华氏度应为 212
		{"device": "bb", "temperature": 20.0},  // 华氏度应为 68
		{"device": "bb", "temperature": 30.0},  // 华氏度应为 86
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.Emit(data)
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口触发（处理时间模式）
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其华氏度温度
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		fahrenheit, ok := result["fahrenheit"].(float64)

		assert.True(t, ok, "fahrenheit应该是float64类型")

		if device == "aa" {
			// (0 + 100)/2 = 50 摄氏度，转华氏度为 50*1.8+32 = 122
			assert.InEpsilon(t, 122.0, fahrenheit, 0.001, "aa设备的平均华氏温度应为122")
		} else if device == "bb" {
			// (20 + 30)/2 = 25 摄氏度，转华氏度为 25*1.8+32 = 77
			assert.InEpsilon(t, 77.0, fahrenheit, 0.001, "bb设备的平均华氏温度应为77")
		}
	}

	//fmt.Println("表达式测试完成")
}

func TestAdvancedFunctionsInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试使用新函数系统的复杂SQL查询
	var rsql = "SELECT device, AVG(abs(temperature - 20)) as abs_diff, CONCAT(device, '_processed') as device_name FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("开始测试高级函数功能")

	// 添加测试数据
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 15.0}, // abs(15-20) = 5
		{"device": "sensor1", "temperature": 25.0}, // abs(25-20) = 5
		{"device": "sensor2", "temperature": 18.0}, // abs(18-20) = 2
		{"device": "sensor2", "temperature": 22.0}, // abs(22-20) = 2
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.Emit(data)
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口触发（处理时间模式）
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其计算结果
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

	//fmt.Println("高级函数测试完成")
}

func TestCustomFunctionInSQL(t *testing.T) {
	t.Parallel()
	// 注册自定义函数：温度华氏度转摄氏度
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

	// 测试使用自定义函数的SQL查询
	var rsql = "SELECT device, AVG(fahrenheit_to_celsius(temperature)) as avg_celsius FROM stream GROUP BY device, TumblingWindow('1s')"
	err = ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("开始测试自定义函数功能")

	// 添加测试数据（华氏度）
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		{"device": "thermometer1", "temperature": 32.0},  // 0°C
		{"device": "thermometer1", "temperature": 212.0}, // 100°C
		{"device": "thermometer2", "temperature": 68.0},  // 20°C
		{"device": "thermometer2", "temperature": 86.0},  // 30°C
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.Emit(data)
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口触发（处理时间模式）
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其计算结果
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

	//fmt.Println("自定义函数测试完成")
}

func TestNewAggregateFunctionsInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试使用新聚合函数的SQL查询
	var rsql = "SELECT device, collect(temperature) as temp_values, last_value(temperature) as last_temp, merge_agg(status) as all_status FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("开始测试新聚合函数功能")

	// 添加测试数据
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 15.0, "status": "good"},
		{"device": "sensor1", "temperature": 25.0, "status": "ok"},
		{"device": "sensor2", "temperature": 18.0, "status": "good"},
		{"device": "sensor2", "temperature": 22.0, "status": "warning"},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.Emit(data)
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口触发（处理时间模式）
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其聚合结果
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		tempValues, ok1 := result["temp_values"]
		lastTemp, ok2 := result["last_temp"]
		allStatus, ok3 := result["all_status"].(string)

		assert.True(t, ok1, "temp_values应该存在")
		assert.True(t, ok2, "last_temp应该存在")
		assert.True(t, ok3, "all_status应该是string类型")

		if device == "sensor1" {
			// collect函数应该收集[15.0, 25.0]
			values, ok := tempValues.([]any)
			assert.True(t, ok, "temp_values应该是数组")
			assert.Len(t, values, 2, "sensor1应该有2个温度值")
			assert.Contains(t, values, 15.0)
			assert.Contains(t, values, 25.0)

			// last_value应该是25.0
			assert.Equal(t, 25.0, lastTemp)

			// merge_agg应该是"good,ok"
			assert.Equal(t, "good,ok", allStatus)
		} else if device == "sensor2" {
			// collect函数应该收集[18.0, 22.0]
			values, ok := tempValues.([]any)
			assert.True(t, ok, "temp_values应该是数组")
			assert.Len(t, values, 2, "sensor2应该有2个温度值")
			assert.Contains(t, values, 18.0)
			assert.Contains(t, values, 22.0)

			// last_value应该是22.0
			assert.Equal(t, 22.0, lastTemp)

			// merge_agg应该是"good,warning"
			assert.Equal(t, "good,warning", allStatus)
		}
	}

	//fmt.Println("新聚合函数测试完成")
}

func TestStatisticalAggregateFunctionsInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试使用统计聚合函数的SQL查询
	var rsql = "SELECT device, stddevs(temperature) as sample_stddev, var(temperature) as population_var, vars(temperature) as sample_var FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("开始测试统计聚合函数功能")

	// 添加测试数据
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 10.0},
		{"device": "sensor1", "temperature": 20.0},
		{"device": "sensor1", "temperature": 30.0},
		{"device": "sensor2", "temperature": 15.0},
		{"device": "sensor2", "temperature": 25.0},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.Emit(data)
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口触发（处理时间模式）
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其统计结果
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		sampleStddev, ok1 := result["sample_stddev"].(float64)
		populationVar, ok2 := result["population_var"].(float64)
		sampleVar, ok3 := result["sample_var"].(float64)

		assert.True(t, ok1, "sample_stddev应该是float64类型")
		assert.True(t, ok2, "population_var应该是float64类型")
		assert.True(t, ok3, "sample_var应该是float64类型")

		if device == "sensor1" {
			// sensor1: [10, 20, 30], 平均值=20
			// 总体方差 = ((10-20)² + (20-20)² + (30-20)²) / 3 = (100 + 0 + 100) / 3 = 66.67
			// 样本方差 = 200 / 2 = 100
			// 样本标准差 = sqrt(100) = 10
			assert.InEpsilon(t, 10.0, sampleStddev, 0.001, "sensor1的样本标准差应约为10")
			assert.InEpsilon(t, 66.67, populationVar, 0.1, "sensor1的总体方差应约为66.67")
			assert.InEpsilon(t, 100.0, sampleVar, 0.001, "sensor1的样本方差应约为100")
		} else if device == "sensor2" {
			// sensor2: [15, 25], 平均值=20
			// 总体方差 = ((15-20)² + (25-20)²) / 2 = (25 + 25) / 2 = 25
			// 样本方差 = 50 / 1 = 50
			// 样本标准差 = sqrt(50) = 7.07
			assert.InEpsilon(t, 7.07, sampleStddev, 0.1, "sensor2的样本标准差应约为7.07")
			assert.InEpsilon(t, 25.0, populationVar, 0.001, "sensor2的总体方差应约为25")
			assert.InEpsilon(t, 50.0, sampleVar, 0.001, "sensor2的样本方差应约为50")
		}
	}

	//fmt.Println("统计聚合函数测试完成")
}

func TestDeduplicateAggregateInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试使用去重聚合函数的SQL查询
	var rsql = "SELECT device, deduplicate(status) as unique_status FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("开始测试去重聚合函数功能")

	// 添加测试数据，包含重复的状态
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		{"device": "sensor1", "status": "good"},
		{"device": "sensor1", "status": "good"}, // 重复
		{"device": "sensor1", "status": "warning"},
		{"device": "sensor1", "status": "good"}, // 重复
		{"device": "sensor2", "status": "error"},
		{"device": "sensor2", "status": "error"}, // 重复
		{"device": "sensor2", "status": "ok"},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.Emit(data)
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口触发（处理时间模式）
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其去重结果
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		uniqueStatus, ok := result["unique_status"]

		assert.True(t, ok, "unique_status应该存在")

		if device == "sensor1" {
			// sensor1应该有去重后的状态：["good", "warning"]
			statusArray, ok := uniqueStatus.([]any)
			assert.True(t, ok, "unique_status应该是数组")
			assert.Len(t, statusArray, 2, "sensor1应该有2个不同的状态")
			assert.Contains(t, statusArray, "good")
			assert.Contains(t, statusArray, "warning")
		} else if device == "sensor2" {
			// sensor2应该有去重后的状态：["error", "ok"]
			statusArray, ok := uniqueStatus.([]any)
			assert.True(t, ok, "unique_status应该是数组")
			assert.Len(t, statusArray, 2, "sensor2应该有2个不同的状态")
			assert.Contains(t, statusArray, "error")
			assert.Contains(t, statusArray, "ok")
		}
	}

	//fmt.Println("去重聚合函数测试完成")
}

func TestExprAggregationFunctions(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试使用表达式运算的聚合函数SQL查询
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

	//fmt.Println("开始测试表达式聚合函数功能")

	// 添加测试数据
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		// device1的数据
		{"device": "device1", "temperature": 20.0, "humidity": 60.0, "status": "normal"},  // 华氏度=68, 偏差=0, 和=80
		{"device": "device1", "temperature": 25.0, "humidity": 65.0, "status": "warning"}, // 华氏度=77, 偏差=10, 和=90
		{"device": "device1", "temperature": 30.0, "humidity": 70.0, "status": "normal"},  // 华氏度=86, 偏差=20, 和=100

		// device2的数据
		{"device": "device2", "temperature": 15.0, "humidity": 55.0, "status": "error"},  // 华氏度=59, 偏差=-10, 和=70
		{"device": "device2", "temperature": 18.0, "humidity": 58.0, "status": "normal"}, // 华氏度=64.4, 偏差=-4, 和=76
		{"device": "device2", "temperature": 22.0, "humidity": 62.0, "status": "error"},  // 华氏度=71.6, 偏差=4, 和=84
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.Emit(data)
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口触发（处理时间模式）
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其计算结果
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
			// device1的验证
			// 平均华氏度: (68 + 77 + 86) / 3 = 77
			assert.InEpsilon(t, 77.0, avgFahrenheit, 0.1, "device1的平均华氏度应约为77")

			// 温度偏差标准差: sqrt(((0-10)² + (10-10)² + (20-10)²) / 2) = sqrt(200/2) = 10
			assert.InEpsilon(t, 10.0, tempStddev, 0.1, "device1的温度偏差标准差应约为10")

			// 温度除以10的方差: ((2-2.5)² + (2.5-2.5)² + (3-2.5)²) / 3 = 0.167
			assert.InEpsilon(t, 0.167, tempVar, 0.01, "device1的温度方差应约为0.167")

			// 温度和湿度的和数组
			tempHumSumArray, ok := tempHumSum.([]any)
			assert.True(t, ok, "temp_hum_sum应该是数组")
			assert.Len(t, tempHumSumArray, 3, "device1应该有3个温度和湿度的和")
			assert.Contains(t, tempHumSumArray, 80.0)
			assert.Contains(t, tempHumSumArray, 90.0)
			assert.Contains(t, tempHumSumArray, 100.0)

			// 最后一个温度和湿度的乘积: 30 * 70 = 2100
			assert.InEpsilon(t, 2100.0, lastTempHum, 0.1, "device1的最后一个温度和湿度乘积应为2100")

			// 设备状态组合
			assert.Contains(t, deviceStatus, "device1_normal")
			assert.Contains(t, deviceStatus, "device1_warning")

			// 状态设备组合去重
			uniqueArray, ok := uniqueStatusDevice.([]any)
			assert.True(t, ok, "unique_status_device应该是数组")
			assert.Len(t, uniqueArray, 2, "device1应该有2个不同的状态设备组合")
			assert.Contains(t, uniqueArray, "normal_device1")
			assert.Contains(t, uniqueArray, "warning_device1")

		} else if device == "device2" {
			// device2的验证
			// 平均华氏度: (59 + 64.4 + 71.6) / 3 = 65
			assert.InEpsilon(t, 65.0, avgFahrenheit, 0.1, "device2的平均华氏度应约为65")

			// 温度偏差标准差: sqrt(((-10-(-3.33))² + (-4-(-3.33))² + (4-(-3.33))²) / 2) = sqrt(147.33/2) = 7.023
			assert.InEpsilon(t, 7.023, tempStddev, 0.1, "device2的温度偏差标准差应约为7.023")

			// 温度除以10的方差: ((1.5-1.83)² + (1.8-1.83)² + (2.2-1.83)²) / 3 = 0.082
			assert.InEpsilon(t, 0.082, tempVar, 0.01, "device2的温度方差应约为0.082")

			// 温度和湿度的和数组
			tempHumSumArray, ok := tempHumSum.([]any)
			assert.True(t, ok, "temp_hum_sum应该是数组")
			assert.Len(t, tempHumSumArray, 3, "device2应该有3个温度和湿度的和")
			assert.Contains(t, tempHumSumArray, 70.0)
			assert.Contains(t, tempHumSumArray, 76.0)
			assert.Contains(t, tempHumSumArray, 84.0)

			// 最后一个温度和湿度的乘积: 22 * 62 = 1364
			assert.InEpsilon(t, 1364.0, lastTempHum, 0.1, "device2的最后一个温度和湿度乘积应为1364")

			// 设备状态组合
			assert.Contains(t, deviceStatus, "device2_error")
			assert.Contains(t, deviceStatus, "device2_normal")

			// 状态设备组合去重
			uniqueArray, ok := uniqueStatusDevice.([]any)
			assert.True(t, ok, "unique_status_device应该是数组")
			assert.Len(t, uniqueArray, 2, "device2应该有2个不同的状态设备组合")
			assert.Contains(t, uniqueArray, "error_device2")
			assert.Contains(t, uniqueArray, "normal_device2")
		}
	}

	//fmt.Println("表达式聚合函数测试完成")
}

func TestAnalyticalFunctionsInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试使用分析函数的SQL查询
	var rsql = "SELECT device, lag(temperature) as prev_temp, latest(temperature) as current_temp, had_changed(temperature) as temp_changed FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	if err != nil {
		t.Skipf("v1.2 分析函数改为直连 OVER 语义，不再与 GROUP BY/窗口混用；窗口+OVER WHEN 见后续版本: %v", err)
	}
	strm := ssql

	//fmt.Println("开始测试分析函数功能")

	// 添加测试数据
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 20.0},
		{"device": "sensor1", "temperature": 25.0},
		{"device": "sensor1", "temperature": 25.0}, // 重复值，测试had_changed
		{"device": "sensor2", "temperature": 18.0},
		{"device": "sensor2", "temperature": 22.0},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.Emit(data)
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口触发（处理时间模式）
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其分析函数结果
	for _, result := range resultSlice {
		device, _ := result["device"].(string)

		assert.Contains(t, result, "prev_temp", "结果应包含prev_temp字段")
		assert.Contains(t, result, "current_temp", "结果应包含current_temp字段")
		assert.Contains(t, result, "temp_changed", "结果应包含temp_changed字段")

		if device == "sensor1" {
			// sensor1有3个温度值: 20.0, 25.0, 25.0
			// latest应该返回最新值
			currentTemp := result["current_temp"]
			assert.NotNil(t, currentTemp, "current_temp不应为空")

			// had_changed应该有变化记录
			tempChanged := result["temp_changed"]
			assert.NotNil(t, tempChanged, "temp_changed不应为空")
		} else if device == "sensor2" {
			// sensor2有2个温度值: 18.0, 22.0
			currentTemp := result["current_temp"]
			assert.NotNil(t, currentTemp, "current_temp不应为空")

			tempChanged := result["temp_changed"]
			assert.NotNil(t, tempChanged, "temp_changed不应为空")
		}
	}

	//fmt.Println("分析函数测试完成")
}
func TestHadChangedFunctionInSQL(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试had_changed函数的SQL查询
	var rsql = "SELECT device, had_changed(temperature) as temp_changed FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	if err != nil {
		t.Skipf("v1.2 分析函数改为直连 OVER 语义，不再与 GROUP BY/窗口混用；窗口+OVER WHEN 见后续版本: %v", err)
	}
	strm := ssql

	//fmt.Println("开始测试had_changed函数功能")

	// 添加测试数据 - 包含重复值和变化值
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		{"device": "monitor", "temperature": 20.0},
		{"device": "monitor", "temperature": 20.0}, // 相同值
		{"device": "monitor", "temperature": 25.0}, // 变化值
		{"device": "monitor", "temperature": 25.0}, // 相同值
		{"device": "monitor", "temperature": 30.0}, // 变化值
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.Emit(data)
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口触发（处理时间模式）
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 1, "应该有1个设备的聚合结果")

	result := resultSlice[0]
	device, _ := result["device"].(string)
	assert.Equal(t, "monitor", device, "设备名应该正确")

	// 验证字段存在
	assert.Contains(t, result, "temp_changed", "结果应包含temp_changed字段")

	// had_changed函数应该返回布尔值
	//tempChanged := result["temp_changed"]
	//fmt.Printf("had_changed函数返回值: %v\n", tempChanged)

	//fmt.Println("had_changed函数测试完成")
}
func TestIncrementalComputationBasic(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试基本的增量计算聚合函数
	var rsql = "SELECT device, sum(temperature) as total, avg(temperature) as average, count(*) as cnt FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	//fmt.Println("开始测试基本增量计算功能")

	// 添加测试数据
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 10.0},
		{"device": "sensor1", "temperature": 20.0},
		{"device": "sensor1", "temperature": 30.0},
		{"device": "sensor2", "temperature": 15.0},
		{"device": "sensor2", "temperature": 25.0},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.Emit(data)
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口触发（处理时间模式）
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其聚合结果
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		total := result["total"]
		average := result["average"]
		count := result["cnt"]

		//fmt.Printf("设备 %s: total=%v, average=%v, count=%v\n", device, total, average, count)

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

	//fmt.Println("基本增量计算测试完成")
}

// TestExprFunctions 测试expr函数的使用
func TestExprFunctions(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试基本expr函数：字符串处理
	var rsql = "SELECT device, upper(device) as upper_device, lower(device) as lower_device FROM stream"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []map[string]any{
		{"device": "SensorA"},
		{"device": "SensorB"},
	}

	// 添加数据
	for _, data := range testData {
		strm.Emit(data)
	}

	// 等待结果
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

	// 验证结果
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

			// 验证upper函数
			assert.Equal(t, strings.ToUpper(device), upperDevice, "upper函数应该正确转换大写")
			// 验证lower函数
			assert.Equal(t, strings.ToLower(device), lowerDevice, "lower函数应该正确转换小写")
		}
	}
}

// TestExprFunctionsInAggregation 测试在聚合中使用expr函数
func TestExprFunctionsInAggregation(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试在聚合函数中使用expr函数：数学计算
	var rsql = "SELECT device, AVG(abs(temperature - 25)) as avg_deviation, MAX(ceil(temperature)) as max_ceil FROM stream GROUP BY device, TumblingWindow('1s')"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	// 添加测试数据
	// 不使用事件时间，不需要时间戳字段
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 23.5}, // abs(23.5-25) = 1.5, ceil(23.5) = 24
		{"device": "sensor1", "temperature": 26.8}, // abs(26.8-25) = 1.8, ceil(26.8) = 27
		{"device": "sensor2", "temperature": 24.2}, // abs(24.2-25) = 0.8, ceil(24.2) = 25
		{"device": "sensor2", "temperature": 25.9}, // abs(25.9-25) = 0.9, ceil(25.9) = 26
	}

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// 添加数据
	for _, data := range testData {
		strm.Emit(data)
	}

	// 等待窗口初始化
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	strm.TriggerWindow()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual any
	select {
	case actual = <-resultChan:
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]any)
	require.True(t, ok, "结果应该是[]map[string]any类型")
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查聚合结果
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

// TestNestedExprFunctions 测试嵌套expr函数调用
func TestNestedExprFunctions(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试嵌套函数：字符串处理 + 数组操作
	var rsql = "SELECT device, len(split(upper(device), 'SENSOR')) as split_count FROM stream"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []map[string]any{
		{"device": "sensor1"},      // upper -> "SENSOR1", split by "SENSOR" -> ["", "1"], len -> 2
		{"device": "sensorsensor"}, // upper -> "SENSORSENSOR", split by "SENSOR" -> ["", "", ""], len -> 3
		{"device": "device1"},      // upper -> "DEVICE1", split by "SENSOR" -> ["DEVICE1"], len -> 1
	}

	// 添加数据
	for _, data := range testData {
		strm.Emit(data)
	}

	// 等待结果
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

	// 验证结果
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

	// 验证嵌套函数调用结果
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

// TestExprFunctionsWithStreamSQLFunctions 测试expr函数与StreamSQL函数混合使用
func TestExprFunctionsWithStreamSQLFunctions(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	// 测试混合使用：StreamSQL的concat函数 + expr的upper函数
	var rsql = "SELECT device, concat(upper(device), '_processed') as processed_name FROM stream"
	err := ssql.Execute(rsql)
	assert.Nil(t, err)
	strm := ssql

	// 创建结果接收通道
	resultChan := make(chan any, 10)

	// 添加结果回调
	strm.AddSink(func(result []map[string]any) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []map[string]any{
		{"device": "sensor1"},
		{"device": "device2"},
	}

	// 添加数据
	for _, data := range testData {
		strm.Emit(data)
	}

	// 等待结果
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

	// 验证结果
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

			// 验证混合函数调用结果
			expected := strings.ToUpper(device) + "_processed"
			assert.Equal(t, expected, processedName, "混合函数调用应该正确处理")
		}
	}
}

// TestSelectAllFeature 专门测试SELECT *功能
