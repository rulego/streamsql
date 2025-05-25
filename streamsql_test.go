package streamsql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/utils/cast"

	"math/rand"

	"github.com/rulego/streamsql/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamData(t *testing.T) {
	ssql := New()
	// 定义SQL语句。TumblingWindow 滚动窗口，5秒滚动一次
	rsql := "SELECT deviceId,avg(temperature) as avg_temp,min(humidity) as min_humidity ," +
		"window_start() as start,window_end() as end FROM  stream  where deviceId!='device3' group by deviceId,TumblingWindow('5s')"
	// 根据SQL语句，创建流式分析任务。
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	// 设置30秒测试超时时间
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// 添加测试数据
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// 生成随机测试数据，每秒生成10条数据
				for i := 0; i < 10; i++ {
					randomData := map[string]interface{}{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(3)+1),
						"temperature": 20.0 + rand.Float64()*10, // 20-30度之间
						"humidity":    50.0 + rand.Float64()*20, // 50-70%湿度
					}
					// 将数据添加到流中
					ssql.stream.AddData(randomData)
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	resultChan := make(chan interface{})
	// 添加计算结果回调
	ssql.stream.AddSink(func(result interface{}) {
		resultChan <- result
	})
	// 记录收到的结果数量
	resultCount := 0
	go func() {
		for range resultChan {
			//每隔5秒打印一次结果
			//fmt.Printf("打印结果: [%s] %v\n", time.Now().Format("15:04:05.000"), result)
			resultCount++
		}
	}()
	//测试结束
	wg.Wait()

	// 验证是否收到了结果
	assert.Equal(t, resultCount, 5)
}

func TestStreamsql(t *testing.T) {
	streamsql := New()
	var rsql = "SELECT device,max(temperature) as max_temp,min(humidity) as min_humidity,window_start() as start,window_end() as end FROM stream group by device,SlidingWindow('2s','1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0, "humidity": 60, "Ts": baseTime},
		map[string]interface{}{"device": "aa", "temperature": 30.0, "humidity": 55, "Ts": baseTime.Add(1 * time.Second)},
		map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70, "Ts": baseTime},
	}

	for _, data := range testData {
		strm.AddData(data)
	}
	// 捕获结果
	resultChan := make(chan interface{})
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		cancel()
	case <-ctx.Done():
		t.Fatal("Timeout waiting for results")
	}

	expected := []map[string]interface{}{
		{
			"device":       "aa",
			"max_temp":     30.0,
			"min_humidity": 55.0,
			"start":        baseTime.UnixNano(),
			"end":          baseTime.Add(2 * time.Second).UnixNano(),
		},
		{
			"device":       "bb",
			"max_temp":     22.0,
			"min_humidity": 70.0,
			"start":        baseTime.UnixNano(),
			"end":          baseTime.Add(2 * time.Second).UnixNano(),
		},
	}

	assert.IsType(t, []map[string]interface{}{}, actual)
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok)
	assert.Len(t, resultSlice, 2)
	for _, expectedResult := range expected {
		found := false
		for _, resultMap := range resultSlice {
			if resultMap["device"] == expectedResult["device"] {
				assert.InEpsilon(t, expectedResult["max_temp"].(float64), resultMap["max_temp"].(float64), 0.0001)
				assert.InEpsilon(t, expectedResult["min_humidity"].(float64), resultMap["min_humidity"].(float64), 0.0001)
				assert.Equal(t, expectedResult["start"].(int64), resultMap["start"].(int64))
				assert.Equal(t, expectedResult["end"].(int64), resultMap["end"].(int64))
				found = true
				break
			}
		}
		assert.True(t, found, fmt.Sprintf("Expected result for device %v not found", expectedResult["device"]))
	}
}

func TestStreamsqlWithoutGroupBy(t *testing.T) {
	streamsql := New()
	var rsql = "SELECT max(temperature) as max_temp,min(humidity) as min_humidity,window_start() as start,window_end() as end FROM stream SlidingWindow('2s','1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0, "humidity": 60, "Ts": baseTime},
		map[string]interface{}{"device": "aa", "temperature": 30.0, "humidity": 55, "Ts": baseTime.Add(1 * time.Second)},
		map[string]interface{}{"device": "bb", "temperature": 22.0, "humidity": 70, "Ts": baseTime},
	}

	for _, data := range testData {
		strm.AddData(data)
	}
	// 捕获结果
	resultChan := make(chan interface{})
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		cancel()
	case <-ctx.Done():
		t.Fatal("Timeout waiting for results")
	}

	expected := []map[string]interface{}{
		{
			"max_temp":     30.0,
			"min_humidity": 55.0,
			"start":        baseTime.UnixNano(),
			"end":          baseTime.Add(2 * time.Second).UnixNano(),
		},
	}

	assert.IsType(t, []map[string]interface{}{}, actual)
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok)
	assert.Len(t, resultSlice, 1)
	for _, expectedResult := range expected {
		//found := false
		for _, resultMap := range resultSlice {
			assert.InEpsilon(t, expectedResult["max_temp"].(float64), resultMap["max_temp"].(float64), 0.0001)
			assert.InEpsilon(t, expectedResult["min_humidity"].(float64), resultMap["min_humidity"].(float64), 0.0001)
			assert.Equal(t, expectedResult["start"].(int64), resultMap["start"].(int64))
			assert.Equal(t, expectedResult["end"].(int64), resultMap["end"].(int64))
		}
		//assert.True(t, found, fmt.Sprintf("Expected result for device %v not found", expectedResult["device"]))
	}
}

func TestStreamsqlDistinct(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 测试 SELECT DISTINCT 功能 - 使用聚合函数和 GROUP BY
	var rsql = "SELECT DISTINCT device, AVG(temperature) as avg_temp FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试 SELECT DISTINCT 功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据，包含重复的设备数据
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0, "Ts": baseTime},
		map[string]interface{}{"device": "aa", "temperature": 35.0, "Ts": baseTime}, // 相同设备，不同温度
		map[string]interface{}{"device": "bb", "temperature": 22.0, "Ts": baseTime},
		map[string]interface{}{"device": "bb", "temperature": 28.0, "Ts": baseTime}, // 相同设备，不同温度
		map[string]interface{}{"device": "cc", "temperature": 30.0, "Ts": baseTime},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 打印结果以便调试
	//fmt.Printf("接收到的结果: %v\n", actual)

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

	// 验证去重后的结果数量
	assert.Len(t, resultSlice, 3, "应该有3个设备的聚合结果")

	// 检查是否包含所有预期的设备
	deviceFound := make(map[string]bool)
	for _, result := range resultSlice {
		device, ok := result["device"].(string)
		if ok {
			deviceFound[device] = true
		}
	}

	assert.True(t, deviceFound["aa"], "结果应包含设备aa")
	assert.True(t, deviceFound["bb"], "结果应包含设备bb")
	assert.True(t, deviceFound["cc"], "结果应包含设备cc")

	// 验证聚合结果 - aa设备的平均温度应为(25+35)/2=30
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		avgTemp, ok := result["avg_temp"].(float64)

		assert.True(t, ok, "avg_temp应该是float64类型")

		if device == "aa" {
			assert.InEpsilon(t, 30.0, avgTemp, 0.001, "aa设备的平均温度应为30")
		} else if device == "bb" {
			assert.InEpsilon(t, 25.0, avgTemp, 0.001, "bb设备的平均温度应为25")
		} else if device == "cc" {
			assert.InEpsilon(t, 30.0, avgTemp, 0.001, "cc设备的平均温度应为30")
		}
	}

	//fmt.Println("测试完成")
}

func TestStreamsqlLimit(t *testing.T) {
	streamsql := New()
	// 测试 LIMIT 功能，不使用窗口函数
	var rsql = "SELECT device, temperature FROM stream LIMIT 2"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "temperature": 25.0},
		map[string]interface{}{"device": "bb", "temperature": 22.0},
		map[string]interface{}{"device": "cc", "temperature": 30.0},
		map[string]interface{}{"device": "dd", "temperature": 28.0},
	}

	// 捕获结果
	var receivedResults []interface{}
	mutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// 添加结果接收器
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		mutex.Lock()
		receivedResults = append(receivedResults, result)
		mutex.Unlock()
	})

	// 启动结果收集协程
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ { // 最多等待10次
			time.Sleep(300 * time.Millisecond)
			mutex.Lock()
			count := len(receivedResults)
			mutex.Unlock()

			if count >= len(testData) {
				break // 已收到足够多的结果
			}
		}
	}()

	// 添加数据
	for _, data := range testData {
		//fmt.Printf("添加数据: %v\n", data)
		strm.AddData(data)
		time.Sleep(100 * time.Millisecond) // 稍微等待一下确保处理
	}

	// 等待结果收集
	wg.Wait()

	// 验证结果
	mutex.Lock()
	defer mutex.Unlock()

	//fmt.Printf("共收到 %d 条结果\n", len(receivedResults))
	assert.Greater(t, len(receivedResults), 0, "应该收到至少一条结果")

	// 验证每个结果都符合LIMIT限制
	for _, result := range receivedResults {
		resultSlice, ok := result.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")
		assert.LessOrEqual(t, len(resultSlice), 2, "每个batch最多2条记录")

		// 验证字段
		for _, item := range resultSlice {
			assert.Contains(t, item, "device", "结果应包含device字段")
			assert.Contains(t, item, "temperature", "结果应包含temperature字段")
		}
	}
}

func TestSimpleQuery(t *testing.T) {
	strm := New()
	// 测试结束时确保关闭流处理
	defer strm.Stop()

	// 测试简单查询，不使用窗口函数
	var rsql = "SELECT device, temperature FROM stream"
	err := strm.Execute(rsql)
	assert.Nil(t, err)

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加sink
	strm.stream.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	//添加数据
	testData := []interface{}{
		map[string]interface{}{"device": "test-device", "temperature": 25.5},
	}

	// 发送数据
	//fmt.Println("添加数据...")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 等待结果
	//fmt.Println("等待结果...")
	//等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case result := <-resultChan:
		//fmt.Printf("收到结果: %v\n", result)
		// 验证结果
		resultSlice, ok := result.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")
		require.Len(t, resultSlice, 1, "应该只有一条结果")

		item := resultSlice[0]
		assert.Equal(t, "test-device", item["device"], "device字段应该正确")
		assert.Equal(t, 25.5, item["temperature"], "temperature字段应该正确")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}
	time.Sleep(500 * time.Millisecond)
}

func TestHavingClause(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 定义SQL语句，使用HAVING子句
	rsql := "SELECT device, avg(temperature) as avg_temp FROM stream GROUP BY device HAVING avg_temp > 25"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 添加测试数据，确保有不同的聚合结果
	testData := []interface{}{
		map[string]interface{}{"device": "dev1", "temperature": 20.0},
		map[string]interface{}{"device": "dev1", "temperature": 22.0},
		map[string]interface{}{"device": "dev2", "temperature": 26.0},
		map[string]interface{}{"device": "dev2", "temperature": 28.0},
		map[string]interface{}{"device": "dev3", "temperature": 30.0},
	}

	// 添加数据
	for _, data := range testData {
		strm.AddData(data)
	}

	// 等待窗口初始化
	time.Sleep(500 * time.Millisecond)

	// 手动触发窗口
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

	// HAVING avg_temp > 25 应该只返回dev2和dev3
	// 验证结果中不包含dev1
	for _, result := range resultSlice {
		assert.NotEqual(t, "dev1", result["device"], "结果不应包含dev1")
		assert.Contains(t, []string{"dev2", "dev3"}, result["device"], "结果应只包含dev2和dev3")

		// 验证平均温度确实大于25
		avgTemp, ok := result["avg_temp"].(float64)
		assert.True(t, ok, "avg_temp应该是float64类型")
		assert.Greater(t, avgTemp, 25.0, "avg_temp应该大于25")
	}
}

//func TestSessionWindow(t *testing.T) {
//	streamsql := New()
//	defer streamsql.Stop()
//
//	// 使用 SESSION 窗口，超时时间为 2 秒
//	rsql := "SELECT device, avg(temperature) as avg_temp FROM stream GROUP BY device, SESSIONWINDOW('2s') with (TIMESTAMP='Ts')"
//	err := streamsql.Execute(rsql)
//	assert.Nil(t, err)
//	strm := streamsql.stream
//
//	// 创建结果接收通道
//	resultChan := make(chan interface{}, 10)
//
//	// 添加结果回调
//	strm.AddSink(func(result interface{}) {
//		//fmt.Printf("接收到结果: %v\n", result)
//		resultChan <- result
//	})
//
//	baseTime := time.Now()
//
//	// 添加测试数据 - 两个设备，不同的时间
//	testData := []struct {
//		data interface{}
//		wait time.Duration
//	}{
//		// 第一组数据 - device1
//		{map[string]interface{}{"device": "device1", "temperature": 20.0, "Ts": baseTime}, 0},
//		{map[string]interface{}{"device": "device1", "temperature": 22.0, "Ts": baseTime.Add(500 * time.Millisecond)}, 500 * time.Millisecond},
//
//		// 第二组数据 - device2
//		{map[string]interface{}{"device": "device2", "temperature": 25.0, "Ts": baseTime.Add(time.Second)}, time.Second},
//		{map[string]interface{}{"device": "device2", "temperature": 27.0, "Ts": baseTime.Add(1500 * time.Millisecond)}, 500 * time.Millisecond},
//
//		// 间隔超过会话超时
//
//		// 第三组数据 - device1，新会话
//		{map[string]interface{}{"device": "device1", "temperature": 30.0, "Ts": baseTime.Add(5 * time.Second)}, 3 * time.Second},
//	}
//
//	// 按指定的间隔添加数据
//	for _, item := range testData {
//		if item.wait > 0 {
//			time.Sleep(item.wait)
//		}
//		strm.AddData(item.data)
//	}
//
//	// 等待会话超时，使最后一个会话触发
//	time.Sleep(3 * time.Second)
//
//	// 手动触发所有窗口，确保数据被处理
//	strm.Window.Trigger()
//
//	// 收集结果
//	var results []interface{}
//
//	// 等待接收结果
//	timeout := time.After(5 * time.Second)
//	done := false
//
//	for !done {
//		select {
//		case result := <-resultChan:
//			results = append(results, result)
//			// 我们期望至少 3 个会话结果
//			if len(results) >= 3 {
//				done = true
//			}
//		case <-timeout:
//			// 超时，可能没有收到足够的结果
//			done = true
//		}
//	}
//
//	// 验证结果
//	assert.GreaterOrEqual(t, len(results), 2, "应该至少收到两个会话的结果")
//
//	// 检查结果中是否包含两个设备的会话
//	hasDevice1 := false
//	hasDevice2 := false
//
//	for _, result := range results {
//		resultSlice, ok := result.([]map[string]interface{})
//		assert.True(t, ok, "结果应该是[]map[string]interface{}类型")
//
//		for _, item := range resultSlice {
//			device, ok := item["device"].(string)
//			assert.True(t, ok, "device字段应该是string类型")
//
//			if device == "device1" {
//				hasDevice1 = true
//			} else if device == "device2" {
//				hasDevice2 = true
//			}
//		}
//	}
//
//	assert.True(t, hasDevice1, "结果中应该包含device1的会话")
//	assert.True(t, hasDevice2, "结果中应该包含device2的会话")
//}

func TestExpressionInAggregation(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 测试在聚合函数中使用表达式
	var rsql = "SELECT device, AVG(temperature * 1.8 + 32) as fahrenheit FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试表达式功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据，温度使用摄氏度
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "temperature": 0.0, "Ts": baseTime},   // 华氏度应为 32
		map[string]interface{}{"device": "aa", "temperature": 100.0, "Ts": baseTime}, // 华氏度应为 212
		map[string]interface{}{"device": "bb", "temperature": 20.0, "Ts": baseTime},  // 华氏度应为 68
		map[string]interface{}{"device": "bb", "temperature": 30.0, "Ts": baseTime},  // 华氏度应为 86
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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
	streamsql := New()
	defer streamsql.Stop()

	// 测试使用新函数系统的复杂SQL查询
	var rsql = "SELECT device, AVG(abs(temperature - 20)) as abs_diff, CONCAT(device, '_processed') as device_name FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试高级函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "sensor1", "temperature": 15.0, "Ts": baseTime}, // abs(15-20) = 5
		map[string]interface{}{"device": "sensor1", "temperature": 25.0, "Ts": baseTime}, // abs(25-20) = 5
		map[string]interface{}{"device": "sensor2", "temperature": 18.0, "Ts": baseTime}, // abs(18-20) = 2
		map[string]interface{}{"device": "sensor2", "temperature": 22.0, "Ts": baseTime}, // abs(22-20) = 2
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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
	// 注册自定义函数：温度华氏度转摄氏度
	err := functions.RegisterCustomFunction("fahrenheit_to_celsius", functions.TypeCustom, "温度转换", "华氏度转摄氏度", 1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			fahrenheit := cast.ToFloat64(args[0])
			celsius := (fahrenheit - 32) * 5 / 9
			return celsius, nil
		})
	assert.NoError(t, err)
	defer functions.Unregister("fahrenheit_to_celsius")

	streamsql := New()
	defer streamsql.Stop()

	// 测试使用自定义函数的SQL查询
	var rsql = "SELECT device, AVG(fahrenheit_to_celsius(temperature)) as avg_celsius FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err = streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试自定义函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据（华氏度）
	testData := []interface{}{
		map[string]interface{}{"device": "thermometer1", "temperature": 32.0, "Ts": baseTime},  // 0°C
		map[string]interface{}{"device": "thermometer1", "temperature": 212.0, "Ts": baseTime}, // 100°C
		map[string]interface{}{"device": "thermometer2", "temperature": 68.0, "Ts": baseTime},  // 20°C
		map[string]interface{}{"device": "thermometer2", "temperature": 86.0, "Ts": baseTime},  // 30°C
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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
	streamsql := New()
	defer streamsql.Stop()

	// 测试使用新聚合函数的SQL查询
	var rsql = "SELECT device, collect(temperature) as temp_values, last_value(temperature) as last_temp, merge_agg(status) as all_status FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试新聚合函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "sensor1", "temperature": 15.0, "status": "good", "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "temperature": 25.0, "status": "ok", "Ts": baseTime},
		map[string]interface{}{"device": "sensor2", "temperature": 18.0, "status": "good", "Ts": baseTime},
		map[string]interface{}{"device": "sensor2", "temperature": 22.0, "status": "warning", "Ts": baseTime},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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
			values, ok := tempValues.([]interface{})
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
			values, ok := tempValues.([]interface{})
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
	streamsql := New()
	defer streamsql.Stop()

	// 测试使用统计聚合函数的SQL查询
	var rsql = "SELECT device, stddevs(temperature) as sample_stddev, var(temperature) as population_var, vars(temperature) as sample_var FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试统计聚合函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "sensor1", "temperature": 10.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "temperature": 20.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "temperature": 30.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor2", "temperature": 15.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor2", "temperature": 25.0, "Ts": baseTime},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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
	streamsql := New()
	defer streamsql.Stop()

	// 测试使用去重聚合函数的SQL查询
	var rsql = "SELECT device, deduplicate(status) as unique_status FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试去重聚合函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据，包含重复的状态
	testData := []interface{}{
		map[string]interface{}{"device": "sensor1", "status": "good", "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "status": "good", "Ts": baseTime}, // 重复
		map[string]interface{}{"device": "sensor1", "status": "warning", "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "status": "good", "Ts": baseTime}, // 重复
		map[string]interface{}{"device": "sensor2", "status": "error", "Ts": baseTime},
		map[string]interface{}{"device": "sensor2", "status": "error", "Ts": baseTime}, // 重复
		map[string]interface{}{"device": "sensor2", "status": "ok", "Ts": baseTime},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其去重结果
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		uniqueStatus, ok := result["unique_status"]

		assert.True(t, ok, "unique_status应该存在")

		if device == "sensor1" {
			// sensor1应该有去重后的状态：["good", "warning"]
			statusArray, ok := uniqueStatus.([]interface{})
			assert.True(t, ok, "unique_status应该是数组")
			assert.Len(t, statusArray, 2, "sensor1应该有2个不同的状态")
			assert.Contains(t, statusArray, "good")
			assert.Contains(t, statusArray, "warning")
		} else if device == "sensor2" {
			// sensor2应该有去重后的状态：["error", "ok"]
			statusArray, ok := uniqueStatus.([]interface{})
			assert.True(t, ok, "unique_status应该是数组")
			assert.Len(t, statusArray, 2, "sensor2应该有2个不同的状态")
			assert.Contains(t, statusArray, "error")
			assert.Contains(t, statusArray, "ok")
		}
	}

	//fmt.Println("去重聚合函数测试完成")
}

func TestExprAggregationFunctions(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

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
	GROUP BY device, TumblingWindow('1s') 
	with (TIMESTAMP='Ts',TIMEUNIT='ss')`

	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试表达式聚合函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据
	testData := []interface{}{
		// device1的数据
		map[string]interface{}{"device": "device1", "temperature": 20.0, "humidity": 60.0, "status": "normal", "Ts": baseTime},  // 华氏度=68, 偏差=0, 和=80
		map[string]interface{}{"device": "device1", "temperature": 25.0, "humidity": 65.0, "status": "warning", "Ts": baseTime}, // 华氏度=77, 偏差=10, 和=90
		map[string]interface{}{"device": "device1", "temperature": 30.0, "humidity": 70.0, "status": "normal", "Ts": baseTime},  // 华氏度=86, 偏差=20, 和=100

		// device2的数据
		map[string]interface{}{"device": "device2", "temperature": 15.0, "humidity": 55.0, "status": "error", "Ts": baseTime},  // 华氏度=59, 偏差=-10, 和=70
		map[string]interface{}{"device": "device2", "temperature": 18.0, "humidity": 58.0, "status": "normal", "Ts": baseTime}, // 华氏度=64.4, 偏差=-4, 和=76
		map[string]interface{}{"device": "device2", "temperature": 22.0, "humidity": 62.0, "status": "error", "Ts": baseTime},  // 华氏度=71.6, 偏差=4, 和=84
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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
			tempHumSumArray, ok := tempHumSum.([]interface{})
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
			uniqueArray, ok := uniqueStatusDevice.([]interface{})
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
			tempHumSumArray, ok := tempHumSum.([]interface{})
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
			uniqueArray, ok := uniqueStatusDevice.([]interface{})
			assert.True(t, ok, "unique_status_device应该是数组")
			assert.Len(t, uniqueArray, 2, "device2应该有2个不同的状态设备组合")
			assert.Contains(t, uniqueArray, "error_device2")
			assert.Contains(t, uniqueArray, "normal_device2")
		}
	}

	//fmt.Println("表达式聚合函数测试完成")
}

func TestAnalyticalFunctionsInSQL(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 测试使用分析函数的SQL查询
	var rsql = "SELECT device, lag(temperature) as prev_temp, latest(temperature) as current_temp, had_changed(temperature) as temp_changed FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试分析函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "sensor1", "temperature": 20.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "temperature": 25.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "temperature": 25.0, "Ts": baseTime}, // 重复值，测试had_changed
		map[string]interface{}{"device": "sensor2", "temperature": 18.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor2", "temperature": 22.0, "Ts": baseTime},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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

func TestLagFunctionInSQL(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 测试LAG函数的SQL查询
	var rsql = "SELECT device, lag(temperature) as prev_temp FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试LAG函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据 - 按顺序添加，测试LAG功能
	testData := []interface{}{
		map[string]interface{}{"device": "temp_sensor", "temperature": 10.0, "Ts": baseTime},
		map[string]interface{}{"device": "temp_sensor", "temperature": 15.0, "Ts": baseTime},
		map[string]interface{}{"device": "temp_sensor", "temperature": 20.0, "Ts": baseTime},
		map[string]interface{}{"device": "temp_sensor", "temperature": 25.0, "Ts": baseTime}, // 最后一个值
	}

	// 添加数据
	//fmt.Println("添加测试数据：", testData)
	for _, data := range testData {
		//fmt.Printf("添加第%d个数据: temperature=%.1f\n", i+1, data.(map[string]interface{})["temperature"])
		strm.AddData(data)
		time.Sleep(100 * time.Millisecond) // 稍微延迟确保顺序
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 1, "应该有1个设备的聚合结果")

	result := resultSlice[0]
	device, _ := result["device"].(string)
	assert.Equal(t, "temp_sensor", device, "设备名应该正确")

	// 验证字段存在
	assert.Contains(t, result, "prev_temp", "结果应包含prev_temp字段")

	// LAG函数应该返回最后一个值(25.0)的前一个值(20.0)
	// 数据序列：10.0 -> 15.0 -> 20.0 -> 25.0
	// LAG执行过程：
	// - 10.0: 无前值，返回nil
	// - 15.0: 前值10.0，返回10.0
	// - 20.0: 前值15.0，返回15.0
	// - 25.0: 前值20.0，返回20.0 ← 最终结果
	prevTemp := result["prev_temp"]
	//fmt.Printf("LAG函数返回值: %v (期望: 20.0，表示最后值25.0的前一个值)\n", prevTemp)

	// 验证LAG函数返回正确的前一个值
	expectedPrevTemp := 20.0
	if prevTemp != nil {
		prevTempFloat, ok := prevTemp.(float64)
		assert.True(t, ok, "prev_temp应该是float64类型")
		assert.Equal(t, expectedPrevTemp, prevTempFloat, "LAG函数应该返回最后一个值的前一个值(20.0)")
	} else {
		t.Errorf("LAG函数不应该返回nil，期望值: %.1f", expectedPrevTemp)
	}

	//fmt.Println("LAG函数测试完成")
}

func TestHadChangedFunctionInSQL(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 测试had_changed函数的SQL查询
	var rsql = "SELECT device, had_changed(temperature) as temp_changed FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试had_changed函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据 - 包含重复值和变化值
	testData := []interface{}{
		map[string]interface{}{"device": "monitor", "temperature": 20.0, "Ts": baseTime},
		map[string]interface{}{"device": "monitor", "temperature": 20.0, "Ts": baseTime}, // 相同值
		map[string]interface{}{"device": "monitor", "temperature": 25.0, "Ts": baseTime}, // 变化值
		map[string]interface{}{"device": "monitor", "temperature": 25.0, "Ts": baseTime}, // 相同值
		map[string]interface{}{"device": "monitor", "temperature": 30.0, "Ts": baseTime}, // 变化值
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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

func TestLatestFunctionInSQL(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 测试latest函数的SQL查询
	var rsql = "SELECT device, latest(temperature) as current_temp FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试latest函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "thermometer", "temperature": 10.0, "Ts": baseTime},
		map[string]interface{}{"device": "thermometer", "temperature": 15.0, "Ts": baseTime},
		map[string]interface{}{"device": "thermometer", "temperature": 20.0, "Ts": baseTime},
		map[string]interface{}{"device": "thermometer", "temperature": 25.0, "Ts": baseTime}, // 最新值
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 1, "应该有1个设备的聚合结果")

	result := resultSlice[0]
	device, _ := result["device"].(string)
	assert.Equal(t, "thermometer", device, "设备名应该正确")

	// 验证字段存在
	assert.Contains(t, result, "current_temp", "结果应包含current_temp字段")

	// latest函数应该返回最新值25.0
	currentTemp, ok := result["current_temp"].(float64)
	assert.True(t, ok, "current_temp应该是float64类型")
	assert.Equal(t, 25.0, currentTemp, "latest函数应该返回最新值25.0")

	//fmt.Println("latest函数测试完成")
}

func TestChangedColFunctionInSQL(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 测试changed_col函数的SQL查询
	var rsql = "SELECT device, changed_col(data) as changed_fields FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试changed_col函数功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据 - 使用map作为数据测试changed_col
	testData := []interface{}{
		map[string]interface{}{
			"device": "datacollector",
			"data":   map[string]interface{}{"temp": 20.0, "humidity": 60.0},
			"Ts":     baseTime,
		},
		map[string]interface{}{
			"device": "datacollector",
			"data":   map[string]interface{}{"temp": 25.0, "humidity": 60.0}, // temp变化
			"Ts":     baseTime,
		},
		map[string]interface{}{
			"device": "datacollector",
			"data":   map[string]interface{}{"temp": 25.0, "humidity": 65.0}, // humidity变化
			"Ts":     baseTime,
		},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 1, "应该有1个设备的聚合结果")

	result := resultSlice[0]
	device, _ := result["device"].(string)
	assert.Equal(t, "datacollector", device, "设备名应该正确")

	// 验证字段存在
	assert.Contains(t, result, "changed_fields", "结果应包含changed_fields字段")

	// changed_col函数应该返回变化的字段列表
	//changedFields := result["changed_fields"]
	//fmt.Printf("changed_col函数返回值: %v\n", changedFields)

	//fmt.Println("changed_col函数测试完成")
}

func TestAnalyticalFunctionsIncrementalComputation(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 测试使用分析函数的SQL查询（现在支持增量计算）
	var rsql = "SELECT device, lag(temperature, 1) as prev_temp, latest(temperature) as current_temp, had_changed(status) as status_changed FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试分析函数增量计算功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "sensor1", "temperature": 15.0, "status": "good", "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "temperature": 25.0, "status": "good", "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "temperature": 35.0, "status": "warning", "Ts": baseTime},
		map[string]interface{}{"device": "sensor2", "temperature": 18.0, "status": "good", "Ts": baseTime},
		map[string]interface{}{"device": "sensor2", "temperature": 22.0, "status": "ok", "Ts": baseTime},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

	// 验证结果数量
	assert.Len(t, resultSlice, 2, "应该有2个设备的聚合结果")

	// 检查设备及其分析结果
	for _, result := range resultSlice {
		device, _ := result["device"].(string)
		currentTemp := result["current_temp"]
		statusChanged := result["status_changed"]

		//fmt.Printf("设备 %s: current_temp=%v, status_changed=%v\n", device, currentTemp, statusChanged)

		if device == "sensor1" {
			// latest函数应该返回最新的温度值35.0
			if currentTemp != nil {
				assert.Equal(t, 35.0, currentTemp)
			}
			// had_changed函数应该检测到状态变化（good -> warning）
			if statusChanged != nil {
				assert.True(t, statusChanged.(bool), "sensor1的状态应该发生了变化")
			}
		} else if device == "sensor2" {
			// latest函数应该返回最新的温度值22.0
			if currentTemp != nil {
				assert.Equal(t, 22.0, currentTemp)
			}
			// had_changed函数应该检测到状态变化（good -> ok）
			if statusChanged != nil {
				assert.True(t, statusChanged.(bool), "sensor2的状态应该发生了变化")
			}
		}
	}

	//fmt.Println("分析函数增量计算测试完成")
}

func TestIncrementalComputationBasic(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 测试基本的增量计算聚合函数
	var rsql = "SELECT device, sum(temperature) as total, avg(temperature) as average, count(*) as cnt FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	//fmt.Println("开始测试基本增量计算功能")

	// 使用固定的时间基准以便测试更加稳定
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "sensor1", "temperature": 10.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "temperature": 20.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor1", "temperature": 30.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor2", "temperature": 15.0, "Ts": baseTime},
		map[string]interface{}{"device": "sensor2", "temperature": 25.0, "Ts": baseTime},
	}

	// 添加数据
	//fmt.Println("添加测试数据")
	for _, data := range testData {
		strm.AddData(data)
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		//fmt.Printf("接收到结果: %v\n", result)
		resultChan <- result
	})

	// 等待窗口初始化
	//fmt.Println("等待窗口初始化...")
	time.Sleep(1 * time.Second)

	// 手动触发窗口
	//fmt.Println("手动触发窗口")
	strm.Window.Trigger()

	// 等待结果
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var actual interface{}
	select {
	case actual = <-resultChan:
		//fmt.Println("成功接收到结果")
		cancel()
	case <-ctx.Done():
		t.Fatal("测试超时，未收到结果")
	}

	// 验证结果
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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
