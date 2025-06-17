package streamsql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/utils/cast"

	"math/rand"

	"github.com/rulego/streamsql/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamData 测试 StreamSQL 的流式数据处理功能
// 这个测试演示了 StreamSQL 的完整工作流程：从创建实例到数据处理再到结果验证
func TestStreamData(t *testing.T) {
	// 步骤1: 创建 StreamSQL 实例
	// StreamSQL 是流式 SQL 处理引擎的核心组件，负责管理整个流处理生命周期
	ssql := New()

	// 步骤2: 定义流式 SQL 查询语句
	// 这个 SQL 语句展示了 StreamSQL 的核心功能：
	// - SELECT: 选择要输出的字段和聚合函数
	// - FROM stream: 指定数据源为流数据
	// - WHERE: 过滤条件，排除 device3 的数据
	// - GROUP BY: 按设备ID分组，配合滚动窗口进行聚合
	// - TumblingWindow('5s'): 5秒滚动窗口，每5秒触发一次计算
	// - avg(), min(): 聚合函数，计算平均值和最小值
	// - window_start(), window_end(): 窗口函数，获取窗口的开始和结束时间
	rsql := "SELECT deviceId,avg(temperature) as avg_temp,min(humidity) as min_humidity ," +
		"window_start() as start,window_end() as end FROM  stream  where deviceId!='device3' group by deviceId,TumblingWindow('5s')"

	// 步骤3: 执行 SQL 语句，启动流式分析任务
	// Execute 方法会解析 SQL、构建执行计划、初始化窗口管理器和聚合器
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	// 步骤4: 设置测试环境和并发控制
	var wg sync.WaitGroup
	wg.Add(1)
	// 设置30秒测试超时时间，防止测试无限运行
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 步骤5: 启动数据生产者协程
	// 模拟实时数据流，持续向 StreamSQL 输入数据
	go func() {
		defer wg.Done()
		// 创建定时器，每秒触发一次数据生成
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// 每秒生成10条随机测试数据，模拟高频数据流
				// 这种数据密度可以测试 StreamSQL 的实时处理能力
				for i := 0; i < 10; i++ {
					// 构造设备数据，包含设备ID、温度和湿度
					randomData := map[string]interface{}{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(3)+1), // 随机选择 device1, device2, device3
						"temperature": 20.0 + rand.Float64()*10,                // 温度范围: 20-30度
						"humidity":    50.0 + rand.Float64()*20,                // 湿度范围: 50-70%
					}
					// 将数据添加到流中，触发 StreamSQL 的实时处理
					// AddData 会将数据分发到相应的窗口和聚合器中
					ssql.stream.AddData(randomData)
				}

			case <-ctx.Done():
				// 超时或取消信号，停止数据生成
				return
			}
		}
	}()

	// 步骤6: 设置结果处理管道
	resultChan := make(chan interface{})
	// 添加计算结果回调函数（Sink）
	// 当窗口触发计算时，结果会通过这个回调函数输出
	ssql.stream.AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 步骤7: 启动结果消费者协程
	// 记录收到的结果数量，用于验证测试效果
	var resultCount int64
	var countMutex sync.Mutex
	go func() {
		for range resultChan {
			// 每当收到一个窗口的计算结果时，计数器加1
			// 注释掉的代码可以用于调试，打印每个结果的详细信息
			//fmt.Printf("打印结果: [%s] %v\n", time.Now().Format("15:04:05.000"), result)
			countMutex.Lock()
			resultCount++
			countMutex.Unlock()
		}
	}()

	// 步骤8: 等待测试完成
	// 等待数据生产者协程结束（30秒超时或手动取消）
	wg.Wait()

	// 步骤9: 验证测试结果
	// 预期在30秒内应该收到5个窗口的计算结果（每5秒一个窗口）
	// 这验证了 StreamSQL 的窗口触发机制是否正常工作
	countMutex.Lock()
	finalCount := resultCount
	countMutex.Unlock()
	assert.Equal(t, finalCount, int64(5))
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
	// 测试场景1：简单LIMIT功能，不使用窗口函数
	t.Run("简单LIMIT查询", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		var rsql = "SELECT * FROM stream LIMIT 2"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)
		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果接收器
		strm.AddSink(func(result interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []interface{}{
			map[string]interface{}{"device": "aa", "temperature": 25.0},
			map[string]interface{}{"device": "bb", "temperature": 22.0},
			map[string]interface{}{"device": "cc", "temperature": 30.0},
			map[string]interface{}{"device": "dd", "temperature": 28.0},
		}

		// 实时验证：添加一条数据，立即验证一条结果
		for i, data := range testData {
			// 添加数据
			strm.AddData(data)

			// 立即等待并验证结果
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			select {
			case result := <-resultChan:
				// 验证结果格式
				resultSlice, ok := result.([]map[string]interface{})
				require.True(t, ok, "结果应该是[]map[string]interface{}类型")

				// 验证LIMIT限制：每个batch最多2条记录
				assert.LessOrEqual(t, len(resultSlice), 2, "每个batch最多2条记录")
				assert.Greater(t, len(resultSlice), 0, "应该有结果")

				// 验证字段
				for _, item := range resultSlice {
					assert.Contains(t, item, "device", "结果应包含device字段")
					assert.Contains(t, item, "temperature", "结果应包含temperature字段")
				}
				_ = i
				//t.Logf("第%d条数据处理完成，收到%d条结果记录", i+1, len(resultSlice))
				cancel()
			case <-ctx.Done():
				cancel()
				//t.Fatalf("第%d条数据添加后超时，未收到实时结果", i+1)
			}
		}

		// 验证总体处理：由于LIMIT 2，应该处理完4条数据
		//t.Log("所有数据都得到了实时处理，符合非聚合场景的流处理特性")
	})

	// 测试场景2：聚合查询 + LIMIT
	t.Run("聚合查询与LIMIT", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		var rsql = "SELECT device, avg(temperature) as avg_temp, count(*) as cnt FROM stream GROUP BY device LIMIT 2"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)
		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果回调
		strm.AddSink(func(result interface{}) {
			resultChan <- result
		})

		// 添加测试数据 - 多个设备的温度数据
		testData := []interface{}{
			map[string]interface{}{"device": "sensor1", "temperature": 20.0},
			map[string]interface{}{"device": "sensor1", "temperature": 22.0},
			map[string]interface{}{"device": "sensor2", "temperature": 25.0},
			map[string]interface{}{"device": "sensor2", "temperature": 27.0},
			map[string]interface{}{"device": "sensor3", "temperature": 30.0},
			map[string]interface{}{"device": "sensor3", "temperature": 32.0},
			map[string]interface{}{"device": "sensor4", "temperature": 35.0},
			map[string]interface{}{"device": "sensor4", "temperature": 37.0},
		}

		// 添加数据
		for _, data := range testData {
			strm.AddData(data)
		}

		// 等待聚合
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
			t.Fatal("测试超时，未收到聚合结果")
		}

		// 验证聚合结果
		resultSlice, ok := actual.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")

		// LIMIT 2 应该只返回2个设备的聚合结果
		assert.LessOrEqual(t, len(resultSlice), 2, "聚合结果应该限制在2条以内")
		assert.Greater(t, len(resultSlice), 0, "应该有聚合结果")

		// 验证聚合字段
		for _, result := range resultSlice {
			assert.Contains(t, result, "device", "结果应包含device字段")
			assert.Contains(t, result, "avg_temp", "结果应包含avg_temp字段")
			assert.Contains(t, result, "cnt", "结果应包含cnt字段")

			// 验证聚合值的类型和合理性
			avgTemp, ok := result["avg_temp"].(float64)
			assert.True(t, ok, "avg_temp应该是float64类型")
			assert.Greater(t, avgTemp, 0.0, "平均温度应该大于0")

			cnt, ok := result["cnt"].(float64)
			assert.True(t, ok, "cnt应该是float64类型")
			assert.GreaterOrEqual(t, cnt, 1.0, "计数应该至少为1")
		}
	})

	// 测试场景3：窗口聚合 + LIMIT
	t.Run("窗口聚合与LIMIT", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		var rsql = "SELECT device, max(temperature) as max_temp, min(temperature) as min_temp FROM stream GROUP BY device, TumblingWindow('1s') LIMIT 3"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)
		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果回调
		strm.AddSink(func(result interface{}) {
			resultChan <- result
		})

		// 添加测试数据 - 5个设备的数据
		testData := []interface{}{
			map[string]interface{}{"device": "dev1", "temperature": 20.0},
			map[string]interface{}{"device": "dev2", "temperature": 25.0},
			map[string]interface{}{"device": "dev3", "temperature": 30.0},
			map[string]interface{}{"device": "dev4", "temperature": 35.0},
			map[string]interface{}{"device": "dev5", "temperature": 40.0},
		}

		// 添加数据
		for _, data := range testData {
			strm.AddData(data)
		}

		// 等待窗口触发
		time.Sleep(1200 * time.Millisecond) // 等待超过窗口大小

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var actual interface{}
		select {
		case actual = <-resultChan:
			cancel()
		case <-ctx.Done():
			t.Fatal("测试超时，未收到窗口聚合结果")
		}

		// 验证窗口聚合结果
		resultSlice, ok := actual.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")

		// LIMIT 3 应该只返回3个设备的聚合结果
		assert.LessOrEqual(t, len(resultSlice), 3, "窗口聚合结果应该限制在3条以内")
		assert.Greater(t, len(resultSlice), 0, "应该有窗口聚合结果")

		// 验证聚合字段
		for _, result := range resultSlice {
			assert.Contains(t, result, "device", "结果应包含device字段")
			assert.Contains(t, result, "max_temp", "结果应包含max_temp字段")
			assert.Contains(t, result, "min_temp", "结果应包含min_temp字段")

			// 验证最大值和最小值
			maxTemp, ok := result["max_temp"].(float64)
			assert.True(t, ok, "max_temp应该是float64类型")
			minTemp, ok := result["min_temp"].(float64)
			assert.True(t, ok, "min_temp应该是float64类型")
			assert.GreaterOrEqual(t, maxTemp, minTemp, "最大值应该大于等于最小值")
		}
	})

	// 测试场景4：HAVING + LIMIT 组合
	t.Run("HAVING与LIMIT组合", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		var rsql = "SELECT device, avg(temperature) as avg_temp FROM stream GROUP BY device HAVING avg_temp > 25 LIMIT 2"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)
		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果回调
		strm.AddSink(func(result interface{}) {
			resultChan <- result
		})

		// 添加测试数据 - 设计一些平均温度大于25的设备
		testData := []interface{}{
			map[string]interface{}{"device": "cold_sensor", "temperature": 15.0},
			map[string]interface{}{"device": "cold_sensor", "temperature": 18.0}, // 平均16.5，不满足条件
			map[string]interface{}{"device": "warm_sensor1", "temperature": 26.0},
			map[string]interface{}{"device": "warm_sensor1", "temperature": 28.0}, // 平均27，满足条件
			map[string]interface{}{"device": "warm_sensor2", "temperature": 30.0},
			map[string]interface{}{"device": "warm_sensor2", "temperature": 32.0}, // 平均31，满足条件
			map[string]interface{}{"device": "warm_sensor3", "temperature": 35.0},
			map[string]interface{}{"device": "warm_sensor3", "temperature": 37.0}, // 平均36，满足条件
		}

		// 添加数据
		for _, data := range testData {
			strm.AddData(data)
		}

		// 等待聚合
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
			t.Fatal("测试超时，未收到HAVING+LIMIT结果")
		}

		// 验证HAVING + LIMIT结果
		resultSlice, ok := actual.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")

		// LIMIT 2 + HAVING条件，最多2条符合条件的结果
		assert.LessOrEqual(t, len(resultSlice), 2, "HAVING+LIMIT结果应该限制在2条以内")

		// 验证所有结果都满足HAVING条件
		for _, result := range resultSlice {
			assert.Contains(t, result, "device", "结果应包含device字段")
			assert.Contains(t, result, "avg_temp", "结果应包含avg_temp字段")

			// 验证不包含cold_sensor（平均温度<25）
			assert.NotEqual(t, "cold_sensor", result["device"], "结果不应包含cold_sensor")

			// 验证平均温度确实大于25
			avgTemp, ok := result["avg_temp"].(float64)
			assert.True(t, ok, "avg_temp应该是float64类型")
			assert.Greater(t, avgTemp, 25.0, "avg_temp应该大于25（满足HAVING条件）")
		}
	})
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

func TestSessionWindow(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 使用 SESSION 窗口，超时时间为 2 秒
	rsql := "SELECT device, avg(temperature) as avg_temp FROM stream GROUP BY device, SESSIONWINDOW('2s') with (TIMESTAMP='Ts')"
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

	baseTime := time.Now()

	// 添加测试数据 - 两个设备，不同的时间
	testData := []struct {
		data interface{}
		wait time.Duration
	}{
		// 第一组数据 - device1
		{map[string]interface{}{"device": "device1", "temperature": 20.0, "Ts": baseTime}, 0},
		{map[string]interface{}{"device": "device1", "temperature": 22.0, "Ts": baseTime.Add(500 * time.Millisecond)}, 500 * time.Millisecond},

		// 第二组数据 - device2
		{map[string]interface{}{"device": "device2", "temperature": 25.0, "Ts": baseTime.Add(time.Second)}, time.Second},
		{map[string]interface{}{"device": "device2", "temperature": 27.0, "Ts": baseTime.Add(1500 * time.Millisecond)}, 500 * time.Millisecond},

		// 间隔超过会话超时

		// 第三组数据 - device1，新会话
		{map[string]interface{}{"device": "device1", "temperature": 30.0, "Ts": baseTime.Add(5 * time.Second)}, 3 * time.Second},
	}

	// 按指定的间隔添加数据
	for _, item := range testData {
		if item.wait > 0 {
			time.Sleep(item.wait)
		}
		strm.AddData(item.data)
	}

	// 等待会话超时，使最后一个会话触发
	time.Sleep(3 * time.Second)

	// 手动触发所有窗口，确保数据被处理
	strm.Window.Trigger()

	// 收集结果
	var results []interface{}
	var resultsMutex sync.Mutex

	// 等待接收结果
	timeout := time.After(5 * time.Second)
	done := false

	for !done {
		select {
		case result := <-resultChan:
			resultsMutex.Lock()
			results = append(results, result)
			resultCount := len(results)
			resultsMutex.Unlock()
			// 我们期望至少 3 个会话结果
			if resultCount >= 3 {
				done = true
			}
		case <-timeout:
			// 超时，可能没有收到足够的结果
			done = true
		}
	}

	// 验证结果
	resultsMutex.Lock()
	resultCount := len(results)
	resultsCopy := make([]interface{}, len(results))
	copy(resultsCopy, results)
	resultsMutex.Unlock()

	assert.GreaterOrEqual(t, resultCount, 2, "应该至少收到两个会话的结果")

	// 检查结果中是否包含两个设备的会话
	hasDevice1 := false
	hasDevice2 := false

	for _, result := range resultsCopy {
		resultSlice, ok := result.([]map[string]interface{})
		assert.True(t, ok, "结果应该是[]map[string]interface{}类型")

		for _, item := range resultSlice {
			device, ok := item["device"].(string)
			assert.True(t, ok, "device字段应该是string类型")

			if device == "device1" {
				hasDevice1 = true
			} else if device == "device2" {
				hasDevice2 = true
			}
		}
	}

	assert.True(t, hasDevice1, "结果中应该包含device1的会话")
	assert.True(t, hasDevice2, "结果中应该包含device2的会话")
}

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

// TestExprFunctions 测试expr函数的使用
func TestExprFunctions(t *testing.T) {
	streamsql := New()
	defer streamsql.Stop()

	// 测试基本expr函数：字符串处理
	var rsql = "SELECT device, upper(device) as upper_device, lower(device) as lower_device FROM stream"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "SensorA"},
		map[string]interface{}{"device": "SensorB"},
	}

	// 添加数据
	for _, data := range testData {
		strm.AddData(data)
	}

	// 等待结果
	var results []interface{}
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
	resultsCopy := make([]interface{}, len(results))
	copy(resultsCopy, results)
	resultsMutex.Unlock()

	assert.Greater(t, finalResultCount, 0, "应该收到至少一条结果")

	for _, result := range resultsCopy {
		resultSlice, ok := result.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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
	streamsql := New()
	defer streamsql.Stop()

	// 测试在聚合函数中使用expr函数：数学计算
	var rsql = "SELECT device, AVG(abs(temperature - 25)) as avg_deviation, MAX(ceil(temperature)) as max_ceil FROM stream GROUP BY device, TumblingWindow('1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	// 使用固定的时间基准
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "sensor1", "temperature": 23.5, "Ts": baseTime}, // abs(23.5-25) = 1.5, ceil(23.5) = 24
		map[string]interface{}{"device": "sensor1", "temperature": 26.8, "Ts": baseTime}, // abs(26.8-25) = 1.8, ceil(26.8) = 27
		map[string]interface{}{"device": "sensor2", "temperature": 24.2, "Ts": baseTime}, // abs(24.2-25) = 0.8, ceil(24.2) = 25
		map[string]interface{}{"device": "sensor2", "temperature": 25.9, "Ts": baseTime}, // abs(25.9-25) = 0.9, ceil(25.9) = 26
	}

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加数据
	for _, data := range testData {
		strm.AddData(data)
	}

	// 等待窗口初始化
	time.Sleep(1 * time.Second)

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
	streamsql := New()
	defer streamsql.Stop()

	// 测试嵌套函数：字符串处理 + 数组操作
	var rsql = "SELECT device, len(split(upper(device), 'SENSOR')) as split_count FROM stream"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "sensor1"},      // upper -> "SENSOR1", split by "SENSOR" -> ["", "1"], len -> 2
		map[string]interface{}{"device": "sensorsensor"}, // upper -> "SENSORSENSOR", split by "SENSOR" -> ["", "", ""], len -> 3
		map[string]interface{}{"device": "device1"},      // upper -> "DEVICE1", split by "SENSOR" -> ["DEVICE1"], len -> 1
	}

	// 添加数据
	for _, data := range testData {
		strm.AddData(data)
	}

	// 等待结果
	var results []interface{}
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
	resultsCopy := make([]interface{}, len(results))
	copy(resultsCopy, results)
	resultsMutex.Unlock()

	assert.Greater(t, finalResultCount, 0, "应该收到至少一条结果")

	deviceResults := make(map[string]float64)
	for _, result := range resultsCopy {
		resultSlice, ok := result.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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
	streamsql := New()
	defer streamsql.Stop()

	// 测试混合使用：StreamSQL的concat函数 + expr的upper函数
	var rsql = "SELECT device, concat(upper(device), '_processed') as processed_name FROM stream"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream

	// 创建结果接收通道
	resultChan := make(chan interface{}, 10)

	// 添加结果回调
	strm.AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []interface{}{
		map[string]interface{}{"device": "sensor1"},
		map[string]interface{}{"device": "device2"},
	}

	// 添加数据
	for _, data := range testData {
		strm.AddData(data)
	}

	// 等待结果
	var results []interface{}
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
	resultsCopy := make([]interface{}, len(results))
	copy(resultsCopy, results)
	resultsMutex.Unlock()

	assert.Greater(t, finalResultCount, 0, "应该收到至少一条结果")

	for _, result := range resultsCopy {
		resultSlice, ok := result.([]map[string]interface{})
		require.True(t, ok, "结果应该是[]map[string]interface{}类型")

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
func TestSelectAllFeature(t *testing.T) {
	// 测试场景1：基本SELECT *查询
	t.Run("基本SELECT *查询", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		var rsql = "SELECT * FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)
		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果接收器
		strm.AddSink(func(result interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := map[string]interface{}{
			"device":      "sensor001",
			"temperature": 25.5,
			"humidity":    60,
			"location":    "room1",
			"status":      "active",
		}

		// 发送数据
		strm.AddData(testData)

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			// 验证结果
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok, "结果应该是[]map[string]interface{}类型")
			require.Len(t, resultSlice, 1, "应该只有一条结果")

			item := resultSlice[0]
			// 验证所有原始字段都存在
			assert.Equal(t, "sensor001", item["device"], "device字段应该正确")
			assert.Equal(t, 25.5, item["temperature"], "temperature字段应该正确")
			assert.Equal(t, 60, item["humidity"], "humidity字段应该正确")
			assert.Equal(t, "room1", item["location"], "location字段应该正确")
			assert.Equal(t, "active", item["status"], "status字段应该正确")

			// 验证字段数量
			assert.Len(t, item, 5, "应该包含所有5个字段")

			cancel()
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})

	// 测试场景2：SELECT * + WHERE条件
	t.Run("SELECT * + WHERE条件", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		var rsql = "SELECT * FROM stream WHERE temperature > 20"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)
		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果接收器
		strm.AddSink(func(result interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []map[string]interface{}{
			{"device": "sensor1", "temperature": 25.0, "humidity": 60}, // 应该被包含
			{"device": "sensor2", "temperature": 15.0, "humidity": 70}, // 应该被过滤掉
			{"device": "sensor3", "temperature": 30.0, "humidity": 50}, // 应该被包含
		}

		var results []interface{}
		var resultsMutex sync.Mutex

		// 发送数据
		for _, data := range testData {
			strm.AddData(data)

			// 立即检查结果
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			select {
			case result := <-resultChan:
				resultsMutex.Lock()
				results = append(results, result)
				resultsMutex.Unlock()
				cancel()
			case <-ctx.Done():
				cancel()
				// 对于不满足条件的数据，超时是正常的
			}
		}

		// 验证结果
		resultsMutex.Lock()
		finalResultCount := len(results)
		resultsCopy := make([]interface{}, len(results))
		copy(resultsCopy, results)
		resultsMutex.Unlock()

		assert.Equal(t, 2, finalResultCount, "应该有2条记录满足条件")

		// 验证结果内容
		deviceFound := make(map[string]bool)
		for _, result := range resultsCopy {
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok, "结果应该是[]map[string]interface{}类型")
			require.Len(t, resultSlice, 1, "每个结果应该只有一条记录")

			item := resultSlice[0]
			device, _ := item["device"].(string)
			temp, _ := item["temperature"].(float64)

			// 验证温度条件
			assert.Greater(t, temp, 20.0, "温度应该大于20")

			// 记录找到的设备
			deviceFound[device] = true

			// 验证所有字段都存在
			assert.Contains(t, item, "device", "应该包含device字段")
			assert.Contains(t, item, "temperature", "应该包含temperature字段")
			assert.Contains(t, item, "humidity", "应该包含humidity字段")
		}

		// 验证正确的设备被包含
		assert.True(t, deviceFound["sensor1"], "sensor1应该被包含")
		assert.True(t, deviceFound["sensor3"], "sensor3应该被包含")
		assert.False(t, deviceFound["sensor2"], "sensor2不应该被包含")
	})

	// 测试场景3：SELECT * + LIMIT
	t.Run("SELECT * + LIMIT", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		var rsql = "SELECT * FROM stream LIMIT 2"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)
		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果接收器
		strm.AddSink(func(result interface{}) {
			resultChan <- result
		})

		// 添加测试数据
		testData := []map[string]interface{}{
			{"device": "sensor1", "temperature": 25.0},
			{"device": "sensor2", "temperature": 26.0},
			{"device": "sensor3", "temperature": 27.0},
			{"device": "sensor4", "temperature": 28.0},
		}

		var results []interface{}
		var resultsMutex sync.Mutex

		// 发送数据
		for _, data := range testData {
			strm.AddData(data)

			// 立即检查结果
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			select {
			case result := <-resultChan:
				resultsMutex.Lock()
				results = append(results, result)
				resultsMutex.Unlock()
				cancel()
			case <-ctx.Done():
				cancel()
			}
		}

		// 验证结果
		resultsMutex.Lock()
		finalResultCount := len(results)
		resultsCopy := make([]interface{}, len(results))
		copy(resultsCopy, results)
		resultsMutex.Unlock()

		assert.GreaterOrEqual(t, finalResultCount, 2, "应该至少有2条结果")

		// 验证结果内容
		for _, result := range resultsCopy {
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok, "结果应该是[]map[string]interface{}类型")

			// 验证LIMIT限制：每个batch最多2条记录
			assert.LessOrEqual(t, len(resultSlice), 2, "每个batch最多2条记录")
			assert.Greater(t, len(resultSlice), 0, "应该有结果")

			// 验证字段
			for _, item := range resultSlice {
				assert.Contains(t, item, "device", "结果应包含device字段")
				assert.Contains(t, item, "temperature", "结果应包含temperature字段")
			}
		}
	})

	// 测试场景4：SELECT * with嵌套字段
	t.Run("SELECT * with嵌套字段", func(t *testing.T) {
		streamsql := New()
		defer streamsql.Stop()

		var rsql = "SELECT * FROM stream"
		err := streamsql.Execute(rsql)
		assert.Nil(t, err)
		strm := streamsql.stream

		// 创建结果接收通道
		resultChan := make(chan interface{}, 10)

		// 添加结果接收器
		strm.AddSink(func(result interface{}) {
			resultChan <- result
		})

		// 添加带嵌套字段的测试数据
		testData := map[string]interface{}{
			"device": "sensor001",
			"metrics": map[string]interface{}{
				"temperature": 25.5,
				"humidity":    60,
			},
			"location": map[string]interface{}{
				"building": "A",
				"room":     "101",
			},
		}

		// 发送数据
		strm.AddData(testData)

		// 等待结果
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		select {
		case result := <-resultChan:
			// 验证结果
			resultSlice, ok := result.([]map[string]interface{})
			require.True(t, ok, "结果应该是[]map[string]interface{}类型")
			require.Len(t, resultSlice, 1, "应该只有一条结果")

			item := resultSlice[0]
			// 验证顶级字段
			assert.Equal(t, "sensor001", item["device"], "device字段应该正确")

			// 验证嵌套字段结构被保留
			metrics, ok := item["metrics"].(map[string]interface{})
			assert.True(t, ok, "metrics应该是map类型")
			assert.Equal(t, 25.5, metrics["temperature"], "嵌套temperature字段应该正确")
			assert.Equal(t, 60, metrics["humidity"], "嵌套humidity字段应该正确")

			location, ok := item["location"].(map[string]interface{})
			assert.True(t, ok, "location应该是map类型")
			assert.Equal(t, "A", location["building"], "嵌套building字段应该正确")
			assert.Equal(t, "101", location["room"], "嵌套room字段应该正确")

			cancel()
		case <-ctx.Done():
			t.Fatal("测试超时，未收到结果")
		}
	})
}

// TestCaseNullValueHandlingInAggregation 测试CASE表达式在聚合函数中正确处理NULL值
func TestCaseNullValueHandlingInAggregation(t *testing.T) {
	sql := `SELECT deviceType,
	              SUM(CASE WHEN temperature > 30 THEN temperature ELSE NULL END) as high_temp_sum,
	              COUNT(CASE WHEN temperature > 30 THEN 1 ELSE NULL END) as high_temp_count,
	              AVG(CASE WHEN temperature > 30 THEN temperature ELSE NULL END) as high_temp_avg
	         FROM stream 
	         GROUP BY deviceType, TumblingWindow('2s')`

	// 创建StreamSQL实例
	ssql := New()
	defer ssql.Stop()

	// 执行SQL
	err := ssql.Execute(sql)
	require.NoError(t, err)

	// 收集结果
	var results []map[string]interface{}
	resultChan := make(chan interface{}, 10)

	ssql.Stream().AddSink(func(result interface{}) {
		resultChan <- result
	})

	// 添加测试数据
	testData := []map[string]interface{}{
		{"deviceType": "sensor", "temperature": 35.0},  // 满足条件
		{"deviceType": "sensor", "temperature": 25.0},  // 不满足条件，返回NULL
		{"deviceType": "sensor", "temperature": 32.0},  // 满足条件
		{"deviceType": "monitor", "temperature": 28.0}, // 不满足条件，返回NULL
		{"deviceType": "monitor", "temperature": 33.0}, // 满足条件
	}

	for _, data := range testData {
		ssql.Stream().AddData(data)
	}

	// 等待窗口触发
	time.Sleep(3 * time.Second)

	// 收集结果
collecting:
	for {
		select {
		case result := <-resultChan:
			if resultSlice, ok := result.([]map[string]interface{}); ok {
				results = append(results, resultSlice...)
			}
		case <-time.After(500 * time.Millisecond):
			break collecting
		}
	}

	// 验证结果
	assert.Len(t, results, 2, "应该有两个设备类型的结果")

	// 验证各个deviceType的结果
	expectedResults := map[string]map[string]interface{}{
		"sensor": {
			"high_temp_sum":   67.0, // 35 + 32
			"high_temp_count": 2.0,  // COUNT应该忽略NULL
			"high_temp_avg":   33.5, // (35 + 32) / 2
		},
		"monitor": {
			"high_temp_sum":   33.0, // 只有33
			"high_temp_count": 1.0,  // COUNT应该忽略NULL
			"high_temp_avg":   33.0, // 只有33
		},
	}

	for _, result := range results {
		deviceType := result["deviceType"].(string)
		expected := expectedResults[deviceType]

		assert.NotNil(t, expected, "应该有设备类型 %s 的期望结果", deviceType)

		// 验证SUM聚合（忽略NULL值）
		assert.Equal(t, expected["high_temp_sum"], result["high_temp_sum"],
			"设备类型 %s 的SUM聚合结果应该正确", deviceType)

		// 验证COUNT聚合（忽略NULL值）
		assert.Equal(t, expected["high_temp_count"], result["high_temp_count"],
			"设备类型 %s 的COUNT聚合结果应该正确", deviceType)

		// 验证AVG聚合（忽略NULL值）
		assert.Equal(t, expected["high_temp_avg"], result["high_temp_avg"],
			"设备类型 %s 的AVG聚合结果应该正确", deviceType)
	}
}
