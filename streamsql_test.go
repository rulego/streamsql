package streamsql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"math/rand"

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
		for result := range resultChan {
			//每隔5秒打印一次结果
			fmt.Printf("打印结果: [%s] %v\n", time.Now().Format("15:04:05.000"), result)
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
	var rsql = "SELECT device,max(age) as max_age,min(score) as min_score,window_start() as start,window_end() as end FROM stream group by device,SlidingWindow('2s','1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "age": 5.0, "score": 100, "Ts": baseTime},
		map[string]interface{}{"device": "aa", "age": 10.0, "score": 200, "Ts": baseTime.Add(1 * time.Second)},
		map[string]interface{}{"device": "bb", "age": 3.0, "score": 300, "Ts": baseTime},
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
			"device":    "aa",
			"max_age":   10.0,
			"min_score": 100.0,
			"start":     baseTime.UnixNano(),
			"end":       baseTime.Add(2 * time.Second).UnixNano(),
		},
		{
			"device":    "bb",
			"max_age":   3.0,
			"min_score": 300.0,
			"start":     baseTime.UnixNano(),
			"end":       baseTime.Add(2 * time.Second).UnixNano(),
		},
	}

	assert.IsType(t, []map[string]interface{}{}, actual)
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok)
	assert.Len(t, resultSlice, 2)
	for _, expectedResult := range expected {
		found := false
		for _, resultMap := range resultSlice {
			//if resultMap, ok := result.(map[string]interface{}); ok {
			if resultMap["device"] == expectedResult["device"] {
				assert.InEpsilon(t, expectedResult["max_age"].(float64), resultMap["max_age"].(float64), 0.0001)
				assert.InEpsilon(t, expectedResult["min_score"].(float64), resultMap["min_score"].(float64), 0.0001)
				assert.Equal(t, expectedResult["start"].(int64), resultMap["start"].(int64))
				assert.Equal(t, expectedResult["end"].(int64), resultMap["end"].(int64))
				found = true
				break
			}
			//}
		}
		assert.True(t, found, fmt.Sprintf("Expected result for device %v not found", expectedResult["device"]))
	}
}

func TestStreamsqlWithoutGroupBy(t *testing.T) {
	streamsql := New()
	var rsql = "SELECT max(age) as max_age,min(score) as min_score,window_start() as start,window_end() as end FROM stream SlidingWindow('2s','1s') with (TIMESTAMP='Ts',TIMEUNIT='ss')"
	err := streamsql.Execute(rsql)
	assert.Nil(t, err)
	strm := streamsql.stream
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)
	testData := []interface{}{
		map[string]interface{}{"device": "aa", "age": 5.0, "score": 100, "Ts": baseTime},
		map[string]interface{}{"device": "aa", "age": 10.0, "score": 200, "Ts": baseTime.Add(1 * time.Second)},
		map[string]interface{}{"device": "bb", "age": 3.0, "score": 300, "Ts": baseTime},
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
			"max_age":   10.0,
			"min_score": 100.0,
			"start":     baseTime.UnixNano(),
			"end":       baseTime.Add(2 * time.Second).UnixNano(),
		},
	}

	assert.IsType(t, []map[string]interface{}{}, actual)
	resultSlice, ok := actual.([]map[string]interface{})
	require.True(t, ok)
	assert.Len(t, resultSlice, 1)
	for _, expectedResult := range expected {
		//found := false
		for _, resultMap := range resultSlice {
			assert.InEpsilon(t, expectedResult["max_age"].(float64), resultMap["max_age"].(float64), 0.0001)
			assert.InEpsilon(t, expectedResult["min_score"].(float64), resultMap["min_score"].(float64), 0.0001)
			assert.Equal(t, expectedResult["start"].(int64), resultMap["start"].(int64))
			assert.Equal(t, expectedResult["end"].(int64), resultMap["end"].(int64))
		}
		//assert.True(t, found, fmt.Sprintf("Expected result for device %v not found", expectedResult["device"]))
	}
}
