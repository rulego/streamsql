package streamsql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"math/rand"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamData(t *testing.T) {
	ssql := New()
	//TumblingWindow 滚动窗口，2秒滚动一次
	rsql := "SELECT deviceId,max(temperature) as max_temp,min(humidity) as min_humidity ,window_start() as start,window_end() as end FROM  stream group by deviceId,TumblingWindow('2s')"
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}
	// 添加测试数据
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// 生成随机测试数据
				randomData := map[string]interface{}{
					"deviceId":    fmt.Sprintf("device%d", rand.Intn(2)+1),
					"temperature": 20.0 + rand.Float64()*10, // 20-30度之间
					"humidity":    50.0 + rand.Float64()*20, // 50-70%湿度
				}
				ssql.stream.AddData(randomData)
			}
		}
	}()

	// 添加结果回调
	resultChan := make(chan interface{})
	ssql.stream.AddSink(func(result interface{}) {
		resultChan <- result
	})
	//打印结果
	go func() {
		for result := range resultChan {
			fmt.Printf("打印结果: %v\n", result)
		}
	}()

	time.Sleep(30 * time.Second)
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
