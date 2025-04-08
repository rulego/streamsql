package streamsql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	// 等待 3 秒触发窗口
	time.Sleep(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
