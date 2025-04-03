package window

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/streamsql/model"
	"github.com/stretchr/testify/assert"
)

func TestSlidingWindow(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	sw, _ := NewSlidingWindow(model.WindowConfig{
		Params: map[string]interface{}{
			"size":  "2s",
			"slide": "1s",
		},
		TsProp: "Ts",
	})
	sw.SetCallback(func(results []interface{}) {
		t.Logf("Received results: %v", results)
	})
	sw.Start()

	// 添加数据
	now := time.Now()
	t_3 := TestDate{Ts: now.Add(-3 * time.Second)}
	t_2 := TestDate{Ts: now.Add(-2 * time.Second)}
	t_1 := TestDate{Ts: now.Add(-1 * time.Second)}
	t_0 := TestDate{Ts: now}

	sw.Add(t_3)
	sw.Add(t_2)
	sw.Add(t_1)
	sw.Add(t_0)

	// 等待一段时间，触发窗口
	time.Sleep(3 * time.Second)

	// 检查结果
	resultsChan := sw.OutputChan()
	var results []interface{}
	select {
	case results = <-resultsChan:
	case <-time.After(100 * time.Second):
		t.Fatal("No results received within timeout")
	}

	// 预期结果：保留最近 2 秒内的数据
	assert.Len(t, results, 2)
	assert.Contains(t, results, t_1)
	assert.Contains(t, results, t_0)
}

type TestDate struct {
	Ts time.Time
}

type TestDate2 struct {
	ts time.Time
}

func (d TestDate2) GetTimestamp() time.Time {
	return d.ts
}

func TestGetTimestamp(t *testing.T) {
	t_0 := time.Now()
	data := map[string]interface{}{"device": "aa", "age": 15.0, "score": 100, "ts": t_0}
	t_1 := GetTimestamp(data, "ts")

	data_1 := TestDate{Ts: t_0}
	t_2 := GetTimestamp(data_1, "Ts")

	data_2 := TestDate2{ts: t_0}
	t_3 := GetTimestamp(data_2, "")

	assert.Equal(t, t_0, t_1)
	assert.Equal(t, t_0, t_2)
	assert.Equal(t, t_0, t_3)
}
