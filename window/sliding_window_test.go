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
		TsProp:   "Ts",
		TimeUnit: time.Second,
	})
	sw.SetCallback(func(results []model.Row) {
		t.Logf("Received results: %v", results)
	})
	sw.Start()

	// 添加数据
	t_3 := TestDate{Ts: time.Date(2025, 4, 7, 16, 46, 56, 789000000, time.UTC), tag: "1"}
	t_2 := TestDate{Ts: time.Date(2025, 4, 7, 16, 46, 57, 789000000, time.UTC), tag: "2"}
	t_1 := TestDate{Ts: time.Date(2025, 4, 7, 16, 46, 58, 789000000, time.UTC), tag: "3"}
	t_0 := TestDate{Ts: time.Date(2025, 4, 7, 16, 46, 59, 789000000, time.UTC), tag: "4"}

	sw.Add(t_3)
	sw.Add(t_2)
	sw.Add(t_1)
	sw.Add(t_0)

	// 等待一段时间，触发窗口
	//time.Sleep(3 * time.Second)

	// 检查结果
	resultsChan := sw.OutputChan()
	var results []model.Row

	for {
		select {
		case results = <-resultsChan:
			raw := make([]TestDate, 0)
			for _, row := range results {
				raw = append(raw, row.Data.(TestDate))
			}

			// 获取当前窗口的时间范围
			windowStart := results[0].Slot.Start
			windowEnd := results[0].Slot.End
			t.Logf("Window range: %v - %v", windowStart, windowEnd)

			// 检查窗口内的数据
			expectedData := make([]TestDate, 0)
			if windowStart.Before(t_3.Ts) && windowEnd.After(t_2.Ts) {
				expectedData = []TestDate{t_3, t_2}
			} else if windowStart.Before(t_2.Ts) && windowEnd.After(t_1.Ts) {
				expectedData = []TestDate{t_2, t_1}
			} else if windowStart.Before(t_1.Ts) && windowEnd.After(t_0.Ts) {
				expectedData = []TestDate{t_1, t_0}
			} else {
				expectedData = []TestDate{t_0}
			}

			// 验证窗口数据
			assert.Equal(t, len(expectedData), len(raw), "窗口数据数量不匹配")
			for _, expected := range expectedData {
				assert.Contains(t, raw, expected, "窗口缺少预期数据")
			}
		default:
			// 通道为空时退出
			goto END
		}
	}

END:
	// 预期结果：保留最近 2 秒内的数据
	assert.Len(t, results, 0)
}

type TestDate struct {
	Ts  time.Time
	tag string
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
