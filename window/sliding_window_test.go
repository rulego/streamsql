package window

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/streamsql/model"
	timex "github.com/rulego/streamsql/utils"
	"github.com/stretchr/testify/assert"
)

type TestResult struct {
	size  int
	data  []TestDate
	start time.Time
	end   time.Time
}

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
		if len(results) == 0 {
			return
		}
		for _, row := range results {
			t.Logf("Slot: %v Received row: %v", row.Slot, row.Data)
		}

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

	// 验证每个窗口的数据
	expected := []TestResult{
		{size: 2, data: []TestDate{t_3, t_2}, start: timex.AlignTime(t_3.Ts, time.Second, true), end: timex.AlignTime(t_2.Ts, time.Second, false)},
		{size: 2, data: []TestDate{t_2, t_1}, start: timex.AlignTime(t_2.Ts, time.Second, true), end: timex.AlignTime(t_1.Ts, time.Second, false)},
		{size: 2, data: []TestDate{t_1, t_0}, start: timex.AlignTime(t_1.Ts, time.Second, true), end: timex.AlignTime(t_0.Ts, time.Second, false)},
		{size: 1, data: []TestDate{t_0}, start: timex.AlignTime(t_0.Ts, time.Second, true), end: timex.AlignTime(t_0.Ts, time.Second, true).Add(sw.size)},
	}
	// 等待一段时间，触发窗口
	//time.Sleep(3 * time.Second)

	// 检查结果
	// resultsChan := sw.OutputChan()
	// results := make(chan []model.Row)
	actual := make([]TestResult, 0)
	timeout := time.After(6 * time.Second)
	for {
		select {
		case results := <-sw.OutputChan():
			raw := make([]TestDate, 0)
			for _, row := range results {
				raw = append(raw, row.Data.(TestDate))
			}
			if len(results) == 0 {
				continue
			}
			actual = append(actual, TestResult{
				size:  len(results),
				data:  raw,
				start: *results[0].Slot.Start,
				end:   *results[0].Slot.End})
		case <-timeout:
			goto END
		default:
		}
	}

END:
	assert.Equal(t, len(actual), len(expected))
	// 预期结果：保留最近 2 秒内的数据
	for i, exp := range expected {
		assert.Equal(t, actual[i].size, exp.size)
		assert.Equal(t, actual[i].start, exp.start)
		assert.Equal(t, actual[i].end, exp.end)
		for _, d := range exp.data {
			assert.Contains(t, actual[i].data, d)
		}
	}
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
	data := map[string]interface{}{"device": "aa", "temperature": 25.0, "humidity": 60, "ts": t_0}
	t_1 := GetTimestamp(data, "ts")

	data_1 := TestDate{Ts: t_0}
	t_2 := GetTimestamp(data_1, "Ts")

	data_2 := TestDate2{ts: t_0}
	t_3 := GetTimestamp(data_2, "")

	assert.Equal(t, t_0, t_1)
	assert.Equal(t, t_0, t_2)
	assert.Equal(t, t_0, t_3)
}
