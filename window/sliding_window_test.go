package window

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
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

	sw, _ := NewSlidingWindow(types.WindowConfig{
		Params:   []interface{}{2 * time.Second, time.Second},
		TsProp:   "Ts",
		TimeUnit: time.Second,
	})
	sw.SetCallback(func(results []types.Row) {
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
	// 移除对齐后，窗口从第一个数据的时间开始
	// 窗口大小2秒，滑动步长1秒
	// 第一个窗口: [t_3, t_3 + 2秒) = [16:46:56.789, 16:46:58.789)
	// 第二个窗口: [t_3 + 1秒, t_3 + 3秒) = [16:46:57.789, 16:46:59.789)
	// 第三个窗口: [t_3 + 2秒, t_3 + 4秒) = [16:46:58.789, 16:47:00.789)
	// 第四个窗口: [t_3 + 3秒, t_3 + 5秒) = [16:46:59.789, 16:47:01.789)
	expected := []TestResult{
		{size: 2, data: []TestDate{t_3, t_2}, start: t_3.Ts, end: t_3.Ts.Add(sw.size)},
		{size: 2, data: []TestDate{t_2, t_1}, start: t_3.Ts.Add(sw.slide), end: t_3.Ts.Add(sw.slide).Add(sw.size)},
		{size: 2, data: []TestDate{t_1, t_0}, start: t_3.Ts.Add(2 * sw.slide), end: t_3.Ts.Add(2 * sw.slide).Add(sw.size)},
		{size: 1, data: []TestDate{t_0}, start: t_3.Ts.Add(3 * sw.slide), end: t_3.Ts.Add(3 * sw.slide).Add(sw.size)},
	}
	// 等待一段时间，触发窗口
	//time.Sleep(3 * time.Second)

	// 检查结果
	// resultsChan := sw.OutputChan()
	// results := make(chan []types.Row)
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
		assert.Equal(t, actual[i].size, exp.size, "窗口 %d 的数据量应该匹配", i+1)
		// 移除对齐后，窗口时间应该精确匹配（允许微小的纳秒级误差）
		assert.WithinDuration(t, exp.start, actual[i].start, 100*time.Millisecond,
			"窗口 %d 的开始时间应该匹配，预期: %v, 实际: %v", i+1, exp.start, actual[i].start)
		assert.WithinDuration(t, exp.end, actual[i].end, 100*time.Millisecond,
			"窗口 %d 的结束时间应该匹配，预期: %v, 实际: %v", i+1, exp.end, actual[i].end)
		for _, d := range exp.data {
			assert.Contains(t, actual[i].data, d, "窗口 %d 应该包含数据 %v", i+1, d)
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
	t_1 := GetTimestamp(data, "ts", time.Millisecond)

	data_1 := TestDate{Ts: t_0}
	t_2 := GetTimestamp(data_1, "Ts", time.Millisecond)

	data_2 := TestDate2{ts: t_0}
	t_3 := GetTimestamp(data_2, "", time.Millisecond)

	assert.Equal(t, t_0, t_1)
	assert.Equal(t, t_0, t_2)
	assert.Equal(t, t_0, t_3)
}
