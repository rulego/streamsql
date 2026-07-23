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
		Params:   []any{2 * time.Second, time.Second},
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

	// Add data
	t_3 := TestDate{Ts: time.Date(2025, 4, 7, 16, 46, 56, 789000000, time.UTC), tag: "1"}
	t_2 := TestDate{Ts: time.Date(2025, 4, 7, 16, 46, 57, 789000000, time.UTC), tag: "2"}
	t_1 := TestDate{Ts: time.Date(2025, 4, 7, 16, 46, 58, 789000000, time.UTC), tag: "3"}
	t_0 := TestDate{Ts: time.Date(2025, 4, 7, 16, 46, 59, 789000000, time.UTC), tag: "4"}

	sw.Add(t_3)
	sw.Add(t_2)
	sw.Add(t_1)
	sw.Add(t_0)

	// Verify data for each window
	// Processing time window aligned to epoch(slide boundary):t_3=16:46:56.789 Aligned to 16:46:56.000
	// Window size is 2 seconds, sliding step length is 1 second
	// First window: [16:46:56.000, 16:46:58.000)
	// Second window: [16:46:57.000, 16:46:59.000)
	// Third window: [16:46:58.000, 16:47:00.000)
	// Window 4: [16:46:59.000, 16:47:01.000)
	alignedStart := alignWindowStart(t_3.Ts, sw.slide)
	expected := []TestResult{
		{size: 2, data: []TestDate{t_3, t_2}, start: alignedStart, end: alignedStart.Add(sw.size)},
		{size: 2, data: []TestDate{t_2, t_1}, start: alignedStart.Add(sw.slide), end: alignedStart.Add(sw.slide).Add(sw.size)},
		{size: 2, data: []TestDate{t_1, t_0}, start: alignedStart.Add(2 * sw.slide), end: alignedStart.Add(2 * sw.slide).Add(sw.size)},
		{size: 1, data: []TestDate{t_0}, start: alignedStart.Add(3 * sw.slide), end: alignedStart.Add(3 * sw.slide).Add(sw.size)},
	}
	// Wait a while, then the window will be triggered
	//time.Sleep(3 * time.Second)

	// Inspection results
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
	// Expected outcome: Data from the last 2 seconds is retained
	for i, exp := range expected {
		assert.Equal(t, actual[i].size, exp.size, "窗口 %d 的数据量应该匹配", i+1)
		// After removing alignment, window times should match precisely (allowing for tiny nanosecond-level errors)
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
	data := map[string]any{"device": "aa", "temperature": 25.0, "humidity": 60, "ts": t_0}
	t_1 := GetTimestamp(data, "ts", time.Millisecond)

	data_1 := TestDate{Ts: t_0}
	t_2 := GetTimestamp(data_1, "Ts", time.Millisecond)

	data_2 := TestDate2{ts: t_0}
	t_3 := GetTimestamp(data_2, "", time.Millisecond)

	assert.Equal(t, t_0, t_1)
	assert.Equal(t, t_0, t_2)
	assert.Equal(t, t_0, t_3)
}
