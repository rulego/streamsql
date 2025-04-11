package window

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rulego/streamsql/model"
	"github.com/stretchr/testify/require"
)

func TestTumblingWindow(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	tw, _ := NewTumblingWindow(model.WindowConfig{
		Type:   "TumblingWindow",
		Params: map[string]interface{}{"size": "2s"},
		TsProp: "Ts",
	})
	tw.SetCallback(func(results []model.Row) {
		// Process results
	})
	go tw.Start()

	// Add data every 500ms
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)
	// 添加测试数据
	for i := 0; i < 5; i++ {
		data := TestDate{
			Ts:  baseTime.Add(time.Duration(i) * 1100 * time.Millisecond),
			tag: fmt.Sprintf("%d", i),
		}
		tw.Add(data)
	}

	// 收集窗口结果
	resultsChan := tw.OutputChan()
	var all [][]model.Row = make([][]model.Row, 0)

	// 收集所有窗口数据
COLLECT:
	for {
		select {
		case results := <-resultsChan:
			all = append(all, results)
			if len(all) >= 3 {
				break COLLECT
			}
		default:
		}
	}

	// 验证窗口数据
	require.Len(t, all, 3, "应该有3个时间窗口的数据")

	// 验证每个窗口的数据
	expectedWindows := []struct {
		size     int
		tags     []string
		startIdx int
	}{
		{size: 2, tags: []string{"0", "1"}, startIdx: 0},
		{size: 2, tags: []string{"2", "3"}, startIdx: 1},
		{size: 1, tags: []string{"4"}, startIdx: 2},
	}

	for i, window := range all {
		expected := expectedWindows[i]
		require.Len(t, window, expected.size, "窗口 %d 数据数量不匹配", i)

		// 验证数据内容
		for _, row := range window {
			require.Contains(t, expected.tags, row.Data.(TestDate).tag)
		}

		// 验证时间槽
		startTime := baseTime.Add(time.Duration(i*2) * time.Second)
		endTime := startTime.Add(2 * time.Second)
		require.True(t, window[0].Slot.Start.Equal(startTime) &&
			window[0].Slot.End.Equal(endTime),
			"窗口 %d 时间槽边界不正确", i)
	}

	// Verify reset and final batch
	tw.Reset()
	tw.Add(TestDate{
		Ts:  baseTime.Add(time.Duration(99) * 1100 * time.Millisecond),
		tag: fmt.Sprintf("%d", 99),
	})
	// time.Sleep(1100 * time.Millisecond)
	cancel()

	select {
	case results := <-resultsChan:
		require.Len(t, results, 1)
		require.Equal(t, "99", results[0].Data.(TestDate).tag)
		startTime := baseTime.Add(108 * time.Second)
		endTime := baseTime.Add(110 * time.Second)
		require.True(t, results[0].Slot.Start.Equal(startTime) && results[0].Slot.End.Equal(endTime))
	}
}
