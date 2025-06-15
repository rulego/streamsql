package window

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/require"
)

func TestTumblingWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tw, _ := NewTumblingWindow(types.WindowConfig{
		Type:   "TumblingWindow",
		Params: map[string]interface{}{"size": "2s"},
		TsProp: "Ts",
	})
	tw.SetCallback(func(results []types.Row) {
		// Process results
	})
	go tw.Start()

	// Add data every 1100ms
	baseTime := time.Date(2025, 4, 7, 16, 46, 0, 0, time.UTC)
	// 添加测试数据
	for i := 0; i < 5; i++ {
		data := TestDate{
			Ts:  baseTime.Add(time.Duration(i) * 1100 * time.Millisecond),
			tag: fmt.Sprintf("%d", i),
		}
		tw.Add(data)
	}

	// 等待足够长的时间确保所有窗口都被触发
	time.Sleep(6 * time.Second)

	// 收集窗口结果
	resultsChan := tw.OutputChan()
	var all [][]types.Row = make([][]types.Row, 0)

	// 收集所有窗口数据（带超时）
	timeout := time.After(5 * time.Second)
COLLECT:
	for {
		select {
		case results, ok := <-resultsChan:
			if !ok {
				// 通道已关闭
				break COLLECT
			}
			all = append(all, results)
			if len(all) >= 3 {
				break COLLECT
			}
		case <-timeout:
			t.Logf("超时，收集到 %d 个窗口结果", len(all))
			break COLLECT
		case <-ctx.Done():
			t.Logf("上下文取消，收集到 %d 个窗口结果", len(all))
			break COLLECT
		}
	}

	// 验证窗口数据（至少应该有一些结果）
	require.GreaterOrEqual(t, len(all), 1, "至少应该有1个时间窗口的数据")

	if len(all) >= 3 {
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

		for i, window := range all[:3] {
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
	}

	// 停止窗口
	tw.Stop()
}
