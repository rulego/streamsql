package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 空闲后新数据的输出延迟不得随空闲时长增长。
// 回归：Trigger() 在窗口无数据时直接 return 不推进 currentSlot，slot 停在过期位置；
// 又因每次 tick 只前进一个 slide，恢复后需 N 次才追上 → 延迟 ≈ 空闲时长
// （实测 slide=200ms 下空闲 2s→1.4s、4s→3.6s）。
func TestSlidingWindow_IdleGapDoesNotDelayOutput(t *testing.T) {
	const (
		size  = 600 * time.Millisecond
		slide = 200 * time.Millisecond
	)
	for _, idle := range []time.Duration{600 * time.Millisecond, 2 * time.Second, 4 * time.Second} {
		idle := idle
		t.Run(idle.String(), func(t *testing.T) {
			sw, err := NewSlidingWindow(types.WindowConfig{
				Params:             []any{size, slide},
				TimeCharacteristic: types.ProcessingTime,
			})
			require.NoError(t, err)
			sw.Start()
			defer sw.Stop()

			sw.Add(map[string]any{"v": 1})
			time.Sleep(idle)
			drainRows(sw)

			sw.Add(map[string]any{"v": 2})
			start := time.Now()
			select {
			case rows := <-sw.OutputChan():
				delay := time.Since(start)
				assert.NotEmpty(t, rows)
				// 允许一个窗口 + 一次 tick 的调度余量；关键是不随 idle 增长
				assert.Less(t, delay, size+2*slide,
					"空闲 %v 后延迟 %v，应与空闲时长无关（≤ %v）", idle, delay, size+2*slide)
				t.Logf("空闲 %v → 延迟 %v", idle, delay.Round(10*time.Millisecond))
			case <-time.After(10 * time.Second):
				t.Fatalf("空闲 %v 后新数据 10s 内未输出", idle)
			}
		})
	}
}

// 对照：滚动窗口同场景（它在无数据时也推进 slot，一直是正确的）。
func TestTumblingWindow_IdleGapDoesNotDelayOutput(t *testing.T) {
	const size = 300 * time.Millisecond
	for _, idle := range []time.Duration{600 * time.Millisecond, 2 * time.Second, 4 * time.Second} {
		idle := idle
		t.Run(idle.String(), func(t *testing.T) {
			tw, err := NewTumblingWindow(types.WindowConfig{
				Params:             []any{size},
				TimeCharacteristic: types.ProcessingTime,
			})
			require.NoError(t, err)
			tw.Start()
			defer tw.Stop()

			tw.Add(map[string]any{"v": 1})
			time.Sleep(idle)
			for {
				select {
				case <-tw.OutputChan():
					continue
				default:
				}
				break
			}

			tw.Add(map[string]any{"v": 2})
			start := time.Now()
			select {
			case rows := <-tw.OutputChan():
				delay := time.Since(start)
				assert.NotEmpty(t, rows)
				assert.Less(t, delay, 3*size, "空闲 %v 后延迟 %v", idle, delay)
				t.Logf("空闲 %v → 延迟 %v", idle, delay.Round(10*time.Millisecond))
			case <-time.After(10 * time.Second):
				t.Fatalf("空闲 %v 后新数据 10s 内未输出", idle)
			}
		})
	}
}

// 空闲追赶不得丢数据：追赶后新数据必须完整出现在输出里。
func TestSlidingWindow_CatchUpKeepsNewRow(t *testing.T) {
	sw, err := NewSlidingWindow(types.WindowConfig{
		Params:             []any{600 * time.Millisecond, 200 * time.Millisecond},
		TimeCharacteristic: types.ProcessingTime,
	})
	require.NoError(t, err)
	sw.Start()
	defer sw.Stop()

	sw.Add(map[string]any{"v": "first"})
	time.Sleep(2 * time.Second)
	drainRows(sw)

	sw.Add(map[string]any{"v": "after-idle"})

	deadline := time.After(3 * time.Second)
	for {
		select {
		case rows := <-sw.OutputChan():
			for _, r := range rows {
				if m, ok := r.Data.(map[string]any); ok && m["v"] == "after-idle" {
					return // 找到了
				}
			}
		case <-deadline:
			t.Fatal("追赶后新数据未出现在输出中（被误丢）")
		}
	}
}

// 连续多次空闲-恢复循环，延迟不得累积放大。
func TestSlidingWindow_RepeatedIdleNoAccumulation(t *testing.T) {
	const (
		size  = 600 * time.Millisecond
		slide = 200 * time.Millisecond
	)
	sw, err := NewSlidingWindow(types.WindowConfig{
		Params:             []any{size, slide},
		TimeCharacteristic: types.ProcessingTime,
	})
	require.NoError(t, err)
	sw.Start()
	defer sw.Stop()

	var delays []time.Duration
	for round := 0; round < 3; round++ {
		sw.Add(map[string]any{"round": round})
		time.Sleep(1500 * time.Millisecond)
		drainRows(sw)

		sw.Add(map[string]any{"round": round, "probe": true})
		start := time.Now()
		select {
		case <-sw.OutputChan():
			delays = append(delays, time.Since(start))
		case <-time.After(5 * time.Second):
			t.Fatalf("第 %d 轮空闲后未输出", round+1)
		}
		drainRows(sw)
	}

	t.Logf("三轮空闲后的延迟: %v", delays)
	for i, d := range delays {
		assert.Less(t, d, size+2*slide, "第 %d 轮延迟 %v 超出预期（不应累积）", i+1, d)
	}
}

// drainRows 排空输出通道里已有的结果。
func drainRows(sw *SlidingWindow) {
	for {
		select {
		case <-sw.OutputChan():
			continue
		default:
		}
		return
	}
}
