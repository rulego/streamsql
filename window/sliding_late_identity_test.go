package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lateProbeStruct struct {
	Ts int64
	V  int
}

// 迟到更新路径不得对非 map 的 Data 崩溃。
// 回归：原实现用 reflect.ValueOf(item.Data).Pointer() 判重，该调用对 struct/int
// 直接 panic（仅 string/map/指针类安全）。Window.Add 收 any，aggregator 也支持
// struct，故这条路径能被真实调用到。
func TestSlidingWindow_LateUpdateNonMapData(t *testing.T) {
	cases := []struct {
		name string
		data []any
	}{
		{"结构体", []any{lateProbeStruct{Ts: 1, V: 1}, lateProbeStruct{Ts: 2, V: 2}}},
		{"整数", []any{1, 2}},
		{"字符串", []any{"a", "b"}},
		{"map", []any{map[string]any{"v": 1}, map[string]any{"v": 2}}},
		{"混合类型", []any{lateProbeStruct{Ts: 1, V: 1}, 42, "x", map[string]any{"v": 2}}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			sw, err := NewSlidingWindow(types.WindowConfig{
				Params:             []any{time.Second, 500 * time.Millisecond},
				TimeCharacteristic: types.EventTime,
				AllowedLateness:    2 * time.Second,
			})
			require.NoError(t, err)

			start := time.Now().Add(-10 * time.Second)
			end := start.Add(time.Second)
			slot := types.NewTimeSlot(&start, &end)
			ts := start.Add(100 * time.Millisecond)

			// 已触发窗口的 snapshot 里放首条，sw.data 里放其余（模拟迟到行）
			sw.triggeredWindows[sw.getWindowKey(*slot.End)] = &triggeredWindowInfo{
				slot:         slot,
				closeTime:    end.Add(2 * time.Second),
				snapshotData: []types.Row{{Data: c.data[0], Timestamp: ts, Slot: slot}},
				snapshotSeq:  map[int64]struct{}{0: {}},
			}
			sw.data = nil
			sw.dataSeq = nil
			for i, d := range c.data {
				sw.data = append(sw.data, types.Row{Data: d, Timestamp: ts.Add(time.Duration(i) * time.Millisecond)})
				sw.dataSeq = append(sw.dataSeq, int64(i))
			}
			sw.nextSeq = int64(len(c.data))

			require.NotPanics(t, func() {
				sw.mu.Lock()
				sw.triggerLateUpdateLocked(slot)
				sw.mu.Unlock()
			}, "迟到更新路径不应 panic")
		})
	}
}

// 迟到更新不得把已并入 snapshot 的行重复计入（COUNT 翻倍）。
func TestSlidingWindow_LateUpdateNoDoubleCount(t *testing.T) {
	sw, err := NewSlidingWindow(types.WindowConfig{
		Params:             []any{time.Second, 500 * time.Millisecond},
		TimeCharacteristic: types.EventTime,
		AllowedLateness:    2 * time.Second,
	})
	require.NoError(t, err)

	start := time.Now().Add(-10 * time.Second)
	end := start.Add(time.Second)
	slot := types.NewTimeSlot(&start, &end)
	ts := start.Add(100 * time.Millisecond)

	// snapshot 已含 seq=0、seq=1 两行；sw.data 里同样有这两行 + 一条新迟到行 seq=2
	rows := []any{
		map[string]any{"v": 1},
		map[string]any{"v": 2},
		map[string]any{"v": 3},
	}
	sw.triggeredWindows[sw.getWindowKey(*slot.End)] = &triggeredWindowInfo{
		slot:      slot,
		closeTime: end.Add(2 * time.Second),
		snapshotData: []types.Row{
			{Data: rows[0], Timestamp: ts, Slot: slot},
			{Data: rows[1], Timestamp: ts.Add(time.Millisecond), Slot: slot},
		},
		snapshotSeq: map[int64]struct{}{0: {}, 1: {}},
	}
	for i, d := range rows {
		sw.data = append(sw.data, types.Row{Data: d, Timestamp: ts.Add(time.Duration(i) * time.Millisecond)})
		sw.dataSeq = append(sw.dataSeq, int64(i))
	}
	sw.nextSeq = int64(len(rows))

	sw.mu.Lock()
	sw.triggerLateUpdateLocked(slot)
	sw.mu.Unlock()

	select {
	case got := <-sw.OutputChan():
		// 应是 snapshot 2 行 + 新迟到 1 行 = 3 行，不是 5 行
		assert.Len(t, got, 3, "已并入 snapshot 的行被重复计入")
	case <-time.After(time.Second):
		t.Fatal("迟到更新未产出结果")
	}
}

// 连续多次迟到更新，每条行只应计入一次。
func TestSlidingWindow_RepeatedLateUpdatesCountOnce(t *testing.T) {
	sw, err := NewSlidingWindow(types.WindowConfig{
		Params:             []any{time.Second, 500 * time.Millisecond},
		TimeCharacteristic: types.EventTime,
		AllowedLateness:    2 * time.Second,
	})
	require.NoError(t, err)

	start := time.Now().Add(-10 * time.Second)
	end := start.Add(time.Second)
	slot := types.NewTimeSlot(&start, &end)
	ts := start.Add(100 * time.Millisecond)

	sw.triggeredWindows[sw.getWindowKey(*slot.End)] = &triggeredWindowInfo{
		slot:         slot,
		closeTime:    end.Add(2 * time.Second),
		snapshotData: []types.Row{{Data: map[string]any{"v": 0}, Timestamp: ts, Slot: slot}},
		snapshotSeq:  map[int64]struct{}{0: {}},
	}
	sw.data = []types.Row{{Data: map[string]any{"v": 0}, Timestamp: ts}}
	sw.dataSeq = []int64{0}
	sw.nextSeq = 1

	// 逐次加入迟到行并触发更新，行数应为 1,2,3 而非 1,3,6
	wantLen := 1
	for i := 1; i <= 2; i++ {
		sw.data = append(sw.data, types.Row{
			Data:      map[string]any{"v": i},
			Timestamp: ts.Add(time.Duration(i) * time.Millisecond),
		})
		sw.dataSeq = append(sw.dataSeq, int64(i))
		sw.nextSeq++
		wantLen++

		sw.mu.Lock()
		sw.triggerLateUpdateLocked(slot)
		sw.mu.Unlock()

		select {
		case got := <-sw.OutputChan():
			assert.Len(t, got, wantLen, "第 %d 次迟到更新行数不符（重复计入）", i)
		case <-time.After(time.Second):
			t.Fatalf("第 %d 次迟到更新未产出结果", i)
		}
	}
}

// 行 id 须随驱逐保持对齐：驱逐旧行后，剩余行的 id 不能错位。
func TestSlidingWindow_SeqStaysAlignedAfterEviction(t *testing.T) {
	sw, err := NewSlidingWindow(types.WindowConfig{
		Params:             []any{time.Second, 500 * time.Millisecond},
		TimeCharacteristic: types.ProcessingTime,
	})
	require.NoError(t, err)

	base := time.Now()
	for i := 0; i < 5; i++ {
		sw.data = append(sw.data, types.Row{
			Data:      map[string]any{"i": i},
			Timestamp: base.Add(time.Duration(i) * 300 * time.Millisecond),
		})
		sw.dataSeq = append(sw.dataSeq, int64(i))
	}
	sw.nextSeq = 5

	// 触发首个窗口，驱逐早于下一窗口起点的行
	slotStart := base
	slotEnd := base.Add(time.Second)
	slot := types.NewTimeSlot(&slotStart, &slotEnd)
	sw.mu.Lock()
	sw.extractWindowDataLocked(slot)
	sw.mu.Unlock()

	require.Equal(t, len(sw.data), len(sw.dataSeq), "data 与 dataSeq 长度须一致")
	// 剩余行的 id 应与其内容对应
	for i, row := range sw.data {
		m := row.Data.(map[string]any)
		assert.EqualValues(t, m["i"], sw.dataSeq[i], "第 %d 行 id 与内容错位", i)
	}
}
