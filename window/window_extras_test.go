/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS, ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- CountingWindow: Trigger no-op + getKey branches ----

func TestCountingWindowTriggerNoOp(t *testing.T) {
	cw, err := NewCountingWindow(types.WindowConfig{Params: []any{3}})
	require.NoError(t, err)
	cw.Start()
	defer cw.Stop()
	// Trigger is a no-op kept for interface compatibility
	assert.NotPanics(t, func() { cw.Trigger() })
}

type keyedPerson struct {
	Name string
	Age  int
}

func TestCountingWindowGetKey(t *testing.T) {
	t.Run("no group keys returns global", func(t *testing.T) {
		cw, err := NewCountingWindow(types.WindowConfig{Params: []any{3}})
		require.NoError(t, err)
		defer cw.Stop()
		assert.Equal(t, "__global__", cw.getKey(map[string]any{"v": 1}))
	})

	t.Run("map data", func(t *testing.T) {
		cw, err := NewCountingWindow(types.WindowConfig{
			Params:      []any{3},
			GroupByKeys: []string{"region"},
		})
		require.NoError(t, err)
		defer cw.Stop()
		assert.Equal(t, "us", cw.getKey(map[string]any{"region": "us"}))
	})

	t.Run("struct data", func(t *testing.T) {
		cw, err := NewCountingWindow(types.WindowConfig{
			Params:      []any{3},
			GroupByKeys: []string{"Name", "Age"},
		})
		require.NoError(t, err)
		defer cw.Stop()
		assert.Equal(t, "alice|30", cw.getKey(keyedPerson{Name: "alice", Age: 30}))
	})
}

// ---- sendResult: drop-oldest and block-strategy branches ----

func fillChan(ch chan []types.Row) {
	for i := 0; i < cap(ch); i++ {
		ch <- []types.Row{{Data: i}}
	}
}

func TestTumblingSendResultDropOldest(t *testing.T) {
	tw, err := NewTumblingWindow(types.WindowConfig{
		Params:            []any{time.Second},
		PerformanceConfig: types.PerformanceConfig{BufferConfig: types.BufferConfig{WindowOutputSize: 2}},
	})
	require.NoError(t, err)
	defer tw.Stop()

	fillChan(tw.outputChan)
	// buffer full: sendResult drops the oldest entry and inserts the new one
	tw.sendResult([]types.Row{{Data: "new"}})
	stats := tw.GetStats()
	assert.GreaterOrEqual(t, stats["sentCount"], int64(1))
}

func TestTumblingSendResultBlockStrategyTimeout(t *testing.T) {
	tw, err := NewTumblingWindow(types.WindowConfig{
		Params: []any{time.Second},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig:   types.BufferConfig{WindowOutputSize: 1},
			OverflowConfig: types.OverflowConfig{Strategy: types.OverflowStrategyBlock, BlockTimeout: 80 * time.Millisecond},
		},
	})
	require.NoError(t, err)
	defer tw.Stop()

	fillChan(tw.outputChan)
	start := time.Now()
	// full buffer + block strategy -> waits until BlockTimeout then drops
	tw.sendResult([]types.Row{{Data: "new"}})
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
	stats := tw.GetStats()
	assert.Equal(t, int64(1), stats["droppedCount"])
}

func TestSlidingSendResultDropOldest(t *testing.T) {
	sw, err := NewSlidingWindow(types.WindowConfig{
		Params:            []any{2 * time.Second, time.Second},
		PerformanceConfig: types.PerformanceConfig{BufferConfig: types.BufferConfig{WindowOutputSize: 2}},
	})
	require.NoError(t, err)
	defer sw.Stop()

	fillChan(sw.outputChan)
	sw.sendResult([]types.Row{{Data: "new"}})
	stats := sw.GetStats()
	assert.GreaterOrEqual(t, stats["sentCount"], int64(1))
}

func TestSlidingSendResultBlockStrategyTimeout(t *testing.T) {
	sw, err := NewSlidingWindow(types.WindowConfig{
		Params: []any{2 * time.Second, time.Second},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig:   types.BufferConfig{WindowOutputSize: 1},
			OverflowConfig: types.OverflowConfig{Strategy: types.OverflowStrategyBlock, BlockTimeout: 80 * time.Millisecond},
		},
	})
	require.NoError(t, err)
	defer sw.Stop()

	fillChan(sw.outputChan)
	sw.sendResult([]types.Row{{Data: "new"}})
	stats := sw.GetStats()
	assert.Equal(t, int64(1), stats["droppedCount"])
}

func TestSessionSendResultDropOldest(t *testing.T) {
	sw, err := NewSessionWindow(types.WindowConfig{
		Params:            []any{time.Second},
		PerformanceConfig: types.PerformanceConfig{BufferConfig: types.BufferConfig{WindowOutputSize: 2}},
	})
	require.NoError(t, err)
	defer sw.Stop()

	fillChan(sw.outputChan)
	sw.sendResult([]types.Row{{Data: "new"}})
	stats := sw.GetStats()
	assert.GreaterOrEqual(t, stats["sentCount"], int64(1))
}

func TestSessionSendResultBlockStrategyTimeout(t *testing.T) {
	sw, err := NewSessionWindow(types.WindowConfig{
		Params: []any{time.Second},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig:   types.BufferConfig{WindowOutputSize: 1},
			OverflowConfig: types.OverflowConfig{Strategy: types.OverflowStrategyBlock, BlockTimeout: 80 * time.Millisecond},
		},
	})
	require.NoError(t, err)
	defer sw.Stop()

	fillChan(sw.outputChan)
	sw.sendResult([]types.Row{{Data: "new"}})
	stats := sw.GetStats()
	assert.Equal(t, int64(1), stats["droppedCount"])
}

func TestCountingSendResultBlockStrategy(t *testing.T) {
	cw, err := NewCountingWindow(types.WindowConfig{
		Params: []any{3},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig:   types.BufferConfig{WindowOutputSize: 1},
			OverflowConfig: types.OverflowConfig{Strategy: types.OverflowStrategyBlock, BlockTimeout: 80 * time.Millisecond, AllowDataLoss: true},
		},
	})
	require.NoError(t, err)
	defer cw.Stop()

	fillChan(cw.outputChan)
	cw.sendResult([]types.Row{{Data: "new"}})
	stats := cw.GetStats()
	assert.Equal(t, int64(1), stats["droppedCount"])
}

// ---- Reset: event-time branch (watermark recreation) ----

func TestTumblingResetEventTime(t *testing.T) {
	tw := newEventTimeTumbling(t, 2*time.Second, 500*time.Millisecond, 0)
	tw.Start()
	tw.Add(etRow(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), 1))
	tw.Stop()
	// Reset on an event-time window recreates the watermark; must not panic
	assert.NotPanics(t, func() { tw.Reset() })
}

func TestSlidingResetEventTime(t *testing.T) {
	sw := newEventTimeSliding(t, 4*time.Second, 2*time.Second, 500*time.Millisecond, 0)
	sw.Start()
	sw.Add(etRow(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), 1))
	sw.Stop()
	assert.NotPanics(t, func() { sw.Reset() })
}

func TestSessionResetEventTime(t *testing.T) {
	sw := newEventTimeSession(t, 2*time.Second, 500*time.Millisecond, 0)
	sw.Start()
	sw.Add(map[string]any{"user": "a", "ts": time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), "v": 1})
	sw.Stop()
	assert.NotPanics(t, func() { sw.Reset() })
}

func TestSessionTriggerEmpty(t *testing.T) {
	// Trigger with no sessions emits nothing and must not panic
	sw, err := NewSessionWindow(types.WindowConfig{Params: []any{time.Second}})
	require.NoError(t, err)
	sw.Start()
	defer sw.Stop()
	assert.NotPanics(t, func() { sw.Trigger() })
}
