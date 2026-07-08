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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

func newEventTimeSliding(t *testing.T, size, slide, maxOutOfOrderness, allowedLateness time.Duration) *SlidingWindow {
	t.Helper()
	sw, err := NewSlidingWindow(types.WindowConfig{
		Type:               TypeSliding,
		Params:             []interface{}{size, slide},
		TsProp:             "ts",
		TimeCharacteristic: types.EventTime,
		MaxOutOfOrderness:  maxOutOfOrderness,
		WatermarkInterval:  20 * time.Millisecond,
		AllowedLateness:    allowedLateness,
	})
	require.NoError(t, err)
	return sw
}

func TestEventTimeSlidingTrigger(t *testing.T) {
	sw := newEventTimeSliding(t, 4*time.Second, 2*time.Second, 500*time.Millisecond, 0)
	sw.Start()
	defer sw.Stop()

	slide := 2 * time.Second
	base := alignWindowStart(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), slide)

	// window [base, base+4s): two events
	sw.Add(etRow(base, 1))
	sw.Add(etRow(base.Add(1*time.Second), 2))
	// far-future event drives watermark (event-500ms) past window end (base+4s)
	sw.Add(etRow(base.Add(5*time.Second), 3))

	select {
	case res := <-sw.OutputChan():
		require.Len(t, res, 2)
		require.NotNil(t, res[0].Slot.Start)
		assert.Equal(t, base, *res[0].Slot.Start)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sliding window trigger")
	}
}

func TestEventTimeSlidingLateData(t *testing.T) {
	sw := newEventTimeSliding(t, 4*time.Second, 2*time.Second, 500*time.Millisecond, 10*time.Second)
	sw.Start()
	defer sw.Stop()

	slide := 2 * time.Second
	base := alignWindowStart(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), slide)

	sw.Add(etRow(base, 1))
	sw.Add(etRow(base.Add(1*time.Second), 2))
	// trigger window [base, base+4s)
	sw.Add(etRow(base.Add(5*time.Second), 3))

	select {
	case <-sw.OutputChan():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial trigger")
	}

	// late data within the triggered window range
	sw.Add(etRow(base.Add(500*time.Millisecond), 99))

	select {
	case res := <-sw.OutputChan():
		// late update = original snapshot (2) + late (1)
		require.Len(t, res, 3)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sliding late update")
	}

	sw.mu.RLock()
	openWindows := len(sw.triggeredWindows)
	sw.mu.RUnlock()
	assert.Equal(t, 1, openWindows)
}

func TestEventTimeSlidingCloseExpiredWindows(t *testing.T) {
	sw := newEventTimeSliding(t, 4*time.Second, 2*time.Second, 500*time.Millisecond, 1*time.Second)
	sw.Start()
	defer sw.Stop()

	slide := 2 * time.Second
	base := alignWindowStart(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), slide)

	sw.Add(etRow(base, 1))
	sw.Add(etRow(base.Add(5*time.Second), 2))
	require.Eventually(t, func() bool {
		return sw.sentCount > 0
	}, 2*time.Second, 10*time.Millisecond)
	<-sw.OutputChan()

	// push watermark far past closeTime (base+4s+1s) to expire the triggered window
	sw.Add(etRow(base.Add(30*time.Second), 3))

	require.Eventually(t, func() bool {
		sw.mu.RLock()
		defer sw.mu.RUnlock()
		return len(sw.triggeredWindows) == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestEventTimeSlidingTriggerNoOp(t *testing.T) {
	sw := newEventTimeSliding(t, 4*time.Second, 2*time.Second, 500*time.Millisecond, 0)
	sw.Start()
	defer sw.Stop()

	sw.Add(etRow(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), 1))
	assert.NotPanics(t, func() { sw.Trigger() })
	select {
	case <-sw.OutputChan():
		t.Fatal("event-time Trigger should not emit")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSlidingGetStatsAndResetStats(t *testing.T) {
	sw := newEventTimeSliding(t, 4*time.Second, 2*time.Second, 500*time.Millisecond, 0)
	defer sw.Stop()

	stats := sw.GetStats()
	assert.Contains(t, stats, "sentCount")
	assert.Contains(t, stats, "droppedCount")
	assert.Contains(t, stats, "bufferSize")
	assert.Equal(t, int64(0), stats["sentCount"])

	sw.ResetStats()
	stats = sw.GetStats()
	assert.Equal(t, int64(0), stats["sentCount"])
	assert.Equal(t, int64(0), stats["droppedCount"])
}
