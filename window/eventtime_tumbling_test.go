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

// etRow builds event-time data carrying a time.Time under key "ts".
func etRow(ts time.Time, v int) map[string]interface{} {
	return map[string]interface{}{"ts": ts, "v": v}
}

func newEventTimeTumbling(t *testing.T, size, maxOutOfOrderness, allowedLateness time.Duration) *TumblingWindow {
	t.Helper()
	tw, err := NewTumblingWindow(types.WindowConfig{
		Type:               TypeTumbling,
		Params:             []interface{}{size},
		TsProp:             "ts",
		TimeCharacteristic: types.EventTime,
		MaxOutOfOrderness:  maxOutOfOrderness,
		WatermarkInterval:  20 * time.Millisecond,
		AllowedLateness:    allowedLateness,
	})
	require.NoError(t, err)
	return tw
}

func TestEventTimeTumblingTrigger(t *testing.T) {
	tw := newEventTimeTumbling(t, 2*time.Second, 500*time.Millisecond, 0)
	tw.Start()
	defer tw.Stop()

	size := 2 * time.Second
	base := alignWindowStart(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), size)

	// window [base, base+2s): two events
	tw.Add(etRow(base, 1))
	tw.Add(etRow(base.Add(500*time.Millisecond), 2))
	// far-future event pushes watermark (eventTime-500ms) past window end
	tw.Add(etRow(base.Add(3*time.Second), 3))

	select {
	case res := <-tw.OutputChan():
		// window [base, base+2s) emitted its two events
		require.Len(t, res, 2)
		require.NotNil(t, res[0].Slot.Start)
		assert.Equal(t, base, *res[0].Slot.Start)
		assert.Equal(t, base.Add(size), *res[0].Slot.End)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event-time window trigger")
	}
}

func TestEventTimeTumblingEmptyWindowSkipped(t *testing.T) {
	tw := newEventTimeTumbling(t, 1*time.Second, 0, 0)
	tw.Start()
	defer tw.Stop()

	size := 1 * time.Second
	base := alignWindowStart(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), size)

	// first window has data
	tw.Add(etRow(base, 1))
	// skip several windows, then add data far ahead; empty windows are skipped silently
	tw.Add(etRow(base.Add(10*time.Second), 2))

	// expect exactly one emitted result (the first window); the far event is still pending
	select {
	case res := <-tw.OutputChan():
		require.Len(t, res, 1)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first window trigger")
	}
}

func TestEventTimeTumblingLateData(t *testing.T) {
	tw := newEventTimeTumbling(t, 2*time.Second, 500*time.Millisecond, 5*time.Second)
	tw.Start()
	defer tw.Stop()

	size := 2 * time.Second
	base := alignWindowStart(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), size)

	// window [base, base+2s): two events
	tw.Add(etRow(base, 1))
	tw.Add(etRow(base.Add(500*time.Millisecond), 2))
	// trigger the window
	tw.Add(etRow(base.Add(3*time.Second), 3))

	select {
	case res := <-tw.OutputChan():
		require.Len(t, res, 2)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial trigger")
	}

	// late data falls into the triggered window's range -> late update
	tw.Add(etRow(base.Add(250*time.Millisecond), 99))

	select {
	case res := <-tw.OutputChan():
		// late update includes original (2) + late (1)
		require.Len(t, res, 3)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for late update")
	}

	// window remains open for late data
	tw.mu.RLock()
	openWindows := len(tw.triggeredWindows)
	tw.mu.RUnlock()
	assert.Equal(t, 1, openWindows)
}

func TestEventTimeTumblingCloseExpiredWindows(t *testing.T) {
	tw := newEventTimeTumbling(t, 2*time.Second, 500*time.Millisecond, 1*time.Second)
	tw.Start()
	defer tw.Stop()

	size := 2 * time.Second
	base := alignWindowStart(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), size)

	tw.Add(etRow(base, 1))
	// trigger window [base, base+2s) and keep it open for late data
	tw.Add(etRow(base.Add(3*time.Second), 2))
	require.Eventually(t, func() bool {
		return len(tw.OutputChan()) > 0 || tw.sentCount > 0
	}, 2*time.Second, 10*time.Millisecond)
	// drain the initial trigger
	<-tw.OutputChan()

	// push watermark well past closeTime (base+2s+1s) to expire the triggered window
	tw.Add(etRow(base.Add(20*time.Second), 3))

	require.Eventually(t, func() bool {
		tw.mu.RLock()
		defer tw.mu.RUnlock()
		return len(tw.triggeredWindows) == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestEventTimeTumblingTriggerNoOp(t *testing.T) {
	// Trigger() on an event-time window is a no-op handled by watermark mechanism
	tw := newEventTimeTumbling(t, 2*time.Second, 500*time.Millisecond, 0)
	tw.Start()
	defer tw.Stop()

	tw.Add(etRow(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), 1))
	// must not panic and must not emit anything
	assert.NotPanics(t, func() { tw.Trigger() })
	select {
	case <-tw.OutputChan():
		t.Fatal("event-time Trigger should not emit")
	case <-time.After(100 * time.Millisecond):
	}
}
