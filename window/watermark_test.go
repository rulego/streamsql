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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWatermarkAndBasics(t *testing.T) {
	wm := NewWatermark(time.Second, 50*time.Millisecond, 0)
	defer wm.Stop()

	require.False(t, wm.GetCurrentWatermark().IsZero() == false) // zero at start
	assert.True(t, wm.GetCurrentWatermark().IsZero())

	// watermark not yet set, nothing is late
	assert.False(t, wm.IsEventTimeLate(time.Now()))

	out := wm.WatermarkChan()
	assert.NotNil(t, out)
}

func TestWatermarkUpdateEventTime(t *testing.T) {
	wm := NewWatermark(time.Second, 50*time.Millisecond, 0)
	defer wm.Stop()

	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	wm.UpdateEventTime(base)

	// watermark = base - maxOutOfOrderness
	expected := base.Add(-time.Second)
	assert.Equal(t, expected, wm.GetCurrentWatermark())

	// older event time is now late
	assert.True(t, wm.IsEventTimeLate(base.Add(-2 * time.Second)))
	// the triggering event itself is not late
	assert.False(t, wm.IsEventTimeLate(base))

	// an update must have been pushed to the channel
	select {
	case w := <-wm.WatermarkChan():
		assert.Equal(t, expected, w)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected watermark on channel")
	}
}

func TestWatermarkIgnoresOlderEventTime(t *testing.T) {
	wm := NewWatermark(time.Second, 50*time.Millisecond, 0)
	defer wm.Stop()

	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	wm.UpdateEventTime(base)
	wmBefore := wm.GetCurrentWatermark()

	// older event time must not move watermark backward
	wm.UpdateEventTime(base.Add(-time.Second))
	assert.Equal(t, wmBefore, wm.GetCurrentWatermark())
	assert.False(t, wm.IsEventTimeLate(base))
}

func TestWatermarkMonotonic(t *testing.T) {
	wm := NewWatermark(time.Second, 50*time.Millisecond, 0)
	defer wm.Stop()

	t1 := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	wm.UpdateEventTime(t1)
	first := wm.GetCurrentWatermark()

	// larger event time advances watermark
	wm.UpdateEventTime(t1.Add(5 * time.Second))
	second := wm.GetCurrentWatermark()
	assert.True(t, second.After(first))
}

func TestWatermarkChannelFullSkips(t *testing.T) {
	// channel buffer is 100; push many increasing timestamps to overflow it
	wm := NewWatermark(time.Millisecond, 50*time.Millisecond, 0)
	defer wm.Stop()

	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 200; i++ {
		wm.UpdateEventTime(base.Add(time.Duration(i) * 2 * time.Millisecond))
	}
	// watermark advanced to the latest event regardless of channel overflow
	assert.True(t, wm.GetCurrentWatermark().After(base))
}

func TestWatermarkIdleTimeout(t *testing.T) {
	// idleTimeout drives watermark by processing time once the source goes idle
	wm := NewWatermark(time.Second, 20*time.Millisecond, 80*time.Millisecond)
	defer wm.Stop()

	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	wm.UpdateEventTime(base)

	eventBased := base.Add(-time.Second)
	// after idleTimeout elapses with no new events, watermark advances past the event-based value
	require.Eventually(t, func() bool {
		return wm.GetCurrentWatermark().After(eventBased)
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestWatermarkUpdateLoopNoEvents(t *testing.T) {
	// with no events at all, update() must not move the watermark
	wm := NewWatermark(time.Second, 20*time.Millisecond, 0)
	defer wm.Stop()

	time.Sleep(80 * time.Millisecond)
	assert.True(t, wm.GetCurrentWatermark().IsZero())
}

func TestAlignWindowStart(t *testing.T) {
	tests := []struct {
		name       string
		timestamp  time.Time
		windowSize time.Duration
		expected   time.Time
	}{
		{
			name:       "aligned to boundary",
			timestamp:  time.Unix(0, 10000000000).UTC(), // 10s
			windowSize: 2 * time.Second,
			expected:   time.Unix(0, 10000000000).UTC(),
		},
		{
			name:       "rounds down to previous boundary",
			timestamp:  time.Unix(0, 10001000000).UTC(), // 10001ms
			windowSize: 2 * time.Second,
			expected:   time.Unix(0, 10000000000).UTC(), // 10000ms
		},
		{
			name:       "1s granularity",
			timestamp:  time.Unix(0, 1500000000).UTC(), // 1.5s
			windowSize: time.Second,
			expected:   time.Unix(0, 1000000000).UTC(), // 1s
		},
		{
			name:       "ns granularity within window",
			timestamp:  time.Unix(0, 2500300000).UTC(), // 2.5003s
			windowSize: 500 * time.Millisecond,
			expected:   time.Unix(0, 2500000000).UTC(), // 2.5s
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := alignWindowStart(tt.timestamp, tt.windowSize)
			assert.Equal(t, tt.expected, got)
		})
	}
}
