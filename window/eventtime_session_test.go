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

func newEventTimeSession(t *testing.T, timeout, maxOutOfOrderness, allowedLateness time.Duration) *SessionWindow {
	t.Helper()
	sw, err := NewSessionWindow(types.WindowConfig{
		Type:               TypeSession,
		Params:             []interface{}{timeout},
		TsProp:             "ts",
		TimeCharacteristic: types.EventTime,
		MaxOutOfOrderness:  maxOutOfOrderness,
		WatermarkInterval:  20 * time.Millisecond,
		AllowedLateness:    allowedLateness,
		GroupByKeys:        []string{"user"},
	})
	require.NoError(t, err)
	return sw
}

func TestEventTimeSessionTrigger(t *testing.T) {
	sw := newEventTimeSession(t, 2*time.Second, 500*time.Millisecond, 0)
	sw.Start()
	defer sw.Stop()

	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	// session for "a": [base, base+2s)
	sw.Add(map[string]interface{}{"user": "a", "ts": base, "v": 1})
	// far-future event for "b" pushes watermark past session "a" end
	sw.Add(map[string]interface{}{"user": "b", "ts": base.Add(10 * time.Second), "v": 2})

	select {
	case res := <-sw.OutputChan():
		require.Len(t, res, 1)
		assert.Equal(t, "a", res[0].Data.(map[string]interface{})["user"])
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for session trigger")
	}
}

func TestEventTimeSessionCloseExpired(t *testing.T) {
	sw := newEventTimeSession(t, 2*time.Second, 500*time.Millisecond, 5*time.Second)
	sw.Start()
	defer sw.Stop()

	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	// session "a" expires and is kept open for late data (closeTime = base+2s+5s)
	sw.Add(map[string]interface{}{"user": "a", "ts": base, "v": 1})
	sw.Add(map[string]interface{}{"user": "b", "ts": base.Add(3 * time.Second), "v": 2})
	require.Eventually(t, func() bool {
		return sw.sentCount > 0
	}, 2*time.Second, 10*time.Millisecond)
	<-sw.OutputChan()

	// verify session "a" is held open for late data
	require.Eventually(t, func() bool {
		sw.mu.RLock()
		defer sw.mu.RUnlock()
		return len(sw.triggeredSessions) >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// push watermark well past closeTime to expire all triggered sessions
	sw.Add(map[string]interface{}{"user": "c", "ts": base.Add(30 * time.Second), "v": 3})
	require.Eventually(t, func() bool {
		sw.mu.RLock()
		defer sw.mu.RUnlock()
		return len(sw.triggeredSessions) == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSessionHandleLateDataDirect(t *testing.T) {
	// handleLateData cannot be reached via Add() without a reentrant-lock deadlock,
	// so exercise it directly with a pre-populated triggered session.
	sw := newEventTimeSession(t, 2*time.Second, 500*time.Millisecond, 5*time.Second)
	defer sw.Stop()

	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	slotStart := base
	slotEnd := base.Add(2 * time.Second)
	slot := types.NewTimeSlot(&slotStart, &slotEnd)

	existing := []types.Row{
		{Data: map[string]interface{}{"user": "a", "ts": base, "v": 1}, Timestamp: base, Slot: slot},
	}
	sw.mu.Lock()
	sw.triggeredSessions["a"] = &sessionInfo{
		session: &session{
			data:       existing,
			lastActive: base,
			slot:       slot,
		},
		closeTime: slotEnd.Add(5 * time.Second),
	}
	sw.mu.Unlock()

	// late event inside the triggered session range
	sw.handleLateData(base.Add(1*time.Second), 5*time.Second)

	select {
	case res := <-sw.OutputChan():
		// session late update re-emits the session's existing data
		require.Len(t, res, 1)
		assert.Equal(t, "a", res[0].Data.(map[string]interface{})["user"])
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for session late update")
	}
}

func TestSessionResetStats(t *testing.T) {
	sw := newEventTimeSession(t, 2*time.Second, 500*time.Millisecond, 0)
	defer sw.Stop()

	sw.ResetStats()
	stats := sw.GetStats()
	assert.Equal(t, int64(0), stats["sentCount"])
	assert.Equal(t, int64(0), stats["droppedCount"])
}
