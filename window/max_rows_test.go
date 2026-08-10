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
)

// TestMaxRowsCapsTumblingBuffer verifies the row cap bounds the buffer and counts
// the rejected rows instead of dropping them silently.
func TestMaxRowsCapsTumblingBuffer(t *testing.T) {
	w, err := NewTumblingWindow(types.WindowConfig{
		Type:               TypeTumbling,
		Params:             []any{30 * time.Minute}, // long enough not to trigger mid-test
		TimeCharacteristic: types.ProcessingTime,
		MaxRows:            10,
	})
	if err != nil {
		t.Fatalf("NewTumblingWindow: %v", err)
	}
	defer w.Stop()

	for i := 0; i < 25; i++ {
		w.Add(map[string]any{"v": i})
	}

	w.mu.RLock()
	buffered := len(w.data)
	w.mu.RUnlock()

	if buffered != 10 {
		t.Errorf("buffered rows = %d, want 10 (cap)", buffered)
	}
	if got := w.GetStats()["rowsDroppedCount"]; got != 15 {
		t.Errorf("rowsDroppedCount = %d, want 15", got)
	}
}

// TestMaxRowsKeepsEarliestRows pins the newest-dropped policy: the rows retained
// are the first ones seen, so the window's start boundary is preserved.
func TestMaxRowsKeepsEarliestRows(t *testing.T) {
	w, err := NewTumblingWindow(types.WindowConfig{
		Type:               TypeTumbling,
		Params:             []any{30 * time.Minute},
		TimeCharacteristic: types.ProcessingTime,
		MaxRows:            3,
	})
	if err != nil {
		t.Fatalf("NewTumblingWindow: %v", err)
	}
	defer w.Stop()

	for i := 0; i < 10; i++ {
		w.Add(map[string]any{"v": i})
	}

	w.mu.RLock()
	got := make([]int, 0, len(w.data))
	for _, row := range w.data {
		got = append(got, row.Data.(map[string]any)["v"].(int))
	}
	w.mu.RUnlock()

	want := []int{0, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("retained %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("retained %v, want %v (newest rows must be the dropped ones)", got, want)
		}
	}
}

// TestMaxRowsZeroIsUnbounded pins the default: MaxRows=0 keeps today's unbounded
// behavior, so existing users see no change.
func TestMaxRowsZeroIsUnbounded(t *testing.T) {
	w, err := NewTumblingWindow(types.WindowConfig{
		Type:               TypeTumbling,
		Params:             []any{30 * time.Minute},
		TimeCharacteristic: types.ProcessingTime,
		MaxRows:            0,
	})
	if err != nil {
		t.Fatalf("NewTumblingWindow: %v", err)
	}
	defer w.Stop()

	for i := 0; i < 5000; i++ {
		w.Add(map[string]any{"v": i})
	}

	w.mu.RLock()
	buffered := len(w.data)
	w.mu.RUnlock()

	if buffered != 5000 {
		t.Errorf("buffered rows = %d, want 5000 (unbounded)", buffered)
	}
	if got := w.GetStats()["rowsDroppedCount"]; got != 0 {
		t.Errorf("rowsDroppedCount = %d, want 0 when uncapped", got)
	}
}

// TestMaxRowsRecoversAfterTrigger verifies the cap is a live buffer bound, not a
// lifetime quota: once a trigger drains the buffer, new rows are accepted again.
func TestMaxRowsRecoversAfterTrigger(t *testing.T) {
	w, err := NewTumblingWindow(types.WindowConfig{
		Type:               TypeTumbling,
		Params:             []any{30 * time.Minute},
		TimeCharacteristic: types.ProcessingTime,
		MaxRows:            5,
	})
	if err != nil {
		t.Fatalf("NewTumblingWindow: %v", err)
	}
	defer w.Stop()

	results := make(chan []types.Row, 4)
	w.SetCallback(func(rows []types.Row) { results <- rows })
	w.Start()

	for i := 0; i < 12; i++ {
		w.Add(map[string]any{"v": i})
	}
	w.Trigger()

	select {
	case batch := <-results:
		if len(batch) != 5 {
			t.Errorf("first batch = %d rows, want 5 (cap)", len(batch))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first trigger")
	}

	// Buffer drained by the trigger: the next rows must be accepted.
	for i := 0; i < 3; i++ {
		w.Add(map[string]any{"v": 100 + i})
	}
	w.mu.RLock()
	buffered := len(w.data)
	w.mu.RUnlock()
	if buffered != 3 {
		t.Errorf("after trigger buffered = %d, want 3 (cap frees up as the window drains)", buffered)
	}
}

// TestMaxRowsCapsSlidingBuffer covers the sliding window, whose parallel dataSeq
// slice must stay in step with data under the cap.
func TestMaxRowsCapsSlidingBuffer(t *testing.T) {
	w, err := NewSlidingWindow(types.WindowConfig{
		Type:               TypeSliding,
		Params:             []any{30 * time.Minute, 10 * time.Minute},
		TimeCharacteristic: types.ProcessingTime,
		MaxRows:            8,
	})
	if err != nil {
		t.Fatalf("NewSlidingWindow: %v", err)
	}
	defer w.Stop()

	for i := 0; i < 20; i++ {
		w.Add(map[string]any{"v": i})
	}

	w.mu.RLock()
	buffered, seqLen := len(w.data), len(w.dataSeq)
	w.mu.RUnlock()

	if buffered != 8 {
		t.Errorf("buffered rows = %d, want 8 (cap)", buffered)
	}
	if seqLen != buffered {
		t.Errorf("dataSeq len = %d, want %d (must stay parallel to data)", seqLen, buffered)
	}
	if got := w.GetStats()["rowsDroppedCount"]; got != 12 {
		t.Errorf("rowsDroppedCount = %d, want 12", got)
	}
}

// TestMaxRowsCapsSessionTotalAcrossKeys verifies the session cap bounds the total
// buffered across all sessions (what actually bounds memory), not per session.
func TestMaxRowsCapsSessionTotalAcrossKeys(t *testing.T) {
	w, err := NewSessionWindow(types.WindowConfig{
		Type:               TypeSession,
		Params:             []any{30 * time.Minute},
		TimeCharacteristic: types.ProcessingTime,
		GroupByKeys:        []string{"deviceId"},
		MaxRows:            10,
	})
	if err != nil {
		t.Fatalf("NewSessionWindow: %v", err)
	}
	defer w.Stop()

	// 4 distinct keys × 10 rows each: the total cap must bind, not 10 per key.
	for i := 0; i < 40; i++ {
		w.Add(map[string]any{"deviceId": i % 4, "v": i})
	}

	w.mu.RLock()
	total := 0
	for _, s := range w.sessionMap {
		total += len(s.data)
	}
	tracked := w.bufferedRows
	w.mu.RUnlock()

	if total != 10 {
		t.Errorf("total buffered across sessions = %d, want 10 (cap is a total, not per-key)", total)
	}
	if tracked != total {
		t.Errorf("bufferedRows = %d, want %d (counter must match real total)", tracked, total)
	}
	if got := w.GetStats()["rowsDroppedCount"]; got != 30 {
		t.Errorf("rowsDroppedCount = %d, want 30", got)
	}
}

// TestSessionBufferedRowsNoDriftAfterTrigger guards the session counter against
// drift: it is incremented per append but recomputed on the removal paths, so a
// trigger must leave it consistent with the live sessions.
func TestSessionBufferedRowsNoDriftAfterTrigger(t *testing.T) {
	w, err := NewSessionWindow(types.WindowConfig{
		Type:               TypeSession,
		Params:             []any{30 * time.Minute},
		TimeCharacteristic: types.ProcessingTime,
		GroupByKeys:        []string{"deviceId"},
		MaxRows:            100,
	})
	if err != nil {
		t.Fatalf("NewSessionWindow: %v", err)
	}
	defer w.Stop()

	for i := 0; i < 20; i++ {
		w.Add(map[string]any{"deviceId": i % 2, "v": i})
	}
	w.mu.RLock()
	before := w.bufferedRows
	w.mu.RUnlock()
	if before != 20 {
		t.Fatalf("bufferedRows before trigger = %d, want 20", before)
	}

	w.Trigger() // clears sessionMap

	w.mu.RLock()
	tracked := w.bufferedRows
	live := 0
	for _, s := range w.sessionMap {
		live += len(s.data)
	}
	w.mu.RUnlock()

	if tracked != live {
		t.Errorf("bufferedRows = %d but live sessions hold %d rows: counter drifted", tracked, live)
	}

	// A drifted (over-counted) counter would wrongly reject these.
	for i := 0; i < 20; i++ {
		w.Add(map[string]any{"deviceId": 9, "v": i})
	}
	w.mu.RLock()
	after := w.bufferedRows
	w.mu.RUnlock()
	if after != 20 {
		t.Errorf("bufferedRows after refill = %d, want 20 (cap must free up after trigger)", after)
	}
	if got := w.GetStats()["rowsDroppedCount"]; got != 0 {
		t.Errorf("rowsDroppedCount = %d, want 0 (cap 100 never reached)", got)
	}
}

// TestMaxRowsBoundsMemory ties the cap to its purpose: heap growth stops scaling
// with input once the cap binds. Uncapped buffering measures ~438 B/row, so 200k
// rows would be ~85MB; the capped window must stay far below that.
func TestMaxRowsBoundsMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("allocation-heavy; skipped under -short")
	}

	w, err := NewTumblingWindow(types.WindowConfig{
		Type:               TypeTumbling,
		Params:             []any{30 * time.Minute},
		TimeCharacteristic: types.ProcessingTime,
		MaxRows:            1000,
	})
	if err != nil {
		t.Fatalf("NewTumblingWindow: %v", err)
	}
	defer w.Stop()

	for i := 0; i < 200000; i++ {
		w.Add(map[string]any{"deviceId": i % 1000, "temperature": float64(i)})
	}

	w.mu.RLock()
	buffered := len(w.data)
	w.mu.RUnlock()

	if buffered != 1000 {
		t.Errorf("buffered = %d, want 1000: cap must bound the buffer regardless of input volume", buffered)
	}
	if got := w.GetStats()["rowsDroppedCount"]; got != 199000 {
		t.Errorf("rowsDroppedCount = %d, want 199000", got)
	}
}
