package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// TestCloseExpiredWindowsKeepsOverlappingData (M1): when a sliding window
// overlaps (size > slide) and allowedLateness > 0, a row that belongs to an
// already-expired window must NOT be deleted by closeExpiredWindows, because
// not-yet-triggered overlapping windows still need it. Row eviction is the job
// of extractWindowDataLocked (which only drops rows older than the next
// window's start).
func TestCloseExpiredWindowsKeepsOverlappingData(t *testing.T) {
	sw, err := NewSlidingWindow(types.WindowConfig{
		Params:             []any{2 * time.Second, 500 * time.Millisecond},
		TsProp:             "Ts",
		TimeUnit:           time.Millisecond,
		TimeCharacteristic: types.EventTime,
		AllowedLateness:    200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSlidingWindow error: %v", err)
	}

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// Row at 1.5s belongs to overlapping windows A=[0,2s), B=[0.5s,2.5s),
	// C=[1s,3s), D=[1.5s,3.5s). Window A has triggered and expired; the row
	// is still needed by B/C/D.
	rowTime := base.Add(1500 * time.Millisecond)
	row := types.Row{Timestamp: rowTime, Data: map[string]any{"v": 1}}

	// Expired window A = [0, 2s), closeTime in the past.
	aStart := base
	aEnd := base.Add(2 * time.Second)
	expiredSlot := types.NewTimeSlot(&aStart, &aEnd)

	sw.mu.Lock()
	sw.data = []types.Row{row}
	sw.triggeredWindows["expired-A"] = &triggeredWindowInfo{
		slot:      expiredSlot,
		closeTime: base.Add(2 * time.Second), // expired well before watermark
	}
	sw.mu.Unlock()

	// Watermark is past A's closeTime + allowedLateness.
	watermark := base.Add(3 * time.Second)
	sw.closeExpiredWindows(watermark)

	sw.mu.Lock()
	defer sw.mu.Unlock()
	// The triggeredWindows bookkeeping entry is gone...
	if _, ok := sw.triggeredWindows["expired-A"]; ok {
		t.Error("expired window entry should be removed from triggeredWindows")
	}
	// ...but the row survives because B/C/D still need it.
	var found bool
	for _, r := range sw.data {
		if r.Timestamp.Equal(rowTime) {
			found = true
		}
	}
	if !found {
		t.Errorf("row at %v was deleted by closeExpiredWindows; overlapping windows still need it", rowTime)
	}
}
