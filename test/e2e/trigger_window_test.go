package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
)

// TestTriggerWindow verifies TriggerWindow. Manually triggered window outputs immediately (not necessarily triggered naturally).
// TumblingWindow('5s') ProcessingTime: Trigger Triggers the current window early (not until 5 seconds).
// Note: CountingWindow.Trigger is an empty implementation (triggered by count, designed accordingly), so TumblingWindow is used for verification.
func TestTriggerWindow(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, COUNT(*) AS cnt FROM stream GROUP BY deviceId, TumblingWindow('5s')"); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var mu sync.Mutex
	var results []map[string]any
	ssql.AddSink(func(rows []map[string]any) {
		mu.Lock()
		results = append(results, rows...)
		mu.Unlock()
	})

	ssql.Emit(map[string]any{"deviceId": "d1"})

	// Let the row enter the window (TumblingWindow initializes on first Add).
	time.Sleep(200 * time.Millisecond)

	ssql.TriggerWindow() // force the current window to emit without waiting 5s

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(results)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(results) == 0 {
		t.Fatal("TriggerWindow should emit the current tumbling window immediately")
	}
}

// TestTriggerWindowNoWindow verifies a non-window (direct path) query calls TriggerWindow not panic (no window can be triggered).
func TestTriggerWindowNoWindow(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId FROM stream"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// No window in direct mode; TriggerWindow must be a safe no-op.
	ssql.TriggerWindow()
}
