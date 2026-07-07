package streamsql

import (
	"sync"
	"testing"
	"time"
)

// TestTriggerWindow 验证 TriggerWindow 手动触发窗口立即输出（不等自然触发）。
// TumblingWindow('5s') ProcessingTime：Trigger 提前触发当前窗口（不等满 5s）。
// 注：CountingWindow.Trigger 为空实现（按 count 触发，设计如此），故用 TumblingWindow 验证。
func TestTriggerWindow(t *testing.T) {
	ssql := New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, COUNT(*) AS cnt FROM stream GROUP BY deviceId, TumblingWindow('5s')"); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var mu sync.Mutex
	var results []map[string]interface{}
	ssql.AddSink(func(rows []map[string]interface{}) {
		mu.Lock()
		results = append(results, rows...)
		mu.Unlock()
	})

	ssql.Emit(map[string]interface{}{"deviceId": "d1"})

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

// TestTriggerWindowNoWindow 验证非窗口（直接路径）查询调 TriggerWindow 不 panic（无窗口可触发）。
func TestTriggerWindowNoWindow(t *testing.T) {
	ssql := New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId FROM stream"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// No window in direct mode; TriggerWindow must be a safe no-op.
	ssql.TriggerWindow()
}
