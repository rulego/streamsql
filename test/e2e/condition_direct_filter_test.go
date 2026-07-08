package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
)

// TestDirectPathFilterUnchanged is a regression test pinning the WHERE filter
// behavior on non-aggregation, non-JOIN (direct) queries. The filter call was
// relocated from the Process loop / ProcessSync into processDirectData /
// processDirectDataSync (to let JOIN queries filter on enriched columns). For
// plain transform queries the relocation must be a no-op: the filter still sees
// the raw stream data and yields identical pass/drop outcomes. This test locks
// that down across the sync and async paths.
func TestDirectPathFilterUnchanged(t *testing.T) {
	// Sync path (EmitSync): matching row projected, non-matching row nil.
	t.Run("sync match and drop", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		if err := ssql.Execute("SELECT deviceId, temperature FROM stream WHERE temperature > 30"); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		got, err := ssql.EmitSync(map[string]interface{}{"deviceId": "d1", "temperature": 35})
		if err != nil {
			t.Fatalf("EmitSync: %v", err)
		}
		if got == nil || got["deviceId"] != "d1" || got["temperature"] != 35 {
			t.Errorf("matching row got=%v, want {deviceId:d1 temperature:35}", got)
		}
		got, _ = ssql.EmitSync(map[string]interface{}{"deviceId": "d2", "temperature": 20})
		if got != nil {
			t.Errorf("non-matching row should be dropped (nil), got=%v", got)
		}
	})

	// Async path (Emit + AddSink): only matching rows reach the sink, in order.
	t.Run("async only matching sinked", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		if err := ssql.Execute("SELECT deviceId FROM stream WHERE temperature > 30"); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		var mu sync.Mutex
		var ids []string
		ssql.AddSink(func(rows []map[string]interface{}) {
			mu.Lock()
			for _, r := range rows {
				if v, ok := r["deviceId"].(string); ok {
					ids = append(ids, v)
				}
			}
			mu.Unlock()
		})
		ssql.Emit(map[string]interface{}{"deviceId": "d1", "temperature": 35}) // match
		ssql.Emit(map[string]interface{}{"deviceId": "d2", "temperature": 20}) // dropped
		ssql.Emit(map[string]interface{}{"deviceId": "d3", "temperature": 40}) // match

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			n := len(ids)
			mu.Unlock()
			if n >= 2 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		mu.Lock()
		defer mu.Unlock()
		// Order is not guaranteed (sink worker pool fans out); assert the SET.
		// d2 must be filtered out, d1 and d3 must pass.
		gotSet := map[string]bool{}
		for _, id := range ids {
			gotSet[id] = true
		}
		if len(ids) != 2 || !gotSet["d1"] || !gotSet["d3"] || gotSet["d2"] {
			t.Errorf("sinked ids=%v, want set {d1 d3} (d2 filtered)", ids)
		}
	})

	// No WHERE: every row passes (filter is nil -> no drop).
	t.Run("no where passes all", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		if err := ssql.Execute("SELECT deviceId FROM stream"); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		for _, id := range []string{"a", "b", "c"} {
			got, _ := ssql.EmitSync(map[string]interface{}{"deviceId": id})
			if got == nil || got["deviceId"] != id {
				t.Errorf("no-WHERE row got=%v, want deviceId=%s", got, id)
			}
		}
	})

	// Compound WHERE (AND/OR with parentheses): precedence honored.
	t.Run("compound where", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		if err := ssql.Execute("SELECT deviceId FROM stream WHERE (temperature > 30 AND humidity < 80) OR deviceId = 'd9'"); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		pass := map[string]interface{}{"deviceId": "d1", "temperature": 35, "humidity": 60}   // both AND branches true
		passOr := map[string]interface{}{"deviceId": "d9", "temperature": 10, "humidity": 99} // OR branch true
		drop := map[string]interface{}{"deviceId": "d2", "temperature": 20, "humidity": 90}   // neither

		for _, row := range []map[string]interface{}{pass, passOr, drop} {
			got, _ := ssql.EmitSync(row)
			if row["deviceId"] == "d2" {
				if got != nil {
					t.Errorf("row %v should be dropped, got=%v", row, got)
				}
			} else {
				if got == nil {
					t.Errorf("row %v should pass, got nil", row)
				}
			}
		}
	})
}
