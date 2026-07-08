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
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/types"
)

func buildTestGlobalWindow(t *testing.T, trigger string, ttl time.Duration) *GlobalWindow {
	t.Helper()
	cfg := types.WindowConfig{
		Type:        TypeGlobal,
		GroupByKeys: []string{"deviceId"},
		SelectFields: map[string]aggregator.AggregateType{
			"cnt": aggregator.Count,
		},
		FieldAlias: map[string]string{
			"cnt": "*",
		},
		TriggerCondition: trigger,
		CountStateTTL:    ttl,
	}
	gw, err := NewGlobalWindow(cfg)
	if err != nil {
		t.Fatalf("NewGlobalWindow: %v", err)
	}
	return gw
}

// collectResults drains the output channel synchronously after each emit by
// installing a callback; the window pushes one batch per fire.
func (gw *GlobalWindow) collectOnCallback() *[]map[string]interface{} {
	var mu sync.Mutex
	var got []map[string]interface{}
	gw.SetCallback(func(rows []types.Row) {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range rows {
			if m, ok := r.Data.(map[string]interface{}); ok {
				got = append(got, m)
			}
		}
	})
	return &got
}

func TestGlobalWindow_FiresWhenPredicateHits(t *testing.T) {
	gw := buildTestGlobalWindow(t, "COUNT(*) >= 3", 0)
	got := gw.collectOnCallback()
	gw.Start()
	defer gw.Stop()

	for i := 0; i < 3; i++ {
		gw.Add(map[string]interface{}{"deviceId": "d1", "temp": float64(i)})
	}
	// callback fires synchronously inside processRow (same goroutine as Add's
	// triggerChan consumer), so by the time the channelized Add returns the row
	// may still be queued. Give the consumer a moment.
	waitFor(t, func() bool {
		return len(*got) > 0
	})
	if len(*got) != 1 {
		t.Fatalf("expected 1 fire, got %d", len(*got))
	}
	if c, _ := (*got)[0]["cnt"].(float64); c != 3 {
		t.Errorf("cnt = %v, want 3 (FIRE_AND_PURGE at >=3)", (*got)[0]["cnt"])
	}
}

func TestGlobalWindow_DoesNotFireBelowThreshold(t *testing.T) {
	gw := buildTestGlobalWindow(t, "COUNT(*) >= 5", 0)
	got := gw.collectOnCallback()
	gw.Start()
	defer gw.Stop()

	for i := 0; i < 4; i++ {
		gw.Add(map[string]interface{}{"deviceId": "d1"})
	}
	time.Sleep(100 * time.Millisecond)
	if len(*got) != 0 {
		t.Fatalf("expected no fire below threshold, got %d results", len(*got))
	}
}

func TestGlobalWindow_PurgesStateAfterFire(t *testing.T) {
	// With FIRE_AND_PURGE, count resets after firing: 3 rows fire (cnt=3), then 3
	// more fire again (cnt=3, not 6).
	gw := buildTestGlobalWindow(t, "COUNT(*) >= 3", 0)
	got := gw.collectOnCallback()
	gw.Start()
	defer gw.Stop()

	for i := 0; i < 6; i++ {
		gw.Add(map[string]interface{}{"deviceId": "d1"})
	}
	waitFor(t, func() bool {
		return len(*got) >= 2
	})
	if len(*got) != 2 {
		t.Fatalf("expected 2 fires (3+3 after purge), got %d", len(*got))
	}
	for i, r := range *got {
		if c, _ := r["cnt"].(float64); c != 3 {
			t.Errorf("fire %d cnt = %v, want 3 (state should purge between fires)", i, r["cnt"])
		}
	}
}

func TestGlobalWindow_GroupsFireIndependently(t *testing.T) {
	gw := buildTestGlobalWindow(t, "COUNT(*) >= 2", 0)
	got := gw.collectOnCallback()
	gw.Start()
	defer gw.Stop()

	// Interleave two devices; each should fire on its own count.
	gw.Add(map[string]interface{}{"deviceId": "a"})
	gw.Add(map[string]interface{}{"deviceId": "b"})
	gw.Add(map[string]interface{}{"deviceId": "a"}) // a fires (cnt=2)
	gw.Add(map[string]interface{}{"deviceId": "b"}) // b fires (cnt=2)

	waitFor(t, func() bool {
		return len(*got) >= 2
	})
	if len(*got) != 2 {
		t.Fatalf("expected 2 independent group fires, got %d", len(*got))
	}
	devs := map[string]bool{}
	for _, r := range *got {
		devs[r["deviceId"].(string)] = true
	}
	if !devs["a"] || !devs["b"] {
		t.Errorf("expected both groups to fire, got %v", devs)
	}
}

func TestGlobalWindow_FieldDrivenTrigger(t *testing.T) {
	// MAX(temp) > 50: trigger only when max crosses 50. Uses a field-driven
	// predicate whose aggregate is also in SELECT.
	cfg := types.WindowConfig{
		Type:        TypeGlobal,
		GroupByKeys: []string{"deviceId"},
		SelectFields: map[string]aggregator.AggregateType{
			"mx": aggregator.Max,
		},
		FieldAlias: map[string]string{
			"mx": "temp",
		},
		TriggerCondition: "MAX(temp) > 50",
	}
	gw, err := NewGlobalWindow(cfg)
	if err != nil {
		t.Fatalf("NewGlobalWindow: %v", err)
	}
	got := gw.collectOnCallback()
	gw.Start()
	defer gw.Stop()

	gw.Add(map[string]interface{}{"deviceId": "d1", "temp": float64(40)}) // no fire
	gw.Add(map[string]interface{}{"deviceId": "d1", "temp": float64(55)}) // fire, max=55
	waitFor(t, func() bool {
		return len(*got) > 0
	})
	if len(*got) != 1 {
		t.Fatalf("expected 1 fire on max>50, got %d", len(*got))
	}
	if mx, _ := (*got)[0]["mx"].(float64); mx != 55 {
		t.Errorf("mx = %v, want 55", (*got)[0]["mx"])
	}
}

func TestGlobalWindow_StateTTLReapsIdleGroup(t *testing.T) {
	// A group that never reaches threshold must be reaped once STATETTL elapses,
	// bounding memory (the core OOM mitigation for global windows). Verify the
	// reap logic directly (the ticker wiring mirrors CountingWindow).
	gw := buildTestGlobalWindow(t, "COUNT(*) >= 100", 200*time.Millisecond)
	gw.Start()
	defer gw.Stop()

	for i := 0; i < 5; i++ {
		gw.Add(map[string]interface{}{"deviceId": "idle"})
	}
	// Wait until the rows are consumed and the group exists.
	waitFor(t, func() bool {
		gw.mu.Lock()
		defer gw.mu.Unlock()
		_, exists := gw.groups["idle"]
		return exists
	})

	// Force the group's lastActive into the distant past and reap.
	gw.mu.Lock()
	if gs := gw.groups["idle"]; gs != nil {
		gs.lastActive = time.Now().Add(-1 * time.Hour)
	}
	gw.mu.Unlock()
	gw.reapIdleKeys(time.Now())

	gw.mu.Lock()
	_, exists := gw.groups["idle"]
	gw.mu.Unlock()
	if exists {
		t.Fatal("idle group should have been reaped by STATETTL")
	}

	// After reap, a new row starts a fresh group whose count is 1, not 6.
	gw.Add(map[string]interface{}{"deviceId": "idle"})
	waitFor(t, func() bool {
		gw.mu.Lock()
		defer gw.mu.Unlock()
		gs := gw.groups["idle"]
		return gs != nil
	})
	gw.mu.Lock()
	gs := gw.groups["idle"]
	cnt := 0
	if gs != nil {
		if a := gs.outputAggs["cnt"]; a != nil {
			if r, ok := a.Result().(float64); ok {
				cnt = int(r)
			}
		}
	}
	gw.mu.Unlock()
	if cnt != 1 {
		t.Errorf("after reap, new group count = %d, want 1 (state was purged by TTL)", cnt)
	}
}

func TestGlobalWindow_NeverTriggerNoOutput(t *testing.T) {
	// Predicate that never holds; combined with no STATETTL, the window must
	// still keep only O(1) state per group (verified via group count, not rows).
	gw := buildTestGlobalWindow(t, "COUNT(*) >= 1000000", 0)
	got := gw.collectOnCallback()
	gw.Start()
	defer gw.Stop()

	for i := 0; i < 1000; i++ {
		gw.Add(map[string]interface{}{"deviceId": "d1"})
	}
	time.Sleep(100 * time.Millisecond)
	if len(*got) != 0 {
		t.Fatalf("expected 0 fires for unreachable predicate, got %d", len(*got))
	}
	gw.mu.Lock()
	groupCount := len(gw.groups)
	gw.mu.Unlock()
	// All rows went to a single group; memory is one group's aggregate state,
	// not 1000 buffered rows.
	if groupCount != 1 {
		t.Errorf("expected 1 accumulated group, got %d", groupCount)
	}
}

// waitFor polls cond until it returns true or the deadline elapses.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// makeGlobalWindow is a thin helper for tests that want to vary SELECT fields.
func makeGlobalWindow(t *testing.T, selectFields map[string]aggregator.AggregateType, fieldAlias map[string]string, trigger string) (*GlobalWindow, *[]map[string]interface{}) {
	t.Helper()
	cfg := types.WindowConfig{
		Type:             TypeGlobal,
		GroupByKeys:      []string{"deviceId"},
		SelectFields:     selectFields,
		FieldAlias:       fieldAlias,
		TriggerCondition: trigger,
	}
	gw, err := NewGlobalWindow(cfg)
	if err != nil {
		t.Fatalf("NewGlobalWindow: %v", err)
	}
	got := gw.collectOnCallback()
	gw.Start()
	return gw, got
}

// TestGlobalWindow_CompoundAndTrigger covers a TRIGGER WHEN with AND between two
// aggregates: it fires only when both hold, and emits both running values.
func TestGlobalWindow_CompoundAndTrigger(t *testing.T) {
	gw, got := makeGlobalWindow(t,
		map[string]aggregator.AggregateType{"cnt": aggregator.Count, "mx": aggregator.Max},
		map[string]string{"cnt": "*", "mx": "temp"},
		"COUNT(*) >= 2 AND MAX(temp) > 50")
	defer gw.Stop()

	gw.Add(map[string]interface{}{"deviceId": "d1", "temp": float64(40)}) // cnt=1, mx=40: no fire
	gw.Add(map[string]interface{}{"deviceId": "d1", "temp": float64(55)}) // cnt=2, mx=55: fire
	waitFor(t, func() bool { return len(*got) > 0 })
	if len(*got) != 1 {
		t.Fatalf("expected 1 fire for compound AND, got %d", len(*got))
	}
	if c, _ := (*got)[0]["cnt"].(float64); c != 2 {
		t.Errorf("cnt = %v, want 2", (*got)[0]["cnt"])
	}
	if mx, _ := (*got)[0]["mx"].(float64); mx != 55 {
		t.Errorf("mx = %v, want 55", (*got)[0]["mx"])
	}
}

// TestGlobalWindow_CompoundOrTrigger covers OR: either branch crossing the
// threshold fires immediately.
func TestGlobalWindow_CompoundOrTrigger(t *testing.T) {
	gw, got := makeGlobalWindow(t,
		map[string]aggregator.AggregateType{"cnt": aggregator.Count, "mx": aggregator.Max},
		map[string]string{"cnt": "*", "mx": "temp"},
		"COUNT(*) >= 5 OR MAX(temp) > 50")
	defer gw.Stop()

	gw.Add(map[string]interface{}{"deviceId": "d1", "temp": float64(55)}) // mx>50 fires at once
	waitFor(t, func() bool { return len(*got) > 0 })
	if len(*got) != 1 {
		t.Fatalf("expected 1 fire for compound OR, got %d", len(*got))
	}
}

// TestGlobalWindow_TriggerOnlyAggregate: the trigger aggregate is not in SELECT.
// It must still drive firing, and the output carries only the SELECT fields.
func TestGlobalWindow_TriggerOnlyAggregate(t *testing.T) {
	gw, got := makeGlobalWindow(t,
		map[string]aggregator.AggregateType{"cnt": aggregator.Count},
		map[string]string{"cnt": "*"},
		"SUM(amount) >= 100")
	defer gw.Stop()

	gw.Add(map[string]interface{}{"deviceId": "d1", "amount": float64(60)}) // sum=60: no
	gw.Add(map[string]interface{}{"deviceId": "d1", "amount": float64(50)}) // sum=110: fire
	waitFor(t, func() bool { return len(*got) > 0 })
	if len(*got) != 1 {
		t.Fatalf("expected 1 fire, got %d", len(*got))
	}
	if c, _ := (*got)[0]["cnt"].(float64); c != 2 {
		t.Errorf("cnt = %v, want 2", (*got)[0]["cnt"])
	}
	if _, present := (*got)[0]["amount"]; present {
		t.Errorf("trigger-only SUM should not appear in output, got %v", (*got)[0])
	}
}

// TestGlobalWindow_EqualityTrigger: a bare = predicate (programmatic config)
// must compile and fire, same as the == form the parser emits.
func TestGlobalWindow_EqualityTrigger(t *testing.T) {
	gw, got := makeGlobalWindow(t,
		map[string]aggregator.AggregateType{"cnt": aggregator.Count},
		map[string]string{"cnt": "*"},
		"COUNT(*) = 3")
	defer gw.Stop()
	for i := 0; i < 3; i++ {
		gw.Add(map[string]interface{}{"deviceId": "d1"})
	}
	waitFor(t, func() bool { return len(*got) > 0 })
	if len(*got) != 1 {
		t.Fatalf("expected 1 fire for bare = trigger, got %d", len(*got))
	}
}

// TestNormalizeTriggerPredicate covers the SQL->expr-lang operator lowering.
func TestNormalizeTriggerPredicate(t *testing.T) {
	cases := []struct{ in, want string }{
		{"COUNT(*) >= 10", "COUNT(*) >= 10"},
		{"a AND b", "a && b"},
		{"a OR b", "a || b"},
		{"a and b or c", "a && b || c"},
		{"COUNT(*) = 3", "COUNT(*) == 3"},
		// already-expr-lang operators pass through unchanged.
		{"a && b || c", "a && b || c"},
		{"x == 3", "x == 3"},
		{"x >= 3", "x >= 3"},
		// quoted text and identifiers are preserved.
		{"state == 'AND'", "state == 'AND'"},
		{"`brand` = 1", "`brand` == 1"},
		// field names containing and/or substrings are not split.
		{"brand > 1", "brand > 1"},
		{"sender >= 2", "sender >= 2"},
	}
	for _, c := range cases {
		if got := normalizeTriggerPredicate(c.in); got != c.want {
			t.Errorf("normalizeTriggerPredicate(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
