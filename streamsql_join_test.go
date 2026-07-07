package streamsql

import (
	"sync"
	"testing"
)

// deviceMetaRows is a small metadata fixture used across JOIN tests.
func deviceMetaRows() []map[string]interface{} {
	return []map[string]interface{}{
		{"deviceId": "d1", "location": "plantA", "type": "temp"},
		{"deviceId": "d2", "location": "plantB", "type": "humid"},
	}
}

func TestJoinInnerEnrich(t *testing.T) {
	ssql := New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location, m.type FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", "deviceId", deviceMetaRows()); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	got, err := ssql.EmitSync(map[string]interface{}{"deviceId": "d1", "temp": 35})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	want := map[string]interface{}{"deviceId": "d1", "location": "plantA", "type": "temp"}
	if got["deviceId"] != want["deviceId"] || got["location"] != want["location"] || got["type"] != want["type"] {
		t.Errorf("got=%v, want=%v", got, want)
	}
}

func TestJoinInnerNoMatchDropped(t *testing.T) {
	ssql := New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", "deviceId", deviceMetaRows()); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	// d3 has no metadata row -> INNER JOIN drops it (nil result, no error).
	got, err := ssql.EmitSync(map[string]interface{}{"deviceId": "d3"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil (dropped), got %v", got)
	}
}

func TestJoinLeftNullFill(t *testing.T) {
	ssql := New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream LEFT JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", "deviceId", deviceMetaRows()); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	got, err := ssql.EmitSync(map[string]interface{}{"deviceId": "d9"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got == nil {
		t.Fatal("LEFT JOIN must keep unmatched stream row")
	}
	if got["deviceId"] != "d9" {
		t.Errorf("deviceId=%v, want d9", got["deviceId"])
	}
	if v, ok := got["location"]; ok && v != nil {
		t.Errorf("location=%v, want nil (no match)", v)
	}
}

func TestJoinCompositeKey(t *testing.T) {
	ssql := New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId AND tenant = m.tenant"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rows := []map[string]interface{}{
		{"deviceId": "d1", "tenant": "t1", "location": "plantA"},
		{"deviceId": "d1", "tenant": "t2", "location": "plantB"},
	}
	if _, err := ssql.RegisterTableKeys("meta", []string{"deviceId", "tenant"}, rows); err != nil {
		t.Fatalf("RegisterTableKeys: %v", err)
	}

	got, err := ssql.EmitSync(map[string]interface{}{"deviceId": "d1", "tenant": "t2"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got["location"] != "plantB" {
		t.Errorf("location=%v, want plantB (composite key)", got["location"])
	}
}

func TestJoinUnregisteredTableErrors(t *testing.T) {
	ssql := New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// No RegisterTable call -> EmitSync must surface the config error.
	if _, err := ssql.EmitSync(map[string]interface{}{"deviceId": "d1"}); err == nil {
		t.Error("expected error for unregistered JOIN table, got nil")
	}
}

// TestJoinConcurrentEmitAndUpsert verifies Lookup stays race-free under
// concurrent reads (Emit) and writes (Upsert). Run with -race in CI.
func TestJoinConcurrentEmitAndUpsert(t *testing.T) {
	ssql := New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	src, err := ssql.RegisterTable("meta", "deviceId", deviceMetaRows())
	if err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = ssql.EmitSync(map[string]interface{}{"deviceId": "d1"})
		}()
		go func() {
			defer wg.Done()
			src.Upsert(map[string]interface{}{"deviceId": "d1", "location": "plantC"})
		}()
	}
	wg.Wait()

	got, err := ssql.EmitSync(map[string]interface{}{"deviceId": "d1"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got["location"] != "plantC" {
		t.Errorf("location=%v, want plantC after upsert", got["location"])
	}
}
