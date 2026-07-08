package e2e

import (
	"sync"
	"testing"

	"github.com/rulego/streamsql"
)

// deviceMetaRows is a small metadata fixture used across JOIN tests.
func deviceMetaRows() []map[string]any {
	return []map[string]any{
		{"deviceId": "d1", "location": "plantA", "type": "temp"},
		{"deviceId": "d2", "location": "plantB", "type": "humid"},
	}
}

// TestJoinMultipleTables verifies a stream can JOIN several metadata tables;
// each is registered separately and its columns are namespaced by its own alias.
func TestJoinMultipleTables(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, l.location, s.model FROM stream JOIN locations l ON deviceId = l.deviceId JOIN models s ON deviceId = s.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("locations", []map[string]any{
		{"deviceId": "d1", "location": "plantA"},
	}); err != nil {
		t.Fatalf("RegisterTable locations: %v", err)
	}
	if _, err := ssql.RegisterTable("models", []map[string]any{
		{"deviceId": "d1", "model": "MX-1"},
	}); err != nil {
		t.Fatalf("RegisterTable models: %v", err)
	}

	got, err := ssql.EmitSync(map[string]any{"deviceId": "d1"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got["location"] != "plantA" || got["model"] != "MX-1" || got["deviceId"] != "d1" {
		t.Errorf("multi-table enrich got=%v, want location=plantA model=MX-1", got)
	}
}

func TestJoinInnerEnrich(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location, m.type FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", deviceMetaRows()); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	got, err := ssql.EmitSync(map[string]any{"deviceId": "d1", "temp": 35})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	want := map[string]any{"deviceId": "d1", "location": "plantA", "type": "temp"}
	if got["deviceId"] != want["deviceId"] || got["location"] != want["location"] || got["type"] != want["type"] {
		t.Errorf("got=%v, want=%v", got, want)
	}
}

func TestJoinInnerNoMatchDropped(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", deviceMetaRows()); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	// d3 has no metadata row -> INNER JOIN drops it (nil result, no error).
	got, err := ssql.EmitSync(map[string]any{"deviceId": "d3"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil (dropped), got %v", got)
	}
}

func TestJoinLeftNullFill(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream LEFT JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", deviceMetaRows()); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	got, err := ssql.EmitSync(map[string]any{"deviceId": "d9"})
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
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId AND tenant = m.tenant"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rows := []map[string]any{
		{"deviceId": "d1", "tenant": "t1", "location": "plantA"},
		{"deviceId": "d1", "tenant": "t2", "location": "plantB"},
	}
	// Composite key auto-derived from the two ON equalities (deviceId, tenant).
	if _, err := ssql.RegisterTable("meta", rows); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	got, err := ssql.EmitSync(map[string]any{"deviceId": "d1", "tenant": "t2"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got["location"] != "plantB" {
		t.Errorf("location=%v, want plantB (composite key)", got["location"])
	}
}

// TestJoinExplicitKeyFields overrides the auto-derived key with explicit
// keyFields (useful when the index column differs from the ON field, or to
// register before the JOIN key is otherwise derivable).
func TestJoinExplicitKeyFields(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// Explicit single key override.
	if _, err := ssql.RegisterTable("meta", deviceMetaRows(), "deviceId"); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	got, err := ssql.EmitSync(map[string]any{"deviceId": "d1"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got["location"] != "plantA" {
		t.Errorf("location=%v, want plantA", got["location"])
	}
}

// TestJoinRegisterTableNotInJoin errors when auto-derive can't find the table in
// any JOIN ON clause and no explicit keyFields were given.
func TestJoinRegisterTableNotInJoin(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId FROM stream"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", deviceMetaRows()); err == nil {
		t.Error("expected error registering a table not referenced by any JOIN, got nil")
	}
}

func TestJoinUnregisteredTableErrors(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// No RegisterTable call -> EmitSync must surface the config error.
	if _, err := ssql.EmitSync(map[string]any{"deviceId": "d1"}); err == nil {
		t.Error("expected error for unregistered JOIN table, got nil")
	}
}

// TestJoinConcurrentEmitAndUpsert verifies Lookup stays race-free under
// concurrent reads (Emit) and writes (Upsert). Run with -race in CI.
func TestJoinConcurrentEmitAndUpsert(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	src, err := ssql.RegisterTable("meta", deviceMetaRows())
	if err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = ssql.EmitSync(map[string]any{"deviceId": "d1"})
		}()
		go func() {
			defer wg.Done()
			src.Upsert(map[string]any{"deviceId": "d1", "location": "plantC"})
		}()
	}
	wg.Wait()

	got, err := ssql.EmitSync(map[string]any{"deviceId": "d1"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got["location"] != "plantC" {
		t.Errorf("location=%v, want plantC after upsert", got["location"])
	}
}
