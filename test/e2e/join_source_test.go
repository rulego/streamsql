package e2e

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/streamsql"
)

// mockSource is a minimal TableSource for tests: an in-memory map the test can
// mutate to simulate external loads/refreshes. Lookup receives the engine-built
// []interface{} key; for a single-key JOIN it reads the first element.
type mockSource struct {
	name   string
	mu     sync.RWMutex
	cache  map[interface{}]map[string]interface{}
	inited int32
	closed int32
}

func newMockSource(name string) *mockSource {
	return &mockSource{name: name, cache: make(map[interface{}]map[string]interface{})}
}

func (m *mockSource) Name() string { return m.name }
func (m *mockSource) Init() error  { atomic.StoreInt32(&m.inited, 1); return nil }
func (m *mockSource) Close() error { atomic.StoreInt32(&m.closed, 1); return nil }

func (m *mockSource) Lookup(key interface{}) (map[string]interface{}, bool) {
	vals, _ := key.([]interface{})
	var k interface{}
	if len(vals) > 0 {
		k = vals[0]
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	row, ok := m.cache[k]
	return row, ok
}

func (m *mockSource) put(k string, row map[string]interface{}) {
	m.mu.Lock()
	m.cache[k] = row
	m.mu.Unlock()
}

// TestJoinCustomTableSource verifies a user-implemented TableSource drives the
// JOIN enrichment, and that Init runs at registration.
func TestJoinCustomTableSource(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	src := newMockSource("meta")
	src.put("d1", map[string]interface{}{"location": "plantA"})
	if err := ssql.RegisterTableSource(src); err != nil {
		t.Fatalf("RegisterTableSource: %v", err)
	}
	if atomic.LoadInt32(&src.inited) != 1 {
		t.Error("Init was not called on registration")
	}
	got, err := ssql.EmitSync(map[string]interface{}{"deviceId": "d1"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got["location"] != "plantA" {
		t.Errorf("custom source enrich got=%v, want location=plantA", got)
	}
}

// TestJoinCustomSourceClosedOnStop verifies Stop closes registered sources.
func TestJoinCustomSourceClosedOnStop(t *testing.T) {
	ssql := streamsql.New()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	src := newMockSource("meta")
	src.put("d1", map[string]interface{}{"location": "plantA"})
	if err := ssql.RegisterTableSource(src); err != nil {
		t.Fatalf("RegisterTableSource: %v", err)
	}
	ssql.Stop()
	if atomic.LoadInt32(&src.closed) != 1 {
		t.Error("Close was not called on Stop")
	}
}

// TestJoinCustomSourceRefresh verifies the source owns its data: a "refresh"
// (external mutation of the source's cache) is visible to subsequent emits,
// without re-registering.
func TestJoinCustomSourceRefresh(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	src := newMockSource("meta")
	src.put("d1", map[string]interface{}{"location": "old"})
	if err := ssql.RegisterTableSource(src); err != nil {
		t.Fatalf("RegisterTableSource: %v", err)
	}

	got, _ := ssql.EmitSync(map[string]interface{}{"deviceId": "d1"})
	if got["location"] != "old" {
		t.Errorf("before refresh got=%v, want old", got)
	}

	src.put("d1", map[string]interface{}{"location": "new"}) // simulate external refresh
	got, _ = ssql.EmitSync(map[string]interface{}{"deviceId": "d1"})
	if got["location"] != "new" {
		t.Errorf("after refresh got=%v, want new", got)
	}
}

// TestJoinAsyncEmit verifies enrichment runs on the async Emit path
// (processDirectData), not only EmitSync.
func TestJoinAsyncEmit(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", deviceMetaRows()); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	var mu sync.Mutex
	var results []map[string]interface{}
	ssql.AddSink(func(rows []map[string]interface{}) {
		mu.Lock()
		results = append(results, rows...)
		mu.Unlock()
	})

	ssql.Emit(map[string]interface{}{"deviceId": "d1"})
	ssql.Emit(map[string]interface{}{"deviceId": "d99"}) // no metadata -> INNER drops

	// Poll for the matching row to arrive (d99 is dropped, never sinked).
	for i := 0; i < 100; i++ {
		mu.Lock()
		n := len(results)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(results) != 1 {
		t.Fatalf("async results len=%d, want 1 (d99 dropped)", len(results))
	}
	if results[0]["location"] != "plantA" {
		t.Errorf("async enrich got=%v, want location=plantA", results[0])
	}
}

// TestJoinUpsertTableAndDelete covers the incremental update wrappers.
func TestJoinUpsertTableAndDelete(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	src, err := ssql.RegisterTable("meta", deviceMetaRows())
	if err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	// UpsertTable wrapper: change d1's location.
	if err := ssql.UpsertTable("meta", map[string]interface{}{"deviceId": "d1", "location": "plantX"}); err != nil {
		t.Fatalf("UpsertTable: %v", err)
	}
	got, _ := ssql.EmitSync(map[string]interface{}{"deviceId": "d1"})
	if got["location"] != "plantX" {
		t.Errorf("after upsert got=%v, want plantX", got)
	}

	// Delete: d1 no longer matches -> INNER drops.
	src.Delete("d1")
	got, _ = ssql.EmitSync(map[string]interface{}{"deviceId": "d1"})
	if got != nil {
		t.Errorf("after delete got=%v, want nil (dropped)", got)
	}
}

// TestJoinWithWhere verifies WHERE filtering on a STREAM column coexists with
// enrichment (WHERE applies after enrichment, stream columns still visible).
func TestJoinWithWhere(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId WHERE temperature > 30"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", deviceMetaRows()); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	// Passes WHERE (temp 35) and matches metadata -> enriched.
	got, _ := ssql.EmitSync(map[string]interface{}{"deviceId": "d1", "temperature": 35})
	if got == nil || got["location"] != "plantA" {
		t.Errorf("matching row got=%v, want enriched", got)
	}
	// Filtered out by WHERE (temp 20).
	got, _ = ssql.EmitSync(map[string]interface{}{"deviceId": "d1", "temperature": 20})
	if got != nil {
		t.Errorf("filtered row got=%v, want nil", got)
	}
}

// TestJoinWhereOnMetadata verifies WHERE can filter on a JOINED metadata column
// (enrichment runs before filtering).
func TestJoinWhereOnMetadata(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId WHERE m.type = 'temp'"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", deviceMetaRows()); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	// d1 has type=temp -> passes WHERE m.type='temp'.
	got, _ := ssql.EmitSync(map[string]interface{}{"deviceId": "d1"})
	if got == nil || got["location"] != "plantA" {
		t.Errorf("d1 should pass WHERE m.type='temp': got=%v", got)
	}
	// d2 has type=humid -> filtered out by WHERE on metadata column.
	got, _ = ssql.EmitSync(map[string]interface{}{"deviceId": "d2"})
	if got != nil {
		t.Errorf("d2 should be filtered by WHERE m.type='temp': got=%v", got)
	}
}

// TestJoinLeftWhereMetadataIsNull verifies LEFT JOIN + WHERE m.<col> IS NULL
// selects stream rows with no metadata match.
func TestJoinLeftWhereMetadataIsNull(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT deviceId FROM stream LEFT JOIN meta m ON deviceId = m.deviceId WHERE m.location IS NULL"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", deviceMetaRows()); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	// d9 has no metadata -> m.location NULL -> passes IS NULL.
	got, _ := ssql.EmitSync(map[string]interface{}{"deviceId": "d9"})
	if got == nil {
		t.Error("unmatched LEFT row should pass WHERE m.location IS NULL")
	}
	// d1 matches -> m.location not NULL -> filtered out.
	got, _ = ssql.EmitSync(map[string]interface{}{"deviceId": "d1"})
	if got != nil {
		t.Errorf("matched row should be filtered by WHERE m.location IS NULL: got=%v", got)
	}
}

// TestJoinWithAggregationRejected verifies JOIN + aggregation is rejected at
// Execute (v0.5: JOIN is transform-only).
func TestJoinWithAggregationRejected(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	err := ssql.Execute("SELECT m.location, AVG(temperature) FROM stream JOIN meta m ON deviceId = m.deviceId GROUP BY m.location")
	if err == nil {
		t.Error("expected error for JOIN + aggregation, got nil")
	}
}

// TestJoinStreamAliasAndNestedField verifies FROM-alias-qualified stream fields
// (s.deviceId) and nested table fields (m.profile.id) resolve correctly.
func TestJoinStreamAliasAndNestedField(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT s.deviceId, m.location, m.profile.id AS pid FROM stream s JOIN meta m ON s.deviceId = m.deviceId"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("meta", []map[string]interface{}{
		{"deviceId": "d1", "location": "plantA", "profile": map[string]interface{}{"id": "u1"}},
	}); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	got, err := ssql.EmitSync(map[string]interface{}{"deviceId": "d1"})
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got["deviceId"] != "d1" {
		t.Errorf("s.deviceId resolved to %v, want d1", got["deviceId"])
	}
	if got["location"] != "plantA" {
		t.Errorf("m.location resolved to %v, want plantA", got["location"])
	}
	if got["pid"] != "u1" {
		t.Errorf("m.profile.id resolved to %v, want u1", got["pid"])
	}
}
