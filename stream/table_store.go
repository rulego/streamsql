package stream

import (
	"fmt"
	"strings"
	"sync"
)

// TableSource backs a stream-table JOIN (v0.5: metadata enrichment).
// Implementations own the data lifecycle (load/refresh/cleanup) and MUST make
// Lookup safe for concurrent use, since it runs on the per-row hot path.
//
// Lookup receives the join key built by the engine: a []interface{} of the
// stream-side key values, in the same order as the ON clause's table-side
// fields (which must match the source's indexed key fields). For a single-key
// JOIN the slice has one element.
type TableSource interface {
	Name() string
	Lookup(key interface{}) (map[string]interface{}, bool)
	Init() error
	Close() error
}

// MemoryTableSource is an in-memory table indexed by one or more key fields.
// It is purely push-based (no background goroutine): callers mutate it via
// Upsert/Delete, or rebuild it wholesale by registering a new source.
type MemoryTableSource struct {
	name      string
	keyFields []string
	mu        sync.RWMutex
	index     map[string]map[string]interface{}
}

// NewMemoryTableSource builds an in-memory table from rows indexed by keyFields.
// keyFields must be in the same order as the JOIN ON table-side fields.
func NewMemoryTableSource(name string, keyFields []string, rows []map[string]interface{}) *MemoryTableSource {
	src := &MemoryTableSource{
		name:      name,
		keyFields: keyFields,
		index:     make(map[string]map[string]interface{}, len(rows)),
	}
	for _, r := range rows {
		src.index[encodeKey(src.encodeRow(r))] = r
	}
	return src
}

// Name returns the table source name.
func (m *MemoryTableSource) Name() string { return m.name }

// KeyFields returns the fields the table is indexed by.
func (m *MemoryTableSource) KeyFields() []string { return m.keyFields }

// Init is a no-op for the in-memory source (data is supplied at construction).
func (m *MemoryTableSource) Init() error { return nil }

// Close is a no-op for the in-memory source.
func (m *MemoryTableSource) Close() error { return nil }

// Lookup returns the row matching key, or (nil, false). key is a []interface{}
// of key-field values in indexed order, or a single value for single-key tables.
func (m *MemoryTableSource) Lookup(key interface{}) (map[string]interface{}, bool) {
	m.mu.RLock()
	row, ok := m.index[encodeKey(key)]
	m.mu.RUnlock()
	return row, ok
}

// Upsert adds or replaces the row, keyed by its key-field values.
func (m *MemoryTableSource) Upsert(row map[string]interface{}) {
	k := encodeKey(m.encodeRow(row))
	m.mu.Lock()
	m.index[k] = row
	m.mu.Unlock()
}

// Delete removes the row whose key-field values match key.
func (m *MemoryTableSource) Delete(key interface{}) {
	k := encodeKey(key)
	m.mu.Lock()
	delete(m.index, k)
	m.mu.Unlock()
}

// encodeRow extracts this row's key-field values in indexed order.
func (m *MemoryTableSource) encodeRow(row map[string]interface{}) []interface{} {
	vals := make([]interface{}, len(m.keyFields))
	for i, f := range m.keyFields {
		vals[i] = row[f]
	}
	return vals
}

// encodeKey serializes a lookup key into a stable, type-tagged string so that
// 1 (int) and "1" (string) never collide. Accepts a single value or a
// []interface{} tuple.
func encodeKey(key interface{}) string {
	if vals, ok := key.([]interface{}); ok {
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = encodeOne(v)
		}
		return strings.Join(parts, "\x1f")
	}
	return encodeOne(key)
}

func encodeOne(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%T:%v", v, v)
}

// tableStore holds registered table sources keyed by name. It is concurrency-safe.
type tableStore struct {
	mu      sync.RWMutex
	sources map[string]TableSource
}

func newTableStore() *tableStore {
	return &tableStore{sources: make(map[string]TableSource)}
}

// register initializes and stores a source under its name. Init runs outside the
// write lock so a slow source load does not block concurrent readers.
func (ts *tableStore) register(src TableSource) error {
	if err := src.Init(); err != nil {
		return err
	}
	ts.mu.Lock()
	ts.sources[src.Name()] = src
	ts.mu.Unlock()
	return nil
}

func (ts *tableStore) get(name string) (TableSource, bool) {
	ts.mu.RLock()
	src, ok := ts.sources[name]
	ts.mu.RUnlock()
	return src, ok
}

// closeAll closes every source. Called from Stream.Stop. Idempotent per source.
func (ts *tableStore) closeAll() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	for _, src := range ts.sources {
		_ = src.Close()
	}
	ts.sources = make(map[string]TableSource)
}
