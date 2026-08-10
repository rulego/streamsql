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

package stream

import (
	"errors"
	"testing"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- MemoryTableSource ---

func TestMemoryTableSource_LookupByAccessors(t *testing.T) {
	src := NewMemoryTableSource("meta", []string{"id"}, []map[string]any{
		{"id": "d1", "location": "north"},
		{"id": "d2", "location": "south"},
	})

	assert.Equal(t, "meta", src.Name())
	assert.Equal(t, []string{"id"}, src.KeyFields())
	require.NoError(t, src.Init())

	row, ok := src.Lookup("d1")
	require.True(t, ok)
	assert.Equal(t, "north", row["location"])

	_, ok = src.Lookup("absent")
	assert.False(t, ok)

	require.NoError(t, src.Close())
}

func TestMemoryTableSource_UpsertAndDelete(t *testing.T) {
	src := NewMemoryTableSource("meta", []string{"id"}, nil)

	src.Upsert(map[string]any{"id": "d1", "location": "north"})
	row, ok := src.Lookup("d1")
	require.True(t, ok)
	assert.Equal(t, "north", row["location"])

	// Upsert on the same key replaces rather than duplicates.
	src.Upsert(map[string]any{"id": "d1", "location": "east"})
	row, ok = src.Lookup("d1")
	require.True(t, ok)
	assert.Equal(t, "east", row["location"], "upsert must replace the existing row")

	src.Delete("d1")
	_, ok = src.Lookup("d1")
	assert.False(t, ok, "deleted key must not be found")
}

func TestMemoryTableSource_CompositeKeyOrderMatters(t *testing.T) {
	src := NewMemoryTableSource("meta", []string{"region", "id"}, []map[string]any{
		{"region": "north", "id": "d1", "owner": "alice"},
	})

	row, ok := src.Lookup([]any{"north", "d1"})
	require.True(t, ok)
	assert.Equal(t, "alice", row["owner"])

	_, ok = src.Lookup([]any{"d1", "north"})
	assert.False(t, ok, "composite key must be order-sensitive")
}

// Numeric keys normalize across types: a JSON-decoded float64 key must match an
// int-typed dimension table, otherwise INNER JOIN silently drops rows.
func TestMemoryTableSource_NumericKeysNormalizeAcrossTypes(t *testing.T) {
	src := NewMemoryTableSource("meta", []string{"id"}, []map[string]any{
		{"id": 1, "name": "one"},
	})

	for _, k := range []any{1, int64(1), float64(1), float32(1), uint(1), uint64(1)} {
		row, ok := src.Lookup(k)
		if assert.Truef(t, ok, "numeric key %T(%v) must match the int-keyed row", k, k) {
			assert.Equal(t, "one", row["name"])
		}
	}
}

// But a numeric key must never collide with the string that looks like it.
func TestMemoryTableSource_StringKeyDoesNotCollideWithNumeric(t *testing.T) {
	src := NewMemoryTableSource("meta", []string{"id"}, []map[string]any{
		{"id": 1, "name": "numeric-one"},
		{"id": "1", "name": "string-one"},
	})

	numeric, ok := src.Lookup(1)
	require.True(t, ok)
	assert.Equal(t, "numeric-one", numeric["name"])

	str, ok := src.Lookup("1")
	require.True(t, ok)
	assert.Equal(t, "string-one", str["name"], `1 and "1" must stay distinct keys`)
}

func TestMemoryTableSource_NilAndBoolKeys(t *testing.T) {
	src := NewMemoryTableSource("meta", []string{"k"}, []map[string]any{
		{"k": nil, "v": "nil-row"},
		{"k": true, "v": "bool-row"},
	})

	row, ok := src.Lookup(nil)
	require.True(t, ok)
	assert.Equal(t, "nil-row", row["v"])

	row, ok = src.Lookup(true)
	require.True(t, ok)
	assert.Equal(t, "bool-row", row["v"])

	_, ok = src.Lookup(false)
	assert.False(t, ok, "false must not match the true-keyed row")
}

// --- tableStore ---

// failingTableSource reports an Init error, simulating a file/DB load failure.
type failingTableSource struct{ name string }

func (f *failingTableSource) Name() string                      { return f.name }
func (f *failingTableSource) Lookup(any) (map[string]any, bool) { return nil, false }
func (f *failingTableSource) Init() error                       { return errors.New("load failed") }
func (f *failingTableSource) Close() error                      { return nil }

// closeTrackingSource records whether Close was called.
type closeTrackingSource struct {
	name   string
	closed bool
}

func (c *closeTrackingSource) Name() string                      { return c.name }
func (c *closeTrackingSource) Lookup(any) (map[string]any, bool) { return nil, false }
func (c *closeTrackingSource) Init() error                       { return nil }
func (c *closeTrackingSource) Close() error                      { c.closed = true; return nil }

func TestTableStore_RegisterPropagatesInitError(t *testing.T) {
	ts := newTableStore()
	err := ts.register(&failingTableSource{name: "broken"})
	require.Error(t, err, "a source that fails to load must not be registered")

	_, ok := ts.get("broken")
	assert.False(t, ok, "a failed source must not be visible to lookups")
}

func TestTableStore_CloseAllClosesAndClears(t *testing.T) {
	ts := newTableStore()
	src := &closeTrackingSource{name: "meta"}
	require.NoError(t, ts.register(src))

	_, ok := ts.get("meta")
	require.True(t, ok)

	ts.closeAll()

	assert.True(t, src.closed, "closeAll must close each source")
	_, ok = ts.get("meta")
	assert.False(t, ok, "closeAll must clear the registry")
}

// --- Stream table registration API ---

func TestStream_RegisterMemoryTableAndUpsertRow(t *testing.T) {
	s, err := NewStream(types.Config{})
	require.NoError(t, err)
	defer s.Stop()

	src, err := s.RegisterMemoryTable("meta", []string{"id"}, []map[string]any{
		{"id": "d1", "location": "north"},
	})
	require.NoError(t, err)
	require.NotNil(t, src)

	require.NoError(t, s.UpsertTableRow("meta", map[string]any{"id": "d2", "location": "south"}))
	row, ok := src.Lookup("d2")
	require.True(t, ok)
	assert.Equal(t, "south", row["location"])

	err = s.UpsertTableRow("absent", map[string]any{"id": "x"})
	assert.Error(t, err, "upsert into an unregistered table must be an error, not a silent no-op")
}

func TestStream_RegisterTableSourceCustom(t *testing.T) {
	s, err := NewStream(types.Config{})
	require.NoError(t, err)
	defer s.Stop()

	require.NoError(t, s.RegisterTableSource(&closeTrackingSource{name: "custom"}))
	_, ok := s.tables.get("custom")
	assert.True(t, ok)

	assert.Error(t, s.RegisterTableSource(&failingTableSource{name: "broken"}))
}

func TestStream_JoinKeyFieldsDerivedFromOnClause(t *testing.T) {
	s, err := NewStream(types.Config{
		JoinConfigs: []types.JoinConfig{{
			Table: "meta", Alias: "m", JoinType: "INNER",
			OnPairs: []types.JoinOnPair{
				{StreamField: "deviceId", TableField: "id"},
				{StreamField: "zone", TableField: "region"},
			},
		}},
	})
	require.NoError(t, err)
	defer s.Stop()

	fields, err := s.JoinKeyFields("meta")
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "region"}, fields,
		"key fields must follow the ON clause table-side order")

	_, err = s.JoinKeyFields("unreferenced")
	assert.Error(t, err)
}

// --- enrichJoin ---

func newJoinStream(t *testing.T, joinType string, rows []map[string]any) *Stream {
	t.Helper()
	s, err := NewStream(types.Config{
		SourceAlias: "s",
		JoinConfigs: []types.JoinConfig{{
			Table: "meta", Alias: "m", JoinType: joinType,
			OnPairs: []types.JoinOnPair{{StreamField: "deviceId", TableField: "id"}},
		}},
	})
	require.NoError(t, err)
	t.Cleanup(s.Stop)

	_, err = s.RegisterMemoryTable("meta", []string{"id"}, rows)
	require.NoError(t, err)
	return s
}

func TestEnrichJoin_NoJoinConfigPassesRowThrough(t *testing.T) {
	s, err := NewStream(types.Config{})
	require.NoError(t, err)
	defer s.Stop()

	in := map[string]any{"v": 1}
	working, keep, err := s.enrichJoin(in)
	require.NoError(t, err)
	assert.True(t, keep)
	assert.Equal(t, in, working, "no-JOIN path must pass the row through untouched")
}

func TestEnrichJoin_InnerMatchAttachesTableRow(t *testing.T) {
	s := newJoinStream(t, "INNER", []map[string]any{{"id": "d1", "location": "north"}})

	in := map[string]any{"deviceId": "d1", "temp": 20.0}
	working, keep, err := s.enrichJoin(in)
	require.NoError(t, err)
	require.True(t, keep)

	meta, ok := working["m"].(map[string]any)
	require.True(t, ok, "matched table row must be attached under its alias")
	assert.Equal(t, "north", meta["location"])

	// The source alias exposes the original row so "s.<field>" resolves.
	self, ok := working["s"].(map[string]any)
	require.True(t, ok, "FROM alias must expose the stream row")
	assert.EqualValues(t, 20.0, self["temp"])

	// The caller's map must not be mutated.
	assert.NotContains(t, in, "m", "enrichJoin must not mutate the caller's row")
}

func TestEnrichJoin_InnerNoMatchDropsRow(t *testing.T) {
	s := newJoinStream(t, "INNER", []map[string]any{{"id": "d1"}})

	_, keep, err := s.enrichJoin(map[string]any{"deviceId": "absent"})
	require.NoError(t, err)
	assert.False(t, keep, "INNER JOIN without a match must drop the row")
}

func TestEnrichJoin_LeftNoMatchKeepsRowWithEmptyAlias(t *testing.T) {
	s := newJoinStream(t, "LEFT", []map[string]any{{"id": "d1"}})

	working, keep, err := s.enrichJoin(map[string]any{"deviceId": "absent"})
	require.NoError(t, err)
	require.True(t, keep, "LEFT JOIN must keep an unmatched row")

	meta, ok := working["m"].(map[string]any)
	require.True(t, ok)
	assert.Empty(t, meta, "unmatched LEFT JOIN alias must be empty so its columns are NULL")
}

func TestEnrichJoin_UnregisteredTableIsAnError(t *testing.T) {
	s, err := NewStream(types.Config{
		JoinConfigs: []types.JoinConfig{{
			Table: "never_registered", Alias: "m",
			OnPairs: []types.JoinOnPair{{StreamField: "deviceId", TableField: "id"}},
		}},
	})
	require.NoError(t, err)
	defer s.Stop()

	_, keep, err := s.enrichJoin(map[string]any{"deviceId": "d1"})
	require.Error(t, err, "a missing table is a config error and must be surfaced, not dropped silently")
	assert.False(t, keep)
	assert.Contains(t, err.Error(), "never_registered")
}

func TestEnrichJoin_NestedStreamFieldAsJoinKey(t *testing.T) {
	s, err := NewStream(types.Config{
		JoinConfigs: []types.JoinConfig{{
			Table: "meta", Alias: "m", JoinType: "INNER",
			OnPairs: []types.JoinOnPair{{StreamField: "tags.id", TableField: "id"}},
		}},
	})
	require.NoError(t, err)
	defer s.Stop()
	_, err = s.RegisterMemoryTable("meta", []string{"id"}, []map[string]any{
		{"id": "d1", "location": "north"},
	})
	require.NoError(t, err)

	working, keep, err := s.enrichJoin(map[string]any{
		"tags": map[string]any{"id": "d1"},
	})
	require.NoError(t, err)
	require.True(t, keep, "a dotted stream field must resolve as a JOIN key")

	meta, _ := working["m"].(map[string]any)
	assert.Equal(t, "north", meta["location"])
}

func TestStreamFieldValue_BareAndNestedPaths(t *testing.T) {
	row := map[string]any{
		"flat":   1,
		"nested": map[string]any{"inner": "deep"},
	}

	v, ok := streamFieldValue(row, "flat")
	require.True(t, ok)
	assert.EqualValues(t, 1, v)

	v, ok = streamFieldValue(row, "nested.inner")
	require.True(t, ok)
	assert.Equal(t, "deep", v)

	_, ok = streamFieldValue(row, "absent")
	assert.False(t, ok)
}
