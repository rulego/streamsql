package stream

import (
	"fmt"
	"strings"

	"github.com/rulego/streamsql/utils/fieldpath"
)

// hasJoin reports whether this stream has any JOIN configured, so callers skip
// enrichment entirely on the common no-JOIN path (zero overhead).
func (s *Stream) hasJoin() bool {
	return len(s.config.JoinConfigs) > 0
}

// enrichJoin resolves all stream-table JOINs for a row.
//
// Returns:
//   - working: the map to use for field projection (a shallow copy of the row
//     with each matched table row attached under its alias). nil when keep=false.
//   - keep: false means an INNER JOIN found no match and the row must be dropped.
//   - err: non-nil when a JOIN references a table that was never registered
//     (configuration error); callers should surface it rather than drop silently.
//
// The working map copies the row so the caller's map is never mutated. When the
// query has a FROM alias, the row is also exposed under it so "s.<field>"
// references resolve. Table columns are read as "<alias>.<col>". For a LEFT JOIN
// with no match the alias maps to nil, so its columns evaluate to NULL.
func (s *Stream) enrichJoin(data map[string]interface{}) (working map[string]interface{}, keep bool, err error) {
	if len(s.config.JoinConfigs) == 0 {
		return data, true, nil
	}
	working = make(map[string]interface{}, len(data)+len(s.config.JoinConfigs)+1)
	for k, v := range data {
		working[k] = v
	}
	if s.config.SourceAlias != "" {
		working[s.config.SourceAlias] = data
	}
	for _, jc := range s.config.JoinConfigs {
		src, ok := s.tables.get(jc.Table)
		if !ok {
			return nil, false, fmt.Errorf("join table %q is not registered", jc.Table)
		}
		key := make([]interface{}, len(jc.OnPairs))
		for i, p := range jc.OnPairs {
			key[i], _ = streamFieldValue(data, p.StreamField)
		}
		row, matched := src.Lookup(key)
		switch {
		case matched:
			working[jc.Alias] = row
		case jc.JoinType == "LEFT":
			working[jc.Alias] = nil
		default:
			return nil, false, nil // INNER JOIN, no match: drop the row
		}
	}
	return working, true, nil
}

// streamFieldValue reads a stream-side field from the row. Bare names use a
// direct map lookup (the common case); dotted/bracket paths go through fieldpath.
func streamFieldValue(data map[string]interface{}, field string) (interface{}, bool) {
	if !strings.ContainsAny(field, ".[]") {
		v, ok := data[field]
		return v, ok
	}
	return fieldpath.GetNestedField(data, field)
}
