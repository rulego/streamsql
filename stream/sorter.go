package stream

import (
	"fmt"
	"sort"
	"time"

	"github.com/rulego/streamsql/types"
)

// Sorter orders result rows by the configured ORDER BY keys. Sorting is applied
// per emit batch (windowed queries) — global ordering over an unbounded stream
// is not possible. Stability: equal-key rows keep their input order
// (sort.SliceStable).
type Sorter struct {
	keys []types.OrderByField
}

// NewSorter builds a Sorter for the given keys. A nil/empty slice is a no-op.
func NewSorter(keys []types.OrderByField) *Sorter {
	return &Sorter{keys: keys}
}

// applyOrderBy sorts results in place when ORDER BY is configured. No-op
// otherwise, so queries without ORDER BY behave identically to before.
func (s *Stream) applyOrderBy(results []map[string]any) {
	if len(s.config.OrderBy) == 0 || len(results) < 2 {
		return
	}
	NewSorter(s.config.OrderBy).Sort(results)
}

// Sort sorts rows in place. It is a no-op when there are no keys or fewer than
// two rows.
func (s *Sorter) Sort(rows []map[string]any) {
	if len(s.keys) == 0 || len(rows) < 2 {
		return
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return s.less(rows[i], rows[j])
	})
}

// less reports whether row a should sort before row b under the configured keys.
func (s *Sorter) less(a, b map[string]any) bool {
	for _, k := range s.keys {
		av, aok := a[k.Expression]
		bv, bok := b[k.Expression]
		c := compareOrderValues(av, aok, bv, bok)
		if c == 0 {
			continue
		}
		if k.Direction == types.SortDesc {
			return c > 0
		}
		return c < 0
	}
	return false
}

// compareOrderValues is a three-way comparator for ORDER BY. Returns -1/0/1 for
// a<b / a==b / a>b. A missing key (ok=false) sorts first (nil is least).
// Numbers compare numerically across int/float kinds; time.Time by instant;
// bool false<true; everything else falls back to a string comparison.
func compareOrderValues(a any, aok bool, b any, bok bool) int {
	if !aok && !bok {
		return 0
	}
	if !aok {
		return -1
	}
	if !bok {
		return 1
	}
	if af, ok := numericFloat(a); ok {
		if bf, ok2 := numericFloat(b); ok2 {
			switch {
			case af < bf:
				return -1
			case af > bf:
				return 1
			default:
				return 0
			}
		}
	}
	if at, ok := a.(time.Time); ok {
		if bt, ok2 := b.(time.Time); ok2 {
			switch {
			case at.Before(bt):
				return -1
			case at.After(bt):
				return 1
			default:
				return 0
			}
		}
	}
	if ab, ok := a.(bool); ok {
		if bb, ok2 := b.(bool); ok2 {
			switch {
			case ab == bb:
				return 0
			case !ab:
				return -1
			default:
				return 1
			}
		}
	}
	as, bs := orderString(a), orderString(b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

// numericFloat converts Go numeric kinds to float64. Non-numeric values (incl.
// strings) return ok=false so they fall through to the string comparator.
func numericFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	}
	return 0, false
}

func orderString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}
