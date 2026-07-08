package stream

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
)

func TestSorter_AscNumeric(t *testing.T) {
	s := NewSorter([]types.OrderByField{{Expression: "v", Direction: types.SortAsc}})
	rows := []map[string]any{
		{"v": float64(3)}, {"v": float64(1)}, {"v": float64(2)},
	}
	s.Sort(rows)
	assert.Equal(t, []float64{1, 2, 3}, []float64{rows[0]["v"].(float64), rows[1]["v"].(float64), rows[2]["v"].(float64)})
}

func TestSorter_DescNumeric(t *testing.T) {
	s := NewSorter([]types.OrderByField{{Expression: "v", Direction: types.SortDesc}})
	rows := []map[string]any{
		{"v": float64(1)}, {"v": float64(3)}, {"v": float64(2)},
	}
	s.Sort(rows)
	assert.Equal(t, []float64{3, 2, 1}, []float64{rows[0]["v"].(float64), rows[1]["v"].(float64), rows[2]["v"].(float64)})
}

func TestSorter_MixedNumericKinds(t *testing.T) {
	// int, int64, float64, uint should all compare numerically.
	s := NewSorter([]types.OrderByField{{Expression: "v", Direction: types.SortAsc}})
	rows := []map[string]any{
		{"v": int(10)}, {"v": float64(2)}, {"v": int64(5)}, {"v": uint(1)},
	}
	s.Sort(rows)
	got := []any{rows[0]["v"], rows[1]["v"], rows[2]["v"], rows[3]["v"]}
	assert.Equal(t, []any{uint(1), float64(2), int64(5), int(10)}, got)
}

func TestSorter_String(t *testing.T) {
	s := NewSorter([]types.OrderByField{{Expression: "name", Direction: types.SortAsc}})
	rows := []map[string]any{
		{"name": "banana"}, {"name": "apple"}, {"name": "cherry"},
	}
	s.Sort(rows)
	assert.Equal(t, []string{"apple", "banana", "cherry"},
		[]string{rows[0]["name"].(string), rows[1]["name"].(string), rows[2]["name"].(string)})
}

func TestSorter_MultiKey(t *testing.T) {
	// Primary a ASC, secondary b DESC.
	s := NewSorter([]types.OrderByField{
		{Expression: "a", Direction: types.SortAsc},
		{Expression: "b", Direction: types.SortDesc},
	})
	rows := []map[string]any{
		{"a": 1, "b": 1}, {"a": 1, "b": 3}, {"a": 1, "b": 2},
		{"a": 2, "b": 0}, {"a": 2, "b": 9},
	}
	s.Sort(rows)
	// a=1 group sorted by b desc: 3,2,1 ; a=2 group: 9,0
	assert.Equal(t, 1, rows[0]["a"])
	assert.Equal(t, 3, rows[0]["b"])
	assert.Equal(t, 1, rows[1]["a"])
	assert.Equal(t, 2, rows[1]["b"])
	assert.Equal(t, 1, rows[2]["a"])
	assert.Equal(t, 1, rows[2]["b"])
	assert.Equal(t, 2, rows[3]["a"])
	assert.Equal(t, 9, rows[3]["b"])
	assert.Equal(t, 2, rows[4]["a"])
	assert.Equal(t, 0, rows[4]["b"])
}

func TestSorter_MissingKeySortsFirst(t *testing.T) {
	s := NewSorter([]types.OrderByField{{Expression: "v", Direction: types.SortAsc}})
	rows := []map[string]any{
		{"v": float64(5)}, {"other": 1}, {"v": float64(2)},
	}
	s.Sort(rows)
	// missing key (nil) is least -> first
	_, ok := rows[0]["v"]
	assert.False(t, ok)
	assert.Equal(t, float64(2), rows[1]["v"])
	assert.Equal(t, float64(5), rows[2]["v"])
}

func TestSorter_BoolAndTime(t *testing.T) {
	t1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	s := NewSorter([]types.OrderByField{{Expression: "t", Direction: types.SortAsc}})
	rows := []map[string]any{{"t": t3}, {"t": t1}, {"t": t2}}
	s.Sort(rows)
	assert.Equal(t, t1, rows[0]["t"])
	assert.Equal(t, t2, rows[1]["t"])
	assert.Equal(t, t3, rows[2]["t"])

	sb := NewSorter([]types.OrderByField{{Expression: "b", Direction: types.SortAsc}})
	brows := []map[string]any{{"b": true}, {"b": false}, {"b": true}}
	sb.Sort(brows)
	assert.Equal(t, false, brows[0]["b"])
	assert.Equal(t, true, brows[1]["b"])
	assert.Equal(t, true, brows[2]["b"])
}

func TestSorter_Stable(t *testing.T) {
	// Equal keys must preserve input order.
	s := NewSorter([]types.OrderByField{{Expression: "k", Direction: types.SortAsc}})
	rows := []map[string]any{
		{"k": 1, "order": "a"}, {"k": 1, "order": "b"}, {"k": 1, "order": "c"},
		{"k": 1, "order": "d"},
	}
	s.Sort(rows)
	assert.Equal(t, []string{"a", "b", "c", "d"},
		[]string{rows[0]["order"].(string), rows[1]["order"].(string), rows[2]["order"].(string), rows[3]["order"].(string)})
}

func TestSorter_NoopWhenNoKeysOrFewRows(t *testing.T) {
	// No keys: unchanged.
	NewSorter(nil).Sort(nil)
	rows := []map[string]any{{"v": 2.0}, {"v": 1.0}}
	NewSorter(nil).Sort(rows)
	assert.Equal(t, 2.0, rows[0]["v"]) // unchanged

	// Single row: unchanged regardless of keys.
	one := []map[string]any{{"v": 9.0}}
	NewSorter([]types.OrderByField{{Expression: "v", Direction: types.SortDesc}}).Sort(one)
	assert.Equal(t, 9.0, one[0]["v"])
}

func TestCompareOrderValues_NumericVsStringFallback(t *testing.T) {
	// A number and a non-numeric-string fall through to string comparison.
	c := compareOrderValues(5, true, "abc", true)
	assert.Equal(t, -1, c) // "5" < "abc"
	c = compareOrderValues("abc", true, "xyz", true)
	assert.Equal(t, -1, c)
	c = compareOrderValues(float64(3), true, int(3), true)
	assert.Equal(t, 0, c) // numeric equality across kinds
}
