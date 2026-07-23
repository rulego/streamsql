package stream

import (
	"testing"
)

// The JOIN key must be unified by SQL numeric semantics: the float64 decoded by the JSON stream corresponds to the int value of the typed dimension table
// Must match; otherwise, INNER JOIN silently drops lines; At the same time, string/bool/nil must not be mismatched.
func TestEncodeKey_NumericNormalization(t *testing.T) {
	cases := []struct {
		name string
		a, b any
		want bool // Expect the two codes to be equal
	}{
		{"int vs float64 same value", int(1), float64(1), true},
		{"int vs int64 same value", int(1), int64(1), true},
		{"uint vs int same value", uint(1), int(1), true},
		{"float64 whole vs int", float64(3.0), int(3), true},
		{"float64 frac distinct", float64(1.5), int(1), false},
		{"number vs string", int(1), "1", false},
		{"string vs string same", "1", "1", true},
		{"bool vs int", true, int(1), false},
		{"nil vs zero", nil, int(0), false},
		{"nil vs nil", nil, nil, true},
		{"neg zero vs zero", float64(0), mathNegZero(), true},
	}
	for _, c := range cases {
		eq := encodeKey(c.a) == encodeKey(c.b)
		if eq != c.want {
			t.Errorf("%s: encodeKey(%T %v)=%q, encodeKey(%T %v)=%q, want equal=%v",
				c.name, c.a, c.a, encodeKey(c.a), c.b, c.b, encodeKey(c.b), c.want)
		}
	}
	// Composite bond: Each component is unified separately
	if encodeKey([]any{int(1), "a"}) != encodeKey([]any{float64(1), "a"}) {
		t.Error("composite key: int/float64 segment should normalize")
	}
}

func mathNegZero() float64 {
	// Return -0.0 to verify that it is normalized to the same key as 0.0.
	var neg float64
	return -neg
}
