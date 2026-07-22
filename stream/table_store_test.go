package stream

import (
	"testing"
)

// JOIN 键必须按 SQL 数值语义归一：JSON 流解码出的 float64 与类型化维度表的 int 同值
// 必须匹配，否则 INNER JOIN 静默丢行；同时 string/bool/nil 不得误匹配。
func TestEncodeKey_NumericNormalization(t *testing.T) {
	cases := []struct {
		name string
		a, b any
		want bool // 期望二者编码是否相等
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
	// 复合键：各分量分别归一
	if encodeKey([]any{int(1), "a"}) != encodeKey([]any{float64(1), "a"}) {
		t.Error("composite key: int/float64 segment should normalize")
	}
}

func mathNegZero() float64 {
	// 返回 -0.0，验证它与 0.0 归一到同一 key。
	var neg float64
	return -neg
}
