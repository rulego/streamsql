package expr

import "testing"

// TestTokenizerBinaryMinus (H12): "expr-digit" must not glue the '-' into a
// negative-number token. Otherwise the parser silently truncates the trailing
// arithmetic (e.g. price*2-1 -> 200 instead of 199).
func TestTokenizerBinaryMinus(t *testing.T) {
	cases := []struct {
		expr string
		want []string
	}{
		{"price*2-1", []string{"price", "*", "2", "-", "1"}},
		{"5-3", []string{"5", "-", "3"}},
		{"2-3-4", []string{"2", "-", "3", "-", "4"}},
		{"a+10-1", []string{"a", "+", "10", "-", "1"}},
		{"(a-b)-1", []string{"(", "a", "-", "b", ")", "-", "1"}},
	}
	for _, c := range cases {
		got, err := tokenize(c.expr)
		if err != nil {
			t.Fatalf("tokenize(%q) error: %v", c.expr, err)
		}
		if !equalStrings(got, c.want) {
			t.Errorf("tokenize(%q) = %v, want %v", c.expr, got, c.want)
		}
	}
}

// Negative sign must still be recognized when it is unary (start of expr or
// after an operator / open paren).
func TestTokenizerUnaryMinus(t *testing.T) {
	cases := []struct {
		expr string
		want []string
	}{
		{"-5", []string{"-5"}},
		{"a+-5", []string{"a", "+", "-5"}},
		{"a*(-5)", []string{"a", "*", "(", "-5", ")"}},
		{"(-5+3)", []string{"(", "-5", "+", "3", ")"}},
	}
	for _, c := range cases {
		got, err := tokenize(c.expr)
		if err != nil {
			t.Fatalf("tokenize(%q) error: %v", c.expr, err)
		}
		if !equalStrings(got, c.want) {
			t.Errorf("tokenize(%q) = %v, want %v", c.expr, got, c.want)
		}
	}
}

// TestEvaluateArithmeticSubtraction (H12): end-to-end, subtraction must apply.
func TestEvaluateArithmeticSubtraction(t *testing.T) {
	cases := []struct {
		expr string
		data map[string]any
		want float64
	}{
		{"price*2-1", map[string]any{"price": float64(100)}, 199},
		{"5-3", nil, 2},
		{"2-3-4", nil, -5},
		{"a+10-1", map[string]any{"a": float64(5)}, 14},
	}
	for _, c := range cases {
		e, err := NewExpression(c.expr)
		if err != nil {
			t.Fatalf("NewExpression(%q) error: %v", c.expr, err)
		}
		got, err := e.Evaluate(c.data)
		if err != nil {
			t.Fatalf("Evaluate(%q) error: %v", c.expr, err)
		}
		if got != c.want {
			t.Errorf("Evaluate(%q) = %v, want %v", c.expr, got, c.want)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
