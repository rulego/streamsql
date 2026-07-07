package condition

import "testing"

// TestFastCompareMatchesExprLang verifies the fast-path produces identical
// results to the expr-lang VM across operators, numeric types, and string
// values. Any mismatch would be a semantic regression.
func TestFastCompareMatchesExprLang(t *testing.T) {
	conditions := []string{
		"temperature > 20",
		"temperature >= 20",
		"temperature < 30",
		"temperature <= 30",
		"cnt == 10",
		"cnt != 10",
		"status == 'active'",
		"status != 'inactive'",
		"label > 'm'",
	}
	rows := []map[string]interface{}{
		{"temperature": 25.5, "cnt": int64(10), "status": "active", "label": "zebra"},
		{"temperature": 20.0, "cnt": 9, "status": "inactive", "label": "apple"},
		{"temperature": 30, "cnt": int(11), "status": "active", "label": "m"},
		{"temperature": 19.9, "cnt": uint(10), "status": "", "label": "zzz"},
		{"temperature": "hot", "status": 1}, // wrong types -> must fall back
		{"humidity": 50},                    // field missing -> fall back
		{"temperature": float32(21), "cnt": 10},
	}

	for _, condStr := range conditions {
		ec, err := NewExprCondition(condStr)
		if err != nil {
			t.Fatalf("NewExprCondition(%q): %v", condStr, err)
		}
		// Build a reference condition with the fast path disabled.
		ref := &ExprCondition{program: ec.(*ExprCondition).program, fast: nil}
		for i, row := range rows {
			got := ec.Evaluate(row)
			want := ref.Evaluate(row)
			if got != want {
				t.Errorf("cond=%q row=%d (%v): fast=%v expr-lang=%v", condStr, i, row, got, want)
			}
		}
	}
}

// TestFastCompareNotAppliedToComplex ensures complex conditions disable the
// fast-path (fall back to expr-lang) so semantics are never guessed.
func TestFastCompareNotAppliedToComplex(t *testing.T) {
	complex := []string{
		"temperature > 20 AND humidity < 80",
		"temperature > 20 OR status = 'a'",
		"abs(temperature - 10) > 5",
		"temperature > 20 + 5",
		"(temperature) > 20",
		"temperature IN (1,2,3)",
	}
	for _, c := range complex {
		ec, err := NewExprCondition(c)
		if err != nil {
			// Some of these may not compile with these options; that's fine.
			continue
		}
		if ec.(*ExprCondition).fast != nil {
			t.Errorf("complex condition %q should not get a fast-path", c)
		}
	}
}
