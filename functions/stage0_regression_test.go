package functions

import "testing"

// TestSubstringMultibyte (M14): substring must slice by rune, not byte, so
// multibyte UTF-8 is not split into invalid fragments.
func TestSubstringMultibyte(t *testing.T) {
	fn, ok := Get("substring")
	if !ok {
		t.Fatal("substring not found")
	}

	// 你好世界 = runes [你 好 世 界]; substring(s, 1, 2) -> "好世".
	got, err := fn.Execute(nil, []interface{}{"你好世界", int64(1), int64(2)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "好世" {
		t.Errorf("substring(你好世界,1,2) = %q, want %q", got, "好世")
	}

	// Two-arg form from the middle.
	got, err = fn.Execute(nil, []interface{}{"你好世界", int64(2)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "世界" {
		t.Errorf("substring(你好世界,2) = %q, want %q", got, "世界")
	}

	// ASCII still works (rune count == byte count).
	got, err = fn.Execute(nil, []interface{}{"abcdef", int64(1), int64(3)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "bcd" {
		t.Errorf("substring(abcdef,1,3) = %q, want %q", got, "bcd")
	}
}

// TestDateAddOverflow (M12): a large hour/minute/second interval must error
// instead of silently wrapping to a wrong (even past) date.
func TestDateAddOverflow(t *testing.T) {
	addFn, ok := Get("date_add")
	if !ok {
		t.Fatal("date_add not found")
	}
	subFn, ok := Get("date_sub")
	if !ok {
		t.Fatal("date_sub not found")
	}

	// 3,000,000 hours overflows time.Duration (~292 years); must error.
	if _, err := addFn.Execute(nil, []interface{}{"2020-01-01 00:00:00", int64(3000000), "hour"}); err == nil {
		t.Error("date_add overflow expected error, got nil")
	}
	if _, err := subFn.Execute(nil, []interface{}{"2020-01-01 00:00:00", int64(3000000), "hour"}); err == nil {
		t.Error("date_sub overflow expected error, got nil")
	}

	// In-range interval still works.
	got, err := addFn.Execute(nil, []interface{}{"2020-01-01 00:00:00", int64(25), "hour"})
	if err != nil {
		t.Fatalf("date_add in-range error: %v", err)
	}
	if got != "2020-01-02 01:00:00" {
		t.Errorf("date_add 25h = %q, want %q", got, "2020-01-02 01:00:00")
	}
}
