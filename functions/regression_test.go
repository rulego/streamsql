package functions

import (
	"sync"
	"testing"
	"time"
)

// TestSubstringMultibyte (M14): substring must slice by rune, not byte, so
// multibyte UTF-8 is not split into invalid fragments.
func TestSubstringMultibyte(t *testing.T) {
	fn, ok := Get("substring")
	if !ok {
		t.Fatal("substring not found")
	}

	// 你好世界 = runes [你 好 世 界]; substring(s, 1, 2) -> "好世".
	got, err := fn.Execute(nil, []any{"你好世界", int64(1), int64(2)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "好世" {
		t.Errorf("substring(你好世界,1,2) = %q, want %q", got, "好世")
	}

	// Two-arg form from the middle.
	got, err = fn.Execute(nil, []any{"你好世界", int64(2)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "世界" {
		t.Errorf("substring(你好世界,2) = %q, want %q", got, "世界")
	}

	// ASCII still works (rune count == byte count).
	got, err = fn.Execute(nil, []any{"abcdef", int64(1), int64(3)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "bcd" {
		t.Errorf("substring(abcdef,1,3) = %q, want %q", got, "bcd")
	}
}

// TestDateFormatMinutePlaceholder (M13): lowercase "mm" renders minutes and
// must not be overwritten by the month value; uppercase "MM" still renders month.
func TestDateFormatMinutePlaceholder(t *testing.T) {
	fn, ok := Get("date_format")
	if !ok {
		t.Fatal("date_format not found")
	}

	got, err := fn.Execute(nil, []any{"2020-03-05 10:20:30", "YYYY-MM-DD HH:mm:ss"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Minute is 20; the bug rendered 03 (month) over it.
	if got != "2020-03-05 10:20:30" {
		t.Errorf("date_format mm = %q, want %q (minute must be 20)", got, "2020-03-05 10:20:30")
	}

	// MM still renders the month.
	got, err = fn.Execute(nil, []any{"2020-03-05 10:20:30", "YYYY-MM-DD"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "2020-03-05" {
		t.Errorf("date_format MM = %q, want %q", got, "2020-03-05")
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
	if _, err := addFn.Execute(nil, []any{"2020-01-01 00:00:00", int64(3000000), "hour"}); err == nil {
		t.Error("date_add overflow expected error, got nil")
	}
	if _, err := subFn.Execute(nil, []any{"2020-01-01 00:00:00", int64(3000000), "hour"}); err == nil {
		t.Error("date_sub overflow expected error, got nil")
	}

	// In-range interval still works.
	got, err := addFn.Execute(nil, []any{"2020-01-01 00:00:00", int64(25), "hour"})
	if err != nil {
		t.Fatalf("date_add in-range error: %v", err)
	}
	if got != "2020-01-02 01:00:00" {
		t.Errorf("date_add 25h = %q, want %q", got, "2020-01-02 01:00:00")
	}
}

// TestIsNullTypedNil (M9): is_null/is_not_null must treat typed-nil pointers
// (e.g. (*int)(nil)) as NULL. Go's == nil does not, so a reflect-based check is
// required. Without the fix, is_null((*int)(nil)) wrongly returns false.
func TestIsNullTypedNil(t *testing.T) {
	isNull, ok := Get("is_null")
	if !ok {
		t.Fatal("is_null not found")
	}
	isNotNull, ok := Get("is_not_null")
	if !ok {
		t.Fatal("is_not_null not found")
	}

	cases := []struct {
		name     string
		value    any
		wantNull bool
	}{
		{"untyped nil", nil, true},
		{"typed-nil pointer", (*int)(nil), true},
		{"typed-nil slice", ([]int)(nil), true},
		{"typed-nil map", (map[string]int)(nil), true},
		{"non-nil pointer", new(int), false},
		{"zero int", 0, false},
		{"empty string", "", false},
	}
	for _, c := range cases {
		got, err := isNull.Execute(nil, []any{c.value})
		if err != nil {
			t.Fatalf("is_null(%s) error: %v", c.name, err)
		}
		if got != c.wantNull {
			t.Errorf("is_null(%s) = %v, want %v", c.name, got, c.wantNull)
		}
		got2, err := isNotNull.Execute(nil, []any{c.value})
		if err != nil {
			t.Fatalf("is_not_null(%s) error: %v", c.name, err)
		}
		if got2 != !c.wantNull {
			t.Errorf("is_not_null(%s) = %v, want %v", c.name, got2, !c.wantNull)
		}
	}
}

// TestPercentileScalar (M10): PercentileFunction.Execute must use args[0] as the
// percentile p (in [0,1]) and sort/index only args[1:], with index clamping.
// Before the fix, p was sorted into the data and the index was unclamped, both
// corrupting the result and panicking on out-of-range p.
func TestPercentileScalar(t *testing.T) {
	fn, ok := Get("percentile")
	if !ok {
		t.Fatal("percentile not found")
	}

	// Median of 10,20,30 -> 20.
	got, err := fn.Execute(nil, []any{0.5, int64(10), int64(20), int64(30)})
	if err != nil {
		t.Fatalf("percentile median error: %v", err)
	}
	if got != float64(20) {
		t.Errorf("percentile(0.5,10,20,30) = %v, want 20", got)
	}

	// Min (p=0) and max (p=1) clamp correctly.
	if got, _ := fn.Execute(nil, []any{0.0, int64(10), int64(20), int64(30)}); got != float64(10) {
		t.Errorf("percentile(0,...) = %v, want 10", got)
	}
	if got, _ := fn.Execute(nil, []any{1.0, int64(10), int64(20), int64(30)}); got != float64(30) {
		t.Errorf("percentile(1,...) = %v, want 30", got)
	}

	// Out-of-range p must error instead of panicking.
	if _, err := fn.Execute(nil, []any{2.0, int64(10), int64(20)}); err == nil {
		t.Error("percentile p=2.0 must error")
	}

	// Degenerate 2-arg form (single value) returns that value, no panic.
	if got, err := fn.Execute(nil, []any{0.5, int64(42)}); err != nil || got != float64(42) {
		t.Errorf("percentile(0.5,42) = %v err=%v, want 42", got, err)
	}
}

// TestRowNumberConcurrent (M11): concurrent Execute calls on the shared
// row_number instance must not race (run under -race to catch it) and must
// produce exactly N distinct increments.
func TestRowNumberConcurrent(t *testing.T) {
	fn, ok := Get("row_number")
	if !ok {
		t.Fatal("row_number not found")
	}
	rf, ok := fn.(*RowNumberFunction)
	if !ok {
		t.Fatal("row_number is not *RowNumberFunction")
	}
	rf.Reset()

	const n = 200
	var wg sync.WaitGroup
	seen := make(map[int64]bool)
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := fn.Execute(nil, nil)
			if err != nil {
				t.Errorf("Execute error: %v", err)
				return
			}
			mu.Lock()
			seen[v.(int64)] = true
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(seen) != n {
		t.Errorf("row_number produced %d distinct values under concurrency, want %d (lost increments = data race)", len(seen), n)
	}
}

// TestLikeMatcherNoBacktracking (M16): the LIKE matcher must complete quickly on
// an adversarial pattern that caused exponential backtracking before the fix
// (the old implementation took ~2min at n=60). It runs in a goroutine with a
// timeout so a regression fails the test instead of hanging CI.
func TestLikeMatcherNoBacktracking(t *testing.T) {
	bridge := GetExprBridge()

	// Build pattern "%a%a%a...%a%b" (k '%' segments) against text of 'a's.
	buildCase := func(k int) (text, pattern string) {
		txt := make([]byte, k)
		pat := make([]byte, 0, k*2+2)
		for i := 0; i < k; i++ {
			txt[i] = 'a'
			pat = append(pat, '%', 'a')
		}
		pat = append(pat, '%', 'b')
		return string(txt), string(pat)
	}

	type result struct {
		matched bool
	}
	done := make(chan result, 1)
	go func() {
		text, pattern := buildCase(60)
		done <- result{matched: bridge.matchesLikePattern(text, pattern)}
	}()

	select {
	case r := <-done:
		if r.matched {
			t.Error("pattern ending with a literal-b wildcard must not match all-a text")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("LIKE matcher did not return within budget — exponential backtracking regression")
	}

	// Sanity: normal patterns still match correctly.
	cases := []struct {
		text, pattern string
		want          bool
	}{
		{"axbyc", "%a%c", true},
		{"axbc", "a_b%", true},
		{"abc", "%a%a%b", false},
		{"hello", "h%o", true},
		{"hello", "x%", false},
	}
	for _, c := range cases {
		if got := bridge.matchesLikePattern(c.text, c.pattern); got != c.want {
			t.Errorf("likeMatch(%q,%q) = %v, want %v", c.text, c.pattern, got, c.want)
		}
	}
}
