package e2e

import (
	"testing"

	"github.com/rulego/streamsql"
)

// TestSelectAllAsterisk Lock SELECT * behavior: Returns all fields in the input row.
// Regression protection: parser once misjudged * as TokenIdent (actually TokenAsterisk),
// This causes the SelectAll flag to never be true (dead code). After fixing, SELECT * should still output all fields.
func TestSelectAllAsterisk(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT * FROM stream"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	in := map[string]any{"deviceId": "d1", "temperature": 35.5, "humidity": 60}
	got, err := ssql.EmitSync(in)
	if err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got == nil {
		t.Fatal("SELECT * returned nil, want all fields")
	}
	if len(got) != len(in) {
		t.Fatalf("SELECT * field count = %d, want %d (got=%v)", len(got), len(in), got)
	}
	for k, v := range in {
		if got[k] != v {
			t.Errorf("SELECT * field %s = %v, want %v", k, got[k], v)
		}
	}
}

// TestSelectAllWithWhere Verify that SELECT * coexists with WHERE: filter first, then output all fields.
func TestSelectAllWithWhere(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT * FROM stream WHERE temperature > 30"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	pass := map[string]any{"deviceId": "d1", "temperature": 35, "humidity": 60}
	got, _ := ssql.EmitSync(pass)
	if got == nil || len(got) != 3 {
		t.Errorf("matching SELECT * got=%v, want 3 fields", got)
	}
	drop := map[string]any{"deviceId": "d2", "temperature": 20}
	got, _ = ssql.EmitSync(drop)
	if got != nil {
		t.Errorf("filtered row got=%v, want nil", got)
	}
}
