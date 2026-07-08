package e2e

import (
	"testing"

	"github.com/rulego/streamsql"
)

// TestSelectAllAsterisk 锁定 SELECT * 行为：返回输入行的全部字段。
// 回归保护：parser 曾把 * 误判为 TokenIdent（实为 TokenAsterisk），
// 导致 SelectAll 标志永不置真（死代码）。修复后 SELECT * 仍应输出所有字段。
func TestSelectAllAsterisk(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT * FROM stream"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	in := map[string]interface{}{"deviceId": "d1", "temperature": 35.5, "humidity": 60}
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

// TestSelectAllWithWhere 验证 SELECT * 与 WHERE 共存：先过滤再输出全部字段。
func TestSelectAllWithWhere(t *testing.T) {
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute("SELECT * FROM stream WHERE temperature > 30"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	pass := map[string]interface{}{"deviceId": "d1", "temperature": 35, "humidity": 60}
	got, _ := ssql.EmitSync(pass)
	if got == nil || len(got) != 3 {
		t.Errorf("matching SELECT * got=%v, want 3 fields", got)
	}
	drop := map[string]interface{}{"deviceId": "d2", "temperature": 20}
	got, _ = ssql.EmitSync(drop)
	if got != nil {
		t.Errorf("filtered row got=%v, want nil", got)
	}
}
