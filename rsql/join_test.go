package rsql

import (
	"testing"

	"github.com/rulego/streamsql/types"
)

func TestParseFromAlias(t *testing.T) {
	cases := []struct {
		name  string
		sql   string
		alias string
		joins int
	}{
		{"no alias", "SELECT a FROM stream", "", 0},
		{"AS alias", "SELECT a FROM stream AS s", "s", 0},
		{"implicit alias", "SELECT a FROM stream s", "s", 0},
		{"alias then where", "SELECT a FROM stream s WHERE x > 1", "s", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, _, err := Parse(c.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if cfg.SourceAlias != c.alias {
				t.Errorf("alias = %q, want %q", cfg.SourceAlias, c.alias)
			}
			if len(cfg.JoinConfigs) != c.joins {
				t.Errorf("joins = %d, want %d", len(cfg.JoinConfigs), c.joins)
			}
		})
	}
}

func TestParseJoin(t *testing.T) {
	cfg, _, err := Parse("SELECT s.deviceId, m.location FROM stream s JOIN meta m ON s.deviceId = m.deviceId")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.JoinConfigs) != 1 {
		t.Fatalf("joins = %d, want 1", len(cfg.JoinConfigs))
	}
	jc := cfg.JoinConfigs[0]
	if jc.Table != "meta" || jc.Alias != "m" || jc.JoinType != "INNER" {
		t.Errorf("join = %+v", jc)
	}
	if len(jc.OnPairs) != 1 || jc.OnPairs[0] != (types.JoinOnPair{StreamField: "deviceId", TableField: "deviceId"}) {
		t.Errorf("onpairs = %+v", jc.OnPairs)
	}
}

func TestParseLeftJoinAndCompositeKey(t *testing.T) {
	cfg, cond, err := Parse("SELECT a FROM stream s LEFT JOIN meta m ON s.k1 = m.k1 AND s.k2 = m.k2 WHERE a > 0")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.JoinConfigs) != 1 {
		t.Fatalf("joins = %d, want 1", len(cfg.JoinConfigs))
	}
	jc := cfg.JoinConfigs[0]
	if jc.JoinType != "LEFT" {
		t.Errorf("joinType = %q, want LEFT", jc.JoinType)
	}
	want := []types.JoinOnPair{{"k1", "k1"}, {"k2", "k2"}}
	if len(jc.OnPairs) != 2 {
		t.Fatalf("onpairs len = %d, want 2", len(jc.OnPairs))
	}
	for i, p := range jc.OnPairs {
		if p != want[i] {
			t.Errorf("onpair[%d] = %+v, want %+v", i, p, want[i])
		}
	}
	if cond == "" {
		t.Error("WHERE condition lost after JOIN")
	}
}

func TestParseDefaultAliasAndBareJoin(t *testing.T) {
	// No table alias -> defaults to table name; bare JOIN == INNER.
	cfg, _, err := Parse("SELECT m.location FROM stream JOIN meta ON deviceId = deviceId")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	jc := cfg.JoinConfigs[0]
	if jc.Alias != "meta" {
		t.Errorf("default alias = %q, want meta", jc.Alias)
	}
	if jc.JoinType != "INNER" {
		t.Errorf("bare join type = %q, want INNER", jc.JoinType)
	}
}
