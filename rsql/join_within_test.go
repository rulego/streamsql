package rsql

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// 语法：WITHIN 两个位置、等价拼写、多级级联。
func TestParseJoinWithinSyntax(t *testing.T) {
	cases := []struct {
		name   string
		sql    string
		stages []types.StreamJoinStage
	}{
		{
			name: "within after alias (ksqlDB position)",
			sql:  "SELECT s.deviceId FROM tempStream AS s JOIN vibrationStream AS v WITHIN 30 SECONDS ON s.deviceId = v.deviceId",
			stages: []types.StreamJoinStage{{
				RightName: "vibrationStream", RightAlias: "v", JoinType: "INNER",
				OnPairs: []types.JoinOnPair{{StreamField: "deviceId", TableField: "deviceId"}},
				Within:  30 * time.Second,
			}},
		},
		{
			name: "within parenthesized",
			sql:  "SELECT s.k FROM a AS s JOIN b AS v WITHIN (30 SECONDS) ON s.k = v.k",
			stages: []types.StreamJoinStage{{
				RightName: "b", RightAlias: "v", JoinType: "INNER",
				OnPairs: []types.JoinOnPair{{StreamField: "k", TableField: "k"}},
				Within:  30 * time.Second,
			}},
		},
		{
			name: "within string duration",
			sql:  "SELECT s.k FROM a AS s JOIN b AS v WITHIN '30s' ON s.k = v.k",
			stages: []types.StreamJoinStage{{
				RightName: "b", RightAlias: "v", JoinType: "INNER",
				OnPairs: []types.JoinOnPair{{StreamField: "k", TableField: "k"}},
				Within:  30 * time.Second,
			}},
		},
		{
			name: "within milliseconds",
			sql:  "SELECT s.k FROM a AS s JOIN b AS v WITHIN 100 MS ON s.k = v.k",
			stages: []types.StreamJoinStage{{
				RightName: "b", RightAlias: "v", JoinType: "INNER",
				OnPairs: []types.JoinOnPair{{StreamField: "k", TableField: "k"}},
				Within:  100 * time.Millisecond,
			}},
		},
		{
			name: "within after ON (postfix position)",
			sql:  "SELECT s.k FROM a AS s JOIN b AS v ON s.k = v.k WITHIN 15 SECONDS",
			stages: []types.StreamJoinStage{{
				RightName: "b", RightAlias: "v", JoinType: "INNER",
				OnPairs: []types.JoinOnPair{{StreamField: "k", TableField: "k"}},
				Within:  15 * time.Second,
			}},
		},
		{
			name: "left join within",
			sql:  "SELECT c.cmdId FROM cmdStream AS c LEFT JOIN ackStream AS r WITHIN 10 SECONDS ON c.cmdId = r.cmdId",
			stages: []types.StreamJoinStage{{
				RightName: "ackStream", RightAlias: "r", JoinType: "LEFT",
				OnPairs: []types.JoinOnPair{{StreamField: "cmdId", TableField: "cmdId"}},
				Within:  10 * time.Second,
			}},
		},
		{
			name: "bare table name within (no alias)",
			sql:  "SELECT k FROM a JOIN b WITHIN 5 SECONDS ON a.k = b.k",
			stages: []types.StreamJoinStage{{
				RightName: "b", RightAlias: "b", JoinType: "INNER",
				OnPairs: []types.JoinOnPair{{StreamField: "a.k", TableField: "k"}},
				Within:  5 * time.Second,
			}},
		},
		{
			name: "composite key within",
			sql:  "SELECT d.gateId FROM cardStream AS d JOIN faceStream AS f WITHIN 5 SECONDS ON d.gateId = f.gateId AND d.userId = f.userId",
			stages: []types.StreamJoinStage{{
				RightName: "faceStream", RightAlias: "f", JoinType: "INNER",
				OnPairs: []types.JoinOnPair{{StreamField: "gateId", TableField: "gateId"}, {StreamField: "userId", TableField: "userId"}},
				Within:  5 * time.Second,
			}},
		},
		{
			name: "three-stream cascade, per-stage within",
			sql:  "SELECT a.k FROM a JOIN b WITHIN 5 SECONDS ON a.k = b.k JOIN c WITHIN 3 SECONDS ON b.k2 = c.k2",
			stages: []types.StreamJoinStage{
				{RightName: "b", RightAlias: "b", JoinType: "INNER", OnPairs: []types.JoinOnPair{{StreamField: "a.k", TableField: "k"}}, Within: 5 * time.Second},
				{RightName: "c", RightAlias: "c", JoinType: "INNER", OnPairs: []types.JoinOnPair{{StreamField: "b.k2", TableField: "k2"}}, Within: 3 * time.Second},
			},
		},
		{
			name: "cascade mixed INNER then LEFT",
			sql:  "SELECT a.k FROM a JOIN b WITHIN 5 SECONDS ON a.k = b.k LEFT JOIN c WITHIN 3 SECONDS ON b.k2 = c.k2",
			stages: []types.StreamJoinStage{
				{RightName: "b", RightAlias: "b", JoinType: "INNER", OnPairs: []types.JoinOnPair{{StreamField: "a.k", TableField: "k"}}, Within: 5 * time.Second},
				{RightName: "c", RightAlias: "c", JoinType: "LEFT", OnPairs: []types.JoinOnPair{{StreamField: "b.k2", TableField: "k2"}}, Within: 3 * time.Second},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, _, err := Parse(c.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if cfg.Mode != types.ExecStreamJoin {
				t.Fatalf("mode = %v, want ExecStreamJoin", cfg.Mode)
			}
			if cfg.StreamJoin == nil {
				t.Fatalf("StreamJoin config is nil")
			}
			if got := cfg.StreamJoin.Stages; len(got) != len(c.stages) {
				t.Fatalf("stages = %+v, want %+v", got, c.stages)
			} else {
				for i := range got {
					if !reflect.DeepEqual(got[i], c.stages[i]) {
						t.Errorf("stage[%d] = %+v, want %+v", i, got[i], c.stages[i])
					}
				}
			}
		})
	}
}

// 语法：WITHIN 存在时 Inputs = [FROM, JOIN1, ...]；TsProp/IdleTimeout 从 WITH 子句透传。
func TestStreamJoinConfigInputsAndWithClause(t *testing.T) {
	cfg, _, err := Parse("SELECT s.k FROM canStream AS can JOIN gpsStream AS gps WITHIN 5 SECONDS ON can.vin = gps.vin WITH (TIMESTAMP = 'ts', IDLETIMEOUT = '40s')")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sj := cfg.StreamJoin
	if sj == nil {
		t.Fatal("StreamJoin nil")
	}
	wantInputs := []string{"canStream", "gpsStream"}
	if len(sj.Inputs) != len(wantInputs) {
		t.Fatalf("inputs = %v, want %v", sj.Inputs, wantInputs)
	}
	for i, n := range wantInputs {
		if sj.Inputs[i] != n {
			t.Errorf("inputs[%d] = %q, want %q", i, sj.Inputs[i], n)
		}
	}
	if sj.TsProp != "ts" {
		t.Errorf("TsProp = %q, want ts", sj.TsProp)
	}
	if sj.IdleTimeout != 40*time.Second {
		t.Errorf("IdleTimeout = %v, want 40s", sj.IdleTimeout)
	}
	// 流-流 JOIN 模式不携带 JoinConfigs（那是流-表富化路径的配置）。
	if len(cfg.JoinConfigs) != 0 {
		t.Errorf("JoinConfigs = %+v, want empty in stream-join mode", cfg.JoinConfigs)
	}
}

// 不带 WITHIN = 流表 JOIN（现状不变）。
func TestParseJoinNoWithinStaysTableJoin(t *testing.T) {
	cfg, _, err := Parse("SELECT s.deviceId, m.location FROM stream AS s JOIN devices AS m ON s.deviceId = m.deviceId")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Mode == types.ExecStreamJoin {
		t.Error("mode should not be ExecStreamJoin without WITHIN")
	}
	if cfg.StreamJoin != nil {
		t.Error("StreamJoin should be nil without WITHIN")
	}
	if len(cfg.JoinConfigs) != 1 || cfg.JoinConfigs[0].Within != 0 {
		t.Errorf("JoinConfigs = %+v, want single entry with Within=0", cfg.JoinConfigs)
	}
}

func TestParseJoinWithinErrors(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"both positions", "SELECT s.k FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k WITHIN 5 SECONDS", "twice"},
		{"zero duration", "SELECT s.k FROM a AS s JOIN b AS v WITHIN 0 SECONDS ON s.k = v.k", "positive"},
		{"negative string", "SELECT s.k FROM a AS s JOIN b AS v WITHIN '-5s' ON s.k = v.k", "positive"},
		{"bad unit", "SELECT s.k FROM a AS s JOIN b AS v WITHIN 5 FURLONGS ON s.k = v.k", "unit"},
		{"right join", "SELECT s.k FROM a AS s RIGHT JOIN b ON s.k = b.k", "RIGHT"},
		{"full join", "SELECT s.k FROM a AS s FULL JOIN b ON s.k = b.k", "FULL"},
		{"cross join", "SELECT s.k FROM a AS s CROSS JOIN b", "CROSS"},
		{"right outer join", "SELECT s.k FROM a AS s RIGHT OUTER JOIN b ON s.k = b.k", "RIGHT"},
		{"unclosed paren", "SELECT s.k FROM a AS s JOIN b AS v WITHIN (5 SECONDS ON s.k = v.k", "close WITHIN"},
		{"no duration", "SELECT s.k FROM a AS s JOIN b AS v WITHIN ON s.k = v.k", "WITHIN duration"},
		{"number without unit", "SELECT s.k FROM a AS s JOIN b AS v WITHIN 30 ON s.k = v.k", "unit"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := Parse(c.sql)
			if err == nil {
				t.Fatalf("expected parse error, got nil")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not contain %q", err.Error(), c.want)
			}
		})
	}
}

// 编译期校验清单：每条"不支持"组合必须报错。
func TestStreamJoinValidationMatrix(t *testing.T) {
	base := func(suffix string) string {
		return "SELECT s.k FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k" + suffix
	}
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"group by window", "SELECT s.k FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k GROUP BY s.k, TumblingWindow('5s')", "GROUP BY"},
		{"aggregation", "SELECT count(s.k) FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k", "aggregat"},
		{"having", base(" HAVING s.k > 1"), "HAVING"},
		{"distinct", "SELECT DISTINCT s.k FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k", "DISTINCT"},
		{"limit", base(" LIMIT 10"), "LIMIT"},
		{"analytic select", "SELECT lag(s.k) FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k", "analytic"},
		{"maxoutoforderness", base(" WITH (TIMESTAMP = 'ts', MAXOUTOFORDERNESS = '1s')"), "MaxOutOfOrderness"},
		{"allowedlateness", base(" WITH (TIMESTAMP = 'ts', ALLOWEDLATENESS = '1s')"), "AllowedLateness"},
		{"mixed with table join", "SELECT s.k FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k JOIN m ON s.k = m.k", "mix"},
		{"match_recognize", "SELECT s.k FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k MATCH_RECOGNIZE (PATTERN (A) DEFINE A AS s.k > 1)", "MATCH_RECOGNIZE"},
		{"unnest", "SELECT unnest(s.tags) FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k", "unnest"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := Parse(c.sql)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not contain %q", err.Error(), c.want)
			}
		})
	}
}

// 字面量里的 "unnest(" 不触发 JOIN 校验（stripStringLiterals 防误判）。
func TestStreamJoinUnnestInStringLiteralAllowed(t *testing.T) {
	cfg, _, err := Parse("SELECT s.k FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k WHERE s.note = 'call unnest(x) later'")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Mode != types.ExecStreamJoin {
		t.Errorf("mode = %v", cfg.Mode)
	}
}

// 允许项：IdleTimeout 与 ORDER BY 与 WITHIN JOIN 组合不报错。
func TestStreamJoinAllowedCombinations(t *testing.T) {
	cfg, _, err := Parse("SELECT s.k FROM a AS s JOIN b AS v WITHIN 5 SECONDS ON s.k = v.k WITH (TIMESTAMP = 'ts', IDLETIMEOUT = '30s') ORDER BY s.k")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Mode != types.ExecStreamJoin {
		t.Errorf("mode = %v", cfg.Mode)
	}
	if len(cfg.OrderBy) != 1 {
		t.Errorf("OrderBy = %+v", cfg.OrderBy)
	}
}
