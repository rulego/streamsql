package e2e

import "testing"

// 验证《模式识别》文档（rulego-doc/03.StreamSQL/15.模式识别.md + 31.案例集锦/07）的
// 场景 SQL 可 Execute 且匹配数符合文档预期，防文档 SQL 与实现漂移。每场景对应文档一个子场景。

// 场景 A：连续越限防抖 A{3}。
func TestDocCEP_A_ConsecutiveThreshold(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			PARTITION BY deviceId ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn, COUNT(*) AS hits, LAST(A.temp) AS peak
			ONE ROW PER MATCH
			PATTERN (A{3})
			WITHIN '1h'
			DEFINE A AS temp > 80
		)`
	rows := []map[string]any{
		{"deviceId": "dev-01", "ts": 1, "temp": 60},
		{"deviceId": "dev-01", "ts": 2, "temp": 82},
		{"deviceId": "dev-01", "ts": 3, "temp": 85},
		{"deviceId": "dev-01", "ts": 4, "temp": 88},
		{"deviceId": "dev-02", "ts": 5, "temp": 90},
		{"deviceId": "dev-02", "ts": 6, "temp": 91},
	}
	got := collectCEP(t, sql, rows, 1)
	if len(flatten(got)) != 1 {
		t.Fatalf("A: want 1 match, got %d", len(flatten(got)))
	}
}

// 场景 B：升温后骤降 A+ B（符号限定字段 B.temp + 聚合 MAX(A.temp)）。
func TestDocCEP_B_RiseThenDrop(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			PARTITION BY deviceId ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn, MAX(A.temp) AS peak, B.temp AS drop_to
			ONE ROW PER MATCH
			PATTERN (A+ B)
			WITHIN '1h'
			DEFINE A AS temp > 80, B AS temp < 30
		)`
	rows := []map[string]any{
		{"deviceId": "dev-01", "ts": 1, "temp": 85},
		{"deviceId": "dev-01", "ts": 2, "temp": 90},
		{"deviceId": "dev-01", "ts": 3, "temp": 95},
		{"deviceId": "dev-01", "ts": 4, "temp": 25},
	}
	got := collectCEP(t, sql, rows, 1)
	if len(flatten(got)) != 1 {
		t.Fatalf("B: want 1 match, got %d", len(flatten(got)))
	}
}

// 场景 C：振动突发 A{5,}。
func TestDocCEP_C_VibrationBurst(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			PARTITION BY deviceId ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn, COUNT(*) AS bursts, MAX(A.amp) AS max_amp
			ONE ROW PER MATCH
			PATTERN (A{5,})
			WITHIN '1h'
			DEFINE A AS amp > 50
		)`
	rows := []map[string]any{
		{"deviceId": "dev-01", "ts": 1, "amp": 60},
		{"deviceId": "dev-01", "ts": 2, "amp": 65},
		{"deviceId": "dev-01", "ts": 3, "amp": 70},
		{"deviceId": "dev-01", "ts": 4, "amp": 62},
		{"deviceId": "dev-01", "ts": 5, "amp": 68},
		{"deviceId": "dev-01", "ts": 6, "amp": 71},
		{"deviceId": "dev-01", "ts": 7, "amp": 40},
	}
	got := collectCEP(t, sql, rows, 1)
	if len(flatten(got)) != 1 {
		t.Fatalf("C: want 1 match, got %d", len(flatten(got)))
	}
}

// 场景 D：开停机工作流——多字符符号名 Start/Running/Stop。
func TestDocCEP_D_StartRunningStop(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			PARTITION BY deviceId ORDER BY ts
			MEASURES MATCH_NUMBER() AS cycle, MAX(Running.power) AS peak_power
			ONE ROW PER MATCH
			PATTERN (Start Running+ Stop)
			WITHIN '24h'
			DEFINE Start AS type == "start", Running AS type == "running", Stop AS type == "stop"
		)`
	rows := []map[string]any{
		{"deviceId": "dev-01", "ts": 1, "type": "start", "power": 0},
		{"deviceId": "dev-01", "ts": 2, "type": "running", "power": 120},
		{"deviceId": "dev-01", "ts": 3, "type": "running", "power": 150},
		{"deviceId": "dev-01", "ts": 4, "type": "stop", "power": 0},
	}
	got := collectCEP(t, sql, rows, 1)
	if len(flatten(got)) != 1 {
		t.Fatalf("D: want 1 match, got %d", len(flatten(got)))
	}
}

// 场景 E：乱序鉴权 PERMUTE(Login, Auth)，两会话各一次 = 2。
func TestDocCEP_E_PermuteAuth(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			PARTITION BY sessionId ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn, FIRST(Login.ts) AS t1, FIRST(Auth.ts) AS t2
			ONE ROW PER MATCH
			PATTERN (PERMUTE(Login, Auth))
			WITHIN '10m'
			DEFINE Login AS event == "login", Auth AS event == "auth"
		)`
	rows := []map[string]any{
		{"sessionId": "s1", "ts": 1, "event": "login"},
		{"sessionId": "s1", "ts": 2, "event": "auth"},
		{"sessionId": "s2", "ts": 3, "event": "auth"},
		{"sessionId": "s2", "ts": 4, "event": "login"},
	}
	got := collectCEP(t, sql, rows, 2)
	if len(flatten(got)) != 2 {
		t.Fatalf("E: want 2 matches, got %d", len(flatten(got)))
	}
}

// 场景 F：时间约束——Alert 后 30s 内 Ack。dev-01 间隔 10s 匹配，dev-02 间隔 60s 超窗。
func TestDocCEP_F_WithinConfirm(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			PARTITION BY deviceId ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn, Alert.ts AS alert_at, Ack.ts AS ack_at
			ONE ROW PER MATCH
			PATTERN (Alert Ack)
			WITHIN '30s'
			DEFINE Alert AS event == "alert", Ack AS event == "ack"
		)`
	rows := []map[string]any{
		{"deviceId": "dev-01", "ts": 1700000000000, "event": "alert"},
		{"deviceId": "dev-01", "ts": 1700000010000, "event": "ack"},
		{"deviceId": "dev-02", "ts": 1700000020000, "event": "alert"},
		{"deviceId": "dev-02", "ts": 1700000080000, "event": "ack"},
	}
	got := collectCEP(t, sql, rows, 1)
	if len(flatten(got)) != 1 {
		t.Fatalf("F: want 1 match, got %d", len(flatten(got)))
	}
}
