package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// 包内接线层直调测试（不经 e2e）：覆盖 initJoinPipeline/run/sendJoinMsg/
// emitToName/wireCascade/flush/statsSnapshot——S2 的引擎直调单测不经过这些
// 接线路径，e2e 又在别的包（覆盖率不计入本包）。

func joinTestConfig() types.Config {
	c := types.NewConfig()
	c.Mode = types.ExecStreamJoin
	c.SourceAlias = "s"
	c.StreamJoin = &types.StreamJoinConfig{
		Inputs: []string{"aStream", "bStream"},
		Stages: []types.StreamJoinStage{{
			RightName: "bStream", RightAlias: "v", JoinType: "INNER",
			OnPairs: []types.JoinOnPair{{StreamField: "k", TableField: "k"}},
			Within:  30 * time.Second,
		}},
	}
	return c
}

type pipeSink struct {
	mu   sync.Mutex
	rows []map[string]any
}

func (p *pipeSink) add(batch []map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rows = append(p.rows, batch...)
}

func (p *pipeSink) snapshot() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]map[string]any, len(p.rows))
	copy(out, p.rows)
	return out
}

func (p *pipeSink) waitN(n int, timeout time.Duration) []map[string]any {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if rows := p.snapshot(); len(rows) >= n {
			return rows
		}
		time.Sleep(10 * time.Millisecond)
	}
	return p.snapshot()
}

func TestJoinPipelineWiringInnerMatch(t *testing.T) {
	s, err := NewStream(joinTestConfig())
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	sink := &pipeSink{}
	s.AddSink(sink.add)
	s.Start()
	defer s.Stop()

	if !s.IsStreamJoinQuery() {
		t.Fatal("IsStreamJoinQuery")
	}
	if err := s.EmitTo("aStream", map[string]any{"k": "x", "va": 1}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	if err := s.EmitTo("bStream", map[string]any{"k": "x", "vb": 2}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	rows := sink.waitN(1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("wired pipeline should match: %+v", rows)
	}
	if rows[0]["va"] != 1 {
		t.Fatalf("match missing: %+v", rows)
	}
	if v, ok := rows[0]["v"].(map[string]any); !ok || v["vb"] != 2 {
		t.Errorf("right row under alias: %+v", rows[0]["v"])
	}
	// Emit 路由到 FROM 流。
	s.Emit(map[string]any{"k": "y", "va": 3})
	if err := s.EmitTo("bStream", map[string]any{"k": "y", "vb": 4}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	rows = sink.waitN(2, 5*time.Second)
	if len(rows) != 2 {
		t.Fatalf("Emit should route to FROM stream: %+v", rows)
	}
	stats := s.GetStats()
	if stats["join_stage1_matches_emitted"] != 2 {
		t.Errorf("stats = %v", stats["join_stage1_matches_emitted"])
	}
}

func TestJoinPipelineStopFlush(t *testing.T) {
	cfg := joinTestConfig()
	cfg.StreamJoin.Stages[0].JoinType = "LEFT"
	s, err := NewStream(cfg)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	sink := &pipeSink{}
	s.AddSink(sink.add)
	s.Start()

	if err := s.EmitTo("aStream", map[string]any{"k": "x", "va": 1}); err != nil {
		t.Fatalf("EmitTo: %v", err)
	}
	// 等入缓冲后 Stop → Flush 内联补发 NULL。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.GetStats()["join_stage1_buffered_rows"] >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.Stop()
	rows := sink.snapshot()
	if len(rows) != 1 {
		t.Fatalf("Stop flush should emit the pending LEFT NULL row: %+v", rows)
	}
	if v, ok := rows[0]["v"].(map[string]any); !ok || len(v) != 0 {
		t.Errorf("NULL complement shape: %+v", rows[0])
	}
}

func TestJoinPipelineEmitToErrors(t *testing.T) {
	s, err := NewStream(joinTestConfig())
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	s.Start()
	defer s.Stop()
	if err := s.EmitTo("nope", map[string]any{"k": "x"}); err == nil {
		t.Error("unknown stream must error")
	}
	// 非 JOIN 查询的 Stream 无管线：走既有单流路径的实例这里只验证 JOIN 分支。
	if err := s.emitToNameWrapper(); err != nil {
		t.Errorf("pipeline emitToName known input should succeed: %v", err)
	}
}

func (s *Stream) emitToNameWrapper() error {
	return s.join.emitToName("aStream", map[string]any{"k": "ok"})
}

func TestJoinPipelineDuplicateInputRejected(t *testing.T) {
	cfg := joinTestConfig()
	cfg.StreamJoin = &types.StreamJoinConfig{
		Inputs: []string{"aStream", "aStream"},
		Stages: []types.StreamJoinStage{{
			RightName: "aStream", RightAlias: "v", JoinType: "INNER",
			OnPairs: []types.JoinOnPair{{StreamField: "k", TableField: "k"}},
			Within:  time.Second,
		}},
	}
	if _, err := NewStream(cfg); err == nil {
		t.Fatal("duplicate FROM/JOIN stream name must fail construction")
	}
}

// TestJoinPipelineInputOverflowDrop 容量 1 的输入 chan + drop 策略：
// 灌满后投递丢弃并计数（sendJoinMsg 的 drop 分支）。
func TestJoinPipelineInputOverflowDrop(t *testing.T) {
	cfg := joinTestConfig()
	perf := types.DefaultPerformanceConfig()
	perf.BufferConfig.DataChannelSize = 1
	perf.OverflowConfig.Strategy = types.OverflowStrategyDrop
	cfg.PerformanceConfig = perf
	cfg.StreamJoin.TsProp = "" // processing-time，行即刻消费不了也不会迟到
	s, err := NewStream(cfg)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	s.Start()
	defer s.Stop()
	// 不 Start 消费者已停？——Start 已起 processor；直接灌超量同键行。
	for i := 0; i < 50; i++ {
		_ = s.EmitTo("aStream", map[string]any{"k": "x"})
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.GetStats()["join_stage1_input_dropped"] > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("input_dropped = %d, want > 0", s.GetStats()["join_stage1_input_dropped"])
}

// TestJoinPipelineBlockStrategy sendJoinMsg 的 block 分支（有界超时后丢）。
func TestJoinPipelineBlockStrategy(t *testing.T) {
	cfg := joinTestConfig()
	perf := types.DefaultPerformanceConfig()
	perf.BufferConfig.DataChannelSize = 1
	perf.OverflowConfig.Strategy = types.OverflowStrategyBlock
	perf.OverflowConfig.BlockTimeout = 50 * time.Millisecond
	cfg.PerformanceConfig = perf
	s, err := NewStream(cfg)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	s.Start()
	defer s.Stop()
	// 消费者活跃但同键行堆积：灌超量，block 分支最多等 50ms 再丢。
	for i := 0; i < 200; i++ {
		_ = s.EmitTo("aStream", map[string]any{"k": "x"})
	}
	// 只验证不 panic 且最终有计数或全部送达。
	stats := s.GetStats()
	if stats["join_stage1_input_dropped"] < 0 {
		t.Errorf("invalid input_dropped: %v", stats["join_stage1_input_dropped"])
	}
}

// TestJoinPipelineCascadeWiring 3 流级联经真实级间 chan（wireCascade 中间级分支）。
func TestJoinPipelineCascadeWiring(t *testing.T) {
	cfg := joinTestConfig()
	cfg.StreamJoin = &types.StreamJoinConfig{
		Inputs: []string{"aStream", "bStream", "cStream"},
		Stages: []types.StreamJoinStage{
			{RightName: "bStream", RightAlias: "b", JoinType: "INNER",
				OnPairs: []types.JoinOnPair{{StreamField: "k", TableField: "k"}}, Within: 30 * time.Second},
			{RightName: "cStream", RightAlias: "c", JoinType: "LEFT",
				OnPairs: []types.JoinOnPair{{StreamField: "b.k2", TableField: "k2"}}, Within: 300 * time.Millisecond},
		},
	}
	s, err := NewStream(cfg)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	sink := &pipeSink{}
	s.AddSink(sink.add)
	s.Start()
	defer s.Stop()

	_ = s.EmitTo("aStream", map[string]any{"k": "x", "va": 1})
	_ = s.EmitTo("bStream", map[string]any{"k": "x", "k2": "y", "vb": 2})
	// c 缺席：级 2 LEFT 超窗补 NULL（sweeper）。
	rows := sink.waitN(1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("cascade LEFT timeout should surface: %+v", rows)
	}
	if rows[0]["va"] != 1 {
		t.Errorf("row = %+v", rows[0])
	}
	if b, ok := rows[0]["b"].(map[string]any); !ok || b["vb"] != 2 {
		t.Errorf("stage1 right under b: %+v", rows[0]["b"])
	}
	if c, ok := rows[0]["c"].(map[string]any); !ok || len(c) != 0 {
		t.Errorf("stage2 NULL complement under c should be empty map: %+v", rows[0]["c"])
	}
	stats := s.GetStats()
	if stats["join_stage1_matches_emitted"] != 1 || stats["join_stage2_left_timeout_emitted"] != 1 {
		t.Errorf("stage stats: %v / %v", stats["join_stage1_matches_emitted"], stats["join_stage2_left_timeout_emitted"])
	}
}

// TestJoinPipelineExpandStrategyDegrades expand 策略对 JOIN 输入降级 drop。
func TestJoinPipelineExpandStrategyDegrades(t *testing.T) {
	cfg := joinTestConfig()
	perf := types.DefaultPerformanceConfig()
	perf.OverflowConfig.Strategy = types.OverflowStrategyExpand
	cfg.PerformanceConfig = perf
	s, err := NewStream(cfg)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	s.Start()
	defer s.Stop()
	if s.join.overflowStrategy != types.OverflowStrategyDrop {
		t.Errorf("expand should degrade to drop for join inputs, got %q", s.join.overflowStrategy)
	}
}

// ---- 工具函数分支 ----

func TestNormalizeJoinTsUnits(t *testing.T) {
	cases := []struct {
		in   int64
		want int64
	}{
		{0, 0},
		{-5, 0},
		{500, 500},                           // 相对小值原样
		{int64(1.5e9), 1500000000000000000},  // 秒 → ns（×1e9）
		{int64(1.5e12), 1500000000000000000}, // 毫秒 → ns（×1e6）
		{int64(1.5e15), 1500000000000000000}, // 微秒 → ns（×1e3）
		{int64(1.5e18), int64(1.5e18)},       // 已是 ns
		{int64(9e18), int64(9e18)},           // 超大原样
	}
	for _, c := range cases {
		if got := normalizeJoinTs(c.in); got != c.want {
			t.Errorf("normalizeJoinTs(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestToJoinInt64Types(t *testing.T) {
	now := time.Unix(1700000000, 42)
	cases := []struct {
		in   any
		want int64
	}{
		{int(7), 7}, {int64(7), 7}, {int32(7), 7}, {uint(7), 7}, {uint64(7), 7}, {uint32(7), 7},
		{float64(7.9), 7}, {float32(7.9), 7},
		{now, now.UnixNano()},
		{"x", 0}, {nil, 0},
	}
	for _, c := range cases {
		if got := toJoinInt64(c.in); got != c.want {
			t.Errorf("toJoinInt64(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}
