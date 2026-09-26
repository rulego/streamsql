package e2e

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/streamsql"
)

// 流-流 JOIN 集成测试(补充集二):生命周期、错误面、输出通道与并发。
// 与 join_integration_test.go(语义/级联/资源闸)互补。

const joinLifecycleSQL = `
	SELECT s.k, v.w FROM lcA AS s JOIN lcB AS v WITHIN 30 SECONDS ON s.k = v.k`

// ---- 生命周期与错误面 ----

// 同一实例二次 Execute 报错(既有约束在 join 查询上同样生效)。
func TestStreamJoinIntegExecuteTwiceRejected(t *testing.T) {
	ssql, _ := newJoinInstance(t, joinLifecycleSQL)
	defer ssql.Stop()
	if err := ssql.Execute(joinLifecycleSQL); err == nil {
		t.Fatal("second Execute must be rejected")
	}
}

// EmitTo 在 Stop 之后报错(不再投递)。
func TestStreamJoinIntegEmitToAfterStop(t *testing.T) {
	ssql, _ := newJoinInstance(t, joinLifecycleSQL)
	ssql.EmitTo("lcA", map[string]any{"k": "x"})
	waitStatsEq(t, ssql, "join_stage1_buffered_rows", 1)
	ssql.Stop()
	if err := ssql.EmitTo("lcA", map[string]any{"k": "y"}); err == nil {
		t.Fatal("EmitTo after Stop must error")
	}
}

// 幂等 Stop / 无数据 Stop 不 panic、不产生输出。
func TestStreamJoinIntegStopIdempotentAndEmpty(t *testing.T) {
	ssql, sink := newJoinInstance(t, joinLifecycleSQL)
	ssql.Stop()
	ssql.Stop() // 二次幂等
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("empty stop emitted %d rows", got)
	}
}

// join 查询上 RegisterTable 报错(表注册只属于流-表富化路径)。
func TestStreamJoinIntegRegisterTableRejected(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	if err := ssql.Execute("SELECT s.k FROM lcA AS s JOIN lcB AS v WITHIN 5 SECONDS ON s.k = v.k"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := ssql.RegisterTable("tbl", []map[string]any{{"k": "x"}}); err == nil {
		t.Fatal("RegisterTable on a stream-join query must error")
	}
}

// 流名大小写敏感:大小写不符报错,错误信息列出已知输入。
func TestStreamJoinIntegStreamNameCaseSensitive(t *testing.T) {
	ssql, _ := newJoinInstance(t, joinLifecycleSQL)
	defer ssql.Stop()
	err := ssql.EmitTo("lca", map[string]any{"k": "x"})
	if err == nil {
		t.Fatal("lowercased stream name must error")
	}
	if !containsStr(err.Error(), "lcA") {
		t.Errorf("error should list known inputs, got: %v", err)
	}
}

// 查询类型三谓词互斥正确。
func TestStreamJoinIntegQueryTypePredicates(t *testing.T) {
	ssql, _ := newJoinInstance(t, joinLifecycleSQL)
	defer ssql.Stop()
	if !ssql.IsStreamJoinQuery() {
		t.Error("IsStreamJoinQuery = false, want true")
	}
	if ssql.IsAggregationQuery() {
		t.Error("IsAggregationQuery = true, want false")
	}
	if ssql.IsCEPQuery() {
		t.Error("IsCEPQuery = true, want false")
	}
}

// nil 行容错:键全为 nil 的左右行按既有约定(<nil> 键)匹配,不 panic。
// (nil==nil 键匹配沿袭流表 JOIN 的 encodeKey 既有约定,见 table_store_test.go。)
func TestStreamJoinIntegNilRowTolerated(t *testing.T) {
	ssql, sink := newJoinInstance(t, joinLifecycleSQL)
	defer ssql.Stop()

	ssql.EmitTo("lcA", nil)
	ssql.EmitTo("lcB", nil)
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 {
		t.Fatalf("nil-key rows should match per encodeKey convention, got %d", len(rows))
	}
	if rows[0]["k"] != nil {
		t.Errorf("k should be nil, got %v", rows[0]["k"])
	}
}

// 校验清单经完整 Execute 路径逐条报错(与 rsql 包的解析级断言互补,验证错误真正
// 从 Execute 冒泡给调用方)。
func TestStreamJoinIntegValidationViaExecute(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"right", "SELECT s.k FROM lcA AS s RIGHT JOIN lcB AS v ON s.k = v.k", "RIGHT"},
		{"full", "SELECT s.k FROM lcA AS s FULL JOIN lcB AS v ON s.k = v.k", "FULL"},
		{"cross", "SELECT s.k FROM lcA AS s CROSS JOIN lcB AS v", "CROSS"},
		{"groupby", "SELECT s.k FROM lcA AS s JOIN lcB AS v WITHIN 5 SECONDS ON s.k = v.k GROUP BY s.k", "GROUP BY"},
		{"limit", "SELECT s.k FROM lcA AS s JOIN lcB AS v WITHIN 5 SECONDS ON s.k = v.k LIMIT 5", "LIMIT"},
		{"having", "SELECT s.k FROM lcA AS s JOIN lcB AS v WITHIN 5 SECONDS ON s.k = v.k HAVING s.k > 1", "HAVING"},
		{"distinct", "SELECT DISTINCT s.k FROM lcA AS s JOIN lcB AS v WITHIN 5 SECONDS ON s.k = v.k", "DISTINCT"},
		{"analytic", "SELECT lag(s.k) FROM lcA AS s JOIN lcB AS v WITHIN 5 SECONDS ON s.k = v.k", "analytic"},
		{"maxoo", "SELECT s.k FROM lcA AS s JOIN lcB AS v WITHIN 5 SECONDS ON s.k = v.k WITH (TIMESTAMP = 'ts', MAXOUTOFORDERNESS = '1s')", "MaxOutOfOrderness"},
		{"mixed", "SELECT s.k FROM lcA AS s JOIN lcB AS v WITHIN 5 SECONDS ON s.k = v.k JOIN tbl AS m ON s.k = m.k", "mix"},
		{"twice", "SELECT s.k FROM lcA AS s JOIN lcB AS v WITHIN 5 SECONDS ON s.k = v.k WITHIN 5 SECONDS", "twice"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ssql := streamsql.New(streamsql.WithDiscardLog())
			defer ssql.Stop()
			err := ssql.Execute(c.sql)
			if err == nil {
				t.Fatalf("expected Execute error for %s", c.name)
			}
			if !containsStr(err.Error(), c.want) {
				t.Errorf("error %q does not contain %q", err.Error(), c.want)
			}
		})
	}
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

// ---- 输出通道与指标 ----

// ToChannel 消费 join 输出(与 AddSink 并存的既有出口)。
func TestStreamJoinIntegToChannelOutput(t *testing.T) {
	ssql, _ := newJoinInstance(t, joinLifecycleSQL)
	defer ssql.Stop()

	ch := ssql.ToChannel()
	received := make(chan []map[string]any, 8)
	go func() {
		// resultChan 永不关闭(既有设计),读到一批即退出,避免 goroutine 泄漏(goleak 门禁)。
		select {
		case batch := <-ch:
			received <- batch
		case <-time.After(5 * time.Second):
		}
	}()

	if err := ssql.EmitTo("lcA", map[string]any{"k": "x"}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("lcB", map[string]any{"k": "x", "w": 7}); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-received:
		if len(batch) != 1 || batch[0]["w"] != 7 {
			t.Fatalf("batch = %+v", batch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for ToChannel output")
	}
}

// 多 sink + 同步 sink 全部收到输出。
func TestStreamJoinIntegMultipleSinks(t *testing.T) {
	ssql, sink1 := newJoinInstance(t, joinLifecycleSQL)
	defer ssql.Stop()
	sink2 := &joinResultSink{}
	var syncCalls int64
	ssql.AddSink(sink2.add)
	// sync sink 回调跑在 sink worker goroutine,计数必须原子(本文件其余收集器
	// 均为锁保护的 sink/channel,-race 下这里是唯一裸共享)。
	ssql.AddSyncSink(func([]map[string]any) { atomic.AddInt64(&syncCalls, 1) })

	if err := ssql.EmitTo("lcA", map[string]any{"k": "x"}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("lcB", map[string]any{"k": "x", "w": 1}); err != nil {
		t.Fatal(err)
	}
	sink1.waitN(t, 1, 5*time.Second)
	if len(sink2.snapshot()) != 1 {
		t.Error("second sink missed the row")
	}
	if atomic.LoadInt64(&syncCalls) == 0 {
		t.Error("sync sink never called")
	}
}

// 零态:未喂数时全部 join 指标存在且为 0。
func TestStreamJoinIntegZeroStateStats(t *testing.T) {
	ssql, _ := newJoinInstance(t, joinLifecycleSQL)
	defer ssql.Stop()

	want := []string{
		"join_stage1_matches_emitted", "join_stage1_left_timeout_emitted",
		"join_stage1_late_dropped", "join_stage1_input_dropped",
		"join_stage1_rows_dropped", "join_stage1_keys_evicted",
		"join_stage1_buffered_rows", "join_stage1_left_watermark", "join_stage1_right_watermark",
	}
	stats := ssql.GetStats()
	for _, k := range want {
		v, ok := stats[k]
		if !ok {
			t.Errorf("metric %q missing from GetStats", k)
			continue
		}
		if v != 0 {
			t.Errorf("metric %q = %d, want 0", k, v)
		}
	}
}

// Metrics() 注册表暴露 join 指标名;GetDetailedStats 不因 join 模式异常。
func TestStreamJoinIntegMetricsRegistryAndDetailed(t *testing.T) {
	ssql, _ := newJoinInstance(t, joinLifecycleSQL)
	defer ssql.Stop()

	found := false
	for _, name := range ssql.Metrics().Names() {
		if name == "join_stage1_matches_emitted" {
			found = true
		}
	}
	if !found {
		t.Error("metrics registry missing join_stage1_matches_emitted")
	}
	detailed := ssql.GetDetailedStats()
	if _, ok := detailed["basic_stats"]; !ok {
		t.Error("GetDetailedStats missing basic_stats")
	}
}

// ---- 并发 ----

// 多 goroutine 并发 EmitTo(Emit 混用)+ 并发 Stop:无 panic、无挂死、无泄漏
// (泄漏由包级 goleak 门禁兜底)。
func TestStreamJoinIntegConcurrentEmitAndStop(t *testing.T) {
	for iter := 0; iter < 5; iter++ {
		ssql, sink := newJoinInstance(t, `
			SELECT s.k FROM ccA AS s JOIN ccB AS v WITHIN 200 MILLISECONDS ON s.k = v.k
			WITH (TIMESTAMP = 'ts')`)
		var wg sync.WaitGroup
		for g := 0; g < 3; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < 150; i++ {
					if g == 2 && i%3 == 0 {
						ssql.Emit(map[string]any{"k": i % 5, "ts": time.Now().UnixMilli()})
						continue
					}
					name := "ccA"
					if g%2 == 1 {
						name = "ccB"
					}
					_ = ssql.EmitTo(name, map[string]any{"k": i % 5, "ts": time.Now().UnixMilli()})
				}
			}(g)
		}
		// 与生产者竞争 Stop。iter 以参数捕获（Go <1.22 循环变量共享，
		// 闭包直读 iter 与测试 goroutine 的自增构成 data race）。
		go func(iter int) {
			time.Sleep(time.Duration(5+iter*5) * time.Millisecond)
			ssql.Stop()
		}(iter)
		wg.Wait()
		ssql.Stop() // 幂等二次
		_ = sink.snapshot()
	}
}

// 并发双生产者高频对打:固定行数下 matches 收敛、无 panic(内部一致性冒烟)。
func TestStreamJoinIntegConcurrentProducersConsistent(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k FROM ccA AS s JOIN ccB AS v WITHIN 30 SECONDS ON s.k = v.k`)
	defer ssql.Stop()

	const perG = 200
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < perG; i++ {
			_ = ssql.EmitTo("ccA", map[string]any{"k": i % 20})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < perG; i++ {
			_ = ssql.EmitTo("ccB", map[string]any{"k": i % 20})
		}
	}()
	wg.Wait()

	// 每键 10 左 × 10 右(30s 窗内全缓冲,append-only 笛卡尔)= 至多 100 对/键;
	// 左右交错到达,每键至少 1 对。断言区间 [20, 2000] 且无输入丢弃。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stats := ssql.GetStats()
		if stats["join_stage1_input_dropped"] > 0 || stats["join_stage1_matches_emitted"] >= perG/10 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	stats := ssql.GetStats()
	m := stats["join_stage1_matches_emitted"]
	if m < perG/10 || m > perG*perG/20 {
		t.Errorf("matches = %d, want in [%d, %d]", m, perG/10, perG*perG/20)
	}
	if d := stats["join_stage1_input_dropped"]; d != 0 {
		t.Errorf("input_dropped = %d, want 0 (chan sized for the load)", d)
	}
	_ = sink.snapshot()
}
