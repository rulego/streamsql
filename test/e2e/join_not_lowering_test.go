package e2e

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/streamsql"
)

// SQL NOT 修复的端到端行为验证。
//
// 根因(expr-lang 判定坐实):expr 只认小写 not/!;rsql 把 AND/OR lower 成
// &&/|| 却把 NOT 原样拼入条件串 → 大写 NOT 成了未定义标识符:
//   - NOT (...) 编译通过(被解析为"调用未定义变量")、运行期恒错,
//     ExprCondition.Evaluate 对求值错误静默返回 false → 整条 WHERE 静默丢弃
//     全部行(不报错、不计数,比编译错更恶劣);
//   - 裸 NOT x > 1 直接编译错(fail-fast)。
// 修复:各条件收集点把 NOT lower 为 !(IS NOT NULL 的 NOT 保留,由 IS NULL
// 预处理整体改写),覆盖 WHERE / HAVING / TRIGGER WHEN / OVER WHEN / DEFINE。

func TestStreamJoinIntegNotWhereDirect(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	if err := ssql.Execute(`SELECT k FROM raw WHERE NOT (temperature > 100 OR vibration > 50)`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	kept, _ := ssql.EmitSync(map[string]any{"k": "low", "temperature": 30, "vibration": 10})
	if kept == nil {
		t.Fatal("double-low row should pass NOT (temperature > 100 OR vibration > 50)")
	}
	rejected, _ := ssql.EmitSync(map[string]any{"k": "hot", "temperature": 30, "vibration": 60})
	if rejected != nil {
		t.Fatal("vibration-high row should be rejected")
	}
}

func TestStreamJoinIntegNotWhereJoin(t *testing.T) {
	ssql, sink := newJoinInstance(t, `
		SELECT s.k FROM nL AS s JOIN nR AS v WITHIN 30 SECONDS ON s.k = v.k
		WHERE NOT (s.temperature > 100 OR v.vibration > 50)`)
	defer ssql.Stop()

	if err := ssql.EmitTo("nL", map[string]any{"k": "z", "temperature": 30}); err != nil {
		t.Fatal(err)
	}
	if err := ssql.EmitTo("nR", map[string]any{"k": "z", "vibration": 10}); err != nil {
		t.Fatal(err)
	}
	rows := sink.waitN(t, 1, 5*time.Second)
	if len(rows) != 1 || rows[0]["k"] != "z" {
		t.Fatalf("join + NOT(OR) should keep the double-low match: %+v", rows)
	}
}

func TestStreamJoinIntegNotMixedWithIsNotNull(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	// NOT 的操作数含 IS NOT NULL:IS NULL 预处理与 NOT lower 必须共存。
	if err := ssql.Execute(`SELECT k FROM raw WHERE NOT (v IS NOT NULL) AND k IS NOT NULL`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	kept, _ := ssql.EmitSync(map[string]any{"k": "b"})
	if kept == nil {
		t.Fatal("v=null row should pass NOT (v IS NOT NULL) AND k IS NOT NULL")
	}
	rejected, _ := ssql.EmitSync(map[string]any{"k": "a", "v": 1})
	if rejected != nil {
		t.Fatal("v non-null row should be rejected")
	}
}

func TestStreamJoinIntegNotHaving(t *testing.T) {
	ssql := streamsql.New(streamsql.WithDiscardLog())
	defer ssql.Stop()
	if err := ssql.Execute(`SELECT count(*) AS c FROM raw GROUP BY TumblingWindow('200ms') HAVING NOT (c > 100)`); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	var sawOutput int64
	ssql.AddSink(func(rows []map[string]any) {
		for _, r := range rows {
			if c, ok := r["c"].(float64); ok && c <= 100 {
				atomic.StoreInt64(&sawOutput, 1)
			}
		}
	})
	// 远低于 HAVING 排除阈值的计数,应通过 NOT (c > 100) 输出。
	for i := 0; i < 3; i++ {
		ssql.Emit(map[string]any{"k": i})
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt64(&sawOutput) == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if atomic.LoadInt64(&sawOutput) == 0 {
		t.Fatal("HAVING NOT (c > 100) should let low counts through")
	}
}
