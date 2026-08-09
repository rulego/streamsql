package e2e

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 畸形 MATCH_RECOGNIZE 必须在 Execute 期失败，不得静默退化为全量透传。
// 回归：右括号缺失时 MR 子句被整段丢弃，CEP 查询变成转发所有输入行。
func TestStrict_MalformedMatchRecognizeRejected(t *testing.T) {
	t.Parallel()
	sqls := map[string]string{
		"右括号缺失":  `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts MEASURES A.v AS av PATTERN (A B DEFINE A AS v > 1, B AS v > 2)`,
		"整个括号缺失": `SELECT * FROM stream MATCH_RECOGNIZE ORDER BY ts PATTERN (A) DEFINE A AS v > 1`,
	}
	for name, sql := range sqls {
		name, sql := name, sql
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ssql := streamsql.New()
			defer ssql.Stop()

			err := ssql.Execute(sql)
			require.Error(t, err, "畸形 MR 应在 Execute 期报错，而非静默退化为透传")

			// 即便调用方忽略错误，也不应把输入行原样转发出去
			var mu sync.Mutex
			forwarded := 0
			ssql.AddSink(func(rows []map[string]any) {
				mu.Lock()
				forwarded += len(rows)
				mu.Unlock()
			})
			for i := 1; i <= 5; i++ {
				ssql.Emit(map[string]any{"ts": int64(i * 100), "v": i})
			}
			time.Sleep(300 * time.Millisecond)
			mu.Lock()
			got := forwarded
			mu.Unlock()
			assert.Zero(t, got, "畸形 MR 不应转发输入行，实际转发 %d 行", got)
		})
	}
}

// 合法 MATCH_RECOGNIZE 仍须正常工作（确认严格化没有误伤正常路径）。
func TestStrict_ValidMatchRecognizeStillWorks(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := `SELECT * FROM stream MATCH_RECOGNIZE (
		ORDER BY ts
		MEASURES A.v AS av, B.v AS bv
		PATTERN (A B)
		DEFINE A AS v > 10, B AS v > 20)`
	require.NoError(t, ssql.Execute(sql))

	var mu sync.Mutex
	var matches []map[string]any
	ssql.AddSyncSink(func(rows []map[string]any) {
		mu.Lock()
		matches = append(matches, rows...)
		mu.Unlock()
	})

	for i, v := range []int{15, 25} {
		ssql.Emit(map[string]any{"ts": int64(i+1) * 100, "v": v})
	}
	time.Sleep(400 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, matches, 1, "A(15)→B(25) 应匹配一次")
	assert.EqualValues(t, 15, matches[0]["av"])
	assert.EqualValues(t, 25, matches[0]["bv"])
}

// 超长 WHERE 的每个条件都须生效。回归：超过 24 个 AND 条件后剩余条件被静默丢弃，
// 本该被过滤的行被放行（告警场景=误报）。
func TestStrict_LongWhereFullyApplied(t *testing.T) {
	t.Parallel()
	for _, n := range []int{24, 25, 40, 100} {
		n := n
		t.Run(fmt.Sprintf("%d个AND条件", n), func(t *testing.T) {
			t.Parallel()
			var conds []string
			for i := 0; i < n; i++ {
				conds = append(conds, fmt.Sprintf("f%d > 0", i))
			}
			conds = append(conds, "gate > 100") // 末位哨兵条件
			sql := "SELECT deviceId FROM stream WHERE " + strings.Join(conds, " AND ")

			ssql := streamsql.New()
			defer ssql.Stop()
			require.NoError(t, ssql.Execute(sql), "合法长 WHERE 应可执行")

			var mu sync.Mutex
			passed := 0
			ssql.AddSink(func(rows []map[string]any) {
				mu.Lock()
				passed += len(rows)
				mu.Unlock()
			})

			// gate=1 不满足 gate>100 → 整行应被过滤
			blocked := map[string]any{"deviceId": "d1", "gate": 1}
			for i := 0; i < n; i++ {
				blocked[fmt.Sprintf("f%d", i)] = 5
			}
			ssql.Emit(blocked)
			time.Sleep(250 * time.Millisecond)

			mu.Lock()
			got := passed
			mu.Unlock()
			assert.Zero(t, got, "末位条件 gate>100 未生效：WHERE 被截断，错误放行 %d 行", got)
		})
	}
}

// 长 WHERE 全部满足时须正常放行（确认不是一味拒绝）。
func TestStrict_LongWherePassesWhenAllTrue(t *testing.T) {
	t.Parallel()
	const n = 40
	var conds []string
	for i := 0; i < n; i++ {
		conds = append(conds, fmt.Sprintf("f%d > 0", i))
	}
	conds = append(conds, "gate > 100")
	sql := "SELECT deviceId FROM stream WHERE " + strings.Join(conds, " AND ")

	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute(sql))

	var mu sync.Mutex
	passed := 0
	ssql.AddSink(func(rows []map[string]any) {
		mu.Lock()
		passed += len(rows)
		mu.Unlock()
	})

	row := map[string]any{"deviceId": "d1", "gate": 500}
	for i := 0; i < n; i++ {
		row[fmt.Sprintf("f%d", i)] = 5
	}
	ssql.Emit(row)
	time.Sleep(250 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, passed, "全部条件满足时应放行")
}

// 超长 GROUP BY 的每个分组键都须参与分组（末位键被截断会导致不同行错误合并）。
func TestStrict_LongGroupByFullyApplied(t *testing.T) {
	t.Parallel()
	const n = 40
	var fields []string
	for i := 0; i < n; i++ {
		fields = append(fields, fmt.Sprintf("g%d", i))
	}
	sql := fmt.Sprintf("SELECT COUNT(*) AS c FROM stream GROUP BY %s, TumblingWindow('300ms')",
		strings.Join(fields, ","))

	ssql := streamsql.New()
	defer ssql.Stop()
	require.NoError(t, ssql.Execute(sql))

	var mu sync.Mutex
	var groups []map[string]any
	ssql.AddSink(func(rows []map[string]any) {
		mu.Lock()
		groups = append(groups, rows...)
		mu.Unlock()
	})

	// 两行仅末位分组键不同 → 必须分成 2 组
	r1 := map[string]any{}
	r2 := map[string]any{}
	for i := 0; i < n; i++ {
		r1[fmt.Sprintf("g%d", i)] = "x"
		r2[fmt.Sprintf("g%d", i)] = "x"
	}
	r2[fmt.Sprintf("g%d", n-1)] = "y"
	ssql.Emit(r1)
	ssql.Emit(r2)
	time.Sleep(800 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, groups, 2, "末位分组键 g%d 未参与分组：两行被错误合并（实际 %v）", n-1, groups)
}
