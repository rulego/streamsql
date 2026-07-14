package e2e

import (
	"sync"
	"testing"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// D3：分析函数 alias 与普通列同名 → 编译期报错，不静默覆盖。
func TestAnalytic_AliasCollisionRejected(t *testing.T) {
	ssql := streamsql.New()
	err := ssql.Execute("SELECT temperature, lag(temperature) AS temperature FROM stream")
	require.Error(t, err, "分析函数 alias 与普通列冲突应报错")
	assert.Contains(t, err.Error(), "duplicate output column")

	// 合法别名不受影响。
	ssql2 := streamsql.New()
	require.NoError(t, ssql2.Execute("SELECT temperature, lag(temperature) AS prev_temp FROM stream"))
	defer ssql2.Stop()
}

// D1：PARTITION BY 键按类型区分，int(1) 与 string("1") 不串台。
func TestAnalytic_PartitionKeyTypeSafe(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT lag(v) OVER (PARTITION BY k) AS prev FROM stream"))
	defer ssql.Stop()

	// k=1(int)：首事件 prev=nil
	r1, _ := ssql.EmitSync(map[string]any{"k": 1, "v": 100})
	assert.Nil(t, r1["prev"])

	// k="1"(string)：不同分区，prev 仍为 nil（若键冲突会拿到 100）
	r2, _ := ssql.EmitSync(map[string]any{"k": "1", "v": 200})
	assert.Nil(t, r2["prev"], `string "1" 不得与 int 1 共用分区`)

	// 回到 k=1(int)：prev=100（int 分区状态保留）
	r3, _ := ssql.EmitSync(map[string]any{"k": 1, "v": 300})
	assert.Equal(t, 100, r3["prev"])
}

// changed_cols(prefix, ignoreNull, expr...) 多列动态输出。
// 仅输出变化列，列名 = prefix + 原列名。
func TestAnalytic_ChangedColsMultiColumn(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT changed_cols("c_", true, temperature, humidity) FROM stream`))
	defer ssql.Stop()

	// 首行：两列都视为变化。
	r1, _ := ssql.EmitSync(map[string]any{"temperature": 23, "humidity": 50})
	require.NotNil(t, r1)
	assert.Equal(t, 23, r1["c_temperature"])
	assert.Equal(t, 50, r1["c_humidity"])

	// 仅 humidity 变化。
	r2, _ := ssql.EmitSync(map[string]any{"temperature": 23, "humidity": 55})
	require.NotNil(t, r2)
	_, hasTemp := r2["c_temperature"]
	assert.False(t, hasTemp, "temperature 未变化不应输出")
	assert.Equal(t, 55, r2["c_humidity"])
}

// changed_cols omitEmpty——无变化时抑制整行（changed_cols 为唯一输出）。
func TestAnalytic_ChangedColsOmitEmpty(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT changed_cols("c_", true, temperature) FROM stream`))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"temperature": 23})
	require.NotNil(t, r1)
	assert.Equal(t, 23, r1["c_temperature"])

	// 未变化 → 整行抑制。
	r2, _ := ssql.EmitSync(map[string]any{"temperature": 23})
	assert.Nil(t, r2, "无变化应抑制整行（omitEmpty）")

	r3, _ := ssql.EmitSync(map[string]any{"temperature": 25})
	require.NotNil(t, r3)
	assert.Equal(t, 25, r3["c_temperature"])
}

// changed_cols 配普通字段：普通字段总输出，changed_cols 仅变化列；不抑制整行。
func TestAnalytic_ChangedColsWithPlainField(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT ts, changed_cols("c_", true, temperature) FROM stream`))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"ts": 1, "temperature": 23})
	assert.Equal(t, 1, r1["ts"])
	assert.Equal(t, 23, r1["c_temperature"])

	// temperature 未变化，但 ts 仍输出（不抑制整行）。
	r2, _ := ssql.EmitSync(map[string]any{"ts": 2, "temperature": 23})
	require.NotNil(t, r2)
	assert.Equal(t, 2, r2["ts"])
	_, hasC := r2["c_temperature"]
	assert.False(t, hasC)
}

// changed_cols '*'：对整行各列检测变化。
func TestAnalytic_ChangedColsStar(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT changed_cols("d_", true, *) FROM stream`))
	defer ssql.Stop()

	// 首行：所有列视为变化。
	r1, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2})
	require.NotNil(t, r1)
	assert.Equal(t, 1, r1["d_a"])
	assert.Equal(t, 2, r1["d_b"])

	// 无变化 → 抑制。
	r2, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2})
	assert.Nil(t, r2)

	// 仅 a 变化。
	r3, _ := ssql.EmitSync(map[string]any{"a": 9, "b": 2})
	require.NotNil(t, r3)
	assert.Equal(t, 9, r3["d_a"])
	_, hasB := r3["d_b"]
	assert.False(t, hasB, "b 未变化不应输出")
}

// had_changed '*'：任一列变化即为 true（CDC 任一变化检测）。
func TestAnalytic_HadChangedStar(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT a FROM stream WHERE had_changed(true, *) == true`))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2})
	require.NotNil(t, r1, "首行视为变化")

	r2, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2})
	assert.Nil(t, r2, "无变化")

	r3, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 5})
	require.NotNil(t, r3, "b 变化应检出")

	r4, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 5})
	assert.Nil(t, r4, "再次无变化")
}

// B3：had_changed '*' 在行 schema 变化（列删除）时按列名正确检出变化。
func TestAnalytic_HadChangedStarSchemaDrift(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT a FROM stream WHERE had_changed(true, *) == true`))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2, "c": 3})
	require.NotNil(t, r1, "首行视为变化")

	// 列 c 被删除 → 应检出变化（位置比较会漏检）。
	r2, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2})
	require.NotNil(t, r2, "列 c 删除应检出变化")

	// 再来一条相同的 → 无变化。
	r3, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2})
	assert.Nil(t, r3, "再次无变化")
}

// B13：并发 EmitSync 纯分析查询不应竞态/崩溃（-race 下验证）。
func TestAnalytic_ConcurrentEmitSync(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT lag(v) OVER (PARTITION BY k) AS prev FROM stream"))
	defer ssql.Stop()

	const n = 300
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = ssql.EmitSync(map[string]any{"k": i % 5, "v": i})
		}(i)
	}
	wg.Wait()
}
