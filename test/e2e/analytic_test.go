package e2e

import (
	"sync"
	"testing"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// D3: Analysis function alias shares the same name as the regular column → Compile time error is not silently overridden.
func TestAnalytic_AliasCollisionRejected(t *testing.T) {
	ssql := streamsql.New()
	err := ssql.Execute("SELECT temperature, lag(temperature) AS temperature FROM stream")
	require.Error(t, err, "分析函数 alias 与普通列冲突应报错")
	assert.Contains(t, err.Error(), "duplicate output column")

	// Legal aliases are not affected.
	ssql2 := streamsql.New()
	require.NoError(t, ssql2.Execute("SELECT temperature, lag(temperature) AS prev_temp FROM stream"))
	defer ssql2.Stop()
}

// D1: PARTITION BY key distinguishes by type; int(1) and string("1") are not cross-connected.
func TestAnalytic_PartitionKeyTypeSafe(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute("SELECT lag(v) OVER (PARTITION BY k) AS prev FROM stream"))
	defer ssql.Stop()

	// k=1(int): first event prev=nil
	r1, _ := ssql.EmitSync(map[string]any{"k": 1, "v": 100})
	assert.Nil(t, r1["prev"])

	// k="1"(string): Different partitions, prev still nil (if key conflicts occur, it will get 100)
	r2, _ := ssql.EmitSync(map[string]any{"k": "1", "v": 200})
	assert.Nil(t, r2["prev"], `string "1" 不得与 int 1 共用分区`)

	// Return k=1(int):p rev=100 (int partition state retained)
	r3, _ := ssql.EmitSync(map[string]any{"k": 1, "v": 300})
	assert.Equal(t, 100, r3["prev"])
}

// changed_cols(prefix, ignoreNull, expr...) multi-column dynamic output.
// Only output variable columns, column name = prefix + original column name.
func TestAnalytic_ChangedColsMultiColumn(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT changed_cols("c_", true, temperature, humidity) FROM stream`))
	defer ssql.Stop()

	// First row: Both columns are considered variants.
	r1, _ := ssql.EmitSync(map[string]any{"temperature": 23, "humidity": 50})
	require.NotNil(t, r1)
	assert.Equal(t, 23, r1["c_temperature"])
	assert.Equal(t, 50, r1["c_humidity"])

	// Only humidity changes.
	r2, _ := ssql.EmitSync(map[string]any{"temperature": 23, "humidity": 55})
	require.NotNil(t, r2)
	_, hasTemp := r2["c_temperature"]
	assert.False(t, hasTemp, "temperature 未变化不应输出")
	assert.Equal(t, 55, r2["c_humidity"])
}

// changed_cols omitEmpty—suppresses entire rows when unchanged (changed_cols is the unique output).
func TestAnalytic_ChangedColsOmitEmpty(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT changed_cols("c_", true, temperature) FROM stream`))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"temperature": 23})
	require.NotNil(t, r1)
	assert.Equal(t, 23, r1["c_temperature"])

	// Unchanged → Entire line suppression.
	r2, _ := ssql.EmitSync(map[string]any{"temperature": 23})
	assert.Nil(t, r2, "无变化应抑制整行（omitEmpty）")

	r3, _ := ssql.EmitSync(map[string]any{"temperature": 25})
	require.NotNil(t, r3)
	assert.Equal(t, 25, r3["c_temperature"])
}

// changed_cols Assign regular fields: total output of regular fields, changed_cols only change columns; Do not suppress the entire line.
func TestAnalytic_ChangedColsWithPlainField(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT ts, changed_cols("c_", true, temperature) FROM stream`))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"ts": 1, "temperature": 23})
	assert.Equal(t, 1, r1["ts"])
	assert.Equal(t, 23, r1["c_temperature"])

	// Temperature does not change, but ts is still output (does not suppress the entire line).
	r2, _ := ssql.EmitSync(map[string]any{"ts": 2, "temperature": 23})
	require.NotNil(t, r2)
	assert.Equal(t, 2, r2["ts"])
	_, hasC := r2["c_temperature"]
	assert.False(t, hasC)
}

// changed_cols '*': Detects changes in each column of the entire row.
func TestAnalytic_ChangedColsStar(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT changed_cols("d_", true, *) FROM stream`))
	defer ssql.Stop()

	// First row: all columns are treated as variables.
	r1, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2})
	require.NotNil(t, r1)
	assert.Equal(t, 1, r1["d_a"])
	assert.Equal(t, 2, r1["d_b"])

	// No change → inhibition.
	r2, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2})
	assert.Nil(t, r2)

	// Only a change.
	r3, _ := ssql.EmitSync(map[string]any{"a": 9, "b": 2})
	require.NotNil(t, r3)
	assert.Equal(t, 9, r3["d_a"])
	_, hasB := r3["d_b"]
	assert.False(t, hasB, "b 未变化不应输出")
}

// had_changed '*': Any change in the column is true (CDC detects any variation).
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

// B3: had_changed '*' Correctly detects changes by column name when the row schema changes (column deletions).
func TestAnalytic_HadChangedStarSchemaDrift(t *testing.T) {
	ssql := streamsql.New()
	require.NoError(t, ssql.Execute(`SELECT a FROM stream WHERE had_changed(true, *) == true`))
	defer ssql.Stop()

	r1, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2, "c": 3})
	require.NotNil(t, r1, "首行视为变化")

	// Column C deleted → should detect changes (position comparison may be missed).
	r2, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2})
	require.NotNil(t, r2, "列 c 删除应检出变化")

	// Another identical → with no change.
	r3, _ := ssql.EmitSync(map[string]any{"a": 1, "b": 2})
	assert.Nil(t, r3, "再次无变化")
}

// B13: Concurrent EmitSync pure analysis queries should not race or crash (verify under -race).
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
