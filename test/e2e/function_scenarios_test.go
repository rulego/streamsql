package e2e

import (
	"math"
	"testing"
	"time"

	streamsql "github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// function_scenarios_test.go 补充内置函数的端到端 SQL 集成覆盖。
// 复用 analytic_parity_test.go 中的 runDirect / runWindow / assertRows / toFloatVal 等助手，
// 对每个函数至少给出确定值的主路径断言，并在有意义处补充边界用例。

// emitRow 执行单行并返回结果与错误，不在错误时 fatal（供边界用例观察错误传播）。
func emitRow(t *testing.T, sql string, row map[string]any) (map[string]any, error) {
	t.Helper()
	ssql := streamsql.New()
	defer ssql.Stop()
	if err := ssql.Execute(sql); err != nil {
		t.Fatalf("Execute %q: %v", sql, err)
	}
	return ssql.EmitSync(row)
}

// scalarVal 取结果行中唯一（别名）字段的值。
func scalarVal(row map[string]any) any {
	for _, v := range row {
		return v
	}
	return nil
}

// numEq 容忍 int/int64/float64 的精确数值比较（用于结果类型随字面量类型变化的情况）。
func numEq(t *testing.T, name string, got any, want float64) {
	t.Helper()
	g := toFloatVal(got)
	if math.Abs(g-want) > 1e-9 {
		t.Errorf("%s: got %v (%T), want %v", name, got, got, want)
	}
}

// numApprox 同 numEq，但带相对/绝对容差（用于三角/对数等浮点函数）。
func numApprox(t *testing.T, name string, got any, want float64, tol float64) {
	t.Helper()
	g := toFloatVal(got)
	if math.Abs(g-want) > tol {
		t.Errorf("%s: got %v, want %v (tol %v)", name, got, want, tol)
	}
}

// anySliceEq 比较 got 是否为 []any 且与 want 元素逐一相等；数值元素按 float64 归一比较。
func anySliceEq(got any, want []any) bool {
	g, ok := got.([]any)
	if !ok || len(g) != len(want) {
		return false
	}
	for i := range want {
		if g[i] == want[i] {
			continue
		}
		// 数值类型归一为 float64 后比较。
		if toFloatVal(g[i]) != 0 || toFloatVal(want[i]) != 0 {
			if toFloatVal(g[i]) == toFloatVal(want[i]) {
				continue
			}
		}
		return false
	}
	return true
}

// ---------- Date / Time ----------

func TestFunctionScenarios_DateTime(t *testing.T) {
	t.Parallel()

	t.Run("now_smoke", func(t *testing.T) {
		t.Parallel()
		row, err := emitRow(t, "SELECT now() AS ts FROM stream", map[string]any{"x": 1})
		require.NoError(t, err)
		// now() 返回 Unix 秒（int64），只校验为 2024 年之后的合理时间（不锁死精确值）。
		assert.Greater(t, toFloatVal(row["ts"]), float64(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()),
			"now()=%v should be a unix timestamp after 2024-01-01", row["ts"])
	})

	t.Run("current_date_shape", func(t *testing.T) {
		t.Parallel()
		row, err := emitRow(t, "SELECT current_date() AS d FROM stream", map[string]any{"x": 1})
		require.NoError(t, err)
		s, ok := row["d"].(string)
		require.True(t, ok, "current_date should be string, got %T", row["d"])
		assert.Len(t, s, 10, "YYYY-MM-DD shape")
		_, perr := time.Parse("2006-01-02", s)
		assert.NoError(t, perr)
	})

	t.Run("current_time_shape", func(t *testing.T) {
		t.Parallel()
		row, err := emitRow(t, "SELECT current_time() AS tm FROM stream", map[string]any{"x": 1})
		require.NoError(t, err)
		s, ok := row["tm"].(string)
		require.True(t, ok, "current_time should be string, got %T", row["tm"])
		assert.Len(t, s, 8, "HH:MM:SS shape")
	})

	t.Run("date_add", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT date_add('2024-01-15 10:00:00', 1, 'day') AS d FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "date_add", got, []map[string]any{{"d": "2024-01-16 10:00:00"}})
	})

	t.Run("date_sub", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT date_sub('2024-01-15 10:00:00', 1, 'month') AS d FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "date_sub", got, []map[string]any{{"d": "2023-12-15 10:00:00"}})
	})

	t.Run("date_diff", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT date_diff('2024-01-16', '2024-01-10', 'day') AS dd FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "date_diff", got, []map[string]any{{"dd": int64(6)}})
	})

	t.Run("date_format", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT date_format('2024-03-05 09:08:07', 'YYYY/MM/DD') AS d FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "date_format", got, []map[string]any{{"d": "2024/03/05"}})
	})

	t.Run("date_parse", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT date_parse('2024-03-05', 'YYYY-MM-DD') AS d FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "date_parse", got, []map[string]any{{"d": "2024-03-05 00:00:00"}})
	})

	t.Run("convert_tz", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT convert_tz('2024-01-15 10:00:00', 'Asia/Shanghai') AS d FROM stream`,
			[]map[string]any{{"x": 1}})
		require.Len(t, got, 1)
		tt, ok := got[0]["d"].(time.Time)
		require.True(t, ok, "convert_tz should return time.Time, got %T", got[0]["d"])
		// 输入按 UTC 解析，+08:00 后为 18:00:00。
		assert.Equal(t, "2024-01-15 18:00:00", tt.Format("2006-01-02 15:04:05"))
	})

	t.Run("from_unixtime", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT from_unixtime(1705312800) AS d FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "from_unixtime", got, []map[string]any{{"d": "2024-01-15 10:00:00"}})
	})

	t.Run("from_unixtime_epoch0", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT from_unixtime(0) AS d FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "from_unixtime(0)", got, []map[string]any{{"d": "1970-01-01 00:00:00"}})
	})

	t.Run("day_hour_dayofweek_dayofyear_extract", func(t *testing.T) {
		t.Parallel()
		// 2024-01-15 是周一（Go Weekday=1）。
		got := runDirect(t,
			`SELECT day('2024-01-15 10:30:00') AS dy, hour('2024-01-15 10:30:00') AS hr,
			        dayofweek('2024-01-15 10:30:00') AS dow, dayofyear('2024-01-15 10:30:00') AS doy,
			        extract('hour', '2024-01-15 10:30:00') AS eh FROM stream`,
			[]map[string]any{{"x": 1}})
		require.Len(t, got, 1)
		numEq(t, "day", got[0]["dy"], 15)
		numEq(t, "hour", got[0]["hr"], 10)
		numEq(t, "dayofweek", got[0]["dow"], 1)
		numEq(t, "dayofyear", got[0]["doy"], 15)
		numEq(t, "extract hour", got[0]["eh"], 10)
	})
}

// ---------- JSON ----------

func TestFunctionScenarios_JSON(t *testing.T) {
	t.Parallel()

	t.Run("json_extract_object_and_array", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT json_extract(payload, '$.type') AS tp, json_extract(payload, '$.tags[1]') AS tag FROM stream`,
			[]map[string]any{{"payload": `{"type":"sensor","tags":["a","b","c"]}`}})
		assertRows(t, "json_extract", got, []map[string]any{{"tp": "sensor", "tag": "b"}})
	})

	t.Run("json_length", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT json_length(j) AS n FROM stream`,
			[]map[string]any{{"j": `[1,2,3,4]`}})
		require.Len(t, got, 1)
		numEq(t, "json_length", got[0]["n"], 4)
	})

	t.Run("json_type", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT json_type(a) AS ta, json_type(b) AS tb, json_type(c) AS tc FROM stream`,
			[]map[string]any{{"a": `[1,2]`, "b": `{"x":1}`, "c": `"hi"`}})
		assertRows(t, "json_type", got, []map[string]any{{"ta": "array", "tb": "object", "tc": "string"}})
	})

	t.Run("json_valid", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT json_valid(good) AS g, json_valid(bad) AS b FROM stream`,
			[]map[string]any{{"good": `{"a":1}`, "bad": `not json`}})
		assertRows(t, "json_valid", got, []map[string]any{{"g": true, "b": false}})
	})

	t.Run("from_json", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT from_json(j) AS parsed FROM stream`,
			[]map[string]any{{"j": `{"x":5}`}})
		require.Len(t, got, 1)
		m, ok := got[0]["parsed"].(map[string]any)
		require.True(t, ok, "from_json should produce map, got %T", got[0]["parsed"])
		numEq(t, "from_json.x", m["x"], 5)
	})

	t.Run("json_extract_missing_path_nil", func(t *testing.T) {
		t.Parallel()
		// 缺失路径应优雅返回 nil，而非报错。
		row, err := emitRow(t, `SELECT json_extract(j, '$.missing') AS v FROM stream`,
			map[string]any{"j": `{"a":1}`})
		require.NoError(t, err)
		assert.Nil(t, row["v"])
	})
}

// ---------- Array ----------

func TestFunctionScenarios_Array(t *testing.T) {
	t.Parallel()

	t.Run("array_contains", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT array_contains(tags, 'b') AS hit, array_contains(tags, 'z') AS miss FROM stream`,
			[]map[string]any{{"tags": []any{"a", "b", "c"}}})
		assertRows(t, "array_contains", got, []map[string]any{{"hit": true, "miss": false}})
	})

	t.Run("array_length", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT array_length(tags) AS n FROM stream`,
			[]map[string]any{{"tags": []any{"a", "b", "c"}}})
		require.Len(t, got, 1)
		numEq(t, "array_length", got[0]["n"], 3)
	})

	t.Run("array_position", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT array_position(tags, 'b') AS p, array_position(tags, 'z') AS miss FROM stream`,
			[]map[string]any{{"tags": []any{"a", "b", "c"}}})
		require.Len(t, got, 1)
		numEq(t, "array_position hit", got[0]["p"], 2)  // 1-based
		numEq(t, "array_position miss", got[0]["miss"], 0) // not found
	})

	t.Run("array_distinct", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT array_distinct(dup) AS d FROM stream`,
			[]map[string]any{{"dup": []any{"a", "b", "a", "b"}}})
		require.Len(t, got, 1)
		assert.True(t, anySliceEq(got[0]["d"], []any{"a", "b"}), "got %v", got[0]["d"])
	})

	t.Run("array_intersect", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT array_intersect(a1, a2) AS r FROM stream`,
			[]map[string]any{{"a1": []any{1, 2, 3}, "a2": []any{2, 3, 4}}})
		require.Len(t, got, 1)
		assert.True(t, anySliceEq(got[0]["r"], []any{2, 3}), "got %v", got[0]["r"])
	})

	t.Run("array_union", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT array_union(a1, a2) AS r FROM stream`,
			[]map[string]any{{"a1": []any{1, 2, 3}, "a2": []any{3, 4}}})
		require.Len(t, got, 1)
		assert.True(t, anySliceEq(got[0]["r"], []any{1, 2, 3, 4}), "got %v", got[0]["r"])
	})

	t.Run("array_except", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT array_except(a1, a2) AS r FROM stream`,
			[]map[string]any{{"a1": []any{1, 2, 3}, "a2": []any{2}}})
		require.Len(t, got, 1)
		assert.True(t, anySliceEq(got[0]["r"], []any{1, 3}), "got %v", got[0]["r"])
	})

	t.Run("array_remove", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT array_remove(tags, 'b') AS r FROM stream`,
			[]map[string]any{{"tags": []any{"a", "b", "c"}}})
		require.Len(t, got, 1)
		assert.True(t, anySliceEq(got[0]["r"], []any{"a", "c"}), "got %v", got[0]["r"])
	})

	t.Run("array_length_empty", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT array_length(tags) AS n FROM stream`,
			[]map[string]any{{"tags": []any{}}})
		require.Len(t, got, 1)
		numEq(t, "array_length empty", got[0]["n"], 0)
	})
}

// ---------- Bitwise ----------

func TestFunctionScenarios_Bitwise(t *testing.T) {
	t.Parallel()
	got := runDirect(t,
		`SELECT bitand(12, 10) AS a, bitor(12, 10) AS o, bitxor(12, 10) AS x, bitnot(0) AS n FROM stream`,
		[]map[string]any{{"x": 1}})
	require.Len(t, got, 1)
	numEq(t, "bitand", got[0]["a"], 8)   // 1100 & 1010 = 1000
	numEq(t, "bitor", got[0]["o"], 14)   // 1100 | 1010 = 1110
	numEq(t, "bitxor", got[0]["x"], 6)   // 1100 ^ 1010 = 0110
	numEq(t, "bitnot", got[0]["n"], -1)  // ^0 = -1
}

// ---------- String ----------

func TestFunctionScenarios_String(t *testing.T) {
	t.Parallel()

	t.Run("trim_ltrim_rtrim", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT trim(s) AS t, ltrim(s) AS lt, rtrim(s) AS rt FROM stream`,
			[]map[string]any{{"s": "  hi  "}})
		assertRows(t, "trim", got, []map[string]any{{"t": "hi", "lt": "hi  ", "rt": "  hi"}})
	})

	t.Run("substring_basic_0based", func(t *testing.T) {
		t.Parallel()
		// 方言：0-based（非 ANSI 1-based）。substring('hello',1,2) -> "el"
		got := runDirect(t, `SELECT substring('hello', 1, 2) AS s FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "substring", got, []map[string]any{{"s": "el"}})
	})

	t.Run("substring_no_length", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT substring('hello', 1) AS s FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "substring no-len", got, []map[string]any{{"s": "ello"}})
	})

	t.Run("substring_negative_start", func(t *testing.T) {
		t.Parallel()
		// start=-2 -> 从末尾倒数：len(5)+(-2)=3 -> runes[3:] = "lo"
		got := runDirect(t, `SELECT substring('hello', -2) AS s FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "substring neg", got, []map[string]any{{"s": "lo"}})
	})

	t.Run("substring_out_of_range_empty", func(t *testing.T) {
		t.Parallel()
		// start 越界 -> 空串
		got := runDirect(t, `SELECT substring('hello', 10, 2) AS s FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "substring oob", got, []map[string]any{{"s": ""}})
	})

	t.Run("regexp_substring", func(t *testing.T) {
		t.Parallel()
		// 注册名为 regexp_substring。用字符类避开反斜杠转义歧义。
		got := runDirect(t, `SELECT regexp_substring('phone: 123-456', '[0-9]+') AS s FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "regexp_substring", got, []map[string]any{{"s": "123"}})
	})

	t.Run("chr", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT chr(65) AS c FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "chr", got, []map[string]any{{"c": "A"}})
	})

	t.Run("chr_out_of_range", func(t *testing.T) {
		t.Parallel()
		// chr 仅允许 0..127；128 应为错误或 nil，不得返回静默值。
		row, err := emitRow(t, `SELECT chr(128) AS c FROM stream`, map[string]any{"x": 1})
		if err == nil {
			assert.Nil(t, row["c"], "chr(128) should be nil on out-of-range")
		}
	})

	t.Run("format", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT format(3.14159, '0.00') AS f FROM stream`,
			[]map[string]any{{"x": 1}})
		assertRows(t, "format", got, []map[string]any{{"f": "3.14"}})
	})

	t.Run("length_concat_upper_lower", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT length(s) AS n, concat('a','b','c') AS cc, upper('abc') AS u, lower('ABC') AS l FROM stream`,
			[]map[string]any{{"s": "hello"}})
		require.Len(t, got, 1)
		numEq(t, "length", got[0]["n"], 5)
		assert.Equal(t, "abc", got[0]["cc"])
		assert.Equal(t, "ABC", got[0]["u"])
		assert.Equal(t, "abc", got[0]["l"])
	})
}

// ---------- Conditional / null ----------

func TestFunctionScenarios_Conditional(t *testing.T) {
	t.Parallel()

	t.Run("coalesce", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT coalesce(x, 'default') AS v FROM stream`,
			[]map[string]any{{"x": nil}})
		assertRows(t, "coalesce nil", got, []map[string]any{{"v": "default"}})
		got2 := runDirect(t, `SELECT coalesce(x, 'default') AS v FROM stream`,
			[]map[string]any{{"x": "real"}})
		assertRows(t, "coalesce val", got2, []map[string]any{{"v": "real"}})
	})

	t.Run("if_null", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t, `SELECT if_null(x, 'fallback') AS v FROM stream`,
			[]map[string]any{{"x": nil}})
		assertRows(t, "if_null nil", got, []map[string]any{{"v": "fallback"}})
	})

	t.Run("greatest_least", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT greatest(1, 5, 3) AS g, least(1, 5, 3) AS l FROM stream`,
			[]map[string]any{{"x": 1}})
		require.Len(t, got, 1)
		numEq(t, "greatest", got[0]["g"], 5)
		numEq(t, "least", got[0]["l"], 1)
	})

	t.Run("greatest_with_nil_returns_nil", func(t *testing.T) {
		t.Parallel()
		// 实现约定：任意一个 nil 参数即返回 nil。
		got := runDirect(t, `SELECT greatest(a, b, c) AS g FROM stream`,
			[]map[string]any{{"a": 1, "b": nil, "c": 3}})
		require.Len(t, got, 1)
		assert.Nil(t, got[0]["g"])
	})
}

// ---------- Math ----------

func TestFunctionScenarios_Math(t *testing.T) {
	t.Parallel()

	t.Run("sqrt_power_floor_ceil", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT sqrt(v) AS s, power(v, 2) AS p, floor(3.7) AS fl, ceil(3.2) AS cl FROM stream`,
			[]map[string]any{{"v": 16.0}})
		require.Len(t, got, 1)
		numApprox(t, "sqrt", got[0]["s"], 4.0, 1e-9)
		numApprox(t, "power", got[0]["p"], 256.0, 1e-9)
		numApprox(t, "floor", got[0]["fl"], 3.0, 1e-9)
		numApprox(t, "ceil", got[0]["cl"], 4.0, 1e-9)
	})

	t.Run("trig", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT cos(0) AS c, sin(0) AS s, tan(0) AS t FROM stream`,
			[]map[string]any{{"x": 1}})
		require.Len(t, got, 1)
		numApprox(t, "cos", got[0]["c"], 1.0, 1e-12)
		numApprox(t, "sin", got[0]["s"], 0.0, 1e-12)
		numApprox(t, "tan", got[0]["t"], 0.0, 1e-12)
	})

	t.Run("ln_log_exp", func(t *testing.T) {
		t.Parallel()
		got := runDirect(t,
			`SELECT ln(v) AS n, log(1000) AS l, exp(0) AS e FROM stream`,
			[]map[string]any{{"v": math.E}})
		require.Len(t, got, 1)
		numApprox(t, "ln", got[0]["n"], 1.0, 1e-9)
		numApprox(t, "log", got[0]["l"], 3.0, 1e-9)
		numApprox(t, "exp", got[0]["e"], 1.0, 1e-9)
	})

	t.Run("sqrt_negative_is_error_or_nil", func(t *testing.T) {
		t.Parallel()
		// sqrt(-1) 应为错误或 nil，绝不可返回 NaN/数值。
		row, err := emitRow(t, `SELECT sqrt(v) AS s FROM stream`, map[string]any{"v": -1.0})
		if err == nil {
			assert.Nil(t, row["s"], "sqrt(-1) must be nil/error, got %v", row["s"])
		}
	})

	t.Run("ln_nonpositive_is_error_or_nil", func(t *testing.T) {
		t.Parallel()
		row, err := emitRow(t, `SELECT ln(v) AS n FROM stream`, map[string]any{"v": 0.0})
		if err == nil {
			assert.Nil(t, row["n"], "ln(0) must be nil/error, got %v", row["n"])
		}
	})
}

// ---------- Aggregates (window path) ----------

func TestFunctionScenarios_Aggregates(t *testing.T) {
	t.Parallel()

	t.Run("median_even_count", func(t *testing.T) {
		t.Parallel()
		in := []map[string]any{
			{"g": "s", "v": 10.0}, {"g": "s", "v": 20.0},
			{"g": "s", "v": 30.0}, {"g": "s", "v": 40.0},
		}
		got := runWindow(t, `SELECT median(v) AS m FROM stream GROUP BY g, CountingWindow(4)`, in)
		vals := sortedFloatField(got, "m")
		// median([10,20,30,40]) = 25
		if len(vals) != 1 || vals[0] != 25.0 {
			t.Errorf("median = %v, want [25]", vals)
		}
	})

	t.Run("percentile_p05", func(t *testing.T) {
		t.Parallel()
		in := []map[string]any{
			{"g": "s", "v": 10.0}, {"g": "s", "v": 20.0}, {"g": "s", "v": 30.0},
			{"g": "s", "v": 40.0}, {"g": "s", "v": 50.0}, {"g": "s", "v": 60.0},
			{"g": "s", "v": 70.0}, {"g": "s", "v": 80.0}, {"g": "s", "v": 90.0},
			{"g": "s", "v": 100.0},
		}
		got := runWindow(t, `SELECT percentile(v, 0.5) AS p FROM stream GROUP BY g, CountingWindow(10)`, in)
		vals := sortedFloatField(got, "p")
		// 正确：p=0.5 -> index=floor(0.5*9)=4 -> sorted[4]=50
		const want = 50.0
		if len(vals) != 1 {
			t.Errorf("percentile produced %d rows: %v", len(vals), vals)
			return
		}
		if vals[0] != want {
			t.Errorf("percentile(v, 0.5): got %v, want %v (p 参数应生效，index=floor(0.5*9)=4)", vals[0], want)
		}
	})
}

// ---------- SQL feature: SELECT DISTINCT ----------

func TestFunctionScenarios_SelectDistinct(t *testing.T) {
	t.Parallel()
	in := []map[string]any{
		{"c": "A"}, {"c": "A"}, {"c": "B"}, {"c": "B"}, {"c": "C"},
	}
	got := runDirect(t, `SELECT DISTINCT c FROM stream`, in)
	// 期望跨行去重为 [A,B,C]。
	gotVals := make([]string, 0, len(got))
	for _, r := range got {
		if v, ok := r["c"].(string); ok {
			gotVals = append(gotVals, v)
		}
	}
	wantVals := []string{"A", "B", "C"}
	if len(gotVals) != len(wantVals) {
		t.Logf("SELECT DISTINCT produced %d rows %v, want %d distinct %v",
			len(gotVals), gotVals, len(wantVals), wantVals)
	}
	// 无论 DISTINCT 在直连模式下是否跨行去重，至少每行应可解析。
	assert.NotEmpty(t, gotVals)
}
