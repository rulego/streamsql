package functions

import (
	"reflect"
	"strings"
)

// AnalyticState 分析函数的流级状态机。每条事件调 Apply：
// 用已从当前行解析出的 args 更新状态并返回当前结果值。每个 PARTITION 各持一份
// 独立状态。与 AggregatorFunction 的批量 Add/Result 不同，这是逐条 Apply。
type AnalyticState interface {
	// Apply 用 args（args[0] 为主参数值，后续为 offset/default/ignoreNull 等）
	// 更新状态，返回当前行的分析结果。
	Apply(args []any) any
	// Reset 重置状态（Stop 或状态清理时调用）。
	Reset()
}

// StatefulAnalytic 由有状态分析函数（lag/latest/had_changed/changed_col/acc_*）实现。
// NewState 创建一份独立状态，供状态机管理器为每个 PARTITION 各持一份。
type StatefulAnalytic interface {
	NewState() AnalyticState
}

// MultiColumnState 由输出多列的分析函数（changed_cols）实现。引擎对 MultiColumn
// 字段走 ApplyColumns：传入 prefix/ignoreNull 与 {列名: 当前值}，返回 {prefix+列名: 新值}
// 仅含发生变化的列。单列函数继续走 Apply。
type MultiColumnState interface {
	ApplyColumns(prefix string, ignoreNull bool, cols map[string]any) map[string]any
}

// NamedRowState 由按列名比较整行的分析函数（had_changed 用 '*' 时）实现。按列名比较
// 避免行 schema 变化（列增删/乱序）时的位置错位。
type NamedRowState interface {
	ApplyNamed(ignoreNull bool, cols map[string]any) any
}

// analyticToInt 容错整数转换：lag offset 等参数经 parseFunctionArgs 后可能为
// int/int64/float64，统一转 int。
func analyticToInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}

// AnalyticToBool 容错布尔转换：had_changed/changed_col(s) 的 ignoreNull 参数经
// parseFunctionArgs 后可能为 bool 或字符串 "true"/"false"（未加引号的 true 落到字符串分支）。
func AnalyticToBool(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	case string:
		return strings.EqualFold(b, "true")
	}
	return false
}

// analyticEqual 值相等比较，数字跨类型（int vs float64）判等，其余 reflect.DeepEqual。
func analyticEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if af, ok := toFloat64Generic(a); ok {
		if bf, ok2 := toFloat64Generic(b); ok2 {
			return af == bf
		}
	}
	return reflect.DeepEqual(a, b)
}

// toFloat64Generic 把数字类型统一到 float64 用于相等/比较。
func toFloat64Generic(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}
