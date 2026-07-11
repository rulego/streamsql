package stream

import (
	"container/list"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/rulego/streamsql/condition"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/types"
)

// defaultMaxPartitions bounds per-field PARTITION state so high-cardinality keys
// (e.g. deviceId) cannot grow memory without limit. When the cap is exceeded the
// least-recently-used partition is evicted (state + cached last-result).
const defaultMaxPartitions = 10000

// AnalyticEngine 管理一条查询里所有分析函数字段的流级状态机。
// 走直连路径（EmitSync/processDirectData）：每条事件在 WHERE 之前求值，结果按 alias
// 注入行（供 WHERE/投影引用）。状态按 PARTITION 分桶，WHEN 控制状态更新。
type AnalyticEngine struct {
	owner  *Stream
	fields []*analyticFieldEngine
}

// HasFields 是否有分析函数字段（nil 引擎视为无）。
func (e *AnalyticEngine) HasFields() bool { return e != nil && len(e.fields) > 0 }

// analyticFieldEngine 单个分析函数字段的状态机（含 PARTITION 分桶 + WHEN 条件）。
type analyticFieldEngine struct {
	af            types.AnalyticField
	stateCtor     func() functions.AnalyticState
	whenCond      condition.Condition // WHEN，nil 表示无
	mu            sync.Mutex
	noPart        functions.AnalyticState // 无 PARTITION 时的单一状态
	partitions    map[string]*list.Element // PARTITION BY 时 per-key 状态（LRU 节点）
	lru           *list.List               // LRU 顺序：front=最近使用，back=待淘汰
	lastResults   map[string]any           // per-partition 上次结果（WHEN 不满足时复用）
	maxPartitions int                      // 分区数上限（超出按 LRU 淘汰）
}

// partitionEntry 是 LRU 链表节点携带的分区状态。
type partitionEntry struct {
	key   string
	state functions.AnalyticState
}

// NewAnalyticEngine 根据配置构建分析函数状态机集合。无分析函数时返回 nil。
func NewAnalyticEngine(owner *Stream, fields []types.AnalyticField) (*AnalyticEngine, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	// 分区上限：选项注入（≤0 用默认）。
	maxPart := defaultMaxPartitions
	if owner != nil && owner.config.AnalyticMaxPartitions > 0 {
		maxPart = owner.config.AnalyticMaxPartitions
	}
	engines := make([]*analyticFieldEngine, 0, len(fields))
	for _, af := range fields {
		fn, ok := functions.Get(af.FuncName)
		if !ok {
			return nil, fmt.Errorf("analytic function %q not found", af.FuncName)
		}
		sf, ok := fn.(functions.StatefulAnalytic)
		if !ok {
			return nil, fmt.Errorf("function %q is not a stateful analytic function", af.FuncName)
		}
		fe := &analyticFieldEngine{
			af:            af,
			stateCtor:     sf.NewState,
			partitions:    make(map[string]*list.Element),
			lru:           list.New(),
			lastResults:   make(map[string]any),
			maxPartitions: maxPart,
		}
		if af.Over != nil && strings.TrimSpace(af.Over.When) != "" {
			cond, err := condition.NewExprCondition(af.Over.When)
			if err != nil {
				return nil, fmt.Errorf("compile OVER WHEN %q failed: %w", af.Over.When, err)
			}
			fe.whenCond = cond
		}
		engines = append(engines, fe)
	}
	return &AnalyticEngine{owner: owner, fields: engines}, nil
}

// Evaluate 对一行求值所有分析函数字段，返回 map[alias]value。
func (e *AnalyticEngine) Evaluate(row map[string]any) map[string]any {
	out := make(map[string]any, len(e.fields))
	for _, fe := range e.fields {
		out[fe.af.Alias] = fe.evaluate(e.owner, row)
	}
	return out
}

func (fe *analyticFieldEngine) evaluate(s *Stream, row map[string]any) (result any) {
	defer func() {
		if r := recover(); r != nil {
			// 单个分析函数解析/求值异常不应中断整条流水线。
			if s != nil && s.log != nil {
				s.log.Error("analytic %q evaluate panic: %v", fe.af.Expression, r)
			}
			result = nil
		}
	}()
	// 多列函数（changed_cols）走 ApplyColumns 扇出分支。
	if fe.af.MultiColumn {
		return fe.evaluateMultiColumn(s, row)
	}
	args, err := s.parseFunctionArgs(fe.af.Expression, row)
	if err != nil || args == nil {
		args = []any{}
	}
	// '*' 展开：had_changed(true, *) → 对整行各列求值。
	if hasStarArg(fe.af.Args) {
		args = expandStarArgs(fe.af.Args, row, args)
	}
	partKey := fe.partitionKey(row)
	fe.mu.Lock()
	defer fe.mu.Unlock()
	// WHEN：满足才更新状态，否则复用上次结果（eKuiper 条件状态语义）。
	if fe.whenCond != nil && !fe.whenCond.Evaluate(row) {
		if last, ok := fe.lastResults[partKey]; ok {
			return last
		}
		return nil
	}
	state := fe.getStateLocked(partKey)
	// had_changed(true, *) 按列名比较整行，避免行 schema 变化（列增删/乱序）的位置错位。
	if fe.af.FuncName == "had_changed" && hasStarArg(fe.af.Args) {
		if named, ok := state.(functions.NamedRowState); ok {
			ignoreNull := false
			if len(fe.af.Args) > 0 {
				ignoreNull = functions.AnalyticToBool(literalValue(fe.af.Args[0]))
			}
			result = named.ApplyNamed(ignoreNull, row)
			fe.lastResults[partKey] = result
			return result
		}
	}
	result = state.Apply(args)
	fe.lastResults[partKey] = result
	return result
}

// evaluateMultiColumn 处理 changed_cols 等多列函数：按 prefix+列名 扇出变化列。
func (fe *analyticFieldEngine) evaluateMultiColumn(s *Stream, row map[string]any) any {
	values, err := s.parseFunctionArgs(fe.af.Expression, row)
	if err != nil || values == nil {
		values = []any{}
	}
	// 位置参数：优先用已求值；"*" 致解析失败时用字面量还原 prefix/ignoreNull。
	argVal := func(idx int) any {
		if idx < len(values) {
			return values[idx]
		}
		if idx < len(fe.af.Args) {
			return literalValue(fe.af.Args[idx])
		}
		return nil
	}
	prefix, _ := argVal(0).(string)
	ignoreNull := functions.AnalyticToBool(argVal(1))
	cols := make(map[string]any)
	for i, nameExpr := range fe.af.Args[2:] {
		name := analyticColName(nameExpr)
		if name == "*" { // changed_cols(prefix, ignoreNull, *) → 整行各列
			for k, v := range row {
				cols[k] = v
			}
			continue
		}
		valIdx := 2 + i
		if valIdx < len(values) {
			// 窗口查询里内联聚合被重写为隐藏键：输出列名用显示名（如 avg → tavg）。
			out := name
			if d, ok := fe.af.InlineAggDisplay[name]; ok {
				out = d
			}
			cols[out] = values[valIdx]
		}
	}
	partKey := fe.partitionKey(row)
	fe.mu.Lock()
	defer fe.mu.Unlock()
	if fe.whenCond != nil && !fe.whenCond.Evaluate(row) {
		if last, ok := fe.lastResults[partKey]; ok {
			return last
		}
		return map[string]any{}
	}
	state := fe.getStateLocked(partKey)
	mcs, ok := state.(functions.MultiColumnState)
	if !ok {
		return map[string]any{}
	}
	result := mcs.ApplyColumns(prefix, ignoreNull, cols)
	fe.lastResults[partKey] = result
	return result
}

// hasStarArg 判断参数表达式列表是否含 "*"（整行展开）。
func hasStarArg(args []string) bool {
	for _, a := range args {
		if strings.TrimSpace(a) == "*" {
			return true
		}
	}
	return false
}

// expandStarArgs 把 "*" 之外的位置参数保留（如 ignoreNull），再追加整行各列的值，
// 供 had_changed(true, *) 等对整行求变化。args 为参数表达式，parsed 为已求值的位置值。
// 行值按键名排序，保证跨事件位置稳定（否则 map 迭代序乱会导致误判变化）。
// 当 parseFunctionArgs 因 "*" 失败导致 parsed 缺项时，用字面量还原标量参数。
func expandStarArgs(args []string, row map[string]any, parsed []any) []any {
	out := make([]any, 0, len(args)+len(row))
	for i, a := range args {
		if strings.TrimSpace(a) == "*" {
			continue
		}
		if i < len(parsed) {
			out = append(out, parsed[i])
		} else {
			out = append(out, literalValue(a))
		}
	}
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, row[k])
	}
	return out
}

// literalValue 解析标量字面量（true/false/数字/带引号字符串），用于 "*" 致解析失败时
// 还原 ignoreNull 等参数。
func literalValue(s string) any {
	s = strings.TrimSpace(s)
	switch s {
	case "true":
		return true
	case "false":
		return false
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	if len(s) >= 2 && (s[0] == '"' && s[len(s)-1] == '"' || s[0] == '\'' && s[len(s)-1] == '\'') {
		return s[1 : len(s)-1]
	}
	return s
}

// analyticColName 从列名表达式取输出列名：去反引号/引号，取限定符最后一段。
func analyticColName(expr string) string {
	s := strings.TrimSpace(expr)
	s = strings.Trim(s, "`")
	if len(s) >= 2 {
		first, last := s[0], s[len(s)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
			s = s[1 : len(s)-1]
		}
	}
	if dot := strings.LastIndex(s, "."); dot >= 0 {
		s = s[dot+1:]
	}
	return s
}

func (fe *analyticFieldEngine) getStateLocked(partKey string) functions.AnalyticState {
	if fe.af.Over == nil || len(fe.af.Over.PartitionBy) == 0 {
		if fe.noPart == nil {
			fe.noPart = fe.stateCtor()
		}
		return fe.noPart
	}
	if el, ok := fe.partitions[partKey]; ok {
		fe.lru.MoveToFront(el) // 命中：提升为最近使用
		return el.Value.(*partitionEntry).state
	}
	entry := &partitionEntry{key: partKey, state: fe.stateCtor()}
	fe.partitions[partKey] = fe.lru.PushFront(entry)
	// 超上限：淘汰最久未使用的分区，同步清理其 lastResults，防止内存泄漏。
	if fe.maxPartitions > 0 && fe.lru.Len() > fe.maxPartitions {
		if oldest := fe.lru.Back(); oldest != nil {
			oe := oldest.Value.(*partitionEntry)
			fe.lru.Remove(oldest)
			delete(fe.partitions, oe.key)
			delete(fe.lastResults, oe.key)
		}
	}
	return entry.state
}

func (fe *analyticFieldEngine) partitionKey(row map[string]any) string {
	if fe.af.Over == nil || len(fe.af.Over.PartitionBy) == 0 {
		return ""
	}
	var sb strings.Builder
	var lbuf [4]byte // 分区键片段长度（十进制，常见 < 1000）
	for _, k := range fe.af.Over.PartitionBy {
		tk := typeKey(row[k])
		// 长度前缀 + 尾分隔，避免值里含 '|' 或类型名导致跨列键碰撞。
		// 直接写 Builder，省去 fmt.Fprintf 的格式串解析。
		lstr := strconv.AppendInt(lbuf[:0], int64(len(tk)), 10)
		sb.Write(lstr)
		sb.WriteByte(':')
		sb.WriteString(tk)
		sb.WriteByte('|')
	}
	return sb.String()
}

// typeKey 生成 "类型|值" 形式的分区键片段，nil 记为 "nil|"。
// 常见标量走类型 switch 手搓字符串，避免 fmt.Sprintf 的反射开销；其余类型
// （切片/map/结构体等）退回 fmt.Sprintf 保证通用与 "%T|%v" 一致。
func typeKey(v any) string {
	switch x := v.(type) {
	case nil:
		return "nil|"
	case string:
		return "string|" + x
	case int:
		return "int|" + strconv.Itoa(x)
	case int64:
		return "int64|" + strconv.FormatInt(x, 10)
	case int32:
		return "int32|" + strconv.FormatInt(int64(x), 10)
	case float64:
		return "float64|" + strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		if x {
			return "bool|true"
		}
		return "bool|false"
	}
	return fmt.Sprintf("%T|%v", v, v)
}
