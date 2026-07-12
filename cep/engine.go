package cep

import (
	"container/list"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/types"
)

// 默认有界参数（边缘内存保护）。
const (
	defaultMaxPartitions = 10000 // 分区数上限（超出 LRU 淘汰）
	defaultMaxRunRows    = 10000 // 单次部分匹配的最大行数（行长上限）
	defaultMaxRuns       = 10000 // 单分区的活跃部分匹配数上限（数量上限，防 A* 类状态爆炸）
	maxInt64             int64 = 1<<63 - 1
)

// Engine 是一个 MATCH_RECOGNIZE 查询的 CEP 引擎：按分区维护 NFA 运行态，逐事件推进，
// 匹配完成时产出 MEASURES 投影行。线程安全（Process 串行；分区 LRU）。
type Engine struct {
	nfa      *NFA
	spec     *types.MatchRecognizeSpec
	symbols  map[string]bool
	measures []types.Measure

	// DEFINE/MEASURES 表达式预编译产物（NewEngine 期一次编译，热路径复用）。
	definePrep  map[string]*preparedExpr
	measurePrep []*preparedExpr

	tsField    string // ORDER BY 首字段，用作事件时间戳
	within     time.Duration
	maxRunRows int
	maxRuns    int

	maxPart int
	mu      sync.Mutex
	partMap map[string]*list.Element // 分区 LRU
	lru     *list.List
	seq     int64 // 全局单调到达序号（SKIP 起点跟踪用）

	log     logger.Logger // 求值诊断日志器（per-engine，消除包级竞争）
	errOnce sync.Map      // key: where+"\x00"+src，每表达式仅记一次求值失败（per-engine 有界）
}

type partition struct {
	key       string
	runs      []*run
	matchNo   int  // 本分区已输出匹配数（MATCH_NUMBER）
	nextStart int64 // 下一个允许起匹配的 seq（SKIP 策略）
}

// frame 是匹配历史的不可变节点（cons-list）：advance 仅 O(1) 追加，前缀天然共享，
// 消除旧实现里每后继全量复制 rows/labels 的 O(N²) 开销。
type frame struct {
	row   map[string]any
	label string
	prev  *frame
}

type run struct {
	states   []*state // 当前 epsilon 闭包状态集
	head     *frame   // 最近一行（链头）；nil 表示空匹配
	nrows    int
	startTs  int64
	startSeq int64
	matchNo  int // 输出时分配
}

// materialize 把 cons-list 反向展开为 rows+labels 切片（O(N)，仅在求值/投影时按需调用）。
func (r *run) materialize() ([]map[string]any, []string) {
	rows := make([]map[string]any, r.nrows)
	labels := make([]string, r.nrows)
	for f, i := r.head, r.nrows-1; f != nil; f, i = f.prev, i-1 {
		rows[i] = f.row
		labels[i] = f.label
	}
	return rows, labels
}

// Validate 校验 spec 可编译（模式树、ORDER BY、DEFINE/MEASURES 表达式）。供 Execute 期 fail-fast。
func Validate(spec *types.MatchRecognizeSpec) error {
	if spec == nil {
		return errPatternRequired
	}
	if spec.Pattern == nil {
		return errPatternRequired
	}
	if len(spec.OrderBy) == 0 {
		return errOrderByRequired
	}
	if _, err := Compile(spec.Pattern); err != nil {
		return err
	}
	// 表达式预编译校验：语法错的 DEFINE/MEASURES 在 Execute 即暴露，
	// 而非运行期静默按不匹配/空值处理（仅一次节流日志，难定位）。
	symbols := collectSymbols(spec)
	for _, d := range spec.Defines {
		if strings.TrimSpace(d.Cond) == "" {
			continue
		}
		if _, err := prepare(d.Cond, symbols); err != nil {
			return fmt.Errorf("DEFINE %s %q: %w", d.Symbol, d.Cond, err)
		}
	}
	for _, m := range spec.Measures {
		if strings.TrimSpace(m.Expr) == "" {
			continue
		}
		if _, err := prepare(m.Expr, symbols); err != nil {
			return fmt.Errorf("MEASURES %s %q: %w", m.Alias, m.Expr, err)
		}
	}
	return nil
}

// NewEngine 编译 spec 为 NFA 并构建引擎，预编译全部 DEFINE/MEASURES 表达式。
func NewEngine(spec *types.MatchRecognizeSpec) (*Engine, error) {
	if spec == nil || spec.Pattern == nil {
		return nil, errPatternRequired
	}
	if len(spec.OrderBy) == 0 {
		return nil, errOrderByRequired
	}
	nfa, err := Compile(spec.Pattern)
	if err != nil {
		return nil, err
	}
	symbols := collectSymbols(spec)
	definePrep := make(map[string]*preparedExpr)
	for _, d := range spec.Defines {
		if strings.TrimSpace(d.Cond) == "" {
			continue
		}
		p, err := prepare(d.Cond, symbols)
		if err != nil {
			return nil, fmt.Errorf("DEFINE %s %q: %w", d.Symbol, d.Cond, err)
		}
		definePrep[d.Symbol] = p
	}
	measurePrep := make([]*preparedExpr, len(spec.Measures))
	for i, m := range spec.Measures {
		p, err := prepare(m.Expr, symbols)
		if err != nil {
			return nil, fmt.Errorf("MEASURES %s %q: %w", m.Alias, m.Expr, err)
		}
		measurePrep[i] = p
	}
	e := &Engine{
		nfa:         nfa,
		spec:        spec,
		symbols:     symbols,
		measures:    spec.Measures,
		definePrep:  definePrep,
		measurePrep: measurePrep,
		tsField:     spec.OrderBy[0].Expression,
		within:      spec.Within,
		maxRunRows:  defaultMaxRunRows,
		maxRuns:     defaultMaxRuns,
		maxPart:     defaultMaxPartitions,
		partMap:     make(map[string]*list.Element),
		lru:         list.New(),
		log:         logger.GetDefault(),
	}
	if e.within <= 0 {
		e.within = types.DefaultMatchWithin
	}
	return e, nil
}

// SetMaxPartitions 覆盖分区上限。
func (e *Engine) SetMaxPartitions(n int) {
	if n > 0 {
		e.maxPart = n
	}
}

// SetMaxRunRows 覆盖单次部分匹配行数上限。
func (e *Engine) SetMaxRunRows(n int) {
	if n > 0 {
		e.maxRunRows = n
	}
}

// SetMaxRuns 覆盖单分区活跃部分匹配数上限。
func (e *Engine) SetMaxRuns(n int) {
	if n > 0 {
		e.maxRuns = n
	}
}

// SetLogger 覆盖求值诊断日志器（per-engine，避免包级共享导致多 Stream 互相覆盖/竞争）。
func (e *Engine) SetLogger(l logger.Logger) {
	if l != nil {
		e.log = l
	}
}

// logEvalErr 首次记录某表达式求值失败（per-engine 去重，条目数受该引擎表达式数有界）。
func (e *Engine) logEvalErr(where, src string, err error) {
	if err == nil || e.log == nil {
		return
	}
	if _, dup := e.errOnce.LoadOrStore(where+"\x00"+src, true); dup {
		return
	}
	e.log.Error("CEP %s 表达式求值失败，按不匹配/空值处理（同类后续不再重复）: %q: %v", where, src, err)
}

// Flush 冲刷所有分区中「已可接受但未终结」的部分匹配（如流末未界的 A+ 突发）。
// 适配器在 Stop 时调用，避免流末未闭合的匹配丢失。返回冲刷出的输出行。
func (e *Engine) Flush() []map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	var emitted []map[string]any
	for el := e.lru.Front(); el != nil; el = el.Next() {
		p := el.Value.(*partition)
		var completions []*run
		for _, r := range p.runs {
			if hasAccept(r.states) {
				completions = append(completions, r)
			}
		}
		if out := e.emitCompletions(p, completions, &[]*run{}); len(out) > 0 {
			emitted = append(emitted, out...)
		}
	}
	return emitted
}

// Process 投入一行事件到其分区，返回本事件完成的匹配经 MEASURES 投影后的输出行。
func (e *Engine) Process(row map[string]any, partitionKey string) []map[string]any {
	if row == nil {
		return nil
	}
	ts := normalizeTs(toInt64(row[e.tsField]))

	e.mu.Lock()
	defer e.mu.Unlock()
	e.seq++
	mrSeq := e.seq

	p := e.getPartition(partitionKey)
	emitted := e.step(p, row, ts, mrSeq)
	e.evictIfNeeded()
	return emitted
}

func (e *Engine) getPartition(key string) *partition {
	if el, ok := e.partMap[key]; ok {
		e.lru.MoveToFront(el)
		return el.Value.(*partition)
	}
	p := &partition{key: key}
	e.partMap[key] = e.lru.PushFront(p)
	return p
}

func (e *Engine) evictIfNeeded() {
	for e.maxPart > 0 && e.lru.Len() > e.maxPart {
		oldest := e.lru.Back()
		if oldest == nil {
			return
		}
		op := e.lru.Remove(oldest).(*partition)
		delete(e.partMap, op.key)
	}
}

// step 推进一个分区的全部 run，处理完成匹配（SKIP 策略）与种子。
func (e *Engine) step(p *partition, row map[string]any, ts, seq int64) []map[string]any {
	var survivors []*run
	var completions []*run

	// 1. 推进现有 run（含未界完成：mr 不属于但 run 已可接受）。
	for _, r := range p.runs {
		if !e.withinOk(r, ts) || r.nrows > e.maxRunRows {
			continue // 超期/超长：丢弃
		}
		succ := e.advance(r, row)
		if len(succ) == 0 {
			if hasAccept(r.states) {
				completions = append(completions, r) // 未界重复（A+/A*）收尾
			}
			continue
		}
		for _, s := range succ {
			if isComplete(s.states) {
				completions = append(completions, s)
			} else {
				survivors = append(survivors, s)
			}
		}
	}

	// 2. 种子：从起点尝试以本事件起始（受 nextStart 限制）。
	if seq >= p.nextStart {
		seed := &run{states: closure(e.nfa.start), startTs: ts, startSeq: seq}
		for _, s := range e.advance(seed, row) {
			if isComplete(s.states) {
				completions = append(completions, s)
			} else {
				survivors = append(survivors, s)
			}
		}
	}

	// 3. 按 SKIP 策略输出完成匹配并裁剪 survivors（消除重叠）。
	emitted := e.emitCompletions(p, completions, &survivors)
	// 4. 数量上限：活跃部分匹配过多时丢弃最旧的，防 A* 类模式状态爆炸。
	if e.maxRuns > 0 && len(survivors) > e.maxRuns {
		excess := len(survivors) - e.maxRuns
		survivors = survivors[excess:]
	}
	p.runs = survivors
	return emitted
}

// advance 测试 row 能否被 run 当前闭包里的各 match-state 消费，返回全部后继 run（非确定性）。
// 仅在符号的 DEFINE 含历史引用（PREV/聚合/符号字段）时才展开历史，否则 O(1)。
func (e *Engine) advance(r *run, row map[string]any) []*run {
	var out []*run
	seen := make(map[*state]bool)
	var buffer []map[string]any
	var labels []string
	materialized := false
	for _, m := range matchStates(r.states) {
		if seen[m] {
			continue
		}
		seen[m] = true
		p := e.definePrep[m.symbol]
		if p != nil && p.needsHist && !materialized {
			buffer, labels = r.materialize()
			materialized = true
		}
		if !e.evalDefine(p, buffer, labels, row, m.symbol) {
			continue
		}
		succ := &run{
			states:   closure(m.out1),
			head:     &frame{row: row, label: m.symbol, prev: r.head},
			nrows:    r.nrows + 1,
			startTs:  r.startTs,
			startSeq: r.startSeq,
		}
		out = append(out, succ)
	}
	return out
}

// evalDefine 求值符号的预编译 DEFINE 条件（布尔）。空条件恒真。
func (e *Engine) evalDefine(p *preparedExpr, buffer []map[string]any, labels []string, candidate map[string]any, candLabel string) bool {
	if p == nil || p.compiled == nil {
		return true
	}
	ctx := &matchCtx{rows: buffer, labels: labels, cur: len(buffer), candidate: candidate, candLabel: candLabel, symbols: e.symbols}
	v, isNull, err := evalPrepared(p, ctx)
	if err != nil {
		e.logEvalErr("DEFINE", p.src, err)
		return false
	}
	if isNull || v == nil {
		return false
	}
	return truthy(v)
}

// evalMeasure 求值预编译 MEASURES 表达式（值）。
func (e *Engine) evalMeasure(p *preparedExpr, rows []map[string]any, labels []string, cur, matchNumber int) (any, bool) {
	ctx := &matchCtx{rows: rows, labels: labels, cur: cur, symbols: e.symbols, matchNumber: matchNumber}
	v, isNull, err := evalPrepared(p, ctx)
	if err != nil {
		e.logEvalErr("MEASURES", p.src, err)
		return nil, true
	}
	return v, isNull
}

// emitCompletions 按 startSeq 升序处理完成匹配，返回全部投影输出并裁剪 survivors。
func (e *Engine) emitCompletions(p *partition, completions []*run, survivors *[]*run) []map[string]any {
	if len(completions) == 0 {
		return nil
	}
	sort.SliceStable(completions, func(i, j int) bool {
		return completions[i].startSeq < completions[j].startSeq
	})
	var emitted []map[string]any
	for _, c := range completions {
		if c.startSeq < p.nextStart {
			continue
		}
		p.matchNo++
		c.matchNo = p.matchNo
		emitted = append(emitted, e.project(c)...)
		p.nextStart = e.skipTo(c)
		e.pruneSurvivors(survivors, p.nextStart)
	}
	return emitted
}

// skipTo 返回该匹配完成后下一个允许起匹配的 seq（依 SKIP 策略）。
func (e *Engine) skipTo(c *run) int64 {
	endSeq := c.startSeq + int64(c.nrows) - 1
	switch e.spec.Skip {
	case types.SkipToNextRow:
		return c.startSeq + 1
	case types.SkipToFirst, types.SkipToLast, types.SkipToVariable:
		if s := seqOfLabel(c, e.spec.SkipSymbol, e.spec.Skip == types.SkipToFirst); s >= 0 {
			return s + 1
		}
	}
	return endSeq + 1
}

// seqOfLabel 返回匹配中某标签首/末出现行的全局 seq（沿 cons-list 遍历）。
func seqOfLabel(c *run, label string, first bool) int64 {
	if label == "" {
		return -1
	}
	idx := -1
	i := c.nrows - 1
	for f := c.head; f != nil; f, i = f.prev, i-1 {
		if f.label == label {
			if !first {
				return c.startSeq + int64(i) // LAST：最近（最先从 head 遇到）
			}
			idx = i // FIRST：记最小 i，走到最老
		}
	}
	if idx < 0 {
		return -1
	}
	return c.startSeq + int64(idx)
}

func (e *Engine) pruneSurvivors(survivors *[]*run, nextStart int64) {
	kept := (*survivors)[:0]
	for _, r := range *survivors {
		if r.startSeq >= nextStart {
			kept = append(kept, r)
		}
	}
	*survivors = kept
}

// project 把一次完成匹配投影为输出行：ONE ROW 在末行求值；ALL ROWS 逐行（RUNNING）求值。
func (e *Engine) project(c *run) []map[string]any {
	rows, labels := c.materialize()
	if e.spec.RowsPerMatch == types.RowsPerMatchAll {
		out := make([]map[string]any, 0, len(rows))
		for i := range rows {
			out = append(out, e.evalMeasures(c, rows, labels, i))
		}
		return out
	}
	if len(rows) == 0 {
		return nil
	}
	return []map[string]any{e.evalMeasures(c, rows, labels, len(rows)-1)}
}

// evalMeasures 投影一次匹配的输出行（对齐 Flink MATCH_RECOGNIZE 关系语义）：
//   - 无 MEASURES：输出当前行字段副本（ONE ROW=末行）。
//   - ONE ROW PER MATCH：仅 MEASURES 别名（关系只暴露 MEASURES 列）。
//   - ALL ROWS PER MATCH：输入行字段 + MEASURES 别名（同名以 MEASURES 为准）。
func (e *Engine) evalMeasures(c *run, rows []map[string]any, labels []string, cur int) map[string]any {
	row := rows[cur]
	if len(e.measures) == 0 {
		return copyRow(row)
	}
	apply := func(out map[string]any) {
		for i, m := range e.measures {
			v, _ := e.evalMeasure(e.measurePrep[i], rows, labels, cur, c.matchNo)
			out[m.Alias] = v
		}
	}
	if e.spec.RowsPerMatch == types.RowsPerMatchAll {
		out := copyRow(row)
		apply(out)
		return out
	}
	out := make(map[string]any, len(e.measures))
	apply(out)
	return out
}

func copyRow(r map[string]any) map[string]any {
	out := make(map[string]any, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}

// withinOk 报告 run 是否仍在 WITHIN 时窗内。
// 限制：WITHIN 为被动检查——仅在事件到达推进 run 时判定，无主动定时器。
// 故空闲分区的部分匹配不会因超时即时清除，需等下一事件到达或分区 LRU 淘汰。
// （内存仍受 maxRuns×maxRunRows×maxPartitions 有界，非泄漏。）
func (e *Engine) withinOk(r *run, curTs int64) bool {
	if e.within <= 0 {
		return true
	}
	return curTs-r.startTs <= e.within.Nanoseconds()
}

// collectSymbols 收集模式中全部模式变量名。
func collectSymbols(spec *types.MatchRecognizeSpec) map[string]bool {
	m := make(map[string]bool)
	if spec.Pattern != nil {
		walkSymbols(spec.Pattern, m)
	}
	return m
}

func walkSymbols(n *types.PatternNode, m map[string]bool) {
	if n == nil {
		return
	}
	if n.Kind == types.PatternLiteral {
		m[n.Symbol] = true
		return
	}
	for _, c := range n.Children {
		walkSymbols(c, m)
	}
}

// normalizeTs 把事件时间戳归一化为纳秒（自动判单位：ns/μs/ms/s epoch）。
// 注意：按数值量级猜单位，边界值（如跨 1e9）可能误判；建议用真实 epoch 时间戳，
// 或同一条流内单位一致。相对小序号（<1e9）按原值比较，WITHIN 须同单位。
// 乘法前做溢出钳制，避免大时间戳回绕为负破坏 WITHIN 判定。
func normalizeTs(v int64) int64 {
	switch {
	case v <= 0:
		return 0
	case v >= 1e18:
		return v
	case v >= 1e15: // 微秒
		if v > maxInt64/1000 {
			return maxInt64
		}
		return v * 1000
	case v >= 1e12: // 毫秒
		if v > maxInt64/1000000 {
			return maxInt64
		}
		return v * 1000000
	case v >= 1e9: // 秒
		if v > maxInt64/1000000000 {
			return maxInt64
		}
		return v * 1000000000
	}
	return v // 小值（相对序号）：原样，WITHIN 按同单位比较
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int64:
		return x
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	case float32:
		return int64(x)
	}
	return 0
}

// 预定义错误（避免在热路径里 fmt）。
var (
	errPatternRequired = cepError("MATCH_RECOGNIZE requires a PATTERN clause")
	errOrderByRequired = cepError("MATCH_RECOGNIZE requires ORDER BY (provides event ordering)")
)

type cepError string

func (e cepError) Error() string { return string(e) }
