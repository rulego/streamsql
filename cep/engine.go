package cep

import (
	"container/list"
	"context"
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
	subsets  map[string][]string // SUBSET 名 → 成员符号（求值期按成员集合过滤）
	lazy     bool                // pattern 含 reluctant 量词：立即 emit 选最短；否则贪婪延迟选最长
	measures []types.Measure

	// DEFINE/MEASURES 表达式预编译产物（NewEngine 期一次编译，热路径复用）。
	definePrep  map[string]*preparedExpr
	measurePrep []*preparedExpr

	tsField    string // ORDER BY 首字段，用作事件时间戳
	within     time.Duration
	maxRunRows int
	maxRuns    int

	// WITHIN 主动过期 sweeper（仅 within>0 时由 Start 启动；Stop join）。
	startMu       sync.Mutex
	started       bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	sweepInterval time.Duration

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
	pending   map[int64][]*run // 贪婪：已完成 run 按 startSeq 暂存，等延伸终止选最长 emit
	matchNo   int              // 本分区已输出匹配数（MATCH_NUMBER）
	nextStart int64            // 下一个允许起匹配的 seq（SKIP 策略）
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
	symbols, _, pattern, err := resolveSymbols(spec)
	if err != nil {
		return err
	}
	if _, err := Compile(pattern); err != nil {
		return err
	}
	// 表达式预编译校验：语法错的 DEFINE/MEASURES 在 Execute 即暴露，
	// 而非运行期静默按不匹配/空值处理（仅一次节流日志，难定位）。
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
	symbols, subsets, pattern, err := resolveSymbols(spec)
	if err != nil {
		return nil, err
	}
	nfa, err := Compile(pattern)
	if err != nil {
		return nil, err
	}
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
		subsets:     subsets,
		lazy:        hasReluctant(spec.Pattern),
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
	e.sweepInterval = e.within / 2
	if e.sweepInterval < 50*time.Millisecond {
		e.sweepInterval = 50 * time.Millisecond // 下限：防极小 WITHIN 产生过密 ticker 占 CPU
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
		if e.lazy {
			if out := e.emitLazy(p, completions, &[]*run{}); len(out) > 0 {
				emitted = append(emitted, out...)
			}
			continue
		}
		// 贪婪：并入 pending（只留最长），流末 survivors 空 → 全部 ready emit。
		e.ingestPending(p, completions)
		if out := e.emitGreedy(p, &[]*run{}); len(out) > 0 {
			emitted = append(emitted, out...)
		}
	}
	return emitted
}

// Start 启动 WITHIN 主动过期 sweeper（仅 within>0 时）。幂等。
// sweeper 定期用 wall-clock 扫描，清掉超窗的部分匹配，避免空闲分区内存滞留。
func (e *Engine) Start() {
	e.startMu.Lock()
	defer e.startMu.Unlock()
	if e.started || e.within <= 0 || e.sweepInterval <= 0 {
		return
	}
	e.ctx, e.cancel = context.WithCancel(context.Background())
	e.started = true
	e.wg.Add(1)
	go e.sweepLoop()
}

func (e *Engine) sweepLoop() {
	defer e.wg.Done()
	t := time.NewTicker(e.sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			e.sweep()
		case <-e.ctx.Done():
			return
		}
	}
}

// sweep 清除超窗的部分匹配。仅对 epoch 量级时间戳用 wall-clock 判过期：
// 小值序号流（非真实时间戳）维持纯被动，避免 wall-clock 误删。
func (e *Engine) sweep() {
	if e.within <= 0 {
		return
	}
	now := time.Now().UnixNano()
	limit := e.within.Nanoseconds()
	e.mu.Lock()
	for el := e.lru.Front(); el != nil; el = el.Next() {
		p := el.Value.(*partition)
		kept := make([]*run, 0, len(p.runs))
		for _, r := range p.runs {
			if r.startTs >= int64(1e9) && now-r.startTs > limit {
				continue // 超窗的延伸中 run：丢弃
			}
			kept = append(kept, r)
		}
		p.runs = kept
		// pending（已完成的合法匹配）不由 sweep 清理：sweep 用 wall-clock、withinOk 用事件时间，
		// 清 pending 会误杀事件时间窗内的合法匹配。pending 仅由 emitGreedy/Flush 产出、capPending 限界。
	}
	e.mu.Unlock()
}

// Stop 停止 sweeper 并 join。幂等；未 Start 时直接返回。
func (e *Engine) Stop() {
	e.startMu.Lock()
	if !e.started {
		e.startMu.Unlock()
		return
	}
	e.started = false
	cancel := e.cancel
	e.startMu.Unlock()
	cancel()
	e.wg.Wait()
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
	p := &partition{key: key, pending: make(map[int64][]*run)}
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

	// 3. 处理完成匹配：懒惰立即 emit 选最短；贪婪暂存 pending，等延伸终止选最长。
	var emitted []map[string]any
	if e.lazy {
		emitted = e.emitLazy(p, completions, &survivors)
	} else {
		e.ingestPending(p, completions)
		emitted = e.emitGreedy(p, &survivors)
	}
	// 4. 数量上限：活跃部分匹配过多时丢弃最旧的，防 A* 类模式状态爆炸。
	if e.maxRuns > 0 && len(survivors) > e.maxRuns {
		excess := len(survivors) - e.maxRuns
		survivors = survivors[excess:]
	}
	e.capPending(p) // pending key 数上限：防贪婪延迟期无界累积
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
	ctx := &matchCtx{rows: buffer, labels: labels, cur: len(buffer), candidate: candidate, candLabel: candLabel, symbols: e.symbols, subsets: e.subsets}
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
	ctx := &matchCtx{rows: rows, labels: labels, cur: cur, symbols: e.symbols, subsets: e.subsets, matchNumber: matchNumber}
	v, isNull, err := evalPrepared(p, ctx)
	if err != nil {
		e.logEvalErr("MEASURES", p.src, err)
		return nil, true
	}
	return v, isNull
}

// emitLazy 处理懒惰模式的完成匹配：立即按 startSeq 升序、同 startSeq 选最短 emit。
func (e *Engine) emitLazy(p *partition, completions []*run, survivors *[]*run) []map[string]any {
	if len(completions) == 0 {
		return nil
	}
	sort.SliceStable(completions, func(i, j int) bool {
		if completions[i].startSeq != completions[j].startSeq {
			return completions[i].startSeq < completions[j].startSeq
		}
		return completions[i].nrows < completions[j].nrows // 懒惰：同 startSeq 选最短
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

// emitGreedy 处理贪婪模式的完成匹配：pending 已按 startSeq 暂存，emit 延伸终止的
// startSeq（survivors 中无同 startSeq 的 run），同 startSeq 选最长。survivors 为空时
// emit 全部 pending（供 Flush）。
func (e *Engine) emitGreedy(p *partition, survivors *[]*run) []map[string]any {
	if len(p.pending) == 0 {
		return nil // 默认贪婪模式每事件调用：无在途匹配时短路，避免无用 map 分配
	}
	active := make(map[int64]bool, len(*survivors))
	for _, r := range *survivors {
		active[r.startSeq] = true
	}
	var ready []int64
	for s := range p.pending {
		if !active[s] && s >= p.nextStart {
			ready = append(ready, s)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
	var emitted []map[string]any
	for _, s := range ready {
		cands := p.pending[s]
		if len(cands) == 0 {
			continue // 可能被前一轮 SKIP 的 prunePending 删除
		}
		best := cands[0] // ingestPending 入队时已只保留最长
		p.matchNo++
		best.matchNo = p.matchNo
		emitted = append(emitted, e.project(best)...)
		p.nextStart = e.skipTo(best)
		delete(p.pending, s)
		e.prunePending(p, p.nextStart)
		e.pruneSurvivors(survivors, p.nextStart)
	}
	return emitted
}

// prunePending 清除 startSeq < nextStart 的暂存完成匹配（已被 SKIP 跳过）。
func (e *Engine) prunePending(p *partition, nextStart int64) {
	for s := range p.pending {
		if s < nextStart {
			delete(p.pending, s)
		}
	}
}

// ingestPending 把 completion 入队 pending（贪婪）：每 startSeq 只保留最长 completion
// （emitGreedy 最终选最长，短的无需保留），供 step 与 Flush 共用。
func (e *Engine) ingestPending(p *partition, completions []*run) {
	for _, c := range completions {
		if c.startSeq < p.nextStart {
			continue
		}
		cur := p.pending[c.startSeq]
		if len(cur) == 0 || c.nrows > cur[0].nrows {
			p.pending[c.startSeq] = []*run{c}
		}
	}
}

// capPending 限制 pending key 数（未 emit 的 startSeq 数），超限时丢弃最旧 startSeq。
// 贪婪延迟 emit 期 startSeq 可能累积，此为内存保护（牺牲最旧匹配换内存有界）。
func (e *Engine) capPending(p *partition) {
	if e.maxRuns <= 0 {
		return
	}
	for len(p.pending) > e.maxRuns {
		var oldest int64 = maxInt64
		for s := range p.pending {
			if s < oldest {
				oldest = s
			}
		}
		delete(p.pending, oldest)
	}
}

// skipTo 返回该匹配完成后下一个允许起匹配的 seq（依 SKIP 策略）。
func (e *Engine) skipTo(c *run) int64 {
	endSeq := c.startSeq + int64(c.nrows) - 1
	switch e.spec.Skip {
	case types.SkipToNextRow:
		return c.startSeq + 1
	case types.SkipToFirst, types.SkipToLast, types.SkipToVariable:
		if s := seqOfLabel(c, e.spec.SkipSymbol, e.spec.Skip == types.SkipToFirst, e.subsets); s >= 0 {
			return s + 1
		}
	}
	return endSeq + 1
}

// seqOfLabel 返回匹配中某标签（或 SUBSET 的任一成分）首/末出现行的全局 seq。
func seqOfLabel(c *run, label string, first bool, subsets map[string][]string) int64 {
	if label == "" {
		return -1
	}
	idx := -1
	i := c.nrows - 1
	for f := c.head; f != nil; f, i = f.prev, i-1 {
		if labelMatches(f.label, label, subsets) {
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

// hasReluctant 报告模式树是否含懒惰量词（Quantifier.Greedy=false）。
// 整体近似：只要含任意 reluctant 量词，整引擎走 emitLazy（同 startSeq 选最短）；
// 混合贪婪/懒惰量词的模式不保证逐量词优先级（纯贪婪/纯懒惰正确）。
func hasReluctant(n *types.PatternNode) bool {
	if n == nil {
		return false
	}
	if n.Kind == types.PatternRepetition && n.Quant != nil && !n.Quant.Greedy {
		return true
	}
	for _, c := range n.Children {
		if hasReluctant(c) {
			return true
		}
	}
	return false
}

// collectSubsets 收集 SUBSET 名 → 成员符号列表。
func collectSubsets(spec *types.MatchRecognizeSpec) map[string][]string {
	m := make(map[string][]string, len(spec.Subsets))
	for _, s := range spec.Subsets {
		m[s.Name] = s.Symbols
	}
	return m
}

// maxSubsetMembers 限制单个 SUBSET 的成员数，防止展开成交替后 NFA 状态膨胀。
const maxSubsetMembers = 8

// expandSubsets 把模式树中的 SUBSET 名（PatternLiteral）展开为其成员的交替：
// PATTERN(S)（S={A,B}）→ PATTERN(A|B)。展开后 match-state 携带真实成分符号，
// CLASSIFIER() 返回成分而非 SUBSET 名（SQL 标准）。
// 传入的 subsets 已扁平化（flattenMembers）、通过环检测（subsetHasCycle）与成员数校验
// （resolveSymbols），成员均为真实模式变量，无需递归或防环。
func expandSubsets(n *types.PatternNode, subsets map[string][]string) *types.PatternNode {
	return expandSubsetsRec(n, subsets)
}

func expandSubsetsRec(n *types.PatternNode, subsets map[string][]string) *types.PatternNode {
	if n == nil {
		return nil
	}
	if n.Kind == types.PatternLiteral {
		members, ok := subsets[n.Symbol]
		if !ok {
			return n // 普通符号：原样
		}
		children := make([]*types.PatternNode, 0, len(members))
		for _, m := range members {
			children = append(children, &types.PatternNode{Kind: types.PatternLiteral, Symbol: m})
		}
		if len(children) == 1 {
			return children[0]
		}
		return &types.PatternNode{Kind: types.PatternAlternation, Children: children}
	}
	out := *n // 浅拷贝，不改原树
	if len(n.Children) > 0 {
		out.Children = make([]*types.PatternNode, len(n.Children))
		for i, c := range n.Children {
			out.Children[i] = expandSubsetsRec(c, subsets)
		}
	}
	return &out
}

// resolveSymbols 展开 SUBSET、合并符号集，供 NewEngine/Validate 共用。
// 返回：prepare 用的符号集（PATTERN literal ∪ DEFINE 声明 ∪ SUBSET 名）、SUBSET 成员表
// （已扁平化为真实模式变量，求值期等值比较即可）、展开后的 pattern。
func resolveSymbols(spec *types.MatchRecognizeSpec) (map[string]bool, map[string][]string, *types.PatternNode, error) {
	subsets := collectSubsets(spec)
	// 已知符号 = PATTERN literal ∪ DEFINE 声明 ∪ SUBSET 名。
	known := collectSymbols(spec)
	for _, d := range spec.Defines {
		known[d.Symbol] = true
	}
	for name := range subsets {
		known[name] = true
	}
	// 成员校验：必须是已知符号，且成员数受 maxSubsetMembers 限制（防 NFA 交替膨胀）。
	for name, members := range subsets {
		if len(members) > maxSubsetMembers {
			return nil, nil, nil, fmt.Errorf("SUBSET %q has too many members (%d > %d)", name, len(members), maxSubsetMembers)
		}
		for _, m := range members {
			if !known[m] {
				return nil, nil, nil, fmt.Errorf("SUBSET %q references unknown symbol %q", name, m)
			}
		}
	}
	// 环检测：SUBSET 依赖图不得有环。
	for name := range subsets {
		if subsetHasCycle(name, subsets, map[string]bool{}) {
			return nil, nil, nil, fmt.Errorf("SUBSET %q has a cyclic definition", name)
		}
	}
	// 扁平化：把成员里的 SUBSET 名递归展开为真实模式变量，求值期无需再递归。
	flat := make(map[string][]string, len(subsets))
	for name, members := range subsets {
		flat[name] = flattenMembers(members, subsets)
	}
	return known, flat, expandSubsets(spec.Pattern, flat), nil
}

// subsetHasCycle 沿 SUBSET 依赖图 DFS 检测环（path 回溯）。
func subsetHasCycle(name string, subsets map[string][]string, path map[string]bool) bool {
	if path[name] {
		return true
	}
	path[name] = true
	for _, m := range subsets[name] {
		if _, ok := subsets[m]; ok && subsetHasCycle(m, subsets, path) {
			return true
		}
	}
	delete(path, name)
	return false
}

// flattenMembers 把成员里的 SUBSET 名递归展开为真实模式变量（去重；已通过环检测）。
func flattenMembers(members []string, subsets map[string][]string) []string {
	var out []string
	seen := make(map[string]bool, len(members))
	var rec func(ms []string)
	rec = func(ms []string) {
		for _, m := range ms {
			if sub, ok := subsets[m]; ok {
				rec(sub)
			} else if !seen[m] {
				seen[m] = true
				out = append(out, m)
			}
		}
	}
	rec(members)
	return out
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
