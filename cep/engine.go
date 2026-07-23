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

// Default bounded parameters (edge memory protection).
const (
	defaultMaxPartitions       = 10000 // Partition limit (exceeding LRU elimination)
	defaultMaxRunRows          = 10000 // Maximum number of rows per partial match (line length limit)
	defaultMaxRuns             = 10000 // Maximum number of matches for active portions in a single partition (maximum quantity, prevents A* status explosion)
	maxInt64             int64 = 1<<63 - 1
)

// Engine is a CEP engine for MATCH_RECOGNIZE queries: maintaining NFA runtime by partition and advancing event by event,
// Produces the MEASURES projection row upon matching. Thread safety (Process serialization; Partition LRU).
type Engine struct {
	nfa      *NFA
	spec     *types.MatchRecognizeSpec
	symbols  map[string]bool
	subsets  map[string][]string // SUBSET Name → Member Symbol (Evaluate by member set)
	lazy     bool                // pattern contains reluctant quantifiers: immediately emit choose the shortest; Otherwise, choose the longest delay for greed
	measures []types.Measure

	// DEFINE/MEASURES expression precompilation product (NewEngine phase one-time compilation, hotpath multiplexing).
	definePrep  map[string]*preparedExpr
	measurePrep []*preparedExpr

	tsField    string // ORDER BY first field, used as the event timestamp
	within     time.Duration
	maxRunRows int
	maxRuns    int

	// WITHIN Actively Expired Sweeper (only starts with Start when within>0 is used; Stop join).
	startMu       sync.Mutex
	started       bool
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	sweepInterval time.Duration

	maxPart int
	mu      sync.Mutex
	partMap map[string]*list.Element // Partitioned LRU
	lru     *list.List
	seq     int64 // Global Monotone Arrival Number (SKIP for starting point tracking)

	log     logger.Logger // Evaluate diagnostic loggers (per-engine, eliminate packet-level contention)
	errOnce sync.Map      // key: where+"\x00"+src, each expression only records one evaluation failure (per-engine is bounded)
}

type partition struct {
	key       string
	runs      []*run
	pending   map[int64][]*run // Greed: Run completed, press startSeq to temporarily store, wait for extension termination and select the longest emit
	matchNo   int              // Number of matches output in this partition (MATCH_NUMBER)
	nextStart int64            // Next to allow matching seq (SKIP strategy)
}

// frame is an immutable node (cons-list) that matches history: advance only adds O(1), and prefixes are naturally shared,
// Eliminates the O(N²) overhead of coping rows/labels in the old implementation every successor.
type frame struct {
	row   map[string]any
	label string
	prev  *frame
}

type run struct {
	states   []*state // The current epsilon closure state set
	head     *frame   // Nearest line (chain head); nil stands for empty match
	nrows    int
	startTs  int64
	startSeq int64
	matchNo  int // Distribution at output
}

// materialize unfolds cons-list backward into rows+labels slices (O(N), called only as needed during evaluation/projection).
func (r *run) materialize() ([]map[string]any, []string) {
	rows := make([]map[string]any, r.nrows)
	labels := make([]string, r.nrows)
	for f, i := r.head, r.nrows-1; f != nil; f, i = f.prev, i-1 {
		rows[i] = f.row
		labels[i] = f.label
	}
	return rows, labels
}

// Validate Validation specs can be compiled (schema tree, ORDER BY, DEFINE/MEASURES expressions). Allows the Execute period to fail-fast.
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
	// Expression precompilation verification: Syntax errors DEFINE/MEASURES are exposed in Execute,
	// Non-runtime silence is treated as mismatch/null (only one throttling log, difficult to locate).
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

// NewEngine compiles specs as NFA and builds the engine, precompiling all DEFINE/MEASURES expressions.
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
		e.sweepInterval = 50 * time.Millisecond // Lower limit: Prevents extremely small WITHIN from generating overly dense tickers that occupy CPU
	}
	return e, nil
}

// SetMaxPartitions override the partition limit.
func (e *Engine) SetMaxPartitions(n int) {
	if n > 0 {
		e.maxPart = n
	}
}

// SetMaxRunRows overrides the maximum number of rows in a single partial match.
func (e *Engine) SetMaxRunRows(n int) {
	if n > 0 {
		e.maxRunRows = n
	}
}

// SetMaxRuns covers the maximum number of matches for active parts of a single partition.
func (e *Engine) SetMaxRuns(n int) {
	if n > 0 {
		e.maxRuns = n
	}
}

// SetLogger overrides the evaluation diagnostic logger (per-engine, preventing packet-level sharing that could overwrite or compete with multiple streams).
func (e *Engine) SetLogger(l logger.Logger) {
	if l != nil {
		e.log = l
	}
}

// logEvalErr records the first instance of an expression evaluation failure (per-engine deduplication, entry count is bounded by the number of expressions in that engine).
func (e *Engine) logEvalErr(where, src string, err error) {
	if err == nil || e.log == nil {
		return
	}
	if _, dup := e.errOnce.LoadOrStore(where+"\x00"+src, true); dup {
		return
	}
	e.log.Error("CEP %s Expression evaluation fails, treated as mismatch/null (similar type will not repeat later): %q: %v", where, src, err)
}

// Flush flushes some 'acceptable but not yet finished' matches in all partitions (such as the A+ burst at the end of the flow).
// The adapter is called on Stop to prevent loss of matches at the end of the stream. Returns the output line that was flushed.
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
		// Greed: Merge into pending (keep only the longest part), end of the stream survivors empty → all ready emit.
		e.ingestPending(p, completions)
		if out := e.emitGreedy(p, &[]*run{}); len(out) > 0 {
			emitted = append(emitted, out...)
		}
	}
	return emitted
}

// Start WITHIN to activate the active expired sweeper (only when within>0). Power equal.
// The sweeper periodically scans with a wall-clock to clear some matches from the overwindow, preventing memory retention in idle partitions.
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

// sweep to clear partial matches in the superwindow. Only epoch-scale timestamps are marked as expired using wall-clock:
// Small-value sequence number flow (not real timestamp) remains purely passive to avoid wall-clock accidental deletion.
func (e *Engine) sweep() {
	if e.within <= 0 {
		return
	}
	now := time.Now().UnixNano()
	limit := e.within.Nanoseconds()
	e.mu.Lock()
	for el := e.lru.Front(); el != nil; el = el.Next() {
		p := el.Value.(*partition)
		kept := p.runs[:0] // In-place filtering: writes index ≤ reads index; no assignment is made if not deleted
		for _, r := range p.runs {
			if r.startTs >= int64(1e9) && now-r.startTs > limit {
				continue // In the extension of the superwindow, run: discard
			}
			kept = append(kept, r)
		}
		p.runs = kept
		// pending (completed valid matches) is not cleaned by sweep: sweep uses wall-clock, withinOk uses event time,
		// Clearing pending will falsely kill legal matches within the event time window. Pending is only produced by emitGreedy/Flush and capPending limits.
	}
	e.mu.Unlock()
}

// Stop the sweeper and join. idempotent; Returns directly when not started.
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

// Process feeds a line of event to its partition, returning the output line of the match completed by this event after the MEASURES projection.
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

// step Advance all runs of a partition to complete matching (SKIP strategy) with seeds.
func (e *Engine) step(p *partition, row map[string]any, ts, seq int64) []map[string]any {
	var survivors []*run
	var completions []*run

	// 1. Advance existing runs (including unbounded completion: mr is not included but run is acceptable).
	for _, r := range p.runs {
		if !e.withinOk(r, ts) || r.nrows > e.maxRunRows {
			continue // Overdue/Overtime: Discarded
		}
		succ := e.advance(r, row)
		if len(succ) == 0 {
			if hasAccept(r.states) {
				completions = append(completions, r) // Unbounded repeat (A+/A*) for the finish
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

	// 2. Seed: Try to start from the starting point with this event (restricted by nextStart).
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

	// 3. After processing and matching: Lazy immediately emit and select the shortest option; Greedy Temporary Storage Pending, wait for extension termination and choose the longest option.
	var emitted []map[string]any
	if e.lazy {
		emitted = e.emitLazy(p, completions, &survivors)
	} else {
		e.ingestPending(p, completions)
		emitted = e.emitGreedy(p, &survivors)
	}
	// 4. Quantity limit: If the active group matches too much, the oldest one is discarded to prevent A* mode status explosions.
	if e.maxRuns > 0 && len(survivors) > e.maxRuns {
		excess := len(survivors) - e.maxRuns
		survivors = survivors[excess:]
	}
	e.capPending(p) // Pending key limit: Unlimited accumulation during anti-greed delay period
	p.runs = survivors
	return emitted
}

// advance tests whether the row can be consumed by the match-states in the current closure, returning all subsequent runs (non-deterministic).
// History is only expanded when the symbol DEFINE contains historical references (PREV/aggregation/symbol fields); otherwise, O(1).
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

// precompilation of evalDefine evaluation symbols DEFINE condition (Boolean). The empty condition is always true.
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

// evalMeasure evaluates the precompiled MEASURES expression (value).
func (e *Engine) evalMeasure(p *preparedExpr, rows []map[string]any, labels []string, cur, matchNumber int) (any, bool) {
	ctx := &matchCtx{rows: rows, labels: labels, cur: cur, symbols: e.symbols, subsets: e.subsets, matchNumber: matchNumber}
	v, isNull, err := evalPrepared(p, ctx)
	if err != nil {
		e.logEvalErr("MEASURES", p.src, err)
		return nil, true
	}
	return v, isNull
}

// emitOne outputs a single match: assign MATCH_NUMBER, project, set nextStart by SKIP, crop survivors.
// emitLazy/emitGreedy are shared to ensure the output backbone of both paths is consistent.
func (e *Engine) emitOne(p *partition, c *run, survivors *[]*run) []map[string]any {
	p.matchNo++
	c.matchNo = p.matchNo
	out := e.project(c)
	p.nextStart = e.skipTo(c)
	e.pruneSurvivors(survivors, p.nextStart)
	return out
}

// emitLazy handles the completion matching of lazy mode: immediately ascend by startSeq and select the shortest emit with startSeq.
func (e *Engine) emitLazy(p *partition, completions []*run, survivors *[]*run) []map[string]any {
	if len(completions) == 0 {
		return nil
	}
	sort.SliceStable(completions, func(i, j int) bool {
		if completions[i].startSeq != completions[j].startSeq {
			return completions[i].startSeq < completions[j].startSeq
		}
		return completions[i].nrows < completions[j].nrows // Laziness: Use startSeq to choose the shortest option
	})
	var emitted []map[string]any
	for _, c := range completions {
		if c.startSeq < p.nextStart {
			continue
		}
		emitted = append(emitted, e.emitOne(p, c, survivors)...)
	}
	return emitted
}

// emitGreedy handles the completion match for greedy mode: pending has been temporarily stored with startSeq (leaving only the longest time), emit extends
// Terminated startSeq (there is no run with the same startSeq in survivors). survivors are empty when all are emit (for Flush).
func (e *Engine) emitGreedy(p *partition, survivors *[]*run) []map[string]any {
	if len(p.pending) == 0 {
		return nil // By default, greedy mode calls per event: short-circuited when no in-transit matching occurs, avoiding useless map allocation
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
		if s < p.nextStart {
			continue // Skipped by the previous SKIP push (direct guard, same as emitLazy)
		}
		best := p.pending[s][0]
		emitted = append(emitted, e.emitOne(p, best, survivors)...)
		delete(p.pending, s)
	}
	e.prunePending(p, p.nextStart)
	return emitted
}

// prunePending clears the startSeq < nextStart temporary completion match (skipped by SKIP).
func (e *Engine) prunePending(p *partition, nextStart int64) {
	for s := range p.pending {
		if s < nextStart {
			delete(p.pending, s)
		}
	}
}

// ingestPending enqueues completion pending: Each startSeq only retains the longest completion
// (emitGreedy finally selects the longest part, shortest option is not kept), for use by step and Flush for sharing.
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

// capPending limits the number of pending keys (the number of startSeq not emitted); if it exceeds the limit, the oldest startSeq is discarded.
// Greedy delay and emit periods may accumulate startSeq during this period, which is memory protection (sacrificing the oldest match for a bounded memory).
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

// skipTo returns the next seq that allows matching after the match is completed (according to the SKIP policy).
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

// seqOfLabel returns the global seq for the first and last lines of a tag (or any component of the SUBSET) in the match.
func seqOfLabel(c *run, label string, first bool, subsets map[string][]string) int64 {
	if label == "" {
		return -1
	}
	idx := -1
	i := c.nrows - 1
	for f := c.head; f != nil; f, i = f.prev, i-1 {
		if labelMatches(f.label, label, subsets) {
			if !first {
				return c.startSeq + int64(i) // LAST: Recently (first encountered from head)
			}
			idx = i // FIRST: Remember the smallest i, walk the oldest one
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

// project projects a single completed matching projection as the output line: ONE ROW evaluates on the last row; ALL ROWS RUNNINGs for evaluation.
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

// evalMeasures projects a matching output line (aligning Flink MATCH_RECOGNIZE relational semantics):
//   - No MEASURES: Outputs a copy of the current row field (ONE ROW = last line).
//   - ONE ROW PER MATCH: Only MEASURES aliases (the relationship only exposes the MEASURES column).
//   - ALL ROWS PER MATCH: Enter row field + MEASURES aliases (MEASURES for the same name).
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

// withinOk reports whether run is still within the WITHIN window.
// Limitation: WITHIN is a passive check—only determined when the event arrives at the advance run, with no active timer.
// Therefore, partial matches in idle partitions will not be cleared immediately due to timeout; they must wait for the next event or partition LRU to be eliminated.
// (Memory is still bounded by maxRuns×maxRunRows×maxPartitions and is not leaked.))
func (e *Engine) withinOk(r *run, curTs int64) bool {
	if e.within <= 0 {
		return true
	}
	return curTs-r.startTs <= e.within.Nanoseconds()
}

// collectSymbols: All variable names in the collection mode.
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

// hasReluctant reports whether the pattern tree contains a laziness quantifier (Quantifier.Greedy=false).
// Overall approximation: As long as any reluctant quantifier is included, the entire engine runs emitLazy (same as startSeq with the shortest option);
// The mixed greed/laziness quantifier pattern does not guarantee per-quantifier priority (pure greed/pure laziness is correct).
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

// collectSubsets Collects a list of SUBSET names → member symbols.
func collectSubsets(spec *types.MatchRecognizeSpec) map[string][]string {
	m := make(map[string][]string, len(spec.Subsets))
	for _, s := range spec.Subsets {
		m[s.Name] = s.Symbols
	}
	return m
}

// maxSubsetMembers limits the number of members in a single SUBSET to prevent NFA state expansion after expansion and trade.
const maxSubsetMembers = 8

// expandSubsets expands the SUBSET name (PatternLiteral) in the pattern tree into the alternation of its members:
// PATTERN(S) (S={A,B})→ PATTERN(A| B). After expansion, match-state carries the true component symbol,
// CLASSIFIER() returns the component instead of the SUBSET name (SQL standard).
// Incoming subsets have been flattened (flattenMembers), passed loop detection (subsetHasCycle), and member count validation
// (resolveSymbols), all members are real pattern variables, no need for recursion or anti-looping.
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
			return n // Regular symbols: as is
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
	out := *n // Shallow copying, no original tree changes
	if len(n.Children) > 0 {
		out.Children = make([]*types.PatternNode, len(n.Children))
		for i, c := range n.Children {
			out.Children[i] = expandSubsetsRec(c, subsets)
		}
	}
	return &out
}

// resolveSymbols expands the SUBSET and merge the symbol set for sharing by NewEngine/Validate.
// Returns: the symbol set for prepare (PATTERN literal ∪ DEFINE declares ∪ SUBSET name), and the SUBSET member table
// (Flattened to the real model variable, just compare the values during the evaluation period) The expanded pattern.
func resolveSymbols(spec *types.MatchRecognizeSpec) (map[string]bool, map[string][]string, *types.PatternNode, error) {
	subsets := collectSubsets(spec)
	// Given symbol = PATTERN literal ∪ DEFINE declares ∪ SUBSET name.
	known := collectSymbols(spec)
	for _, d := range spec.Defines {
		known[d.Symbol] = true
	}
	// No SUBSET: Skips flattening/expanding/validation and full-tree deep copy; returns pattern as is.
	if len(subsets) == 0 {
		return known, nil, spec.Pattern, nil
	}
	for name := range subsets {
		known[name] = true
	}
	// Member validation: must be a known symbol, and the number of members is limited by maxSubsetMembers (to prevent NFA alternating bloat).
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
	// Ring detection: SUBSET dependency graphs must not have rings.
	for name := range subsets {
		if subsetHasCycle(name, subsets, map[string]bool{}) {
			return nil, nil, nil, fmt.Errorf("SUBSET %q has a cyclic definition", name)
		}
	}
	// Flattening: Revising the SUBSET names in members into real pattern variables, so no further recursion is needed during evaluation.
	flat := make(map[string][]string, len(subsets))
	for name, members := range subsets {
		flat[name] = flattenMembers(members, subsets)
	}
	return known, flat, expandSubsets(spec.Pattern, flat), nil
}

// subsetHasCycle runs along the SUBSET dependency diagram DFS detection loop (path backtracking).
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

// flattenMembers recursively expands the SUBSET names in the member into the real pattern variable (deduplication; Passed ring detection).
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

// normalizeTs normalizes event timestamps to nanoseconds (automatic units: ns/μs/ms/s epoch).
// Note: Guessing units based on numerical magnitude may result in misjudgment of boundary values (such as crossing 1e9); It is recommended to use a real epoch timestamp,
// Or the same unit within the same flow. For smaller sequence numbers (<1e9), compare according to the original value; WITHIN must be the same unit.
// Perform overflow clamping before multiplication to avoid large timestamp looping and negative damage WITHIN determination.
func normalizeTs(v int64) int64 {
	switch {
	case v <= 0:
		return 0
	case v >= 1e18:
		return v
	case v >= 1e15: // Microseconds
		if v > maxInt64/1000 {
			return maxInt64
		}
		return v * 1000
	case v >= 1e12: // Milliseconds
		if v > maxInt64/1000000 {
			return maxInt64
		}
		return v * 1000000
	case v >= 1e9: // seconds
		if v > maxInt64/1000000000 {
			return maxInt64
		}
		return v * 1000000000
	}
	return v // Small value (relative number): As is, WITHIN is compared with the same unit
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

// Predefined errors (avoid FMT in hot paths).
var (
	errPatternRequired = cepError("MATCH_RECOGNIZE requires a PATTERN clause")
	errOrderByRequired = cepError("MATCH_RECOGNIZE requires ORDER BY (provides event ordering)")
)

type cepError string

func (e cepError) Error() string { return string(e) }
