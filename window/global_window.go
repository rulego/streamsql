/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package window

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/condition"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/rulego/streamsql/utils/fieldpath"
)

var _ Window = (*GlobalWindow)(nil)

// aggTriggerFuncNames are the aggregate function names recognized inside a
// TRIGGER WHEN predicate. They mirror the common SQL aggregates; the runtime
// value comes from a per-group AggregatorFunction, so unknown names simply
// don't match and the predicate is left untouched (and will likely fail to
// evaluate, surfacing the problem at first Add).
var aggTriggerFuncNames = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
	"median": true, "stddev": true, "first_value": true, "last_value": true,
}

var aggCallRe = regexp.MustCompile(`(?i)\b([a-z_]+)\s*\(\s*([^)]*?)\s*\)`)

// GlobalWindow aligns with Flink GlobalWindows + Trigger: it has no built-in
// boundary and never fires on its own. Each arriving row updates a per-group
// running aggregate (O(1) state per group, raw rows are not buffered); the
// TRIGGER WHEN predicate is evaluated against that running aggregate and, on a
// hit, the group emits its current aggregate result and is purged
// (FIRE_AND_PURGE, the Flink default for global windows).
//
// Memory bound: O(group count x aggregate state). When TRIGGER WHEN rarely
// fires, set WITH(STATETTL='...') so idle groups are reaped (same reapIdleKeys
// pattern as CountingWindow).
type GlobalWindow struct {
	config types.WindowConfig

	// SELECT aggregation setup: one prototype per output alias. New() spawns a
	// fresh per-group instance. inputField=="*" means count-all.
	outputSpecs []aggSpec

	// groupByKeys mirrors config.GroupByKeys; empty means a single global group.
	groupByKeys []string

	// TRIGGER WHEN: predicate with aggregate calls rewritten to placeholders,
	// plus the per-placeholder aggregator prototypes. A placeholder may alias a
	// SELECT output (so its value is read from the output aggregators) or be a
	// trigger-only aggregate.
	triggerSpecs       []triggerSpec
	rewrittenPredicate string
	triggerCond        condition.Condition

	// per-group running state.
	groups      map[string]*globalGroupState
	mu          sync.Mutex
	callback    func([]types.Row)
	outputChan  chan []types.Row
	ctx         context.Context
	cancelFunc  context.CancelFunc
	triggerChan chan types.Row

	countStateTTL time.Duration

	sentCount    int64
	droppedCount int64
	stopped      bool
}

// aggSpec describes one SELECT aggregation field for output.
type aggSpec struct {
	alias      string
	aggType    aggregator.AggregateType
	inputField string // "*" for count(*)
	prototype  aggregator.AggregatorFunction
}

// triggerSpec describes one aggregate call extracted from TRIGGER WHEN.
// placeholder is the identifier substituted into the rewritten predicate; if
// outputAlias != "", the value is read from the matching output aggregator
// instead of maintaining a separate one.
type triggerSpec struct {
	placeholder string
	aggType     aggregator.AggregateType
	inputField  string
	outputAlias string
	prototype   aggregator.AggregatorFunction // nil when reusing an output alias
}

type globalGroupState struct {
	key         string
	keyValues   map[string]any
	outputAggs  map[string]aggregator.AggregatorFunction
	triggerAggs map[string]aggregator.AggregatorFunction // placeholder -> agg
	windowStart time.Time
	windowEnd   time.Time
	lastActive  time.Time
	hasData     bool
}

// NewGlobalWindow builds a global window from config. The SELECT/FieldAlias
// maps carry the aggregation fields so the window can maintain running state
// without buffering rows.
func NewGlobalWindow(config types.WindowConfig) (*GlobalWindow, error) {
	if config.TimeCharacteristic == types.EventTime {
		return nil, fmt.Errorf("global window does not support event time in this release, use processing time")
	}
	predicate := strings.TrimSpace(config.TriggerCondition)
	if predicate == "" {
		return nil, fmt.Errorf("global window requires a TRIGGER WHEN predicate")
	}

	bufferSize := 1000
	if config.PerformanceConfig.BufferConfig.WindowOutputSize > 0 {
		bufferSize = config.PerformanceConfig.BufferConfig.WindowOutputSize
	}

	ctx, cancel := context.WithCancel(context.Background())
	gw := &GlobalWindow{
		config:        config,
		groupByKeys:   config.GroupByKeys,
		groups:        make(map[string]*globalGroupState),
		outputChan:    make(chan []types.Row, bufferSize),
		ctx:           ctx,
		cancelFunc:    cancel,
		triggerChan:   make(chan types.Row, bufferSize),
		countStateTTL: config.CountStateTTL,
	}

	if err := gw.buildOutputSpecs(); err != nil {
		cancel()
		return nil, err
	}
	if err := gw.buildTrigger(predicate); err != nil {
		cancel()
		return nil, err
	}

	if config.Callback != nil {
		gw.callback = config.Callback
	}
	return gw, nil
}

// buildOutputSpecs turns the SELECT aggregation map into per-alias prototypes.
func (gw *GlobalWindow) buildOutputSpecs() error {
	for alias, aggType := range gw.config.SelectFields {
		inputField := gw.config.FieldAlias[alias]
		if inputField == "" {
			inputField = alias
		}
		// Skip structural markers that aren't runnable builtin aggregators:
		// "post_aggregation" (multi-aggregate expression evaluated downstream)
		// and "expression" (needs a registered expression evaluator the global
		// window doesn't own). Output for these in a global window is TBD; the
		// common COUNT/SUM/AVG/MIN/MIN/MAX-over-field path below is supported.
		if aggType == aggregator.PostAggregation || aggType == aggregator.Expression {
			continue
		}
		proto := aggregator.CreateBuiltinAggregator(aggType)
		if proto == nil {
			continue
		}
		gw.outputSpecs = append(gw.outputSpecs, aggSpec{
			alias:      alias,
			aggType:    aggType,
			inputField: inputField,
			prototype:  proto,
		})
	}
	return nil
}

// triggerAggRef is a parsed aggregate call from the predicate.
type triggerAggRef struct {
	funcName   string
	inputField string
}

// buildTrigger compiles the TRIGGER WHEN predicate: it finds aggregate calls,
// binds them to existing output aggregates where possible (or creates
// trigger-only ones), rewrites the predicate to use placeholders, and compiles
// it with the condition engine.
func (gw *GlobalWindow) buildTrigger(predicate string) error {
	// Normalize SQL logical/equality operators to the expr-lang form the
	// condition engine compiles. The rsql parser already lowers AND/OR/=, but a
	// programmatic WindowConfig (rulego component config, tests) may pass them
	// verbatim; without this, "a AND b" or "x = 3" fails to compile. Quoted
	// literals are left untouched.
	predicate = normalizeTriggerPredicate(predicate)
	refs := gw.findAggCalls(predicate)
	rewritten := predicate
	gw.triggerSpecs = nil

	for i, ref := range refs {
		placeholder := fmt.Sprintf("__trig_%d__", i)
		spec := triggerSpec{
			placeholder: placeholder,
			aggType:     aggregator.AggregateType(strings.ToLower(ref.funcName)),
			inputField:  ref.inputField,
		}

		// Prefer reusing a SELECT output aggregate of the same type+field so the
		// value isn't computed twice.
		if idx := gw.findOutputSpec(spec.aggType, spec.inputField); idx >= 0 {
			spec.outputAlias = gw.outputSpecs[idx].alias
		} else {
			proto := aggregator.CreateBuiltinAggregator(spec.aggType)
			if proto == nil {
				return fmt.Errorf("TRIGGER WHEN references unsupported aggregate %s", ref.funcName)
			}
			spec.prototype = proto
		}
		gw.triggerSpecs = append(gw.triggerSpecs, spec)

		// Replace the first occurrence of this exact call. findAggCalls returns
		// calls in document order with their match strings, so substring replace
		// is safe here (no overlap between distinct matches).
		rewritten = strings.Replace(rewritten, ref.matchStr, placeholder, 1)
	}

	cond, err := condition.NewExprCondition(rewritten)
	if err != nil {
		return fmt.Errorf("compile TRIGGER WHEN predicate %q (rewritten %q): %w", predicate, rewritten, err)
	}
	gw.rewrittenPredicate = rewritten
	gw.triggerCond = cond
	return nil
}

type triggerAggRefWithMatch struct {
	triggerAggRef
	matchStr string
}

func (gw *GlobalWindow) findAggCalls(predicate string) []triggerAggRefWithMatch {
	var out []triggerAggRefWithMatch
	for _, m := range aggCallRe.FindAllStringSubmatchIndex(predicate, -1) {
		full := predicate[m[0]:m[1]]
		name := strings.ToLower(predicate[m[2]:m[3]])
		arg := strings.TrimSpace(predicate[m[4]:m[5]])
		if !aggTriggerFuncNames[name] {
			continue
		}
		field := arg
		if strings.ToLower(arg) == "*" || arg == "" {
			field = "*"
		}
		out = append(out, triggerAggRefWithMatch{
			triggerAggRef: triggerAggRef{funcName: name, inputField: field},
			matchStr:      full,
		})
	}
	return out
}

// normalizeTriggerPredicate rewrites SQL logical/equality operators to expr-lang
// form: AND->&&, OR->||, and a bare '=' (not already part of ==, >=, <=, !=)
// becomes '=='. String literals and backtick identifiers are skipped so
// operator-like text inside them is preserved. Mirrors the lowering the rsql
// parser applies, so SQL-sourced predicates are unaffected.
func normalizeTriggerPredicate(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 4)
	inQuote := byte(0)
	prev := byte(0)
	n := len(s)
	for i := 0; i < n; i++ {
		c := s[i]
		if inQuote != 0 {
			b.WriteByte(c)
			if c == inQuote {
				inQuote = 0
			}
			prev = c
			continue
		}
		switch c {
		case '\'', '"', '`':
			inQuote = c
			b.WriteByte(c)
			prev = c
		case '=':
			next := byte(0)
			if i+1 < n {
				next = s[i+1]
			}
			// Only a standalone '=' needs doubling; leave ==, >=, <=, != as-is.
			if !isOpChar(prev) && !isOpChar(next) {
				b.WriteString("==")
				prev = '='
			} else {
				b.WriteByte(c)
				prev = c
			}
		default:
			if (c == 'A' || c == 'a') && hasWordAt(s, i, "and") && !isWordChar(prev) {
				next := byte(0)
				if i+3 < n {
					next = s[i+3]
				}
				if !isWordChar(next) {
					b.WriteString("&&")
					i += 2
					prev = '&'
					continue
				}
			}
			if (c == 'O' || c == 'o') && hasWordAt(s, i, "or") && !isWordChar(prev) {
				next := byte(0)
				if i+2 < n {
					next = s[i+2]
				}
				if !isWordChar(next) {
					b.WriteString("||")
					i += 1
					prev = '|'
					continue
				}
			}
			b.WriteByte(c)
			prev = c
		}
	}
	return b.String()
}

func hasWordAt(s string, i int, word string) bool {
	if i+len(word) > len(s) {
		return false
	}
	for j := 0; j < len(word); j++ {
		if toLower(s[i+j]) != word[j] {
			return false
		}
	}
	return true
}

func toLower(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + 32
	}
	return c
}

func isWordChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

func isOpChar(c byte) bool {
	return c == '=' || c == '>' || c == '<' || c == '!'
}

// findOutputSpec returns the index of an output aggregate matching type+field,
// or -1. count(*) is matched by inputField=="*".
func (gw *GlobalWindow) findOutputSpec(aggType aggregator.AggregateType, inputField string) int {
	for i := range gw.outputSpecs {
		if gw.outputSpecs[i].aggType == aggType && normalizeField(gw.outputSpecs[i].inputField) == normalizeField(inputField) {
			return i
		}
	}
	return -1
}

func normalizeField(f string) string {
	f = strings.TrimSpace(strings.ToLower(f))
	if f == "" {
		return "*"
	}
	return f
}

func (gw *GlobalWindow) Add(data any) {
	gw.mu.Lock()
	stopped := gw.stopped
	gw.mu.Unlock()
	if stopped {
		return
	}
	t := GetTimestamp(data, gw.config.TsProp, gw.config.TimeUnit)
	row := types.Row{Data: data, Timestamp: t}
	select {
	case gw.triggerChan <- row:
	case <-gw.ctx.Done():
	}
}

func (gw *GlobalWindow) Start() {
	go func() {
		defer gw.cancelFunc()

		// STATETTL reap reuses this goroutine via a ticker (same shape as
		// CountingWindow). nil tickChan disables reaping (TTL=0 default).
		var tickChan <-chan time.Time
		if gw.countStateTTL > 0 {
			interval := gw.countStateTTL / 2
			if interval < time.Second {
				interval = time.Second
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			tickChan = ticker.C
		}

		for {
			select {
			case row, ok := <-gw.triggerChan:
				if !ok {
					return
				}
				gw.processRow(row)
			case <-tickChan:
				gw.reapIdleKeys(time.Now())
			case <-gw.ctx.Done():
				return
			}
		}
	}()
}

// processRow updates one group's running aggregate, evaluates the trigger, and
// on a hit emits the group's current result and purges its state.
func (gw *GlobalWindow) processRow(row types.Row) {
	data, ok := row.Data.(map[string]any)
	if !ok {
		return
	}
	key, keyValues := gw.getKeyAndValues(data)

	gw.mu.Lock()
	defer gw.mu.Unlock()

	gs := gw.groups[key]
	if gs == nil {
		gs = newGroupState(key, keyValues, gw.outputSpecs, gw.triggerSpecs)
		gw.groups[key] = gs
		gs.windowStart = row.Timestamp
	}
	if !gs.hasData {
		gs.windowStart = row.Timestamp
		gs.hasData = true
	}
	gs.windowEnd = row.Timestamp
	gs.lastActive = time.Now()
	// Refresh key values in case the group was re-created after a purge.
	for k, v := range keyValues {
		gs.keyValues[k] = v
	}

	feedAggs(gs.outputAggs, gw.outputSpecs, data)
	feedTriggerAggs(gs.triggerAggs, gw.triggerSpecs, data)

	if gw.shouldFire(gs) {
		result := gw.buildResult(gs)
		// FIRE_AND_PURGE: drop the group so the next rows start fresh.
		delete(gw.groups, key)
		gw.mu.Unlock()
		gw.deliver(result)
		gw.mu.Lock()
	}
}

// shouldFire evaluates the rewritten TRIGGER WHEN predicate against the group's
// current aggregate values.
func (gw *GlobalWindow) shouldFire(gs *globalGroupState) bool {
	env := make(map[string]any, len(gw.triggerSpecs))
	for _, ts := range gw.triggerSpecs {
		if ts.outputAlias != "" {
			if agg := gs.outputAggs[ts.outputAlias]; agg != nil {
				env[ts.placeholder] = agg.Result()
			}
		} else if agg := gs.triggerAggs[ts.placeholder]; agg != nil {
			env[ts.placeholder] = agg.Result()
		}
	}
	return gw.triggerCond.Evaluate(env)
}

// buildResult assembles the final result map for a group: group key fields +
// aggregate aliases + window bounds.
func (gw *GlobalWindow) buildResult(gs *globalGroupState) map[string]any {
	result := make(map[string]any, len(gs.keyValues)+len(gw.outputSpecs)+2)
	for k, v := range gs.keyValues {
		result[k] = v
	}
	for _, spec := range gw.outputSpecs {
		if agg := gs.outputAggs[spec.alias]; agg != nil {
			result[spec.alias] = agg.Result()
		}
	}
	result["window_start"] = gs.windowStart
	result["window_end"] = gs.windowEnd
	return result
}

func (gw *GlobalWindow) deliver(result map[string]any) {
	rows := []types.Row{{Data: result}}
	if gw.callback != nil {
		gw.callback(rows)
	}
	gw.sendResult(rows)
}

func (gw *GlobalWindow) sendResult(data []types.Row) {
	strategy := gw.config.PerformanceConfig.OverflowConfig.Strategy
	timeout := gw.config.PerformanceConfig.OverflowConfig.BlockTimeout
	if strategy == types.OverflowStrategyBlock {
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		select {
		case gw.outputChan <- data:
			atomic.AddInt64(&gw.sentCount, 1)
		case <-time.After(timeout):
			atomic.AddInt64(&gw.droppedCount, 1)
		case <-gw.ctx.Done():
		}
		return
	}
	select {
	case gw.outputChan <- data:
		atomic.AddInt64(&gw.sentCount, 1)
	case <-gw.ctx.Done():
		return
	default:
		select {
		case <-gw.outputChan:
			select {
			case gw.outputChan <- data:
				atomic.AddInt64(&gw.sentCount, 1)
			default:
				atomic.AddInt64(&gw.droppedCount, 1)
			}
		default:
			atomic.AddInt64(&gw.droppedCount, 1)
		}
	}
}

// reapIdleKeys removes groups whose last arrival is older than countStateTTL.
// Called from the Start goroutine's ticker; caller guarantees TTL > 0.
func (gw *GlobalWindow) reapIdleKeys(now time.Time) {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	for key, gs := range gw.groups {
		if now.Sub(gs.lastActive) > gw.countStateTTL {
			delete(gw.groups, key)
		}
	}
}

func (gw *GlobalWindow) Trigger() {
	// Trigger logic is driven per-row inside processRow; nothing to do here.
	// The method exists to satisfy the Window interface.
}

func (gw *GlobalWindow) Stop() {
	gw.mu.Lock()
	stopped := gw.stopped
	if !stopped {
		gw.stopped = true
	}
	gw.mu.Unlock()
	if !stopped {
		gw.cancelFunc()
	}
}

func (gw *GlobalWindow) Reset() {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	gw.groups = make(map[string]*globalGroupState)
	atomic.StoreInt64(&gw.sentCount, 0)
	atomic.StoreInt64(&gw.droppedCount, 0)
}

func (gw *GlobalWindow) GetStats() map[string]int64 {
	return map[string]int64{
		"sentCount":    atomic.LoadInt64(&gw.sentCount),
		"droppedCount": atomic.LoadInt64(&gw.droppedCount),
		"bufferSize":   int64(cap(gw.outputChan)),
		"bufferUsed":   int64(len(gw.outputChan)),
	}
}

func (gw *GlobalWindow) OutputChan() <-chan []types.Row {
	return gw.outputChan
}

func (gw *GlobalWindow) SetCallback(callback func([]types.Row)) {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	gw.callback = callback
}

// getKeyAndValues extracts the group key and a map of group-by field values
// from the row. With no GroupByKeys, a single "__global__" key is used.
func (gw *GlobalWindow) getKeyAndValues(data map[string]any) (string, map[string]any) {
	if len(gw.groupByKeys) == 0 {
		return "__global__", nil
	}
	v := reflect.ValueOf(data)
	parts := make([]string, 0, len(gw.groupByKeys))
	values := make(map[string]any, len(gw.groupByKeys))
	for _, k := range gw.groupByKeys {
		var val any
		if fieldpath.IsNestedField(k) {
			val, _ = fieldpath.GetNestedField(data, k)
		} else if v.IsValid() && v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String {
			if mv := v.MapIndex(reflect.ValueOf(k)); mv.IsValid() {
				val = mv.Interface()
			}
		}
		values[k] = val
		if val == nil {
			parts = append(parts, "")
		} else if s, ok := val.(string); ok {
			parts = append(parts, s)
		} else {
			parts = append(parts, fmt.Sprintf("%v", val))
		}
	}
	return strings.Join(parts, "|"), values
}

// feedAggs feeds the row's field values into a group's output aggregators.
func feedAggs(target map[string]aggregator.AggregatorFunction, specs []aggSpec, data map[string]any) {
	for _, spec := range specs {
		agg := target[spec.alias]
		if agg == nil {
			continue
		}
		if spec.inputField == "*" {
			agg.Add(1)
			continue
		}
		val, ok := lookupFieldValue(data, spec.inputField)
		if !ok || val == nil {
			continue
		}
		agg.Add(toAggregateValue(val))
	}
}

// feedTriggerAggs feeds row values into trigger-only aggregators (those not
// bound to a SELECT output alias).
func feedTriggerAggs(target map[string]aggregator.AggregatorFunction, specs []triggerSpec, data map[string]any) {
	for _, spec := range specs {
		if spec.prototype == nil {
			continue // value comes from an output alias
		}
		agg := target[spec.placeholder]
		if agg == nil {
			continue
		}
		if spec.inputField == "*" {
			agg.Add(1)
			continue
		}
		val, ok := lookupFieldValue(data, spec.inputField)
		if !ok || val == nil {
			continue
		}
		agg.Add(toAggregateValue(val))
	}
}

func lookupFieldValue(data map[string]any, field string) (any, bool) {
	if fieldpath.IsNestedField(field) {
		return fieldpath.GetNestedField(data, field)
	}
	v, ok := data[field]
	return v, ok
}

// toAggregateValue normalizes a raw field value to the numeric form aggregators
// expect; non-numeric values pass through unchanged (min/max/first_value etc.
// accept them).
func toAggregateValue(v any) any {
	if n, err := cast.ToFloat64E(v); err == nil {
		return n
	}
	return v
}

func newGroupState(key string, keyValues map[string]any, outputSpecs []aggSpec, triggerSpecs []triggerSpec) *globalGroupState {
	gs := &globalGroupState{
		key:         key,
		keyValues:   make(map[string]any, len(keyValues)),
		outputAggs:  make(map[string]aggregator.AggregatorFunction, len(outputSpecs)),
		triggerAggs: make(map[string]aggregator.AggregatorFunction, len(triggerSpecs)),
		lastActive:  time.Now(),
	}
	for k, v := range keyValues {
		gs.keyValues[k] = v
	}
	for _, spec := range outputSpecs {
		gs.outputAggs[spec.alias] = spec.prototype.New()
	}
	for _, spec := range triggerSpecs {
		if spec.prototype == nil {
			continue
		}
		gs.triggerAggs[spec.placeholder] = spec.prototype.New()
	}
	return gs
}
