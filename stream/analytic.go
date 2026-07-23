package stream

import (
	"container/list"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/rulego/streamsql/condition"
	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/fieldpath"
)

// defaultMaxPartitions bounds per-field PARTITION state so high-cardinality keys
// (e.g. deviceId) cannot grow memory without limit. When the cap is exceeded the
// least-recently-used partition is evicted (state + cached last-result).
const defaultMaxPartitions = 10000

// AnalyticEngine manages the stream-level state machine for all numeric segments of the analysis function in a query.
// Direct connection path (EmitSync/processDirectData): Each event evaluates before WHERE and is followed by alias
// Inject lines (for WHERE/projection reference). Status is bucketed by PARTITION, and status updates are controlled when WHEN is maintained.
type AnalyticEngine struct {
	owner  *Stream
	fields []*analyticFieldEngine
}

// Does HasFields have numeric paragraphs for analysis functions (nil engines consider none).
func (e *AnalyticEngine) HasFields() bool { return e != nil && len(e.fields) > 0 }

// analyticFieldEngine is a state machine with several segments of a single analysis function (including PARTITION buckets + WHEN condition).
// A field can contain multiple analysis calls (e.g., acc_max(v) - acc_min(v)), each call independent state machine and shared partition.
type analyticFieldEngine struct {
	af            types.AnalyticField
	stateCtors    []func() functions.AnalyticState // Each analysis calls a state initializer
	whenCond      condition.Condition              // WHEN, nil means none
	mu            sync.Mutex
	noPart        []functions.AnalyticState // Per-call state without PARTITION
	partitions    map[string]*list.Element  // Per-key state during PARTITION BY (LRU node)
	lru           *list.List                // LRU order: front = recently used, back = to be eliminated
	lastResults   map[string]any            // per-partition Last result (reused WHEN not met)
	maxPartitions int                       // Upper limit on the number of partitions (exceeding this is eliminated by LRU)
	wrapperParsed *expr.Expression          // The wrapper's expr packet parsing cache (used only when bridge fails, e.g., CASE), parsing each field once
}

// partitionEntry is the partition state carried by the LRU linked list node (one per analysis call).
type partitionEntry struct {
	key    string
	states []functions.AnalyticState
}

// NewAnalyticEngine builds a set of state machines for analysis functions based on the configuration. Returns nil when no analysis function is present.
func NewAnalyticEngine(owner *Stream, fields []types.AnalyticField) (*AnalyticEngine, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	// Partition limit: option injection (≤0 uses default).
	maxPart := defaultMaxPartitions
	if owner != nil && owner.config.AnalyticMaxPartitions > 0 {
		maxPart = owner.config.AnalyticMaxPartitions
	}
	engines := make([]*analyticFieldEngine, 0, len(fields))
	for _, af := range fields {
		ctors, err := buildStateCtors(af)
		if err != nil {
			return nil, err
		}
		fe := &analyticFieldEngine{
			af:            af,
			stateCtors:    ctors,
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

// buildStateCtors is a field for each analysis call to construct the state initializer. Calls when empty (such as when WHERE placeholder calls,
// Only FuncName is set) degraded to single call.
func buildStateCtors(af types.AnalyticField) ([]func() functions.AnalyticState, error) {
	names := make([]string, 0, len(af.Calls)+1)
	for _, c := range af.Calls {
		names = append(names, c.FuncName)
	}
	if len(names) == 0 {
		names = append(names, af.FuncName)
	}
	ctors := make([]func() functions.AnalyticState, 0, len(names))
	for _, name := range names {
		fn, ok := functions.Get(name)
		if !ok {
			return nil, fmt.Errorf("analytic function %q not found", name)
		}
		sf, ok := fn.(functions.StatefulAnalytic)
		if !ok {
			return nil, fmt.Errorf("function %q is not a stateful analytic function", name)
		}
		ctors = append(ctors, sf.NewState)
	}
	return ctors, nil
}

// Evaluate Evaluates all numeric segments of the analysis function on a single line, returning map[alias]value.
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
			// Abnormal parsing/evaluation of a single analysis function should not interrupt the entire pipeline.
			if s != nil && s.log != nil {
				s.log.Error("analytic %q evaluate panic: %v", fe.af.Expression, r)
			}
			result = nil
		}
	}()
	// Multi-column functions (changed_cols) go through ApplyColumns to fan out branches.
	if fe.af.MultiColumn {
		return fe.evaluateMultiColumn(s, row)
	}
	partKey := fe.partitionKey(row)
	fe.mu.Lock()
	defer fe.mu.Unlock()
	// WHEN: Only update the status when the condition is satisfied; otherwise, the previous result will be reused.
	if fe.whenCond != nil && !fe.whenCond.Evaluate(row) {
		if last, ok := fe.lastResults[partKey]; ok {
			return last
		}
		return nil
	}
	states := fe.getStateLocked(partKey)
	calls := fe.af.Calls
	if len(calls) == 0 {
		// WHERE placeholder calls and similar functions only set FuncName/Expression/Args, degrading to single calls.
		calls = []types.AnalyticCall{{FuncName: fe.af.FuncName, BareCall: fe.af.Expression, Args: fe.af.Args}}
	}
	// had_changed(true, *) Compares entire rows by column name to avoid misalignment caused by schema changes (column addition, deletion, or disorder of rows).
	if fe.af.FuncName == "had_changed" && hasStarArg(fe.af.Args) {
		if named, ok := states[0].(functions.NamedRowState); ok {
			ignoreNull := false
			if len(fe.af.Args) > 0 {
				ignoreNull = functions.AnalyticToBool(literalValue(fe.af.Args[0]))
			}
			result = named.ApplyNamed(ignoreNull, row)
			fe.lastResults[partKey] = result
			return result
		}
	}
	// Single-call field (no outer expression): Returns the first call result directly.
	if fe.af.WrapperExpr == "" {
		result = fe.applyCall(s, row, calls[0], states[0])
		fe.lastResults[partKey] = result
		return result
	}
	// Expression package analysis function (single or multiple calls): Each call to Apply advances its respective state, and each result (including nil) is injected into a placeholder,
	// Then find the wrapper. nil spaceholder is left to the wrapper to handle: coalesce(__analytic_self__,-1)→-1,
	// CASE WHEN __analytic_self__>... Go ELSE; Arithmetic __analytic_self__-x fails on nil→ null propagation returns nil.
	rowCopy := make(map[string]any, len(row)+len(calls))
	for k, v := range row {
		rowCopy[k] = v
	}
	anyNil := false
	for i, c := range calls {
		v := fe.applyCall(s, row, c, states[i])
		if v == nil {
			anyNil = true
		}
		rowCopy[types.AnalyticSelfTokenN(i)] = v
	}
	v, isNull, err := fe.evalWrapper(rowCopy)
	if err != nil {
		// When any analysis is nil, the failure of a wrapper that does not support nil operands in arithmetic or other operations is normal null propagation and returns nil silently;
		// Failing at NIL is the real anomaly—log this.
		if !anyNil && s != nil && s.log != nil {
			s.log.Error("analytic wrapper %q evaluate failed: %v", fe.af.WrapperExpr, err)
		}
		fe.lastResults[partKey] = nil
		return nil
	}
	if isNull {
		fe.lastResults[partKey] = nil
		return nil
	}
	fe.lastResults[partKey] = v
	return v
}

// evalWrapper evaluates wrapper expression: first use expr bridge (function/arithmetic/string concatenation),
// If it fails, the expr package is reverted (supporting statements like CASE). wrapper keeps each field unchanged, so expr parses the package
// Results are cached by field (lazy parsing, caller holds fe.mu with no contention), avoiding repeated parsing line by line. isNull distinguishes between explicit NULL and nil.
func (fe *analyticFieldEngine) evalWrapper(data map[string]any) (any, bool, error) {
	if v, err := functions.GetExprBridge().EvaluateExpression(fe.af.WrapperExpr, data); err == nil {
		return v, false, nil
	}
	if fe.wrapperParsed == nil {
		e, parseErr := expr.NewExpression(fe.af.WrapperExpr)
		if parseErr != nil {
			return nil, false, parseErr
		}
		fe.wrapperParsed = e
	}
	return fe.wrapperParsed.EvaluateValueWithNull(data)
}

// applyCall for a single analysis call: parsing parameters (including '*' in full line expansion), applied to the state machine.
func (fe *analyticFieldEngine) applyCall(s *Stream, row map[string]any, c types.AnalyticCall, state functions.AnalyticState) any {
	args, err := s.parseFunctionArgs(c.BareCall, row)
	if err != nil || args == nil {
		args = []any{}
	}
	if hasStarArg(c.Args) {
		args = expandStarArgs(c.Args, row, args)
	}
	return state.Apply(args)
}

// evaluateMultiColumn handles multi-column functions like changed_cols: fan out the variable column by prefix+ column name.
func (fe *analyticFieldEngine) evaluateMultiColumn(s *Stream, row map[string]any) any {
	values, err := s.parseFunctionArgs(fe.af.Expression, row)
	if err != nil || values == nil {
		values = []any{}
	}
	// Position parameters: prioritize the already evaluated; "*" Restore prefix/ignoreNull with literal value when parsing fails.
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
		if name == "*" { // changed_cols(prefix, ignoreNull, *) → Columns of the entire row
			for k, v := range row {
				cols[k] = v
			}
			continue
		}
		valIdx := 2 + i
		if valIdx < len(values) {
			// In window queries, inline aggregation is overridden to hidden keys: output column names are displayed names (e.g., avg → tavg).
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
	state := fe.getStateLocked(partKey)[0]
	mcs, ok := state.(functions.MultiColumnState)
	if !ok {
		return map[string]any{}
	}
	result := mcs.ApplyColumns(prefix, ignoreNull, cols)
	fe.lastResults[partKey] = result
	return result
}

// hasStarArg checks whether the parameter expression list contains "*" (full line expansion).
func hasStarArg(args []string) bool {
	for _, a := range args {
		if strings.TrimSpace(a) == "*" {
			return true
		}
	}
	return false
}

// expandStarArgs keeps position parameters outside "*" (such as ignoreNull), then adds values for each row of rows,
// Supply had_changed(true, *) and other parameters to find variation in the entire line. args is the parameter expression, parsed is the evaluated position value.
// Row values are sorted by keyname to ensure stable positions across events (otherwise, map iteration disorder may cause misjudgment changes).
// When parseFunctionArgs fails due to "*" causing parsed missing entries, restore scalar parameters with literals.
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

// literalValue parses scalar literals (true/false/number/quoted strings), used when "*" causes parsing failure
// Restores ignoreNull and other parameters.
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

// analyticColName Retrieves the output column name from the column name expression: remove backquotes/quotes, and take the last paragraph of the qualifier.
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

func (fe *analyticFieldEngine) getStateLocked(partKey string) []functions.AnalyticState {
	newStates := func() []functions.AnalyticState {
		states := make([]functions.AnalyticState, len(fe.stateCtors))
		for i, ctor := range fe.stateCtors {
			states[i] = ctor()
		}
		return states
	}
	if fe.af.Over == nil || len(fe.af.Over.PartitionBy) == 0 {
		if fe.noPart == nil {
			fe.noPart = newStates()
		}
		return fe.noPart
	}
	if el, ok := fe.partitions[partKey]; ok {
		fe.lru.MoveToFront(el) // Hit: Upgraded to recently used
		return el.Value.(*partitionEntry).states
	}
	entry := &partitionEntry{key: partKey, states: newStates()}
	fe.partitions[partKey] = fe.lru.PushFront(entry)
	// Over-limit: Eliminates the longest unused partitions and synchronously cleans up their lastResults to prevent memory leaks.
	if fe.maxPartitions > 0 && fe.lru.Len() > fe.maxPartitions {
		if oldest := fe.lru.Back(); oldest != nil {
			oe := oldest.Value.(*partitionEntry)
			fe.lru.Remove(oldest)
			delete(fe.partitions, oe.key)
			delete(fe.lastResults, oe.key)
		}
	}
	return entry.states
}

func (fe *analyticFieldEngine) partitionKey(row map[string]any) string {
	if fe.af.Over == nil || len(fe.af.Over.PartitionBy) == 0 {
		return ""
	}
	var sb strings.Builder
	var lbuf [4]byte // Partition key fragment length (decimal, common < 1000)
	for _, k := range fe.af.Over.PartitionBy {
		tk := typeKey(resolvePartitionField(row, k))
		// Length prefix + suffix separator, preventing key collisions caused by values containing '|' or type names.
		// Write Builder directly, skipping fmt.Fprintf format string parsing.
		lstr := strconv.AppendInt(lbuf[:0], int64(len(tk)), 10)
		sb.Write(lstr)
		sb.WriteByte(':')
		sb.WriteString(tk)
		sb.WriteByte('|')
	}
	return sb.String()
}

// resolvePartitionField parses the actual value of the PARTITION BY key. Bare list direct lookup hit (deviceId/temp);
// JOIN Nested columns (m.location, stored in augmented row["m"]["location"]) via fieldpath;
// Window flat limit column (stream.k, output row stored by the suffix k) and remove the lookupRowField suffix as backup.
// None of these will hit the return nil (the old implementation of bare row[k] always placed nil → on the same partition with all restricted columns, resulting in mute errors).
func resolvePartitionField(row map[string]any, key string) any {
	if v, ok := row[key]; ok {
		return v
	}
	if v, ok := fieldpath.GetNestedField(row, key); ok {
		return v
	}
	if v, ok := lookupRowField(row, key); ok {
		return v
	}
	return nil
}

// typeKey generates a partition key fragment in the form "type|value", with nil denoted as "nil|".
// Common scalar types are switched and manually twisted strings to avoid fmt.Sprintf reflection overhead; The rest of the types
// (Slices/maps/structs, etc.) Return fmt.Sprintf ensures that generality matches "%T|%v".
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
