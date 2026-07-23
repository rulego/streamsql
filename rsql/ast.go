package rsql

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/rulego/streamsql/window"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/logger"
)

type SelectStatement struct {
	Fields      []Field
	Distinct    bool
	SelectAll   bool // Flag to indicate if this is a SELECT * query
	Source      string
	SourceAlias string // optional FROM alias (e.g. "s" in "FROM stream AS s")
	Condition   string
	Window      WindowDefinition
	GroupBy     []string
	Limit       int
	Having      string
	OrderBy     []types.OrderByField
	JoinConfigs []types.JoinConfig
	// MatchRecognize carries MATCH_RECOGNIZE clauses (after FROM, before WHERE clauses). When not idle, follow the CEP path.
	MatchRecognize *types.MatchRecognizeSpec
}

type Field struct {
	Expression string
	Alias      string
	AggType    string
	OverSpec   *types.OverSpec // Analysis function OVER clause, nil means none
}

type WindowDefinition struct {
	Type              string
	Params            []any
	TsProp            string
	TimeUnit          time.Duration
	MaxOutOfOrderness time.Duration   // Maximum allowed out-of-orderness for event time
	AllowedLateness   time.Duration   // Maximum allowed lateness for event time windows
	IdleTimeout       time.Duration   // Idle source timeout: when no data arrives within this duration, watermark advances based on processing time
	CountStateTTL     time.Duration   // Counting-window keyed state TTL; inactive keys reaped after this (0 = disabled)
	TriggerCondition  string          // Global-window TRIGGER WHEN predicate (raw string)
	Over              *types.OverSpec // GROUP BY window OVER(...) clause (WHEN input gating only)
}

// ToStreamConfig converts AST to Stream configuration
func (s *SelectStatement) ToStreamConfig() (*types.Config, string, error) {
	if s.Source == "" {
		return nil, "", fmt.Errorf("missing FROM clause")
	}

	// Parse window configuration
	windowType := window.TypeTumbling
	switch strings.ToUpper(s.Window.Type) {
	case "TUMBLINGWINDOW":
		windowType = window.TypeTumbling
	case "SLIDINGWINDOW":
		windowType = window.TypeSliding
	case "COUNTINGWINDOW":
		windowType = window.TypeCounting
	case "SESSIONWINDOW":
		windowType = window.TypeSession
	case "GLOBALWINDOW":
		windowType = window.TypeGlobal
		// Global window with no TRIGGER WHEN would never emit.
		// Reject at parse time rather than silently swallowing data.
		if strings.TrimSpace(s.Window.TriggerCondition) == "" {
			return nil, "", fmt.Errorf("GLOBAL WINDOW requires a TRIGGER WHEN clause (without it the window never emits)")
		}
	}

	// Parse window parameters - now returns array directly
	params := s.Window.Params

	// Validate and convert parameters based on window type
	if len(params) > 0 {
		var err error
		params, err = validateWindowParams(params, windowType)
		if err != nil {
			return nil, "", fmt.Errorf("failed to validate window parameters: %w", err)
		}
	}

	// Check if window processing is needed
	needWindow := s.Window.Type != ""
	var simpleFields []string

	// Separate analysis function segments: The analysis function follows a direct path to the state machine, without entering the aggregation path.
	// The remaining fields (true aggregation + regular fields) retain the original parsing logic.
	analyticFields := make([]types.AnalyticField, 0, len(s.Fields))
	otherFields := make([]Field, 0, len(s.Fields))
	for _, f := range s.Fields {
		if isAnalyticField(f) {
			// Nesting of the verification analysis function itself: Analytical suite analysis and aggregate suite analysis are not allowed
			// (Analysis set aggregation is allowed in window queries and handled by extractInlineAggregates).
			if err := detectNestedAggregation(f.Expression); err != nil {
				return nil, "", err
			}
			analyticFields = append(analyticFields, buildAnalyticField(f))
			continue
		}
		// Expression package analysis functions: arithmetic (ts-lag(ts)), scalar set (coalesce(lag(temp)), UPPER(lag)),
		// CASE(lag), etc. For top-level non-naked analysis calls, isAnalyticField is false; Includes analysis calls, i.e., routing into analysis paths,
		// splitAnalyticExprMulti extracts analysis calls and uses outer expressions as WrapperExpr substitutions. Polymerization set analysis
		// (e.g., count(lag)) is intercepted by detectNestedAggregation.
		if containsAnalyticCall(f.Expression) {
			if err := detectNestedAggregation(f.Expression); err != nil {
				return nil, "", err
			}
			analyticFields = append(analyticFields, buildAnalyticField(f))
			continue
		}
		otherFields = append(otherFields, f)
	}

	// Check if there are aggregation functions
	hasAggregation := false
	for _, field := range otherFields {
		if isAggregationFunction(field.Expression) {
			hasAggregation = true
			break
		}
	}

	// If no window is specified but has aggregation functions, use tumbling window by default
	if !needWindow && hasAggregation {
		needWindow = true
		windowType = window.TypeTumbling
		params = []any{10 * time.Second} // Default 10-second window
	}

	// Window queries allow analysis functions: the analysis function evaluates the output row in the window, with the state retained across windows
	// (See stream.processAggregationResults.) Analyze inline aggregation within function parameters
	// (For example, changed_cols("t", true, avg(temperature))) are extracted below as hidden calculation fields.

	// If no aggregation functions, collect simple fields
	if !hasAggregation {
		// If SELECT * query, set special marker
		if s.SelectAll {
			simpleFields = append(simpleFields, "*")
		} else {
			for _, field := range otherFields {
				fieldName := field.Expression
				if field.Alias != "" {
					// If has alias, use alias as field name
					simpleFields = append(simpleFields, fieldName+":"+field.Alias)
				} else {
					// For fields without alias, check if it's a string literal
					_, n, _, _, err := ParseAggregateTypeWithExpression(fieldName)
					if err != nil {
						return nil, "", err
					}
					if n != "" {
						// If string literal, use parsed field name (remove quotes)
						simpleFields = append(simpleFields, n)
					} else {
						// Otherwise use original expression
						simpleFields = append(simpleFields, fieldName)
					}
				}
			}
		}
		logger.Debug("Collected simple fields: %v", simpleFields)
	}

	// Build field mapping and expression information
	aggs, fields, expressions, postAggExpressions, err := buildSelectFieldsWithExpressions(otherFields)
	if err != nil {
		return nil, "", err
	}

	// Analysis function in window query: aggregates inline parameters (such as avg(...) in changed_cols))
	// Extract as hidden calculation fields, override parameters as hidden key references, and provide window aggregation for consumption after computation.
	if needWindow && len(analyticFields) > 0 {
		extractInlineAggregates(analyticFields, aggs, fields)
		// The analysis function is partitioned by the GROUP BY key by default: each window is reserved for each group's own state,
		// Prevents crosstalk caused by sharing the output of windows in different groups.
		gk := extractGroupFields(s)
		if len(gk) > 0 {
			for i := range analyticFields {
				af := &analyticFields[i]
				if af.Over == nil {
					af.Over = &types.OverSpec{}
				}
				if len(af.Over.PartitionBy) == 0 {
					af.Over.PartitionBy = append([]string(nil), gk...)
				}
			}
		}
		// Check: In window queries, the parameters of the analysis function must reference the window output field (aggregation or GROUP BY key).
		// You cannot reference the raw columns—otherwise, if you don't get a value during evaluation, you'll silently get the column string instead of the result.
		if err := validateWindowAnalyticArgs(analyticFields, gk); err != nil {
			return nil, "", err
		}
	}

	// Extract field order information
	fieldOrder, err := extractFieldOrder(s.Fields)
	if err != nil {
		return nil, "", err
	}

	// D3: an analytic-field alias must not silently override another output
	// column (e.g. "SELECT temperature, lag(temperature) AS temperature").
	// fieldOrder already resolves every SELECT item to its final output name, so
	// an analytic alias appearing more than once collides with another field —
	// reject at parse time instead of letting map writes silently overwrite.
	if len(analyticFields) > 0 {
		nameCount := make(map[string]int, len(fieldOrder))
		for _, n := range fieldOrder {
			nameCount[n]++
		}
		for _, af := range analyticFields {
			if nameCount[af.Alias] > 1 {
				return nil, "", fmt.Errorf("duplicate output column %q: analytic alias collides with another field; use a distinct AS alias", af.Alias)
			}
		}
	}

	// Determine time characteristic based on whether TIMESTAMP is specified in WITH clause
	// If TsProp is set, use EventTime; otherwise use ProcessingTime (default)
	timeCharacteristic := types.ProcessingTime
	if s.Window.TsProp != "" {
		timeCharacteristic = types.EventTime
	}

	// The GROUP BY window does not support OVER(...): The input gate semantics of window OVER hide dip and corruption detection,
	// Threshold/continuous detection is used for HAVING (e.g., HAVING min(concurrency) > 200).
	if s.Window.Over != nil {
		return nil, "", fmt.Errorf("OVER(...) on a GROUP BY window is not supported; for threshold/sustained detection use HAVING (e.g. HAVING min(concurrency) > 200)")
	}

	// HAVING can reference unselected aggregates (standard SQL). Call the aggregation in the HAVING text
	// Map to selected alias, or register as a hidden aggregation __having_N__ to have aggregator complete the calculation; aggs/fields expanded in place.
	selectAlias := buildSelectAliasMap(s.Fields)
	havingRewritten := extractHavingAggregates(s.Having, aggs, fields, selectAlias)

	// Execution path mode: MATCH_RECOGNIZE→ CEP; Window/Aggregate→Window; Otherwise, use Direct.
	// Interception MATCH_RECOGNIZE combined with GROUP/aggregation and JOIN (supported in later stages).
	mode := types.ExecDirect
	if needWindow {
		mode = types.ExecWindow
	}
	if s.MatchRecognize != nil {
		if needWindow {
			return nil, "", fmt.Errorf("MATCH_RECOGNIZE cannot be combined with GROUP BY/aggregation yet")
		}
		if len(s.JoinConfigs) > 0 {
			return nil, "", fmt.Errorf("MATCH_RECOGNIZE with JOIN is not supported yet")
		}
		if s.MatchRecognize.Pattern == nil {
			return nil, "", fmt.Errorf("MATCH_RECOGNIZE requires a PATTERN clause")
		}
		if len(s.MatchRecognize.OrderBy) == 0 {
			return nil, "", fmt.Errorf("MATCH_RECOGNIZE requires ORDER BY (provides event ordering)")
		}
		// ORDER BY provides event timing fields in CEP; DESC is meaningless in streaming (in order of arrival), refusal to avoid silent ignoring.
		for _, ob := range s.MatchRecognize.OrderBy {
			if ob.Direction == types.SortDesc {
				return nil, "", fmt.Errorf("MATCH_RECOGNIZE ORDER BY 暂不支持 DESC（流式按到达序处理）")
			}
		}
		mode = types.ExecCEP
	}

	// Build Stream configuration
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:               windowType,
			Params:             params,
			TsProp:             s.Window.TsProp,
			TimeUnit:           s.Window.TimeUnit,
			TimeCharacteristic: timeCharacteristic,
			MaxOutOfOrderness:  s.Window.MaxOutOfOrderness,
			AllowedLateness:    s.Window.AllowedLateness,
			IdleTimeout:        s.Window.IdleTimeout,
			CountStateTTL:      s.Window.CountStateTTL,
			GroupByKeys:        extractGroupFields(s),
			// Global-window fields (no-op for other window types).
			TriggerCondition: s.Window.TriggerCondition,
			SelectFields:     aggs,
			FieldAlias:       fields,
		},
		GroupFields:        extractGroupFields(s),
		SelectFields:       aggs,
		FieldAlias:         fields,
		SelectAlias:        selectAlias,
		Distinct:           s.Distinct,
		Limit:              s.Limit,
		NeedWindow:         needWindow,
		Mode:               mode,
		MatchRecognize:     s.MatchRecognize,
		AnalyticFields:     analyticFields,
		SimpleFields:       simpleFields,
		Having:             havingRewritten,
		FieldExpressions:   expressions,
		PostAggExpressions: postAggExpressions,
		FieldOrder:         fieldOrder,
		OrderBy:            s.OrderBy,
		JoinConfigs:        s.JoinConfigs,
		SourceAlias:        s.SourceAlias,
	}

	// Extract the analysis function call (including OVER) from WHERE and replace it with placeholders for direct path state machine evaluation.
	rewrittenCondition, whereCalls, err := extractWhereAnalyticCalls(s.Condition)
	if err != nil {
		return nil, "", err
	}
	config.WhereAnalyticCalls = whereCalls

	return &config, rewrittenCondition, nil
}

// isAnalyticField checks whether a field is an analysis function (TypeAnalytical).
func isAnalyticField(f Field) bool {
	funcName := extractFunctionName(f.Expression)
	if funcName == "" {
		return false
	}
	if fn, exists := functions.Get(strings.ToLower(funcName)); exists {
		return fn.GetType() == functions.TypeAnalytical
	}
	return false
}

// containsAnalyticCall checks whether the expression contains a TypeAnalytical call.
// Intercept and embed analysis functions into scalar functions (such as UPPER(changed_col(...))):
// The analysis function must cross lines; using a stateless scalar path will silently seek errors and must be rejected during the parsing period.
// First, remove string literals to avoid misinterpreting text like "lag(") as function calls.
func containsAnalyticCall(expr string) bool {
	for _, name := range extractAllFunctions(stripStringLiterals(expr)) {
		if fn, exists := functions.Get(name); exists && fn.GetType() == functions.TypeAnalytical {
			return true
		}
	}
	return false
}

// stripStringLiterals removes the string literal content, keeping only the expression text outside the literal.
// This dialect single quotation mark '...' and double quotation marks "..." are all string literals (e.g., changed_cols("t",...)),
// Both must be stripped away; otherwise, analysis function names in double-quoted literals like "lag(x)" will be mistakenly interpreted as calls.
// Handle two consecutive quotes (” or "") in SQL escaping.
func stripStringLiterals(expr string) string {
	var b strings.Builder
	b.Grow(len(expr))
	var quote byte // 0 = not included in literal values; '\''|'"' = Current literal delimiter
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		if c == '\'' || c == '"' {
			if quote == c && i+1 < len(expr) && expr[i+1] == c {
				i++ // Escape quotation marks and skip it, still within the literal range
				continue
			}
			if quote == 0 {
				quote = c // Let's go to the literal amount
			} else if quote == c {
				quote = 0 // Literally, the amount ends
			}
			// Treat mismatched quotes (for example, ' inside ") as literal content without changing state
			continue
		}
		if quote == 0 {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// buildAnalyticField converts the analysis function Field to AnalyticField, retaining the OVER clause.
// Supports "expression package analysis functions" (e.g., ts - lag(ts)): split the bare analysis call for state machine calculations, outer expressions
// Store as WrapperExpr (parse call replaced with types.AnalyticSelfToken) supply and demand value periods for a period of regeneration.
// When the same expression contains multiple analysis calls (e.g., acc_max(v) - acc_min(v)), all are extracted, and each allocation occupies its own place.
func buildAnalyticField(f Field) types.AnalyticField {
	calls, wrapper := splitAnalyticExprMulti(f.Expression)
	alias := f.Alias
	if alias == "" {
		alias = f.Expression
	}
	af := types.AnalyticField{
		Alias:       alias,
		Over:        f.OverSpec,
		WrapperExpr: wrapper,
		Calls:       calls,
	}
	if len(calls) > 0 {
		af.FuncName = strings.ToLower(calls[0].FuncName)
		af.Expression = calls[0].BareCall
		af.Args = calls[0].Args
		// changed_cols Outputs multiple columns (dynamic column name prefix+colname), only SELECT.
		if af.FuncName == "changed_cols" {
			af.MultiColumn = true
		}
	}
	return af
}

// splitAnalyticExprMulti extracts all Analytic calls from the expression (in order they appear), replacing each call substring with
// types.AnalyticSelfTokenN(i) spaced to form the wrapper. When calling a single and overwriting the polynomial, wrapper=""(pure analysis field semantics);
// Otherwise, wrapper includes placeholders. Returns when not using parse calls (nil, "").
func splitAnalyticExprMulti(expr string) (calls []types.AnalyticCall, wrapper string) {
	pattern := regexp.MustCompile(`(?i)\b([a-z_]+)\s*\(`)
	type span struct{ start, closeParen int }
	var spans []span
	for _, m := range pattern.FindAllStringSubmatchIndex(expr, -1) {
		nm := strings.ToLower(expr[m[2]:m[3]])
		fn, exists := functions.Get(nm)
		if !exists || fn.GetType() != functions.TypeAnalytical {
			continue
		}
		openParen := m[1] - 1
		cp := findMatchingParenInternal(expr, openParen)
		if cp < 0 {
			continue
		}
		// Skips nested matches within recorded calls (analysis set analysis is not allowed, so it is not counted for safety).
		nested := false
		for _, s := range spans {
			if m[0] > s.start && cp < s.closeParen {
				nested = true
				break
			}
		}
		if nested {
			continue
		}
		spans = append(spans, span{m[0], cp})
		calls = append(calls, types.AnalyticCall{
			FuncName: nm,
			BareCall: expr[m[0] : cp+1],
			Args:     splitCallArgs(expr[m[0] : cp+1]),
		})
	}
	if len(calls) == 0 {
		return nil, ""
	}
	// Pure call and override polynomial: wrapper is left blank (keeps the semantics of the pure analysis field).
	if len(calls) == 1 {
		leading := len(expr) - len(strings.TrimLeft(expr, " \t"))
		trailing := len(strings.TrimRight(expr, " \t"))
		s := spans[0]
		if s.start == leading && s.closeParen == trailing-1 && strings.TrimSpace(expr) == strings.TrimSpace(calls[0].BareCall) {
			return calls, ""
		}
	}
	// Replace each call span from right to left to avoid index shift.
	wrapper = expr
	for i := len(spans) - 1; i >= 0; i-- {
		s := spans[i]
		wrapper = wrapper[:s.start] + types.AnalyticSelfTokenN(i) + wrapper[s.closeParen+1:]
	}
	return calls, wrapper
}

// extractInlineAggregates extracts the inline aggregates from the analysis function parameters into hidden computed fields.
// For example, changed_cols("t", true, avg(temperature)) → register aggs[__winagg_0__]=avg(temperature),
// The parameter is rewritten to __winagg_0__, and InlineAggDisplay records {__winagg_0__:"avg"}.
// Compound expression parameters (such as avg(temp) + 1) only replace the aggregate call span, while the outer operator is retained,
// The parameter becomes __winagg_0__ + 1, and the runtime is calculated by the expression evaluator.
// Hidden key prefix __winagg_ is stripped after window output and not entered for final output; The display name is used to changed_cols output column names.
func extractInlineAggregates(analyticFields []types.AnalyticField, aggs map[string]aggregator.AggregateType, fieldMap map[string]string) {
	pattern := regexp.MustCompile(`(?i)\b([a-z_]+)\s*\(`)
	seq := 0
	for i := range analyticFields {
		af := &analyticFields[i]
		// Traverse each parameter of the analysis call within the field, extract inline aggregation, and override the call in place (Args+BareCall).
		// No Calls (should not occur in the SELECT field, as a safe backup) has degenerated into single calls.
		if len(af.Calls) == 0 {
			af.Calls = []types.AnalyticCall{{FuncName: af.FuncName, BareCall: af.Expression, Args: af.Args}}
		}
		for ci := range af.Calls {
			args := af.Calls[ci].Args
			for j, arg := range args {
				trimmed := strings.TrimSpace(arg)
				idx := pattern.FindStringSubmatchIndex(trimmed)
				if idx == nil {
					continue
				}
				funcName := strings.ToLower(trimmed[idx[2]:idx[3]])
				fn, ok := functions.Get(funcName)
				if !ok || fn.GetType() != functions.TypeAggregation {
					continue
				}
				// Aggregation call span: from the function name start to the matching right parenthesis (idx[1]-1 is the position of '(').
				openParen := idx[1] - 1
				closeParen := findMatchingParenInternal(trimmed, openParen)
				if closeParen < 0 {
					continue
				}
				aggCall := trimmed[idx[0] : closeParen+1]
				// Only parse the aggregation call itself (excluding the outer operator) to avoid the full parameter being mistakenly identified as an "expression" aggregation.
				aggType, name, _, _, perr := ParseAggregateTypeWithExpression(aggCall)
				if perr != nil || aggType == "" {
					continue
				}
				hidden := fmt.Sprintf("__winagg_%d__", seq)
				seq++
				aggs[hidden] = aggType
				// Input field: name is the input field for aggregation (e.g., temperature in avg(temperature));
				// When there is no explicit field (e.g., count(*)), use the hidden key itself, and the aggregator handles it as needed.
				if name != "" {
					fieldMap[hidden] = name
				} else {
					fieldMap[hidden] = hidden
				}
				if af.InlineAggDisplay == nil {
					af.InlineAggDisplay = make(map[string]string)
				}
				af.InlineAggDisplay[hidden] = funcName
				// Only replace the aggregation call substring, retaining the outer operator (composite expression).
				args[j] = trimmed[:idx[0]] + hidden + trimmed[closeParen+1:]
				af.Calls[ci].BareCall = strings.Replace(af.Calls[ci].BareCall, aggCall, hidden, 1)
			}
		}
		// Old paths (changed_cols multi-column fanouts, etc.) still read Expression/Args and remain synchronized with the first call.
		af.Expression = af.Calls[0].BareCall
		af.Args = af.Calls[0].Args
	}
}

// collapseSpacesOutsideQuotes Remove the blank space outside the quotes, while keeping the string literal in quotes as is.
// Normalizes aggregate calls containing spaces in HAVING (stored by the parser as "max ( v )") so the parsing function can be reused.
func collapseSpacesOutsideQuotes(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' || c == '"' {
			if inQuote == c {
				inQuote = 0
			} else if inQuote == 0 {
				inQuote = c
			}
			b.WriteByte(c)
			continue
		}
		if c == ' ' && inQuote == 0 {
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// extractHavingAggregates handles aggregations referenced by HAVING (standard SQL: HAVING can reference arbitrary aggregations without having to be in SELECT).
// For each aggregation in the HAVING text, call ac:
//   - selectAlias[ac] hits (ac AS alias in SELECT) → rewrites ac in HAVING to alias (aggregation already calculated).
//   - aggs[ac] hits (no alias, selected, key is called text) → does not move.
//   - Otherwise (not selected) → register hidden aggregation __having_N__ (aggs/fieldMap in-place expansion), rewrite ac as __having_N__.
//
// Returns the rewritten HAVING text. aggs/fieldMap is a map reference, modified in place.
func extractHavingAggregates(having string, aggs map[string]aggregator.AggregateType, fieldMap map[string]string, selectAlias map[string]string) string {
	if strings.TrimSpace(having) == "" {
		return having
	}
	pattern := regexp.MustCompile(`(?i)\b([a-z_]+)\s*\(`)
	type span struct{ start, closeParen int }
	var spans []span
	var calls []string
	for _, m := range pattern.FindAllStringSubmatchIndex(having, -1) {
		nm := strings.ToLower(having[m[2]:m[3]])
		fn, ok := functions.Get(nm)
		if !ok || fn.GetType() != functions.TypeAggregation {
			continue
		}
		openParen := m[1] - 1
		cp := findMatchingParenInternal(having, openParen)
		if cp < 0 {
			continue
		}
		spans = append(spans, span{m[0], cp})
		calls = append(calls, having[m[0]:cp+1])
	}
	if len(spans) == 0 {
		return having
	}
	repl := make([]string, len(calls))
	seq := 0
	for i, ac := range calls {
		if a, ok := selectAlias[ac]; ok && a != "" {
			repl[i] = a
			continue
		}
		if _, ok := aggs[ac]; ok {
			repl[i] = ac
			continue
		}
		aggType, name, _, _, perr := ParseAggregateTypeWithExpression(collapseSpacesOutsideQuotes(ac))
		if perr != nil || aggType == "" {
			repl[i] = ac // Retain parsing failure as is (evaluation fails but does not corrupt text)
			continue
		}
		hidden := fmt.Sprintf("__having_%d__", seq)
		seq++
		aggs[hidden] = aggType
		if name != "" {
			fieldMap[hidden] = name
		} else {
			fieldMap[hidden] = hidden
		}
		repl[i] = hidden
	}
	out := having
	for i := len(spans) - 1; i >= 0; i-- {
		s := spans[i]
		out = out[:s.start] + repl[i] + out[s.closeParen+1:]
	}
	return out
}

// validateWindowAnalyticArgs The parsing function parameters in validateWindowAnalyticArgs queries must not reference the raw raw columns:
// The window produces rows that only contain aggregation and GROUP BY keys; if a bare column does not get a value, it will silently receive a column name string.
// Allowed: literals, __winagg_ Hide aggregation keys, GROUP BY keys, function calls, complex expressions (including operators).
// Only intercepts the most common misuse of "bare column name and not GROUP BY key."
func validateWindowAnalyticArgs(analyticFields []types.AnalyticField, groupKeys []string) error {
	keySet := make(map[string]bool, len(groupKeys))
	for _, k := range groupKeys {
		keySet[k] = true
	}
	for _, af := range analyticFields {
		for _, arg := range af.Args {
			a := strings.TrimSpace(arg)
			if a == "" || strings.HasPrefix(a, "__winagg_") {
				continue
			}
			if isLiteralToken(a) {
				continue
			}
			// Function calls or complex expressions containing operators: aggregate extraction of processed func(), not detailed here.
			if strings.ContainsAny(a, " ()<>=+-*/%") {
				continue
			}
			last := a
			if dot := strings.LastIndex(a, "."); dot >= 0 {
				last = a[dot+1:]
			}
			if !keySet[last] {
				return fmt.Errorf("analytic argument %q in a windowed query must reference an aggregate or a GROUP BY field, not a raw column", a)
			}
		}
	}
	return nil
}

// isLiteralToken checks whether it is a literal quantity (number/boolean/nil/quotation string).
func isLiteralToken(s string) bool {
	if s == "true" || s == "false" || s == "nil" {
		return true
	}
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return true
	}
	if s == "" {
		return false
	}
	c := s[0]
	return (c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.'
}

// splitCallArgs extracts a segment of top-level parameter expression from the function call text (not evaluated).
// For example, changed_cols("c_", true, temperature, humidity) → ["\"c_\"", "true", "temperature", "humidity"].
// Returns nil when parsing fails or has no parameters.
func splitCallArgs(expr string) []string {
	open := strings.IndexByte(expr, '(')
	if open < 0 {
		return nil
	}
	close := strings.LastIndexByte(expr, ')')
	if close <= open {
		return nil
	}
	body := expr[open+1 : close]
	return splitTopLevelCommas(body)
}

// splitTopLevelCommas splits commas by top-level commas, ignoring commas within nested parentheses and string literals.
func splitTopLevelCommas(s string) []string {
	var args []string
	depth := 0
	last := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case '"':
			i++
			for i < len(s) && s[i] != '"' {
				i++
			}
		case '\'':
			i++
			for i < len(s) && s[i] != '\'' {
				i++
			}
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(s[last:i]))
				last = i + 1
			}
		}
	}
	tail := strings.TrimSpace(s[last:])
	if tail == "" && len(args) == 0 {
		return nil
	}
	args = append(args, tail)
	return args
}

// groupKeyIsScalarFunctionExpr reports whether expr is a function expression whose
// The top-level function is a registered scalar (non-aggregation/analysis/window) function. Used for release
// GROUP BY upper(device) Function expressions like this group key, while rejecting misspelled window functions (e.g., Foo(5)).
func groupKeyIsScalarFunctionExpr(expr string) bool {
	fn, exists := functions.Get(strings.ToLower(extractFunctionName(expr)))
	if !exists {
		return false
	}
	switch fn.GetType() {
	case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
		return false
	}
	return true
}

// Check if expression is an aggregation function
func isAggregationFunction(expr string) bool {
	// Extract function name
	funcName := extractFunctionName(expr)
	if funcName == "" {
		return false
	}

	// Check if it's a registered function
	if fn, exists := functions.Get(funcName); exists {
		// Determine if aggregation processing is needed based on function type
		switch fn.GetType() {
		case functions.TypeAggregation:
			// Aggregation function needs aggregation processing
			return true
		case functions.TypeAnalytical:
			// Analytical function also needs aggregation processing (state management)
			return true
		case functions.TypeWindow:
			// Window function needs aggregation processing
			return true

		default:
			// Other types of functions (string, conversion, etc.) don't need aggregation processing
			return false
		}
	}

	// For unregistered functions, check if it's expr-lang built-in function
	// These functions are handled through ExprBridge, don't need aggregation mode
	bridge := functions.GetExprBridge()
	if bridge.IsExprLangFunction(funcName) {
		return false
	}

	// If not registered function and not expr-lang function, but contains parentheses, conservatively assume it might be aggregation function
	if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
		return true
	}
	return false
}

// extractFieldOrder extracts original order of fields from Fields slice
// Returns field names list in order of appearance in SELECT statement
func extractFieldOrder(fields []Field) ([]string, error) {
	var fieldOrder []string

	for _, field := range fields {
		// If has alias, use alias as field name
		if field.Alias != "" {
			fieldOrder = append(fieldOrder, field.Alias)
		} else {
			// Without alias, try to parse expression to get field name
			_, fieldName, _, _, err := ParseAggregateTypeWithExpression(field.Expression)
			if err != nil {
				return nil, err
			}
			if fieldName != "" {
				// If parsed field name (like string literal), use parsed name
				fieldOrder = append(fieldOrder, fieldName)
			} else {
				// Otherwise use original expression as field name
				fieldOrder = append(fieldOrder, field.Expression)
			}
		}
	}

	return fieldOrder, nil
}
func extractGroupFields(s *SelectStatement) []string {
	var fields []string
	for _, f := range s.GroupBy {
		// Retain bare lists and scalar function expressions (such as upper(device)); Only excludes aggregation functions as grouping keys (meaningless).
		if isAggregationFunction(f) {
			continue
		}
		fields = append(fields, f)
	}
	return fields
}

// buildSelectAliasMap maps each SELECT item's raw expression to its AS alias.
// Items without an alias are omitted. Used by the aggregation path to name
// output columns for grouped non-aggregate columns (e.g. "m.location AS loc"),
// matching the direct path.
func buildSelectAliasMap(fields []Field) map[string]string {
	m := make(map[string]string, len(fields))
	for _, f := range fields {
		if f.Alias != "" {
			m[f.Expression] = f.Alias
		}
	}
	return m
}

func buildSelectFields(fields []Field) (aggMap map[string]aggregator.AggregateType, fieldMap map[string]string, err error) {
	selectFields := make(map[string]aggregator.AggregateType)
	fieldMap = make(map[string]string)

	for _, f := range fields {
		if alias := f.Alias; alias != "" {
			t, n, _, _, parseErr := ParseAggregateTypeWithExpression(f.Expression)
			if parseErr != nil {
				return nil, nil, parseErr
			}
			if t != "" {
				// Use alias as key for aggregator, not field name
				selectFields[alias] = t

				// Field mapping: output field name(alias) -> input field name (consistent with buildSelectFieldsWithExpressions)
				if n != "" {
					fieldMap[alias] = n
				} else {
					// If no field name extracted, use alias itself
					fieldMap[alias] = alias
				}
			}
		} else {
			// Without alias, use expression itself as field name
			t, n, _, _, parseErr := ParseAggregateTypeWithExpression(f.Expression)
			if parseErr != nil {
				return nil, nil, parseErr
			}
			if t != "" && n != "" {
				selectFields[n] = t
				fieldMap[n] = n
			}
		}
	}
	return selectFields, fieldMap, nil
}

// detectNestedAggregation detects whether the expression contains nested aggregation functions
// If nested aggregation functions are found, an error message is returned
func detectNestedAggregation(expr string) error {
	return detectNestedAggregationRecursive(expr, false, false)
}

// detectNestedAggregationRecursive: Recursive detection of nested aggregation/analysis functions.
// inAggregation: currently inside the true aggregation (TypeAggregation); inAnalytic: Currently inside the analysis function.
// Rule: Aggregation set aggregation → Error report; Analysis set analysis → error reporting; Aggregation set analysis → error reporting;
//
//	Analyze the aggregation → allowed (e.g., changed_cols(avg(...)), the analysis function evaluates the output of the window aggregation).
func detectNestedAggregationRecursive(expr string, inAggregation, inAnalytic bool) error {
	pattern := regexp.MustCompile(`(?i)([a-z_]+)\s*\(`)
	matches := pattern.FindAllStringSubmatchIndex(expr, -1)

	for _, match := range matches {
		funcStart := match[0]
		funcName := strings.ToLower(expr[match[2]:match[3]])

		if fn, exists := functions.Get(funcName); exists {
			ft := fn.GetType()
			if ft == functions.TypeAggregation || ft == functions.TypeAnalytical || ft == functions.TypeWindow {
				switch ft {
				case functions.TypeAggregation:
					// Aggregation functions cannot be embedded inside the aggregation function.
					if inAggregation {
						return fmt.Errorf("aggregate function calls cannot be nested")
					}
				case functions.TypeAnalytical:
					// Analyzer set analysis, or aggregation set analysis → error (the parser function can only wrap aggregation).
					if inAnalytic || inAggregation {
						return fmt.Errorf("analytic functions cannot be nested in %s", funcName)
					}
				case functions.TypeWindow:
					if inAggregation || inAnalytic {
						return fmt.Errorf("window function %s cannot be nested here", funcName)
					}
				}
				funcEnd := findMatchingParenInternal(expr, funcStart+len(funcName))
				if funcEnd > funcStart {
					paramStart := funcStart + len(funcName) + 1
					params := expr[paramStart:funcEnd]
					// Enter true aggregation: mark inAggregation; Enter the analysis: Mark inAnalytic (do not mark inAggregation,
					// This allows further aggregation within the analysis function, i.e., "analysis set aggregation").
					nextAgg := inAggregation || ft == functions.TypeAggregation
					nextAna := inAnalytic || ft == functions.TypeAnalytical
					if err := detectNestedAggregationRecursive(params, nextAgg, nextAna); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// Parse aggregation function and return expression information
func ParseAggregateTypeWithExpression(exprStr string) (aggType aggregator.AggregateType, name string, expression string, allFields []string, err error) {
	// First, check for nested aggregation functions
	if err := detectNestedAggregation(exprStr); err != nil {
		// If nested aggregation is found, an error is returned
		return "", "", "", nil, err
	}

	// Special handling for CASE expressions
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(exprStr)), "CASE") {
		// CASE expressions are handled as special expressions
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", "", exprStr, allFields, nil
	}

	// Check if it's an expression containing operators with functions
	if containsOperatorsOutsideFunctions(exprStr) && containsFunctions(exprStr) {
		// This is a complex expression with functions and operators
		// Extract all fields referenced in the expression
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		// Return as expression type for post-aggregation evaluation
		return "expression", "", exprStr, allFields, nil
	}

	// Original logic for single function (moved up to prioritize outer function detection)
	// Extract function name
	funcName := extractFunctionName(exprStr)

	// Check if it's nested functions without operators
	hasNested := hasNestedFunctions(exprStr)
	if hasNested && funcName != "" {
		// For nested functions, check if the outer function is an aggregation function
		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				// Outer function is aggregation - handle as aggregation with expression parameter
				name, expression, allFields := extractAggFieldWithExpression(exprStr, funcName)

				return aggregator.AggregateType(funcName), name, expression, allFields, nil
			}
		}
		// Multiple functions but no operators and outer function is not aggregation - treat as expression
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", "", exprStr, allFields, nil
	}
	if funcName == "" {
		// Special handling for SELECT * case
		if strings.TrimSpace(exprStr) == "*" {
			return "", "", "", nil, nil // Don't treat * as expression
		}

		// Check if it's a string literal
		trimmed := strings.TrimSpace(exprStr)
		if (strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'")) ||
			(strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"")) {
			// String literal: use content without quotes as field name
			fieldName := trimmed[1 : len(trimmed)-1]
			return "expression", fieldName, exprStr, nil, nil
		}

		// If not a function call but contains operators or keywords, it might be an expression
		if strings.ContainsAny(exprStr, "+-*/<>=!&|") ||
			strings.Contains(strings.ToUpper(exprStr), "AND") ||
			strings.Contains(strings.ToUpper(exprStr), "OR") {
			// Handle as expression
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
			return "expression", "", exprStr, allFields, nil
		}
		return "", "", "", nil, nil
	}

	// Check if it's a registered function
	fn, exists := functions.Get(funcName)
	if !exists {
		return "", "", "", nil, nil
	}

	// Extract function parameters and expression information
	name, expression, allFields = extractAggFieldWithExpression(exprStr, funcName)

	// Determine aggregation type based on function type
	switch fn.GetType() {
	case functions.TypeAggregation:
		// Aggregation function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields, nil

	case functions.TypeAnalytical:
		// Analytical function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields, nil

	case functions.TypeWindow:
		// Window function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields, nil

	case functions.TypeString, functions.TypeConversion, functions.TypeCustom, functions.TypeMath:
		// String, conversion, custom, math functions: handle as expressions in aggregation queries
		// Use "expression" as special aggregation type, indicating this is an expression calculation
		// For these functions, should save complete function call as expression, not just parameter part
		fullExpression := exprStr
		if parsedExpr, err := expr.NewExpression(fullExpression); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", name, fullExpression, allFields, nil

	default:
		// Other types of functions don't use aggregation
		// These functions will be handled directly in non-window mode
		return "", "", "", nil, nil
	}
}

// extractFunctionName extracts function name from expression
func extractFunctionName(expr string) string {
	// Find first left parenthesis
	parenIndex := strings.Index(expr, "(")
	if parenIndex == -1 {
		return ""
	}

	// Extract function name part
	funcName := strings.TrimSpace(expr[:parenIndex])

	// If function name contains other operators or spaces, it's not a simple function call
	if strings.ContainsAny(funcName, " +-*/=<>!&|") {
		return ""
	}

	return funcName
}

// Extract all function names from expression
func extractAllFunctions(expr string) []string {
	var funcNames []string

	// Simple function name matching
	i := 0
	for i < len(expr) {
		// Find function name pattern
		start := i
		for i < len(expr) && (expr[i] >= 'a' && expr[i] <= 'z' || expr[i] >= 'A' && expr[i] <= 'Z' || expr[i] == '_') {
			i++
		}

		if i < len(expr) && expr[i] == '(' && i > start {
			// Found possible function name
			funcName := expr[start:i]
			if _, exists := functions.Get(funcName); exists {
				funcNames = append(funcNames, funcName)
			}
		}

		if i < len(expr) {
			i++
		}
	}

	return funcNames
}

// Check if expression contains nested functions
func hasNestedFunctions(expr string) bool {
	funcs := extractAllFunctions(expr)
	return len(funcs) > 1
}

// containsOperators checks if expression contains arithmetic or comparison operators
func containsOperators(expr string) bool {
	return strings.ContainsAny(expr, "+-*/<>=!&|")
}

// containsFunctions checks if expression contains function calls
func containsFunctions(expr string) bool {
	funcs := extractAllFunctions(expr)
	return len(funcs) > 0
}

// Extract aggregation function fields and parse expression information
func extractAggFieldWithExpression(exprStr string, funcName string) (fieldName string, expression string, allFields []string) {

	start := strings.Index(strings.ToLower(exprStr), strings.ToLower(funcName)+"(")
	if start < 0 {
		return "", "", nil
	}
	start += len(funcName) + 1

	end := strings.LastIndex(exprStr, ")")
	if end <= start {
		return "", "", nil
	}

	// Extract expression within parentheses
	fieldExpr := strings.TrimSpace(exprStr[start:end])

	// Special handling for count(*) case
	if strings.ToLower(funcName) == "count" && fieldExpr == "*" {
		return "*", "", nil
	}

	// Check if it's a registered function and get its type
	if fn, exists := functions.Get(funcName); exists {
		// For string functions that need special parameter parsing
		if fn.GetType() == functions.TypeString {
			// Intelligently parse function parameters to extract field names
			var fields []string
			params := parseSmartParameters(fieldExpr)
			for _, param := range params {
				param = strings.TrimSpace(param)
				// If parameter is not string constant (not surrounded by quotes), consider it as field name
				if !((strings.HasPrefix(param, "'") && strings.HasSuffix(param, "'")) ||
					(strings.HasPrefix(param, "\"") && strings.HasSuffix(param, "\""))) {
					if isIdentifier(param) {
						fields = append(fields, param)
					}
				}
			}
			if len(fields) > 0 {
				// For string functions, save complete function call as expression
				// Return all extracted fields as allFields
				return fields[0], strings.ToLower(funcName) + "(" + fieldExpr + ")", fields
			}
			// If no field found, return empty field name but keep expression
			return "", strings.ToLower(funcName) + "(" + fieldExpr + ")", nil
		}
	}

	// Check if it's a multi-parameter function call (contains comma)
	if strings.Contains(fieldExpr, ",") {
		// For multi-parameter functions, extract the first parameter as the field name
		params := strings.Split(fieldExpr, ",")
		if len(params) > 0 {
			firstParam := strings.TrimSpace(params[0])
			// Return first parameter as field name, and full expression for parameter processing
			return firstParam, fieldExpr, nil
		}
	}

	// Check if it's a simple field name (only letters, numbers, underscores)
	isSimpleField := true
	for _, char := range fieldExpr {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			isSimpleField = false
			break
		}
	}

	// If simple field, return field name directly, don't create expression
	if isSimpleField {
		return fieldExpr, "", nil
	}

	// For complex expressions, including multi-parameter function calls
	expression = fieldExpr

	// Use expression engine to parse
	parsedExpr, err := expr.NewExpression(fieldExpr)
	if err != nil {
		// If expression parsing fails, try manual parameter parsing
		// This is mainly used to handle multi-parameter functions like distance(x1, y1, x2, y2)
		if strings.Contains(fieldExpr, ",") {
			// Split parameters
			params := strings.Split(fieldExpr, ",")
			var fields []string
			for _, param := range params {
				param = strings.TrimSpace(param)
				if isIdentifier(param) {
					fields = append(fields, param)
				}
			}
			if len(fields) > 0 {
				// For multi-parameter functions, use all parameter fields, main field name is first parameter
				return fields[0], expression, fields
			}
		}

		// If still fails to parse, try simple extraction method
		fieldName = extractSimpleField(fieldExpr)
		return fieldName, expression, []string{fieldName}
	}

	// Get all fields referenced in expression
	allFields = parsedExpr.GetFields()

	// If only one field, return directly
	if len(allFields) == 1 {
		return allFields[0], expression, allFields
	}

	// If multiple fields, use first field name as main field
	if len(allFields) > 0 {
		// Record complete expression and all fields
		return allFields[0], expression, allFields
	}

	// If no fields (pure constant expression), return entire expression as field name

	return fieldExpr, expression, nil
}

// parseSmartParameters intelligently parses function parameters, correctly handles commas within quotes
func parseSmartParameters(paramsStr string) []string {
	var params []string
	var current strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(paramsStr); i++ {
		ch := paramsStr[i]

		if !inQuotes {
			if ch == '\'' || ch == '"' {
				inQuotes = true
				quoteChar = ch
				current.WriteByte(ch)
			} else if ch == ',' {
				// Parameter separator
				params = append(params, current.String())
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		} else {
			if ch == quoteChar {
				inQuotes = false
				quoteChar = 0
			}
			current.WriteByte(ch)
		}
	}

	// Add the last parameter
	if current.Len() > 0 {
		params = append(params, current.String())
	}

	return params
}

// isIdentifier checks if string is a valid identifier
func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}
	// First character must be letter or underscore
	if !((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') || s[0] == '_') {
		return false
	}
	// Remaining characters must be letters, numbers, or underscores
	for i := 1; i < len(s); i++ {
		char := s[i]
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	return true
}

// extractSimpleField for backward compatibility
func extractSimpleField(fieldExpr string) string {
	// If contains operators, extract first operand as field name
	for _, op := range []string{"/", "*", "+", "-"} {
		if opIndex := strings.Index(fieldExpr, op); opIndex > 0 {
			return strings.TrimSpace(fieldExpr[:opIndex])
		}
	}
	return fieldExpr
}

// validateWindowParams validates and converts window parameters based on window type
// Returns validated parameters array with proper types
func validateWindowParams(params []any, windowType string) ([]any, error) {
	if len(params) == 0 {
		return params, nil
	}

	validated := make([]any, 0, len(params))

	if windowType == window.TypeCounting {
		// CountingWindow expects integer count as first parameter
		if len(params) == 0 {
			return nil, fmt.Errorf("counting window requires at least one parameter")
		}

		// Convert first parameter to int using cast utility
		count, err := cast.ToIntE(params[0])
		if err != nil {
			return nil, fmt.Errorf("invalid count parameter: %w", err)
		}

		if count <= 0 {
			return nil, fmt.Errorf("counting window count must be positive, got: %d", count)
		}

		validated = append(validated, count)

		// Add any additional parameters
		if len(params) > 1 {
			validated = append(validated, params[1:]...)
		}

		return validated, nil
	}

	// Helper function to convert a value to time.Duration
	// For numeric types, treats them as seconds
	// For strings, uses time.ParseDuration
	convertToDuration := func(val any) (time.Duration, error) {
		switch v := val.(type) {
		case time.Duration:
			return v, nil
		case string:
			// Use ToDurationE which handles string parsing
			return cast.ToDurationE(v)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			// Treat numeric integers as seconds
			return time.Duration(cast.ToInt(v)) * time.Second, nil
		case float32, float64:
			// Treat numeric floats as seconds
			return time.Duration(int(cast.ToFloat64(v))) * time.Second, nil
		default:
			// Try ToDurationE as fallback
			return cast.ToDurationE(v)
		}
	}

	if windowType == window.TypeSession {
		// SessionWindow expects timeout duration as first parameter
		if len(params) == 0 {
			return nil, fmt.Errorf("session window requires at least one parameter")
		}

		timeout, err := convertToDuration(params[0])
		if err != nil {
			return nil, fmt.Errorf("invalid timeout duration: %w", err)
		}

		if timeout <= 0 {
			return nil, fmt.Errorf("session window timeout must be positive, got: %v", timeout)
		}

		validated = append(validated, timeout)

		// Add any additional parameters
		if len(params) > 1 {
			validated = append(validated, params[1:]...)
		}

		return validated, nil
	}

	// For TumblingWindow and SlidingWindow, convert parameters to time.Duration
	for index, v := range params {
		dur, err := convertToDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid duration parameter at index %d: %w", index, err)
		}

		if dur <= 0 {
			return nil, fmt.Errorf("duration parameter at index %d must be positive, got: %v", index, dur)
		}

		validated = append(validated, dur)
	}

	return validated, nil
}

func parseAggregateExpression(expr string) string {
	if strings.Contains(expr, functions.AvgStr+"(") {
		return functions.AvgStr
	}
	if strings.Contains(expr, functions.SumStr+"(") {
		return functions.SumStr
	}
	if strings.Contains(expr, functions.MaxStr+"(") {
		return functions.MaxStr
	}
	if strings.Contains(expr, functions.MinStr+"(") {
		return functions.MinStr
	}
	return ""
}

// Parse field information including expressions with post-aggregation support
func buildSelectFieldsWithExpressions(fields []Field) (
	aggMap map[string]aggregator.AggregateType,
	fieldMap map[string]string,
	expressions map[string]types.FieldExpression,
	postAggExpressions []types.PostAggregationExpression,
	err error) {

	selectFields := make(map[string]aggregator.AggregateType)
	fieldMap = make(map[string]string)
	expressions = make(map[string]types.FieldExpression)
	postAggExpressions = make([]types.PostAggregationExpression, 0)

	for _, f := range fields {
		alias := f.Alias
		if alias == "" {
			// For string literals without alias, use the content without quotes as alias
			trimmed := strings.TrimSpace(f.Expression)
			if (strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'")) ||
				(strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"")) {
				alias = trimmed[1 : len(trimmed)-1] // Remove quotes
			} else {
				alias = f.Expression
			}
		}

		// Check if this is a complex aggregation expression
		if isComplexAggregationExpression(f.Expression) {
			// Parse complex aggregation expression
			aggFields, exprTemplate, err := parseComplexAggregationExpression(f.Expression)
			if err == nil && len(aggFields) > 0 {
				// Add individual aggregation functions
				for _, aggField := range aggFields {
					selectFields[aggField.Placeholder] = aggField.AggType
					fieldMap[aggField.Placeholder] = aggField.InputField
				}

				// Add post-aggregation expression
				postAggExpressions = append(postAggExpressions, types.PostAggregationExpression{
					OutputField:        alias,
					OriginalExpr:       f.Expression,
					ExpressionTemplate: exprTemplate,
					RequiredFields:     aggFields,
				})

				// Mark the main field as post-aggregation
				selectFields[alias] = "post_aggregation"
				fieldMap[alias] = alias
				continue
			}
		}

		// Handle as regular expression
		t, n, expression, allFields, parseErr := ParseAggregateTypeWithExpression(f.Expression)
		if parseErr != nil {
			// If nested aggregation functions are detected, an error is returned
			return nil, nil, nil, nil, parseErr
		}
		if t != "" {
			// Check if this is a multi-parameter function that needs special handling
			isMultiParamFunction := false
			if expression != "" && strings.Contains(expression, ",") {
				// Check if the function needs multi-parameter handling
				funcName := extractFunctionName(f.Expression)
				if fn, exists := functions.Get(funcName); exists {
					minArgs := fn.GetMinArgs()
					maxArgs := fn.GetMaxArgs()
					// Function needs multi-parameter handling if it has multiple parameters
					isMultiParamFunction = minArgs > 1 || (maxArgs > minArgs && minArgs >= 1)
				}
			}

			// For multi-parameter functions, treat as post-aggregation expression
			if isMultiParamFunction {
				// Parse as single aggregation function with parameters
				aggFields := []types.AggregationFieldInfo{{
					FuncName:    extractFunctionName(f.Expression),
					InputField:  n,
					Placeholder: "__" + extractFunctionName(f.Expression) + "_" + alias + "__",
					AggType:     aggregator.AggregateType(extractFunctionName(f.Expression)),
					FullCall:    f.Expression,
				}}

				// Add the aggregation function
				selectFields[aggFields[0].Placeholder] = aggFields[0].AggType
				fieldMap[aggFields[0].Placeholder] = aggFields[0].InputField

				// Add post-aggregation expression (which just returns the placeholder value)
				postAggExpressions = append(postAggExpressions, types.PostAggregationExpression{
					OutputField:        alias,
					OriginalExpr:       f.Expression,
					ExpressionTemplate: aggFields[0].Placeholder,
					RequiredFields:     aggFields,
				})

				// Mark the main field as post-aggregation
				selectFields[alias] = "post_aggregation"
				fieldMap[alias] = alias
				continue
			}

			// Use alias as key so each aggregation function has unique key
			selectFields[alias] = t

			// Field mapping: output field name -> input field name (prepare correct mapping for aggregator)
			if n != "" {
				fieldMap[alias] = n
			} else {
				// If no field name extracted, use alias itself
				fieldMap[alias] = alias
			}

			// If expression exists, save expression information
			if expression != "" {
				expressions[alias] = types.FieldExpression{
					Field:      n,
					Expression: expression,
					Fields:     allFields,
				}
			}
		}
	}
	return selectFields, fieldMap, expressions, postAggExpressions, nil
}

// isComplexAggregationExpression checks if an expression contains multiple aggregation functions or operators with aggregation functions
func isComplexAggregationExpression(expr string) bool {
	// Check if expression contains aggregation functions
	funcs := extractAllFunctions(expr)
	aggCount := 0
	nonAggCount := 0

	for _, funcName := range funcs {
		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				aggCount++
			default:
				nonAggCount++
			}
		} else {
			nonAggCount++
		}
	}

	// Determine the outermost function name (if any)
	outerFuncName := ""
	if m := regexp.MustCompile(`(?i)^\s*([a-z_][a-z0-9_]*)\s*\(`).FindStringSubmatch(expr); len(m) == 2 {
		outerFuncName = strings.ToLower(m[1])
	}
	outerIsAggregation := false
	if outerFuncName != "" {
		if fn, ok := functions.Get(outerFuncName); ok {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				outerIsAggregation = true
			}
		}
	}

	// Special case: single aggregation function with nested expression (only when OUTER is aggregation)
	isSingleAggWithNestedFunc := false
	if aggCount == 1 && outerIsAggregation {
		start := strings.Index(expr, "(")
		end := strings.LastIndex(expr, ")")
		if start != -1 && end != -1 && end > start {
			innerExpr := strings.TrimSpace(expr[start+1 : end])
			if !containsOperators(innerExpr) {
				isSingleAggWithNestedFunc = true
			}
		}
	}

	result := (aggCount > 1) ||
		(aggCount > 0 && containsOperatorsOutsideFunctions(expr) && !isSingleAggWithNestedFunc) ||
		(aggCount > 0 && nonAggCount > 0 && !isSingleAggWithNestedFunc)

	return result
}

// containsOperatorsOutsideFunctions checks if expression contains operators outside function calls
func containsOperatorsOutsideFunctions(expr string) bool {
	// Remove function calls first, then check for operators
	// Simple approach: if it's just a single function call, it shouldn't be treated as complex
	trimmed := strings.TrimSpace(expr)

	// If it starts with a function name and ends with ), it's likely a simple function call
	if match := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\s*\([^)]*\)$`).FindString(trimmed); match == trimmed {
		return false
	}

	// Check for operators
	return containsOperators(expr)
}

// parseComplexAggregationExpression parses expressions containing multiple aggregation functions
func parseComplexAggregationExpression(expr string) ([]types.AggregationFieldInfo, string, error) {
	return parseComplexAggExpressionInternal(expr)
}

// parseComplexAggExpressionInternal implements the actual parsing logic
func parseComplexAggExpressionInternal(expr string) ([]types.AggregationFieldInfo, string, error) {
	// First, detect nested polymerization
	if err := detectNestedAggregation(expr); err != nil {
		return nil, "", err
	}

	// Using an improved recursive analysis method
	aggFields, exprTemplate := parseNestedFunctionsInternal(expr, make([]types.AggregationFieldInfo, 0))
	return aggFields, exprTemplate, nil
}

// parseNestedFunctionsInternal Recursive Parsing nested function call
func parseNestedFunctionsInternal(expr string, aggFields []types.AggregationFieldInfo) ([]types.AggregationFieldInfo, string) {
	// Match function calls, supporting case-insensitive calls
	pattern := regexp.MustCompile(`(?i)([a-z_]+)\s*\(`)

	// Find the starting position of all function calls
	matches := pattern.FindAllStringSubmatchIndex(expr, -1)
	if len(matches) == 0 {
		return aggFields, expr
	}

	// Handle from right to left to avoid index offset issues
	for i := len(matches) - 1; i >= 0; i-- {
		match := matches[i]
		funcStart := match[0]
		funcName := strings.ToLower(expr[match[2]:match[3]])

		// Find the matching right parenthesis
		parenStart := match[3]
		parenEnd := findMatchingParenInternal(expr, parenStart)
		if parenEnd == -1 {
			continue
		}

		fullFuncCall := expr[funcStart : parenEnd+1]
		funcParam := expr[parenStart+1 : parenEnd]

		// Check if it is an aggregate function
		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				// Generates a unique placeholder
				callHash := 0
				for _, c := range fullFuncCall {
					callHash = callHash*31 + int(c)
				}
				if callHash < 0 {
					callHash = -callHash
				}
				placeholder := fmt.Sprintf("__%s_%d__", funcName, callHash)

				// Parsing function parameters
				inputField := strings.TrimSpace(funcParam)
				// For aggregate functions, if the parameters contain nested function calls, keep the full argument
				// Segmentation is only performed when the parameters are simply comma-separated lists
				if strings.Contains(funcParam, ",") && !containsNestedFunctions(funcParam) {
					params := strings.Split(funcParam, ",")
					if len(params) > 0 {
						inputField = strings.TrimSpace(params[0])
					}
				}

				// Add to the aggregated field list
				fieldInfo := types.AggregationFieldInfo{
					FuncName:    funcName,
					InputField:  inputField,
					Placeholder: placeholder,
					AggType:     aggregator.AggregateType(funcName),
					FullCall:    fullFuncCall,
				}
				aggFields = append(aggFields, fieldInfo)

				// Replace aggregation function calls in expressions
				expr = expr[:funcStart] + placeholder + expr[parenEnd+1:]
			}
		}
	}

	return aggFields, expr
}

// containsNestedFunctions checks whether the parameter string contains nested function calls
func containsNestedFunctions(param string) bool {
	// Simple check: If the function name pattern is followed by parentheses, it is considered a nested function
	pattern := regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*\s*\(`)
	return pattern.MatchString(param)
}

// findMatchingParenInternal Find the right bracket of the match
func findMatchingParenInternal(s string, start int) int {
	if start >= len(s) || s[start] != '(' {
		return -1
	}

	count := 1
	for i := start + 1; i < len(s); i++ {
		switch s[i] {
		case '(':
			count++
		case ')':
			count--
			if count == 0 {
				return i
			}
		}
	}
	return -1 // No matching right bracket found
}
