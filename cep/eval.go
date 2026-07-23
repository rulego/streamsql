// Package cep implements MATCH_RECOGNIZE pattern recognition (SQL:2016).
//
// Structure:
//   - pattern.go Composable schema tree → NFA (Thompson construct), supporting sequences/quantifiers/selection/grouping/PERMUTE.
//   - nfa.go NFA State Machine + epsilon closure.
//   - eval.go DEFINE/MEASURES Evaluation: The expression is precompiled during the NewEngine phase,
//     Navigation/aggregation/symbol constraint fields are rewritten as placeholders, and the handwritten expr engine is used for evaluation.
//   - engine.go Partition NFA Analog + LRU + WITHIN/ Row Limit (Bounded) + MEASURES Projection + Match Output.
//
// CEP is an independent subsystem that neither depends on nor contaminates existing direct/window/analysis paths and three sets of evaluators.
package cep

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/utils/cast"
)

// matchCtx is the context for DEFINE/MEASURES evaluation: all rows + classification labels for a single match attempt.
// When evaluating DEFINE, candidate = row to be classified, cur = len(rows) (candidate is conceptually at the cur position);
// When evaluating MEASURES, candidate=nil, cur=inscript of the evaluated line (ONE ROW = last line).
type matchCtx struct {
	rows        []map[string]any
	labels      []string
	cur         int
	candidate   map[string]any
	candLabel   string
	symbols     map[string]bool
	subsets     map[string][]string // SUBSET Name → Member Symbol (filter by member set; nil = common symbol)
	matchNumber int
}

// placeholder prefix: Evaluates the navigation/aggregation/symbol limit field and injects the base composite key.
const placeholderPrefix = "__cep_"

// ekind identifies the expression token category (used only internally in rewriter).
type ekind int

const (
	ekIdent ekind = iota
	ekNumber
	ekString
	ekOp
	ekLParen
	ekRParen
	ekComma
	ekDot
)

type etoken struct {
	kind ekind
	val  string
}

// The tokenize expression is token stream. It only serves structure recognition (function calls, symbol qualification, bracket balancing);
// The semantics of arithmetic/comparison/logic are handed over to the expr engine, so operators are always kept as is.
func tokenize(s string) ([]etoken, error) {
	var toks []etoken
	runes := []rune(s)
	i := 0
	for i < len(runes) {
		c := runes[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, etoken{ekLParen, "("})
			i++
		case c == ')':
			toks = append(toks, etoken{ekRParen, ")"})
			i++
		case c == ',':
			toks = append(toks, etoken{ekComma, ","})
			i++
		case c == '.':
			toks = append(toks, etoken{ekDot, "."})
			i++
		case c == '\'' || c == '"' || c == '`':
			j := i + 1
			for j < len(runes) && runes[j] != c {
				j++
			}
			if j >= len(runes) {
				return nil, fmt.Errorf("unterminated quote in CEP expression: %s", s)
			}
			toks = append(toks, etoken{ekString, string(runes[i : j+1])})
			i = j + 1
		case unicode.IsLetter(c) || c == '_':
			j := i
			for j < len(runes) && (unicode.IsLetter(runes[j]) || unicode.IsDigit(runes[j]) || runes[j] == '_') {
				j++
			}
			toks = append(toks, etoken{ekIdent, string(runes[i:j])})
			i = j
		case unicode.IsDigit(c):
			j := i
			for j < len(runes) && (unicode.IsDigit(runes[j]) || runes[j] == '.') {
				j++
			}
			toks = append(toks, etoken{ekNumber, string(runes[i:j])})
			i = j
		default:
			// Operators (with multicharacter >=, <=,!=, ==, &&, ||) All are collected as OP and kept as is.
			j := i
			for j < len(runes) && isOpRune(runes[j]) {
				j++
			}
			if j == i {
				return nil, fmt.Errorf("unexpected character %q in CEP expression", c)
			}
			toks = append(toks, etoken{ekOp, string(runes[i:j])})
			i = j
		}
	}
	return toks, nil
}

func isOpRune(c rune) bool {
	switch c {
	case '>', '<', '=', '!', '&', '|', '+', '-', '*', '/':
		return true
	}
	return false
}

// navFuncs is the CEP navigation/aggregation function name (rewriter intercepts them, and other function calls are passed as-is to the expr engine).
var navFuncs = map[string]bool{
	"PREV": true, "NEXT": true, "FIRST": true, "LAST": true,
	"CLASSIFIER": true, "MATCH_NUMBER": true,
	"SUM": true, "AVG": true, "COUNT": true, "MIN": true, "MAX": true,
}

// phKind identification placeholder evaluation method (precompiled product).
type phKind int

const (
	phNav phKind = iota // Navigation/aggregation: name+args
	phSym               // Symbol-limited field: name(symbol)+field
)

// phDesc describes how a placeholder evaluates from matchCtx. Placeholder key = placeholderPrefix+i+ "__".
type phDesc struct {
	kind  phKind
	name  string   // phNav: Function name; phSym: symbol name
	args  []string // phNav: Parameter fragment
	field string   // phSym: Field name (unquoted/incorrect)
	final bool     // phNav: true=FINAL (full segment match), false=RUNNING (to current line, default)
}

// preparedExpr is a precompiled DEFINE/MEASURES expression: the rewritten string is processed by expr.NewExpression
// Compiling a single cache; When evaluating, only placeholder values and bare fields are recalculated, and the compiled product is reused (to avoid repeated parsing of hot paths).
type preparedExpr struct {
	src        string
	compiled   *expr.Expression
	phs        []phDesc // Placeholder Description (phs[i] → key placeholderPrefix+i+"__")
	bareFields []string // Pass-through bare field name (inserted into the current row field during evaluation)
	needsHist  bool     // Includes navigation/aggregation/symbol constraints → Evaluation requires historical rows
}

// placeholderKey returns the base key of the i-th placeholder.
func placeholderKey(i int) string {
	return placeholderPrefix + strconv.Itoa(i) + "__"
}

// prepare compiles the expression into preparedExpr(tokenize → rewrite plan → NewExpression).
// Purely structural, not valuable; Failure (lexical/paraphrase/compile) returns error for validate fail-fast.
func prepare(src string, symbols map[string]bool) (*preparedExpr, error) {
	src = strings.TrimSpace(src)
	toks, err := tokenize(src)
	if err != nil {
		return nil, err
	}
	p := &preparedExpr{src: src}
	var pieces []string
	push := func(d phDesc) string {
		k := placeholderKey(len(p.phs))
		p.phs = append(p.phs, d)
		return k
	}
	i := 0
	for i < len(toks) {
		t := toks[i]
		final := false
		if t.kind == ekIdent {
			up := strings.ToUpper(t.val)
			// The FINAL/RUNNING prefix modifies immediately after navigation/aggregation (only affecting aggregation and FIRST/LAST).
			if (up == "FINAL" || up == "RUNNING") && i+2 < len(toks) &&
				toks[i+1].kind == ekIdent && navFuncs[strings.ToUpper(toks[i+1].val)] &&
				toks[i+2].kind == ekLParen {
				final = (up == "FINAL")
				i++ // Consume prefixes and move them down to the function token name
				t = toks[i]
				up = strings.ToUpper(t.val)
			}
			// Function calls: PREV(...) / SUM(...), etc.
			if navFuncs[up] && i+1 < len(toks) && toks[i+1].kind == ekLParen {
				args, consumed, err := readCallArgs(toks, i+1)
				if err != nil {
					return nil, err
				}
				pieces = append(pieces, push(phDesc{kind: phNav, name: up, args: args, final: final}))
				p.needsHist = true
				i += 1 + consumed
				continue
			}
			// SYMBOL.field: SYMBOL is a declared mode variable followed by a.field.
			// field can be an identifier or a reserved column name wrapped in backquotes/quotes.
			if symbols[t.val] && i+2 < len(toks) && toks[i+1].kind == ekDot &&
				(toks[i+2].kind == ekIdent || toks[i+2].kind == ekString) {
				fld := fieldName(toks[i+2].val)
				pieces = append(pieces, push(phDesc{kind: phSym, name: t.val, field: fld}))
				p.needsHist = true
				i += 3
				continue
			}
		}
		// The rest is passed as is (bare fields, operators, scalar functions, literals).
		pieces = append(pieces, t.val)
		if t.kind == ekIdent {
			p.bareFields = append(p.bareFields, t.val)
		}
		i++
	}
	joined := joinTokens(pieces)
	if joined == "" {
		return &preparedExpr{src: src, compiled: nil}, nil // Null expression (undefined symbol)
	}
	compiled, err := expr.NewExpression(joined)
	if err != nil {
		return nil, err
	}
	p.compiled = compiled
	return p, nil
}

// evalPrepared evaluates the precompiled expression using ctx: recalculates placeholder values + injects bare fields, and reuses the compiled product.
// baseMapPool reuses evalPrepared's base map: DEFINE/MEASURES, and builds a base (main allocation source for hot paths) after each evaluation.
// EvaluateValueWithNull synchronously evaluates and returns scalar/field references without holding base; Put can be reused by clearing before returning to the pool.
var baseMapPool = sync.Pool{
	New: func() any { return make(map[string]any, 8) },
}

func evalPrepared(p *preparedExpr, ctx *matchCtx) (any, bool, error) {
	if p == nil || p.compiled == nil {
		return nil, true, nil // Null expressions: → NULL (caller is treated as inherent to undefined symbols)
	}
	base := baseMapPool.Get().(map[string]any)
	for k := range base {
		delete(base, k)
	}
	for i, d := range p.phs {
		base[placeholderKey(i)] = evalDesc(d, ctx)
	}
	if row := currentRow(ctx); row != nil {
		for _, f := range p.bareFields {
			if _, used := base[f]; !used {
				if v, ok := row[f]; ok {
					base[f] = v
				}
			}
		}
	}
	v, isNull, err := p.compiled.EvaluateValueWithNull(base)
	baseMapPool.Put(base)
	return v, isNull, err
}

// evalDesc evaluates a placeholder description.
func evalDesc(d phDesc, ctx *matchCtx) any {
	switch d.kind {
	case phNav:
		v, _ := evalNav(d.name, d.args, ctx, d.final)
		return v
	case phSym:
		return resolveSymbolField(ctx, d.name, d.field)
	}
	return nil
}

func currentRow(ctx *matchCtx) map[string]any {
	if ctx.candidate != nil {
		return ctx.candidate
	}
	if ctx.cur >= 0 && ctx.cur < len(ctx.rows) {
		return ctx.rows[ctx.cur]
	}
	return nil
}

// readCallArgs reads the trimmed ekRParen from toks[at] (which should be ekLParen), returns the parentheses with a top comma
// The parameter fragments to be split (each segment is a set of tokens, concatenated as a string) and the number of tokens consumed (including parentheses).
func readCallArgs(toks []etoken, at int) ([]string, int, error) {
	if at >= len(toks) || toks[at].kind != ekLParen {
		return nil, 0, fmt.Errorf("expected '(' after CEP function")
	}
	depth := 0
	var segs []string
	var cur []string
	for j := at; j < len(toks); j++ {
		t := toks[j]
		switch t.kind {
		case ekLParen:
			depth++
		case ekRParen:
			depth--
			if depth == 0 {
				if len(cur) > 0 || len(segs) > 0 {
					segs = append(segs, joinTokens(cur))
				}
				return segs, j - at + 1, nil
			}
		case ekComma:
			if depth == 1 {
				segs = append(segs, joinTokens(cur))
				cur = nil
				continue
			}
		}
		if t.kind == ekLParen && j == at {
			continue
		}
		cur = append(cur, t.val)
	}
	return nil, 0, fmt.Errorf("unterminated '(' in CEP function call")
}

func joinTokens(parts []string) string {
	var sb strings.Builder
	for i, p := range parts {
		if i > 0 {
			// Spaces are needed between identifiers/numbers to avoid sticking them into new tokens; Operators and dots are not added.
			if needSpace(parts[i-1], p) {
				sb.WriteByte(' ')
			}
		}
		sb.WriteString(p)
	}
	return sb.String()
}

func needSpace(a, b string) bool {
	return isAlphaNumLike(a) && isAlphaNumLike(b)
}

func isAlphaNumLike(s string) bool {
	if s == "" {
		return false
	}
	c := s[0]
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// evalNav evaluates a navigation/aggregation call. final only affects aggregation and FIRST/LAST (FINAL = whole segment match).
// args are the top-level parameter fragments inside parentheses (parentheses removed, outer commas removed).
func evalNav(name string, args []string, ctx *matchCtx, final bool) (any, error) {
	switch name {
	case "CLASSIFIER":
		if ctx.candidate != nil {
			return ctx.candLabel, nil
		}
		if ctx.cur >= 0 && ctx.cur < len(ctx.labels) {
			return ctx.labels[ctx.cur], nil
		}
		return nil, nil
	case "MATCH_NUMBER":
		return ctx.matchNumber, nil
	case "PREV":
		return positionalField(ctx, args, -1), nil
	case "NEXT":
		return positionalField(ctx, args, +1), nil
	case "FIRST":
		return fromEndField(ctx, args, true, final), nil
	case "LAST":
		return fromEndField(ctx, args, false, final), nil
	case "SUM", "AVG", "COUNT", "MIN", "MAX":
		return aggregate(name, args, ctx, final), nil
	}
	return nil, fmt.Errorf("unsupported CEP function %s", name)
}

// posIndex returns the current row index: DEFINE=len(rows)(candidate position), MEASURES=cur.
func posIndex(ctx *matchCtx) int {
	if ctx.candidate != nil {
		return len(ctx.rows)
	}
	return ctx.cur
}

// stripQuotes: Remove backquotes, single quotes, and double quotes from the beginning and end.
func stripQuotes(a string) string {
	a = strings.TrimSpace(a)
	a = strings.Trim(a, "`")
	if len(a) >= 2 && (a[0] == '\'' && a[len(a)-1] == '\'' || a[0] == '"' && a[len(a)-1] == '"') {
		return a[1 : len(a)-1]
	}
	return a
}

// fieldName Extracts field names from parameter fragments (remove SYMBOL. prefix, dequotation/backquotation). The '*' of COUNT(*) returns as is.
func fieldName(arg string) string {
	a := stripQuotes(arg)
	if dot := strings.LastIndex(a, "."); dot >= 0 {
		a = a[dot+1:]
	}
	return a
}

// fieldAndSymbol splits the aggregate parameter as (field, symbol).
// "A.v" → ("v","A");  "v" → ("v","");  "*" → ("*","");  Remove backquotes/quotation packets.
func fieldAndSymbol(arg string) (field, symbol string) {
	a := strings.TrimSpace(arg)
	if dot := strings.IndexByte(a, '.'); dot >= 0 {
		return stripQuotes(a[dot+1:]), stripQuotes(a[:dot])
	}
	return stripQuotes(a), ""
}

func optInt(args []string, idx, def int) int {
	if idx >= len(args) {
		return def
	}
	a := strings.TrimSpace(args[idx])
	if n, err := strconv.Atoi(a); err == nil {
		return n
	}
	return def
}

// rowsLabels returns the evaluated row set: DEFINE with candidates; MEASURES final = false is taken to the current line (RUNNING),
// final = true takes the whole segment (FINAL). aggregate is shared with fromEndField.
func rowsLabels(ctx *matchCtx, final bool) ([]map[string]any, []string) {
	rows := ctx.rows
	labels := ctx.labels
	if ctx.candidate != nil {
		cr := make([]map[string]any, 0, len(rows)+1)
		cl := make([]string, 0, len(rows)+1)
		cr = append(cr, rows...)
		cl = append(cl, labels...)
		cr = append(cr, ctx.candidate)
		cl = append(cl, ctx.candLabel)
		return cr, cl
	}
	if !final && ctx.cur >= 0 && ctx.cur < len(rows) {
		return rows[:ctx.cur+1], labels[:ctx.cur+1]
	}
	return rows, labels
}

// positionalField to get PREV/NEXT: Offset from the current position to fetch rows. Crossing boundaries to return to nil.
func positionalField(ctx *matchCtx, args []string, sign int) any {
	if len(args) == 0 {
		return nil
	}
	f := fieldName(args[0])
	idx := posIndex(ctx) + sign*optInt(args, 1, 1)
	rows := ctx.rows
	if idx < 0 || idx >= len(rows) {
		return nil
	}
	return rows[idx][f]
}

// fromEndField to find FIRST/LAST: take the nth from the end/end (n starts from 1). n<1 clamp is 1 to avoid crossing boundaries.
// When final=true, the entire segment is matched to FINAL; otherwise, it is cut to the current line (RUNNING).
func fromEndField(ctx *matchCtx, args []string, fromHead bool, final bool) any {
	if len(args) == 0 {
		return nil
	}
	f := fieldName(args[0])
	n := optInt(args, 1, 1)
	if n < 1 {
		n = 1
	}
	rows, _ := rowsLabels(ctx, final)
	if len(rows) == 0 {
		return nil
	}
	if fromHead {
		idx := n - 1
		if idx >= len(rows) {
			idx = len(rows) - 1
		}
		return rows[idx][f]
	}
	idx := len(rows) - n
	if idx < 0 {
		idx = 0
	}
	return rows[idx][f]
}

// aggregate to find the RUNNING semantics within the matching range.
// Symbol qualifiers (SUM(A.x)) aggregate only the row of the symbol label; COUNT(expr) counts non-NULL values, COUNT(*) counts the number of rows.
// Numerical conversion uses cast.ToFloat64E (supports uint/string numbers, etc.), skips non-numeric values by NULL.
// aggregate Calculate the aggregation within the matching range. final=true Matches the whole segment (FINAL); otherwise, it cuts to the current line (RUNNING).
func aggregate(name string, args []string, ctx *matchCtx, final bool) any {
	var rows []map[string]any
	var labels []string
	rows, labels = rowsLabels(ctx, final)
	f, symbol := "", ""
	if len(args) > 0 {
		f, symbol = fieldAndSymbol(args[0])
	}
	star := f == "" || f == "*"
	var vals []float64
	cntNonNull := 0
	cntRows := 0
	for i, r := range rows {
		if !labelMatches(labels[i], symbol, ctx.subsets) {
			continue // Symbol/SUBSET limit: Only the ingredient label line counts
		}
		cntRows++
		if star {
			continue
		}
		rv, has := r[f]
		if !has || rv == nil {
			continue // NULL ignores it
		}
		cntNonNull++
		if x, err := cast.ToFloat64E(rv); err == nil {
			vals = append(vals, x)
		}
	}
	switch name {
	case "COUNT":
		if star {
			return float64(cntRows)
		}
		return float64(cntNonNull)
	case "SUM":
		var s float64
		for _, v := range vals {
			s += v
		}
		return s
	case "AVG":
		if len(vals) == 0 {
			return nil
		}
		var s float64
		for _, v := range vals {
			s += v
		}
		return s / float64(len(vals))
	case "MIN":
		if len(vals) == 0 {
			return nil
		}
		m := vals[0]
		for _, v := range vals[1:] {
			if v < m {
				m = v
			}
		}
		return m
	case "MAX":
		if len(vals) == 0 {
			return nil
		}
		m := vals[0]
		for _, v := range vals[1:] {
			if v > m {
				m = v
			}
		}
		return m
	}
	return nil
}

// labelMatches reports whether lbl matches a symbol (a common symbol equivalent; symbol is a component of the SUBSET name).
// Shared by aggregate/resolveSymbolField/seqOfLabel to ensure consistency between SUBSET tags.
func labelMatches(lbl, symbol string, subsets map[string][]string) bool {
	if symbol == "" || lbl == symbol {
		return true
	}
	for _, m := range subsets[symbol] {
		if lbl == m {
			return true
		}
	}
	return false
}

// resolveSymbolField retrieves the last row of the SYMBOL (or any component of the SUBSET) in the match.
// When DEFINED, if the candidate tag belongs to that symbol/component, select the candidate line for that field.
func resolveSymbolField(ctx *matchCtx, symbol, field string) any {
	if ctx.candidate != nil && labelMatches(ctx.candLabel, symbol, ctx.subsets) {
		return ctx.candidate[field]
	}
	for i := len(ctx.labels) - 1; i >= 0; i-- {
		if labelMatches(ctx.labels[i], symbol, ctx.subsets) {
			return ctx.rows[i][field]
		}
	}
	return nil
}

// truthy SQL Boolean semantics: Numeric values are nonzero, boolean true, and non-empty strings are true.
func truthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case float64:
		return x != 0
	case int:
		return x != 0
	case int64:
		return x != 0
	case string:
		return x != ""
	}
	return v != nil
}

// EvalDefine instantly compiles and evaluates the DEFINE condition (Boolean) of the symbol. For testing/external use only;
// The engine thermal path uses Engine.evalDefine (precompiled product). buffer = matched row, candidate = row to be classified.
// Does not carry SUBSET member tables—references with SUBSET-restricted elements (such as S.v) must be supported by EvalDefineWithSubsets.
func EvalDefine(cond string, buffer []map[string]any, labels []string, candidate map[string]any, candLabel string, symbols map[string]bool) bool {
	return EvalDefineWithSubsets(cond, buffer, labels, candidate, candLabel, symbols, nil)
}

// EvalDefineWithSubsets is the same as EvalDefine, but carries a SUBSET member table and supports SUBSET-specific references.
func EvalDefineWithSubsets(cond string, buffer []map[string]any, labels []string, candidate map[string]any, candLabel string, symbols map[string]bool, subsets map[string][]string) bool {
	cond = strings.TrimSpace(cond)
	if cond == "" {
		return true // Undefined symbols are always true (SQL standard)
	}
	p, err := prepare(cond, symbols)
	if err != nil {
		return false
	}
	ctx := &matchCtx{rows: buffer, labels: labels, cur: len(buffer), candidate: candidate, candLabel: candLabel, symbols: symbols, subsets: subsets}
	v, isNull, err := evalPrepared(p, ctx)
	if err != nil || isNull || v == nil {
		return false
	}
	return truthy(v)
}

// EvalMeasure instantly compiles and evaluates the MEASURES expression (value). For testing/external use only.
// Does not carry SUBSET member tables—references with SUBSET-specific references require EvalMeasureWithSubsets.
func EvalMeasure(expression string, rows []map[string]any, labels []string, cur, matchNumber int, symbols map[string]bool) (any, bool) {
	return EvalMeasureWithSubsets(expression, rows, labels, cur, matchNumber, symbols, nil)
}

// EvalMeasureWithSubsets is the same as EvalMeasure, but carries a SUBSET member table and supports references specific to SUBSET.
func EvalMeasureWithSubsets(expression string, rows []map[string]any, labels []string, cur, matchNumber int, symbols map[string]bool, subsets map[string][]string) (any, bool) {
	p, err := prepare(expression, symbols)
	if err != nil {
		return nil, true
	}
	ctx := &matchCtx{rows: rows, labels: labels, cur: cur, symbols: symbols, subsets: subsets, matchNumber: matchNumber}
	v, isNull, err := evalPrepared(p, ctx)
	if err != nil {
		return nil, true
	}
	return v, isNull
}
