package condition

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

type Condition interface {
	Evaluate(env any) bool
}

type ExprCondition struct {
	program *vm.Program
	// fast is a compiled fast-path for trivial `field OP literal` comparisons.
	// compound extends it to flat `a AND/OR b AND/OR ...` of such comparisons.
	// When set and the runtime values match the assumed types, Evaluate skips
	// the expr-lang VM (and its reflect-based map field fetch). Anything the
	// fast path cannot handle falls back to expr-lang, so semantics are
	// preserved exactly.
	fast     *fastCompare
	compound *fastCompound
}

func NewExprCondition(expression string) (Condition, error) {
	// Add custom string function support (startsWith, endsWith, contains are built-in operators)
	options := []expr.Option{
		expr.Function("like_match", func(params ...any) (any, error) {
			if len(params) != 2 {
				return false, fmt.Errorf("like_match function requires 2 parameters")
			}
			text, ok1 := params[0].(string)
			pattern, ok2 := params[1].(string)
			if !ok1 || !ok2 {
				return false, fmt.Errorf("like_match function requires string parameters")
			}
			return matchesLikePattern(text, pattern), nil
		}),
		expr.Function("is_null", func(params ...any) (any, error) {
			if len(params) != 1 {
				return false, fmt.Errorf("is_null function requires 1 parameter")
			}
			return isNilValue(params[0]), nil
		}),
		expr.Function("is_not_null", func(params ...any) (any, error) {
			if len(params) != 1 {
				return false, fmt.Errorf("is_not_null function requires 1 parameter")
			}
			return !isNilValue(params[0]), nil
		}),
		expr.AllowUndefinedVariables(),
		expr.AsBool(),
	}

	program, err := expr.Compile(expression, options...)
	if err != nil {
		return nil, err
	}
	ec := &ExprCondition{program: program}
	if fc := tryFastCompound(expression); fc != nil {
		ec.compound = fc
	} else if fc := tryFastCompare(expression); fc != nil {
		ec.fast = fc
	}
	return ec, nil
}

func (ec *ExprCondition) Evaluate(env any) bool {
	if ec.compound != nil {
		if r, ok := ec.compound.eval(env); ok {
			return r
		}
	} else if ec.fast != nil {
		if r, ok := ec.fast.eval(env); ok {
			return r
		}
	}
	result, err := expr.Run(ec.program, env)
	if err != nil {
		return false
	}
	return result.(bool)
}

// fastCompare is a compiled fast-path for a single `field OP literal`
// comparison. It only fires when the field value's type matches the literal's
// kind (numeric vs string); otherwise the caller falls back to expr-lang.
type fastCompare struct {
	field    string
	op       string
	numLit   float64
	strLit   string
	isString bool
}

var (
	fastFieldOpNum = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|!=|<>|==|=|>|<)\s*(-?\d+(?:\.\d+)?)\s*$`)
	fastFieldOpStr = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|!=|<>|==|=|>|<)\s*'([^']*)'\s*$`)
)

// tryFastCompare returns a fast-path for trivial comparisons, or nil if the
// expression is not a simple `field OP literal` form.
func tryFastCompare(expression string) *fastCompare {
	if m := fastFieldOpNum.FindStringSubmatch(expression); m != nil {
		n, err := strconv.ParseFloat(m[3], 64)
		if err != nil {
			return nil
		}
		return &fastCompare{field: m[1], op: m[2], numLit: n}
	}
	if m := fastFieldOpStr.FindStringSubmatch(expression); m != nil {
		return &fastCompare{field: m[1], op: m[2], strLit: m[3], isString: true}
	}
	return nil
}

// eval evaluates the fast-path. The bool result is valid only when ok is true;
// ok==false means "could not handle this value, fall back to expr-lang".
func (fc *fastCompare) eval(env any) (bool, bool) {
	data, ok := env.(map[string]any)
	if !ok {
		return false, false
	}
	v, exists := data[fc.field]
	if !exists || v == nil {
		return false, false
	}
	if fc.isString {
		s, ok := v.(string)
		if !ok {
			return false, false
		}
		return compareStr(s, fc.op, fc.strLit), true
	}
	f, ok := toFloat64Fast(v)
	if !ok {
		return false, false
	}
	return compareNum(f, fc.op, fc.numLit), true
}

// fastCompound is a flat AND/OR chain of simple comparisons. It fires only when
// every part fast-evaluates; if any part cannot (type mismatch/missing), the
// whole condition falls back to expr-lang.
type fastCompound struct {
	op    string // "AND" or "OR"
	parts []*fastCompare
}

func (fc *fastCompound) eval(env any) (bool, bool) {
	data, ok := env.(map[string]any)
	if !ok {
		return false, false
	}
	result := fc.op == "AND" // AND starts true, OR starts false
	for _, p := range fc.parts {
		r, ok := p.evalMap(data)
		if !ok {
			return false, false
		}
		if fc.op == "AND" {
			result = result && r
		} else {
			result = result || r
		}
	}
	return result, true
}

// evalMap is like eval but assumes env is already a map (used by fastCompound).
func (fc *fastCompare) evalMap(data map[string]any) (bool, bool) {
	v, exists := data[fc.field]
	if !exists || v == nil {
		return false, false
	}
	if fc.isString {
		s, ok := v.(string)
		if !ok {
			return false, false
		}
		return compareStr(s, fc.op, fc.strLit), true
	}
	f, ok := toFloat64Fast(v)
	if !ok {
		return false, false
	}
	return compareNum(f, fc.op, fc.numLit), true
}

// fastAndOr splits a flat condition on its sole logical operator (all && or
// all ||). The rsql parser lowers SQL AND/OR to &&/|| before reaching here, so
// we split on the C-style operators.
var fastAndOr = regexp.MustCompile(`\s*(&&|\|\|)\s*`)

func tryFastCompound(expression string) *fastCompound {
	// Skip grouping (a flat split cannot honor parentheses). Quoted string
	// literals are fine: if a literal contained &&/|| and got mis-split, each
	// resulting part would fail the strict per-part regex below and we bail.
	if strings.ContainsAny(expression, "()") {
		return nil
	}
	hasAnd := strings.Contains(expression, "&&")
	hasOr := strings.Contains(expression, "||")
	var op string
	if hasAnd && hasOr {
		return nil // mixed logic — fall back
	} else if hasAnd {
		op = "AND"
	} else if hasOr {
		op = "OR"
	} else {
		return nil // single comparison handled by tryFastCompare
	}
	parts := fastAndOr.Split(expression, -1)
	if len(parts) < 2 {
		return nil
	}
	compares := make([]*fastCompare, 0, len(parts))
	for _, p := range parts {
		fc := tryFastCompare(p)
		if fc == nil {
			return nil
		}
		compares = append(compares, fc)
	}
	return &fastCompound{op: op, parts: compares}
}

func toFloat64Fast(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint64:
		return float64(x), true
	case uint32:
		return float64(x), true
	}
	return 0, false
}

func compareNum(a float64, op string, b float64) bool {
	switch op {
	case ">":
		return a > b
	case ">=":
		return a >= b
	case "<":
		return a < b
	case "<=":
		return a <= b
	case "=", "==":
		return a == b
	case "!=", "<>":
		return a != b
	}
	return false
}

func compareStr(a, op, b string) bool {
	switch op {
	case "=", "==":
		return a == b
	case "!=", "<>":
		return a != b
	case ">":
		return a > b
	case ">=":
		return a >= b
	case "<":
		return a < b
	case "<=":
		return a <= b
	}
	return false
}

// matchesLikePattern implements LIKE pattern matching.
// Supports % (matches any character sequence) and _ (matches a single character).
// Uses the classic two-pointer backtracking algorithm: O(n*m) worst case, with no
// exponential blow-up on adversarial patterns (unlike a naive per-'%' recursion).
func matchesLikePattern(text, pattern string) bool {
	ti, pi := 0, 0
	starIdx, matchIdx := -1, 0 // last '%' index in pattern; text index when we took it
	for ti < len(text) {
		if pi < len(pattern) && (pattern[pi] == '_' || pattern[pi] == text[ti]) {
			ti++
			pi++
		} else if pi < len(pattern) && pattern[pi] == '%' {
			starIdx = pi
			matchIdx = ti
			pi++
		} else if starIdx != -1 {
			// backtrack: let the last '%' consume one more character
			pi = starIdx + 1
			matchIdx++
			ti = matchIdx
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}
	return pi == len(pattern)
}

// isNilValue reports whether v is nil, including typed-nil values (e.g.
// (*int)(nil)) which compare != nil under Go's == operator but should be
// treated as NULL by is_null/is_not_null.
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return rv.IsNil()
	}
	return false
}
