package condition

import (
	"fmt"
	"reflect"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

type Condition interface {
	Evaluate(env interface{}) bool
}

type ExprCondition struct {
	program *vm.Program
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
	return &ExprCondition{program: program}, nil
}

func (ec *ExprCondition) Evaluate(env interface{}) bool {
	result, err := expr.Run(ec.program, env)
	if err != nil {
		return false
	}
	return result.(bool)
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
func isNilValue(v interface{}) bool {
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
