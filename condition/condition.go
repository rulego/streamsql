package condition

import (
	"fmt"

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
			return params[0] == nil, nil
		}),
		expr.Function("is_not_null", func(params ...any) (any, error) {
			if len(params) != 1 {
				return false, fmt.Errorf("is_not_null function requires 1 parameter")
			}
			return params[0] != nil, nil
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

// matchesLikePattern implements LIKE pattern matching
// Supports % (matches any character sequence) and _ (matches single character)
func matchesLikePattern(text, pattern string) bool {
	return likeMatch(text, pattern, 0, 0)
}

// likeMatch recursively implements LIKE matching algorithm
func likeMatch(text, pattern string, textIndex, patternIndex int) bool {
	// If pattern has been fully matched
	if patternIndex >= len(pattern) {
		return textIndex >= len(text) // Text should also be fully matched
	}

	// If text has ended but pattern still has non-% characters, no match
	if textIndex >= len(text) {
		// Check if remaining pattern characters are all %
		for i := patternIndex; i < len(pattern); i++ {
			if pattern[i] != '%' {
				return false
			}
		}
		return true
	}

	// Process current pattern character
	patternChar := pattern[patternIndex]

	if patternChar == '%' {
		// % can match 0 or more characters
		// Try matching 0 characters (skip %)
		if likeMatch(text, pattern, textIndex, patternIndex+1) {
			return true
		}
		// Try matching 1 or more characters
		for i := textIndex; i < len(text); i++ {
			if likeMatch(text, pattern, i+1, patternIndex+1) {
				return true
			}
		}
		return false
	} else if patternChar == '_' {
		// _ matches exactly one character
		return likeMatch(text, pattern, textIndex+1, patternIndex+1)
	} else {
		// Regular characters must match exactly
		if text[textIndex] == patternChar {
			return likeMatch(text, pattern, textIndex+1, patternIndex+1)
		}
		return false
	}
}
