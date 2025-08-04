package rsql

import (
	"regexp"
	"strings"

	"github.com/rulego/streamsql/functions"
)

// FunctionValidator validates SQL functions in expressions
type FunctionValidator struct {
	errorRecovery *ErrorRecovery
}

// NewFunctionValidator creates a new function validator
func NewFunctionValidator(errorRecovery *ErrorRecovery) *FunctionValidator {
	return &FunctionValidator{
		errorRecovery: errorRecovery,
	}
}

// ValidateExpression validates functions within expressions
func (fv *FunctionValidator) ValidateExpression(expression string, position int) {
	functionCalls := fv.extractFunctionCalls(expression)

	for _, funcCall := range functionCalls {
		funcName := funcCall.Name

		// Check if function exists in registry
		if _, exists := functions.Get(funcName); !exists {
			// Check if it's a built-in function
			if !fv.isBuiltinFunction(funcName) {
				// Check if it's an expr-lang function
				bridge := functions.GetExprBridge()
				if !bridge.IsExprLangFunction(funcName) {
					// Create unknown function error
					err := CreateUnknownFunctionError(funcName, position+funcCall.Position)
					fv.errorRecovery.AddError(err)
				}
			}
		}
	}
}

// FunctionCall contains function call information
type FunctionCall struct {
	Name     string
	Position int
}

// extractFunctionCalls extracts function calls from expressions
func (fv *FunctionValidator) extractFunctionCalls(expression string) []FunctionCall {
	var functionCalls []FunctionCall

	// Use regex to match function call patterns: identifier(
	funcPattern := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*\(`)
	matches := funcPattern.FindAllStringSubmatchIndex(expression, -1)

	for _, match := range matches {
		// match[0] is the start position of entire match
		// match[1] is the end position of entire match
		// match[2] is the start position of first capture group (function name)
		// match[3] is the end position of first capture group (function name)
		funcName := expression[match[2]:match[3]]
		position := match[2]

		// Filter out keywords (like CASE, IF, etc.)
		if !fv.isKeyword(funcName) {
			functionCalls = append(functionCalls, FunctionCall{
				Name:     funcName,
				Position: position,
			})
		}
	}

	return functionCalls
}

// isBuiltinFunction checks if it's a built-in function
func (fv *FunctionValidator) isBuiltinFunction(funcName string) bool {
	builtinFunctions := []string{
		"abs", "sqrt", "sin", "cos", "tan", "floor", "ceil", "round",
		"log", "log10", "exp", "pow", "mod",
	}

	funcLower := strings.ToLower(funcName)
	for _, builtin := range builtinFunctions {
		if funcLower == builtin {
			return true
		}
	}
	return false
}

// isKeyword checks if it's an SQL keyword
func (fv *FunctionValidator) isKeyword(word string) bool {
	keywords := []string{
		"SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER",
		"AS", "DISTINCT", "LIMIT", "WITH", "TIMESTAMP", "TIMEUNIT",
		"TUMBLINGWINDOW", "SLIDINGWINDOW", "COUNTINGWINDOW", "SESSIONWINDOW",
		"AND", "OR", "NOT", "IN", "LIKE", "IS", "NULL", "TRUE", "FALSE",
		"BETWEEN", "IS", "NULL", "TRUE", "FALSE", "CASE", "WHEN",
		"THEN", "ELSE", "END", "IF", "CAST", "CONVERT",
	}

	wordUpper := strings.ToUpper(word)
	for _, keyword := range keywords {
		if wordUpper == keyword {
			return true
		}
	}
	return false
}
