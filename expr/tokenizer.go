package expr

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType represents token type
type TokenType int

const (
	// TokenKeyword keyword token
	TokenKeyword TokenType = iota
	// TokenField field token
	TokenField
	// TokenOperator operator token
	TokenOperator
	// TokenNumber number token
	TokenNumber
	// TokenString string token
	TokenString
	// TokenLeftParen left parenthesis token
	TokenLeftParen
	// TokenRightParen right parenthesis token
	TokenRightParen
	// TokenComma comma token
	TokenComma
)

// Token represents a token
type Token struct {
	// Type token type
	Type TokenType
	// Value token value
	Value string
}

// tokenize breaks expression string into token list
// Supports numbers, identifiers, operators, parentheses, string literals, etc.
func tokenize(expr string) ([]string, error) {
	// Check empty expression
	if len(strings.TrimSpace(expr)) == 0 {
		return nil, fmt.Errorf("empty expression")
	}

	var tokens []string
	i := 0

	for i < len(expr) {
		// Skip whitespace characters
		if unicode.IsSpace(rune(expr[i])) {
			i++
			continue
		}

		// Handle string literals
		if expr[i] == '\'' || expr[i] == '"' {
			quote := expr[i]
			start := i
			i++ // Skip opening quote

			// Find closing quote
			for i < len(expr) && expr[i] != quote {
				if expr[i] == '\\' && i+1 < len(expr) {
					i += 2 // Skip escape character
				} else {
					i++
				}
			}

			if i >= len(expr) {
				return nil, fmt.Errorf("unterminated string literal")
			}

			i++ // Skip closing quote
			tokens = append(tokens, expr[start:i])
			continue
		}

		// Handle backtick identifiers
		if expr[i] == '`' {
			start := i
			i++ // Skip opening backtick

			// Find closing backtick
			for i < len(expr) && expr[i] != '`' {
				i++
			}

			if i >= len(expr) {
				return nil, fmt.Errorf("unterminated backtick identifier")
			}

			i++ // Skip closing backtick
			tokens = append(tokens, expr[start:i])
			continue
		}

		// Handle numbers (including negative numbers and numbers starting with decimal point)
		// Note: Numbers starting with decimal point are only valid when not preceded by digit character
		if isDigit(expr[i]) || (expr[i] == '-' && i+1 < len(expr) && isDigit(expr[i+1])) || (expr[i] == '.' && i+1 < len(expr) && isDigit(expr[i+1]) && (i == 0 || (!isDigit(expr[i-1]) && expr[i-1] != '.'))) {
			start := i
			if expr[i] == '-' {
				i++ // Skip negative sign
			}

			// Read integer part
			for i < len(expr) && isDigit(expr[i]) {
				i++
			}

			// Handle decimal point (only one decimal point allowed)
			hasDecimal := false
			if i < len(expr) && expr[i] == '.' {
				// Check if there's already a decimal point or next character is not a digit
				if !hasDecimal && i+1 < len(expr) && isDigit(expr[i+1]) {
					hasDecimal = true
					i++
					// Read decimal part
					for i < len(expr) && isDigit(expr[i]) {
						i++
					}
				}
			}

			// Handle scientific notation
			if i < len(expr) && (expr[i] == 'e' || expr[i] == 'E') {
				i++
				if i < len(expr) && (expr[i] == '+' || expr[i] == '-') {
					i++
				}
				for i < len(expr) && isDigit(expr[i]) {
					i++
				}
			}

			tokens = append(tokens, expr[start:i])
			continue
		}

		// Handle multi-character operators
		if i+1 < len(expr) {
			twoChar := expr[i : i+2]
			if isOperator(twoChar) {
				tokens = append(tokens, twoChar)
				i += 2
				continue
			}
		}

		// Handle single-character operators and parentheses (including standalone decimal point)
		if isOperator(string(expr[i])) || expr[i] == '(' || expr[i] == ')' || expr[i] == ',' || expr[i] == '.' {
			tokens = append(tokens, string(expr[i]))
			i++
			continue
		}

		// Handle identifiers and keywords
		if isLetter(expr[i]) || expr[i] == '_' || expr[i] == '$' {
			start := i
			for i < len(expr) && (isLetter(expr[i]) || isDigit(expr[i]) || expr[i] == '_' || expr[i] == '.' || expr[i] == '$') {
				i++
			}
			tokens = append(tokens, expr[start:i])
			continue
		}

		// Unknown character
		return nil, fmt.Errorf("unexpected character '%c' at position %d", expr[i], i)
	}

	return tokens, nil
}

// isDigit checks if character is a digit
func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// isLetter checks if character is a letter
func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

// isNumber checks if string is a number
func isNumber(s string) bool {
	if len(s) == 0 {
		return false
	}

	i := 0
	// Handle negative sign
	if s[0] == '-' {
		i = 1
		if len(s) == 1 {
			return false
		}
	}

	hasDigit := false
	hasDot := false

	for i < len(s) {
		if isDigit(s[i]) {
			hasDigit = true
		} else if s[i] == '.' && !hasDot {
			hasDot = true
		} else if s[i] == 'e' || s[i] == 'E' {
			// Scientific notation
			i++
			if i < len(s) && (s[i] == '+' || s[i] == '-') {
				i++
			}
			for i < len(s) && isDigit(s[i]) {
				i++
			}
			break
		} else {
			return false
		}
		i++
	}

	return hasDigit
}

// isIdentifier checks if string is a valid identifier
func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	// First character must be letter or underscore
	if !isLetter(s[0]) && s[0] != '_' {
		return false
	}

	// Remaining characters can be letters, digits, or underscores
	for i := 1; i < len(s); i++ {
		if !isLetter(s[i]) && !isDigit(s[i]) && s[i] != '_' {
			return false
		}
	}

	return true
}

// isOperator checks if string is an operator
func isOperator(s string) bool {
	operators := []string{
		"+", "-", "*", "/", "%", "^",
		"=", "==", "!=", "<>", ">", "<", ">=", "<=",
		"AND", "OR", "NOT", "LIKE", "IS",
	}

	for _, op := range operators {
		if strings.EqualFold(s, op) {
			return true
		}
	}

	return false
}

// isComparisonOperator checks if it's a comparison operator
func isComparisonOperator(op string) bool {
	comparisonOps := []string{"==", "=", "!=", "<>", ">", "<", ">=", "<=", "LIKE", "IS"}
	for _, compOp := range comparisonOps {
		if strings.EqualFold(op, compOp) {
			return true
		}
	}
	return false
}

// isStringLiteral checks if it's a string literal
func isStringLiteral(s string) bool {
	return len(s) >= 2 && ((s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"'))
}
