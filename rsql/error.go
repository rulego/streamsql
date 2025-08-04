package rsql

import (
	"fmt"
	"strings"
)

// ErrorType defines error types
type ErrorType int

const (
	ErrorTypeSyntax ErrorType = iota
	ErrorTypeLexical
	ErrorTypeSemantics
	ErrorTypeUnexpectedToken
	ErrorTypeMissingToken
	ErrorTypeInvalidExpression
	ErrorTypeUnknownKeyword
	ErrorTypeInvalidNumber
	ErrorTypeUnterminatedString
	ErrorTypeMaxIterations
	ErrorTypeUnknownFunction
)

// ParseError enhanced parsing error structure
type ParseError struct {
	Type        ErrorType
	Message     string
	Position    int
	Line        int
	Column      int
	Token       string
	Expected    []string
	Suggestions []string
	Context     string
	Recoverable bool
}

// Error implements the error interface
func (e *ParseError) Error() string {
	var builder strings.Builder
	
	// Basic error information
	builder.WriteString(fmt.Sprintf("[%s] %s", e.getErrorTypeName(), e.Message))
	
	// Position information
	if e.Line > 0 && e.Column > 0 {
		builder.WriteString(fmt.Sprintf(" at line %d, column %d", e.Line, e.Column))
	} else if e.Position >= 0 {
		builder.WriteString(fmt.Sprintf(" at position %d", e.Position))
	}
	
	// Current token information
	if e.Token != "" {
		builder.WriteString(fmt.Sprintf(" (found '%s')", e.Token))
	}
	
	// Expected token
	if len(e.Expected) > 0 {
		builder.WriteString(fmt.Sprintf(", expected: %s", strings.Join(e.Expected, ", ")))
	}
	
	// Context information
	if e.Context != "" {
		builder.WriteString(fmt.Sprintf("\nContext: %s", e.Context))
	}
	
	// Suggestions
	if len(e.Suggestions) > 0 {
		builder.WriteString(fmt.Sprintf("\nSuggestions: %s", strings.Join(e.Suggestions, "; ")))
	}
	
	return builder.String()
}

// getErrorTypeName gets error type name
func (e *ParseError) getErrorTypeName() string {
	switch e.Type {
	case ErrorTypeSyntax:
		return "SYNTAX_ERROR"
	case ErrorTypeLexical:
		return "LEXICAL_ERROR"
	case ErrorTypeSemantics:
		return "SEMANTIC_ERROR"
	case ErrorTypeUnexpectedToken:
		return "UNEXPECTED_TOKEN"
	case ErrorTypeMissingToken:
		return "MISSING_TOKEN"
	case ErrorTypeInvalidExpression:
		return "INVALID_EXPRESSION"
	case ErrorTypeUnknownKeyword:
		return "UNKNOWN_KEYWORD"
	case ErrorTypeInvalidNumber:
		return "INVALID_NUMBER"
	case ErrorTypeUnterminatedString:
		return "UNTERMINATED_STRING"
	case ErrorTypeMaxIterations:
		return "MAX_ITERATIONS"
	case ErrorTypeUnknownFunction:
		return "UNKNOWN_FUNCTION"
	default:
		return "UNKNOWN_ERROR"
	}
}

// IsRecoverable checks if error is recoverable
func (e *ParseError) IsRecoverable() bool {
	return e.Recoverable
}

// ErrorRecovery error recovery strategy
type ErrorRecovery struct {
	parser *Parser
	errors []*ParseError
}

// NewErrorRecovery creates error recovery instance
func NewErrorRecovery(parser *Parser) *ErrorRecovery {
	return &ErrorRecovery{
		parser: parser,
		errors: make([]*ParseError, 0),
	}
}

// AddError adds an error
func (er *ErrorRecovery) AddError(err *ParseError) {
	er.errors = append(er.errors, err)
}

// GetErrors gets all errors
func (er *ErrorRecovery) GetErrors() []*ParseError {
	return er.errors
}

// HasErrors checks if there are errors
func (er *ErrorRecovery) HasErrors() bool {
	return len(er.errors) > 0
}

// RecoverFromError recovers from error
func (er *ErrorRecovery) RecoverFromError(errorType ErrorType) bool {
	switch errorType {
	case ErrorTypeUnexpectedToken:
		// Skip current token and continue parsing
		er.parser.lexer.NextToken()
		return true
	case ErrorTypeMissingToken:
		// Insert default token or skip
		return true
	case ErrorTypeInvalidExpression:
		// Jump to next comma or keyword
		return er.skipToNextDelimiter()
	case ErrorTypeSyntax:
		// Syntax errors also attempt recovery and continue parsing
		return true
	case ErrorTypeUnknownKeyword:
		// Unknown keyword errors also attempt recovery
		return true
	default:
		return false
	}
}

// skipToNextDelimiter jumps to next delimiter
func (er *ErrorRecovery) skipToNextDelimiter() bool {
	maxSkip := 10
	skipped := 0
	
	for skipped < maxSkip {
		tok := er.parser.lexer.NextToken()
		if tok.Type == TokenEOF {
			return false
		}
		if tok.Type == TokenComma || tok.Type == TokenFROM || 
		   tok.Type == TokenWHERE || tok.Type == TokenGROUP {
			return true
		}
		skipped++
	}
	return false
}

// CreateSyntaxError creates syntax error
func CreateSyntaxError(message string, position int, token string, expected []string) *ParseError {
	line, column := calculateLineColumn(position)
	return &ParseError{
		Type:        ErrorTypeSyntax,
		Message:     message,
		Position:    position,
		Line:        line,
		Column:      column,
		Token:       token,
		Expected:    expected,
		Suggestions: generateSuggestions(token, expected),
		Recoverable: true,
	}
}

// CreateLexicalError creates lexical error
func CreateLexicalError(message string, position int, char byte) *ParseError {
	line, column := calculateLineColumn(position)
	return &ParseError{
		Type:        ErrorTypeLexical,
		Message:     message,
		Position:    position,
		Line:        line,
		Column:      column,
		Token:       string(char),
		Suggestions: []string{"Check for invalid characters", "Ensure strings are properly closed"},
		Recoverable: false,
	}
}

// CreateLexicalErrorWithPosition creates lexical error with accurate position
func CreateLexicalErrorWithPosition(message string, position int, line int, column int, char byte) *ParseError {
	return &ParseError{
		Type:        ErrorTypeLexical,
		Message:     message,
		Position:    position,
		Line:        line,
		Column:      column,
		Token:       string(char),
		Suggestions: []string{"Check for invalid characters", "Ensure strings are properly closed"},
		Recoverable: false,
	}
}

// CreateUnexpectedTokenError creates unexpected token error
func CreateUnexpectedTokenError(found string, expected []string, position int) *ParseError {
	line, column := calculateLineColumn(position)
	return &ParseError{
		Type:        ErrorTypeUnexpectedToken,
		Message:     fmt.Sprintf("Unexpected token '%s'", found),
		Position:    position,
		Line:        line,
		Column:      column,
		Token:       found,
		Expected:    expected,
		Suggestions: generateSuggestions(found, expected),
		Recoverable: true,
	}
}

// CreateMissingTokenError creates missing token error
func CreateMissingTokenError(expected string, position int) *ParseError {
	line, column := calculateLineColumn(position)
	return &ParseError{
		Type:        ErrorTypeMissingToken,
		Message:     fmt.Sprintf("Missing required token '%s'", expected),
		Position:    position,
		Line:        line,
		Column:      column,
		Expected:    []string{expected},
		Suggestions: []string{fmt.Sprintf("Add missing '%s'", expected)},
		Recoverable: true,
	}
}

// CreateUnknownFunctionError creates unknown function error
func CreateUnknownFunctionError(functionName string, position int) *ParseError {
	line, column := calculateLineColumn(position)
	return &ParseError{
		Type:        ErrorTypeUnknownFunction,
		Message:     fmt.Sprintf("Unknown function '%s'", functionName),
		Position:    position,
		Line:        line,
		Column:      column,
		Token:       functionName,
		Suggestions: generateFunctionSuggestions(functionName),
		Recoverable: true,
	}
}

// calculateLineColumn calculates line and column numbers
// Note: This is a simplified implementation, actual line/column should be provided by lexer
func calculateLineColumn(position int) (int, int) {
	// Simplified implementation, should be calculated based on input text
	// Returns estimated value based on position
	line := position/50 + 1  // 假设平均每行50个字符
	column := position%50 + 1
	return line, column
}

// generateSuggestions generates suggestions
func generateSuggestions(found string, expected []string) []string {
	suggestions := make([]string, 0)
	
	if len(expected) > 0 {
		suggestions = append(suggestions, fmt.Sprintf("Try using '%s' instead of '%s'", expected[0], found))
	}
	
	// Generate suggestions based on common error patterns
	switch strings.ToUpper(found) {
	case "SELCT":
		suggestions = append(suggestions, "Did you mean 'SELECT'?")
	case "FORM":
		suggestions = append(suggestions, "Did you mean 'FROM'?")
	case "WHER":
		suggestions = append(suggestions, "Did you mean 'WHERE'?")
	case "GROPU":
		suggestions = append(suggestions, "Did you mean 'GROUP'?")
	case "ODER":
		suggestions = append(suggestions, "Did you mean 'ORDER'?")
	}
	
	return suggestions
}

// generateFunctionSuggestions generates function suggestions
func generateFunctionSuggestions(functionName string) []string {
	suggestions := make([]string, 0)
	
	// Generate suggestions based on common function name misspellings
	funcLower := strings.ToLower(functionName)
	switch {
	case strings.Contains(funcLower, "coun"):
		suggestions = append(suggestions, "Did you mean 'COUNT' function?")
	case strings.Contains(funcLower, "su") && strings.Contains(funcLower, "m"):
		suggestions = append(suggestions, "Did you mean 'SUM' function?")
	case strings.Contains(funcLower, "av") && strings.Contains(funcLower, "g"):
		suggestions = append(suggestions, "Did you mean 'AVG' function?")
	case strings.Contains(funcLower, "ma") && strings.Contains(funcLower, "x"):
		suggestions = append(suggestions, "Did you mean 'MAX' function?")
	case strings.Contains(funcLower, "mi") && strings.Contains(funcLower, "n"):
		suggestions = append(suggestions, "Did you mean 'MIN' function?")
	case strings.Contains(funcLower, "upp"):
		suggestions = append(suggestions, "Did you mean 'UPPER' function?")
	case strings.Contains(funcLower, "low"):
		suggestions = append(suggestions, "Did you mean 'LOWER' function?")
	case strings.Contains(funcLower, "len"):
		suggestions = append(suggestions, "Did you mean 'LENGTH' function?")
	case strings.Contains(funcLower, "sub"):
		suggestions = append(suggestions, "Did you mean 'SUBSTRING' function?")
	case strings.Contains(funcLower, "con"):
		suggestions = append(suggestions, "Did you mean 'CONCAT' function?")
	case strings.Contains(funcLower, "abs"):
		suggestions = append(suggestions, "Did you mean 'ABS' function?")
	case strings.Contains(funcLower, "sqrt"):
		suggestions = append(suggestions, "Did you mean 'SQRT' function?")
	case strings.Contains(funcLower, "round"):
		suggestions = append(suggestions, "Did you mean 'ROUND' function?")
	case strings.Contains(funcLower, "floor"):
		suggestions = append(suggestions, "Did you mean 'FLOOR' function?")
	case strings.Contains(funcLower, "ceil"):
		suggestions = append(suggestions, "Did you mean 'CEILING' function?")
	}
	
	// Generic suggestions
	suggestions = append(suggestions, "Check if the function name is spelled correctly")
	suggestions = append(suggestions, "Confirm that the function is registered or is a built-in function")
	suggestions = append(suggestions, "View the list of available functions")
	
	return suggestions
}

// FormatErrorContext formats error context
func FormatErrorContext(input string, position int, contextLength int) string {
	if position < 0 || position >= len(input) {
		return ""
	}
	
	start := position - contextLength
	if start < 0 {
		start = 0
	}
	
	end := position + contextLength
	if end > len(input) {
		end = len(input)
	}
	
	context := input[start:end]
	pointer := strings.Repeat(" ", position-start) + "^"
	
	return fmt.Sprintf("%s\n%s", context, pointer)
}