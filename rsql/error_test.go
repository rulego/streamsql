package rsql

import (
	"fmt"
	"strings"
	"testing"
)

// TestParseError Tests the ParseError structure
func TestParseError(t *testing.T) {
	err := &ParseError{
		Type:        ErrorTypeSyntax,
		Message:     "Invalid syntax",
		Position:    10,
		Line:        2,
		Column:      5,
		Token:       "SELECT",
		Expected:    []string{"FROM", "WHERE"},
		Suggestions: []string{"Add FROM clause", "Check syntax"},
		Context:     "SELECT statement",
		Recoverable: true,
	}

	// Test the Error() method
	errorStr := err.Error()
	if !strings.Contains(errorStr, "SYNTAX_ERROR") {
		t.Errorf("Error string should contain 'SYNTAX_ERROR', got: %s", errorStr)
	}
	if !strings.Contains(errorStr, "Invalid syntax") {
		t.Errorf("Error string should contain message, got: %s", errorStr)
	}
	if !strings.Contains(errorStr, "line 2, column 5") {
		t.Errorf("Error string should contain position info, got: %s", errorStr)
	}
	if !strings.Contains(errorStr, "found 'SELECT'") {
		t.Errorf("Error string should contain token info, got: %s", errorStr)
	}
	if !strings.Contains(errorStr, "expected: FROM, WHERE") {
		t.Errorf("Error string should contain expected tokens, got: %s", errorStr)
	}
	if !strings.Contains(errorStr, "Context: SELECT statement") {
		t.Errorf("Error string should contain context, got: %s", errorStr)
	}
	if !strings.Contains(errorStr, "Suggestions: Add FROM clause; Check syntax") {
		t.Errorf("Error string should contain suggestions, got: %s", errorStr)
	}

	// Test the IsRecoverable() method
	if !err.IsRecoverable() {
		t.Error("Error should be recoverable")
	}
}

// TestEnhancedErrorHandling tests enhanced error handling
func TestEnhancedErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedErrors int
		errorType      ErrorType
		contains       string
		recoverable    bool
	}{
		{
			name:           "Missing FROM keyword",
			input:          "SELECT * table1",
			expectedErrors: 1,
			errorType:      ErrorTypeUnexpectedToken,
			contains:       "Expected source identifier after FROM",
			recoverable:    true,
		},
		{
			name:           "Typo in SELECT",
			input:          "SELCT * FROM table1",
			expectedErrors: 1,
			errorType:      ErrorTypeUnknownKeyword,
			contains:       "Unknown keyword 'SELCT'",
			recoverable:    true,
		},
		{
			name:           "Invalid character",
			input:          "SELECT * FROM table1 WHERE id # 5",
			expectedErrors: 1,
			errorType:      ErrorTypeLexical,
			contains:       "Unexpected character",
			recoverable:    false,
		},
		{
			name:           "Unterminated string",
			input:          "SELECT * FROM table1 WHERE name = 'test",
			expectedErrors: 1,
			errorType:      ErrorTypeUnterminatedString,
			contains:       "Unterminated string literal",
			recoverable:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.input)
			_, err := parser.Parse()

			// There should be errors
			if err == nil && !parser.HasErrors() {
				t.Errorf("Expected error but got none")
				return
			}

			// Check the number of errors
			if test.expectedErrors > 0 {
				errors := parser.GetErrors()
				if len(errors) != test.expectedErrors {
					t.Errorf("Expected %d errors, got %d", test.expectedErrors, len(errors))
				}
			}

			// Check for errors
			if test.contains != "" {
				errorFound := false
				for _, parseErr := range parser.GetErrors() {
					if strings.Contains(parseErr.Message, test.contains) {
						errorFound = true
						break
					}
				}
				if !errorFound {
					t.Errorf("Expected error containing '%s'", test.contains)
				}
			}
		})
	}
}

// TestErrorTypes Test error types
func TestErrorTypes(t *testing.T) {
	errorTypes := []ErrorType{
		ErrorTypeSyntax,
		ErrorTypeLexical,
		ErrorTypeSemantics,
		ErrorTypeUnexpectedToken,
		ErrorTypeMissingToken,
		ErrorTypeInvalidExpression,
		ErrorTypeUnknownKeyword,
		ErrorTypeInvalidNumber,
		ErrorTypeUnterminatedString,
		ErrorTypeMaxIterations,
		ErrorTypeUnknownFunction,
	}

	for _, errorType := range errorTypes {
		t.Run(fmt.Sprintf("ErrorType_%d", int(errorType)), func(t *testing.T) {
			err := &ParseError{
				Type:    errorType,
				Message: "Test error",
			}
			errorStr := err.Error()
			if errorStr == "" {
				t.Error("Error string should not be empty")
			}
		})
	}
}

// TestErrorRecovery: Test error recovery mechanism
func TestErrorRecovery(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		errorCount  int
	}{
		{
			name:        "Multiple syntax errors",
			input:       "SELCT * FORM table WHRE id = 1",
			expectError: true,
			errorCount:  3, // SELCT, FORM, WHRE
		},
		{
			name:        "Missing tokens",
			input:       "SELECT FROM WHERE",
			expectError: true,
			errorCount:  1,
		},
		{
			name:        "Incomplete WHERE clause",
			input:       "SELECT * FROM table WHERE (",
			expectError: false,
			errorCount:  0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.input)
			_, err := parser.Parse()

			if test.expectError {
				if err == nil && !parser.HasErrors() {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil || parser.HasErrors() {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// TestNewFunctionValidator Test FunctionValidator created
func TestNewFunctionValidator(t *testing.T) {
	lexer := NewLexer("SELECT * FROM table")
	parser := &Parser{lexer: lexer}
	er := NewErrorRecovery(parser)
	fv := NewFunctionValidator(er)

	if fv == nil {
		t.Error("NewFunctionValidator should not return nil")
		return
	}

	if fv.errorRecovery != er {
		t.Error("FunctionValidator should store the provided ErrorRecovery")
	}
}

// TestFunctionValidatorValidateExpression Validates the expression of the test function validator
func TestFunctionValidatorValidateExpression(t *testing.T) {
	tests := []struct {
		name           string
		expression     string
		expectedErrors int
		errorType      ErrorType
		errorMessage   string
	}{
		{
			name:           "Valid builtin function",
			expression:     "abs(temperature)",
			expectedErrors: 0,
		},
		{
			name:           "Valid nested builtin functions",
			expression:     "sqrt(abs(temperature))",
			expectedErrors: 0,
		},
		{
			name:           "Unknown function",
			expression:     "unknown_func(temperature)",
			expectedErrors: 1,
			errorType:      ErrorTypeUnknownFunction,
			errorMessage:   "unknown_func",
		},
		{
			name:           "Multiple unknown functions",
			expression:     "unknown1(temperature) + unknown2(humidity)",
			expectedErrors: 2,
			errorType:      ErrorTypeUnknownFunction,
		},
		{
			name:           "Mixed valid and invalid functions",
			expression:     "abs(temperature) + unknown_func(humidity)",
			expectedErrors: 1,
			errorType:      ErrorTypeUnknownFunction,
			errorMessage:   "unknown_func",
		},
		{
			name:           "No functions in expression",
			expression:     "temperature + humidity",
			expectedErrors: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer("SELECT * FROM table")
			parser := &Parser{lexer: lexer}
			er := NewErrorRecovery(parser)
			fv := NewFunctionValidator(er)

			fv.ValidateExpression(test.expression, 0)

			errors := er.GetErrors()
			if len(errors) != test.expectedErrors {
				t.Errorf("Expected %d errors, got %d", test.expectedErrors, len(errors))
				return
			}

			if test.expectedErrors > 0 {
				if errors[0].Type != test.errorType {
					t.Errorf("Expected error type %v, got %v", test.errorType, errors[0].Type)
				}

				if test.errorMessage != "" && !strings.Contains(errors[0].Message, test.errorMessage) {
					t.Errorf("Expected error message to contain '%s', got '%s'", test.errorMessage, errors[0].Message)
				}
			}
		})
	}
}

// TestFunctionValidatorBuiltins Test function Verifier built-in function
func TestFunctionValidatorBuiltins(t *testing.T) {
	lexer := NewLexer("SELECT * FROM table")
	parser := &Parser{lexer: lexer}
	er := NewErrorRecovery(parser)
	validator := NewFunctionValidator(er)

	// Testing built-in function validation (mathematical functions based on actual implementation)
	builtinFunctions := []string{"ABS", "ROUND", "SQRT", "SIN", "COS", "FLOOR", "CEIL"}
	for _, funcName := range builtinFunctions {
		t.Run("Builtin_"+funcName, func(t *testing.T) {
			if !validator.isBuiltinFunction(funcName) {
				t.Errorf("Expected %s to be a valid builtin function", funcName)
			}
		})
	}

	// Test aggregate functions (these are valid in function registration systems)
	aggregateFunctions := []string{"COUNT", "SUM", "AVG", "MAX", "MIN"}
	for _, funcName := range aggregateFunctions {
		t.Run("Aggregate_"+funcName, func(t *testing.T) {
			// Aggregate functions should exist in the function registration system
			if !validator.isBuiltinFunction(funcName) {
				t.Errorf("Expected %s to be a valid function (it's registered in the function registry)", funcName)
			}
		})
	}

	// Test the invalid function
	invalidFunctions := []string{"INVALID_FUNC", "UNKNOWN", ""}
	for _, funcName := range invalidFunctions {
		t.Run("Invalid_"+funcName, func(t *testing.T) {
			if validator.isBuiltinFunction(funcName) {
				t.Errorf("Expected %s to be an invalid function", funcName)
			}
		})
	}
}
