package rsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunctionValidator_ValidateExpression(t *testing.T) {
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
		{
			name:           "Function with complex arguments",
			expression:     "abs(temperature * 2 + humidity)",
			expectedErrors: 0,
		},
		{
			name:           "Keyword should not be treated as function",
			expression:     "CASE WHEN temperature > 0 THEN 1 ELSE 0 END",
			expectedErrors: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorRecovery := &ErrorRecovery{}
			validator := NewFunctionValidator(errorRecovery)

			validator.ValidateExpression(tt.expression, 0)

			errors := errorRecovery.GetErrors()
			assert.Equal(t, tt.expectedErrors, len(errors), "Expected %d errors, got %d", tt.expectedErrors, len(errors))

			if tt.expectedErrors > 0 {
				assert.Equal(t, tt.errorType, errors[0].Type, "Expected error type %v, got %v", tt.errorType, errors[0].Type)
				if tt.errorMessage != "" {
					assert.Contains(t, errors[0].Message, tt.errorMessage, "Error message should contain %s", tt.errorMessage)
				}
			}
		})
	}
}

func TestFunctionValidator_ExtractFunctionCalls(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		expected   []FunctionCall
	}{
		{
			name:       "Single function",
			expression: "abs(x)",
			expected: []FunctionCall{
				{Name: "abs", Position: 0},
			},
		},
		{
			name:       "Multiple functions",
			expression: "abs(x) + sqrt(y)",
			expected: []FunctionCall{
				{Name: "abs", Position: 0},
				{Name: "sqrt", Position: 9},
			},
		},
		{
			name:       "Nested functions",
			expression: "sqrt(abs(x))",
			expected: []FunctionCall{
				{Name: "sqrt", Position: 0},
				{Name: "abs", Position: 5},
			},
		},
		{
			name:       "Function with spaces",
			expression: "abs ( x )",
			expected: []FunctionCall{
				{Name: "abs", Position: 0},
			},
		},
		{
			name:       "No functions",
			expression: "x + y * 2",
			expected:   []FunctionCall{},
		},
		{
			name:       "Keywords should be filtered",
			expression: "CASE(x) WHEN(y)",
			expected:   []FunctionCall{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorRecovery := &ErrorRecovery{}
			validator := NewFunctionValidator(errorRecovery)

			result := validator.extractFunctionCalls(tt.expression)
			assert.Equal(t, len(tt.expected), len(result), "Expected %d function calls, got %d", len(tt.expected), len(result))

			for i, expected := range tt.expected {
				if i < len(result) {
					assert.Equal(t, expected.Name, result[i].Name, "Expected function name %s, got %s", expected.Name, result[i].Name)
					assert.Equal(t, expected.Position, result[i].Position, "Expected position %d, got %d", expected.Position, result[i].Position)
				}
			}
		})
	}
}

func TestFunctionValidator_IsBuiltinFunction(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		expected bool
	}{
		{"abs function", "abs", true},
		{"ABS function (case insensitive)", "ABS", true},
		{"sqrt function", "sqrt", true},
		{"unknown function", "unknown_func", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorRecovery := &ErrorRecovery{}
			validator := NewFunctionValidator(errorRecovery)

			result := validator.isBuiltinFunction(tt.funcName)
			assert.Equal(t, tt.expected, result, "Expected %v for function %s", tt.expected, tt.funcName)
		})
	}
}

func TestFunctionValidator_IsKeyword(t *testing.T) {
	tests := []struct {
		name     string
		word     string
		expected bool
	}{
		{"SELECT keyword", "SELECT", true},
		{"select keyword (case insensitive)", "select", true},
		{"CASE keyword", "CASE", true},
		{"regular identifier", "temperature", false},
		{"function name", "abs", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorRecovery := &ErrorRecovery{}
			validator := NewFunctionValidator(errorRecovery)

			result := validator.isKeyword(tt.word)
			assert.Equal(t, tt.expected, result, "Expected %v for word %s", tt.expected, tt.word)
		})
	}
}