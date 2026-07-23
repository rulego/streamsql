package functions

import (
	"testing"
)

// TestTypeFunctions
func TestTypeFunctions(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []any
		expected any
	}{
		{
			name:     "is_numeric with float",
			function: NewIsNumericFunction(),
			args:     []any{3.14},
			expected: true,
		},
		{
			name:     "is_numeric with int64",
			function: NewIsNumericFunction(),
			args:     []any{int64(123)},
			expected: true,
		},
		{
			name:     "is_numeric with float32",
			function: NewIsNumericFunction(),
			args:     []any{float32(3.14)},
			expected: true,
		},
		{
			name:     "is_numeric with float64",
			function: NewIsNumericFunction(),
			args:     []any{float64(3.14)},
			expected: true,
		},
		{
			name:     "is_numeric with int32",
			function: NewIsNumericFunction(),
			args:     []any{int32(123)},
			expected: true,
		},
		{
			name:     "is_numeric with uint",
			function: NewIsNumericFunction(),
			args:     []any{uint(123)},
			expected: true,
		},
		{
			name:     "is_numeric with uint64",
			function: NewIsNumericFunction(),
			args:     []any{uint64(123)},
			expected: true,
		},
		{
			name:     "is_numeric with uint32",
			function: NewIsNumericFunction(),
			args:     []any{uint32(123)},
			expected: true,
		},
		{
			name:     "is_numeric with bool",
			function: NewIsNumericFunction(),
			args:     []any{true},
			expected: false,
		},
		{
			name:     "is_numeric with nil",
			function: NewIsNumericFunction(),
			args:     []any{nil},
			expected: false,
		},
		{
			name:     "is_string with empty string",
			function: NewIsStringFunction(),
			args:     []any{""},
			expected: true,
		},
		{
			name:     "is_bool with false",
			function: NewIsBoolFunction(),
			args:     []any{false},
			expected: true,
		},
		{
			name:     "is_bool with nil",
			function: NewIsBoolFunction(),
			args:     []any{nil},
			expected: false,
		},
		{
			name:     "is_array",
			function: NewIsArrayFunction(),
			args:     []any{[]int{1, 2, 3}},
			expected: true,
		},
		{
			name:     "is_array with nil",
			function: NewIsArrayFunction(),
			args:     []any{nil},
			expected: false,
		},
		{
			name:     "is_object",
			function: NewIsObjectFunction(),
			args:     []any{map[string]int{"a": 1, "b": 2}},
			expected: true,
		},
		{
			name:     "is_object with nil",
			function: NewIsObjectFunction(),
			args:     []any{nil},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Validate parameters
			if err := tt.function.Validate(tt.args); err != nil {
				t.Errorf("Validate() error = %v", err)
				return
			}
			result, err := tt.function.Execute(&FunctionContext{}, tt.args)
			if err != nil {
				t.Errorf("Execute() error = %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("Execute() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestTypeFunctionValidation: Validation of parameters for the test-type function
func TestTypeFunctionValidation(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []any
		wantErr  bool
	}{
		{
			name:     "is_null no args",
			function: NewIsNullFunction(),
			args:     []any{},
			wantErr:  true,
		},
		{
			name:     "is_null too many args",
			function: NewIsNullFunction(),
			args:     []any{"test", "extra"},
			wantErr:  true,
		},
		{
			name:     "is_null valid args",
			function: NewIsNullFunction(),
			args:     []any{"test"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.function.Validate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
