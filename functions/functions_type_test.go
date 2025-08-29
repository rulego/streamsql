package functions

import (
	"testing"
)

// TestTypeFunctions 测试类型函数
func TestTypeFunctions(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		expected interface{}
	}{
		{
			name:     "is_numeric with float",
			function: NewIsNumericFunction(),
			args:     []interface{}{3.14},
			expected: true,
		},
		{
			name:     "is_numeric with int64",
			function: NewIsNumericFunction(),
			args:     []interface{}{int64(123)},
			expected: true,
		},
		{
			name:     "is_numeric with float32",
			function: NewIsNumericFunction(),
			args:     []interface{}{float32(3.14)},
			expected: true,
		},
		{
			name:     "is_numeric with float64",
			function: NewIsNumericFunction(),
			args:     []interface{}{float64(3.14)},
			expected: true,
		},
		{
			name:     "is_numeric with int32",
			function: NewIsNumericFunction(),
			args:     []interface{}{int32(123)},
			expected: true,
		},
		{
			name:     "is_numeric with uint",
			function: NewIsNumericFunction(),
			args:     []interface{}{uint(123)},
			expected: true,
		},
		{
			name:     "is_numeric with uint64",
			function: NewIsNumericFunction(),
			args:     []interface{}{uint64(123)},
			expected: true,
		},
		{
			name:     "is_numeric with uint32",
			function: NewIsNumericFunction(),
			args:     []interface{}{uint32(123)},
			expected: true,
		},
		{
			name:     "is_numeric with bool",
			function: NewIsNumericFunction(),
			args:     []interface{}{true},
			expected: false,
		},
		{
			name:     "is_numeric with nil",
			function: NewIsNumericFunction(),
			args:     []interface{}{nil},
			expected: false,
		},
		{
			name:     "is_string with empty string",
			function: NewIsStringFunction(),
			args:     []interface{}{""},
			expected: true,
		},
		{
			name:     "is_bool with false",
			function: NewIsBoolFunction(),
			args:     []interface{}{false},
			expected: true,
		},
		{
			name:     "is_bool with nil",
			function: NewIsBoolFunction(),
			args:     []interface{}{nil},
			expected: false,
		},
		{
			name:     "is_array",
			function: NewIsArrayFunction(),
			args:     []interface{}{[]int{1, 2, 3}},
			expected: true,
		},
		{
			name:     "is_array with nil",
			function: NewIsArrayFunction(),
			args:     []interface{}{nil},
			expected: false,
		},
		{
			name:     "is_object",
			function: NewIsObjectFunction(),
			args:     []interface{}{map[string]int{"a": 1, "b": 2}},
			expected: true,
		},
		{
			name:     "is_object with nil",
			function: NewIsObjectFunction(),
			args:     []interface{}{nil},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 验证参数
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

// TestTypeFunctionValidation 测试类型函数的参数验证
func TestTypeFunctionValidation(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "is_null no args",
			function: NewIsNullFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "is_null too many args",
			function: NewIsNullFunction(),
			args:     []interface{}{"test", "extra"},
			wantErr:  true,
		},
		{
			name:     "is_null valid args",
			function: NewIsNullFunction(),
			args:     []interface{}{"test"},
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
