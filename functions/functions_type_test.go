package functions

import (
	"testing"
)

// TestTypeFunctions 测试类型检查函数的基本功能
func TestTypeFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
	}{
		{
			name:     "is_null true",
			funcName: "is_null",
			args:     []interface{}{nil},
			expected: true,
		},
		{
			name:     "is_null false",
			funcName: "is_null",
			args:     []interface{}{"test"},
			expected: false,
		},
		{
			name:     "is_not_null true",
			funcName: "is_not_null",
			args:     []interface{}{"test"},
			expected: true,
		},
		{
			name:     "is_not_null false",
			funcName: "is_not_null",
			args:     []interface{}{nil},
			expected: false,
		},
		{
			name:     "is_numeric true",
			funcName: "is_numeric",
			args:     []interface{}{123},
			expected: true,
		},
		{
			name:     "is_numeric false",
			funcName: "is_numeric",
			args:     []interface{}{"test"},
			expected: false,
		},
		{
			name:     "is_string true",
			funcName: "is_string",
			args:     []interface{}{"test"},
			expected: true,
		},
		{
			name:     "is_string false",
			funcName: "is_string",
			args:     []interface{}{123},
			expected: false,
		},
		{
			name:     "is_bool true",
			funcName: "is_bool",
			args:     []interface{}{true},
			expected: true,
		},
		{
			name:     "is_bool false",
			funcName: "is_bool",
			args:     []interface{}{"test"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.funcName)
			if !exists {
				t.Fatalf("Function %s not found", tt.funcName)
			}

			result, err := fn.Execute(&FunctionContext{}, tt.args)
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
		{
			name:     "is_not_null no args",
			function: NewIsNotNullFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "is_not_null valid args",
			function: NewIsNotNullFunction(),
			args:     []interface{}{nil},
			wantErr:  false,
		},
		{
			name:     "is_numeric no args",
			function: NewIsNumericFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "is_numeric valid args",
			function: NewIsNumericFunction(),
			args:     []interface{}{123},
			wantErr:  false,
		},
		{
			name:     "is_string no args",
			function: NewIsStringFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "is_string valid args",
			function: NewIsStringFunction(),
			args:     []interface{}{"test"},
			wantErr:  false,
		},
		{
			name:     "is_bool no args",
			function: NewIsBoolFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "is_bool valid args",
			function: NewIsBoolFunction(),
			args:     []interface{}{true},
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

// TestTypeFunctionEdgeCases 测试类型函数的边界情况
func TestTypeFunctionEdgeCases(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
