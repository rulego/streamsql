package functions

import (
	"testing"
)

// TestConditionalFunctions 测试条件函数的基本功能
func TestConditionalFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "if_null with null",
			funcName: "if_null",
			args:     []interface{}{nil, "default"},
			expected: "default",
		},
		{
			name:     "if_null with value",
			funcName: "if_null",
			args:     []interface{}{"value", "default"},
			expected: "value",
		},
		{
			name:     "null_if equal",
			funcName: "null_if",
			args:     []interface{}{"test", "test"},
			expected: nil,
		},
		{
			name:     "null_if not equal",
			funcName: "null_if",
			args:     []interface{}{"test", "other"},
			expected: "test",
		},
		{
			name:     "greatest basic",
			funcName: "greatest",
			args:     []interface{}{1, 3, 2},
			expected: 3,
		},
		{
			name:     "least basic",
			funcName: "least",
			args:     []interface{}{1, 3, 2},
			expected: 1,
		},

		// case_when 函数测试
		{
			name:     "case_when simple",
			funcName: "case_when",
			args:     []interface{}{true, "result1", false, "result2", "default"},
			expected: "result1",
		},
		{
			name:     "case_when second condition",
			funcName: "case_when",
			args:     []interface{}{false, "result1", true, "result2", "default"},
			expected: "result2",
		},
		{
			name:     "case_when default",
			funcName: "case_when",
			args:     []interface{}{false, "result1", false, "result2", "default"},
			expected: "default",
		},
		{
			name:     "case_when no default",
			funcName: "case_when",
			args:     []interface{}{false, "result1", false, "result2"},
			expected: nil,
		},
		{
			name:     "case_when invalid args",
			funcName: "case_when",
			args:     []interface{}{true},
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.funcName)
			if !exists {
				t.Fatalf("Function %s not found", tt.funcName)
			}

			result, err := fn.Execute(&FunctionContext{}, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expected {
				t.Errorf("Execute() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestConditionalFunctionValidation 测试条件函数的参数验证
func TestConditionalFunctionValidation(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "if_null no args",
			function: NewIfNullFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "if_null one arg",
			function: NewIfNullFunction(),
			args:     []interface{}{"test"},
			wantErr:  true,
		},
		{
			name:     "if_null valid args",
			function: NewIfNullFunction(),
			args:     []interface{}{nil, "default"},
			wantErr:  false,
		},
		{
			name:     "coalesce no args",
			function: NewCoalesceFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "coalesce valid args",
			function: NewCoalesceFunction(),
			args:     []interface{}{nil, "default"},
			wantErr:  false,
		},
		{
			name:     "null_if no args",
			function: NewNullIfFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "null_if one arg",
			function: NewNullIfFunction(),
			args:     []interface{}{"test"},
			wantErr:  true,
		},
		{
			name:     "null_if valid args",
			function: NewNullIfFunction(),
			args:     []interface{}{"test", "test"},
			wantErr:  false,
		},
		{
			name:     "greatest no args",
			function: NewGreatestFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "greatest valid args",
			function: NewGreatestFunction(),
			args:     []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "least no args",
			function: NewLeastFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "least valid args",
			function: NewLeastFunction(),
			args:     []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "case_when no args",
			function: NewCaseWhenFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "case_when one arg",
			function: NewCaseWhenFunction(),
			args:     []interface{}{true},
			wantErr:  true,
		},
		{
			name:     "case_when valid args",
			function: NewCaseWhenFunction(),
			args:     []interface{}{true, "result"},
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

// TestConditionalFunctionEdgeCases 测试条件函数的边界情况
func TestConditionalFunctionEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "coalesce all null",
			function: NewCoalesceFunction(),
			args:     []interface{}{nil, nil, nil},
			expected: nil,
			wantErr:  false,
		},
		{
			name:     "coalesce first non-null",
			function: NewCoalesceFunction(),
			args:     []interface{}{"first", nil, "third"},
			expected: "first",
			wantErr:  false,
		},
		{
			name:     "coalesce middle non-null",
			function: NewCoalesceFunction(),
			args:     []interface{}{nil, "second", "third"},
			expected: "second",
			wantErr:  false,
		},
		{
			name:     "greatest with mixed types",
			function: NewGreatestFunction(),
			args:     []interface{}{1, 3.14, 2},
			expected: 3.14,
			wantErr:  false,
		},
		{
			name:     "least with mixed types",
			function: NewLeastFunction(),
			args:     []interface{}{1, 3.14, 2},
			expected: 1,
			wantErr:  false,
		},
		{
			name:     "greatest with strings",
			function: NewGreatestFunction(),
			args:     []interface{}{"apple", "banana", "cherry"},
			expected: "cherry",
			wantErr:  false,
		},
		{
			name:     "least with strings",
			function: NewLeastFunction(),
			args:     []interface{}{"apple", "banana", "cherry"},
			expected: "apple",
			wantErr:  false,
		},
		{
			name:     "case_when with complex conditions",
			function: NewCaseWhenFunction(),
			args:     []interface{}{false, "first", false, "second", true, "third", "default"},
			expected: "third",
			wantErr:  false,
		},
		{
			name:     "null_if with different types",
			function: NewNullIfFunction(),
			args:     []interface{}{"123", 123},
			expected: "123",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.function.Execute(&FunctionContext{}, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expected {
				t.Errorf("Execute() = %v, want %v", result, tt.expected)
			}
		})
	}
}
