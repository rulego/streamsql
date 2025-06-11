package functions

import (
	"testing"
)

// 测试数组函数
func TestArrayFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
	}{
		{
			name:     "array_length basic",
			funcName: "array_length",
			args:     []interface{}{[]interface{}{1, 2, 3}},
			expected: 3,
		},
		{
			name:     "array_contains true",
			funcName: "array_contains",
			args:     []interface{}{[]interface{}{1, 2, 3}, 2},
			expected: true,
		},
		{
			name:     "array_contains false",
			funcName: "array_contains",
			args:     []interface{}{[]interface{}{1, 2, 3}, 4},
			expected: false,
		},
		{
			name:     "array_position found",
			funcName: "array_position",
			args:     []interface{}{[]interface{}{1, 2, 3}, 2},
			expected: 2,
		},
		{
			name:     "array_position not found",
			funcName: "array_position",
			args:     []interface{}{[]interface{}{1, 2, 3}, 4},
			expected: 0,
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