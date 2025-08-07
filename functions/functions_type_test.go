package functions

import (
	"testing"
)

// 测试类型检查函数
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
