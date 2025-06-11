package functions

import (
	"testing"
)

// 测试条件函数
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