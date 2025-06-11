package functions

import (
	"testing"
)

// 测试哈希函数
func TestHashFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
	}{
		{
			name:     "md5 basic",
			funcName: "md5",
			args:     []interface{}{"hello"},
			expected: "5d41402abc4b2a76b9719d911017c592",
		},
		{
			name:     "sha1 basic",
			funcName: "sha1",
			args:     []interface{}{"hello"},
			expected: "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
		},
		{
			name:     "sha256 basic",
			funcName: "sha256",
			args:     []interface{}{"hello"},
			expected: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
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