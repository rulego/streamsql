package functions

import (
	"testing"
)

// 测试JSON函数
func TestJsonFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "to_json basic",
			funcName: "to_json",
			args:     []interface{}{map[string]interface{}{"name": "test", "value": 123}},
			expected: `{"name":"test","value":123}`,
		},
		{
			name:     "from_json basic",
			funcName: "from_json",
			args:     []interface{}{`{"name":"test","value":123}`},
			expected: map[string]interface{}{"name": "test", "value": float64(123)},
		},
		{
			name:     "json_extract basic",
			funcName: "json_extract",
			args:     []interface{}{`{"name":"test","value":123}`, "$.name"},
			expected: "test",
		},
		{
			name:     "json_valid true",
			funcName: "json_valid",
			args:     []interface{}{`{"name":"test"}`},
			expected: true,
		},
		{
			name:     "json_valid false",
			funcName: "json_valid",
			args:     []interface{}{`{"name":"test"`},
			expected: false,
		},
		{
			name:     "json_type object",
			funcName: "json_type",
			args:     []interface{}{`{"name":"test"}`},
			expected: "object",
		},
		{
			name:     "json_length array",
			funcName: "json_length",
			args:     []interface{}{`[1,2,3]`},
			expected: 3,
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

			if !tt.wantErr && !compareResults(result, tt.expected) {
				t.Errorf("Execute() = %v, want %v", result, tt.expected)
			}
		})
	}
}

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

// 测试条件函数
func TestConditionalFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
	}{
		{
			name:     "coalesce first non-null",
			funcName: "coalesce",
			args:     []interface{}{nil, "test", "other"},
			expected: "test",
		},
		{
			name:     "nullif equal",
			funcName: "nullif",
			args:     []interface{}{"test", "test"},
			expected: nil,
		},
		{
			name:     "nullif not equal",
			funcName: "nullif",
			args:     []interface{}{"test", "other"},
			expected: "test",
		},
		{
			name:     "greatest numeric",
			funcName: "greatest",
			args:     []interface{}{1, 3, 2},
			expected: 3,
		},
		{
			name:     "least numeric",
			funcName: "least",
			args:     []interface{}{3, 1, 2},
			expected: 1,
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

			if !compareResults(result, tt.expected) {
				t.Errorf("Execute() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// 辅助函数：比较结果
func compareResults(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	
	// 对于map类型的特殊处理
	if mapA, okA := a.(map[string]interface{}); okA {
		if mapB, okB := b.(map[string]interface{}); okB {
			if len(mapA) != len(mapB) {
				return false
			}
			for k, v := range mapA {
				if mapB[k] != v {
					return false
				}
			}
			return true
		}
	}
	
	return a == b
}