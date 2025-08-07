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
