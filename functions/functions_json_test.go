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
			name:     "to_json array",
			funcName: "to_json",
			args:     []interface{}{[]interface{}{1, 2, 3}},
			expected: `[1,2,3]`,
		},
		{
			name:     "to_json string",
			funcName: "to_json",
			args:     []interface{}{"hello"},
			expected: `"hello"`,
		},
		{
			name:     "from_json basic",
			funcName: "from_json",
			args:     []interface{}{`{"name":"test","value":123}`},
			expected: map[string]interface{}{"name": "test", "value": float64(123)},
		},
		{
			name:     "from_json array",
			funcName: "from_json",
			args:     []interface{}{`[1,2,3]`},
			expected: []interface{}{float64(1), float64(2), float64(3)},
		},
		{
			name:     "from_json invalid",
			funcName: "from_json",
			args:     []interface{}{`{"name":"test"`},
			wantErr:  true,
		},
		{
			name:     "from_json non-string",
			funcName: "from_json",
			args:     []interface{}{123},
			wantErr:  true,
		},
		{
			name:     "json_extract basic",
			funcName: "json_extract",
			args:     []interface{}{`{"name":"test","value":123}`, "$.name"},
			expected: "test",
		},
		{
			name:     "json_extract number",
			funcName: "json_extract",
			args:     []interface{}{`{"name":"test","value":123}`, "$.value"},
			expected: float64(123),
		},
		{
			name:     "json_extract invalid json",
			funcName: "json_extract",
			args:     []interface{}{`{"name":"test"`, "$.name"},
			wantErr:  true,
		},
		{
			name:     "json_extract non-string json",
			funcName: "json_extract",
			args:     []interface{}{123, "$.name"},
			wantErr:  true,
		},
		{
			name:     "json_extract non-string path",
			funcName: "json_extract",
			args:     []interface{}{`{"name":"test"}`, 123},
			wantErr:  true,
		},
		{
			name:     "json_extract invalid path",
			funcName: "json_extract",
			args:     []interface{}{`{"name":"test"}`, "invalid_path"},
			wantErr:  true,
		},
		{
			name:     "json_extract non-object",
			funcName: "json_extract",
			args:     []interface{}{`[1,2,3]`, "$.name"},
			wantErr:  true,
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
			name:     "json_valid non-string",
			funcName: "json_valid",
			args:     []interface{}{123},
			expected: false,
		},
		{
			name:     "json_type object",
			funcName: "json_type",
			args:     []interface{}{`{"name":"test"}`},
			expected: "object",
		},
		{
			name:     "json_type array",
			funcName: "json_type",
			args:     []interface{}{`[1,2,3]`},
			expected: "array",
		},
		{
			name:     "json_type string",
			funcName: "json_type",
			args:     []interface{}{`"hello"`},
			expected: "string",
		},
		{
			name:     "json_type number",
			funcName: "json_type",
			args:     []interface{}{`123`},
			expected: "number",
		},
		{
			name:     "json_type boolean",
			funcName: "json_type",
			args:     []interface{}{`true`},
			expected: "boolean",
		},
		{
			name:     "json_type null",
			funcName: "json_type",
			args:     []interface{}{`null`},
			expected: "null",
		},
		{
			name:     "json_type invalid",
			funcName: "json_type",
			args:     []interface{}{`{"name":"test"`},
			expected: "invalid",
		},
		{
			name:     "json_type non-string",
			funcName: "json_type",
			args:     []interface{}{123},
			expected: "unknown",
		},
		{
			name:     "json_length array",
			funcName: "json_length",
			args:     []interface{}{`[1,2,3]`},
			expected: 3,
		},
		{
			name:     "json_length object",
			funcName: "json_length",
			args:     []interface{}{`{"a":1,"b":2}`},
			expected: 2,
		},
		{
			name:     "json_length empty array",
			funcName: "json_length",
			args:     []interface{}{`[]`},
			expected: 0,
		},
		{
			name:     "json_length empty object",
			funcName: "json_length",
			args:     []interface{}{`{}`},
			expected: 0,
		},
		{
			name:     "json_length invalid json",
			funcName: "json_length",
			args:     []interface{}{`{"name":"test"`},
			wantErr:  true,
		},
		{
			name:     "json_length non-string",
			funcName: "json_length",
			args:     []interface{}{123},
			wantErr:  true,
		},
		{
			name:     "json_length string value",
			funcName: "json_length",
			args:     []interface{}{`"hello"`},
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

			if !tt.wantErr && !compareResults(result, tt.expected) {
				t.Errorf("Execute() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestJsonFunctionValidation 测试JSON函数参数验证
func TestJsonFunctionValidation(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "to_json no args",
			funcName: "to_json",
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "to_json too many args",
			funcName: "to_json",
			args:     []interface{}{"test", "extra"},
			wantErr:  true,
		},
		{
			name:     "from_json no args",
			funcName: "from_json",
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "from_json too many args",
			funcName: "from_json",
			args:     []interface{}{"test", "extra"},
			wantErr:  true,
		},
		{
			name:     "json_extract one arg",
			funcName: "json_extract",
			args:     []interface{}{"test"},
			wantErr:  true,
		},
		{
			name:     "json_extract too many args",
			funcName: "json_extract",
			args:     []interface{}{"test", "path", "extra"},
			wantErr:  true,
		},
		{
			name:     "json_valid no args",
			funcName: "json_valid",
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "json_type no args",
			funcName: "json_type",
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "json_length no args",
			funcName: "json_length",
			args:     []interface{}{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.funcName)
			if !exists {
				t.Fatalf("Function %s not found", tt.funcName)
			}

			err := fn.Validate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestJsonFunctionCreation 测试JSON函数创建
func TestJsonFunctionCreation(t *testing.T) {
	tests := []struct {
		name        string
		constructor func() Function
		expectedName string
	}{
		{
			name:        "ToJsonFunction",
			constructor: func() Function { return NewToJsonFunction() },
			expectedName: "to_json",
		},
		{
			name:        "FromJsonFunction",
			constructor: func() Function { return NewFromJsonFunction() },
			expectedName: "from_json",
		},
		{
			name:        "JsonExtractFunction",
			constructor: func() Function { return NewJsonExtractFunction() },
			expectedName: "json_extract",
		},
		{
			name:        "JsonValidFunction",
			constructor: func() Function { return NewJsonValidFunction() },
			expectedName: "json_valid",
		},
		{
			name:        "JsonTypeFunction",
			constructor: func() Function { return NewJsonTypeFunction() },
			expectedName: "json_type",
		},
		{
			name:        "JsonLengthFunction",
			constructor: func() Function { return NewJsonLengthFunction() },
			expectedName: "json_length",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := tt.constructor()
			if fn == nil {
				t.Error("Constructor returned nil")
				return
			}

			if fn.GetName() != tt.expectedName {
				t.Errorf("Expected name %s, got %s", tt.expectedName, fn.GetName())
			}

			if fn.GetType() == "" {
				t.Error("Function type should not be empty")
			}

			if fn.GetCategory() == "" {
				t.Error("Function category should not be empty")
			}

			if fn.GetDescription() == "" {
				t.Error("Function description should not be empty")
			}

			// Test argument validation through Validate method
			// Most JSON functions require exactly 1 argument, except json_extract which needs 2
			if tt.expectedName == "json_extract" {
				err := fn.Validate([]interface{}{"test", "$.path"})
				if err != nil {
					t.Errorf("Function %s should accept 2 arguments: %v", tt.expectedName, err)
				}
			} else if tt.expectedName != "json_length" { // json_length might have different requirements
				err := fn.Validate([]interface{}{"test"})
				if err != nil {
					t.Errorf("Function %s should accept 1 argument: %v", tt.expectedName, err)
				}
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
				if !compareResults(v, mapB[k]) {
					return false
				}
			}
			return true
		}
	}

	// 对于slice类型的特殊处理
	if sliceA, okA := a.([]interface{}); okA {
		if sliceB, okB := b.([]interface{}); okB {
			if len(sliceA) != len(sliceB) {
				return false
			}
			for i, v := range sliceA {
				if !compareResults(v, sliceB[i]) {
					return false
				}
			}
			return true
		}
	}

	return a == b
}
