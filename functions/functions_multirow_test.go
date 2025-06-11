package functions

import (
	"reflect"
	"testing"
)

// TestUnnestFunction 测试unnest函数
func TestUnnestFunction(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		// unnest 函数测试
		{
			name:     "unnest array",
			funcName: "unnest",
			args:     []interface{}{[]interface{}{1, 2, 3}},
			expected: []interface{}{1, 2, 3},
		},
		{
			name:     "unnest empty array",
			funcName: "unnest",
			args:     []interface{}{[]interface{}{}},
			expected: []interface{}{},
		},
		{
			name:     "unnest nil",
			funcName: "unnest",
			args:     []interface{}{nil},
			expected: []interface{}{},
		},
		{
			name:     "unnest non-array",
			funcName: "unnest",
			args:     []interface{}{"not an array"},
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

			if !tt.wantErr {
				if !reflect.DeepEqual(result, tt.expected) {
					t.Errorf("Execute() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

// TestUnnestWithObjects 测试 unnest 函数处理对象数组
func TestUnnestWithObjects(t *testing.T) {
	fn, exists := Get("unnest")
	if !exists {
		t.Fatal("Function unnest not found")
	}

	// 测试对象数组
	objectArray := []interface{}{
		map[string]interface{}{"name": "Alice", "age": 30},
		map[string]interface{}{"name": "Bob", "age": 25},
	}

	result, err := fn.Execute(&FunctionContext{}, []interface{}{objectArray})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// 检查结果是否包含特殊标记
	resultSlice, ok := result.([]interface{})
	if !ok {
		t.Fatalf("Expected []interface{}, got %T", result)
	}

	if len(resultSlice) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(resultSlice))
	}

	// 检查第一个对象是否有特殊标记
	firstItem, ok := resultSlice[0].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map[string]interface{}, got %T", resultSlice[0])
	}

	if unnestFlag, exists := firstItem["__unnest_object__"]; !exists || unnestFlag != true {
		t.Error("Expected __unnest_object__ flag to be true")
	}

	if data, exists := firstItem["__data__"]; !exists {
		t.Error("Expected __data__ field to exist")
	} else {
		dataMap, ok := data.(map[string]interface{})
		if !ok {
			t.Errorf("Expected __data__ to be map[string]interface{}, got %T", data)
		} else {
			if dataMap["name"] != "Alice" || dataMap["age"] != 30 {
				t.Errorf("Unexpected data: %v", dataMap)
			}
		}
	}
}