package functions

import (
	"reflect"
	"testing"
)

// 测试数组函数
func TestArrayFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "array_length basic",
			funcName: "array_length",
			args:     []interface{}{[]interface{}{1, 2, 3}},
			expected: 3,
			wantErr:  false,
		},
		{
			name:     "array_length empty",
			funcName: "array_length",
			args:     []interface{}{[]interface{}{}},
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "array_contains true",
			funcName: "array_contains",
			args:     []interface{}{[]interface{}{1, 2, 3}, 2},
			expected: true,
			wantErr:  false,
		},
		{
			name:     "array_contains false",
			funcName: "array_contains",
			args:     []interface{}{[]interface{}{1, 2, 3}, 4},
			expected: false,
			wantErr:  false,
		},
		{
			name:     "array_contains empty array",
			funcName: "array_contains",
			args:     []interface{}{[]interface{}{}, 1},
			expected: false,
			wantErr:  false,
		},
		{
			name:     "array_position found",
			funcName: "array_position",
			args:     []interface{}{[]interface{}{1, 2, 3}, 2},
			expected: 2,
			wantErr:  false,
		},
		{
			name:     "array_position not found",
			funcName: "array_position",
			args:     []interface{}{[]interface{}{1, 2, 3}, 4},
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "array_position empty array",
			funcName: "array_position",
			args:     []interface{}{[]interface{}{}, 1},
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "array_remove basic",
			funcName: "array_remove",
			args:     []interface{}{[]interface{}{1, 2, 3, 2}, 2},
			expected: []interface{}{1, 3},
			wantErr:  false,
		},
		{
			name:     "array_remove not found",
			funcName: "array_remove",
			args:     []interface{}{[]interface{}{1, 2, 3}, 4},
			expected: []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "array_remove empty array",
			funcName: "array_remove",
			args:     []interface{}{[]interface{}{}, 1},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "array_distinct basic",
			funcName: "array_distinct",
			args:     []interface{}{[]interface{}{1, 2, 2, 3, 1}},
			expected: []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "array_distinct empty",
			funcName: "array_distinct",
			args:     []interface{}{[]interface{}{}},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "array_intersect basic",
			funcName: "array_intersect",
			args:     []interface{}{[]interface{}{1, 2, 3}, []interface{}{2, 3, 4}},
			expected: []interface{}{2, 3},
			wantErr:  false,
		},
		{
			name:     "array_intersect no intersection",
			funcName: "array_intersect",
			args:     []interface{}{[]interface{}{1, 2}, []interface{}{3, 4}},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "array_intersect first empty",
			funcName: "array_intersect",
			args:     []interface{}{[]interface{}{}, []interface{}{1, 2}},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "array_intersect second empty",
			funcName: "array_intersect",
			args:     []interface{}{[]interface{}{1, 2}, []interface{}{}},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "array_union basic",
			funcName: "array_union",
			args:     []interface{}{[]interface{}{1, 2}, []interface{}{2, 3}},
			expected: []interface{}{1, 2, 3},
			wantErr:  false,
		},
		{
			name:     "array_union first empty",
			funcName: "array_union",
			args:     []interface{}{[]interface{}{}, []interface{}{1, 2}},
			expected: []interface{}{1, 2},
			wantErr:  false,
		},
		{
			name:     "array_union second empty",
			funcName: "array_union",
			args:     []interface{}{[]interface{}{1, 2}, []interface{}{}},
			expected: []interface{}{1, 2},
			wantErr:  false,
		},
		{
			name:     "array_except basic",
			funcName: "array_except",
			args:     []interface{}{[]interface{}{1, 2, 3}, []interface{}{2}},
			expected: []interface{}{1, 3},
			wantErr:  false,
		},
		{
			name:     "array_except no overlap",
			funcName: "array_except",
			args:     []interface{}{[]interface{}{1, 2}, []interface{}{3, 4}},
			expected: []interface{}{1, 2},
			wantErr:  false,
		},
		{
			name:     "array_except first empty",
			funcName: "array_except",
			args:     []interface{}{[]interface{}{}, []interface{}{1, 2}},
			expected: []interface{}{},
			wantErr:  false,
		},
		{
			name:     "array_except second empty",
			funcName: "array_except",
			args:     []interface{}{[]interface{}{1, 2}, []interface{}{}},
			expected: []interface{}{1, 2},
			wantErr:  false,
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

		if !tt.wantErr && !reflect.DeepEqual(result, tt.expected) {
			t.Errorf("Execute() = %v, want %v", result, tt.expected)
		}
		})
	}
}

// TestArrayFunctionErrors 测试数组函数的错误处理
func TestArrayFunctionErrors(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		wantErr  bool
	}{
		// array_length 错误测试
		{"array_length nil", "array_length", []interface{}{nil}, true},
		{"array_length invalid type", "array_length", []interface{}{"not an array"}, true},
		
		// array_contains 错误测试
		{"array_contains nil array", "array_contains", []interface{}{nil, 1}, true},
		{"array_contains invalid type", "array_contains", []interface{}{"not an array", 1}, true},
		
		// array_position 错误测试
		{"array_position nil array", "array_position", []interface{}{nil, 1}, true},
		{"array_position invalid type", "array_position", []interface{}{"not an array", 1}, true},
		
		// array_remove 错误测试
		{"array_remove nil array", "array_remove", []interface{}{nil, 1}, true},
		{"array_remove invalid type", "array_remove", []interface{}{"not an array", 1}, true},
		
		// array_distinct 错误测试
		{"array_distinct nil", "array_distinct", []interface{}{nil}, true},
		{"array_distinct invalid type", "array_distinct", []interface{}{"not an array"}, true},
		
		// array_intersect 错误测试
		{"array_intersect first nil", "array_intersect", []interface{}{nil, []interface{}{1, 2}}, true},
		{"array_intersect second nil", "array_intersect", []interface{}{[]interface{}{1, 2}, nil}, true},
		{"array_intersect first invalid type", "array_intersect", []interface{}{"not an array", []interface{}{1, 2}}, true},
		{"array_intersect second invalid type", "array_intersect", []interface{}{[]interface{}{1, 2}, "not an array"}, true},
		
		// array_union 错误测试
		{"array_union first nil", "array_union", []interface{}{nil, []interface{}{1, 2}}, true},
		{"array_union second nil", "array_union", []interface{}{[]interface{}{1, 2}, nil}, true},
		{"array_union first invalid type", "array_union", []interface{}{"not an array", []interface{}{1, 2}}, true},
		{"array_union second invalid type", "array_union", []interface{}{[]interface{}{1, 2}, "not an array"}, true},
		
		// array_except 错误测试
		{"array_except first nil", "array_except", []interface{}{nil, []interface{}{1, 2}}, true},
		{"array_except second nil", "array_except", []interface{}{[]interface{}{1, 2}, nil}, true},
		{"array_except first invalid type", "array_except", []interface{}{"not an array", []interface{}{1, 2}}, true},
		{"array_except second invalid type", "array_except", []interface{}{[]interface{}{1, 2}, "not an array"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.funcName)
			if !exists {
				t.Fatalf("Function %s not found", tt.funcName)
			}

			_, err := fn.Execute(&FunctionContext{}, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
