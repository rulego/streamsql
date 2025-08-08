package functions

import (
	"math"
	"testing"
)

// TestMathFunctions 测试数学函数的基本功能
func TestMathFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "abs positive",
			funcName: "abs",
			args:     []interface{}{5},
			expected: float64(5),
			wantErr:  false,
		},
		{
			name:     "abs negative",
			funcName: "abs",
			args:     []interface{}{-5},
			expected: float64(5),
			wantErr:  false,
		},
		{
			name:     "abs zero",
			funcName: "abs",
			args:     []interface{}{0},
			expected: float64(0),
			wantErr:  false,
		},
		{
			name:     "sqrt positive",
			funcName: "sqrt",
			args:     []interface{}{9},
			expected: float64(3),
			wantErr:  false,
		},
		{
			name:     "sqrt zero",
			funcName: "sqrt",
			args:     []interface{}{0},
			expected: float64(0),
			wantErr:  false,
		},
		{
			name:     "sqrt 16",
			funcName: "sqrt",
			args:     []interface{}{16.0},
			expected: float64(4),
			wantErr:  false,
		},
		{
			name:     "sqrt 1",
			funcName: "sqrt",
			args:     []interface{}{1.0},
			expected: float64(1),
			wantErr:  false,
		},
		{
			name:     "acos valid",
			funcName: "acos",
			args:     []interface{}{1},
			expected: float64(0),
			wantErr:  false,
		},
		{
			name:     "asin valid",
			funcName: "asin",
			args:     []interface{}{0},
			expected: float64(0),
			wantErr:  false,
		},
		{
			name:     "atan valid",
			funcName: "atan",
			args:     []interface{}{0},
			expected: float64(0),
			wantErr:  false,
		},
		{
			name:     "cos zero",
			funcName: "cos",
			args:     []interface{}{0},
			expected: float64(1),
			wantErr:  false,
		},
		{
			name:     "sin zero",
			funcName: "sin",
			args:     []interface{}{0},
			expected: float64(0),
			wantErr:  false,
		},
		{
			name:     "tan zero",
			funcName: "tan",
			args:     []interface{}{0},
			expected: float64(0),
			wantErr:  false,
		},
		{
			name:     "exp zero",
			funcName: "exp",
			args:     []interface{}{0},
			expected: float64(1),
			wantErr:  false,
		},
		{
			name:     "log natural",
			funcName: "log",
			args:     []interface{}{10.0},
			expected: float64(1),
			wantErr:  false,
		},
		{
			name:     "log10 hundred",
			funcName: "log10",
			args:     []interface{}{100},
			expected: float64(2),
			wantErr:  false,
		},
		{
			name:     "ceil positive",
			funcName: "ceil",
			args:     []interface{}{3.14},
			expected: float64(4),
			wantErr:  false,
		},
		{
			name:     "floor positive",
			funcName: "floor",
			args:     []interface{}{3.14},
			expected: float64(3),
			wantErr:  false,
		},
		{
			name:     "round positive",
			funcName: "round",
			args:     []interface{}{3.14},
			expected: float64(3),
			wantErr:  false,
		},
		{
			name:     "round half up",
			funcName: "round",
			args:     []interface{}{3.5},
			expected: float64(4),
			wantErr:  false,
		},
		{
			name:     "power basic",
			funcName: "power",
			args:     []interface{}{2, 3},
			expected: float64(8),
			wantErr:  false,
		},
		{
			name:     "mod basic",
			funcName: "mod",
			args:     []interface{}{10, 3},
			expected: float64(1),
			wantErr:  false,
		},
		{
			name:     "log2 8",
			funcName: "log2",
			args:     []interface{}{8.0},
			expected: float64(3),
			wantErr:  false,
		},
		{
			name:     "log2 1",
			funcName: "log2",
			args:     []interface{}{1.0},
			expected: float64(0),
			wantErr:  false,
		},
		{
			name:     "log2 2",
			funcName: "log2",
			args:     []interface{}{2.0},
			expected: float64(1),
			wantErr:  false,
		},
		{
			name:     "sign positive",
			funcName: "sign",
			args:     []interface{}{5.0},
			expected: 1,
			wantErr:  false,
		},
		{
			name:     "sign negative",
			funcName: "sign",
			args:     []interface{}{-5.0},
			expected: -1,
			wantErr:  false,
		},
		{
			name:     "sign zero",
			funcName: "sign",
			args:     []interface{}{0.0},
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "mod basic",
			funcName: "mod",
			args:     []interface{}{10.0, 3.0},
			expected: float64(1),
			wantErr:  false,
		},
		{
			name:     "mod decimal",
			funcName: "mod",
			args:     []interface{}{10.5, 3.0},
			expected: float64(1.5),
			wantErr:  false,
		},
		{
			name:     "mod negative",
			funcName: "mod",
			args:     []interface{}{-10.0, 3.0},
			expected: float64(-1),
			wantErr:  false,
		},
		{
			name:     "round up",
			funcName: "round",
			args:     []interface{}{3.7},
			expected: float64(4),
			wantErr:  false,
		},
		{
			name:     "round down",
			funcName: "round",
			args:     []interface{}{3.2},
			expected: float64(3),
			wantErr:  false,
		},
		{
			name:     "round negative",
			funcName: "round",
			args:     []interface{}{-3.7},
			expected: float64(-4),
			wantErr:  false,
		},
		{
			name:     "power basic",
			funcName: "power",
			args:     []interface{}{2.0, 3.0},
			expected: float64(8),
			wantErr:  false,
		},
		{
			name:     "power square",
			funcName: "power",
			args:     []interface{}{5.0, 2.0},
			expected: float64(25),
			wantErr:  false,
		},
		{
			name:     "power zero exponent",
			funcName: "power",
			args:     []interface{}{2.0, 0.0},
			expected: float64(1),
			wantErr:  false,
		},
		// 错误处理测试用例
		{
			name:     "abs invalid type",
			funcName: "abs",
			args:     []interface{}{"invalid"},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "sqrt invalid type",
			funcName: "sqrt",
			args:     []interface{}{"invalid"},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "mod division by zero",
			funcName: "mod",
			args:     []interface{}{10.0, 0.0},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "power invalid base",
			funcName: "power",
			args:     []interface{}{"invalid", 2.0},
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
			if tt.wantErr {
				if err == nil {
					t.Errorf("Execute() expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("Execute() result = %v, want %v", result, tt.expected)
			}
		})
	}

	// 特殊测试：rand函数（因为结果是随机的）
	t.Run("rand function", func(t *testing.T) {
		fn, exists := Get("rand")
		if !exists {
			t.Fatal("rand function not found")
		}

		result, err := fn.Execute(&FunctionContext{}, []interface{}{})
		if err != nil {
			t.Errorf("rand() error = %v", err)
			return
		}

		val, ok := result.(float64)
		if !ok {
			t.Errorf("rand() result type = %T, want float64", result)
			return
		}
		if val < 0.0 || val >= 1.0 {
			t.Errorf("rand() result = %v, want [0.0, 1.0)", val)
		}
	})
}

// TestMathFunctionValidation 测试数学函数的参数验证
func TestMathFunctionValidation(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "abs no args",
			function: NewAbsFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "abs too many args",
			function: NewAbsFunction(),
			args:     []interface{}{1, 2},
			wantErr:  true,
		},
		{
			name:     "abs valid args",
			function: NewAbsFunction(),
			args:     []interface{}{-5},
			wantErr:  false,
		},
		{
			name:     "sqrt no args",
			function: NewSqrtFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "sqrt valid args",
			function: NewSqrtFunction(),
			args:     []interface{}{9},
			wantErr:  false,
		},
		{
			name:     "power no args",
			function: NewPowerFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "power one arg",
			function: NewPowerFunction(),
			args:     []interface{}{2},
			wantErr:  true,
		},
		{
			name:     "power valid args",
			function: NewPowerFunction(),
			args:     []interface{}{2, 3},
			wantErr:  false,
		},
		{
			name:     "mod no args",
			function: NewModFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "mod one arg",
			function: NewModFunction(),
			args:     []interface{}{10},
			wantErr:  true,
		},
		{
			name:     "mod valid args",
			function: NewModFunction(),
			args:     []interface{}{10, 3},
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

// TestMathFunctionErrors 测试数学函数的错误处理
func TestMathFunctionErrors(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "abs non-numeric",
			function: NewAbsFunction(),
			args:     []interface{}{"not a number"},
			wantErr:  true,
		},
		{
			name:     "sqrt negative",
			function: NewSqrtFunction(),
			args:     []interface{}{-1},
			wantErr:  true,
		},
		{
			name:     "sqrt non-numeric",
			function: NewSqrtFunction(),
			args:     []interface{}{"not a number"},
			wantErr:  true,
		},
		{
			name:     "log zero",
			function: NewLogFunction(),
			args:     []interface{}{0},
			wantErr:  true,
		},
		{
			name:     "log negative",
			function: NewLogFunction(),
			args:     []interface{}{-1},
			wantErr:  true,
		},
		{
			name:     "log non-numeric",
			function: NewLogFunction(),
			args:     []interface{}{"not a number"},
			wantErr:  true,
		},
		{
			name:     "power non-numeric base",
			function: NewPowerFunction(),
			args:     []interface{}{"not a number", 2},
			wantErr:  true,
		},
		{
			name:     "power non-numeric exponent",
			function: NewPowerFunction(),
			args:     []interface{}{2, "not a number"},
			wantErr:  true,
		},
		{
			name:     "mod division by zero",
			function: NewModFunction(),
			args:     []interface{}{10, 0},
			wantErr:  true,
		},
		{
			name:     "mod non-numeric dividend",
			function: NewModFunction(),
			args:     []interface{}{"not a number", 3},
			wantErr:  true,
		},
		{
			name:     "mod non-numeric divisor",
			function: NewModFunction(),
			args:     []interface{}{10, "not a number"},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.function.Execute(&FunctionContext{}, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestMathFunctionEdgeCases 测试数学函数的边界情况
func TestMathFunctionEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "abs float",
			function: NewAbsFunction(),
			args:     []interface{}{-3.14},
			expected: 3.14,
			wantErr:  false,
		},
		{
			name:     "sqrt float",
			function: NewSqrtFunction(),
			args:     []interface{}{2.25},
			expected: 1.5,
			wantErr:  false,
		},
		{
			name:     "ceiling negative",
			function: NewCeilingFunction(),
			args:     []interface{}{-3.14},
			expected: float64(-3),
			wantErr:  false,
		},
		{
			name:     "floor negative",
			function: NewFloorFunction(),
			args:     []interface{}{-3.14},
			expected: float64(-4),
			wantErr:  false,
		},
		{
			name:     "round negative",
			function: NewRoundFunction(),
			args:     []interface{}{-3.5},
			expected: float64(-4),
			wantErr:  false,
		},
		{
			name:     "power zero exponent",
			function: NewPowerFunction(),
			args:     []interface{}{5, 0},
			expected: float64(1),
			wantErr:  false,
		},
		{
			name:     "power negative base",
			function: NewPowerFunction(),
			args:     []interface{}{-2, 3},
			expected: float64(-8),
			wantErr:  false,
		},
		{
			name:     "mod negative dividend",
			function: NewModFunction(),
			args:     []interface{}{-10, 3},
			expected: float64(-1),
			wantErr:  false,
		},
		{
			name:     "mod negative divisor",
			function: NewModFunction(),
			args:     []interface{}{10, -3},
			expected: float64(1),
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

			if !tt.wantErr {
				// 对于浮点数比较，使用近似相等
				if expected, ok := tt.expected.(float64); ok {
					if actual, ok := result.(float64); ok {
						if math.Abs(actual-expected) > 1e-9 {
							t.Errorf("Execute() = %v, want %v", actual, expected)
						}
					} else {
						t.Errorf("Execute() result type = %T, want float64", result)
					}
				} else if result != tt.expected {
					t.Errorf("Execute() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}