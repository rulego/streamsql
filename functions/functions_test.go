package functions

import (
	"github.com/rulego/streamsql/utils/cast"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasicFunctionRegistry(t *testing.T) {
	// 测试基本函数注册
	tests := []struct {
		name         string
		functionName string
		expectedType FunctionType
	}{
		{"abs function", "abs", TypeMath},
		{"concat function", "concat", TypeString},
		{"sqrt function", "sqrt", TypeMath},
		{"upper function", "upper", TypeString},
		{"cast function", "cast", TypeConversion},
		{"now function", "now", TypeDateTime},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.functionName)
			assert.True(t, exists, "%s should be registered", tt.functionName)
			assert.NotNil(t, fn)
			assert.Equal(t, tt.functionName, fn.GetName())
			assert.Equal(t, tt.expectedType, fn.GetType())
		})
	}

	// 测试不存在的函数
	_, exists := Get("nonexistent")
	assert.False(t, exists, "nonexistent function should not be found")
}

func TestNewMathFunctions(t *testing.T) {
	ctx := &FunctionContext{
		Data: map[string]interface{}{},
	}

	// 表驱动测试用例
	tests := []struct {
		name         string
		functionName string
		args         []interface{}
		expected     interface{}
		expectError  bool
		errorMsg     string
		delta        float64 // 用于浮点数比较的精度
	}{
		// Log function tests
		{"log valid", "log", []interface{}{math.E}, 1.0, false, "", 1e-10},
		{"log negative", "log", []interface{}{-1}, nil, true, "value must be positive", 0},
		{"log zero", "log", []interface{}{0}, nil, true, "value must be positive", 0},
		
		// Log10 function tests
		{"log10 100", "log10", []interface{}{100}, 2.0, false, "", 1e-10},
		{"log10 10", "log10", []interface{}{10}, 1.0, false, "", 1e-10},
		
		// Log2 function tests
		{"log2 8", "log2", []interface{}{8}, 3.0, false, "", 1e-10},
		{"log2 2", "log2", []interface{}{2}, 1.0, false, "", 1e-10},
		
		// Mod function tests
		{"mod 10,3", "mod", []interface{}{10, 3}, 1.0, false, "", 1e-10},
		{"mod 7.5,2.5", "mod", []interface{}{7.5, 2.5}, 0.0, false, "", 1e-10},
		{"mod division by zero", "mod", []interface{}{10, 0}, nil, true, "division by zero", 0},
		
		// Round function tests
		{"round 3.7", "round", []interface{}{3.7}, 4.0, false, "", 1e-10},
		{"round 3.2", "round", []interface{}{3.2}, 3.0, false, "", 1e-10},
		{"round with precision", "round", []interface{}{3.14159, 2}, 3.14, false, "", 1e-10},
		
		// Sign function tests
		{"sign positive", "sign", []interface{}{5.5}, 1, false, "", 0},
		{"sign negative", "sign", []interface{}{-3.2}, -1, false, "", 0},
		{"sign zero", "sign", []interface{}{0}, 0, false, "", 0},
		
		// Trigonometric function tests
		{"sin 0", "sin", []interface{}{0}, 0.0, false, "", 1e-10},
		{"sin π/2", "sin", []interface{}{math.Pi / 2}, 1.0, false, "", 1e-10},
		{"sinh 0", "sinh", []interface{}{0}, 0.0, false, "", 1e-10},
		{"tan 0", "tan", []interface{}{0}, 0.0, false, "", 1e-10},
		{"tanh 0", "tanh", []interface{}{0}, 0.0, false, "", 1e-10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.functionName)
			assert.True(t, exists, "Function %s should be registered", tt.functionName)
			
			result, err := fn.Execute(ctx, tt.args)
			
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				if tt.delta > 0 {
					assert.InDelta(t, tt.expected, result, tt.delta)
				} else {
					assert.Equal(t, tt.expected, result)
				}
			}
		})
	}

	// 特殊测试：rand函数（因为结果是随机的）
	t.Run("rand function", func(t *testing.T) {
		fn, exists := Get("rand")
		assert.True(t, exists)
		
		result, err := fn.Execute(ctx, []interface{}{})
		assert.NoError(t, err)
		
		val, ok := result.(float64)
		assert.True(t, ok)
		assert.GreaterOrEqual(t, val, 0.0)
		assert.Less(t, val, 1.0)
	})
}

func TestFunctionExecution(t *testing.T) {
	ctx := &FunctionContext{
		Data: map[string]interface{}{},
	}

	// 函数执行测试用例
	tests := []struct {
		name         string
		functionName string
		args         []interface{}
		expected     interface{}
		expectError  bool
	}{
		// 数学函数
		{"abs with positive", "abs", []interface{}{5.5}, 5.5, false},
		{"abs with negative", "abs", []interface{}{-5.5}, 5.5, false},
		{"abs with zero", "abs", []interface{}{0}, 0.0, false},
		{"sqrt with perfect square", "sqrt", []interface{}{16.0}, 4.0, false},
		{"sqrt with decimal", "sqrt", []interface{}{2.0}, 1.4142135623730951, false},
		{"sqrt with zero", "sqrt", []interface{}{0}, 0.0, false},
		{"sqrt with negative", "sqrt", []interface{}{-1}, nil, true},

		// 时间日期函数
		{"now basic", "now", []interface{}{}, time.Now().Unix(), false},
		{"current_time basic", "current_time", []interface{}{}, time.Now().Format("15:04:05"), false},
		{"current_date basic", "current_date", []interface{}{}, time.Now().Format("2006-01-02"), false},

		// 新增数学函数测试
		{"acos valid", "acos", []interface{}{0.5}, math.Acos(0.5), false},
		{"acos invalid", "acos", []interface{}{2.0}, nil, true},
		{"asin valid", "asin", []interface{}{0.5}, math.Asin(0.5), false},
		{"asin invalid", "asin", []interface{}{2.0}, nil, true},
		{"atan valid", "atan", []interface{}{1.0}, math.Atan(1.0), false},
		{"atan2 valid", "atan2", []interface{}{1.0, 1.0}, math.Atan2(1.0, 1.0), false},
		{"bitand valid", "bitand", []interface{}{5, 3}, int64(1), false},
		{"bitor valid", "bitor", []interface{}{5, 3}, int64(7), false},
		{"bitxor valid", "bitxor", []interface{}{5, 3}, int64(6), false},
		{"bitnot valid", "bitnot", []interface{}{5}, int64(-6), false},
		{"ceiling positive", "ceiling", []interface{}{3.7}, 4.0, false},
		{"ceiling negative", "ceiling", []interface{}{-3.7}, -3.0, false},
		{"cos valid", "cos", []interface{}{0.0}, 1.0, false},
		{"cosh valid", "cosh", []interface{}{0.0}, 1.0, false},
		{"exp valid", "exp", []interface{}{1.0}, math.E, false},
		{"floor positive", "floor", []interface{}{3.7}, 3.0, false},
		{"floor negative", "floor", []interface{}{-3.7}, -4.0, false},
		{"ln valid", "ln", []interface{}{math.E}, 1.0, false},
		{"ln invalid", "ln", []interface{}{-1.0}, nil, true},
		{"power valid", "power", []interface{}{2.0, 3.0}, 8.0, false},

		// 字符串函数
		{"concat basic", "concat", []interface{}{"hello", " ", "world"}, "hello world", false},
		{"concat single", "concat", []interface{}{"hello"}, "hello", false},
		{"concat numbers", "concat", []interface{}{1, 2, 3}, "123", false},
		{"length basic", "length", []interface{}{"hello"}, int64(5), false},
		{"length empty", "length", []interface{}{""}, int64(0), false},
		{"upper basic", "upper", []interface{}{"hello"}, "HELLO", false},
		{"upper mixed", "upper", []interface{}{"Hello World"}, "HELLO WORLD", false},
		{"lower basic", "lower", []interface{}{"HELLO"}, "hello", false},
		{"lower mixed", "lower", []interface{}{"Hello World"}, "hello world", false},

		// 转换函数
		{"cast to int64", "cast", []interface{}{"123", "int64"}, int64(123), false},
		{"cast to float64", "cast", []interface{}{"123.45", "float64"}, 123.45, false},
		{"cast to string", "cast", []interface{}{123, "string"}, "123", false},
		{"hex2dec basic", "hex2dec", []interface{}{"ff"}, int64(255), false},
		{"hex2dec upper", "hex2dec", []interface{}{"FF"}, int64(255), false},
		{"hex2dec with prefix", "hex2dec", []interface{}{"a0"}, int64(160), false},
		{"dec2hex basic", "dec2hex", []interface{}{255}, "ff", false},
		{"dec2hex zero", "dec2hex", []interface{}{0}, "0", false},
		{"dec2hex large", "dec2hex", []interface{}{4095}, "fff", false},
		{"encode base64", "encode", []interface{}{"hello", "base64"}, "aGVsbG8=", false},
		{"encode hex", "encode", []interface{}{"hello", "hex"}, "68656c6c6f", false},
		{"encode url", "encode", []interface{}{"hello world", "url"}, "hello+world", false},
		{"encode invalid format", "encode", []interface{}{"hello", "invalid"}, nil, true},
		{"encode invalid input", "encode", []interface{}{123, "base64"}, nil, true},

		{"decode base64", "decode", []interface{}{"aGVsbG8=", "base64"}, "hello", false},
		{"decode hex", "decode", []interface{}{"68656c6c6f", "hex"}, "hello", false},
		{"decode url", "decode", []interface{}{"hello+world", "url"}, "hello world", false},
		{"decode invalid format", "decode", []interface{}{"hello", "invalid"}, nil, true},
		{"decode invalid base64", "decode", []interface{}{"invalid!", "base64"}, nil, true},
		{"decode invalid hex", "decode", []interface{}{"invalid!", "hex"}, nil, true},

		// 聚合函数
		{"sum basic", "sum", []interface{}{1, 2, 3}, 6.0, false},
		{"sum float", "sum", []interface{}{1.5, 2.5}, 4.0, false},
		{"avg basic", "avg", []interface{}{1, 2, 3}, 2.0, false},
		{"min basic", "min", []interface{}{3, 1, 2}, 1.0, false},
		{"max basic", "max", []interface{}{3, 1, 2}, 3.0, false},
		{"count basic", "count", []interface{}{1, 2, 3, 4, 5}, int64(5), false},

		// 错误情况
		{"hex2dec invalid", "hex2dec", []interface{}{"xyz"}, nil, true},

		// 新增的字符串函数
		{"trim basic", "trim", []interface{}{"  hello world  "}, "hello world", false},
		{"trim empty", "trim", []interface{}{""}, "", false},
		{"format number 2 decimals", "format", []interface{}{123.456, "0.00"}, "123.46", false},
		{"format number 0 decimals", "format", []interface{}{123.456, "0"}, "123", false},
		{"format string only", "format", []interface{}{"hello"}, "hello", false},

		// 新增的聚合函数
		{"collect basic", "collect", []interface{}{1, 2, 3}, []interface{}{1, 2, 3}, false},
		{"last_value basic", "last_value", []interface{}{1, 2, 3, 4}, 4, false},
		{"merge_agg basic", "merge_agg", []interface{}{"a", "b", "c"}, "a,b,c", false},
		{"stddevs basic", "stddevs", []interface{}{1.0, 2.0, 3.0, 4.0, 5.0}, 1.5811388300841898, false},
		{"deduplicate basic", "deduplicate", []interface{}{1, 2, 2, 3, 3, 3}, []interface{}{1, 2, 3}, false},
		{"var basic", "var", []interface{}{1.0, 2.0, 3.0, 4.0, 5.0}, 2.0, false},
		{"vars basic", "vars", []interface{}{1.0, 2.0, 3.0, 4.0, 5.0}, 2.5, false},

		// 窗口函数
		{"row_number basic", "row_number", []interface{}{}, int64(1), false},

		// 分析函数
		{"latest basic", "latest", []interface{}{"hello"}, "hello", false},
		{"had_changed first", "had_changed", []interface{}{"value1"}, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.functionName)
			assert.True(t, exists, "function %s should exist", tt.functionName)
			if !exists || fn == nil {
				t.Errorf("Function %s not found or is nil", tt.functionName)
				return
			}

			result, err := fn.Execute(ctx, tt.args)

			if tt.expectError {
				assert.Error(t, err, "expected error for %s", tt.name)
			} else {
				assert.NoError(t, err, "no error expected for %s", tt.name)
				if tt.expected != nil {
					switch expected := tt.expected.(type) {
					case float64:
						if resultFloat, ok := result.(float64); ok {
							assert.InDelta(t, expected, resultFloat, 0.0001, "result should match for %s", tt.name)
						} else {
							t.Errorf("Expected float64 but got %T for %s", result, tt.name)
						}
					case int64:
						if tt.functionName == "now" {
							// 对于 now 函数，我们只检查结果是否为 int64 类型，因为具体值会随时间变化
							_, ok := result.(int64)
							assert.True(t, ok, "now function should return int64")
						} else {
							assert.Equal(t, expected, result, "result should match for %s", tt.name)
						}
					case string:
						if tt.functionName == "current_time" || tt.functionName == "current_date" {
							// 对于时间日期函数，我们只检查格式是否正确
							resultStr, ok := result.(string)
							assert.True(t, ok, "%s function should return string", tt.functionName)
							if tt.functionName == "current_time" {
								_, err := time.Parse("15:04:05", resultStr)
								assert.NoError(t, err, "current_time should return valid time format")
							} else if tt.functionName == "current_date" {
								_, err := time.Parse("2006-01-02", resultStr)
								assert.NoError(t, err, "current_date should return valid date format")
							}
						} else {
							assert.Equal(t, expected, result, "result should match for %s", tt.name)
						}
					default:
						assert.Equal(t, expected, result, "result should match for %s", tt.name)
					}
				}
			}
		})
	}
}

func TestFunctionValidation(t *testing.T) {
	// 参数验证测试用例
	tests := []struct {
		name         string
		functionName string
		args         []interface{}
		expectError  bool
		description  string
	}{
		// abs 函数 - 需要1个参数
		{"abs no args", "abs", []interface{}{}, true, "abs requires 1 argument"},
		{"abs too many args", "abs", []interface{}{1.0, 2.0}, true, "abs accepts only 1 argument"},
		{"abs correct args", "abs", []interface{}{1.0}, false, "abs should accept 1 argument"},

		// 时间日期函数参数验证
		{"current_time with args", "current_time", []interface{}{1}, true, "current_time should not accept arguments"},
		{"current_date with args", "current_date", []interface{}{1}, true, "current_date should not accept arguments"},

		// concat 函数 - 需要至少1个参数
		{"concat no args", "concat", []interface{}{}, true, "concat requires at least 1 argument"},
		{"concat one arg", "concat", []interface{}{"hello"}, false, "concat should accept 1 argument"},
		{"concat multiple args", "concat", []interface{}{"a", "b", "c"}, false, "concat should accept multiple arguments"},

		// cast 函数 - 需要恰好2个参数
		{"cast no args", "cast", []interface{}{}, true, "cast requires 2 arguments"},
		{"cast one arg", "cast", []interface{}{"123"}, true, "cast requires 2 arguments"},
		{"cast correct args", "cast", []interface{}{"123", "int64"}, false, "cast should accept 2 arguments"},
		{"cast too many args", "cast", []interface{}{"123", "int64", "extra"}, true, "cast accepts only 2 arguments"},

		// now 函数 - 不需要参数
		{"now no args", "now", []interface{}{}, false, "now should accept no arguments"},
		{"now with args", "now", []interface{}{1}, true, "now should not accept arguments"},

		// 新增数学函数参数验证
		{"acos no args", "acos", []interface{}{}, true, "acos requires 1 argument"},
		{"acos too many args", "acos", []interface{}{1.0, 2.0}, true, "acos accepts only 1 argument"},
		{"atan2 no args", "atan2", []interface{}{}, true, "atan2 requires 2 arguments"},
		{"atan2 one arg", "atan2", []interface{}{1.0}, true, "atan2 requires 2 arguments"},
		{"atan2 too many args", "atan2", []interface{}{1.0, 2.0, 3.0}, true, "atan2 accepts only 2 arguments"},
		{"bitand no args", "bitand", []interface{}{}, true, "bitand requires 2 arguments"},
		{"bitand one arg", "bitand", []interface{}{1}, true, "bitand requires 2 arguments"},
		{"bitand too many args", "bitand", []interface{}{1, 2, 3}, true, "bitand accepts only 2 arguments"},
		{"bitnot no args", "bitnot", []interface{}{}, true, "bitnot requires 1 argument"},
		{"bitnot too many args", "bitnot", []interface{}{1, 2}, true, "bitnot accepts only 1 argument"},
		{"power no args", "power", []interface{}{}, true, "power requires 2 arguments"},
		{"power one arg", "power", []interface{}{2.0}, true, "power requires 2 arguments"},
		{"power too many args", "power", []interface{}{2.0, 3.0, 4.0}, true, "power accepts only 2 arguments"},

		// 转换函数参数验证
		{"encode no args", "encode", []interface{}{}, true, "encode requires 2 arguments"},
		{"encode one arg", "encode", []interface{}{"hello"}, true, "encode requires 2 arguments"},
		{"encode three args", "encode", []interface{}{"hello", "base64", "extra"}, true, "encode requires exactly 2 arguments"},
		{"encode invalid format type", "encode", []interface{}{"hello", 123}, true, "encode format must be a string"},

		{"decode no args", "decode", []interface{}{}, true, "decode requires 2 arguments"},
		{"decode one arg", "decode", []interface{}{"aGVsbG8="}, true, "decode requires 2 arguments"},
		{"decode three args", "decode", []interface{}{"aGVsbG8=", "base64", "extra"}, true, "decode requires exactly 2 arguments"},
		{"decode invalid input type", "decode", []interface{}{123, "base64"}, true, "decode input must be a string"},
		{"decode invalid format type", "decode", []interface{}{"aGVsbG8=", 123}, true, "decode format must be a string"},

		// 新增函数的验证测试
		{"trim no args", "trim", []interface{}{}, true, "function trim requires at least 1 arguments"},
		{"trim too many args", "trim", []interface{}{"hello", "world"}, true, "function trim accepts at most 1 arguments"},
		{"format too many args", "format", []interface{}{"hello", "pattern", "locale", "extra"}, true, "function format accepts at most 3 arguments"},
		{"collect no args", "collect", []interface{}{}, true, "function collect requires at least 1 arguments"},
		{"row_number with args", "row_number", []interface{}{"invalid"}, true, "function row_number accepts at most 0 arguments"},
		{"latest no args", "latest", []interface{}{}, true, "function latest requires at least 1 arguments"},
		{"had_changed no args", "had_changed", []interface{}{}, true, "function had_changed requires at least 1 arguments"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.functionName)
			assert.True(t, exists, "function %s should exist", tt.functionName)

			err := fn.Validate(tt.args)

			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
			}
		})
	}
}

func TestFunctionTypes(t *testing.T) {
	// 函数类型分类测试
	tests := []struct {
		functionType FunctionType
		functions    []string
	}{
		{TypeMath, []string{
			"abs", "sqrt", "acos", "asin", "atan", "atan2",
			"bitand", "bitor", "bitxor", "bitnot",
			"ceiling", "cos", "cosh", "exp", "floor", "ln", "power",
		}},
		{TypeString, []string{"concat", "length", "upper", "lower", "trim", "format"}},
		{TypeConversion, []string{"cast", "hex2dec", "dec2hex", "encode", "decode"}},
		{TypeDateTime, []string{"now", "current_time", "current_date"}},
		{TypeAggregation, []string{"sum", "avg", "min", "max", "count", "stddev", "median", "collect", "last_value", "merge_agg", "stddevs", "deduplicate", "var", "vars"}},
		{TypeWindow, []string{"row_number"}},
		{TypeAnalytical, []string{"lag", "latest", "changed_col", "had_changed"}},
	}

	for _, tt := range tests {
		t.Run(string(tt.functionType), func(t *testing.T) {
			functions := GetByType(tt.functionType)
			assert.GreaterOrEqual(t, len(functions), len(tt.functions),
				"should have at least %d functions of type %s", len(tt.functions), tt.functionType)

			// 验证特定函数存在
			functionNames := make(map[string]bool)
			for _, fn := range functions {
				functionNames[fn.GetName()] = true
			}

			for _, expectedFn := range tt.functions {
				assert.True(t, functionNames[expectedFn],
					"function %s should be of type %s", expectedFn, tt.functionType)
			}
		})
	}
}

func TestCustomFunction(t *testing.T) {
	// 注册自定义函数
	err := RegisterCustomFunction("double2", TypeCustom, "自定义函数", "将数值乘以2", 1, 1,
		func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
			val := cast.ToFloat64(args[0])
			return val * 2, nil
		})
	assert.NoError(t, err)

	// 测试自定义函数
	tests := []struct {
		name     string
		args     []interface{}
		expected interface{}
	}{
		{"double positive", []interface{}{5.0}, 10.0},
		{"double negative", []interface{}{-3.0}, -6.0},
		{"double zero", []interface{}{0}, 0.0},
		{"double string number", []interface{}{"2.5"}, 5.0},
	}

	ctx := &FunctionContext{
		Data: map[string]interface{}{},
	}

	doubleFunc, exists := Get("double2")
	assert.True(t, exists)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := doubleFunc.Execute(ctx, tt.args)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}

	// 清理
	Unregister("double2")
}

func TestComplexFunctionCombinations(t *testing.T) {
	ctx := &FunctionContext{
		Data: map[string]interface{}{},
	}

	// 测试复杂函数组合
	tests := []struct {
		name        string
		description string
		operations  func() (interface{}, error)
		expected    interface{}
	}{
		{
			name:        "abs of negative sum",
			description: "计算负数之和的绝对值",
			operations: func() (interface{}, error) {
				sumFn, _ := Get("sum")
				sum, err := sumFn.Execute(ctx, []interface{}{-1, -2, -3})
				if err != nil {
					return nil, err
				}
				absFn, _ := Get("abs")
				return absFn.Execute(ctx, []interface{}{sum})
			},
			expected: 6.0,
		},
		{
			name:        "concat and upper",
			description: "连接字符串后转大写",
			operations: func() (interface{}, error) {
				concatFn, _ := Get("concat")
				concat, err := concatFn.Execute(ctx, []interface{}{"hello", " ", "world"})
				if err != nil {
					return nil, err
				}
				upperFn, _ := Get("upper")
				return upperFn.Execute(ctx, []interface{}{concat})
			},
			expected: "HELLO WORLD",
		},
		{
			name:        "hex conversion round trip",
			description: "十进制转十六进制再转回十进制",
			operations: func() (interface{}, error) {
				dec2hexFn, _ := Get("dec2hex")
				hex, err := dec2hexFn.Execute(ctx, []interface{}{255})
				if err != nil {
					return nil, err
				}
				hex2decFn, _ := Get("hex2dec")
				return hex2decFn.Execute(ctx, []interface{}{hex})
			},
			expected: int64(255),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.operations()
			assert.NoError(t, err, tt.description)
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}
