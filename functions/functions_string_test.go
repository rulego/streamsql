package functions

import (
	"testing"
)

func TestNewStringFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		// endswith tests
		{"endswith_true", "endswith", []interface{}{"hello world", "world"}, true, false},
		{"endswith_false", "endswith", []interface{}{"hello world", "hello"}, false, false},
		{"endswith_empty", "endswith", []interface{}{"hello", ""}, true, false},

		// startswith tests
		{"startswith_true", "startswith", []interface{}{"hello world", "hello"}, true, false},
		{"startswith_false", "startswith", []interface{}{"hello world", "world"}, false, false},
		{"startswith_empty", "startswith", []interface{}{"hello", ""}, true, false},

		// indexof tests
		{"indexof_found", "indexof", []interface{}{"hello world", "world"}, int64(6), false},
		{"indexof_not_found", "indexof", []interface{}{"hello world", "xyz"}, int64(-1), false},
		{"indexof_first_char", "indexof", []interface{}{"hello", "h"}, int64(0), false},

		// substring tests
		{"substring_start_only", "substring", []interface{}{"hello world", int64(6)}, "world", false},
		{"substring_start_length", "substring", []interface{}{"hello world", int64(0), int64(5)}, "hello", false},
		{"substring_out_of_bounds", "substring", []interface{}{"hello", int64(10)}, "", false},

		// replace tests
		{"replace_simple", "replace", []interface{}{"hello world", "world", "Go"}, "hello Go", false},
		{"replace_multiple", "replace", []interface{}{"hello hello", "hello", "hi"}, "hi hi", false},
		{"replace_not_found", "replace", []interface{}{"hello world", "xyz", "abc"}, "hello world", false},

		// split tests
		{"split_comma", "split", []interface{}{"a,b,c", ","}, []string{"a", "b", "c"}, false},
		{"split_space", "split", []interface{}{"hello world", " "}, []string{"hello", "world"}, false},
		{"split_not_found", "split", []interface{}{"hello", ","}, []string{"hello"}, false},

		// lpad tests
		{"lpad_default", "lpad", []interface{}{"hello", int64(10)}, "     hello", false},
		{"lpad_custom", "lpad", []interface{}{"hello", int64(8), "*"}, "***hello", false},
		{"lpad_no_padding", "lpad", []interface{}{"hello", int64(3)}, "hello", false},

		// rpad tests
		{"rpad_default", "rpad", []interface{}{"hello", int64(10)}, "hello     ", false},
		{"rpad_custom", "rpad", []interface{}{"hello", int64(8), "*"}, "hello***", false},
		{"rpad_no_padding", "rpad", []interface{}{"hello", int64(3)}, "hello", false},

		// ltrim tests
		{"ltrim_spaces", "ltrim", []interface{}{"   hello world   "}, "hello world   ", false},
		{"ltrim_tabs", "ltrim", []interface{}{"\t\nhello"}, "hello", false},
		{"ltrim_no_whitespace", "ltrim", []interface{}{"hello"}, "hello", false},

		// rtrim tests
		{"rtrim_spaces", "rtrim", []interface{}{"   hello world   "}, "   hello world", false},
		{"rtrim_tabs", "rtrim", []interface{}{"hello\t\n"}, "hello", false},
		{"rtrim_no_whitespace", "rtrim", []interface{}{"hello"}, "hello", false},

		// regexp_matches tests
		{"regexp_matches_true", "regexp_matches", []interface{}{"hello123", "[0-9]+"}, true, false},
		{"regexp_matches_false", "regexp_matches", []interface{}{"hello", "[0-9]+"}, false, false},
		{"regexp_matches_email", "regexp_matches", []interface{}{"test@example.com", "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"}, true, false},

		// regexp_replace tests
		{"regexp_replace_digits", "regexp_replace", []interface{}{"hello123world456", "[0-9]+", "X"}, "helloXworldX", false},
		{"regexp_replace_no_match", "regexp_replace", []interface{}{"hello", "[0-9]+", "X"}, "hello", false},

		// regexp_substring tests
		{"regexp_substring_found", "regexp_substring", []interface{}{"hello123world", "[0-9]+"}, "123", false},
		{"regexp_substring_not_found", "regexp_substring", []interface{}{"hello", "[0-9]+"}, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.funcName)
			if !exists {
				t.Fatalf("Function %s not found", tt.funcName)
			}
			// 验证参数
			if err := fn.Validate(tt.args); err != nil {
				t.Errorf("Validate() error = %v", err)
				return
			}
			ctx := &FunctionContext{}
			result, err := fn.Execute(ctx, tt.args)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error for %s, got nil", tt.name)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error for %s: %v", tt.name, err)
				return
			}

			// 特殊处理 split 函数的结果比较
			if tt.funcName == "split" {
				expectedSlice, ok := tt.expected.([]string)
				if !ok {
					t.Errorf("Expected slice for split function")
					return
				}
				resultSlice, ok := result.([]string)
				if !ok {
					t.Errorf("Result is not a slice for split function")
					return
				}
				if len(expectedSlice) != len(resultSlice) {
					t.Errorf("Expected %v, got %v for %s", expectedSlice, resultSlice, tt.name)
					return
				}
				for i, v := range expectedSlice {
					if v != resultSlice[i] {
						t.Errorf("Expected %v, got %v for %s", expectedSlice, resultSlice, tt.name)
						return
					}
				}
			} else {
				if result != tt.expected {
					t.Errorf("Expected %v, got %v for %s", tt.expected, result, tt.name)
				}
			}
		})
	}
}

// TestStringFunctionValidation 测试字符串函数的参数验证
func TestStringFunctionValidation(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "concat no args",
			function: NewConcatFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "concat valid args",
			function: NewConcatFunction(),
			args:     []interface{}{"hello", "world"},
			wantErr:  false,
		},
		{
			name:     "length no args",
			function: NewLengthFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "length too many args",
			function: NewLengthFunction(),
			args:     []interface{}{"hello", "world"},
			wantErr:  true,
		},
		{
			name:     "length valid args",
			function: NewLengthFunction(),
			args:     []interface{}{"hello"},
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

// TestStringFunctionErrors 测试字符串函数的错误处理
func TestStringFunctionErrors(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "concat non-string input",
			function: NewConcatFunction(),
			args:     []interface{}{123, 456},
			wantErr:  false,
		},
		{
			name:     "length non-string input",
			function: NewLengthFunction(),
			args:     []interface{}{123},
			wantErr:  false,
		},
		{
			name:     "upper non-string input",
			function: NewUpperFunction(),
			args:     []interface{}{123},
			wantErr:  false,
		},
		{
			name:     "endswith non-string input",
			function: NewEndswithFunction(),
			args:     []interface{}{123, "3"},
			wantErr:  false,
		},
		{
			name:     "substring non-string input",
			function: NewSubstringFunction(),
			args:     []interface{}{123, 1, 2},
			wantErr:  false,
		},
		{
			name:     "substring non-numeric start",
			function: NewSubstringFunction(),
			args:     []interface{}{"hello", "world"},
			wantErr:  true,
		},
		{
			name:     "replace non-string input",
			function: NewReplaceFunction(),
			args:     []interface{}{123, "2", "X"},
			wantErr:  false,
		},
		{
			name:     "regexp_matches invalid pattern",
			function: NewRegexpMatchesFunction(),
			args:     []interface{}{"hello", "[invalid"},
			wantErr:  true,
		},
		{
			name:     "regexp_replace invalid pattern",
			function: NewRegexpReplaceFunction(),
			args:     []interface{}{"hello", "[invalid", "x"},
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

// TestStringFunctionEdgeCases 测试字符串函数的边界情况
func TestStringFunctionEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "concat empty strings",
			function: NewConcatFunction(),
			args:     []interface{}{"", ""},
			expected: "",
			wantErr:  false,
		},
		{
			name:     "length empty string",
			function: NewLengthFunction(),
			args:     []interface{}{""},
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "upper empty string",
			function: NewUpperFunction(),
			args:     []interface{}{""},
			expected: "",
			wantErr:  false,
		},
		{
			name:     "lower empty string",
			function: NewLowerFunction(),
			args:     []interface{}{""},
			expected: "",
			wantErr:  false,
		},
		{
			name:     "trim empty string",
			function: NewTrimFunction(),
			args:     []interface{}{""},
			expected: "",
			wantErr:  false,
		},
		{
			name:     "substring negative start",
			function: NewSubstringFunction(),
			args:     []interface{}{"hello", -1, 5},
			expected: "o",
			wantErr:  false,
		},
		{
			name:     "lpad zero length",
			function: NewLpadFunction(),
			args:     []interface{}{"hello", 0},
			expected: "hello",
			wantErr:  false,
		},
		{
			name:     "split empty delimiter",
			function: NewSplitFunction(),
			args:     []interface{}{"hello", ""},
			expected: []string{"h", "e", "l", "l", "o"},
			wantErr:  false,
		},
		// 新增测试用例
		{
			name:     "length array",
			function: NewLengthFunction(),
			args:     []interface{}{[]string{"a", "b", "c"}},
			expected: 3,
			wantErr:  false,
		},
		{
			name:     "length map",
			function: NewLengthFunction(),
			args:     []interface{}{map[string]int{"a": 1, "b": 2}},
			expected: 2,
			wantErr:  false,
		},

		{
			name:     "lpad custom char",
			function: NewLpadFunction(),
			args:     []interface{}{"test", int64(8), "*"},
			expected: "****test",
			wantErr:  false,
		},
		{
			name:     "rpad custom char",
			function: NewRpadFunction(),
			args:     []interface{}{"test", int64(8), "*"},
			expected: "test****",
			wantErr:  false,
		},
		{
			name:     "regexp_matches invalid pattern",
			function: NewRegexpMatchesFunction(),
			args:     []interface{}{"hello", "["},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "regexp_replace invalid pattern",
			function: NewRegexpReplaceFunction(),
			args:     []interface{}{"hello", "[", "x"},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "regexp_substring invalid pattern",
			function: NewRegexpSubstringFunction(),
			args:     []interface{}{"hello", "["},
			expected: nil,
			wantErr:  true,
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
				// 特殊处理 split 函数的结果比较
				if tt.name == "split empty delimiter" {
					expectedSlice, ok := tt.expected.([]string)
					if !ok {
						t.Errorf("Expected result is not []string")
						return
					}
					// split函数返回的是[]string类型
					actualSlice, ok := result.([]string)
					if !ok {
						t.Errorf("Actual result is not []string")
						return
					}
					if len(expectedSlice) != len(actualSlice) {
						t.Errorf("Execute() = %v, want %v", result, tt.expected)
						return
					}
					for i, expected := range expectedSlice {
						if actualSlice[i] != expected {
							t.Errorf("Execute() = %v, want %v", result, tt.expected)
							return
						}
					}
				} else if result != tt.expected {
					t.Errorf("Execute() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}
