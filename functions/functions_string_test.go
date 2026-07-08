package functions

import (
	"testing"
)

func TestNewStringFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []any
		expected any
		wantErr  bool
	}{
		// endswith tests
		{"endswith_true", "endswith", []any{"hello world", "world"}, true, false},
		{"endswith_false", "endswith", []any{"hello world", "hello"}, false, false},
		{"endswith_empty", "endswith", []any{"hello", ""}, true, false},

		// startswith tests
		{"startswith_true", "startswith", []any{"hello world", "hello"}, true, false},
		{"startswith_false", "startswith", []any{"hello world", "world"}, false, false},
		{"startswith_empty", "startswith", []any{"hello", ""}, true, false},

		// indexof tests
		{"indexof_found", "indexof", []any{"hello world", "world"}, int64(6), false},
		{"indexof_not_found", "indexof", []any{"hello world", "xyz"}, int64(-1), false},
		{"indexof_first_char", "indexof", []any{"hello", "h"}, int64(0), false},

		// substring tests
		{"substring_start_only", "substring", []any{"hello world", int64(6)}, "world", false},
		{"substring_start_length", "substring", []any{"hello world", int64(0), int64(5)}, "hello", false},
		{"substring_out_of_bounds", "substring", []any{"hello", int64(10)}, "", false},

		// replace tests
		{"replace_simple", "replace", []any{"hello world", "world", "Go"}, "hello Go", false},
		{"replace_multiple", "replace", []any{"hello hello", "hello", "hi"}, "hi hi", false},
		{"replace_not_found", "replace", []any{"hello world", "xyz", "abc"}, "hello world", false},

		// split tests
		{"split_comma", "split", []any{"a,b,c", ","}, []string{"a", "b", "c"}, false},
		{"split_space", "split", []any{"hello world", " "}, []string{"hello", "world"}, false},
		{"split_not_found", "split", []any{"hello", ","}, []string{"hello"}, false},

		// lpad tests
		{"lpad_default", "lpad", []any{"hello", int64(10)}, "     hello", false},
		{"lpad_custom", "lpad", []any{"hello", int64(8), "*"}, "***hello", false},
		{"lpad_no_padding", "lpad", []any{"hello", int64(3)}, "hello", false},

		// rpad tests
		{"rpad_default", "rpad", []any{"hello", int64(10)}, "hello     ", false},
		{"rpad_custom", "rpad", []any{"hello", int64(8), "*"}, "hello***", false},
		{"rpad_no_padding", "rpad", []any{"hello", int64(3)}, "hello", false},

		// ltrim tests
		{"ltrim_spaces", "ltrim", []any{"   hello world   "}, "hello world   ", false},
		{"ltrim_tabs", "ltrim", []any{"\t\nhello"}, "hello", false},
		{"ltrim_no_whitespace", "ltrim", []any{"hello"}, "hello", false},

		// rtrim tests
		{"rtrim_spaces", "rtrim", []any{"   hello world   "}, "   hello world", false},
		{"rtrim_tabs", "rtrim", []any{"hello\t\n"}, "hello", false},
		{"rtrim_no_whitespace", "rtrim", []any{"hello"}, "hello", false},

		// regexp_matches tests
		{"regexp_matches_true", "regexp_matches", []any{"hello123", "[0-9]+"}, true, false},
		{"regexp_matches_false", "regexp_matches", []any{"hello", "[0-9]+"}, false, false},
		{"regexp_matches_email", "regexp_matches", []any{"test@example.com", "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"}, true, false},

		// regexp_replace tests
		{"regexp_replace_digits", "regexp_replace", []any{"hello123world456", "[0-9]+", "X"}, "helloXworldX", false},
		{"regexp_replace_no_match", "regexp_replace", []any{"hello", "[0-9]+", "X"}, "hello", false},

		// regexp_substring tests
		{"regexp_substring_found", "regexp_substring", []any{"hello123world", "[0-9]+"}, "123", false},
		{"regexp_substring_not_found", "regexp_substring", []any{"hello", "[0-9]+"}, "", false},
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
		args     []any
		wantErr  bool
	}{
		{
			name:     "concat no args",
			function: NewConcatFunction(),
			args:     []any{},
			wantErr:  true,
		},
		{
			name:     "concat valid args",
			function: NewConcatFunction(),
			args:     []any{"hello", "world"},
			wantErr:  false,
		},
		{
			name:     "length no args",
			function: NewLengthFunction(),
			args:     []any{},
			wantErr:  true,
		},
		{
			name:     "length too many args",
			function: NewLengthFunction(),
			args:     []any{"hello", "world"},
			wantErr:  true,
		},
		{
			name:     "length valid args",
			function: NewLengthFunction(),
			args:     []any{"hello"},
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
		args     []any
		wantErr  bool
	}{
		{
			name:     "concat non-string input",
			function: NewConcatFunction(),
			args:     []any{123, 456},
			wantErr:  false,
		},
		{
			name:     "length non-string input",
			function: NewLengthFunction(),
			args:     []any{123},
			wantErr:  false,
		},
		{
			name:     "upper non-string input",
			function: NewUpperFunction(),
			args:     []any{123},
			wantErr:  false,
		},
		{
			name:     "endswith non-string input",
			function: NewEndswithFunction(),
			args:     []any{123, "3"},
			wantErr:  false,
		},
		{
			name:     "substring non-string input",
			function: NewSubstringFunction(),
			args:     []any{123, 1, 2},
			wantErr:  false,
		},
		{
			name:     "substring non-numeric start",
			function: NewSubstringFunction(),
			args:     []any{"hello", "world"},
			wantErr:  true,
		},
		{
			name:     "replace non-string input",
			function: NewReplaceFunction(),
			args:     []any{123, "2", "X"},
			wantErr:  false,
		},
		{
			name:     "regexp_matches invalid pattern",
			function: NewRegexpMatchesFunction(),
			args:     []any{"hello", "[invalid"},
			wantErr:  true,
		},
		{
			name:     "regexp_replace invalid pattern",
			function: NewRegexpReplaceFunction(),
			args:     []any{"hello", "[invalid", "x"},
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
		args     []any
		expected any
		wantErr  bool
	}{
		{
			name:     "concat empty strings",
			function: NewConcatFunction(),
			args:     []any{"", ""},
			expected: "",
			wantErr:  false,
		},
		{
			name:     "length empty string",
			function: NewLengthFunction(),
			args:     []any{""},
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "upper empty string",
			function: NewUpperFunction(),
			args:     []any{""},
			expected: "",
			wantErr:  false,
		},
		{
			name:     "lower empty string",
			function: NewLowerFunction(),
			args:     []any{""},
			expected: "",
			wantErr:  false,
		},
		{
			name:     "trim empty string",
			function: NewTrimFunction(),
			args:     []any{""},
			expected: "",
			wantErr:  false,
		},
		{
			name:     "substring negative start",
			function: NewSubstringFunction(),
			args:     []any{"hello", -1, 5},
			expected: "o",
			wantErr:  false,
		},
		{
			name:     "lpad zero length",
			function: NewLpadFunction(),
			args:     []any{"hello", 0},
			expected: "hello",
			wantErr:  false,
		},
		{
			name:     "split empty delimiter",
			function: NewSplitFunction(),
			args:     []any{"hello", ""},
			expected: []string{"h", "e", "l", "l", "o"},
			wantErr:  false,
		},
		// 新增测试用例
		{
			name:     "length array",
			function: NewLengthFunction(),
			args:     []any{[]string{"a", "b", "c"}},
			expected: 3,
			wantErr:  false,
		},
		{
			name:     "length map",
			function: NewLengthFunction(),
			args:     []any{map[string]int{"a": 1, "b": 2}},
			expected: 2,
			wantErr:  false,
		},

		{
			name:     "lpad custom char",
			function: NewLpadFunction(),
			args:     []any{"test", int64(8), "*"},
			expected: "****test",
			wantErr:  false,
		},
		{
			name:     "rpad custom char",
			function: NewRpadFunction(),
			args:     []any{"test", int64(8), "*"},
			expected: "test****",
			wantErr:  false,
		},
		{
			name:     "regexp_matches invalid pattern",
			function: NewRegexpMatchesFunction(),
			args:     []any{"hello", "["},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "regexp_replace invalid pattern",
			function: NewRegexpReplaceFunction(),
			args:     []any{"hello", "[", "x"},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "regexp_substring invalid pattern",
			function: NewRegexpSubstringFunction(),
			args:     []any{"hello", "["},
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
