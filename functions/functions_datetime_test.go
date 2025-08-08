package functions

import (
	"testing"
)

func TestDateTimeFunctions(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		// DateFormatFunction 测试
		{
			name:     "date_format basic",
			function: NewDateFormatFunction(),
			args:     []interface{}{"2023-12-25 15:30:45", "YYYY-MM-DD HH:MI:SS"},
			expected: "2023-12-25 15:30:45",
			wantErr:  false,
		},
		{
			name:     "date_format custom",
			function: NewDateFormatFunction(),
			args:     []interface{}{"2023-12-25 15:30:45", "YYYY/MM/DD"},
			expected: "2023/12/25",
			wantErr:  false,
		},
		// DateAddFunction 测试
		{
			name:     "date_add days",
			function: NewDateAddFunction(),
			args:     []interface{}{"2023-12-25", 7, "days"},
			expected: "2024-01-01 00:00:00",
			wantErr:  false,
		},
		{
			name:     "date_add months",
			function: NewDateAddFunction(),
			args:     []interface{}{"2023-12-25", 1, "months"},
			expected: "2024-01-25 00:00:00",
			wantErr:  false,
		},
		// DateSubFunction 测试
		{
			name:     "date_sub days",
			function: NewDateSubFunction(),
			args:     []interface{}{"2024-01-01", 7, "days"},
			expected: "2023-12-25 00:00:00",
			wantErr:  false,
		},
		// DateDiffFunction 测试
		{
			name:     "date_diff days",
			function: NewDateDiffFunction(),
			args:     []interface{}{"2024-01-01", "2023-12-25", "days"},
			expected: int64(7),
			wantErr:  false,
		},
		// YearFunction 测试
		{
			name:     "year extraction",
			function: NewYearFunction(),
			args:     []interface{}{"2023-12-25 15:30:45"},
			expected: float64(2023),
			wantErr:  false,
		},
		// MonthFunction 测试
		{
			name:     "month extraction",
			function: NewMonthFunction(),
			args:     []interface{}{"2023-12-25 15:30:45"},
			expected: float64(12),
			wantErr:  false,
		},
		// DayFunction 测试
		{
			name:     "day extraction",
			function: NewDayFunction(),
			args:     []interface{}{"2023-12-25 15:30:45"},
			expected: 25,
			wantErr:  false,
		},
		// HourFunction 测试
		{
			name:     "hour extraction",
			function: NewHourFunction(),
			args:     []interface{}{"2023-12-25 15:30:45"},
			expected: 15,
			wantErr:  false,
		},
		// MinuteFunction 测试
		{
			name:     "minute extraction",
			function: NewMinuteFunction(),
			args:     []interface{}{"2023-12-25 15:30:45"},
			expected: 30,
			wantErr:  false,
		},
		// SecondFunction 测试
		{
			name:     "second extraction",
			function: NewSecondFunction(),
			args:     []interface{}{"2023-12-25 15:30:45"},
			expected: 45,
			wantErr:  false,
		},
		// UnixTimestampFunction 测试
		{
			name:     "unix_timestamp",
			function: NewUnixTimestampFunction(),
			args:     []interface{}{"2023-01-01 00:00:00"},
			expected: int64(1672531200),
			wantErr:  false,
		},
		// FromUnixtimeFunction 测试
		{
			name:     "from_unixtime",
			function: NewFromUnixtimeFunction(),
			args:     []interface{}{1672531200},
			expected: "2023-01-01 00:00:00",
			wantErr:  false,
		},
		// ExtractFunction 测试
		{
			name:     "extract year",
			function: NewExtractFunction(),
			args:     []interface{}{"year", "2023-12-25 15:30:45"},
			expected: 2023,
			wantErr:  false,
		},
		{
			name:     "extract month",
			function: NewExtractFunction(),
			args:     []interface{}{"month", "2023-12-25 15:30:45"},
			expected: 12,
			wantErr:  false,
		},
		// DayOfWeekFunction 测试
		{
			name:     "dayofweek",
			function: NewDayOfWeekFunction(),
			args:     []interface{}{"2023-12-25"},
			expected: 1, // Monday
			wantErr:  false,
		},
		// DateParseFunction 测试
		{
			name:     "date_parse",
			function: NewDateParseFunction(),
			args:     []interface{}{"2023/12/25", "YYYY/MM/DD"},
			expected: "2023-12-25 00:00:00",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 验证参数
			if err := tt.function.Validate(tt.args); err != nil {
				if !tt.wantErr {
					t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			// 执行函数
			result, err := tt.function.Execute(nil, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && result != tt.expected {
				t.Errorf("Execute() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestDateTimeFunctionValidation 测试日期时间函数的参数验证
func TestDateTimeFunctionValidation(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "now no args",
			function: NewNowFunction(),
			args:     []interface{}{},
			wantErr:  false,
		},
		{
			name:     "now too many args",
			function: NewNowFunction(),
			args:     []interface{}{"extra"},
			wantErr:  true,
		},
		{
			name:     "current_time no args",
			function: NewCurrentTimeFunction(),
			args:     []interface{}{},
			wantErr:  false,
		},
		{
			name:     "current_date no args",
			function: NewCurrentDateFunction(),
			args:     []interface{}{},
			wantErr:  false,
		},
		{
			name:     "date_format no args",
			function: NewDateFormatFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "date_format one arg",
			function: NewDateFormatFunction(),
			args:     []interface{}{"2023-12-25"},
			wantErr:  true,
		},
		{
			name:     "date_format valid args",
			function: NewDateFormatFunction(),
			args:     []interface{}{"2023-12-25", "YYYY-MM-DD"},
			wantErr:  false,
		},
		{
			name:     "date_add no args",
			function: NewDateAddFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "date_add two args",
			function: NewDateAddFunction(),
			args:     []interface{}{"2023-12-25", 7},
			wantErr:  true,
		},
		{
			name:     "date_add valid args",
			function: NewDateAddFunction(),
			args:     []interface{}{"2023-12-25", 7, "days"},
			wantErr:  false,
		},
		{
			name:     "year no args",
			function: NewYearFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "year valid args",
			function: NewYearFunction(),
			args:     []interface{}{"2023-12-25"},
			wantErr:  false,
		},
		{
			name:     "extract no args",
			function: NewExtractFunction(),
			args:     []interface{}{},
			wantErr:  true,
		},
		{
			name:     "extract one arg",
			function: NewExtractFunction(),
			args:     []interface{}{"year"},
			wantErr:  true,
		},
		{
			name:     "extract valid args",
			function: NewExtractFunction(),
			args:     []interface{}{"year", "2023-12-25"},
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

// TestDateTimeFunctionErrors 测试日期时间函数的错误处理
func TestDateTimeFunctionErrors(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		wantErr  bool
	}{
		{
			name:     "date_format invalid date",
			function: NewDateFormatFunction(),
			args:     []interface{}{"invalid-date", "YYYY-MM-DD"},
			wantErr:  true,
		},
		{
			name:     "date_add invalid date",
			function: NewDateAddFunction(),
			args:     []interface{}{"invalid-date", 7, "days"},
			wantErr:  true,
		},
		{
			name:     "date_add invalid unit",
			function: NewDateAddFunction(),
			args:     []interface{}{"2023-12-25", 7, "invalid-unit"},
			wantErr:  true,
		},
		{
			name:     "year invalid date",
			function: NewYearFunction(),
			args:     []interface{}{"invalid-date"},
			wantErr:  true,
		},
		{
			name:     "extract invalid unit",
			function: NewExtractFunction(),
			args:     []interface{}{"invalid-unit", "2023-12-25"},
			wantErr:  true,
		},
		{
			name:     "extract invalid date",
			function: NewExtractFunction(),
			args:     []interface{}{"year", "invalid-date"},
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

// TestDateTimeFunctionEdgeCases 测试日期时间函数的边界情况
func TestDateTimeFunctionEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "now function",
			function: NewNowFunction(),
			args:     []interface{}{},
			expected: nil, // 不检查具体值，只检查不出错
			wantErr:  false,
		},
		{
			name:     "current_time function",
			function: NewCurrentTimeFunction(),
			args:     []interface{}{},
			expected: nil, // 不检查具体值，只检查不出错
			wantErr:  false,
		},
		{
			name:     "current_date function",
			function: NewCurrentDateFunction(),
			args:     []interface{}{},
			expected: nil, // 不检查具体值，只检查不出错
			wantErr:  false,
		},
		{
			name:     "unix_timestamp with valid date",
			function: NewUnixTimestampFunction(),
			args:     []interface{}{"2023-01-01 00:00:00"},
			expected: nil, // 不检查具体值，只检查不出错
			wantErr:  false,
		},
		// 新增边界情况测试
		{
			name:     "date_format empty string",
			function: NewDateFormatFunction(),
			args:     []interface{}{"", "YYYY-MM-DD"},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "date_add zero days",
			function: NewDateAddFunction(),
			args:     []interface{}{"2023-12-25", 0, "days"},
			expected: "2023-12-25 00:00:00",
			wantErr:  false,
		},
		{
			name:     "date_diff same date",
			function: NewDateDiffFunction(),
			args:     []interface{}{"2023-12-25", "2023-12-25", "days"},
			expected: int64(0),
			wantErr:  false,
		},
		{
			name:     "dayofyear function",
			function: NewDayOfYearFunction(),
			args:     []interface{}{"2023-12-25"},
			expected: 359,
			wantErr:  false,
		},
		{
			name:     "weekofyear function",
			function: NewWeekOfYearFunction(),
			args:     []interface{}{"2023-12-25"},
			expected: 52,
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
			if !tt.wantErr && tt.expected != nil && result != tt.expected {
				t.Errorf("Execute() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDateTimeRegistration(t *testing.T) {
	// 测试函数是否正确注册
	dateTimeFunctions := []string{
		"date_format",
		"date_add",
		"date_sub",
		"date_diff",
		"date_parse",
		"extract",
		"unix_timestamp",
		"from_unixtime",
		"year",
		"month",
		"day",
		"hour",
		"minute",
		"second",
		"dayofweek",
		"dayofyear",
		"weekofyear",
		"now",
		"current_time",
		"current_date",
	}

	for _, funcName := range dateTimeFunctions {
		t.Run("register_"+funcName, func(t *testing.T) {
			func_, exists := Get(funcName)
			if !exists {
				t.Errorf("Function %s not registered", funcName)
				return
			}
			if func_ == nil {
				t.Errorf("Function %s is nil", funcName)
			}
		})
	}
}

func TestDateFormatConversion(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"YYYY-MM-DD", "2006-01-02"},
		{"yyyy/mm/dd", "2006/01/02"},
		{"DD/MM/YYYY", "02/01/2006"},
		{"HH:MI:SS", "15:04:05"},
		{"YYYY-MM-DD HH:MI:SS", "2006-01-02 15:04:05"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := convertToGoFormat(tt.input)
			if result != tt.expected {
				t.Errorf("convertToGoFormat(%s) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}
