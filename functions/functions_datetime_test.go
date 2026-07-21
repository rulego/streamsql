package functions

import (
	"testing"
	"time"
)

func TestDateTimeFunctions(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []any
		expected any
		wantErr  bool
	}{
		// DateFormatFunction 测试
		{
			name:     "date_format basic",
			function: NewDateFormatFunction(),
			args:     []any{"2025-08-25 15:30:45", "YYYY-MM-DD HH:MI:SS"},
			expected: "2025-08-25 15:30:45",
			wantErr:  false,
		},
		{
			name:     "date_format custom",
			function: NewDateFormatFunction(),
			args:     []any{"2025-08-25 15:30:45", "YYYY/MM/DD"},
			expected: "2025/08/25",
			wantErr:  false,
		},
		// DateAddFunction 测试
		{
			name:     "date_add years",
			function: NewDateAddFunction(),
			args:     []any{"2025-08-25", 1, "year"},
			expected: "2026-08-25 00:00:00",
			wantErr:  false,
		},
		{
			name:     "date_add months",
			function: NewDateAddFunction(),
			args:     []any{"2025-08-25", 1, "months"},
			expected: "2025-09-25 00:00:00",
			wantErr:  false,
		},
		{
			name:     "date_add days",
			function: NewDateAddFunction(),
			args:     []any{"2025-08-25", 7, "days"},
			expected: "2025-09-01 00:00:00",
			wantErr:  false,
		},
		{
			name:     "date_add hours",
			function: NewDateAddFunction(),
			args:     []any{"2025-08-25 15:04:05", 1, "hours"},
			expected: "2025-08-25 16:04:05",
			wantErr:  false,
		},
		{
			name:     "date_add minutes",
			function: NewDateAddFunction(),
			args:     []any{"2025-08-25 15:04:05", 20, "minutes"},
			expected: "2025-08-25 15:24:05",
			wantErr:  false,
		},
		{
			name:     "date_add seconds",
			function: NewDateAddFunction(),
			args:     []any{"2025-08-25 15:04:05", 20, "seconds"},
			expected: "2025-08-25 15:04:25",
			wantErr:  false,
		},
		{
			name:     "date_add with unit invalid",
			function: NewDateAddFunction(),
			args:     []any{"2025-08-25", 1, "invalid"},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "date_add with interval invalid",
			function: NewDateAddFunction(),
			args:     []any{"2025-08-25", "invalid", "months"},
			expected: nil,
			wantErr:  true,
		},
		// DateSubFunction 测试
		{
			name:     "date_sub years",
			function: NewDateSubFunction(),
			args:     []any{"2025-08-25", 1, "year"},
			expected: "2024-08-25 00:00:00",
			wantErr:  false,
		},
		{
			name:     "date_sub months",
			function: NewDateSubFunction(),
			args:     []any{"2025-08-25", 1, "months"},
			expected: "2025-07-25 00:00:00",
			wantErr:  false,
		},
		{
			name:     "date_sub days",
			function: NewDateSubFunction(),
			args:     []any{"2025-09-01", 7, "days"},
			expected: "2025-08-25 00:00:00",
			wantErr:  false,
		},
		{
			name:     "date_sub hours",
			function: NewDateSubFunction(),
			args:     []any{"2025-08-25 15:04:05", 1, "hours"},
			expected: "2025-08-25 14:04:05",
			wantErr:  false,
		},
		{
			name:     "date_sub minutes",
			function: NewDateSubFunction(),
			args:     []any{"2025-08-25 15:04:05", 20, "minutes"},
			expected: "2025-08-25 14:44:05",
			wantErr:  false,
		},
		{
			name:     "date_sub seconds",
			function: NewDateSubFunction(),
			args:     []any{"2025-08-25 15:04:05", 20, "seconds"},
			expected: "2025-08-25 15:03:45",
			wantErr:  false,
		},
		{
			name:     "date_sub with unit invalid",
			function: NewDateSubFunction(),
			args:     []any{"2025-08-25", 1, "invalid"},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "date_sub with interval invalid",
			function: NewDateSubFunction(),
			args:     []any{"2025-08-25", "invalid", "months"},
			expected: nil,
			wantErr:  true,
		},
		// DateDiffFunction 测试
		{
			name:     "date_diff years",
			function: NewDateDiffFunction(),
			args:     []any{"2025-09-01", "2024-08-25", "years"},
			expected: int64(1),
			wantErr:  false,
		},
		{
			name:     "date_diff months",
			function: NewDateDiffFunction(),
			args:     []any{"2025-10-01", "2025-08-25", "months"},
			expected: int64(1),
			wantErr:  false,
		},
		{
			name:     "date_diff days",
			function: NewDateDiffFunction(),
			args:     []any{"2025-09-01", "2025-08-25", "days"},
			expected: int64(7),
			wantErr:  false,
		},
		{
			name:     "date_diff hours",
			function: NewDateDiffFunction(),
			args:     []any{"2025-08-25 17:04:05", "2025-08-25 15:04:05", "hours"},
			expected: int64(2),
			wantErr:  false,
		},
		{
			name:     "date_diff minutes",
			function: NewDateDiffFunction(),
			args:     []any{"2025-08-25 15:09:05", "2025-08-25 15:04:05", "minutes"},
			expected: int64(5),
			wantErr:  false,
		},
		{
			name:     "date_diff seconds",
			function: NewDateDiffFunction(),
			args:     []any{"2025-08-25 15:04:35", "2025-08-25 15:04:05", "seconds"},
			expected: int64(30),
			wantErr:  false,
		},
		{
			name:     "date_diff seconds with args0 invalid",
			function: NewDateDiffFunction(),
			args:     []any{"invalid", "2025-08-25 15:04:05", "seconds"},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "date_diff seconds with args1 invalid",
			function: NewDateDiffFunction(),
			args:     []any{"2025-08-25 15:04:05", "invalid", "seconds"},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "date_diff seconds with unit invalid",
			function: NewDateDiffFunction(),
			args:     []any{"invalid", "2025-08-25 15:04:05", "invalid"},
			expected: nil,
			wantErr:  true,
		},
		// YearFunction 测试
		{
			name:     "year extraction",
			function: NewYearFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 2025,
			wantErr:  false,
		},
		{
			name:     "year extraction with YYYY-MM-dd",
			function: NewYearFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 2025,
			wantErr:  false,
		},
		// MonthFunction 测试
		{
			name:     "month extraction",
			function: NewMonthFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 8,
			wantErr:  false,
		},
		{
			name:     "month extraction with YYYY-MM-dd",
			function: NewMonthFunction(),
			args:     []any{"2025-08-25"},
			expected: 8,
			wantErr:  false,
		},
		{
			name:     "month extraction with invalid",
			function: NewMonthFunction(),
			args:     []any{"invalid"},
			expected: nil,
			wantErr:  true,
		},
		// DayFunction 测试
		{
			name:     "day extraction",
			function: NewDayFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 25,
			wantErr:  false,
		},
		{
			name:     "day extraction with YYYY-MM-dd",
			function: NewDayFunction(),
			args:     []any{"2025-08-25"},
			expected: 25,
			wantErr:  false,
		},
		{
			name:     "day extraction with invalid",
			function: NewDayFunction(),
			args:     []any{"invalid"},
			expected: nil,
			wantErr:  true,
		},
		// HourFunction 测试
		{
			name:     "hour extraction",
			function: NewHourFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 15,
			wantErr:  false,
		},
		{
			name:     "hour extraction with YYYY-MM-dd",
			function: NewHourFunction(),
			args:     []any{"2025-08-25"},
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "hour extraction with invalid",
			function: NewHourFunction(),
			args:     []any{"invalid"},
			expected: nil,
			wantErr:  true,
		},
		// MinuteFunction 测试
		{
			name:     "minute extraction",
			function: NewMinuteFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 30,
			wantErr:  false,
		},
		{
			name:     "minute extraction with YYYY-MM-dd",
			function: NewMinuteFunction(),
			args:     []any{"2025-08-25"},
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "minute extraction with invalid",
			function: NewMinuteFunction(),
			args:     []any{"invalid"},
			expected: nil,
			wantErr:  true,
		},
		// SecondFunction 测试
		{
			name:     "second extraction",
			function: NewSecondFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 45,
			wantErr:  false,
		},
		{
			name:     "second extraction with YYYY-MM-dd",
			function: NewSecondFunction(),
			args:     []any{"2025-08-25"},
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "second extraction with invalid",
			function: NewSecondFunction(),
			args:     []any{"invalid"},
			expected: nil,
			wantErr:  true,
		},
		// UnixTimestampFunction 测试
		{
			name:     "unix_timestamp",
			function: NewUnixTimestampFunction(),
			args:     []any{"2023-01-01 00:00:00"},
			expected: int64(1672531200),
			wantErr:  false,
		},
		{
			name:     "unix_timestamp_yyyy-MM-dd",
			function: NewUnixTimestampFunction(),
			args:     []any{"2023-01-01"},
			expected: int64(1672531200),
			wantErr:  false,
		},
		{
			name:     "unix_timestamp with invalid date",
			function: NewUnixTimestampFunction(),
			args:     []any{"2023-01"},
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "unix_timestamp with time.Time",
			function: NewUnixTimestampFunction(),
			args:     []any{time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)},
			expected: int64(1672531200),
			wantErr:  false,
		},
		{
			name:     "unix_timestamp with numeric",
			function: NewUnixTimestampFunction(),
			args:     []any{int64(1672531200)},
			expected: int64(1672531200),
			wantErr:  false,
		},
		// FromUnixtimeFunction 测试
		{
			name:     "from_unixtime",
			function: NewFromUnixtimeFunction(),
			args:     []any{1672531200},
			expected: "2023-01-01 00:00:00",
			wantErr:  false,
		},
		{
			name:     "from_unixtime with invalid",
			function: NewFromUnixtimeFunction(),
			args:     []any{"invalid"},
			expected: nil,
			wantErr:  true,
		},
		// ExtractFunction 测试
		{
			name:     "extract year",
			function: NewExtractFunction(),
			args:     []any{"year", "2023-12-25 15:30:45"},
			expected: 2023,
			wantErr:  false,
		},
		{
			name:     "extract month",
			function: NewExtractFunction(),
			args:     []any{"month", "2023-12-25 15:30:45"},
			expected: 12,
			wantErr:  false,
		},
		{
			name:     "extract day",
			function: NewExtractFunction(),
			args:     []any{"day", "2023-12-25 15:30:45"},
			expected: 25,
			wantErr:  false,
		},
		{
			name:     "extract hour",
			function: NewExtractFunction(),
			args:     []any{"hour", "2023-12-25 15:30:45"},
			expected: 15,
			wantErr:  false,
		},
		{
			name:     "extract minute",
			function: NewExtractFunction(),
			args:     []any{"minute", "2023-12-25 15:30:45"},
			expected: 30,
			wantErr:  false,
		},
		{
			name:     "extract second",
			function: NewExtractFunction(),
			args:     []any{"second", "2023-12-25 15:30:45"},
			expected: 45,
			wantErr:  false,
		},
		{
			name:     "extract weekday",
			function: NewExtractFunction(),
			args:     []any{"second", "2025-08-25 15:30:45"},
			expected: 45,
			wantErr:  false,
		},
		{
			name:     "extract yearday",
			function: NewExtractFunction(),
			args:     []any{"yearday", "2025-08-25 15:30:45"},
			expected: 237,
			wantErr:  false,
		},
		{
			name:     "extract unit error",
			function: NewExtractFunction(),
			args:     []any{"abc", "2025-08-25 15:30:45"},
			expected: 0,
			wantErr:  true,
		},
		// DayOfWeekFunction 测试
		{
			name:     "dayofweek",
			function: NewDayOfWeekFunction(),
			args:     []any{"2023-12-25 15:04:05"},
			expected: 1, // Monday
			wantErr:  false,
		},
		{
			name:     "dayofweek with YYYY-MM-DD",
			function: NewDayOfWeekFunction(),
			args:     []any{"2023-12-25"},
			expected: 1, // Monday
			wantErr:  false,
		},
		// DayOfYearFunction 测试
		{
			name:     "dayofyear",
			function: NewDayOfYearFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 237,
			wantErr:  false,
		},
		{
			name:     "dayofweek with YYYY-MM-DD",
			function: NewDayOfYearFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 237,
			wantErr:  false,
		},
		// WeekOfYearFunction 测试
		{
			name:     "weekofyear",
			function: NewWeekOfYearFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 35,
			wantErr:  false,
		},
		{
			name:     "weekofyear with YYYY-MM-DD",
			function: NewWeekOfYearFunction(),
			args:     []any{"2025-08-25 15:30:45"},
			expected: 35,
			wantErr:  false,
		},
		// DateParseFunction 测试
		{
			name:     "date_parse",
			function: NewDateParseFunction(),
			args:     []any{"2023/12/25", "YYYY/MM/DD"},
			expected: "2023-12-25 00:00:00",
			wantErr:  false,
		},
		{
			name:     "date_parse with invalid",
			function: NewDateParseFunction(),
			args:     []any{"2023/12/25", "invalid"},
			expected: nil,
			wantErr:  true,
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

// TestUnixTimestampNoArgs 校验 unix_timestamp() 无参返回当前 Unix 秒
func TestUnixTimestampNoArgs(t *testing.T) {
	f := NewUnixTimestampFunction()
	if err := f.Validate([]any{}); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	result, err := f.Execute(nil, []any{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	sec, ok := result.(int64)
	if !ok {
		t.Fatalf("unix_timestamp() should return int64, got %T", result)
	}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	if sec < base {
		t.Fatalf("unix_timestamp() = %d, should be after 2024-01-01", sec)
	}
}

// TestDateTimeFunctionValidation 测试日期时间函数的参数验证
func TestDateTimeFunctionValidation(t *testing.T) {
	tests := []struct {
		name     string
		function Function
		args     []any
		wantErr  bool
	}{
		{
			name:     "too many args",
			function: NewNowFunction(),
			args:     []any{"extra"},
			wantErr:  true,
		},
		{
			name:     "date_format no args",
			function: NewDateFormatFunction(),
			args:     []any{},
			wantErr:  true,
		},
		{
			name:     "date_format one arg",
			function: NewDateFormatFunction(),
			args:     []any{"2023-12-25"},
			wantErr:  true,
		},
		{
			name:     "date_format valid args",
			function: NewDateFormatFunction(),
			args:     []any{"2023-12-25", "YYYY-MM-DD"},
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
		{"yyyy/MM/dd", "2006/01/02"}, // MM = month (uppercase M)
		{"yyyy-MM-dd", "2006-01-02"}, // lowercase yyyy/dd accepted; MM month
		{"yyyy-MM-dd HH:mm:ss", "2006-01-02 15:04:05"},
		{"DD/MM/YYYY", "02/01/2006"},
		{"HH:MI:SS", "15:04:05"},
		{"YYYY-MM-DD HH:MI:SS", "2006-01-02 15:04:05"},
		// mm (lowercase m) = minute, distinct from MM (month). M13 fix.
		{"HH:mm:ss", "15:04:05"},
		{"YYYY-MM-DD HH:mm:ss", "2006-01-02 15:04:05"},
		// Case distinction in one string: MM=month(01), mm=minute(04).
		{"MM/mm", "01/04"},
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
