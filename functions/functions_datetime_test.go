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
