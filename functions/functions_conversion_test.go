package functions

import (
	"testing"
	"time"
)

func TestNewConversionFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		want     interface{}
		wantErr  bool
	}{
		// convert_tz 函数测试
		{
			name:     "convert_tz with time.Time",
			funcName: "convert_tz",
			args:     []interface{}{time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC), "Asia/Shanghai"},
			want:     time.Date(2023, 1, 1, 20, 0, 0, 0, time.FixedZone("CST", 8*3600)),
			wantErr:  false,
		},
		{
			name:     "convert_tz with string",
			funcName: "convert_tz",
			args:     []interface{}{"2023-01-01 12:00:00", "America/New_York"},
			wantErr:  false,
		},
		{
			name:     "convert_tz invalid timezone",
			funcName: "convert_tz",
			args:     []interface{}{time.Now(), "Invalid/Timezone"},
			wantErr:  true,
		},
		{
			name:     "convert_tz invalid time format",
			funcName: "convert_tz",
			args:     []interface{}{"invalid-time", "UTC"},
			wantErr:  true,
		},

		// to_seconds 函数测试
		{
			name:     "to_seconds with time.Time",
			funcName: "to_seconds",
			args:     []interface{}{time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)},
			want:     int64(1672531200),
			wantErr:  false,
		},
		{
			name:     "to_seconds with string",
			funcName: "to_seconds",
			args:     []interface{}{"2023-01-01T00:00:00Z"},
			want:     int64(1672531200),
			wantErr:  false,
		},
		{
			name:     "to_seconds invalid time format",
			funcName: "to_seconds",
			args:     []interface{}{"invalid-time"},
			wantErr:  true,
		},

		// chr 函数测试
		{
			name:     "chr valid ASCII code",
			funcName: "chr",
			args:     []interface{}{65},
			want:     "A",
			wantErr:  false,
		},
		{
			name:     "chr space character",
			funcName: "chr",
			args:     []interface{}{32},
			want:     " ",
			wantErr:  false,
		},
		{
			name:     "chr invalid code negative",
			funcName: "chr",
			args:     []interface{}{-1},
			wantErr:  true,
		},
		{
			name:     "chr invalid code too large",
			funcName: "chr",
			args:     []interface{}{128},
			wantErr:  true,
		},

		// trunc 函数测试
		{
			name:     "trunc positive number",
			funcName: "trunc",
			args:     []interface{}{3.14159, 2},
			want:     3.14,
			wantErr:  false,
		},

		// url_encode 函数测试
		{
			name:     "url_encode basic",
			funcName: "url_encode",
			args:     []interface{}{"hello world"},
			want:     "hello+world",
			wantErr:  false,
		},
		{
			name:     "url_encode special chars",
			funcName: "url_encode",
			args:     []interface{}{"hello@world.com"},
			want:     "hello%40world.com",
			wantErr:  false,
		},
		{
			name:     "url_encode empty",
			funcName: "url_encode",
			args:     []interface{}{""},
			want:     "",
			wantErr:  false,
		},
		{
			name:     "url_encode nil",
			funcName: "url_encode",
			args:     []interface{}{nil},
			wantErr:  true,
		},

		// url_decode 函数测试
		{
			name:     "url_decode basic",
			funcName: "url_decode",
			args:     []interface{}{"hello+world"},
			want:     "hello world",
			wantErr:  false,
		},
		{
			name:     "url_decode special chars",
			funcName: "url_decode",
			args:     []interface{}{"hello%40world.com"},
			want:     "hello@world.com",
			wantErr:  false,
		},
		{
			name:     "url_decode empty",
			funcName: "url_decode",
			args:     []interface{}{""},
			want:     "",
			wantErr:  false,
		},
		{
			name:     "url_decode nil",
			funcName: "url_decode",
			args:     []interface{}{nil},
			wantErr:  true,
		},
		{
			name:     "url_decode invalid",
			funcName: "url_decode",
			args:     []interface{}{"hello%ZZ"},
			wantErr:  true,
		},
		{
			name:     "trunc negative number",
			funcName: "trunc",
			args:     []interface{}{-3.14159, 3},
			want:     -3.141,
			wantErr:  false,
		},
		{
			name:     "trunc zero precision",
			funcName: "trunc",
			args:     []interface{}{3.14159, 0},
			want:     3.0,
			wantErr:  false,
		},
		{
			name:     "trunc negative precision",
			funcName: "trunc",
			args:     []interface{}{3.14159, -1},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.funcName)
			if !exists {
				t.Fatalf("Function %s not found", tt.funcName)
			}

			result, err := fn.Execute(nil, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// 对于时间类型，需要特殊处理比较
				if tt.funcName == "convert_tz" {
					if resultTime, ok := result.(time.Time); ok {
						if wantTime, ok := tt.want.(time.Time); ok {
							// 比较时间戳而不是直接比较时间对象
							if resultTime.Unix() != wantTime.Unix() {
								t.Errorf("Execute() = %v, want %v", result, tt.want)
							}
						} else {
							// 如果期望值不是时间类型，只检查结果是否为时间类型
							if resultTime.IsZero() {
								t.Errorf("Execute() returned zero time")
							}
						}
					} else {
						t.Errorf("Execute() result is not time.Time")
					}
				} else if tt.want != nil && result != tt.want {
					t.Errorf("Execute() = %v, want %v", result, tt.want)
				}
			}
		})
	}
}
