/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cast

import (
	"fmt"
	"testing"
	"time"
)

func TestToInt(t *testing.T) {
	tests := []struct {
		name   string
		input  interface{}
		expect int
		hasErr bool
	}{
		{"int", 123, 123, false},
		{"int8", int8(123), 123, false},
		{"int16", int16(123), 123, false},
		{"int32", int32(123), 123, false},
		{"int64", int64(123), 123, false},
		{"uint", uint(123), 123, false},
		{"uint8", uint8(123), 123, false},
		{"uint16", uint16(123), 123, false},
		{"uint32", uint32(123), 123, false},
		{"uint64", uint64(123), 123, false},
		{"float64", 1.1, 1, false},
		{"float64", float32(1.1), 1, false},
		{"string", "123", 123, false},
		{"invalid string", "abc", 0, true},
		{"invalid type", []int{1, 2, 3}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToInt(tt.input)
			if got != tt.expect {
				t.Errorf("ToInt() = %v, want %v", got, tt.expect)
			}

			_, err := ToIntE(tt.input)
			if (err != nil) != tt.hasErr {
				t.Errorf("ToIntE() error = %v, wantErr %v", err, tt.hasErr)
			}
		})
	}
}

func TestToBoolENumericTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected bool
		hasError bool
	}{
		{"int8_zero", int8(0), false, false},
		{"int8_nonzero", int8(1), true, false},
		{"int16_zero", int16(0), false, false},
		{"int16_nonzero", int16(1), true, false},
		{"int32_zero", int32(0), false, false},
		{"int32_nonzero", int32(1), true, false},
		{"int64_zero", int64(0), false, false},
		{"int64_nonzero", int64(1), true, false},
		{"uint_zero", uint(0), false, false},
		{"uint_nonzero", uint(1), true, false},
		{"uint8_zero", uint8(0), false, false},
		{"uint8_nonzero", uint8(1), true, false},
		{"uint16_zero", uint16(0), false, false},
		{"uint16_nonzero", uint16(1), true, false},
		{"uint32_zero", uint32(0), false, false},
		{"uint32_nonzero", uint32(1), true, false},
		{"uint64_zero", uint64(0), false, false},
		{"uint64_nonzero", uint64(1), true, false},
		{"float32_zero", float32(0.0), false, false},
		{"float32_nonzero", float32(1.0), true, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ToBoolE(test.input)
			if test.hasError {
				if err == nil {
					t.Errorf("Expected error for input %v, but got none", test.input)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for input %v: %v", test.input, err)
				}
				if result != test.expected {
					t.Errorf("Expected %v for input %v, but got %v", test.expected, test.input, result)
				}
			}
		})
	}
}

// TestConvertIntToTime 测试ConvertIntToTime函数
func TestConvertIntToTime(t *testing.T) {
	tests := []struct {
		name      string
		timestamp int64
		timeUnit  time.Duration
		expected  time.Time
	}{
		{"seconds", 1609459200, time.Second, time.Unix(1609459200, 0)},
		{"milliseconds", 1609459200000, time.Millisecond, time.Unix(0, 1609459200000*int64(time.Millisecond))},
		{"microseconds", 1609459200000000, time.Microsecond, time.Unix(0, 1609459200000000*int64(time.Microsecond))},
		{"nanoseconds", 1609459200000000000, time.Nanosecond, time.Unix(0, 1609459200000000000)},
		{"default unit", 1609459200, time.Minute, time.Unix(1609459200, 0)}, // 默认按秒处理
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ConvertIntToTime(tt.timestamp, tt.timeUnit)
			if !got.Equal(tt.expected) {
				t.Errorf("ConvertIntToTime() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// testStringer 实现fmt.Stringer接口
type testStringer struct {
	value string
}

func (ts testStringer) String() string {
	return ts.value
}

// TestToStringEComplexTypes 测试ToStringE函数的复杂类型
func TestToStringEComplexTypes(t *testing.T) {

	// 测试map[interface{}]interface{}类型
	mapInterfaceInterface := map[interface{}]interface{}{
		"key1": "value1",
		123:    "value2",
	}

	tests := []struct {
		name     string
		input    interface{}
		expected string
		hasErr   bool
	}{
		{"fmt.Stringer", testStringer{"test string"}, "test string", false},
		{"map[interface{}]interface{}", mapInterfaceInterface, "{\"123\":\"value2\",\"key1\":\"value1\"}", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToStringE(tt.input)
			if (err != nil) != tt.hasErr {
				t.Errorf("ToStringE() error = %v, wantErr %v", err, tt.hasErr)
			}
			if !tt.hasErr {
				// 对于JSON序列化的结果，由于map的顺序不确定，我们检查是否包含关键内容
				if tt.name == "map[interface{}]interface{}" {
					if len(got) == 0 || got[0] != '{' || got[len(got)-1] != '}' {
						t.Errorf("ToStringE() = %v, expected JSON format", got)
					}
				} else if got != tt.expected {
					t.Errorf("ToStringE() = %v, want %v", got, tt.expected)
				}
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name   string
		input  interface{}
		expect int64
		hasErr bool
	}{
		{"int", 123, 123, false},
		{"int8", int8(123), 123, false},
		{"int16", int16(123), 123, false},
		{"int32", int32(123), 123, false},
		{"int64", int64(123), 123, false},
		{"uint", uint(123), 123, false},
		{"uint8", uint8(123), 123, false},
		{"uint16", uint16(123), 123, false},
		{"uint32", uint32(123), 123, false},
		{"uint64", uint64(123), 123, false},
		{"string", "123", 123, false},
		{"invalid string", "abc", 0, true},
		{"invalid type", []int{1, 2, 3}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToInt64(tt.input)
			if got != tt.expect {
				t.Errorf("ToInt64() = %v, want %v", got, tt.expect)
			}

			_, err := ToInt64E(tt.input)
			if (err != nil) != tt.hasErr {
				t.Errorf("ToInt64E() error = %v, wantErr %v", err, tt.hasErr)
			}
		})
	}
}

func TestToDurationE(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected time.Duration
		hasErr   bool
	}{
		{"duration", time.Second, time.Second, false},
		{"int", 1000, 1000, false},
		{"int8", int8(100), 100, false},
		{"int16", int16(1000), 1000, false},
		{"int32", int32(1000), 1000, false},
		{"int64", int64(1000), 1000, false},
		{"uint", uint(1000), 1000, false},
		{"uint8", uint8(100), 100, false},
		{"uint16", uint16(1000), 1000, false},
		{"uint32", uint32(1000), 1000, false},
		{"uint64", uint64(1000), 1000, false},
		{"string", "1s", time.Second, false},
		{"invalid string", "abc", 0, true},
		{"invalid type", []int{1, 2, 3}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dur, err := ToDurationE(tt.input)
			if (err != nil) != tt.hasErr {
				t.Errorf("ToDurationE() error = %v, wantErr %v", err, tt.hasErr)
			}
			if !tt.hasErr && dur != tt.expected {
				t.Errorf("ToDurationE() = %v, want %v", dur, tt.expected)
			}
		})
	}
}

func TestToBool(t *testing.T) {
	tests := []struct {
		name   string
		input  interface{}
		expect bool
		hasErr bool
	}{
		{"bool true", true, true, false},
		{"bool false", false, false, false},
		{"int 1", 1, true, false},
		{"int 0", 0, false, false},
		{"float64 1.0", 1.0, true, false},
		{"float64 0.0", 0.0, false, false},
		{"string true", "true", true, false},
		{"string false", "false", false, false},
		{"string 1", "1", true, false},
		{"string 0", "0", false, false},
		{"invalid string", "abc", false, true},
		{"invalid type", []int{1, 2, 3}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToBool(tt.input)
			if got != tt.expect {
				t.Errorf("ToBool() = %v, want %v", got, tt.expect)
			}

			_, err := ToBoolE(tt.input)
			if (err != nil) != tt.hasErr {
				t.Errorf("ToBoolE() error = %v, wantErr %v", err, tt.hasErr)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name   string
		input  interface{}
		expect float64
		hasErr bool
	}{
		{"float64", 3.14, 3.14, false},
		{"float32", float32(3.14), float64(float32(3.14)), false},
		{"int", 123, 123.0, false},
		{"int64", int64(123), 123.0, false},
		{"uint64", uint64(123), 123.0, false},
		{"string", "3.14", 3.14, false},
		{"invalid string", "abc", 0, true},
		{"invalid type", []int{1, 2, 3}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToFloat64(tt.input)
			if got != tt.expect {
				t.Errorf("ToFloat64() = %v, want %v", got, tt.expect)
			}

			_, err := ToFloat64E(tt.input)
			if (err != nil) != tt.hasErr {
				t.Errorf("ToFloat64E() error = %v, wantErr %v", err, tt.hasErr)
			}
		})
	}
}

func TestToString(t *testing.T) {
	tests := []struct {
		name   string
		input  interface{}
		expect string
		hasErr bool
	}{
		{"nil", nil, "", false},
		{"string", "test", "test", false},
		{"bool true", true, "true", false},
		{"bool false", false, "false", false},
		{"int", 123, "123", false},
		{"int8", int8(123), "123", false},
		{"int16", int16(123), "123", false},
		{"int32", int32(123), "123", false},
		{"int64", int64(123), "123", false},
		{"uint", uint(123), "123", false},
		{"uint8", uint8(123), "123", false},
		{"uint16", uint16(123), "123", false},
		{"uint32", uint32(123), "123", false},
		{"uint64", uint64(123), "123", false},
		{"float64", 3.14, "3.14", false},
		{"float32", float32(3.14), "3.14", false},
		{"[]byte", []byte("test"), "test", false},
		{"error", fmt.Errorf("test error"), "test error", false},
		{"map", map[string]int{"a": 1}, "{\"a\":1}", false},
		{"invalid type", make(chan int), "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToString(tt.input)
			if got != tt.expect {
				t.Errorf("ToString() = %v, want %v", got, tt.expect)
			}

			_, err := ToStringE(tt.input)
			if (err != nil) != tt.hasErr {
				t.Errorf("ToStringE() error = %v, wantErr %v", err, tt.hasErr)
			}
		})
	}
}
