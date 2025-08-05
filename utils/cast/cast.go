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
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// ToInt converts an interface{} to int.
// It returns 0 if conversion fails.
func ToInt(value interface{}) int {
	v, _ := ToIntE(value)
	return v
}

// ToIntE converts an interface{} to int with error handling.
// Returns the converted int value and nil error if successful.
// Returns 0 and an error if conversion fails.
func ToIntE(value interface{}) (int, error) {
	switch v := value.(type) {
	case int:
		return v, nil
	case int8:
		return int(v), nil
	case int16:
		return int(v), nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case uint:
		return int(v), nil
	case uint8:
		return int(v), nil
	case uint16:
		return int(v), nil
	case uint32:
		return int(v), nil
	case uint64:
		return int(v), nil
	case float64:
		return int(v), nil
	case float32:
		return int(v), nil
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return i, nil
		} else {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unable to cast %v of type %T to int", value, value)
	}
}

// ToInt64 converts an interface{} to int64.
// It returns 0 if conversion fails.
func ToInt64(value interface{}) int64 {
	v, _ := ToInt64E(value)
	return v
}

// ToInt64E converts an interface{} to int64 with error handling.
// Returns the converted int64 value and nil error if successful.
// Returns 0 and an error if conversion fails.
func ToInt64E(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return int64(i), nil
		} else {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unable to cast %v of type %T to int", value, value)
	}
}

// ToDurationE converts an interface{} to time.Duration with error handling.
// Returns the converted duration value and nil error if successful.
// Returns 0 and an error if conversion fails.
func ToDurationE(value interface{}) (time.Duration, error) {
	switch v := value.(type) {
	case time.Duration:
		return v, nil
	case int:
		return time.Duration(v), nil
	case int8:
		return time.Duration(v), nil
	case int16:
		return time.Duration(v), nil
	case int32:
		return time.Duration(v), nil
	case int64:
		return time.Duration(v), nil
	case uint:
		return time.Duration(v), nil
	case uint8:
		return time.Duration(v), nil
	case uint16:
		return time.Duration(v), nil
	case uint32:
		return time.Duration(v), nil
	case uint64:
		return time.Duration(v), nil
	case string:
		if dur, err := time.ParseDuration(v); err == nil {
			return dur, nil
		} else {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unable to cast %v of type %T to int", value, value)
	}
}

// ToBool converts an interface{} to bool.
// It returns false if conversion fails.
func ToBool(value interface{}) bool {
	v, _ := ToBoolE(value)
	return v
}

// ToBoolE converts an interface{} to bool with error handling.
// Returns the converted bool value and nil error if successful.
// Returns false and an error if conversion fails.
func ToBoolE(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	case int8:
		return v != 0, nil
	case int16:
		return v != 0, nil
	case int32:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case uint:
		return v != 0, nil
	case uint8:
		return v != 0, nil
	case uint16:
		return v != 0, nil
	case uint32:
		return v != 0, nil
	case uint64:
		return v != 0, nil
	case float32:
		return v != 0.0, nil
	case float64:
		return v != 0.0, nil
	case string:
		if b, err := strconv.ParseBool(v); err == nil {
			return b, nil
		}
		return false, fmt.Errorf("unable to cast %v of type %T to bool", value, value)
	default:
		return false, fmt.Errorf("unable to cast %v of type %T to bool", value, value)
	}
}

// ToFloat64 converts an interface{} to float64.
// It returns 0 if conversion fails.
func ToFloat64(value interface{}) float64 {
	v, _ := ToFloat64E(value)
	return v
}

// ToFloat64E converts an interface{} to float64 with error handling.
// Returns the converted float64 value and nil error if successful.
// Returns 0 and an error if conversion fails.
func ToFloat64E(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		} else {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unable to cast %v of type %T to float64", value, value)
	}
}

// ToString converts an interface{} to string.
// It returns empty string if conversion fails.
func ToString(input interface{}) string {
	v, _ := ToStringE(input)
	return v
}

// convertNumericToString 将数字类型转换为字符串
func convertNumericToString(input interface{}) (string, bool) {
	switch v := input.(type) {
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), true
	case int:
		return strconv.Itoa(v), true
	case uint:
		return strconv.Itoa(int(v)), true
	case int8:
		return strconv.Itoa(int(v)), true
	case uint8:
		return strconv.Itoa(int(v)), true
	case int16:
		return strconv.Itoa(int(v)), true
	case uint16:
		return strconv.Itoa(int(v)), true
	case int32:
		return strconv.Itoa(int(v)), true
	case uint32:
		return strconv.Itoa(int(v)), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case uint64:
		return strconv.FormatUint(v, 10), true
	default:
		return "", false
	}
}

// convertComplexToString 将复杂类型转换为字符串
func convertComplexToString(input interface{}) (string, error) {
	switch v := input.(type) {
	case []byte:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	case error:
		return v.Error(), nil
	case map[interface{}]interface{}:
		// 转换为 map[string]interface{}
		convertedInput := make(map[string]interface{})
		for k, value := range v {
			convertedInput[fmt.Sprintf("%v", k)] = value
		}
		return marshalToString(convertedInput)
	default:
		return marshalToString(input)
	}
}

// marshalToString 通过JSON序列化转换为字符串
func marshalToString(input interface{}) (string, error) {
	if newValue, err := json.Marshal(input); err == nil {
		return string(newValue), nil
	} else {
		return "", err
	}
}

// ToStringE converts an interface{} to string with error handling.
// Returns the converted string value and nil error if successful.
// Returns empty string and an error if conversion fails.
func ToStringE(input interface{}) (string, error) {
	if input == nil {
		return "", nil
	}

	switch v := input.(type) {
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		// 尝试数字类型转换
		if str, ok := convertNumericToString(input); ok {
			return str, nil
		}
		// 尝试复杂类型转换
		return convertComplexToString(input)
	}
}

// ConvertIntToTime 将整数时间戳转换为 time.Time
func ConvertIntToTime(timestampInt int64, timeUnit time.Duration) time.Time {
	switch timeUnit {
	case time.Second:
		return time.Unix(timestampInt, 0)
	case time.Millisecond:
		return time.Unix(0, timestampInt*int64(time.Millisecond))
	case time.Microsecond:
		return time.Unix(0, timestampInt*int64(time.Microsecond))
	case time.Nanosecond:
		return time.Unix(0, timestampInt)
	default:
		return time.Unix(timestampInt, 0) // 默认按秒处理
	}
}
