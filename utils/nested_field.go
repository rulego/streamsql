package utils

import (
	"reflect"
	"strings"
)

// GetNestedField 从嵌套的map或结构体中获取字段值
// 支持点号分隔的字段路径，如 "device.info.name"
func GetNestedField(data interface{}, fieldPath string) (interface{}, bool) {
	if fieldPath == "" {
		return nil, false
	}

	// 分割字段路径
	fields := strings.Split(fieldPath, ".")
	current := data

	for _, field := range fields {
		val, found := getFieldValue(current, field)
		if !found {
			return nil, false
		}
		current = val
	}

	return current, true
}

// getFieldValue 从单个层级获取字段值
func getFieldValue(data interface{}, fieldName string) (interface{}, bool) {
	if data == nil {
		return nil, false
	}

	v := reflect.ValueOf(data)

	// 如果是指针，解引用
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, false
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Map:
		// 处理 map[string]interface{}
		if v.Type().Key().Kind() == reflect.String {
			mapVal := v.MapIndex(reflect.ValueOf(fieldName))
			if mapVal.IsValid() {
				return mapVal.Interface(), true
			}
		}
		return nil, false

	case reflect.Struct:
		// 处理结构体
		fieldVal := v.FieldByName(fieldName)
		if fieldVal.IsValid() {
			return fieldVal.Interface(), true
		}
		return nil, false

	default:
		return nil, false
	}
}

// SetNestedField 在嵌套的map中设置字段值
// 如果路径中的某些层级不存在，会自动创建
func SetNestedField(data map[string]interface{}, fieldPath string, value interface{}) {
	if fieldPath == "" {
		return
	}

	fields := strings.Split(fieldPath, ".")
	current := data

	// 遍历到倒数第二层，确保路径存在
	for i := 0; i < len(fields)-1; i++ {
		field := fields[i]
		if next, exists := current[field]; exists {
			if nextMap, ok := next.(map[string]interface{}); ok {
				current = nextMap
			} else {
				// 如果存在但不是map，创建新的map覆盖
				newMap := make(map[string]interface{})
				current[field] = newMap
				current = newMap
			}
		} else {
			// 如果不存在，创建新的map
			newMap := make(map[string]interface{})
			current[field] = newMap
			current = newMap
		}
	}

	// 设置最终的值
	lastField := fields[len(fields)-1]
	current[lastField] = value
}

// IsNestedField 检查字段名是否包含点号（嵌套字段）
func IsNestedField(fieldName string) bool {
	return strings.Contains(fieldName, ".")
}

// ExtractTopLevelField 从嵌套字段路径中提取顶级字段名
// 例如："device.info.name" 返回 "device"
func ExtractTopLevelField(fieldPath string) string {
	if fieldPath == "" {
		return ""
	}

	parts := strings.Split(fieldPath, ".")
	return parts[0]
}

// GetAllReferencedFields 获取嵌套字段路径中引用的所有顶级字段
// 例如：["device.info.name", "sensor.temperature"] 返回 ["device", "sensor"]
func GetAllReferencedFields(fieldPaths []string) []string {
	topLevelFields := make(map[string]bool)

	for _, path := range fieldPaths {
		if path != "" {
			topField := ExtractTopLevelField(path)
			if topField != "" {
				topLevelFields[topField] = true
			}
		}
	}

	result := make([]string, 0, len(topLevelFields))
	for field := range topLevelFields {
		result = append(result, field)
	}

	return result
}
