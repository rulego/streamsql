package reflectutil

import (
	"fmt"
	"reflect"
)

// SafeFieldByName 安全地获取结构体字段
func SafeFieldByName(v reflect.Value, fieldName string) (reflect.Value, error) {
	// 检查Value是否有效
	if !v.IsValid() {
		return reflect.Value{}, fmt.Errorf("invalid value")
	}

	// 检查是否为结构体类型
	if v.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("value is not a struct, got %v", v.Kind())
	}

	// 安全地获取字段
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return reflect.Value{}, fmt.Errorf("field %s not found", fieldName)
	}

	return field, nil
}
