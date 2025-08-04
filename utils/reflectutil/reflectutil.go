package reflectutil

import (
	"fmt"
	"reflect"
)

// SafeFieldByName safely gets struct field
func SafeFieldByName(v reflect.Value, fieldName string) (reflect.Value, error) {
	// Check if Value is valid
	if !v.IsValid() {
		return reflect.Value{}, fmt.Errorf("invalid value")
	}

	// Check if it's a struct type
	if v.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("value is not a struct, got %v", v.Kind())
	}

	// Safely get field
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return reflect.Value{}, fmt.Errorf("field %s not found", fieldName)
	}

	return field, nil
}
