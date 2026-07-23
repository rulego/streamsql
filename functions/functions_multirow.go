package functions

import (
	"fmt"
	"reflect"
)

const (
	UnnestObjectMarker = "__unnest_object__"
	UnnestDataKey      = "__data__"
	UnnestEmptyMarker  = "__empty_unnest__"
	DefaultValueKey    = "value"
)

type UnnestFunction struct {
	*BaseFunction
}

func NewUnnestFunction() *UnnestFunction {
	return &UnnestFunction{
		BaseFunction: NewBaseFunction("unnest", TypeString, "多行函数", "将数组展开为多行", 1, 1),
	}
}

func (f *UnnestFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *UnnestFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	array := args[0]
	if array == nil {
		// Returns an empty result marked with unnest
		return []any{
			map[string]any{
				UnnestObjectMarker: true,
				UnnestEmptyMarker:  true, // Mark this as an empty unnest result
			},
		}, nil
	}

	// Use reflection to check whether the array is a slice
	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("unnest requires an array or slice, got %T", array)
	}

	// If the array is empty, it returns an empty array with a mark
	if v.Len() == 0 {
		// Returns an empty result marked with unnest
		return []any{
			map[string]any{
				UnnestObjectMarker: true,
				UnnestEmptyMarker:  true, // Mark this as an empty unnest result
			},
		}, nil
	}

	// Convert to []any, and all elements are marked as unnest results
	result := make([]any, v.Len())
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()

		// If the array element is an object (map), expand it into a column
		if elemMap, ok := elem.(map[string]any); ok {
			// For objects, we return a special structure to indicate that the columns need to be expanded
			result[i] = map[string]any{
				UnnestObjectMarker: true,
				UnnestDataKey:      elemMap,
			}
		} else {
			// For ordinary elements, you also need to mark them as unnest results
			result[i] = map[string]any{
				UnnestObjectMarker: true,
				UnnestDataKey:      elem,
			}
		}
	}

	return result, nil
}

type UnnestResult struct {
	Rows []map[string]any
}

func IsUnnestResult(value any) bool {
	slice, ok := value.([]any)
	if !ok || len(slice) == 0 {
		return false
	}

	// Check if there are any unnest tagged elements in the array
	for _, item := range slice {
		if itemMap, ok := item.(map[string]any); ok {
			if unnest, exists := itemMap[UnnestObjectMarker]; exists {
				if unnestBool, ok := unnest.(bool); ok && unnestBool {
					return true
				}
			}
		}
	}

	// If the unnest tag is not found, it is not the unnest result
	return false
}

func ProcessUnnestResult(value any) []map[string]any {
	slice, ok := value.([]any)
	if !ok {
		return nil
	}

	var rows []map[string]any
	for _, item := range slice {
		if itemMap, ok := item.(map[string]any); ok {
			if unnest, exists := itemMap[UnnestObjectMarker]; exists {
				if unnestBool, ok := unnest.(bool); ok && unnestBool {
					if data, exists := itemMap[UnnestDataKey]; exists {
						// Check if the data is an object (map)
						if dataMap, ok := data.(map[string]any); ok {
							// Expand object data directly into columns
							rows = append(rows, dataMap)
						} else {
							// Ordinary data uses the default field name
							row := map[string]any{
								DefaultValueKey: data,
							}
							rows = append(rows, row)
						}
					}
					continue
				}
			}
		}
		// For non-marked elements, create a row containing a single value (backward compatible)
		row := map[string]any{
			DefaultValueKey: item,
		}
		rows = append(rows, row)
	}

	return rows
}

func ProcessUnnestResultWithFieldName(value any, fieldName string) []map[string]any {
	slice, ok := value.([]any)
	if !ok {
		return nil
	}

	var rows []map[string]any
	for _, item := range slice {
		if itemMap, ok := item.(map[string]any); ok {
			if unnest, exists := itemMap[UnnestObjectMarker]; exists {
				if unnestBool, ok := unnest.(bool); ok && unnestBool {
					// Check if the result is empty
					if itemMap[UnnestEmptyMarker] == true {
						// Empty unnest result returns an empty array
						return []map[string]any{}
					}

					if data, exists := itemMap[UnnestDataKey]; exists {
						// Check if the data is an object (map)
						if dataMap, ok := data.(map[string]any); ok {
							// Expand object data directly into columns
							rows = append(rows, dataMap)
						} else {
							// Ordinary data uses specified field names
							row := map[string]any{
								fieldName: data,
							}
							rows = append(rows, row)
						}
					}
					continue
				}
			}
		}
		// For non-marked elements, create rows using specified field names (backward compatible)
		row := map[string]any{
			fieldName: item,
		}
		rows = append(rows, row)
	}

	return rows
}
