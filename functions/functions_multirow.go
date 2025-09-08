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

func (f *UnnestFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *UnnestFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	array := args[0]
	if array == nil {
		// 返回带有unnest标记的空结果
		return []interface{}{
			map[string]interface{}{
				UnnestObjectMarker: true,
				UnnestEmptyMarker: true, // 标记这是空unnest结果
			},
		}, nil
	}

	// 使用反射检查是否为数组或切片
	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("unnest requires an array or slice, got %T", array)
	}

	// 如果数组为空，返回带标记的空数组
	if v.Len() == 0 {
		// 返回带有unnest标记的空结果
		return []interface{}{
			map[string]interface{}{
				UnnestObjectMarker: true,
				UnnestEmptyMarker: true, // 标记这是空unnest结果
			},
		}, nil
	}

	// 转换为 []interface{}，所有元素都标记为unnest结果
	result := make([]interface{}, v.Len())
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()

		// 如果数组元素是对象（map），则展开为列
		if elemMap, ok := elem.(map[string]interface{}); ok {
			// 对于对象，我们返回一个特殊的结构来表示需要展开为列
			result[i] = map[string]interface{}{
				UnnestObjectMarker: true,
				UnnestDataKey:      elemMap,
			}
		} else {
			// 对于普通元素，也需要标记为unnest结果
			result[i] = map[string]interface{}{
				UnnestObjectMarker: true,
				UnnestDataKey:      elem,
			}
		}
	}

	return result, nil
}

type UnnestResult struct {
	Rows []map[string]interface{}
}

func IsUnnestResult(value interface{}) bool {
	slice, ok := value.([]interface{})
	if !ok || len(slice) == 0 {
		return false
	}
	
	// 检查数组中是否有任何unnest标记的元素
	for _, item := range slice {
		if itemMap, ok := item.(map[string]interface{}); ok {
			if unnest, exists := itemMap[UnnestObjectMarker]; exists {
				if unnestBool, ok := unnest.(bool); ok && unnestBool {
					return true
				}
			}
		}
	}
	
	// 如果没有找到unnest标记，则不是unnest结果
	return false
}

func ProcessUnnestResult(value interface{}) []map[string]interface{} {
	slice, ok := value.([]interface{})
	if !ok {
		return nil
	}

	var rows []map[string]interface{}
	for _, item := range slice {
		if itemMap, ok := item.(map[string]interface{}); ok {
			if unnest, exists := itemMap[UnnestObjectMarker]; exists {
				if unnestBool, ok := unnest.(bool); ok && unnestBool {
					if data, exists := itemMap[UnnestDataKey]; exists {
						// 检查数据是否为对象（map）
						if dataMap, ok := data.(map[string]interface{}); ok {
							// 对象数据直接展开为列
							rows = append(rows, dataMap)
						} else {
							// 普通数据使用默认字段名
							row := map[string]interface{}{
								DefaultValueKey: data,
							}
							rows = append(rows, row)
						}
					}
					continue
				}
			}
		}
		// 对于非标记元素，创建一个包含单个值的行（向后兼容）
		row := map[string]interface{}{
			DefaultValueKey: item,
		}
		rows = append(rows, row)
	}

	return rows
}

func ProcessUnnestResultWithFieldName(value interface{}, fieldName string) []map[string]interface{} {
	slice, ok := value.([]interface{})
	if !ok {
		return nil
	}

	var rows []map[string]interface{}
	for _, item := range slice {
		if itemMap, ok := item.(map[string]interface{}); ok {
			if unnest, exists := itemMap[UnnestObjectMarker]; exists {
				if unnestBool, ok := unnest.(bool); ok && unnestBool {
					// 检查是否为空unnest结果
					if itemMap[UnnestEmptyMarker] == true {
						// 空unnest结果，返回空数组
						return []map[string]interface{}{}
					}
					
					if data, exists := itemMap[UnnestDataKey]; exists {
						// 检查数据是否为对象（map）
						if dataMap, ok := data.(map[string]interface{}); ok {
							// 对象数据直接展开为列
							rows = append(rows, dataMap)
						} else {
							// 普通数据使用指定字段名
							row := map[string]interface{}{
								fieldName: data,
							}
							rows = append(rows, row)
						}
					}
					continue
				}
			}
		}
		// 对于非标记元素，使用指定的字段名创建行（向后兼容）
		row := map[string]interface{}{
			fieldName: item,
		}
		rows = append(rows, row)
	}

	return rows
}
