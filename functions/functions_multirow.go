package functions

import (
	"fmt"
	"reflect"
)

// UnnestFunction 将数组展开为多行
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
		return []interface{}{}, nil
	}

	// 使用反射检查是否为数组或切片
	v := reflect.ValueOf(array)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("unnest requires an array or slice, got %T", array)
	}

	// 转换为 []interface{}
	result := make([]interface{}, v.Len())
	for i := 0; i < v.Len(); i++ {
		elem := v.Index(i).Interface()

		// 如果数组元素是对象（map），则展开为列
		if elemMap, ok := elem.(map[string]interface{}); ok {
			// 对于对象，我们返回一个特殊的结构来表示需要展开为列
			result[i] = map[string]interface{}{
				"__unnest_object__": true,
				"__data__":          elemMap,
			}
		} else {
			result[i] = elem
		}
	}

	return result, nil
}

// UnnestResult 表示 unnest 函数的结果
type UnnestResult struct {
	Rows []map[string]interface{}
}

// IsUnnestResult 检查是否为 unnest 结果
func IsUnnestResult(value interface{}) bool {
	if slice, ok := value.([]interface{}); ok {
		for _, item := range slice {
			if itemMap, ok := item.(map[string]interface{}); ok {
				if unnest, exists := itemMap["__unnest_object__"]; exists {
					if unnestBool, ok := unnest.(bool); ok && unnestBool {
						return true
					}
				}
			}
		}
	}
	return false
}

// ProcessUnnestResult 处理 unnest 结果，将其转换为多行
func ProcessUnnestResult(value interface{}) []map[string]interface{} {
	slice, ok := value.([]interface{})
	if !ok {
		return nil
	}

	var rows []map[string]interface{}
	for _, item := range slice {
		if itemMap, ok := item.(map[string]interface{}); ok {
			if unnest, exists := itemMap["__unnest_object__"]; exists {
				if unnestBool, ok := unnest.(bool); ok && unnestBool {
					if data, exists := itemMap["__data__"]; exists {
						if dataMap, ok := data.(map[string]interface{}); ok {
							rows = append(rows, dataMap)
						}
					}
					continue
				}
			}
		}
		// 对于非对象元素，创建一个包含单个值的行
		row := map[string]interface{}{
			"value": item,
		}
		rows = append(rows, row)
	}

	return rows
}
