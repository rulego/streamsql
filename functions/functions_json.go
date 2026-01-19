package functions

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rulego/streamsql/utils/fieldpath"
)

// ToJsonFunction converts value to JSON string
type ToJsonFunction struct {
	*BaseFunction
}

func NewToJsonFunction() *ToJsonFunction {
	return &ToJsonFunction{
		BaseFunction: NewBaseFunction("to_json", TypeConversion, "json", "Convert value to JSON string", 1, 1),
	}
}

func (f *ToJsonFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ToJsonFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	value := args[0]
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to JSON: %v", err)
	}
	return string(jsonBytes), nil
}

// FromJsonFunction parses value from JSON string
type FromJsonFunction struct {
	*BaseFunction
}

func NewFromJsonFunction() *FromJsonFunction {
	return &FromJsonFunction{
		BaseFunction: NewBaseFunction("from_json", TypeConversion, "json", "Parse value from JSON string", 1, 1),
	}
}

func (f *FromJsonFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *FromJsonFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	jsonStr, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("from_json requires string input")
	}

	var result interface{}
	err := json.Unmarshal([]byte(jsonStr), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %v", err)
	}
	return result, nil
}

// JsonExtractFunction extracts JSON field value
type JsonExtractFunction struct {
	*BaseFunction
}

func NewJsonExtractFunction() *JsonExtractFunction {
	return &JsonExtractFunction{
		BaseFunction: NewBaseFunction("json_extract", TypeString, "json", "Extract JSON field value", 2, 2),
	}
}

func (f *JsonExtractFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *JsonExtractFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	var data interface{}
	var err error

	// Support string (JSON), map, and slice input
	switch v := args[0].(type) {
	case string:
		err = json.Unmarshal([]byte(v), &data)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %v", err)
		}
	case map[string]interface{}:
		data = v
	case []interface{}:
		data = v
	default:
		return nil, fmt.Errorf("json_extract requires string, map, or array input")
	}

	path, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("json_extract path must be string")
	}

	// Handle JSON Path format
	// If path starts with $, strip it
	if strings.HasPrefix(path, "$") {
		path = path[1:]
	}
	// If path starts with ., strip it (unless it's empty, though GetNestedField handles empty path)
	if strings.HasPrefix(path, ".") {
		path = path[1:]
	}

	// Use fieldpath utility to extract value
	val, found := fieldpath.GetNestedField(data, path)
	if !found {
		// Try to see if it's a simple key access that fieldpath might have missed or if data structure is simple
		// But fieldpath covers most cases. If not found, it means the path doesn't exist.
		// However, for compatibility with previous simple implementation:
		// Previous implementation returned nil if key not found (implicit in map lookup)
		// So returning nil, nil is correct behavior when field is missing
		return nil, nil
	}

	return val, nil
}

// JsonValidFunction 验证JSON格式是否有效
type JsonValidFunction struct {
	*BaseFunction
}

func NewJsonValidFunction() *JsonValidFunction {
	return &JsonValidFunction{
		BaseFunction: NewBaseFunction("json_valid", TypeString, "JSON函数", "验证JSON格式是否有效", 1, 1),
	}
}

func (f *JsonValidFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *JsonValidFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	jsonStr, ok := args[0].(string)
	if !ok {
		return false, nil
	}

	var temp interface{}
	err := json.Unmarshal([]byte(jsonStr), &temp)
	return err == nil, nil
}

// JsonTypeFunction 返回JSON值的类型
type JsonTypeFunction struct {
	*BaseFunction
}

func NewJsonTypeFunction() *JsonTypeFunction {
	return &JsonTypeFunction{
		BaseFunction: NewBaseFunction("json_type", TypeString, "JSON函数", "返回JSON值的类型", 1, 1),
	}
}

func (f *JsonTypeFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *JsonTypeFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	jsonStr, ok := args[0].(string)
	if !ok {
		return "unknown", nil
	}

	var data interface{}
	err := json.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		return "invalid", nil
	}

	switch data.(type) {
	case nil:
		return "null", nil
	case bool:
		return "boolean", nil
	case float64:
		return "number", nil
	case string:
		return "string", nil
	case []interface{}:
		return "array", nil
	case map[string]interface{}:
		return "object", nil
	default:
		return "unknown", nil
	}
}

// JsonLengthFunction 返回JSON数组或对象的长度
type JsonLengthFunction struct {
	*BaseFunction
}

func NewJsonLengthFunction() *JsonLengthFunction {
	return &JsonLengthFunction{
		BaseFunction: NewBaseFunction("json_length", TypeString, "JSON函数", "返回JSON数组或对象的长度", 1, 1),
	}
}

func (f *JsonLengthFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *JsonLengthFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	jsonStr, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("json_length requires string input")
	}

	var data interface{}
	err := json.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %v", err)
	}

	switch v := data.(type) {
	case []interface{}:
		return len(v), nil
	case map[string]interface{}:
		return len(v), nil
	default:
		return nil, fmt.Errorf("JSON value is not an array or object")
	}
}
