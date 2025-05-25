package functions

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/rulego/streamsql/utils/cast"
	"net/url"
	"strconv"
)

// CastFunction 类型转换函数
type CastFunction struct {
	*BaseFunction
}

func NewCastFunction() *CastFunction {
	return &CastFunction{
		BaseFunction: NewBaseFunction("cast", TypeConversion, "转换函数", "类型转换", 2, 2),
	}
}

func (f *CastFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CastFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	value := args[0]
	targetType := cast.ToString(args[1])

	switch targetType {
	case "bigint", "int64":
		return cast.ToInt64E(value)
	case "int", "int32":
		val, err := cast.ToInt64E(value)
		if err != nil {
			return nil, err
		}
		return int32(val), nil
	case "float", "float64":
		return cast.ToFloat64E(value)
	case "string":
		return cast.ToStringE(value)
	case "bool", "boolean":
		return cast.ToBoolE(value)
	default:
		return nil, fmt.Errorf("unsupported cast type: %s", targetType)
	}
}

// Hex2DecFunction 十六进制转十进制函数
type Hex2DecFunction struct {
	*BaseFunction
}

func NewHex2DecFunction() *Hex2DecFunction {
	return &Hex2DecFunction{
		BaseFunction: NewBaseFunction("hex2dec", TypeConversion, "转换函数", "十六进制转十进制", 1, 1),
	}
}

func (f *Hex2DecFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *Hex2DecFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	hexStr := cast.ToString(args[0])

	val, err := strconv.ParseInt(hexStr, 16, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid hex string: %s", hexStr)
	}

	return val, nil
}

// Dec2HexFunction 十进制转十六进制函数
type Dec2HexFunction struct {
	*BaseFunction
}

func NewDec2HexFunction() *Dec2HexFunction {
	return &Dec2HexFunction{
		BaseFunction: NewBaseFunction("dec2hex", TypeConversion, "转换函数", "十进制转十六进制", 1, 1),
	}
}

func (f *Dec2HexFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *Dec2HexFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToInt64E(args[0])
	if err != nil {
		return nil, err
	}

	return fmt.Sprintf("%x", val), nil
}

// EncodeFunction 将输入值编码为指定格式的字符串
type EncodeFunction struct {
	*BaseFunction
}

func NewEncodeFunction() *EncodeFunction {
	return &EncodeFunction{
		BaseFunction: NewBaseFunction("encode", TypeConversion, "转换函数", "将输入值编码为指定格式", 2, 2),
	}
}

func (f *EncodeFunction) Validate(args []interface{}) error {
	if err := f.ValidateArgCount(args); err != nil {
		return err
	}
	format, ok := args[1].(string)
	if !ok {
		return fmt.Errorf("encode format must be a string")
	}
	switch format {
	case "base64", "hex", "url":
		return nil
	default:
		return fmt.Errorf("unsupported encode format: %s", format)
	}
}

func (f *EncodeFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	value := args[0]
	format := args[1].(string)

	var input []byte
	switch v := value.(type) {
	case string:
		input = []byte(v)
	case []byte:
		input = v
	default:
		return nil, fmt.Errorf("encode input must be string or []byte")
	}

	switch format {
	case "base64":
		return base64.StdEncoding.EncodeToString(input), nil
	case "hex":
		return hex.EncodeToString(input), nil
	case "url":
		return url.QueryEscape(string(input)), nil
	default:
		return nil, fmt.Errorf("unsupported encode format: %s", format)
	}
}

// DecodeFunction 将编码的字符串解码为原始数据
type DecodeFunction struct {
	*BaseFunction
}

func NewDecodeFunction() *DecodeFunction {
	return &DecodeFunction{
		BaseFunction: NewBaseFunction("decode", TypeConversion, "转换函数", "将编码的字符串解码为原始数据", 2, 2),
	}
}

func (f *DecodeFunction) Validate(args []interface{}) error {
	if err := f.ValidateArgCount(args); err != nil {
		return err
	}
	if _, ok := args[0].(string); !ok {
		return fmt.Errorf("decode input must be a string")
	}
	format, ok := args[1].(string)
	if !ok {
		return fmt.Errorf("decode format must be a string")
	}
	switch format {
	case "base64", "hex", "url":
		return nil
	default:
		return fmt.Errorf("unsupported decode format: %s", format)
	}
}

func (f *DecodeFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	encoded := args[0].(string)
	format := args[1].(string)

	switch format {
	case "base64":
		result, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid base64 string: %v", err)
		}
		return string(result), nil
	case "hex":
		result, err := hex.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid hex string: %v", err)
		}
		return string(result), nil
	case "url":
		result, err := url.QueryUnescape(encoded)
		if err != nil {
			return nil, fmt.Errorf("invalid url encoded string: %v", err)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported decode format: %s", format)
	}
}
