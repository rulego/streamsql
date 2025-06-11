package functions

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/rulego/streamsql/utils/cast"
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

// ConvertTzFunction 时区转换函数
type ConvertTzFunction struct {
	*BaseFunction
}

func NewConvertTzFunction() *ConvertTzFunction {
	return &ConvertTzFunction{
		BaseFunction: NewBaseFunction("convert_tz", TypeConversion, "转换函数", "将时间转换为指定时区", 2, 2),
	}
}

func (f *ConvertTzFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ConvertTzFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 获取时间值
	var t time.Time
	switch v := args[0].(type) {
	case time.Time:
		t = v
	case string:
		var err error
		// 尝试多种时间格式解析
		formats := []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05.000",
			"2006-01-02T15:04:05.000Z",
		}
		for _, format := range formats {
			if t, err = time.Parse(format, v); err == nil {
				break
			}
		}
		if err != nil {
			return nil, fmt.Errorf("invalid time format: %s", v)
		}
	default:
		return nil, fmt.Errorf("time value must be time.Time or string")
	}

	// 获取目标时区
	timezone, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}

	// 加载时区
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone: %s", timezone)
	}

	// 转换时区
	return t.In(loc), nil
}

// ToSecondsFunction 转换为Unix时间戳（秒）
type ToSecondsFunction struct {
	*BaseFunction
}

func NewToSecondsFunction() *ToSecondsFunction {
	return &ToSecondsFunction{
		BaseFunction: NewBaseFunction("to_seconds", TypeConversion, "转换函数", "将日期时间转换为Unix时间戳（秒）", 1, 1),
	}
}

func (f *ToSecondsFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ToSecondsFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 获取时间值
	var t time.Time
	switch v := args[0].(type) {
	case time.Time:
		t = v
	case string:
		var err error
		// 尝试多种时间格式解析
		formats := []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05.000",
			"2006-01-02T15:04:05.000Z",
		}
		for _, format := range formats {
			if t, err = time.Parse(format, v); err == nil {
				break
			}
		}
		if err != nil {
			return nil, fmt.Errorf("invalid time format: %s", v)
		}
	default:
		return nil, fmt.Errorf("time value must be time.Time or string")
	}

	return t.Unix(), nil
}

// ChrFunction 返回对应ASCII字符
type ChrFunction struct {
	*BaseFunction
}

func NewChrFunction() *ChrFunction {
	return &ChrFunction{
		BaseFunction: NewBaseFunction("chr", TypeConversion, "转换函数", "返回对应ASCII字符", 1, 1),
	}
}

func (f *ChrFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ChrFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	code, err := cast.ToInt64E(args[0])
	if err != nil {
		return nil, err
	}

	if code < 0 || code > 127 {
		return nil, fmt.Errorf("ASCII code must be between 0 and 127, got %d", code)
	}

	return string(rune(code)), nil
}



// UrlEncodeFunction URL编码函数
type UrlEncodeFunction struct {
	*BaseFunction
}

func NewUrlEncodeFunction() *UrlEncodeFunction {
	return &UrlEncodeFunction{
		BaseFunction: NewBaseFunction("url_encode", TypeConversion, "转换函数", "URL编码", 1, 1),
	}
}

func (f *UrlEncodeFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *UrlEncodeFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	if args[0] == nil {
		return nil, fmt.Errorf("url_encode: input cannot be nil")
	}

	input := cast.ToString(args[0])
	return url.QueryEscape(input), nil
}

// UrlDecodeFunction URL解码函数
type UrlDecodeFunction struct {
	*BaseFunction
}

func NewUrlDecodeFunction() *UrlDecodeFunction {
	return &UrlDecodeFunction{
		BaseFunction: NewBaseFunction("url_decode", TypeConversion, "转换函数", "URL解码", 1, 1),
	}
}

func (f *UrlDecodeFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *UrlDecodeFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	if args[0] == nil {
		return nil, fmt.Errorf("url_decode: input cannot be nil")
	}

	input := cast.ToString(args[0])
	result, err := url.QueryUnescape(input)
	if err != nil {
		return nil, fmt.Errorf("URL decode failed: %v", err)
	}
	return result, nil
}

// TruncFunction 截断小数位数
type TruncFunction struct {
	*BaseFunction
}

// NewTruncFunction 创建新的 trunc 函数
func NewTruncFunction() *TruncFunction {
	return &TruncFunction{
		BaseFunction: NewBaseFunction("trunc", TypeConversion, "转换函数", "截断小数位数", 2, 2),
	}
}

// Validate 验证参数
func (f *TruncFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

// Execute 执行函数
func (f *TruncFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}

	// 转换第一个参数为浮点数
	num := cast.ToFloat64(args[0])

	// 转换第二个参数为整数（精度）
	precision := cast.ToInt(args[1])

	// 精度不能为负数
	if precision < 0 {
		return nil, fmt.Errorf("trunc precision cannot be negative")
	}

	// 计算截断
	multiplier := math.Pow(10, float64(precision))
	if num >= 0 {
		return math.Floor(num*multiplier) / multiplier, nil
	} else {
		return math.Ceil(num*multiplier) / multiplier, nil
	}
}
