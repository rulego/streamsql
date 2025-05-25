package functions

import (
	"fmt"
	"github.com/rulego/streamsql/utils/cast"
	"strings"
)

// ConcatFunction 字符串连接函数
type ConcatFunction struct {
	*BaseFunction
}

func NewConcatFunction() *ConcatFunction {
	return &ConcatFunction{
		BaseFunction: NewBaseFunction("concat", TypeString, "字符串函数", "连接多个字符串", 1, -1),
	}
}

func (f *ConcatFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ConcatFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	var result strings.Builder
	for _, arg := range args {
		str, err := cast.ToStringE(arg)
		if err != nil {
			return nil, err
		}
		result.WriteString(str)
	}
	return result.String(), nil
}

// LengthFunction 字符串长度函数
type LengthFunction struct {
	*BaseFunction
}

func NewLengthFunction() *LengthFunction {
	return &LengthFunction{
		BaseFunction: NewBaseFunction("length", TypeString, "字符串函数", "获取字符串长度", 1, 1),
	}
}

func (f *LengthFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *LengthFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return int64(len(str)), nil
}

// UpperFunction 转大写函数
type UpperFunction struct {
	*BaseFunction
}

func NewUpperFunction() *UpperFunction {
	return &UpperFunction{
		BaseFunction: NewBaseFunction("upper", TypeString, "字符串函数", "转换为大写", 1, 1),
	}
}

func (f *UpperFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *UpperFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return strings.ToUpper(str), nil
}

// LowerFunction 转小写函数
type LowerFunction struct {
	*BaseFunction
}

func NewLowerFunction() *LowerFunction {
	return &LowerFunction{
		BaseFunction: NewBaseFunction("lower", TypeString, "字符串函数", "转换为小写", 1, 1),
	}
}

func (f *LowerFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *LowerFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return strings.ToLower(str), nil
}

// TrimFunction 去除首尾空格函数
type TrimFunction struct {
	*BaseFunction
}

func NewTrimFunction() *TrimFunction {
	return &TrimFunction{
		BaseFunction: NewBaseFunction("trim", TypeString, "字符串函数", "去除字符串首尾空格", 1, 1),
	}
}

func (f *TrimFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *TrimFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return strings.TrimSpace(str), nil
}

// FormatFunction 格式化函数
type FormatFunction struct {
	*BaseFunction
}

func NewFormatFunction() *FormatFunction {
	return &FormatFunction{
		BaseFunction: NewBaseFunction("format", TypeString, "字符串函数", "格式化数值或字符串", 1, 3),
	}
}

func (f *FormatFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *FormatFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	value := args[0]

	// 如果只有一个参数，转换为字符串
	if len(args) == 1 {
		return cast.ToStringE(value)
	}

	// 如果有格式参数
	pattern, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}

	// 处理数值格式化
	if val, err := cast.ToFloat64E(value); err == nil {
		// 简单的数值格式化支持
		switch pattern {
		case "0":
			return fmt.Sprintf("%.0f", val), nil
		case "0.0":
			return fmt.Sprintf("%.1f", val), nil
		case "0.00":
			return fmt.Sprintf("%.2f", val), nil
		case "0.000":
			return fmt.Sprintf("%.3f", val), nil
		default:
			// 尝试解析精度参数
			if strings.Contains(pattern, ".") {
				precision := len(strings.Split(pattern, ".")[1])
				return fmt.Sprintf("%."+fmt.Sprintf("%d", precision)+"f", val), nil
			}
			return fmt.Sprintf("%.2f", val), nil
		}
	}

	// 字符串格式化
	str, err := cast.ToStringE(value)
	if err != nil {
		return nil, err
	}

	// 如果有第三个参数（locale），这里简化处理
	return str, nil
}
