package functions

import (
	"fmt"
	"github.com/rulego/streamsql/utils/cast"
	"regexp"
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

// EndswithFunction 检查字符串是否以指定后缀结尾
type EndswithFunction struct {
	*BaseFunction
}

func NewEndswithFunction() *EndswithFunction {
	return &EndswithFunction{
		BaseFunction: NewBaseFunction("endswith", TypeString, "字符串函数", "检查字符串是否以指定后缀结尾", 2, 2),
	}
}

func (f *EndswithFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *EndswithFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	suffix, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}
	return strings.HasSuffix(str, suffix), nil
}

// StartswithFunction 检查字符串是否以指定前缀开始
type StartswithFunction struct {
	*BaseFunction
}

func NewStartswithFunction() *StartswithFunction {
	return &StartswithFunction{
		BaseFunction: NewBaseFunction("startswith", TypeString, "字符串函数", "检查字符串是否以指定前缀开始", 2, 2),
	}
}

func (f *StartswithFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *StartswithFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	prefix, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}
	return strings.HasPrefix(str, prefix), nil
}

// IndexofFunction 返回子字符串在字符串中的位置
type IndexofFunction struct {
	*BaseFunction
}

func NewIndexofFunction() *IndexofFunction {
	return &IndexofFunction{
		BaseFunction: NewBaseFunction("indexof", TypeString, "字符串函数", "返回子字符串在字符串中的位置", 2, 2),
	}
}

func (f *IndexofFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *IndexofFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	substr, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}
	return int64(strings.Index(str, substr)), nil
}

// SubstringFunction 提取子字符串
type SubstringFunction struct {
	*BaseFunction
}

func NewSubstringFunction() *SubstringFunction {
	return &SubstringFunction{
		BaseFunction: NewBaseFunction("substring", TypeString, "字符串函数", "提取子字符串", 2, 3),
	}
}

func (f *SubstringFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *SubstringFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	start, err := cast.ToInt64E(args[1])
	if err != nil {
		return nil, err
	}
	
	strLen := int64(len(str))
	if start < 0 || start >= strLen {
		return "", nil
	}
	
	if len(args) == 2 {
		return str[start:], nil
	}
	
	length, err := cast.ToInt64E(args[2])
	if err != nil {
		return nil, err
	}
	
	end := start + length
	if end > strLen {
		end = strLen
	}
	
	return str[start:end], nil
}

// ReplaceFunction 替换字符串中的内容
type ReplaceFunction struct {
	*BaseFunction
}

func NewReplaceFunction() *ReplaceFunction {
	return &ReplaceFunction{
		BaseFunction: NewBaseFunction("replace", TypeString, "字符串函数", "替换字符串中的内容", 3, 3),
	}
}

func (f *ReplaceFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ReplaceFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	old, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}
	new, err := cast.ToStringE(args[2])
	if err != nil {
		return nil, err
	}
	return strings.ReplaceAll(str, old, new), nil
}

// SplitFunction 按分隔符分割字符串
type SplitFunction struct {
	*BaseFunction
}

func NewSplitFunction() *SplitFunction {
	return &SplitFunction{
		BaseFunction: NewBaseFunction("split", TypeString, "字符串函数", "按分隔符分割字符串", 2, 2),
	}
}

func (f *SplitFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *SplitFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	delimiter, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}
	return strings.Split(str, delimiter), nil
}

// LpadFunction 左填充字符串
type LpadFunction struct {
	*BaseFunction
}

func NewLpadFunction() *LpadFunction {
	return &LpadFunction{
		BaseFunction: NewBaseFunction("lpad", TypeString, "字符串函数", "左填充字符串", 2, 3),
	}
}

func (f *LpadFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *LpadFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	length, err := cast.ToInt64E(args[1])
	if err != nil {
		return nil, err
	}
	
	pad := " "
	if len(args) == 3 {
		pad, err = cast.ToStringE(args[2])
		if err != nil {
			return nil, err
		}
	}
	
	strLen := int64(len(str))
	if strLen >= length {
		return str, nil
	}
	
	padLen := length - strLen
	padStr := strings.Repeat(pad, int(padLen/int64(len(pad))+1))
	return padStr[:padLen] + str, nil
}

// RpadFunction 右填充字符串
type RpadFunction struct {
	*BaseFunction
}

func NewRpadFunction() *RpadFunction {
	return &RpadFunction{
		BaseFunction: NewBaseFunction("rpad", TypeString, "字符串函数", "右填充字符串", 2, 3),
	}
}

func (f *RpadFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *RpadFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	length, err := cast.ToInt64E(args[1])
	if err != nil {
		return nil, err
	}
	
	pad := " "
	if len(args) == 3 {
		pad, err = cast.ToStringE(args[2])
		if err != nil {
			return nil, err
		}
	}
	
	strLen := int64(len(str))
	if strLen >= length {
		return str, nil
	}
	
	padLen := length - strLen
	padStr := strings.Repeat(pad, int(padLen/int64(len(pad))+1))
	return str + padStr[:padLen], nil
}

// LtrimFunction 去除左侧空白字符
type LtrimFunction struct {
	*BaseFunction
}

func NewLtrimFunction() *LtrimFunction {
	return &LtrimFunction{
		BaseFunction: NewBaseFunction("ltrim", TypeString, "字符串函数", "去除左侧空白字符", 1, 1),
	}
}

func (f *LtrimFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *LtrimFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return strings.TrimLeftFunc(str, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r'
	}), nil
}

// RtrimFunction 去除右侧空白字符
type RtrimFunction struct {
	*BaseFunction
}

func NewRtrimFunction() *RtrimFunction {
	return &RtrimFunction{
		BaseFunction: NewBaseFunction("rtrim", TypeString, "字符串函数", "去除右侧空白字符", 1, 1),
	}
}

func (f *RtrimFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *RtrimFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return strings.TrimRightFunc(str, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r'
	}), nil
}

// RegexpMatchesFunction 正则表达式匹配
type RegexpMatchesFunction struct {
	*BaseFunction
}

func NewRegexpMatchesFunction() *RegexpMatchesFunction {
	return &RegexpMatchesFunction{
		BaseFunction: NewBaseFunction("regexp_matches", TypeString, "字符串函数", "正则表达式匹配", 2, 2),
	}
}

func (f *RegexpMatchesFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *RegexpMatchesFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	pattern, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}
	
	matched, err := regexp.MatchString(pattern, str)
	if err != nil {
		return nil, err
	}
	return matched, nil
}

// RegexpReplaceFunction 正则表达式替换
type RegexpReplaceFunction struct {
	*BaseFunction
}

func NewRegexpReplaceFunction() *RegexpReplaceFunction {
	return &RegexpReplaceFunction{
		BaseFunction: NewBaseFunction("regexp_replace", TypeString, "字符串函数", "正则表达式替换", 3, 3),
	}
}

func (f *RegexpReplaceFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *RegexpReplaceFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	pattern, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}
	replacement, err := cast.ToStringE(args[2])
	if err != nil {
		return nil, err
	}
	
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	return re.ReplaceAllString(str, replacement), nil
}

// RegexpSubstringFunction 正则表达式提取子字符串
type RegexpSubstringFunction struct {
	*BaseFunction
}

func NewRegexpSubstringFunction() *RegexpSubstringFunction {
	return &RegexpSubstringFunction{
		BaseFunction: NewBaseFunction("regexp_substring", TypeString, "字符串函数", "正则表达式提取子字符串", 2, 2),
	}
}

func (f *RegexpSubstringFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *RegexpSubstringFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	pattern, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}
	
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	
	match := re.FindString(str)
	return match, nil
}
