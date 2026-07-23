package functions

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/rulego/streamsql/utils/cast"
)

// ConcatFunction concatenates multiple strings
type ConcatFunction struct {
	*BaseFunction
}

func NewConcatFunction() *ConcatFunction {
	return &ConcatFunction{
		BaseFunction: NewBaseFunction("concat", TypeString, "string", "Concatenate multiple strings", 1, -1),
	}
}

func (f *ConcatFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ConcatFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// LengthFunction returns the length of a string
type LengthFunction struct {
	*BaseFunction
}

func NewLengthFunction() *LengthFunction {
	return &LengthFunction{
		BaseFunction: NewBaseFunctionWithAliases("length", TypeString, "string", "Get length of string or array", 1, 1, []string{"len"}),
	}
}

func (f *LengthFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

// Execute calculates the length of a string or array.
// Supports strings, arrays, slices, etc., using Go's standard len() function.
func (f *LengthFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	arg := args[0]

	v := reflect.ValueOf(arg)
	var length int
	switch v.Kind() {
	case reflect.String:
		length = len(v.String())
	case reflect.Array, reflect.Slice:
		length = v.Len()
	case reflect.Map:
		length = v.Len()
	case reflect.Chan:
		length = v.Len()
	default:
		str, err := cast.ToStringE(arg)
		if err != nil {
			return nil, fmt.Errorf("unsupported type for len function: %T", arg)
		}
		length = len(str)
	}
	return length, nil
}

// UpperFunction converts string to uppercase
type UpperFunction struct {
	*BaseFunction
}

func NewUpperFunction() *UpperFunction {
	return &UpperFunction{
		BaseFunction: NewBaseFunction("upper", TypeString, "string", "Convert to uppercase", 1, 1),
	}
}

func (f *UpperFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *UpperFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return strings.ToUpper(str), nil
}

// LowerFunction converts string to lowercase
type LowerFunction struct {
	*BaseFunction
}

func NewLowerFunction() *LowerFunction {
	return &LowerFunction{
		BaseFunction: NewBaseFunction("lower", TypeString, "string", "Convert to lowercase", 1, 1),
	}
}

func (f *LowerFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *LowerFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return strings.ToLower(str), nil
}

// TrimFunction removes spaces at the beginning and end
type TrimFunction struct {
	*BaseFunction
}

func NewTrimFunction() *TrimFunction {
	return &TrimFunction{
		BaseFunction: NewBaseFunction("trim", TypeString, "字符串函数", "去除字符串首尾空格", 1, 1),
	}
}

func (f *TrimFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *TrimFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return strings.TrimSpace(str), nil
}

// FormatFunction formatting function
type FormatFunction struct {
	*BaseFunction
}

func NewFormatFunction() *FormatFunction {
	return &FormatFunction{
		BaseFunction: NewBaseFunction("format", TypeString, "字符串函数", "格式化数值或字符串", 1, 3),
	}
}

func (f *FormatFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *FormatFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	value := args[0]

	// If there is only one parameter, convert it to a string
	if len(args) == 1 {
		return cast.ToStringE(value)
	}

	// If there are format parameters,
	pattern, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, err
	}

	// Handle numerical formatting
	if val, err := cast.ToFloat64E(value); err == nil {
		// Simple numerical formatting support
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
			// Try to analyze the accuracy parameters
			if strings.Contains(pattern, ".") {
				precision := len(strings.Split(pattern, ".")[1])
				return fmt.Sprintf("%."+fmt.Sprintf("%d", precision)+"f", val), nil
			}
			return fmt.Sprintf("%.2f", val), nil
		}
	}

	// String formatting
	str, err := cast.ToStringE(value)
	if err != nil {
		return nil, err
	}

	// If there is a third parameter (locale), this simplifies the handling here
	return str, nil
}

// EndswithFunction checks whether the string ends with a specified suffix
type EndswithFunction struct {
	*BaseFunction
}

func NewEndswithFunction() *EndswithFunction {
	return &EndswithFunction{
		BaseFunction: NewBaseFunction("endswith", TypeString, "字符串函数", "检查字符串是否以指定后缀结尾", 2, 2),
	}
}

func (f *EndswithFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *EndswithFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// StartswithFunction checks whether the string starts with a specified prefix
type StartswithFunction struct {
	*BaseFunction
}

func NewStartswithFunction() *StartswithFunction {
	return &StartswithFunction{
		BaseFunction: NewBaseFunction("startswith", TypeString, "字符串函数", "检查字符串是否以指定前缀开始", 2, 2),
	}
}

func (f *StartswithFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *StartswithFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// IndexofFunction returns the position of the substring within the string
type IndexofFunction struct {
	*BaseFunction
}

func NewIndexofFunction() *IndexofFunction {
	return &IndexofFunction{
		BaseFunction: NewBaseFunction("indexof", TypeString, "字符串函数", "返回子字符串在字符串中的位置", 2, 2),
	}
}

func (f *IndexofFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *IndexofFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// SubstringFunction extracts substrings.
//
// Dialect note: positions are 0-based and a negative start counts from the end
// (Go style), which DEVIATES from ANSI SQL / MySQL / PostgreSQL where
// SUBSTRING is 1-based. So substring('hello',1,2) returns "el" here, not "he".
// This is an intentional lightweight-engine dialect choice; changing it would
// silently break existing queries.
type SubstringFunction struct {
	*BaseFunction
}

func NewSubstringFunction() *SubstringFunction {
	return &SubstringFunction{
		BaseFunction: NewBaseFunction("substring", TypeString, "字符串函数", "提取子字符串", 2, 3),
	}
}

func (f *SubstringFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *SubstringFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	start, err := cast.ToInt64E(args[1])
	if err != nil {
		return nil, err
	}

	// Slice by rune so multibyte UTF-8 characters are not split into invalid bytes.
	if len(args) == 2 {
		return substringByRune(str, start, 0, false)
	}

	length, err := cast.ToInt64E(args[2])
	if err != nil {
		return nil, err
	}
	return substringByRune(str, start, length, true)
}

// substringByRune slices str by rune (character) offsets, not bytes, so multibyte
// UTF-8 sequences are not split. start/length are rune positions; a negative start
// counts from the end.
func substringByRune(str string, start, length int64, hasLength bool) (string, error) {
	runes := []rune(str)
	runeLen := int64(len(runes))
	if start < 0 {
		start = runeLen + start
	}
	if start < 0 {
		start = 0
	}
	if start >= runeLen {
		return "", nil
	}
	if !hasLength {
		return string(runes[start:]), nil
	}
	if length < 0 {
		return "", nil
	}
	end := start + length
	if end > runeLen {
		end = runeLen
	}
	return string(runes[start:end]), nil
}

// ReplaceFunction replaces the contents of the string
type ReplaceFunction struct {
	*BaseFunction
}

func NewReplaceFunction() *ReplaceFunction {
	return &ReplaceFunction{
		BaseFunction: NewBaseFunction("replace", TypeString, "字符串函数", "替换字符串中的内容", 3, 3),
	}
}

func (f *ReplaceFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ReplaceFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// SplitFunction Splits strings by separator
type SplitFunction struct {
	*BaseFunction
}

func NewSplitFunction() *SplitFunction {
	return &SplitFunction{
		BaseFunction: NewBaseFunction("split", TypeString, "字符串函数", "按分隔符分割字符串", 2, 2),
	}
}

func (f *SplitFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *SplitFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// LpadFunction left fills the string
type LpadFunction struct {
	*BaseFunction
}

func NewLpadFunction() *LpadFunction {
	return &LpadFunction{
		BaseFunction: NewBaseFunction("lpad", TypeString, "字符串函数", "左填充字符串", 2, 3),
	}
}

func (f *LpadFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *LpadFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

	if pad == "" {
		pad = " "
	}
	padLen := length - strLen
	padStr := strings.Repeat(pad, int(padLen/int64(len(pad))+1))
	return padStr[:padLen] + str, nil
}

// RpadFunction to fill the right string
type RpadFunction struct {
	*BaseFunction
}

func NewRpadFunction() *RpadFunction {
	return &RpadFunction{
		BaseFunction: NewBaseFunction("rpad", TypeString, "字符串函数", "右填充字符串", 2, 3),
	}
}

func (f *RpadFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *RpadFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

	if pad == "" {
		pad = " "
	}
	padLen := length - strLen
	padStr := strings.Repeat(pad, int(padLen/int64(len(pad))+1))
	return str + padStr[:padLen], nil
}

// LtrimFunction removes the whitespace characters on the left
type LtrimFunction struct {
	*BaseFunction
}

func NewLtrimFunction() *LtrimFunction {
	return &LtrimFunction{
		BaseFunction: NewBaseFunction("ltrim", TypeString, "字符串函数", "去除左侧空白字符", 1, 1),
	}
}

func (f *LtrimFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *LtrimFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return strings.TrimLeftFunc(str, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r'
	}), nil
}

// RtrimFunction removes the whitespace character on the right
type RtrimFunction struct {
	*BaseFunction
}

func NewRtrimFunction() *RtrimFunction {
	return &RtrimFunction{
		BaseFunction: NewBaseFunction("rtrim", TypeString, "字符串函数", "去除右侧空白字符", 1, 1),
	}
}

func (f *RtrimFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *RtrimFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, err
	}
	return strings.TrimRightFunc(str, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r'
	}), nil
}

// RegexpMatchesFunction Regular expression matching
type RegexpMatchesFunction struct {
	*BaseFunction
}

func NewRegexpMatchesFunction() *RegexpMatchesFunction {
	return &RegexpMatchesFunction{
		BaseFunction: NewBaseFunction("regexp_matches", TypeString, "字符串函数", "正则表达式匹配", 2, 2),
	}
}

func (f *RegexpMatchesFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *RegexpMatchesFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// RegexpReplaceFunction replaces regular expressions
type RegexpReplaceFunction struct {
	*BaseFunction
}

func NewRegexpReplaceFunction() *RegexpReplaceFunction {
	return &RegexpReplaceFunction{
		BaseFunction: NewBaseFunction("regexp_replace", TypeString, "字符串函数", "正则表达式替换", 3, 3),
	}
}

func (f *RegexpReplaceFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *RegexpReplaceFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// RegexpSubstringFunction Extracts substrings from regular expressions
type RegexpSubstringFunction struct {
	*BaseFunction
}

func NewRegexpSubstringFunction() *RegexpSubstringFunction {
	return &RegexpSubstringFunction{
		BaseFunction: NewBaseFunction("regexp_substring", TypeString, "字符串函数", "正则表达式提取子字符串", 2, 2),
	}
}

func (f *RegexpSubstringFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *RegexpSubstringFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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
