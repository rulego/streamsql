package functions

import (
	"fmt"
	"strings"
	"time"

	"github.com/rulego/streamsql/utils/cast"
)

// NowFunction 当前时间函数
type NowFunction struct {
	*BaseFunction
}

func NewNowFunction() *NowFunction {
	return &NowFunction{
		BaseFunction: NewBaseFunction("now", TypeDateTime, "时间日期函数", "获取当前时间戳", 0, 0),
	}
}

func (f *NowFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *NowFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return time.Now().Unix(), nil
}

// CurrentTimeFunction 当前时间函数
type CurrentTimeFunction struct {
	*BaseFunction
}

func NewCurrentTimeFunction() *CurrentTimeFunction {
	return &CurrentTimeFunction{
		BaseFunction: NewBaseFunction("current_time", TypeDateTime, "时间日期函数", "获取当前时间（HH:MM:SS）", 0, 0),
	}
}

func (f *CurrentTimeFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CurrentTimeFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	now := time.Now()
	return now.Format("15:04:05"), nil
}

// CurrentDateFunction 当前日期函数
type CurrentDateFunction struct {
	*BaseFunction
}

func NewCurrentDateFunction() *CurrentDateFunction {
	return &CurrentDateFunction{
		BaseFunction: NewBaseFunction("current_date", TypeDateTime, "时间日期函数", "获取当前日期（YYYY-MM-DD）", 0, 0),
	}
}

func (f *CurrentDateFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CurrentDateFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	now := time.Now()
	return now.Format("2006-01-02"), nil
}

// DateAddFunction 日期加法函数
type DateAddFunction struct {
	*BaseFunction
}

func NewDateAddFunction() *DateAddFunction {
	return &DateAddFunction{
		BaseFunction: NewBaseFunction("date_add", TypeDateTime, "时间日期函数", "日期加法", 3, 3),
	}
}

func (f *DateAddFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *DateAddFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	interval, err := cast.ToInt64E(args[1])
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %v", err)
	}

	unit, err := cast.ToStringE(args[2])
	if err != nil {
		return nil, fmt.Errorf("invalid unit: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		// 尝试其他格式
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	switch strings.ToLower(unit) {
	case "year", "years":
		t = t.AddDate(int(interval), 0, 0)
	case "month", "months":
		t = t.AddDate(0, int(interval), 0)
	case "day", "days":
		t = t.AddDate(0, 0, int(interval))
	case "hour", "hours":
		t = t.Add(time.Duration(interval) * time.Hour)
	case "minute", "minutes":
		t = t.Add(time.Duration(interval) * time.Minute)
	case "second", "seconds":
		t = t.Add(time.Duration(interval) * time.Second)
	default:
		return nil, fmt.Errorf("unsupported unit: %s", unit)
	}

	return t.Format("2006-01-02 15:04:05"), nil
}

// DateSubFunction 日期减法函数
type DateSubFunction struct {
	*BaseFunction
}

func NewDateSubFunction() *DateSubFunction {
	return &DateSubFunction{
		BaseFunction: NewBaseFunction("date_sub", TypeDateTime, "时间日期函数", "日期减法", 3, 3),
	}
}

func (f *DateSubFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *DateSubFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	interval, err := cast.ToInt64E(args[1])
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %v", err)
	}

	unit, err := cast.ToStringE(args[2])
	if err != nil {
		return nil, fmt.Errorf("invalid unit: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	switch strings.ToLower(unit) {
	case "year", "years":
		t = t.AddDate(-int(interval), 0, 0)
	case "month", "months":
		t = t.AddDate(0, -int(interval), 0)
	case "day", "days":
		t = t.AddDate(0, 0, -int(interval))
	case "hour", "hours":
		t = t.Add(-time.Duration(interval) * time.Hour)
	case "minute", "minutes":
		t = t.Add(-time.Duration(interval) * time.Minute)
	case "second", "seconds":
		t = t.Add(-time.Duration(interval) * time.Second)
	default:
		return nil, fmt.Errorf("unsupported unit: %s", unit)
	}

	return t.Format("2006-01-02 15:04:05"), nil
}

// DateDiffFunction 日期差函数
type DateDiffFunction struct {
	*BaseFunction
}

func NewDateDiffFunction() *DateDiffFunction {
	return &DateDiffFunction{
		BaseFunction: NewBaseFunction("date_diff", TypeDateTime, "时间日期函数", "计算日期差", 3, 3),
	}
}

func (f *DateDiffFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *DateDiffFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	date1Str, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date1: %v", err)
	}

	date2Str, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, fmt.Errorf("invalid date2: %v", err)
	}

	unit, err := cast.ToStringE(args[2])
	if err != nil {
		return nil, fmt.Errorf("invalid unit: %v", err)
	}

	t1, err := time.Parse("2006-01-02 15:04:05", date1Str)
	if err != nil {
		if t1, err = time.Parse("2006-01-02", date1Str); err != nil {
			return nil, fmt.Errorf("invalid date1 format: %v", err)
		}
	}

	t2, err := time.Parse("2006-01-02 15:04:05", date2Str)
	if err != nil {
		if t2, err = time.Parse("2006-01-02", date2Str); err != nil {
			return nil, fmt.Errorf("invalid date2 format: %v", err)
		}
	}

	diff := t1.Sub(t2)

	switch strings.ToLower(unit) {
	case "year", "years":
		return int64(diff.Hours() / (24 * 365)), nil
	case "month", "months":
		return int64(diff.Hours() / (24 * 30)), nil
	case "day", "days":
		return int64(diff.Hours() / 24), nil
	case "hour", "hours":
		return int64(diff.Hours()), nil
	case "minute", "minutes":
		return int64(diff.Minutes()), nil
	case "second", "seconds":
		return int64(diff.Seconds()), nil
	default:
		return nil, fmt.Errorf("unsupported unit: %s", unit)
	}
}

// DateFormatFunction 日期格式化函数
type DateFormatFunction struct {
	*BaseFunction
}

func NewDateFormatFunction() *DateFormatFunction {
	return &DateFormatFunction{
		BaseFunction: NewBaseFunction("date_format", TypeDateTime, "时间日期函数", "格式化日期", 2, 2),
	}
}

func (f *DateFormatFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *DateFormatFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	format, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, fmt.Errorf("invalid format: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	// 转换常见的格式字符串
	goFormat := convertToGoFormat(format)
	return t.Format(goFormat), nil
}

// convertToGoFormat 将常见的日期格式转换为Go的时间格式
func convertToGoFormat(format string) string {
	// 按照长度从长到短的顺序替换，避免短的模式覆盖长的模式
	replacements := []struct {
		old string
		new string
	}{
		{"YYYY", "2006"},
		{"yyyy", "2006"},
		{"YY", "06"},
		{"yy", "06"},
		{"MM", "01"},
		{"mm", "01"},
		{"DD", "02"},
		{"dd", "02"},
		{"HH", "15"},
		{"hh", "15"},
		{"MI", "04"},
		{"mi", "04"},
		{"SS", "05"},
		{"ss", "05"},
	}

	result := format
	for _, r := range replacements {
		result = strings.ReplaceAll(result, r.old, r.new)
	}
	return result
}

// DateParseFunction 日期解析函数
type DateParseFunction struct {
	*BaseFunction
}

func NewDateParseFunction() *DateParseFunction {
	return &DateParseFunction{
		BaseFunction: NewBaseFunction("date_parse", TypeDateTime, "时间日期函数", "解析日期字符串", 2, 2),
	}
}

func (f *DateParseFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *DateParseFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date string: %v", err)
	}

	format, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, fmt.Errorf("invalid format: %v", err)
	}

	goFormat := convertToGoFormat(format)
	t, err := time.Parse(goFormat, dateStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse date: %v", err)
	}

	return t.Format("2006-01-02 15:04:05"), nil
}

// ExtractFunction 提取日期部分函数
type ExtractFunction struct {
	*BaseFunction
}

func NewExtractFunction() *ExtractFunction {
	return &ExtractFunction{
		BaseFunction: NewBaseFunction("extract", TypeDateTime, "时间日期函数", "提取日期部分", 2, 2),
	}
}

func (f *ExtractFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ExtractFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	unit, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid unit: %v", err)
	}

	dateStr, err := cast.ToStringE(args[1])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	switch strings.ToLower(unit) {
	case "year":
		return t.Year(), nil
	case "month":
		return int(t.Month()), nil
	case "day":
		return t.Day(), nil
	case "hour":
		return t.Hour(), nil
	case "minute":
		return t.Minute(), nil
	case "second":
		return t.Second(), nil
	case "weekday":
		return int(t.Weekday()), nil
	case "yearday":
		return t.YearDay(), nil
	default:
		return nil, fmt.Errorf("unsupported unit: %s", unit)
	}
}

// UnixTimestampFunction Unix时间戳函数
type UnixTimestampFunction struct {
	*BaseFunction
}

func NewUnixTimestampFunction() *UnixTimestampFunction {
	return &UnixTimestampFunction{
		BaseFunction: NewBaseFunction("unix_timestamp", TypeDateTime, "时间日期函数", "转换为Unix时间戳", 1, 1),
	}
}

func (f *UnixTimestampFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *UnixTimestampFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	return t.Unix(), nil
}

// FromUnixtimeFunction 从Unix时间戳转换函数
type FromUnixtimeFunction struct {
	*BaseFunction
}

func NewFromUnixtimeFunction() *FromUnixtimeFunction {
	return &FromUnixtimeFunction{
		BaseFunction: NewBaseFunction("from_unixtime", TypeDateTime, "时间日期函数", "从Unix时间戳转换", 1, 1),
	}
}

func (f *FromUnixtimeFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *FromUnixtimeFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	timestamp, err := cast.ToInt64E(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %v", err)
	}

	t := time.Unix(timestamp, 0).UTC()
	return t.Format("2006-01-02 15:04:05"), nil
}

// YearFunction 提取年份函数
type YearFunction struct {
	*BaseFunction
}

func NewYearFunction() *YearFunction {
	return &YearFunction{
		BaseFunction: NewBaseFunction("year", TypeDateTime, "时间日期函数", "提取年份", 1, 1),
	}
}

func (f *YearFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *YearFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	return t.Year(), nil
}

// MonthFunction 提取月份函数
type MonthFunction struct {
	*BaseFunction
}

func NewMonthFunction() *MonthFunction {
	return &MonthFunction{
		BaseFunction: NewBaseFunction("month", TypeDateTime, "时间日期函数", "提取月份", 1, 1),
	}
}

func (f *MonthFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *MonthFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	return int(t.Month()), nil
}

// DayFunction 提取日期函数
type DayFunction struct {
	*BaseFunction
}

func NewDayFunction() *DayFunction {
	return &DayFunction{
		BaseFunction: NewBaseFunction("day", TypeDateTime, "时间日期函数", "提取日期", 1, 1),
	}
}

func (f *DayFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *DayFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	return t.Day(), nil
}

// HourFunction 提取小时函数
type HourFunction struct {
	*BaseFunction
}

func NewHourFunction() *HourFunction {
	return &HourFunction{
		BaseFunction: NewBaseFunction("hour", TypeDateTime, "时间日期函数", "提取小时", 1, 1),
	}
}

func (f *HourFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *HourFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	return t.Hour(), nil
}

// MinuteFunction 提取分钟函数
type MinuteFunction struct {
	*BaseFunction
}

func NewMinuteFunction() *MinuteFunction {
	return &MinuteFunction{
		BaseFunction: NewBaseFunction("minute", TypeDateTime, "时间日期函数", "提取分钟", 1, 1),
	}
}

func (f *MinuteFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *MinuteFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	return t.Minute(), nil
}

// SecondFunction 提取秒数函数
type SecondFunction struct {
	*BaseFunction
}

func NewSecondFunction() *SecondFunction {
	return &SecondFunction{
		BaseFunction: NewBaseFunction("second", TypeDateTime, "时间日期函数", "提取秒数", 1, 1),
	}
}

func (f *SecondFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *SecondFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	return t.Second(), nil
}

// DayOfWeekFunction 获取星期几函数
type DayOfWeekFunction struct {
	*BaseFunction
}

func NewDayOfWeekFunction() *DayOfWeekFunction {
	return &DayOfWeekFunction{
		BaseFunction: NewBaseFunction("dayofweek", TypeDateTime, "时间日期函数", "获取星期几", 1, 1),
	}
}

func (f *DayOfWeekFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *DayOfWeekFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	return int(t.Weekday()), nil
}

// DayOfYearFunction 获取一年中的第几天函数
type DayOfYearFunction struct {
	*BaseFunction
}

func NewDayOfYearFunction() *DayOfYearFunction {
	return &DayOfYearFunction{
		BaseFunction: NewBaseFunction("dayofyear", TypeDateTime, "时间日期函数", "获取一年中的第几天", 1, 1),
	}
}

func (f *DayOfYearFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *DayOfYearFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	return t.YearDay(), nil
}

// WeekOfYearFunction 获取一年中的第几周函数
type WeekOfYearFunction struct {
	*BaseFunction
}

func NewWeekOfYearFunction() *WeekOfYearFunction {
	return &WeekOfYearFunction{
		BaseFunction: NewBaseFunction("weekofyear", TypeDateTime, "时间日期函数", "获取一年中的第几周", 1, 1),
	}
}

func (f *WeekOfYearFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *WeekOfYearFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	dateStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date: %v", err)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		if t, err = time.Parse("2006-01-02", dateStr); err != nil {
			return nil, fmt.Errorf("invalid date format: %v", err)
		}
	}

	_, week := t.ISOWeek()
	return week, nil
}
