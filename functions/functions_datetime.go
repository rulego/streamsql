package functions

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/rulego/streamsql/utils/cast"
)

// durationFromInterval multiplies interval by unit, returning an error on
// time.Duration (int64 nanoseconds) overflow instead of silently wrapping to a
// wrong or sign-flipped duration. unit must be a positive Duration.
func durationFromInterval(interval int64, unit time.Duration) (time.Duration, error) {
	if interval == 0 {
		return 0, nil
	}
	if interval > math.MaxInt64/int64(unit) || interval < math.MinInt64/int64(unit) {
		return 0, fmt.Errorf("interval %d overflows time.Duration for unit %v", interval, unit)
	}
	return time.Duration(interval) * unit, nil
}

// dateArgString Convert the date parameter into a string that can be time.Parse. time.Time Entry (e.g., now())
// Format according to "2006-01-02 15:04:05" to avoid time.Time.String() in the timezone/nanosecond text causes parsing failure;
// The rest of the types go cast.ToStringE. Unified use for date functions such as year/month/date_add.
func dateArgString(v any) (string, error) {
	if t, ok := v.(time.Time); ok {
		return t.Format("2006-01-02 15:04:05"), nil
	}
	return cast.ToStringE(v)
}

// NowFunction returns current timestamp
type NowFunction struct {
	*BaseFunction
}

func NewNowFunction() *NowFunction {
	return &NowFunction{
		BaseFunction: NewBaseFunction("now", TypeDateTime, "datetime", "Get current timestamp", 0, 0),
	}
}

func (f *NowFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *NowFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return time.Now(), nil
}

// CurrentTimeFunction returns current time
type CurrentTimeFunction struct {
	*BaseFunction
}

func NewCurrentTimeFunction() *CurrentTimeFunction {
	return &CurrentTimeFunction{
		BaseFunction: NewBaseFunction("current_time", TypeDateTime, "datetime", "Get current time (HH:MM:SS)", 0, 0),
	}
}

func (f *CurrentTimeFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *CurrentTimeFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	now := time.Now()
	return now.Format("15:04:05"), nil
}

// CurrentDateFunction returns current date
type CurrentDateFunction struct {
	*BaseFunction
}

func NewCurrentDateFunction() *CurrentDateFunction {
	return &CurrentDateFunction{
		BaseFunction: NewBaseFunction("current_date", TypeDateTime, "datetime", "Get current date (YYYY-MM-DD)", 0, 0),
	}
}

func (f *CurrentDateFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *CurrentDateFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	now := time.Now()
	return now.Format("2006-01-02"), nil
}

// DateAddFunction performs date addition
type DateAddFunction struct {
	*BaseFunction
}

func NewDateAddFunction() *DateAddFunction {
	return &DateAddFunction{
		BaseFunction: NewBaseFunction("date_add", TypeDateTime, "datetime", "Date addition", 3, 3),
	}
}

func (f *DateAddFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *DateAddFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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
		// Try other formats
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
		d, err := durationFromInterval(interval, time.Hour)
		if err != nil {
			return nil, err
		}
		t = t.Add(d)
	case "minute", "minutes":
		d, err := durationFromInterval(interval, time.Minute)
		if err != nil {
			return nil, err
		}
		t = t.Add(d)
	case "second", "seconds":
		d, err := durationFromInterval(interval, time.Second)
		if err != nil {
			return nil, err
		}
		t = t.Add(d)
	default:
		return nil, fmt.Errorf("unsupported unit: %s", unit)
	}

	return t.Format("2006-01-02 15:04:05"), nil
}

// DateSubFunction is a date subtraction function
type DateSubFunction struct {
	*BaseFunction
}

func NewDateSubFunction() *DateSubFunction {
	return &DateSubFunction{
		BaseFunction: NewBaseFunction("date_sub", TypeDateTime, "时间日期函数", "日期减法", 3, 3),
	}
}

func (f *DateSubFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *DateSubFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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
		d, err := durationFromInterval(interval, time.Hour)
		if err != nil {
			return nil, err
		}
		t = t.Add(-d)
	case "minute", "minutes":
		d, err := durationFromInterval(interval, time.Minute)
		if err != nil {
			return nil, err
		}
		t = t.Add(-d)
	case "second", "seconds":
		d, err := durationFromInterval(interval, time.Second)
		if err != nil {
			return nil, err
		}
		t = t.Add(-d)
	default:
		return nil, fmt.Errorf("unsupported unit: %s", unit)
	}

	return t.Format("2006-01-02 15:04:05"), nil
}

// DateDiffFunction
type DateDiffFunction struct {
	*BaseFunction
}

func NewDateDiffFunction() *DateDiffFunction {
	return &DateDiffFunction{
		BaseFunction: NewBaseFunction("date_diff", TypeDateTime, "时间日期函数", "计算日期差", 3, 3),
	}
}

func (f *DateDiffFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *DateDiffFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	date1Str, err := dateArgString(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid date1: %v", err)
	}

	date2Str, err := dateArgString(args[1])
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

// DateFormatFunction Date formatting function
type DateFormatFunction struct {
	*BaseFunction
}

func NewDateFormatFunction() *DateFormatFunction {
	return &DateFormatFunction{
		BaseFunction: NewBaseFunction("date_format", TypeDateTime, "时间日期函数", "格式化日期", 2, 2),
	}
}

func (f *DateFormatFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *DateFormatFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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

	// Convert common format strings
	goFormat := convertToGoFormat(format)
	return t.Format(goFormat), nil
}

// convertToGoFormat converts common date formats into Go time formats
func convertToGoFormat(format string) string {
	// Replace them in order from length to shortest, avoiding short patterns overriding long patterns
	replacements := []struct {
		old string
		new string
	}{
		{"YYYY", "2006"},
		{"yyyy", "2006"},
		{"YY", "06"},
		{"yy", "06"},
		{"MM", "01"}, // month (case-sensitive: uppercase M = month)
		{"mm", "04"}, // minute (lowercase m = minute, Java/JS convention)
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

// DateParseFunction Date parsing function
type DateParseFunction struct {
	*BaseFunction
}

func NewDateParseFunction() *DateParseFunction {
	return &DateParseFunction{
		BaseFunction: NewBaseFunction("date_parse", TypeDateTime, "时间日期函数", "解析日期字符串", 2, 2),
	}
}

func (f *DateParseFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *DateParseFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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

// ExtractFunction Extract the date part function
type ExtractFunction struct {
	*BaseFunction
}

func NewExtractFunction() *ExtractFunction {
	return &ExtractFunction{
		BaseFunction: NewBaseFunction("extract", TypeDateTime, "时间日期函数", "提取日期部分", 2, 2),
	}
}

func (f *ExtractFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ExtractFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	unit, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid unit: %v", err)
	}

	dateStr, err := dateArgString(args[1])
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

// UnixTimestampFunction: Unix timestamp function
type UnixTimestampFunction struct {
	*BaseFunction
}

func NewUnixTimestampFunction() *UnixTimestampFunction {
	return &UnixTimestampFunction{
		BaseFunction: NewBaseFunction("unix_timestamp", TypeDateTime, "时间日期函数", "转换为Unix时间戳", 0, 1),
	}
}

func (f *UnixTimestampFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *UnixTimestampFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// Returns the current Unix second without parameters
	if len(args) == 0 {
		return time.Now().Unix(), nil
	}

	switch v := args[0].(type) {
	case time.Time:
		return v.Unix(), nil
	case int, int32, int64, float32, float64:
		sec, err := cast.ToInt64E(v)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp: %v", err)
		}
		return sec, nil
	case string:
		t, err := time.Parse("2006-01-02 15:04:05", v)
		if err != nil {
			if t, err = time.Parse("2006-01-02", v); err != nil {
				return nil, fmt.Errorf("invalid date format: %v", err)
			}
		}
		return t.Unix(), nil
	default:
		return nil, fmt.Errorf("invalid date type: %T", args[0])
	}
}

// FromUnixtimeFunction: A function that converts Unix timestamps
type FromUnixtimeFunction struct {
	*BaseFunction
}

func NewFromUnixtimeFunction() *FromUnixtimeFunction {
	return &FromUnixtimeFunction{
		BaseFunction: NewBaseFunction("from_unixtime", TypeDateTime, "时间日期函数", "从Unix时间戳转换", 1, 1),
	}
}

func (f *FromUnixtimeFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *FromUnixtimeFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	timestamp, err := cast.ToInt64E(args[0])
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %v", err)
	}

	t := time.Unix(timestamp, 0).UTC()
	return t.Format("2006-01-02 15:04:05"), nil
}

// YearFunction extracts the year function
type YearFunction struct {
	*BaseFunction
}

func NewYearFunction() *YearFunction {
	return &YearFunction{
		BaseFunction: NewBaseFunction("year", TypeDateTime, "时间日期函数", "提取年份", 1, 1),
	}
}

func (f *YearFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *YearFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// Try converting to strings and parsing
	dateStr, err := dateArgString(args[0])
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

// MonthFunction extracts the month function
type MonthFunction struct {
	*BaseFunction
}

func NewMonthFunction() *MonthFunction {
	return &MonthFunction{
		BaseFunction: NewBaseFunction("month", TypeDateTime, "时间日期函数", "提取月份", 1, 1),
	}
}

func (f *MonthFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *MonthFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// Convert to strings and parse
	dateStr, err := dateArgString(args[0])
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

// DayFunction extracts the date function
type DayFunction struct {
	*BaseFunction
}

func NewDayFunction() *DayFunction {
	return &DayFunction{
		BaseFunction: NewBaseFunction("day", TypeDateTime, "时间日期函数", "提取日期", 1, 1),
	}
}

func (f *DayFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *DayFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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

// HourFunction extracts hourly functions
type HourFunction struct {
	*BaseFunction
}

func NewHourFunction() *HourFunction {
	return &HourFunction{
		BaseFunction: NewBaseFunction("hour", TypeDateTime, "时间日期函数", "提取小时", 1, 1),
	}
}

func (f *HourFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *HourFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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

// MinuteFunction extracts the minute function
type MinuteFunction struct {
	*BaseFunction
}

func NewMinuteFunction() *MinuteFunction {
	return &MinuteFunction{
		BaseFunction: NewBaseFunction("minute", TypeDateTime, "时间日期函数", "提取分钟", 1, 1),
	}
}

func (f *MinuteFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *MinuteFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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

// SecondFunction extracts the seconds function
type SecondFunction struct {
	*BaseFunction
}

func NewSecondFunction() *SecondFunction {
	return &SecondFunction{
		BaseFunction: NewBaseFunction("second", TypeDateTime, "时间日期函数", "提取秒数", 1, 1),
	}
}

func (f *SecondFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *SecondFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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

// DayOfWeekFunction to get the day of the week
type DayOfWeekFunction struct {
	*BaseFunction
}

func NewDayOfWeekFunction() *DayOfWeekFunction {
	return &DayOfWeekFunction{
		BaseFunction: NewBaseFunction("dayofweek", TypeDateTime, "时间日期函数", "获取星期几", 1, 1),
	}
}

func (f *DayOfWeekFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *DayOfWeekFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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

// DayOfYearFunction Gets the day of the year
type DayOfYearFunction struct {
	*BaseFunction
}

func NewDayOfYearFunction() *DayOfYearFunction {
	return &DayOfYearFunction{
		BaseFunction: NewBaseFunction("dayofyear", TypeDateTime, "时间日期函数", "获取一年中的第几天", 1, 1),
	}
}

func (f *DayOfYearFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *DayOfYearFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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

// WeekOfYearFunction gets the week of the year
type WeekOfYearFunction struct {
	*BaseFunction
}

func NewWeekOfYearFunction() *WeekOfYearFunction {
	return &WeekOfYearFunction{
		BaseFunction: NewBaseFunction("weekofyear", TypeDateTime, "时间日期函数", "获取一年中的第几周", 1, 1),
	}
}

func (f *WeekOfYearFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *WeekOfYearFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	dateStr, err := dateArgString(args[0])
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
