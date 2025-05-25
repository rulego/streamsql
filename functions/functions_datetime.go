package functions

import (
	"time"
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
