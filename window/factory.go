package window

import (
	"fmt"
	"reflect"
	"time"

	"github.com/rulego/streamsql/utils/cast"

	"github.com/rulego/streamsql/types"
)

const (
	TypeTumbling = "tumbling"
	TypeSliding  = "sliding"
	TypeCounting = "counting"
	TypeSession  = "session"
)

type Window interface {
	Add(item interface{})
	//GetResults() []interface{}
	Reset()
	Start()
	OutputChan() <-chan []types.Row
	SetCallback(callback func([]types.Row))
	Trigger()
}

func CreateWindow(config types.WindowConfig) (Window, error) {
	switch config.Type {
	case TypeTumbling:
		return NewTumblingWindow(config)
	case TypeSliding:
		return NewSlidingWindow(config)
	case TypeCounting:
		return NewCountingWindow(config)
	case TypeSession:
		return NewSessionWindow(config)
	default:
		return nil, fmt.Errorf("unsupported window type: %s", config.Type)
	}
}

func (cw *CountingWindow) SetCallback(callback func([]types.Row)) {
	cw.callback = callback
}

// GetTimestamp 从数据中获取时间戳。
func GetTimestamp(data interface{}, tsProp string, timeUnit time.Duration) time.Time {
	if ts, ok := data.(interface{ GetTimestamp() time.Time }); ok {
		return ts.GetTimestamp()
	} else if tsProp != "" {
		v := reflect.ValueOf(data)

		// 处理不同类型
		switch v.Kind() {
		case reflect.Struct:
			// 如果是结构体，使用反射获取字段值
			if f := v.FieldByName(tsProp); f.IsValid() {
				if t, ok := f.Interface().(time.Time); ok {
					return t
				}
			}
		case reflect.Map:
			// 如果是map，直接通过key获取值
			if v.Type().Key().Kind() == reflect.String {
				if value := v.MapIndex(reflect.ValueOf(tsProp)); value.IsValid() {
					if t, ok := value.Interface().(time.Time); ok {
						return t
					} else if timestampInt, isInt := value.Interface().(int64); isInt {
						return cast.ConvertIntToTime(timestampInt, timeUnit)
					}
				}
			}
		}
	}
	return time.Now()
}
