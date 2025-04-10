package window

import (
	"fmt"
	"reflect"
	"time"

	"github.com/rulego/streamsql/model"
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
	OutputChan() <-chan []model.Row
	SetCallback(callback func([]model.Row))
	Trigger()
}

func CreateWindow(config model.WindowConfig) (Window, error) {
	switch config.Type {
	case TypeTumbling:
		return NewTumblingWindow(config)
	case TypeSliding:
		return NewSlidingWindow(config)
	case TypeCounting:
		return NewCountingWindow(config)
	default:
		return nil, fmt.Errorf("unsupported window type: %s", config.Type)
	}
}

func (cw *CountingWindow) SetCallback(callback func([]model.Row)) {
	cw.callback = callback
}

// GetTimestamp 从数据中获取时间戳。
func GetTimestamp(data interface{}, tsProp string) time.Time {
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
					return value.Interface().(time.Time)
				}
			}
		}
	}
	return time.Now()
}
