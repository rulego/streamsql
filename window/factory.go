package window

import (
	"fmt"
	"github.com/spf13/cast"
)

type Window interface {
	Add(item interface{})
	GetResults() []interface{}
	Reset()
	Start()
	OutputChan() <-chan []interface{}
	SetCallback(callback func([]interface{}))
	Trigger()
}

func CreateWindow(windowType string, params map[string]interface{}) (Window, error) {
	switch windowType {
	case "tumbling":
		size, err := cast.ToDurationE(params["size"])
		if err != nil {
			return nil, fmt.Errorf("invalid size for tumbling window: %v", err)
		}
		return NewTumblingWindow(size), nil
	case "sliding":
		size, err := cast.ToDurationE(params["size"])
		if err != nil {
			return nil, fmt.Errorf("invalid size for sliding window: %v", err)
		}
		slide, err := cast.ToDurationE(params["slide"])
		if err != nil {
			return nil, fmt.Errorf("invalid slide for sliding window: %v", err)
		}
		return NewSlidingWindow(size, slide), nil
	case "counting":
		count := cast.ToInt(params["count"])
		if count <= 0 {
			return nil, fmt.Errorf("count must be a positive integer")
		}
		cw := NewCountingWindow(count, nil)
		if callback, ok := params["callback"].(func([]interface{})); ok {
			cw.SetCallback(callback)
		}
		return cw, nil
	default:
		return nil, fmt.Errorf("unsupported window type: %s", windowType)
	}
}

func (cw *CountingWindow) SetCallback(callback func([]interface{})) {
	cw.callback = callback
}
