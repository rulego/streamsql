/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
	Stop() // Stop window operations and clean up resources
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

// GetTimestamp extracts timestamp from data
func GetTimestamp(data interface{}, tsProp string, timeUnit time.Duration) time.Time {
	if ts, ok := data.(interface{ GetTimestamp() time.Time }); ok {
		return ts.GetTimestamp()
	} else if tsProp != "" {
		v := reflect.ValueOf(data)

		// Handle different types
		switch v.Kind() {
		case reflect.Struct:
			// If it's a struct, use reflection to get field value
			if f := v.FieldByName(tsProp); f.IsValid() {
				if t, ok := f.Interface().(time.Time); ok {
					return t
				}
			}
		case reflect.Map:
			// If it's a map, get value directly through key
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
