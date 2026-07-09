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
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/rulego/streamsql/utils/cast"

	"github.com/rulego/streamsql/types"
)

const (
	TypeTumbling = "tumbling"
	TypeSliding  = "sliding"
	TypeCounting = "counting"
	TypeSession  = "session"
	TypeGlobal   = "global"
)

type Window interface {
	Add(item any)
	//GetResults() []any
	Reset()
	Start()
	Stop() // Stop window operations and clean up resources
	OutputChan() <-chan []types.Row
	SetCallback(callback func([]types.Row))
	Trigger()
	GetStats() map[string]int64
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
	case TypeGlobal:
		return NewGlobalWindow(config)
	default:
		return nil, fmt.Errorf("unsupported window type: %s", config.Type)
	}
}

func (cw *CountingWindow) SetCallback(callback func([]types.Row)) {
	cw.callback = callback
}

// GetTimestamp extracts timestamp from data, falling back to time.Now() when no
// timestamp can be derived (processing-time semantics). Event-time callers that
// must reject unplaceable events should use extractTimestamp and drop on !ok,
// since substituting wall-clock time silently corrupts event-time semantics.
func GetTimestamp(data any, tsProp string, timeUnit time.Duration) time.Time {
	t, ok := extractTimestamp(data, tsProp, timeUnit)
	if !ok {
		return time.Now()
	}
	return t
}

// extractTimestamp returns the event timestamp and true when one can be derived
// from the data: a GetTimestamp() time.Time method, a tsProp field holding a
// time.Time, or a numeric epoch (int/int64/float64 — JSON decodes numbers to
// float64 — or a numeric string). A numeric epoch requires a non-zero TimeUnit;
// otherwise the unit is ambiguous (s vs ms) and it is treated as unplaceable.
// Returns (zero, false) otherwise.
func extractTimestamp(data any, tsProp string, timeUnit time.Duration) (time.Time, bool) {
	if ts, ok := data.(interface{ GetTimestamp() time.Time }); ok {
		return ts.GetTimestamp(), true
	}
	if tsProp == "" {
		return time.Time{}, false
	}
	var fieldVal any
	switch v := reflect.ValueOf(data); v.Kind() {
	case reflect.Struct:
		if f := v.FieldByName(tsProp); f.IsValid() {
			fieldVal = f.Interface()
		}
	case reflect.Map:
		if v.Type().Key().Kind() == reflect.String {
			if mv := v.MapIndex(reflect.ValueOf(tsProp)); mv.IsValid() {
				fieldVal = mv.Interface()
			}
		}
	}
	if fieldVal == nil {
		return time.Time{}, false
	}
	if t, ok := fieldVal.(time.Time); ok {
		return t, true
	}
	// Numeric/string epoch. JSON numbers arrive as float64, so accept the full
	// numeric family rather than only int64.
	timestampInt, err := cast.ToInt64E(fieldVal)
	if err != nil {
		return time.Time{}, false
	}
	if timeUnit == 0 {
		warnUnplaceableTimestamp(tsProp)
		return time.Time{}, false
	}
	return cast.ConvertIntToTime(timestampInt, timeUnit), true
}

var tsWarnOnce sync.Once

// warnUnplaceableTimestamp warns once that an event-time query is dropping
// events because a numeric timestamp field has no TIMEUNIT set (the unit is
// ambiguous). Once-per-process keeps the log from flooding under sustained input.
func warnUnplaceableTimestamp(tsProp string) {
	tsWarnOnce.Do(func() {
		log.Printf("[streamsql] event-time: numeric timestamp field %q has no TIMEUNIT set; "+
			"events are being dropped. Declare WITH (TIMESTAMP=%q, TIMEUNIT='ms'|'s'|'us'|'ns')",
			tsProp, tsProp)
	})
}
