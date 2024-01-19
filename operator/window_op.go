/*
 * Copyright 2024 The RuleGo Authors.
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

package operator

import (
	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/window"
	"time"
)

type WindowOp struct {
	*BaseOp
	WindowType rsql.WindowType
}

func (o *WindowOp) Init(context types.StreamSqlContext) error {
	return nil
}

func (o *WindowOp) Apply(context types.StreamSqlContext) error {
	if ctx, ok := context.(types.SelectStreamSqlContext); ok && !ctx.IsInitWindow(ctx.GetCurrentGroupValues()) {
		var w types.Window
		switch o.WindowType {
		case rsql.TUMBLING_WINDOW:
			w = window.NewTumblingWindow(ctx, "", 10*time.Second, ctx.CreteWindowObserver())
		case rsql.COUNT_WINDOW:
			w = window.NewCountWindow("", 100, ctx.CreteWindowObserver())
		case rsql.SLIDING_WINDOW:
			w = window.NewSlidingWindow("", 10*time.Second, 5*time.Second, ctx.CreteWindowObserver())
		case rsql.SESSION_WINDOW:
			w = window.NewSessionWindow("", 10*time.Second, ctx.CreteWindowObserver())
		}
		ctx.AddWindow(ctx.GetCurrentGroupValues(), w)
	}
	return nil
}
