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

package window

import (
	"github.com/rulego/streamsql/types"
	queue2 "github.com/rulego/streamsql/utils/queue"
	"sync"
	"time"
)

// TumblingWindow is a structure that represents a tumbling window
type TumblingWindow struct {
	context    types.StreamSqlOperatorContext
	observer   types.WindowObserver
	windowSize time.Duration // the size of the window

	lastTime  time.Time // the last time the window was updated
	fieldName string    // the name of the field that is being aggregated
	//maxCapacity 最大容量
	maxCapacity int
	queue       *queue2.Queue
	startTime   time.Time
	endTime     time.Time
	locker      sync.Mutex
	ticker      *time.Ticker
	//退出标志
	quit chan struct{}
}

// NewTumblingWindow creates a new rolling window with the given size
func NewTumblingWindow(context types.StreamSqlOperatorContext, fieldName string, windowSize time.Duration, observer types.WindowObserver) *TumblingWindow {
	maxCapacity := 100000
	w := &TumblingWindow{
		context:     context,
		fieldName:   fieldName,
		windowSize:  windowSize,
		lastTime:    time.Now(),
		maxCapacity: maxCapacity,
		queue:       queue2.NewCircleQueue(maxCapacity),
		observer:    observer,
	}

	//开始新的窗口
	w.start()
	// 创建一个通道，用于通知ticker的结束
	w.quit = make(chan struct{})
	//开始定时器
	w.ticker = time.NewTicker(windowSize)
	go func() {
		for {
			select {
			case <-w.ticker.C:
				w.checkNextWindow()
			case <-w.quit:
				return
			}
		}
	}()
	return w
}

// 检查是否需要是下一个窗口
func (w *TumblingWindow) checkNextWindow() {
	//w.locker.Lock()
	//defer w.locker.Unlock()
	if time.Now().Sub(w.startTime) > w.windowSize {
		//结束当前窗口
		w.end()
		//开始新的窗口
		w.start()
	}
}

// 开始窗口事件
func (w *TumblingWindow) start() {
	w.locker.Lock()
	w.startTime = time.Now()
	w.queue.Reset()
	w.locker.Unlock()
	if w.observer.StartHandler != nil {
		w.observer.StartHandler(w.context)
	}
}

// 结束窗口事件
func (w *TumblingWindow) end() {
	w.endTime = time.Now()
	if w.observer.EndHandler != nil {
		w.locker.Lock()
		values := w.queue.PopAll()
		w.locker.Unlock()

		w.observer.EndHandler(w.context, values)
	}
}

// 队列满，触发full事件，并重置队列
func (w *TumblingWindow) full() {
	if w.observer.ArchiveHandler != nil {
		w.observer.ArchiveHandler(w.context, w.queue.PopAll())
	}
	//重置队列
	w.queue.Reset()
}

// Add 添加数据
func (w *TumblingWindow) Add(data float64) {
	if time.Now().Sub(w.startTime) > w.windowSize {
		w.checkNextWindow()
	}
	if w.queue.IsFull() {
		w.full()
	}

	_ = w.queue.Push(data)
	if w.observer.AddHandler != nil {
		w.observer.AddHandler(w.context, data)
	}
}

// FieldName 获取聚合运算字段名称
func (w *TumblingWindow) FieldName() string {
	return w.fieldName
}

// LastData 获取最后一条数据
func (w *TumblingWindow) LastData() (float64, bool) {
	return w.queue.Back()
}

// StartTime 获取窗口开始时间
func (w *TumblingWindow) StartTime() time.Time {
	return w.startTime
}

// EndTime 获取窗口结束时间
func (w *TumblingWindow) EndTime() time.Time {
	return w.endTime
}

// Archive 保存数据
func (w *TumblingWindow) Archive() {

}
func (w *TumblingWindow) Stop() {

	if w.ticker != nil {
		w.ticker.Stop()
	}
	w.quit <- struct{}{}
}
