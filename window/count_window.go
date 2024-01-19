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
	"sync/atomic"
	"time"
)

// CountWindow is a structure that represents a tumbling window
type CountWindow struct {
	context    types.SelectStreamSqlContext
	observer   types.WindowObserver
	windowSize int32 // the size of the window
	count      int32
	lastTime   time.Time // the last time the window was updated
	fieldName  string    // the name of the field that is being aggregated
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

// NewCountWindow creates a new rolling window with the given size
func NewCountWindow(fieldName string, windowSize int32, observer types.WindowObserver) *CountWindow {
	maxCapacity := 100000
	w := &CountWindow{
		fieldName:   fieldName,
		windowSize:  windowSize,
		lastTime:    time.Now(),
		maxCapacity: maxCapacity,
		queue:       queue2.NewCircleQueue(maxCapacity),
		observer:    observer,
	}

	//开始新的窗口
	w.start()
	return w
}

// 检查是否需要是下一个窗口
func (w *CountWindow) checkNextWindow() {
	w.locker.Lock()
	defer w.locker.Unlock()
	if atomic.LoadInt32(&w.count) >= w.windowSize {
		//结束当前窗口
		w.end()
		//开始新的窗口
		w.start()
	}
}

// 开始窗口事件
func (w *CountWindow) start() {
	w.startTime = time.Now()
	atomic.StoreInt32(&w.count, 0)
	w.queue.Reset()

	if w.observer.StartHandler != nil {
		w.observer.StartHandler(w.context)
	}
}

// 结束窗口事件
func (w *CountWindow) end() {
	w.endTime = time.Now()
	if w.observer.EndHandler != nil {
		w.observer.EndHandler(w.context, w.queue.PopAll())
	}
}

// 队列满，触发full事件，并重置队列
func (w *CountWindow) full() {
	if w.observer.ArchiveHandler != nil {
		w.observer.ArchiveHandler(w.context, w.queue.PopAll())
	}
	//重置队列
	w.queue.Reset()
}

// Add 添加数据
func (w *CountWindow) Add(data float64) {
	if atomic.LoadInt32(&w.count) >= w.windowSize {
		w.checkNextWindow()
	}
	if w.queue.IsFull() {
		w.full()
	}
	atomic.AddInt32(&w.count, 1)
	_ = w.queue.Push(data)
	if w.observer.AddHandler != nil {
		w.observer.AddHandler(w.context, data)
	}
}

// FieldName 获取聚合运算字段名称
func (w *CountWindow) FieldName() string {
	return w.fieldName
}

// LastData 获取最后一条数据
func (w *CountWindow) LastData() (float64, bool) {
	return w.queue.Back()
}

// StartTime 获取窗口开始时间
func (w *CountWindow) StartTime() time.Time {
	return w.startTime
}

// EndTime 获取窗口结束时间
func (w *CountWindow) EndTime() time.Time {
	return w.endTime
}

// Archive 保存数据
func (w *CountWindow) Archive() {

}
func (w *CountWindow) Stop() {

	if w.ticker != nil {
		w.ticker.Stop()
	}
	w.quit <- struct{}{}
}
