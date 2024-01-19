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

package queue

import (
	"errors"
	"fmt"
	"sync/atomic"
)

type snapshot struct {
	head  int32 // 队首的索引，使用int32类型，方便原子操作
	tail  int32 // 队尾的索引，使用int32类型，方便原子操作
	count int32 // 队列中元素的个数，使用int32类型，方便原子操作
}
type Queue struct {
	data   []float64 // 存储数据的切片
	head   int32     // 队首的索引，使用int32类型，方便原子操作
	tail   int32     // 队尾的索引，使用int32类型，方便原子操作
	cap    int32     // 队列的大小，使用int32类型，方便原子操作
	count  int32     // 队列中元素的个数，使用int32类型，方便原子操作
	buffer []float64 // 缓冲切片，用于复用
}

// NewCircleQueue 创建一个指定大小的环形队列
func NewCircleQueue(size int) *Queue {
	return &Queue{
		data:   make([]float64, size),
		head:   0,
		tail:   0,
		cap:    int32(size),
		count:  0,
		buffer: make([]float64, size), // 初始化缓冲切片
	}
}

// IsEmpty 判断队列是否为空
func (q *Queue) IsEmpty() bool {
	return atomic.LoadInt32(&q.count) == 0 // 原子读取count的值
}

// IsFull 判断队列是否已满
func (q *Queue) IsFull() bool {
	return atomic.LoadInt32(&q.count) == q.cap // 原子读取count和size的值
}

// Push 向队尾添加一个元素，如果队列已满，返回错误
func (q *Queue) Push(x float64) error {
	for {
		if q.IsFull() {
			// 队列满时，返回错误
			return errors.New("queue is full")
		}
		tail := atomic.LoadInt32(&q.tail)                    // 原子读取tail的值
		next := (tail + 1) % q.cap                           // 计算下一个tail的值
		if atomic.CompareAndSwapInt32(&q.tail, tail, next) { // 原子比较并交换tail的值，如果成功则表示没有其他协程修改过tail
			q.data[tail] = x             // 写入数据
			atomic.AddInt32(&q.count, 1) // 原子增加count的值
			return nil                   // 返回nil表示成功
		}
		// 否则，表示有其他协程修改了tail，重新尝试
	}
}

// Pop 从队首删除一个元素，并返回它
func (q *Queue) Pop() (float64, bool) {
	for {
		if q.IsEmpty() {
			// 队列空时，返回错误
			return 0, false
		}
		head := atomic.LoadInt32(&q.head)                    // 原子读取head的值
		next := (head + 1) % q.cap                           // 计算下一个head的值
		if atomic.CompareAndSwapInt32(&q.head, head, next) { // 原子比较并交换head的值，如果成功则表示没有其他协程修改过head
			x := q.data[head]             // 读取数据
			atomic.AddInt32(&q.count, -1) // 原子减少count的值
			return x, true                // 返回数据和成功标志
		}
		// 否则，表示有其他协程修改了head，重新尝试
	}
}

// Back 返回队尾元素，不出队
func (q *Queue) Back() (float64, bool) {
	if q.IsEmpty() {
		// 队列空时，返回错误
		return 0, false
	}
	// 队尾元素的索引是 (q.tail - 1 + q.cap) % q.cap
	tail := atomic.LoadInt32(&q.tail) // 原子读取tail的值
	x := q.data[(tail-1+q.cap)%q.cap]
	return x, true
}

// PopAll 返回并删除队列中的所有元素，并重置队列的状态
func (q *Queue) PopAll() []float64 {
	if q.IsEmpty() {
		// 队列空时，返回空切片
		return nil
	}
	// 复用缓冲切片，避免内存浪费
	slice := q.buffer
	if q.head < q.tail {
		// 队列中的元素是连续的，直接截取切片
		slice = slice[:q.tail-q.head]
		copy(slice, q.data[q.head:q.tail])
	} else {
		// 队列中的元素是环形的，需要拼接两部分切片
		slice = slice[:q.cap-q.head+q.tail]
		copy(slice, append(q.data[q.head:], q.data[:q.tail]...))
	}

	//重置队列的状态
	q.Reset()
	return slice
}

// RemoveRange 删除队列中的指定范围元素
func (q *Queue) RemoveRange(head, tail int32) {
	if q.IsEmpty() {
		return
	}
	var sliceLen int32
	if head < tail {
		sliceLen = tail - head
	} else {
		sliceLen = q.cap - head + tail
	}

	// 原子重置head，tail和count的值
	atomic.StoreInt32(&q.head, tail)
	atomic.StoreInt32(&q.count, q.count-sliceLen)
}

// Reset 清空队列中的所有元素，但不释放内存空间，只是重置队列的状态
func (q *Queue) Reset() {
	// 原子重置head，tail和count的值
	atomic.StoreInt32(&q.head, 0)
	atomic.StoreInt32(&q.tail, 0)
	atomic.StoreInt32(&q.count, 0)
}

func (q *Queue) Count() int32 {
	return q.count
}

// Print 打印队列中的元素
func (q *Queue) Print() {
	fmt.Println("队列中的元素：")
	for i := int32(0); i < atomic.LoadInt32(&q.count); i++ { // 原子读取count的值
		fmt.Printf("%f ", q.data[(atomic.LoadInt32(&q.head)+i)%q.cap]) // 原子读取head的值
	}
	fmt.Println()
}

func main() {
	// 创建一个大小为 5 的环形队列
	q := NewCircleQueue(5)
	// 向队列中添加元素
	q.Push(1.1)
	q.Push(2.2)
	q.Push(3.3)
	q.Push(4.4)
	q.Push(5.5)
	// 打印队列中的元素
	q.Print()
	// 从队列中删除元素
	x, ok := q.Pop()
	if ok {
		fmt.Println("删除的元素：", x)
	}
	// 打印队列中的元素
	q.Print()
	// 向队列中添加元素
	q.Push(6)
	// 打印队列中的元素
	q.Print()

	q.Push(7)
	q.Print()
	fmt.Println(q.Back())
}
