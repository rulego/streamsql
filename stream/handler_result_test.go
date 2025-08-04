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

package stream

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResultHandler_NewResultHandler 测试结果处理器创建
func TestResultHandler_NewResultHandler(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	handler := NewResultHandler(stream)
	assert.NotNil(t, handler)
	assert.Equal(t, stream, handler.stream)
}

// TestStream_StartSinkWorkerPool 测试启动Sink工作池
func TestStream_StartSinkWorkerPool(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// 测试默认工作池大小
	stream.startSinkWorkerPool(0)     // 传入0应该使用默认值
	time.Sleep(10 * time.Millisecond) // 等待工作池启动

	// 测试自定义工作池大小
	stream.startSinkWorkerPool(4)
	time.Sleep(10 * time.Millisecond)

	// 验证工作池可以接收任务
	var taskExecuted int32
	task := func() {
		atomic.StoreInt32(&taskExecuted, 1)
	}

	// 发送任务到工作池
	select {
	case stream.sinkWorkerPool <- task:
		// 任务成功发送
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to send task to worker pool")
	}

	// 等待任务执行
	time.Sleep(50 * time.Millisecond)
	assert.True(t, atomic.LoadInt32(&taskExecuted) == 1)
}

// TestStream_SinkWorkerPool_ErrorRecovery 测试Sink工作池错误恢复
func TestStream_SinkWorkerPool_ErrorRecovery(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	stream.startSinkWorkerPool(2)
	time.Sleep(10 * time.Millisecond)

	// 创建会panic的任务
	panicTask := func() {
		panic("test panic")
	}

	// 创建正常任务
	var normalTaskExecuted int32
	normalTask := func() {
		atomic.StoreInt32(&normalTaskExecuted, 1)
	}

	// 发送panic任务
	select {
	case stream.sinkWorkerPool <- panicTask:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to send panic task")
	}

	// 等待panic处理
	time.Sleep(50 * time.Millisecond)

	// 发送正常任务，验证工作池仍然可用
	select {
	case stream.sinkWorkerPool <- normalTask:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to send normal task after panic")
	}

	// 等待正常任务执行
	time.Sleep(50 * time.Millisecond)
	assert.True(t, atomic.LoadInt32(&normalTaskExecuted) == 1)
}

// TestStream_SinkWorkerPool_Concurrent 测试Sink工作池并发处理
func TestStream_SinkWorkerPool_Concurrent(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	stream.startSinkWorkerPool(4)
	time.Sleep(10 * time.Millisecond)

	var executedCount int64
	var wg sync.WaitGroup

	// 发送多个任务
	taskCount := 20
	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		task := func() {
			defer wg.Done()
			atomic.AddInt64(&executedCount, 1)
			time.Sleep(10 * time.Millisecond) // 模拟处理时间
		}

		select {
		case stream.sinkWorkerPool <- task:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Failed to send task %d", i)
		}
	}

	// 等待所有任务完成
	wg.Wait()

	// 验证所有任务都被执行
	assert.Equal(t, int64(taskCount), atomic.LoadInt64(&executedCount))
}

// TestStream_SinkWorkerPool_Shutdown 测试Sink工作池关闭
func TestStream_SinkWorkerPool_Shutdown(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)

	stream.startSinkWorkerPool(2)
	time.Sleep(10 * time.Millisecond)

	// 发送一个任务
	var taskExecuted int32
	task := func() {
		atomic.StoreInt32(&taskExecuted, 1)
	}

	select {
	case stream.sinkWorkerPool <- task:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to send task")
	}

	// 等待任务执行
	time.Sleep(50 * time.Millisecond)
	assert.True(t, atomic.LoadInt32(&taskExecuted) == 1)

	// 关闭stream
	func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// 等待工作协程退出
	time.Sleep(100 * time.Millisecond)

	// 验证工作池在关闭后仍然可以接收任务（通道本身没有关闭）
	// 但是没有工作协程处理这些任务
	var newTaskExecuted int32
	newTask := func() {
		atomic.StoreInt32(&newTaskExecuted, 1)
	}

	// 发送任务应该成功（通道未关闭），但任务不会被执行
	select {
	case stream.sinkWorkerPool <- newTask:
		// 任务发送成功，但不会被执行因为工作协程已退出
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Should be able to send task to channel")
	}

	// 等待一段时间，验证任务没有被执行
	time.Sleep(100 * time.Millisecond)
	assert.False(t, atomic.LoadInt32(&newTaskExecuted) == 1, "Task should not be executed after workers shutdown")
}

// TestStream_SinkWorkerPool_WorkerCount 测试不同工作池大小
func TestStream_SinkWorkerPool_WorkerCount(t *testing.T) {
	tests := []struct {
		name        string
		workerCount int
		expected    int
	}{
		{"Zero workers (default)", 0, 8},
		{"Negative workers (default)", -1, 8},
		{"Single worker", 1, 1},
		{"Multiple workers", 5, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.Config{
				SimpleFields: []string{"name", "age"},
			}
			stream, err := NewStream(config)
			require.NoError(t, err)
			defer func() {
				if stream != nil {
					close(stream.done)
				}
			}()

			stream.startSinkWorkerPool(tt.workerCount)
			time.Sleep(20 * time.Millisecond)

			// 验证工作池可以处理任务
			var taskExecuted int32
			task := func() {
				atomic.StoreInt32(&taskExecuted, 1)
			}

			select {
			case stream.sinkWorkerPool <- task:
			case <-time.After(100 * time.Millisecond):
				t.Fatal("Failed to send task")
			}

			time.Sleep(50 * time.Millisecond)
			assert.True(t, atomic.LoadInt32(&taskExecuted) == 1)
		})
	}
}
