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

// TestStream_StartSinkWorkerPool Test the startup of the Sink working pool
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

	// Test the default working pool size
	stream.startSinkWorkerPool(0)     // Passing in 0 should use the default value
	time.Sleep(10 * time.Millisecond) // Wait for the work pool to start

	// Test the size of the custom working pool
	stream.startSinkWorkerPool(4)
	time.Sleep(10 * time.Millisecond)

	// The validation work pool can receive tasks
	var taskExecuted int32
	task := func() {
		atomic.StoreInt32(&taskExecuted, 1)
	}

	// Send tasks to the work pool
	select {
	case stream.sinkWorkerPool <- task:
		// Mission successfully sent
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to send task to worker pool")
	}

	// Waiting for the task to be executed
	time.Sleep(50 * time.Millisecond)
	assert.True(t, atomic.LoadInt32(&taskExecuted) == 1)
}

// TestStream_SinkWorkerPool_ErrorRecovery Test Sink working pool error recovery
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

	// Create tasks that can cause panic
	panicTask := func() {
		panic("test panic")
	}

	// Create normal tasks
	var normalTaskExecuted int32
	normalTask := func() {
		atomic.StoreInt32(&normalTaskExecuted, 1)
	}

	// Send a panic task
	select {
	case stream.sinkWorkerPool <- panicTask:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to send panic task")
	}

	// Waiting for panic to be handled
	time.Sleep(50 * time.Millisecond)

	// Sending normal tasks, the verification work pool remains available
	select {
	case stream.sinkWorkerPool <- normalTask:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to send normal task after panic")
	}

	// Wait for normal tasks to be executed
	time.Sleep(50 * time.Millisecond)
	assert.True(t, atomic.LoadInt32(&normalTaskExecuted) == 1)
}

// TestStream_SinkWorkerPool_Concurrent Test the concurrency of the Sink working pool
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

	// Send multiple tasks
	taskCount := 20
	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		task := func() {
			defer wg.Done()
			atomic.AddInt64(&executedCount, 1)
			time.Sleep(10 * time.Millisecond) // Simulated processing time
		}

		select {
		case stream.sinkWorkerPool <- task:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Failed to send task %d", i)
		}
	}

	// Wait for all tasks to be completed
	wg.Wait()

	// Verify that all tasks are being executed
	assert.Equal(t, int64(taskCount), atomic.LoadInt64(&executedCount))
}

// TestStream_SinkWorkerPool_Shutdown Test the Sink working pool to be closed
func TestStream_SinkWorkerPool_Shutdown(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)

	stream.startSinkWorkerPool(2)
	time.Sleep(10 * time.Millisecond)

	// Send a task
	var taskExecuted int32
	task := func() {
		atomic.StoreInt32(&taskExecuted, 1)
	}

	select {
	case stream.sinkWorkerPool <- task:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Failed to send task")
	}

	// Waiting for the task to be executed
	time.Sleep(50 * time.Millisecond)
	assert.True(t, atomic.LoadInt32(&taskExecuted) == 1)

	// Close the stream
	func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// Wait for the work coroutine to exit
	time.Sleep(100 * time.Millisecond)

	// The validation work pool can still receive tasks after closure (the channel itself is not closed).
	// But no working coroutine handles these tasks
	var newTaskExecuted int32
	newTask := func() {
		atomic.StoreInt32(&newTaskExecuted, 1)
	}

	// The sending task should succeed (the channel is not closed), but the task will not be executed
	select {
	case stream.sinkWorkerPool <- newTask:
		// The task was sent successfully, but it would not be executed because the work coroutine had exited
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Should be able to send task to channel")
	}

	// After waiting for a while, the verification task was not executed
	time.Sleep(100 * time.Millisecond)
	assert.False(t, atomic.LoadInt32(&newTaskExecuted) == 1, "Task should not be executed after workers shutdown")
}

// TestStream_SinkWorkerPool_WorkerCount Test different working pool sizes
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

			// The verification work pool can handle tasks
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
