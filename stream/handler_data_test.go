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
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStream_SafeGetDataChan tests safe data channel retrieval
func TestStream_SafeGetDataChan(t *testing.T) {
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

	// Test normal retrieval
	dataChan := stream.safeGetDataChan()
	assert.NotNil(t, dataChan)
	assert.Equal(t, 1000, cap(dataChan))
}

// TestStream_SafeSendToDataChan tests safe data sending to channel
func TestStream_SafeSendToDataChan_Duplicate(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize: 2, // Use small capacity for testing
			},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// Test successful send
	data1 := map[string]any{"test": "value1"}
	success := stream.safeSendToDataChan(data1)
	assert.True(t, success)

	// Test send again
	data2 := map[string]any{"test": "value2"}
	success = stream.safeSendToDataChan(data2)
	assert.True(t, success)

	// Test send failure after buffer is full
	data3 := map[string]any{"test": "value3"}
	success = stream.safeSendToDataChan(data3)
	assert.False(t, success) // Should fail because buffer is full
}

// TestStream_SafeSendToDataChan_Concurrent tests concurrent safe data sending
func TestStream_SafeSendToDataChan_Concurrent(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize: 10, // Small buffer for testing
			},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// Start consumer goroutine
	go func() {
		for {
			select {
			case <-stream.dataChan:
				// Consumption data
			case <-time.After(100 * time.Millisecond):
				return
			}
		}
	}()

	var wg sync.WaitGroup
	successCount := int64(0)
	var mu sync.Mutex

	// Initiate multiple producer coroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				data := map[string]any{
					"id":    id,
					"value": j,
				}
				if stream.safeSendToDataChan(data) {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(50 * time.Millisecond) // Wait for processing to complete

	// Verify that at least some data was successfully sent
	mu.Lock()
	assert.Greater(t, successCount, int64(0))
	mu.Unlock()
}

// TestStream_DataChanMutex Test data channel mutex
func TestStream_SafeSendToDataChan(t *testing.T) {
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

	var wg sync.WaitGroup

	// Testing concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				dataChan := stream.safeGetDataChan()
				assert.NotNil(t, dataChan)
			}
		}()
	}

	// Test concurrent sending
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				data := map[string]any{
					"id":    id,
					"value": j,
				}
				stream.safeSendToDataChan(data)
			}
		}(i)
	}

	wg.Wait()
}

// TestStream_DataHandling_EdgeCases Test data processing boundary conditions
func TestStream_SafeSendToDataChan_EdgeCases(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize: 1,
			},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			close(stream.done)
		}
	}()

	// Test empty data
	emptyData := map[string]any{}
	success := stream.safeSendToDataChan(emptyData)
	assert.True(t, success)

	// Cleared the passage
	select {
	case <-stream.dataChan:
	default:
	}

	// Test the nil value
	nilData := map[string]any{"key": nil}
	success = stream.safeSendToDataChan(nilData)
	assert.True(t, success)
}
