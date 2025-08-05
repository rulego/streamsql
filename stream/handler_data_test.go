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

// TestDataHandler_NewDataHandler 测试数据处理器创建
func TestDataHandler_Constructor(t *testing.T) {
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

	handler := NewDataHandler(stream)
	assert.NotNil(t, handler)
	assert.Equal(t, stream, handler.stream)
}

// TestStream_SafeGetDataChan 测试安全获取数据通道
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

	// 测试正常获取
	dataChan := stream.safeGetDataChan()
	assert.NotNil(t, dataChan)
	assert.Equal(t, 1000, cap(dataChan))
}

// TestStream_SafeSendToDataChan 测试安全发送数据到通道
func TestStream_SafeSendToDataChan_Duplicate(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize: 2, // 使用小容量便于测试
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

	// 测试成功发送
	data1 := map[string]interface{}{"test": "value1"}
	success := stream.safeSendToDataChan(data1)
	assert.True(t, success)

	// 测试再次发送
	data2 := map[string]interface{}{"test": "value2"}
	success = stream.safeSendToDataChan(data2)
	assert.True(t, success)

	// 填满缓冲区后测试发送失败
	data3 := map[string]interface{}{"test": "value3"}
	success = stream.safeSendToDataChan(data3)
	assert.False(t, success) // 应该失败，因为缓冲区已满
}

// TestStream_SafeSendToDataChan_Concurrent 测试并发安全发送
func TestStream_SafeSendToDataChan_Concurrent(t *testing.T) {
	config := types.Config{
		SimpleFields: []string{"name", "age"},
		PerformanceConfig: types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize: 10, // 小缓冲区用于测试
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

	// 启动消费者协程
	go func() {
		for {
			select {
			case <-stream.dataChan:
				// 消费数据
			case <-time.After(100 * time.Millisecond):
				return
			}
		}
	}()

	var wg sync.WaitGroup
	successCount := int64(0)
	var mu sync.Mutex

	// 启动多个生产者协程
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				data := map[string]interface{}{
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
	time.Sleep(50 * time.Millisecond) // 等待处理完成

	// 验证至少有一些数据成功发送
	mu.Lock()
	assert.Greater(t, successCount, int64(0))
	mu.Unlock()
}

// TestStream_DataChanMutex 测试数据通道互斥锁
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

	// 测试并发读取
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

	// 测试并发发送
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				data := map[string]interface{}{
					"id":    id,
					"value": j,
				}
				stream.safeSendToDataChan(data)
			}
		}(i)
	}

	wg.Wait()
}

// TestStream_DataHandling_EdgeCases 测试数据处理边界情况
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

	// 测试空数据
	emptyData := map[string]interface{}{}
	success := stream.safeSendToDataChan(emptyData)
	assert.True(t, success)

	// 清空通道
	select {
	case <-stream.dataChan:
	default:
	}

	// 测试nil值
	nilData := map[string]interface{}{"key": nil}
	success = stream.safeSendToDataChan(nilData)
	assert.True(t, success)
}
