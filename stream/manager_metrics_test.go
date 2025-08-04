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
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStatsManager_NewStatsManager 测试统计管理器创建
func TestStatsManager_NewStatsManager(t *testing.T) {
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

	manager := NewStatsManager(stream)
	assert.NotNil(t, manager)
	assert.Equal(t, stream, manager.stream)
	assert.NotNil(t, manager.statsCollector)
}

// TestStream_GetStats 测试获取基本统计信息
func TestStream_GetStats(t *testing.T) {
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

	// 获取初始统计信息
	stats := stream.GetStats()
	assert.NotNil(t, stats)

	// 验证基本字段存在
	assert.Contains(t, stats, InputCount)
	assert.Contains(t, stats, OutputCount)
	assert.Contains(t, stats, DroppedCount)
	assert.Contains(t, stats, DataChanLen)
	assert.Contains(t, stats, DataChanCap)
	assert.Contains(t, stats, ResultChanLen)
	assert.Contains(t, stats, ResultChanCap)
	assert.Contains(t, stats, SinkPoolLen)
	assert.Contains(t, stats, SinkPoolCap)
	assert.Contains(t, stats, ActiveRetries)
	assert.Contains(t, stats, Expanding)

	// 验证初始值
	assert.Equal(t, int64(0), stats[InputCount])
	assert.Equal(t, int64(0), stats[OutputCount])
	assert.Equal(t, int64(0), stats[DroppedCount])
	assert.Equal(t, int64(1000), stats[DataChanCap])
	assert.Equal(t, int64(100), stats[ResultChanCap])
	assert.Equal(t, int64(4), stats[SinkPoolCap])
	assert.Equal(t, int64(0), stats[ActiveRetries])
	assert.Equal(t, int64(0), stats[Expanding])
}

// TestStream_GetStats_WithData 测试有数据时的统计信息
func TestStream_GetStats_WithData(t *testing.T) {
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

	// 模拟一些统计数据
	atomic.AddInt64(&stream.inputCount, 100)
	atomic.AddInt64(&stream.outputCount, 80)
	atomic.AddInt64(&stream.droppedCount, 20)

	// 向数据通道添加一些数据
	for i := 0; i < 3; i++ {
		select {
		case stream.dataChan <- map[string]interface{}{"test": i}:
		default:
			break
		}
	}

	stats := stream.GetStats()

	// 验证统计数据
	assert.Equal(t, int64(100), stats[InputCount])
	assert.Equal(t, int64(80), stats[OutputCount])
	assert.Equal(t, int64(20), stats[DroppedCount])
	assert.True(t, stats[DataChanLen] >= 0) // 数据通道长度应该大于等于0
}

// TestStream_GetDetailedStats 测试获取详细统计信息
func TestStream_GetDetailedStats(t *testing.T) {
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

	// 模拟一些统计数据
	atomic.AddInt64(&stream.inputCount, 100)
	atomic.AddInt64(&stream.outputCount, 90)
	atomic.AddInt64(&stream.droppedCount, 10)

	detailedStats := stream.GetDetailedStats()
	assert.NotNil(t, detailedStats)

	// 验证详细统计字段存在
	assert.Contains(t, detailedStats, BasicStats)
	assert.Contains(t, detailedStats, DataChanUsage)
	assert.Contains(t, detailedStats, ResultChanUsage)
	assert.Contains(t, detailedStats, SinkPoolUsage)
	assert.Contains(t, detailedStats, ProcessRate)
	assert.Contains(t, detailedStats, DropRate)
	assert.Contains(t, detailedStats, PerformanceLevel)

	// 验证基本统计信息
	basicStats, ok := detailedStats[BasicStats].(map[string]int64)
	assert.True(t, ok)
	assert.Equal(t, int64(100), basicStats[InputCount])
	assert.Equal(t, int64(90), basicStats[OutputCount])
	assert.Equal(t, int64(10), basicStats[DroppedCount])

	// 验证计算的指标
	processRate, ok := detailedStats[ProcessRate].(float64)
	assert.True(t, ok)
	assert.Equal(t, 90.0, processRate)

	dropRate, ok := detailedStats[DropRate].(float64)
	assert.True(t, ok)
	assert.Equal(t, 10.0, dropRate)

	// 验证性能级别
	perfLevel, ok := detailedStats[PerformanceLevel].(string)
	assert.True(t, ok)
	assert.NotEmpty(t, perfLevel)
}

// TestStream_GetDetailedStats_ZeroInput 测试零输入时的详细统计
func TestStream_GetDetailedStats_ZeroInput(t *testing.T) {
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

	detailedStats := stream.GetDetailedStats()

	// 验证零输入时的处理率和丢弃率
	processRate, ok := detailedStats[ProcessRate].(float64)
	assert.True(t, ok)
	assert.Equal(t, 100.0, processRate) // 默认处理率应该是100%

	dropRate, ok := detailedStats[DropRate].(float64)
	assert.True(t, ok)
	assert.Equal(t, 0.0, dropRate) // 默认丢弃率应该是0%

	perfLevel, ok := detailedStats[PerformanceLevel].(string)
	assert.True(t, ok)
	assert.Equal(t, PerformanceLevelOptimal, perfLevel)
}

// TestStream_GetDetailedStats_WithPersistence 测试带持久化的详细统计
func TestStream_GetDetailedStats_WithPersistence(t *testing.T) {
	// 创建临时目录用于持久化
	tempDir := t.TempDir()

	config := types.Config{
		SimpleFields: []string{"name", "age"},
		PerformanceConfig: types.PerformanceConfig{
			OverflowConfig: types.OverflowConfig{
				Strategy: "persist",
				PersistenceConfig: &types.PersistenceConfig{
					DataDir:       tempDir,
					MaxFileSize:   1024 * 1024, // 1MB
					FlushInterval: 100 * time.Millisecond,
				},
			},
		},
	}
	stream, err := NewStream(config)
	require.NoError(t, err)
	defer func() {
		if stream != nil {
			if stream.persistenceManager != nil {
				stream.persistenceManager.Stop()
			}
			close(stream.done)
		}
	}()

	detailedStats := stream.GetDetailedStats()

	// 验证持久化统计信息存在
	assert.Contains(t, detailedStats, "Persistence")
	persistenceStats, ok := detailedStats["Persistence"].(map[string]interface{})
	assert.True(t, ok)
	assert.NotNil(t, persistenceStats)
}

// TestStream_ResetStats 测试重置统计信息
func TestStream_ResetStats(t *testing.T) {
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

	// 设置一些统计数据
	atomic.AddInt64(&stream.inputCount, 100)
	atomic.AddInt64(&stream.outputCount, 80)
	atomic.AddInt64(&stream.droppedCount, 20)

	// 验证统计数据已设置
	stats := stream.GetStats()
	assert.Equal(t, int64(100), stats[InputCount])
	assert.Equal(t, int64(80), stats[OutputCount])
	assert.Equal(t, int64(20), stats[DroppedCount])

	// 重置统计信息
	stream.ResetStats()

	// 验证统计信息已重置
	stats = stream.GetStats()
	assert.Equal(t, int64(0), stats[InputCount])
	assert.Equal(t, int64(0), stats[OutputCount])
	assert.Equal(t, int64(0), stats[DroppedCount])
}

// TestStream_GetStats_ThreadSafety 测试统计信息获取的线程安全性
func TestStream_GetStats_ThreadSafety(t *testing.T) {
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

	// 并发获取统计信息
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				stats := stream.GetStats()
				assert.NotNil(t, stats)
				detailedStats := stream.GetDetailedStats()
				assert.NotNil(t, detailedStats)
			}
			done <- true
		}()
	}

	// 并发修改统计数据
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				atomic.AddInt64(&stream.inputCount, 1)
				atomic.AddInt64(&stream.outputCount, 1)
				atomic.AddInt64(&stream.droppedCount, 1)
			}
			done <- true
		}()
	}

	// 等待所有协程完成
	for i := 0; i < 15; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Test timeout")
		}
	}

	// 验证最终统计数据
	stats := stream.GetStats()
	assert.Equal(t, int64(500), stats[InputCount])
	assert.Equal(t, int64(500), stats[OutputCount])
	assert.Equal(t, int64(500), stats[DroppedCount])
}
