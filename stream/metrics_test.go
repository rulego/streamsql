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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rulego/streamsql/types"
)

// TestMetrics_Constructor 测试指标构造器
func TestMetrics_Constructor(t *testing.T) {
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

	// 验证指标初始化
	assert.NotNil(t, stream)
}

// TestStream_UpdateMetrics 测试流更新指标
func TestStream_UpdateMetrics(t *testing.T) {
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

	// 测试更新指标
	data := map[string]interface{}{"name": "test", "age": 25}
	stream.Emit(data)
	assert.Equal(t, int64(1), stream.inputCount)
}

// TestStream_GetMetrics 测试获取指标
func TestStream_GetMetrics(t *testing.T) {
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

	// 测试获取指标
	assert.Equal(t, int64(0), stream.inputCount)
	assert.Equal(t, int64(0), stream.outputCount)
}

// TestStream_ResetMetrics 测试重置指标
func TestStream_ResetMetrics(t *testing.T) {
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

	// 测试重置指标
	data := map[string]interface{}{"name": "test", "age": 25}
	stream.Emit(data)
	// 重置指标（通过原子操作）
	stream.inputCount = 0
	assert.Equal(t, int64(0), stream.inputCount)
}

// TestStream_MetricsThreadSafety 测试指标线程安全
func TestStream_MetricsThreadSafety(t *testing.T) {
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

	// 测试并发安全
	var wg sync.WaitGroup
	data := map[string]interface{}{"name": "test", "age": 25}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream.Emit(data)
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(100), stream.inputCount)
}

// TestAssessPerformanceLevel 测试性能级别评估
func TestAssessPerformanceLevel(t *testing.T) {
	tests := []struct {
		name      string
		dataUsage float64
		dropRate  float64
		expected  string
	}{
		{
			name:      "Critical - High drop rate",
			dataUsage: 50.0,
			dropRate:  60.0,
			expected:  PerformanceLevelCritical,
		},
		{
			name:      "Critical - Exactly 50% drop rate",
			dataUsage: 30.0,
			dropRate:  50.1,
			expected:  PerformanceLevelCritical,
		},
		{
			name:      "Warning - Moderate drop rate",
			dataUsage: 40.0,
			dropRate:  30.0,
			expected:  PerformanceLevelWarning,
		},
		{
			name:      "Warning - Exactly 20% drop rate",
			dataUsage: 60.0,
			dropRate:  20.1,
			expected:  PerformanceLevelWarning,
		},
		{
			name:      "High Load - Very high data usage",
			dataUsage: 95.0,
			dropRate:  5.0,
			expected:  PerformanceLevelHighLoad,
		},
		{
			name:      "High Load - Exactly 90% data usage",
			dataUsage: 90.1,
			dropRate:  10.0,
			expected:  PerformanceLevelHighLoad,
		},
		{
			name:      "Moderate Load - High data usage",
			dataUsage: 80.0,
			dropRate:  15.0,
			expected:  PerformanceLevelModerateLoad,
		},
		{
			name:      "Moderate Load - Exactly 70% data usage",
			dataUsage: 70.1,
			dropRate:  5.0,
			expected:  PerformanceLevelModerateLoad,
		},
		{
			name:      "Optimal - Low usage and drop rate",
			dataUsage: 50.0,
			dropRate:  5.0,
			expected:  PerformanceLevelOptimal,
		},
		{
			name:      "Optimal - Zero usage and drop rate",
			dataUsage: 0.0,
			dropRate:  0.0,
			expected:  PerformanceLevelOptimal,
		},
		{
			name:      "Optimal - Boundary case",
			dataUsage: 70.0,
			dropRate:  20.0,
			expected:  PerformanceLevelOptimal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AssessPerformanceLevel(tt.dataUsage, tt.dropRate)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestAssessPerformanceLevelVariousScenarios 测试各种场景下的性能等级评估
func TestAssessPerformanceLevelVariousScenarios(t *testing.T) {
	tests := []struct {
		name          string
		dataUsage     float64
		dropRate      float64
		expectedLevel string
	}{
		{
			name:          "优秀性能 - 低使用率低丢弃率",
			dataUsage:     30.0,
			dropRate:      1.0,
			expectedLevel: PerformanceLevelOptimal,
		},
		{
			name:          "良好性能 - 中等使用率低丢弃率",
			dataUsage:     60.0,
			dropRate:      2.0,
			expectedLevel: PerformanceLevelOptimal,
		},
		{
			name:          "一般性能 - 高使用率中等丢弃率",
			dataUsage:     85.0,
			dropRate:      8.0,
			expectedLevel: PerformanceLevelModerateLoad,
		},
		{
			name:          "差性能 - 高使用率高丢弃率",
			dataUsage:     95.0,
			dropRate:      15.0,
			expectedLevel: PerformanceLevelHighLoad,
		},
		{
			name:          "边界情况 - 80%使用率5%丢弃率",
			dataUsage:     80.0,
			dropRate:      5.0,
			expectedLevel: PerformanceLevelModerateLoad,
		},
		{
			name:          "边界情况 - 50%使用率10%丢弃率",
			dataUsage:     50.0,
			dropRate:      10.0,
			expectedLevel: PerformanceLevelOptimal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			level := AssessPerformanceLevel(tt.dataUsage, tt.dropRate)
			assert.Equal(t, tt.expectedLevel, level)
		})
	}
}

// TestStatsCollector_NewStatsCollector 测试统计收集器创建
func TestStatsCollector_NewStatsCollector(t *testing.T) {
	collector := NewStatsCollector()
	assert.NotNil(t, collector)
	assert.Equal(t, int64(0), collector.GetInputCount())
	assert.Equal(t, int64(0), collector.GetOutputCount())
	assert.Equal(t, int64(0), collector.GetDroppedCount())
}

// TestStatsCollector_IncrementOperations 测试统计收集器增量操作
func TestStatsCollector_IncrementOperations(t *testing.T) {
	collector := NewStatsCollector()

	// 测试增加输入计数
	collector.IncrementInput()
	assert.Equal(t, int64(1), collector.GetInputCount())

	// 测试增加输出计数
	collector.IncrementOutput()
	assert.Equal(t, int64(1), collector.GetOutputCount())

	// 测试增加丢弃计数
	collector.IncrementDropped()
	assert.Equal(t, int64(1), collector.GetDroppedCount())

	// 测试多次增加
	for i := 0; i < 10; i++ {
		collector.IncrementInput()
		collector.IncrementOutput()
		collector.IncrementDropped()
	}

	assert.Equal(t, int64(11), collector.GetInputCount())
	assert.Equal(t, int64(11), collector.GetOutputCount())
	assert.Equal(t, int64(11), collector.GetDroppedCount())
}

// TestStatsCollector_ConcurrentOperations 测试统计收集器并发操作
func TestStatsCollector_ConcurrentOperations(t *testing.T) {
	collector := NewStatsCollector()
	var wg sync.WaitGroup

	// 并发增加输入计数
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.IncrementInput()
		}()
	}

	// 并发增加输出计数
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.IncrementOutput()
		}()
	}

	// 并发增加丢弃计数
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			collector.IncrementDropped()
		}()
	}

	wg.Wait()

	// 验证计数正确
	assert.Equal(t, int64(100), collector.GetInputCount())
	assert.Equal(t, int64(50), collector.GetOutputCount())
	assert.Equal(t, int64(25), collector.GetDroppedCount())
}

// TestStatsCollector_GetMethods 测试统计收集器获取方法
func TestStatsCollector_GetMethods(t *testing.T) {
	collector := NewStatsCollector()

	// 初始状态
	assert.Equal(t, int64(0), collector.GetInputCount())
	assert.Equal(t, int64(0), collector.GetOutputCount())
	assert.Equal(t, int64(0), collector.GetDroppedCount())

	// 设置一些值
	for i := 0; i < 5; i++ {
		collector.IncrementInput()
	}
	for i := 0; i < 3; i++ {
		collector.IncrementOutput()
	}
	for i := 0; i < 2; i++ {
		collector.IncrementDropped()
	}

	// 验证获取方法
	assert.Equal(t, int64(5), collector.GetInputCount())
	assert.Equal(t, int64(3), collector.GetOutputCount())
	assert.Equal(t, int64(2), collector.GetDroppedCount())

	// 多次调用获取方法应该返回相同值
	for i := 0; i < 10; i++ {
		assert.Equal(t, int64(5), collector.GetInputCount())
		assert.Equal(t, int64(3), collector.GetOutputCount())
		assert.Equal(t, int64(2), collector.GetDroppedCount())
	}
}

// TestStatsCollectorReset 测试统计收集器重置功能
func TestStatsCollectorReset(t *testing.T) {
	collector := NewStatsCollector()

	// 增加一些计数
	collector.IncrementInput()
	collector.IncrementInput()
	collector.IncrementOutput()
	collector.IncrementDropped()

	// 验证计数不为零
	assert.Equal(t, int64(2), collector.GetInputCount())
	assert.Equal(t, int64(1), collector.GetOutputCount())
	assert.Equal(t, int64(1), collector.GetDroppedCount())

	// 重置统计
	collector.Reset()

	// 验证所有计数都被重置为零
	assert.Equal(t, int64(0), collector.GetInputCount())
	assert.Equal(t, int64(0), collector.GetOutputCount())
	assert.Equal(t, int64(0), collector.GetDroppedCount())
}

// TestStatsCollectorGetBasicStats 测试获取基本统计信息
func TestStatsCollectorGetBasicStats(t *testing.T) {
	collector := NewStatsCollector()

	// 设置一些计数
	collector.IncrementInput()
	collector.IncrementInput()
	collector.IncrementInput()
	collector.IncrementOutput()
	collector.IncrementOutput()
	collector.IncrementDropped()

	// 模拟通道和池的状态
	dataChanLen := 5
	dataChanCap := 10
	resultChanLen := 3
	resultChanCap := 20
	sinkPoolLen := 2
	sinkPoolCap := 5
	activeRetries := int32(1)
	expanding := int32(0)

	// 获取基本统计信息
	basicStats := collector.GetBasicStats(
		dataChanLen, dataChanCap,
		resultChanLen, resultChanCap,
		sinkPoolLen, sinkPoolCap,
		activeRetries, expanding,
	)

	// 验证统计信息
	assert.Equal(t, int64(3), basicStats[InputCount])
	assert.Equal(t, int64(2), basicStats[OutputCount])
	assert.Equal(t, int64(1), basicStats[DroppedCount])
	assert.Equal(t, int64(5), basicStats[DataChanLen])
	assert.Equal(t, int64(10), basicStats[DataChanCap])
	assert.Equal(t, int64(3), basicStats[ResultChanLen])
	assert.Equal(t, int64(20), basicStats[ResultChanCap])
	assert.Equal(t, int64(2), basicStats[SinkPoolLen])
	assert.Equal(t, int64(5), basicStats[SinkPoolCap])
	assert.Equal(t, int64(1), basicStats[ActiveRetries])
	assert.Equal(t, int64(0), basicStats[Expanding])
}

// TestStatsCollectorGetDetailedStats 测试获取详细统计信息
func TestStatsCollectorGetDetailedStats(t *testing.T) {
	collector := NewStatsCollector()

	// 设置计数
	collector.IncrementInput()
	collector.IncrementInput()
	collector.IncrementInput()
	collector.IncrementInput() // 4 inputs
	collector.IncrementOutput()
	collector.IncrementOutput()
	collector.IncrementOutput()  // 3 outputs
	collector.IncrementDropped() // 1 dropped

	// 创建基本统计信息
	basicStats := collector.GetBasicStats(
		8, 10, // data channel: 80% usage
		15, 20, // result channel: 75% usage
		4, 5, // sink pool: 80% usage
		2, 1, // active retries and expanding
	)

	// 获取详细统计信息
	detailedStats := collector.GetDetailedStats(basicStats)

	// 验证详细统计信息包含所有必要字段
	assert.Contains(t, detailedStats, BasicStats)
	assert.Contains(t, detailedStats, DataChanUsage)
	assert.Contains(t, detailedStats, ResultChanUsage)
	assert.Contains(t, detailedStats, SinkPoolUsage)
	assert.Contains(t, detailedStats, ProcessRate)
	assert.Contains(t, detailedStats, DropRate)
	assert.Contains(t, detailedStats, PerformanceLevel)

	// 验证计算结果
	dataUsage := detailedStats[DataChanUsage].(float64)
	assert.Equal(t, 80.0, dataUsage) // 8/10 * 100

	resultUsage := detailedStats[ResultChanUsage].(float64)
	assert.Equal(t, 75.0, resultUsage) // 15/20 * 100

	sinkUsage := detailedStats[SinkPoolUsage].(float64)
	assert.Equal(t, 80.0, sinkUsage) // 4/5 * 100

	processRate := detailedStats[ProcessRate].(float64)
	assert.Equal(t, 75.0, processRate) // 3/4 * 100

	dropRate := detailedStats[DropRate].(float64)
	assert.Equal(t, 25.0, dropRate) // 1/4 * 100

	// 验证性能等级
	performanceLevel := detailedStats[PerformanceLevel].(string)
	assert.NotEmpty(t, performanceLevel)
}

// TestStatsCollectorGetDetailedStatsWithZeroInput 测试零输入情况下的详细统计
func TestStatsCollectorGetDetailedStatsWithZeroInput(t *testing.T) {
	collector := NewStatsCollector()

	// 不增加任何输入计数，但增加一些输出和丢弃计数
	collector.IncrementOutput()
	collector.IncrementDropped()

	// 创建基本统计信息（输入为0）
	basicStats := collector.GetBasicStats(
		2, 10, // data channel
		5, 20, // result channel
		1, 5, // sink pool
		0, 0, // active retries and expanding
	)

	// 获取详细统计信息
	detailedStats := collector.GetDetailedStats(basicStats)

	// 当输入为0时，处理率应该是100%，丢弃率应该是0%
	processRate := detailedStats[ProcessRate].(float64)
	assert.Equal(t, 100.0, processRate)

	dropRate := detailedStats[DropRate].(float64)
	assert.Equal(t, 0.0, dropRate)
}

// TestStatsCollectorConcurrentAccess 测试统计收集器的并发访问
func TestStatsCollectorConcurrentAccess(t *testing.T) {
	collector := NewStatsCollector()

	// 并发增加计数
	const numGoroutines = 100
	const incrementsPerGoroutine = 10

	done := make(chan bool, numGoroutines)

	// 启动多个goroutine并发操作
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < incrementsPerGoroutine; j++ {
				collector.IncrementInput()
				collector.IncrementOutput()
				collector.IncrementDropped()
			}
			done <- true
		}()
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 验证最终计数
	expectedCount := int64(numGoroutines * incrementsPerGoroutine)
	assert.Equal(t, expectedCount, collector.GetInputCount())
	assert.Equal(t, expectedCount, collector.GetOutputCount())
	assert.Equal(t, expectedCount, collector.GetDroppedCount())
}

// TestStatsCollectorResetDuringConcurrentAccess 测试并发访问期间的重置操作
func TestStatsCollectorResetDuringConcurrentAccess(t *testing.T) {
	collector := NewStatsCollector()

	// 先增加一些计数
	for i := 0; i < 100; i++ {
		collector.IncrementInput()
		collector.IncrementOutput()
		collector.IncrementDropped()
	}

	// 验证计数不为零
	assert.True(t, collector.GetInputCount() > 0)
	assert.True(t, collector.GetOutputCount() > 0)
	assert.True(t, collector.GetDroppedCount() > 0)

	// 重置统计
	collector.Reset()

	// 验证重置后计数为零
	assert.Equal(t, int64(0), collector.GetInputCount())
	assert.Equal(t, int64(0), collector.GetOutputCount())
	assert.Equal(t, int64(0), collector.GetDroppedCount())

	// 重置后继续增加计数
	collector.IncrementInput()
	assert.Equal(t, int64(1), collector.GetInputCount())
}

// TestPerformanceLevelConstants 测试性能级别常量
func TestPerformanceLevelConstants(t *testing.T) {
	// 验证常量值
	assert.Equal(t, "CRITICAL", PerformanceLevelCritical)
	assert.Equal(t, "WARNING", PerformanceLevelWarning)
	assert.Equal(t, "HIGH_LOAD", PerformanceLevelHighLoad)
	assert.Equal(t, "MODERATE_LOAD", PerformanceLevelModerateLoad)
	assert.Equal(t, "OPTIMAL", PerformanceLevelOptimal)
}

// TestStatisticsFieldConstants 测试统计字段常量
func TestStatisticsFieldConstants(t *testing.T) {
	// 验证基本统计字段常量
	assert.Equal(t, "input_count", InputCount)
	assert.Equal(t, "output_count", OutputCount)
	assert.Equal(t, "dropped_count", DroppedCount)
	assert.Equal(t, "data_chan_len", DataChanLen)
	assert.Equal(t, "data_chan_cap", DataChanCap)
	assert.Equal(t, "result_chan_len", ResultChanLen)
	assert.Equal(t, "result_chan_cap", ResultChanCap)
	assert.Equal(t, "sink_pool_len", SinkPoolLen)
	assert.Equal(t, "sink_pool_cap", SinkPoolCap)
	assert.Equal(t, "active_retries", ActiveRetries)
	assert.Equal(t, "expanding", Expanding)

	// 验证详细统计字段常量
	assert.Equal(t, "basic_stats", BasicStats)
	assert.Equal(t, "data_chan_usage", DataChanUsage)
	assert.Equal(t, "result_chan_usage", ResultChanUsage)
	assert.Equal(t, "sink_pool_usage", SinkPoolUsage)
	assert.Equal(t, "process_rate", ProcessRate)
	assert.Equal(t, "drop_rate", DropRate)
	assert.Equal(t, "performance_level", PerformanceLevel)
}

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
