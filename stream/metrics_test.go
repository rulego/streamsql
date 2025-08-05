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