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

package streamsql

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmitSyncWithAddSink 测试EmitSync同时触发AddSink回调
func TestEmitSyncWithAddSink(t *testing.T) {
	t.Run("非聚合查询同步+异步结果", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		// 执行非聚合查询
		sql := "SELECT temperature, humidity, temperature * 1.8 + 32 as temp_fahrenheit FROM stream WHERE temperature > 20"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		// 验证是非聚合查询
		assert.False(t, ssql.IsAggregationQuery())

		// 设置AddSink回调来收集异步结果
		var sinkCallCount int32
		var sinkResults []interface{}
		ssql.AddSink(func(result interface{}) {
			atomic.AddInt32(&sinkCallCount, 1)
			sinkResults = append(sinkResults, result)
		})

		// 测试数据
		testData := []map[string]interface{}{
			{"temperature": 25.0, "humidity": 60.0}, // 符合条件
			{"temperature": 15.0, "humidity": 70.0}, // 被过滤
			{"temperature": 30.0, "humidity": 80.0}, // 符合条件
		}

		var syncResults []interface{}

		// 处理测试数据
		for _, data := range testData {
			// 同步处理
			result, err := ssql.EmitSync(data)
			require.NoError(t, err)

			if result != nil {
				syncResults = append(syncResults, result)
			}
		}

		// 等待异步回调完成
		time.Sleep(100 * time.Millisecond)

		// 验证同步结果
		assert.Equal(t, 2, len(syncResults), "应该有2条同步结果（温度>20）")

		// 验证异步回调结果
		finalSinkCallCount := atomic.LoadInt32(&sinkCallCount)
		assert.Equal(t, int32(2), finalSinkCallCount, "AddSink应该被调用2次")
		assert.Equal(t, 2, len(sinkResults), "应该收集到2条异步结果")

		// 验证同步和异步结果的内容一致性
		if len(syncResults) > 0 && len(sinkResults) > 0 {
			// 检查第一个结果
			syncResult, ok1 := syncResults[0].(map[string]interface{})
			require.True(t, ok1, "同步结果应该是map类型")

			// AddSink收到的是数组格式 []map[string]interface{}
			sinkResultArray, ok2 := sinkResults[0].([]map[string]interface{})
			require.True(t, ok2, "异步结果应该是数组类型")
			require.True(t, len(sinkResultArray) > 0, "异步结果数组不应为空")

			sinkResult := sinkResultArray[0]

			// 验证关键字段
			assert.Equal(t, 25.0, syncResult["temperature"])
			assert.Equal(t, 25.0, sinkResult["temperature"])
			assert.Equal(t, 60.0, syncResult["humidity"])
			assert.Equal(t, 60.0, sinkResult["humidity"])

			// 验证计算字段
			assert.InDelta(t, 77.0, syncResult["temp_fahrenheit"], 0.1)
			assert.InDelta(t, 77.0, sinkResult["temp_fahrenheit"], 0.1)
		}
	})

	t.Run("聚合查询不支持EmitSync", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		// 执行聚合查询
		sql := "SELECT AVG(temperature) as avg_temp FROM stream GROUP BY TumblingWindow('1s')"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		// 验证是聚合查询
		assert.True(t, ssql.IsAggregationQuery())

		// 尝试同步处理应该返回错误
		data := map[string]interface{}{"temperature": 25.0}
		result, err := ssql.EmitSync(data)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "同步模式仅支持非聚合查询")
	})

	t.Run("多个AddSink回调都被触发", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		// 执行非聚合查询
		sql := "SELECT temperature FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		// 添加多个AddSink回调
		var sink1Count, sink2Count, sink3Count int32

		ssql.AddSink(func(result interface{}) {
			atomic.AddInt32(&sink1Count, 1)
		})

		ssql.AddSink(func(result interface{}) {
			atomic.AddInt32(&sink2Count, 1)
		})

		ssql.AddSink(func(result interface{}) {
			atomic.AddInt32(&sink3Count, 1)
		})

		// 处理一条数据
		data := map[string]interface{}{"temperature": 25.0}
		result, err := ssql.EmitSync(data)
		require.NoError(t, err)
		require.NotNil(t, result)

		// 等待异步回调
		time.Sleep(100 * time.Millisecond)

		// 验证所有回调都被触发
		assert.Equal(t, int32(1), atomic.LoadInt32(&sink1Count))
		assert.Equal(t, int32(1), atomic.LoadInt32(&sink2Count))
		assert.Equal(t, int32(1), atomic.LoadInt32(&sink3Count))
	})

	t.Run("过滤条件不匹配时AddSink不触发", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		// 执行带过滤条件的查询
		sql := "SELECT temperature FROM stream WHERE temperature > 30"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		// 添加AddSink回调
		var sinkCallCount int32
		ssql.AddSink(func(result interface{}) {
			atomic.AddInt32(&sinkCallCount, 1)
		})

		// 处理不符合条件的数据
		data := map[string]interface{}{"temperature": 20.0} // 不符合 > 30 的条件
		result, err := ssql.EmitSync(data)
		require.NoError(t, err)
		assert.Nil(t, result, "不符合过滤条件应该返回nil")

		// 等待可能的异步回调
		time.Sleep(100 * time.Millisecond)

		// 验证AddSink没有被触发
		assert.Equal(t, int32(0), atomic.LoadInt32(&sinkCallCount), "过滤掉的数据不应触发AddSink")
	})
}

// TestEmitSyncPerformance 测试EmitSync性能（包括AddSink触发）
func TestEmitSyncPerformance(t *testing.T) {
	ssql := New()
	defer ssql.Stop()

	sql := "SELECT temperature, humidity FROM stream WHERE temperature > 0"
	err := ssql.Execute(sql)
	require.NoError(t, err)

	// 添加AddSink回调
	var sinkCallCount int32
	ssql.AddSink(func(result interface{}) {
		atomic.AddInt32(&sinkCallCount, 1)
	})

	// 性能测试
	testCount := 1000

	start := time.Now()
	for i := 0; i < testCount; i++ {
		data := map[string]interface{}{
			"temperature": float64(20 + i%20),
			"humidity":    float64(50 + i%30),
		}

		result, err := ssql.EmitSync(data)
		require.NoError(t, err)
		require.NotNil(t, result)
	}
	duration := time.Since(start)

	// 等待所有异步回调完成
	time.Sleep(200 * time.Millisecond)

	t.Logf("处理 %d 条数据耗时: %v", testCount, duration)
	t.Logf("平均每条数据: %v", duration/time.Duration(testCount))
	t.Logf("AddSink 触发次数: %d", atomic.LoadInt32(&sinkCallCount))

	// 验证性能和一致性
	assert.Less(t, duration, 1*time.Second, "性能应该足够好")
	assert.Equal(t, int32(testCount), atomic.LoadInt32(&sinkCallCount), "所有数据都应触发AddSink")
}
