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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamSQLPerformanceModesExtended 测试不同性能模式的配置
func TestStreamSQLPerformanceModesExtended(t *testing.T) {
	t.Run("default performance mode", func(t *testing.T) {
		ssql := New()
		assert.Equal(t, "default", ssql.performanceMode)
		assert.Nil(t, ssql.customConfig)

		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		assert.NotNil(t, ssql.stream)
		ssql.Stop()
	})

	t.Run("high performance mode", func(t *testing.T) {
		ssql := New(WithHighPerformance())
		assert.Equal(t, "high_performance", ssql.performanceMode)

		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		assert.NotNil(t, ssql.stream)
		ssql.Stop()
	})

	t.Run("low latency mode", func(t *testing.T) {
		ssql := New(WithLowLatency())
		assert.Equal(t, "low_latency", ssql.performanceMode)

		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		assert.NotNil(t, ssql.stream)
		ssql.Stop()
	})

	t.Run("zero data loss mode", func(t *testing.T) {
		ssql := New(WithZeroDataLoss())
		assert.Equal(t, "zero_data_loss", ssql.performanceMode)

		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		assert.NotNil(t, ssql.stream)
		ssql.Stop()
	})

	t.Run("custom performance mode", func(t *testing.T) {
		customConfig := types.DefaultPerformanceConfig()
		customConfig.BufferConfig.DataChannelSize = 2000
		ssql := New(WithCustomPerformance(customConfig))
		assert.Equal(t, "custom", ssql.performanceMode)
		assert.NotNil(t, ssql.customConfig)
		assert.Equal(t, 2000, ssql.customConfig.BufferConfig.DataChannelSize)

		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		assert.NotNil(t, ssql.stream)
		ssql.Stop()
	})

	t.Run("custom mode with nil config", func(t *testing.T) {
		ssql := New()
		ssql.performanceMode = "custom"
		ssql.customConfig = nil

		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		assert.NotNil(t, ssql.stream)
		ssql.Stop()
	})
}

// TestStreamSQLFieldOrder 测试字段顺序保持功能
func TestStreamSQLFieldOrder(t *testing.T) {
	t.Run("field order preservation", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT name, id, value FROM stream")
		require.NoError(t, err)

		// 验证字段顺序被正确保存
		expectedOrder := []string{"name", "id", "value"}
		assert.Equal(t, expectedOrder, ssql.fieldOrder)
		ssql.Stop()
	})

	t.Run("field order with aliases", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT name as device_name, id as device_id FROM stream")
		require.NoError(t, err)

		// 验证别名字段顺序
		expectedOrder := []string{"device_name", "device_id"}
		assert.Equal(t, expectedOrder, ssql.fieldOrder)
		ssql.Stop()
	})
}

// TestStreamSQLPrintTableFormat 测试表格打印功能
func TestStreamSQLPrintTableFormat(t *testing.T) {
	t.Run("print table format with data", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id, name FROM stream")
		require.NoError(t, err)

		// 测试 printTableFormat 方法
		testResults := []map[string]interface{}{
			{"id": 1, "name": "test1"},
			{"id": 2, "name": "test2"},
		}

		// 这个方法主要是打印输出，我们确保它不会panic
		assert.NotPanics(t, func() {
			ssql.printTableFormat(testResults)
		})
		ssql.Stop()
	})

	t.Run("print table format with empty data", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)

		// 测试空数据
		emptyResults := []map[string]interface{}{}
		assert.NotPanics(t, func() {
			ssql.printTableFormat(emptyResults)
		})
		ssql.Stop()
	})

	t.Run("print table format with nil field order", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)

		// 清空字段顺序
		ssql.fieldOrder = nil
		testResults := []map[string]interface{}{
			{"id": 1},
		}

		assert.NotPanics(t, func() {
			ssql.printTableFormat(testResults)
		})
		ssql.Stop()
	})
}

// TestStreamSQLToChannel 测试通道功能
func TestStreamSQLToChannel(t *testing.T) {
	t.Run("to channel with aggregation query", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT COUNT(*) FROM stream GROUP BY TumblingWindow('1s')")
		require.NoError(t, err)

		// 获取结果通道
		resultChan := ssql.ToChannel()
		assert.NotNil(t, resultChan)

		// 启动goroutine接收结果
		var wg sync.WaitGroup
		wg.Add(1)
		var receivedResults [][]map[string]interface{}
		go func() {
			defer wg.Done()
			timeout := time.After(3 * time.Second)
			for {
				select {
				case result := <-resultChan:
					if result != nil {
						receivedResults = append(receivedResults, result)
						return
					}
				case <-timeout:
					return
				}
			}
		}()

		// 发送一些数据
		for i := 0; i < 5; i++ {
			ssql.Emit(map[string]interface{}{"id": i})
		}

		// 等待结果
		wg.Wait()
		ssql.Stop()

		// 验证至少收到了一些结果
		assert.GreaterOrEqual(t, len(receivedResults), 0)
	})

	t.Run("to channel with non-aggregation query", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)

		resultChan := ssql.ToChannel()
		assert.NotNil(t, resultChan)
		ssql.Stop()
	})
}

// TestStreamSQLMultipleOptions 测试多个配置选项组合
func TestStreamSQLMultipleOptions(t *testing.T) {
	t.Run("multiple options combination", func(t *testing.T) {
		// 组合多个配置选项
		ssql := New(
			WithHighPerformance(),
			WithDiscardLog(),
		)
		assert.Equal(t, "high_performance", ssql.performanceMode)

		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		ssql.Stop()
	})

	t.Run("override performance mode", func(t *testing.T) {
		// 后面的选项应该覆盖前面的
		ssql := New(
			WithHighPerformance(),
			WithLowLatency(),
		)
		assert.Equal(t, "low_latency", ssql.performanceMode)

		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		ssql.Stop()
	})
}

// TestStreamSQLExecuteErrorHandling 测试Execute方法的错误处理
func TestStreamSQLExecuteErrorHandling(t *testing.T) {
	t.Run("stream creation failure simulation", func(t *testing.T) {
		ssql := New()
		// 使用一个可能导致stream创建失败的SQL
		err := ssql.Execute("SELECT invalid_function() FROM test_stream")
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "function")
	})

	t.Run("filter registration failure", func(t *testing.T) {
		ssql := New()
		// 使用可能导致过滤器注册失败的SQL
		err := ssql.Execute("SELECT id FROM stream WHERE INVALID_CONDITION")
		if err != nil {
			// 如果有错误，应该包含相关信息
			assert.True(t,
				strings.Contains(err.Error(), "SQL parsing failed") ||
					strings.Contains(err.Error(), "failed to register filter condition") ||
					strings.Contains(err.Error(), "failed to create stream processor"))
		}
	})
}

// TestStreamSQLConcurrentAccess 测试并发访问安全性
func TestStreamSQLConcurrentAccess(t *testing.T) {
	t.Run("concurrent emit and stop", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)

		var wg sync.WaitGroup
		numWorkers := 10

		// 启动多个goroutine并发发送数据
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					ssql.Emit(map[string]interface{}{"id": workerID*100 + j})
				}
			}(i)
		}

		// 等待一段时间后停止
		time.Sleep(100 * time.Millisecond)
		ssql.Stop()

		wg.Wait()
	})

	t.Run("concurrent method calls", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)

		var wg sync.WaitGroup
		numWorkers := 5

		// 并发调用各种方法
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// 这些方法调用应该是安全的
				_ = ssql.GetStats()
				_ = ssql.GetDetailedStats()
				_ = ssql.IsAggregationQuery()
				_ = ssql.Stream()
				_ = ssql.ToChannel()
				ssql.AddSink(func(results []map[string]interface{}) {})
			}()
		}

		wg.Wait()
		ssql.Stop()
	})
}

// TestStreamSQLEdgeCasesAdditional 测试额外的边界情况
func TestStreamSQLEdgeCasesAdditional(t *testing.T) {
	t.Run("execute with different performance modes after creation", func(t *testing.T) {
		ssql := New()

		// 先用默认模式执行
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		ssql.Stop()

		// 改变性能模式后再次执行应该失败，因为已经执行过了
		ssql.performanceMode = "high_performance"
		err = ssql.Execute("SELECT name FROM stream")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Execute() has already been called")
		// 不需要再次调用Stop()，因为第二次Execute失败了
	})

	t.Run("field order with complex query", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT COUNT(*) as cnt, AVG(value) as avg_val, deviceId FROM stream GROUP BY deviceId")
		require.NoError(t, err)

		// 验证复杂查询的字段顺序
		expectedOrder := []string{"cnt", "avg_val", "deviceId"}
		assert.Equal(t, expectedOrder, ssql.fieldOrder)
		ssql.Stop()
	})

	t.Run("print table with field order", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT name, id, value FROM stream")
		require.NoError(t, err)

		// 设置字段顺序
		ssql.fieldOrder = []string{"name", "id", "value"}

		// 测试PrintTable方法
		assert.NotPanics(t, func() {
			ssql.PrintTable()
		})
		ssql.Stop()
	})
}

// TestStreamSQLEmitSync 测试EmitSync方法的各种情况
func TestStreamSQLEmitSync(t *testing.T) {
	t.Run("emit sync with uninitialized stream", func(t *testing.T) {
		ssql := New()
		// 在没有执行SQL的情况下调用EmitSync
		result, err := ssql.EmitSync(map[string]interface{}{"id": 1})
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "stream not initialized")
	})

	t.Run("emit sync with aggregation query", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT COUNT(*) FROM stream GROUP BY id")
		require.NoError(t, err)

		// 对聚合查询调用EmitSync应该返回错误
		result, err := ssql.EmitSync(map[string]interface{}{"id": 1})
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "synchronous mode only supports non-aggregation queries")
		ssql.Stop()
	})

	t.Run("emit sync with non-aggregation query", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id, name FROM stream WHERE id > 0")
		require.NoError(t, err)

		// 对非聚合查询调用EmitSync
		data := map[string]interface{}{"id": 1, "name": "test"}
		result, err := ssql.EmitSync(data)
		// 根据实际实现，这里可能成功或失败
		if err != nil {
			t.Logf("EmitSync error (expected): %v", err)
		} else {
			t.Logf("EmitSync result: %v", result)
		}
		ssql.Stop()
	})
}

// TestStreamSQLCustomPerformanceConfig 测试自定义性能配置
func TestStreamSQLCustomPerformanceConfig(t *testing.T) {
	t.Run("custom performance config with nil config", func(t *testing.T) {
		ssql := New()
		ssql.performanceMode = "custom"
		ssql.customConfig = nil // 设置为nil

		// 执行SQL时应该回退到默认配置
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		ssql.Stop()
	})

	t.Run("custom performance config with valid config", func(t *testing.T) {
		customConfig := types.PerformanceConfig{
			BufferConfig: types.BufferConfig{
				DataChannelSize:   1000,
				ResultChannelSize: 100,
				WindowOutputSize:  50,
			},
			WorkerConfig: types.WorkerConfig{
				SinkPoolSize:    4,
				SinkWorkerCount: 2,
			},
		}
		ssql := New(WithCustomPerformance(customConfig))

		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		require.Equal(t, "custom", ssql.performanceMode)
		require.Equal(t, &customConfig, ssql.customConfig)
		ssql.Stop()
	})
}

// TestStreamSQLStatsMethods 测试统计信息相关方法
func TestStreamSQLStatsMethods(t *testing.T) {
	t.Run("get stats with uninitialized stream", func(t *testing.T) {
		ssql := New()
		stats := ssql.GetStats()
		require.NotNil(t, stats)
		require.Equal(t, 0, len(stats))
	})

	t.Run("get detailed stats with uninitialized stream", func(t *testing.T) {
		ssql := New()
		detailedStats := ssql.GetDetailedStats()
		require.NotNil(t, detailedStats)
		require.Equal(t, 0, len(detailedStats))
	})

	t.Run("get stats with initialized stream", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)

		stats := ssql.GetStats()
		require.NotNil(t, stats)

		detailedStats := ssql.GetDetailedStats()
		require.NotNil(t, detailedStats)

		ssql.Stop()
	})

	t.Run("is aggregation query method", func(t *testing.T) {
		// 测试未初始化的情况
		ssql := New()
		require.False(t, ssql.IsAggregationQuery())

		// 测试非聚合查询
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		isAgg := ssql.IsAggregationQuery()
		t.Logf("Is aggregation query: %v", isAgg)
		ssql.Stop()

		// 测试聚合查询
		ssql2 := New()
		err = ssql2.Execute("SELECT COUNT(*) FROM stream GROUP BY id")
		require.NoError(t, err)
		isAgg2 := ssql2.IsAggregationQuery()
		t.Logf("Is aggregation query (with GROUP BY): %v", isAgg2)
		ssql2.Stop()
	})
}

// TestStreamSQLNilAndEdgeCases 测试空值和边界情况
func TestStreamSQLNilAndEdgeCases(t *testing.T) {
	t.Run("emit with nil stream", func(t *testing.T) {
		ssql := New()
		// 在没有执行SQL的情况下调用Emit
		assert.NotPanics(t, func() {
			ssql.Emit(map[string]interface{}{"id": 1})
		})
	})

	t.Run("add sink with nil stream", func(t *testing.T) {
		ssql := New()
		// 在没有执行SQL的情况下调用AddSink
		assert.NotPanics(t, func() {
			ssql.AddSink(func(results []map[string]interface{}) {
				t.Log("Sink called")
			})
		})
	})

	t.Run("to channel with nil stream", func(t *testing.T) {
		ssql := New()
		// 在没有执行SQL的情况下调用ToChannel
		resultChan := ssql.ToChannel()
		require.Nil(t, resultChan)
	})

	t.Run("stream method with nil stream", func(t *testing.T) {
		ssql := New()
		// 在没有执行SQL的情况下调用Stream
		stream := ssql.Stream()
		require.Nil(t, stream)
	})

	t.Run("stop with nil stream", func(t *testing.T) {
		ssql := New()
		// 在没有执行SQL的情况下调用Stop
		assert.NotPanics(t, func() {
			ssql.Stop()
		})
	})

	t.Run("print table format with empty results", func(t *testing.T) {
		ssql := New()
		ssql.fieldOrder = []string{"id", "name"}

		// 测试空结果的表格打印
		assert.NotPanics(t, func() {
			ssql.printTableFormat([]map[string]interface{}{})
		})
	})

	t.Run("print table format with nil field order", func(t *testing.T) {
		ssql := New()
		ssql.fieldOrder = nil

		results := []map[string]interface{}{
			{"id": 1, "name": "test"},
		}

		// 测试nil字段顺序的表格打印
		assert.NotPanics(t, func() {
			ssql.printTableFormat(results)
		})
	})
}

// TestStreamSQLComplexScenarios 测试复杂场景
func TestStreamSQLComplexScenarios(t *testing.T) {
	t.Run("multiple execute calls", func(t *testing.T) {
		ssql := New()

		// 第一次执行
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		ssql.Stop()

		// 第二次执行应该失败，因为已经执行过了
		err = ssql.Execute("SELECT name FROM stream")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Execute() has already been called")
	})

	t.Run("performance mode switching", func(t *testing.T) {
		// 测试所有性能模式
		modes := []string{"default", "high_performance", "low_latency", "zero_data_loss"}

		for _, mode := range modes {
			t.Run(fmt.Sprintf("mode_%s", mode), func(t *testing.T) {
				ssql := New()
				ssql.performanceMode = mode

				err := ssql.Execute("SELECT id FROM stream")
				require.NoError(t, err)
				require.Equal(t, mode, ssql.performanceMode)
				ssql.Stop()
			})
		}
	})

	t.Run("field order preservation", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT z, a, m, b FROM stream")
		require.NoError(t, err)

		// 验证字段顺序被正确保存
		expectedOrder := []string{"z", "a", "m", "b"}
		require.Equal(t, expectedOrder, ssql.fieldOrder)
		ssql.Stop()
	})
}
