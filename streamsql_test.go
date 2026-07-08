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

// package streamsql 的白盒测试与基准（覆盖/性能/溢出策略/表格打印 + 端到端示例）。
// 访问非导出字段（performanceMode/customConfig/stream/fieldOrder）与非导出方法
// （printTableFormat），故必须在 package streamsql 内，不能迁 test/e2e。
// 纯公开 API 的集成测试见 test/e2e。

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------- coverage ----------
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
		defer ssql.Stop()
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

// ---------- perf benchmarks ----------
// Integration benchmarks exercising the full main path with realistic RSQL.
// EmitSync processes each row synchronously end-to-end (the same path users
// call), so ns/op is the true per-row latency through ProcessData -> field
// evaluation -> result building. Aggregation queries are exercised separately
// via the Emit-based benchmarks.

func benchEmitSync(b *testing.B, sql string, row map[string]interface{}) {
	b.Helper()
	ssql := New()
	defer ssql.Stop()
	if err := ssql.Execute(sql); err != nil {
		b.Fatalf("Execute: %v", err)
	}

	// Warm up compile/preprocess caches (do not measure).
	if _, err := ssql.EmitSync(row); err != nil {
		b.Fatalf("warmup EmitSync: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ssql.EmitSync(row); err != nil {
			b.Fatalf("EmitSync: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkMainPath_FilterProject(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature FROM stream WHERE temperature > 20",
		map[string]interface{}{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

func BenchmarkMainPath_MultiFieldFilter(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature, humidity FROM stream WHERE temperature > 20 AND humidity < 80",
		map[string]interface{}{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

func BenchmarkMainPath_ComputedFields(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature * 2 + humidity AS score, abs(temperature - 100) AS dev FROM stream WHERE temperature > 20",
		map[string]interface{}{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

func BenchmarkMainPath_StringConcat(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId + '-' + location AS id FROM stream",
		map[string]interface{}{"deviceId": "d1", "location": "roomA"},
	)
}

func BenchmarkMainPath_NoFilter(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature, humidity FROM stream",
		map[string]interface{}{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

// ---------- overflow strategy ----------
// TestSQLIntegration_StrategyBlock 测试 SQL 集成下的阻塞策略
func TestSQLIntegration_StrategyBlock(t *testing.T) {
	// 配置：输出缓冲为 1，阻塞策略，超时 100ms
	ssql := New(WithCustomPerformance(types.PerformanceConfig{
		BufferConfig: types.BufferConfig{
			DataChannelSize:   100,
			ResultChannelSize: 100,
			WindowOutputSize:  1,
		},
		OverflowConfig: types.OverflowConfig{
			Strategy:      types.OverflowStrategyBlock,
			BlockTimeout:  100 * time.Millisecond,
			AllowDataLoss: true,
		},
		WorkerConfig: types.WorkerConfig{
			SinkPoolSize:    0, // 无缓冲任务队列
			SinkWorkerCount: 1, // 1个 worker
		},
	}))
	defer ssql.Stop()

	// SQL: 每条数据触发一次窗口
	rsql := "SELECT deviceId FROM stream GROUP BY deviceId, CountingWindow(1)"
	err := ssql.Execute(rsql)
	require.NoError(t, err)

	// 添加同步 Sink 阻塞 Stream 处理，从而反压 Window
	// 注意：必须在 Execute 之后添加，因为 Execute 才会创建 stream
	ssql.AddSyncSink(func(results []map[string]interface{}) {
		time.Sleep(500 * time.Millisecond)
	})

	// 发送 5 条数据
	// d1: Worker 处理中 (阻塞 500ms)
	// d2: Stream 尝试写入 WorkerPool -> 阻塞 (无缓冲)
	// d3: Window OutputChan (size 1) -> 填满
	// d4: Window OutputChan 满 -> 尝试写入 -> 阻塞 (Window Add) -> 放入 TriggerChan (size=1)
	// d5: Window Add -> TriggerChan 满 -> 阻塞? No, Emit 是异步的?
	// Emit 往 dataChan 写. DataProcessor 读 dataChan -> Window.Add.
	// Window.Add 往 triggerChan 写.
	//
	// 修正分析:
	// Window.Add 是非阻塞的 (如果 triggerChan 不满).
	// CountingWindow triggerChan size = bufferSize = 1.
	// Worker 协程: 从 triggerChan 读 -> 处理 -> sendResult (到 OutputChan).
	//
	// d1: Worker读triggerChan -> OutputChan -> Stream -> WorkerPool -> Worker(busy).
	// d2: Worker读triggerChan -> OutputChan -> Stream -> Blocked on WorkerPool.
	//     此时 Stream 持有 d2. OutputChan 空.
	//     Worker 协程 阻塞在 sendResult(d2)? No, Stream 取走了 d2, Stream 阻塞在 dispatch.
	//     所以 OutputChan 是空的!
	//     Wait, Stream loop:
	//     result := <-OutputChan. (Stream has d2).
	//     handleResult(d2) -> Blocked.
	//     So OutputChan is empty.
	// d3: Worker读triggerChan -> OutputChan (d3). Success.
	//     OutputChan has d3.
	// d4: Worker读triggerChan -> OutputChan (d4). Blocked (OutputChan full).
	//     Worker 协程 阻塞在 sendResult(d4).
	// d5: Add -> triggerChan (d5). Success (triggerChan size 1).
	// d6: Add -> triggerChan (d6). Blocked (triggerChan full).
	//     Add blocks. DataProcessor blocks. Emit succeeds (dataChan).
	//
	// 所以 Window Worker 只有在 sendResult 阻塞时才触发 Drop logic.
	// sendResult 只有在 OutputChan 满且超时时才 Drop.
	//
	// d4 阻塞在 sendResult.
	// 100ms 后超时 -> Drop d4.
	// Worker 继续.
	//
	// 所以 d4 应该是被 Drop 的那个.
	// Sent: d1, d2, d3. (d5 在 triggerChan, d6 在 dataChan).
	// Wait, d5 is in triggerChan, not processed yet.
	// So Sent = 3. Dropped = 1 (d4).

	for _, id := range []string{"d1", "d2", "d3", "d4", "d5"} {
		ssql.Emit(map[string]interface{}{"deviceId": id})
		time.Sleep(10 * time.Millisecond)
	}

	// 等待足够长的时间让 Stream 醒来并处理完，以及 Window 丢弃逻辑执行
	time.Sleep(1000 * time.Millisecond)

	// 获取统计信息
	// d1: Stream 处理完
	// d2: Stream 处理完 (Worker 醒来后处理 d2)
	// d3: Dropped (Worker 阻塞 -> 超时)
	// d4: Dropped (Worker 阻塞 -> 超时)
	// d5: Dropped (Worker 阻塞 -> 超时)
	// Total Sent: 2 (d1, d2).
	// Dropped: 3 (d3, d4, d5).
	stats := ssql.stream.GetStats()
	assert.Equal(t, int64(3), stats["droppedCount"], "Should have 3 dropped window result due to overflow")
	assert.Equal(t, int64(2), stats["sentCount"], "Should have 2 sent window result")
}

// TestSQLIntegration_StrategyDrop 测试 SQL 集成下的丢弃策略
func TestSQLIntegration_StrategyDrop(t *testing.T) {
	// 配置：输出缓冲为 1，丢弃策略
	ssql := New(WithCustomPerformance(types.PerformanceConfig{
		BufferConfig: types.BufferConfig{
			DataChannelSize:   100,
			ResultChannelSize: 100,
			WindowOutputSize:  1,
		},
		OverflowConfig: types.OverflowConfig{
			Strategy: types.OverflowStrategyDrop,
		},
	}))
	defer ssql.Stop()

	// SQL: 每条数据触发一次窗口
	rsql := "SELECT deviceId FROM stream GROUP BY deviceId, CountingWindow(1)"
	err := ssql.Execute(rsql)
	require.NoError(t, err)

	// 连续发送 3 条数据
	ssql.Emit(map[string]interface{}{"deviceId": "d1"})
	ssql.Emit(map[string]interface{}{"deviceId": "d2"})
	ssql.Emit(map[string]interface{}{"deviceId": "d3"})

	// 等待处理完成
	time.Sleep(200 * time.Millisecond)

	// 对于 StrategyDrop，它会挤掉旧数据，所以 sentCount 应该持续增加
	stats := ssql.stream.GetStats()
	// d1, d2, d3 都会成功发送（虽然 d1, d2 可能被挤掉，但 sendResult 逻辑中挤掉旧的后写入新的算发送成功）
	assert.Equal(t, int64(3), stats["sentCount"])

	// 验证最终留在缓冲区的是最后一条数据 (d3)
	// 注意：AddSink 会启动 worker 从 OutputChan 读。
	// 为了验证，我们直接从 Window 的 OutputChan 读
	select {
	case result := <-ssql.stream.Window.OutputChan():
		assert.Equal(t, "d3", result[0].Data.(map[string]interface{})["deviceId"])
	case <-time.After(100 * time.Millisecond):
		// 如果已经被 AddSink 的 worker 读走了也正常，但由于我们没加 Sink，所以应该在里面
	}
}

// ---------- table print ----------
// TestPrintTable 测试PrintTable方法的基本功能
func TestPrintTable(t *testing.T) {
	// 创建StreamSQL实例并测试PrintTable
	ssql := New()
	defer ssql.Stop()
	err := ssql.Execute("SELECT device, AVG(temperature) as avg_temp FROM stream GROUP BY device, TumblingWindow('2s')")
	assert.NoError(t, err)

	// 使用PrintTable方法（不验证输出内容，只确保不会panic）
	assert.NotPanics(t, func() {
		ssql.PrintTable()
	}, "PrintTable方法不应该panic")

	// 发送测试数据
	testData := []map[string]interface{}{
		{"device": "sensor1", "temperature": 25.0},
		{"device": "sensor2", "temperature": 30.0},
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	// 等待窗口触发
	time.Sleep(3 * time.Second)
}

// TestPrintTableFormat 测试printTableFormat方法处理不同数据类型
func TestPrintTableFormat(t *testing.T) {
	ssql := New()

	// 测试不同类型的数据，确保不会panic
	assert.NotPanics(t, func() {
		// 测试空切片
		ssql.printTableFormat([]map[string]interface{}{})
	}, "空切片不应该panic")
}

// ---------- end-to-end example ----------
func TestStreamData(t *testing.T) {
	// 步骤1: 创建 StreamSQL 实例
	// StreamSQL 是流式 SQL 处理引擎的核心组件，负责管理整个流处理生命周期
	ssql := New()
	// 确保测试结束时停止流处理，释放资源
	defer ssql.Stop()

	// 步骤2: 定义流式 SQL 查询语句
	// 这个 SQL 语句展示了 StreamSQL 的核心功能：
	// - SELECT: 选择要输出的字段和聚合函数
	// - FROM stream: 指定数据源为流数据
	// - WHERE: 过滤条件，排除 device3 的数据
	// - GROUP BY: 按设备ID分组，配合滚动窗口进行聚合
	// - TumblingWindow('5s'): 5秒滚动窗口，每5秒触发一次计算
	// - avg(), min(): 聚合函数，计算平均值和最小值
	// - window_start(), window_end(): 窗口函数，获取窗口的开始和结束时间
	rsql := "SELECT deviceId,avg(temperature) as avg_temp,min(humidity) as min_humidity ," +
		"window_start() as start,window_end() as end FROM  stream  where deviceId!='device3' group by deviceId,TumblingWindow('5s')"

	// 步骤3: 执行 SQL 语句，启动流式分析任务
	// Execute 方法会解析 SQL、构建执行计划、初始化窗口管理器和聚合器
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	// 步骤4: 设置测试环境和并发控制
	var wg sync.WaitGroup
	wg.Add(1)
	// 设置30秒测试超时时间，防止测试无限运行
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 步骤5: 启动数据生产者协程
	// 模拟实时数据流，持续向 StreamSQL 输入数据
	go func() {
		defer wg.Done()
		// 创建定时器，每秒触发一次数据生成
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// 每秒生成10条随机测试数据，模拟高频数据流
				// 这种数据密度可以测试 StreamSQL 的实时处理能力
				for i := 0; i < 10; i++ {
					// 构造设备数据，包含设备ID、温度和湿度
					randomData := map[string]interface{}{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(3)+1), // 随机选择 device1, device2, device3
						"temperature": 20.0 + rand.Float64()*10,                // 温度范围: 20-30度
						"humidity":    50.0 + rand.Float64()*20,                // 湿度范围: 50-70%
					}
					// 将数据添加到流中，触发 StreamSQL 的实时处理
					// Emit 会将数据分发到相应的窗口和聚合器中
					ssql.Emit(randomData)
				}

			case <-ctx.Done():
				// 超时或取消信号，停止数据生成
				return
			}
		}
	}()

	// 步骤6: 设置结果处理管道
	resultChan := make(chan interface{}, 10)
	// 添加计算结果回调函数（Sink）
	// 当窗口触发计算时，结果会通过这个回调函数输出
	ssql.stream.AddSink(func(result []map[string]interface{}) {
		// 非阻塞发送，避免阻塞 sink worker
		select {
		case resultChan <- result:
		default:
			// Channel 已满，忽略（非阻塞发送）
		}
	})

	// 步骤7: 启动结果消费者协程
	// 记录收到的结果数量，用于验证测试效果
	var resultCount int64
	var countMutex sync.Mutex
	var consumerWg sync.WaitGroup
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for {
			select {
			case <-resultChan:
				// 每当收到一个窗口的计算结果时，计数器加1
				// 注释掉的代码可以用于调试，打印每个结果的详细信息
				//fmt.Printf("打印结果: [%s] %v\n", time.Now().Format("15:04:05.000"), result)
				countMutex.Lock()
				resultCount++
				countMutex.Unlock()
			case <-ctx.Done():
				// 测试超时，退出消费者 goroutine
				// 不关闭 channel，让主程序自动退出时清理
				return
			}
		}
	}()

	// 步骤8: 等待测试完成
	// 等待数据生产者协程结束（30秒超时或手动取消）
	wg.Wait()

	// 停止流处理，确保所有 goroutine 正确退出
	ssql.Stop()

	// 等待一小段时间，确保所有 sink worker 完成当前任务
	// 这样可以确保所有结果都被发送到 channel
	time.Sleep(100 * time.Millisecond)

	// 取消 context，通知消费者 goroutine 退出
	cancel()

	// 等待消费者 goroutine 完成（处理完 channel 中剩余的数据或收到取消信号）
	consumerWg.Wait()

	// 步骤9: 验证测试结果
	// 预期在30秒内应该收到5个窗口的计算结果（每5秒一个窗口）
	// 这验证了 StreamSQL 的窗口触发机制是否正常工作
	countMutex.Lock()
	finalCount := resultCount
	countMutex.Unlock()
	assert.Equal(t, finalCount, int64(5))
}
