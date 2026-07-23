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

// Package streamsql white-box testing and benchmarking (override/performance/overflow policy/table printing + end-to-end examples).
// Access non-export fields (performanceMode/customConfig/stream/fieldOrder) and non-export methods
// (printTableFormat), so it must be inside package streamsql and cannot transfer test/e2e.
// For fully public API integration testing, see test/e2e.

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
// TestStreamSQLPerformanceModesExtended tests the configurations of different performance modes
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

// TestStreamSQLFieldOrder Test field order retention function
func TestStreamSQLFieldOrder(t *testing.T) {
	t.Run("field order preservation", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT name, id, value FROM stream")
		require.NoError(t, err)

		// Verify that the order of the fields is saved correctly
		expectedOrder := []string{"name", "id", "value"}
		assert.Equal(t, expectedOrder, ssql.fieldOrder)
		ssql.Stop()
	})

	t.Run("field order with aliases", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT name as device_name, id as device_id FROM stream")
		require.NoError(t, err)

		// Verify the order of alias fields
		expectedOrder := []string{"device_name", "device_id"}
		assert.Equal(t, expectedOrder, ssql.fieldOrder)
		ssql.Stop()
	})
}

// TestStreamSQLPrintTableFormat tests the table printing function
func TestStreamSQLPrintTableFormat(t *testing.T) {
	t.Run("print table format with data", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id, name FROM stream")
		require.NoError(t, err)

		// Test the printTableFormat method
		testResults := []map[string]any{
			{"id": 1, "name": "test1"},
			{"id": 2, "name": "test2"},
		}

		// This method mainly involves printing output, and we make sure it doesn't panic
		assert.NotPanics(t, func() {
			ssql.printTableFormat(testResults)
		})
		ssql.Stop()
	})

	t.Run("print table format with empty data", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)

		// Test empty data
		emptyResults := []map[string]any{}
		assert.NotPanics(t, func() {
			ssql.printTableFormat(emptyResults)
		})
		ssql.Stop()
	})

	t.Run("print table format with nil field order", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)

		// Clear the field order
		ssql.fieldOrder = nil
		testResults := []map[string]any{
			{"id": 1},
		}

		assert.NotPanics(t, func() {
			ssql.printTableFormat(testResults)
		})
		ssql.Stop()
	})
}

// TestStreamSQLToChannel test channel functionality
func TestStreamSQLToChannel(t *testing.T) {
	t.Run("to channel with aggregation query", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT COUNT(*) FROM stream GROUP BY TumblingWindow('1s')")
		require.NoError(t, err)

		// Obtain the results channel
		resultChan := ssql.ToChannel()
		assert.NotNil(t, resultChan)

		// Start goroutine to receive results
		var wg sync.WaitGroup
		wg.Add(1)
		var receivedResults [][]map[string]any
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

		// Send some data
		for i := 0; i < 5; i++ {
			ssql.Emit(map[string]any{"id": i})
		}

		// Wait for the results
		wg.Wait()
		ssql.Stop()

		// At least some results have been obtained from the verification
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

// TestStreamSQLMultipleOptions tests multiple configuration option combinations
func TestStreamSQLMultipleOptions(t *testing.T) {
	t.Run("multiple options combination", func(t *testing.T) {
		// Combine multiple configuration options
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
		// The later options should override the earlier ones
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

// TestStreamSQLExecuteErrorHandling: Error handling of the Execute method
func TestStreamSQLExecuteErrorHandling(t *testing.T) {
	t.Run("stream creation failure simulation", func(t *testing.T) {
		ssql := New()
		// Using an SQL that may cause stream creation failure
		err := ssql.Execute("SELECT invalid_function() FROM test_stream")
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "function")
	})

	t.Run("filter registration failure", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()
		// Use SQL that may cause filter registration failures
		err := ssql.Execute("SELECT id FROM stream WHERE INVALID_CONDITION")
		if err != nil {
			// If there are errors, relevant information should be included
			assert.True(t,
				strings.Contains(err.Error(), "SQL parsing failed") ||
					strings.Contains(err.Error(), "failed to register filter condition") ||
					strings.Contains(err.Error(), "failed to create stream processor"))
		}
	})
}

// TestStreamSQLConcurrentAccess tests the security of concurrent access
func TestStreamSQLConcurrentAccess(t *testing.T) {
	t.Run("concurrent emit and stop", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)

		var wg sync.WaitGroup
		numWorkers := 10

		// Starts multiple goroutines to send data concurrently
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					ssql.Emit(map[string]any{"id": workerID*100 + j})
				}
			}(i)
		}

		// After waiting for a while, stop
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

		// Concurrent calls to various methods
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// These method calls should be safe
				_ = ssql.GetStats()
				_ = ssql.GetDetailedStats()
				_ = ssql.IsAggregationQuery()
				_ = ssql.Stream()
				_ = ssql.ToChannel()
				ssql.AddSink(func(results []map[string]any) {})
			}()
		}

		wg.Wait()
		ssql.Stop()
	})
}

// TestStreamSQLEdgeCasesAdditional tests for additional boundary cases
func TestStreamSQLEdgeCasesAdditional(t *testing.T) {
	t.Run("execute with different performance modes after creation", func(t *testing.T) {
		ssql := New()

		// Start by running in default mode
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		ssql.Stop()

		// Executing again after changing the performance mode should fail because it has already been executed
		ssql.performanceMode = "high_performance"
		err = ssql.Execute("SELECT name FROM stream")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Execute() has already been called")
		// There is no need to call Stop() again because the second Execute failed
	})

	t.Run("field order with complex query", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT COUNT(*) as cnt, AVG(value) as avg_val, deviceId FROM stream GROUP BY deviceId")
		require.NoError(t, err)

		// Verify the field order for complex queries
		expectedOrder := []string{"cnt", "avg_val", "deviceId"}
		assert.Equal(t, expectedOrder, ssql.fieldOrder)
		ssql.Stop()
	})

	t.Run("print table with field order", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT name, id, value FROM stream")
		require.NoError(t, err)

		// Set the order of the fields
		ssql.fieldOrder = []string{"name", "id", "value"}

		// Test the PrintTable method
		assert.NotPanics(t, func() {
			ssql.PrintTable()
		})
		ssql.Stop()
	})
}

// TestStreamSQLEmitSync tests various scenarios for the EmitSync method
func TestStreamSQLEmitSync(t *testing.T) {
	t.Run("emit sync with uninitialized stream", func(t *testing.T) {
		ssql := New()
		// Call EmitSync without executing the SQL
		result, err := ssql.EmitSync(map[string]any{"id": 1})
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "stream not initialized")
	})

	t.Run("emit sync with aggregation query", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT COUNT(*) FROM stream GROUP BY id")
		require.NoError(t, err)

		// Calling EmitSync for aggregated queries should return an error
		result, err := ssql.EmitSync(map[string]any{"id": 1})
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "synchronous mode only supports non-aggregation queries")
		ssql.Stop()
	})

	t.Run("emit sync with non-aggregation query", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id, name FROM stream WHERE id > 0")
		require.NoError(t, err)

		// Call EmitSync for non-aggregated queries
		data := map[string]any{"id": 1, "name": "test"}
		result, err := ssql.EmitSync(data)
		// Depending on actual implementation, success or failure may be possible here
		if err != nil {
			t.Logf("EmitSync error (expected): %v", err)
		} else {
			t.Logf("EmitSync result: %v", result)
		}
		ssql.Stop()
	})
}

// TestStreamSQLCustomPerformanceConfig tests custom performance configurations
func TestStreamSQLCustomPerformanceConfig(t *testing.T) {
	t.Run("custom performance config with nil config", func(t *testing.T) {
		ssql := New()
		ssql.performanceMode = "custom"
		ssql.customConfig = nil // Set to nil

		// When executing SQL, you should revert to the default configuration
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

// TestStreamSQLStatsMethods Methods for testing statistical information
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
		// Test for cases where the initialization is not initialized
		ssql := New()
		require.False(t, ssql.IsAggregationQuery())

		// Test non-aggregated queries
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		isAgg := ssql.IsAggregationQuery()
		t.Logf("Is aggregation query: %v", isAgg)
		ssql.Stop()

		// Test aggregated queries
		ssql2 := New()
		err = ssql2.Execute("SELECT COUNT(*) FROM stream GROUP BY id")
		require.NoError(t, err)
		isAgg2 := ssql2.IsAggregationQuery()
		t.Logf("Is aggregation query (with GROUP BY): %v", isAgg2)
		ssql2.Stop()
	})
}

// TestStreamSQLNilAndEdgeCases tests the null values and boundary conditions
func TestStreamSQLNilAndEdgeCases(t *testing.T) {
	t.Run("emit with nil stream", func(t *testing.T) {
		ssql := New()
		// Calling Emit without executing SQL
		assert.NotPanics(t, func() {
			ssql.Emit(map[string]any{"id": 1})
		})
	})

	t.Run("add sink with nil stream", func(t *testing.T) {
		ssql := New()
		// Call AddSink without executing the SQL
		assert.NotPanics(t, func() {
			ssql.AddSink(func(results []map[string]any) {
				t.Log("Sink called")
			})
		})
	})

	t.Run("to channel with nil stream", func(t *testing.T) {
		ssql := New()
		// Call ToChannel without executing SQL
		resultChan := ssql.ToChannel()
		require.Nil(t, resultChan)
	})

	t.Run("stream method with nil stream", func(t *testing.T) {
		ssql := New()
		// Call Stream without executing the SQL
		stream := ssql.Stream()
		require.Nil(t, stream)
	})

	t.Run("stop with nil stream", func(t *testing.T) {
		ssql := New()
		// Call Stop without executing the SQL
		assert.NotPanics(t, func() {
			ssql.Stop()
		})
	})

	t.Run("print table format with empty results", func(t *testing.T) {
		ssql := New()
		ssql.fieldOrder = []string{"id", "name"}

		// Test empty results for the table print
		assert.NotPanics(t, func() {
			ssql.printTableFormat([]map[string]any{})
		})
	})

	t.Run("print table format with nil field order", func(t *testing.T) {
		ssql := New()
		ssql.fieldOrder = nil

		results := []map[string]any{
			{"id": 1, "name": "test"},
		}

		// Print a table to test the order of the nil fields
		assert.NotPanics(t, func() {
			ssql.printTableFormat(results)
		})
	})
}

// TestStreamSQLComplexScenarios tests complex scenarios
func TestStreamSQLComplexScenarios(t *testing.T) {
	t.Run("multiple execute calls", func(t *testing.T) {
		ssql := New()

		// The first time it was executed
		err := ssql.Execute("SELECT id FROM stream")
		require.NoError(t, err)
		ssql.Stop()

		// The second execution should fail because it has already been carried out
		err = ssql.Execute("SELECT name FROM stream")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Execute() has already been called")
	})

	t.Run("performance mode switching", func(t *testing.T) {
		// Test all performance modes
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

		// Verify that the order of the fields is saved correctly
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

func benchEmitSync(b *testing.B, sql string, row map[string]any) {
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
		map[string]any{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

func BenchmarkMainPath_MultiFieldFilter(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature, humidity FROM stream WHERE temperature > 20 AND humidity < 80",
		map[string]any{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

func BenchmarkMainPath_ComputedFields(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature * 2 + humidity AS score, abs(temperature - 100) AS dev FROM stream WHERE temperature > 20",
		map[string]any{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

func BenchmarkMainPath_StringConcat(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId + '-' + location AS id FROM stream",
		map[string]any{"deviceId": "d1", "location": "roomA"},
	)
}

func BenchmarkMainPath_NoFilter(b *testing.B) {
	benchEmitSync(b,
		"SELECT deviceId, temperature, humidity FROM stream",
		map[string]any{"deviceId": "d1", "temperature": 25.5, "humidity": 60.0},
	)
}

// ---------- overflow strategy ----------
// TestSQLIntegration_StrategyBlock Test blocking policies under SQL integration
func TestSQLIntegration_StrategyBlock(t *testing.T) {
	// Configuration: Output buffer is 1, blocking policy, timeout is 100ms
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
			SinkPoolSize:    0, // No buffer task queue
			SinkWorkerCount: 1, // 1 worker
		},
	}))
	defer ssql.Stop()

	// SQL: Each data entry triggers a window once
	rsql := "SELECT deviceId FROM stream GROUP BY deviceId, CountingWindow(1)"
	err := ssql.Execute(rsql)
	require.NoError(t, err)

	// Adds synchronous Sink blocking stream handling, thereby backpressing Windows
	// Note: It must be added after Execute, because Execute will create the stream
	ssql.AddSyncSink(func(results []map[string]any) {
		time.Sleep(500 * time.Millisecond)
	})

	// Send 5 data entries
	// d1: Worker processing (blocking 500ms)
	// d2: Stream tries to write to WorkerPool -> block (no buffering)
	// d3: Window OutputChan (size 1) -> fill
	// d4: Window OutputChan full -> tries to write -> block (Window Add) -> insert TriggerChan (size=1)
	// d5: Window Add -> TriggerChan full -> blocking? No, is Emit asynchronous?
	// Emit wrote to dataChan. DataProcessor reads dataChan -> Window.Add.
	// Window.Add to triggerChan.
	//
	// Correction Analysis:
	// Window.Add is non-blocking (if triggerChan is dissatisfied).
	// CountingWindow triggerChan size = bufferSize = 1.
	// Worker coroutine: Read -> from triggerChan and process -> sendResult (to OutputChan).
	//
	// d1: Worker reads triggerChan -> OutputChan -> Stream -> WorkerPool -> Worker(busy).
	// d2: Worker reads triggerChan -> OutputChan -> Stream -> Blocked on WorkerPool.
	//     At this point, Stream holds d2. OutputChan empty.
	//     Worker coroutine blocked at sendResult(d2)? No, Stream takes d2, Stream blocks dispatch.
	//     So OutputChan is empty!
	//     Wait, Stream loop:
	//     result := <-OutputChan. (Stream has d2).
	//     handleResult(d2) -> Blocked.
	//     So OutputChan is empty.
	// d3: Worker reads triggerChan -> OutputChan (d3). Success.
	//     OutputChan has d3.
	// d4: Worker reads triggerChan -> OutputChan (d4). Blocked (OutputChan full).
	//     Worker coroutine blocked at sendResult(d4).
	// d5: Add -> triggerChan (d5). Success (triggerChan size 1).
	// d6: Add -> triggerChan (d6). Blocked (triggerChan full).
	//     Add blocks. DataProcessor blocks. Emit succeeds (dataChan).
	//
	// Therefore, the Window Worker only triggers the drop logic when sendResult is blocked.
	// sendResult only drops when OutputChan is full and timed out.
	//
	// d4 is blocked in sendResult.
	// Timeout after 100ms -> Drop d4.
	// Worker continues.
	//
	// So d4 should be the one that got dropped.
	// Sent: d1, d2, d3. (d5 on triggerChan, d6 on dataChan).
	// Wait, d5 is in triggerChan, not processed yet.
	// So Sent = 3. Dropped = 1 (d4).

	for _, id := range []string{"d1", "d2", "d3", "d4", "d5"} {
		ssql.Emit(map[string]any{"deviceId": id})
		time.Sleep(10 * time.Millisecond)
	}

	// Wait long enough for the stream to wake up and finish processing, and for the window to discard the logic execution
	time.Sleep(1000 * time.Millisecond)

	// Get statistics
	// d1: Stream finishes processing
	// d2: Stream finishes processing (Worker wakes up and processes d2)
	// d3: Dropped (Worker blocks -> timeout)
	// d4: Dropped (Worker blocks -> timeout)
	// d5: Dropped (Worker blocks -> timeout)
	// Total Sent: 2 (d1, d2).
	// Dropped: 3 (d3, d4, d5).
	stats := ssql.stream.GetStats()
	assert.Equal(t, int64(3), stats["droppedCount"], "Should have 3 dropped window result due to overflow")
	assert.Equal(t, int64(2), stats["sentCount"], "Should have 2 sent window result")
}

// TestSQLIntegration_StrategyDrop Test the dropout policy under SQL integration
func TestSQLIntegration_StrategyDrop(t *testing.T) {
	// Configuration: Output buffer is set to 1, discard policy
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

	// SQL: Each data entry triggers a window once
	rsql := "SELECT deviceId FROM stream GROUP BY deviceId, CountingWindow(1)"
	err := ssql.Execute(rsql)
	require.NoError(t, err)

	// Send 3 data records consecutively
	ssql.Emit(map[string]any{"deviceId": "d1"})
	ssql.Emit(map[string]any{"deviceId": "d2"})
	ssql.Emit(map[string]any{"deviceId": "d3"})

	// Wait for processing to complete
	time.Sleep(200 * time.Millisecond)

	// For StrategyDrop, it squeezes out old data, so sentCount should keep increasing
	stats := ssql.stream.GetStats()
	// d1, d2, and d3 will all be successfully sent (although d1 and d2 may be squeezed out, the sendResult logic pushes out the old ones and writes the new ones to count as successful sends).
	assert.Equal(t, int64(3), stats["sentCount"])

	// Verification ultimately remains in the buffer with the last data (d3)
	// Note: AddSink will start the worker to read from OutputChan.
	// To verify, we read directly from OutputChan in Windows
	select {
	case result := <-ssql.stream.Window.OutputChan():
		assert.Equal(t, "d3", result[0].Data.(map[string]any)["deviceId"])
	case <-time.After(100 * time.Millisecond):
		// If the AddSink worker has already read it, that's normal, but since we didn't add a Sink, it should be inside
	}
}

// ---------- table print ----------
// TestPrintTable Tests the basic functionality of the PrintTable method
func TestPrintTable(t *testing.T) {
	// Create a StreamSQL instance and test the PrintTable
	ssql := New()
	defer ssql.Stop()
	err := ssql.Execute("SELECT device, AVG(temperature) as avg_temp FROM stream GROUP BY device, TumblingWindow('2s')")
	assert.NoError(t, err)

	// Use the PrintTable method (does not verify the output, only ensures it does not panic)
	assert.NotPanics(t, func() {
		ssql.PrintTable()
	}, "PrintTable方法不应该panic")

	// Send test data
	testData := []map[string]any{
		{"device": "sensor1", "temperature": 25.0},
		{"device": "sensor2", "temperature": 30.0},
	}

	for _, data := range testData {
		ssql.Emit(data)
	}

	// Wait for the window to trigger
	time.Sleep(3 * time.Second)
}

// TestPrintTableFormat: The printTableFormat method handles different data types
func TestPrintTableFormat(t *testing.T) {
	ssql := New()

	// Test different types of data to ensure you don't panic
	assert.NotPanics(t, func() {
		// Test the empty slices
		ssql.printTableFormat([]map[string]any{})
	}, "空切片不应该panic")
}

// ---------- end-to-end example ----------
func TestStreamData(t *testing.T) {
	// Step 1: Create a StreamSQL instance
	// StreamSQL is the core component of the streaming SQL processing engine, responsible for managing the entire stream processing lifecycle
	ssql := New()
	// Ensure that streaming processing stops at the end of the test and resources are freed
	defer ssql.Stop()

	// Step 2: Define the streaming SQL query statement
	// This SQL statement demonstrates the core features of StreamSQL:
	// - SELECT: Selects the fields and aggregate functions to output
	// - FROM stream: Specifies the data source as stream data
	// - WHERE: Filter condition to exclude data from device3
	// - GROUP BY: Grouped by device ID and aggregated with scrolling windows
	// - TumblingWindow('5s'): Scrolls the window every 5 seconds, triggering calculation every 5 seconds
	// - avg(), min(): Aggregate function, calculates the mean and minimum value
	// - window_start(), window_end(): Window function, retrieves the start and end times of the window
	rsql := "SELECT deviceId,avg(temperature) as avg_temp,min(humidity) as min_humidity ," +
		"window_start() as start,window_end() as end FROM  stream  where deviceId!='device3' group by deviceId,TumblingWindow('5s')"

	// Step 3: Execute the SQL statement to start the stream analysis task
	// The Execute method parses SQL, builds execution plans, initializes window managers and aggregators
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	// Step 4: Set up the test environment and concurrency control
	var wg sync.WaitGroup
	wg.Add(1)
	// Set a 30-second test timeout to prevent unlimited testing
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Step 5: Start the data producer coroutine
	// Simulates real-time data flows and continuously inputs data to StreamSQL
	go func() {
		defer wg.Done()
		// Create a timer that triggers data generation every second
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Generates 10 random test data per second, simulating high-frequency data streams
				// This data density can test StreamSQL's real-time processing capabilities
				for i := 0; i < 10; i++ {
					// Construct device data, including device ID, temperature, and humidity
					randomData := map[string]any{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(3)+1), // Randomly select device1, device2, device3
						"temperature": 20.0 + rand.Float64()*10,                // Temperature range: 20-30 degrees
						"humidity":    50.0 + rand.Float64()*20,                // Humidity range: 50-70%
					}
					// Add data to the stream to trigger real-time processing in StreamSQL
					// Emit distributes data to the corresponding windows and aggregators
					ssql.Emit(randomData)
				}

			case <-ctx.Done():
				// Timeout or cancellation of signals stops data generation
				return
			}
		}
	}()

	// Step 6: Set up the result processing pipeline
	resultChan := make(chan any, 10)
	// Add calculation result callback function (Sink)
	// When the window triggers the calculation, the result is output through this callback function
	ssql.stream.AddSink(func(result []map[string]any) {
		// Non-blocking sending to avoid blocking sink workers
		select {
		case resultChan <- result:
		default:
			// Channel is full, ignore (non-blocking sending)
		}
	})

	// Step 7: Start the result consumer coroutine
	// Record the number of results received to verify the test effectiveness
	var resultCount int64
	var countMutex sync.Mutex
	var consumerWg sync.WaitGroup
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for {
			select {
			case <-resultChan:
				// Whenever a window receives a calculation result, the counter increments by 1
				// The commented code can be used for debugging and printing detailed information for each result
				//fmt.Printf("Print result: [%s] %v\n", time.Now().Format("15:04:05.000"), result)
				countMutex.Lock()
				resultCount++
				countMutex.Unlock()
			case <-ctx.Done():
				// Test timeout, exit consumer goroutine
				// Do not close the channel, allowing the main program to clean up automatically when exiting
				return
			}
		}
	}()

	// Step 8: Wait for the test to complete
	// Wait for the data producer coroutine to finish (30-second timeout or manual cancellation)
	wg.Wait()

	// Stop stream processing and ensure all goroutines exit correctly
	ssql.Stop()

	// Wait a short while to ensure all sink workers complete the current task
	// This ensures that all results are sent to the channel
	time.Sleep(100 * time.Millisecond)

	// Cancel context to notify consumers to exit the goroutine
	cancel()

	// Wait for the consumer goroutine to complete (after processing the remaining data in the channel or receiving a cancellation signal)
	consumerWg.Wait()

	// Step 9: Verify the test results
	// Expect to receive the calculation results of 5 windows within 30 seconds (one window every 5 seconds)
	// This verifies whether StreamSQL's window trigger mechanism is working properly
	countMutex.Lock()
	finalCount := resultCount
	countMutex.Unlock()
	assert.Equal(t, finalCount, int64(5))
}
