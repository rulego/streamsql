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

package e2e

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/streamsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmitSyncWithAddSink TestEmitSync simultaneously triggers the AddSink callback
func TestEmitSyncWithAddSink(t *testing.T) {
	t.Parallel()
	t.Run("非聚合查询同步+异步结果", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Perform non-aggregated queries - test the mixed use of backquoted fields and string constants
		sql := "SELECT `temperature`, humidity, `temperature` * 1.8 + 32 as temp_fahrenheit, 'normal' as status, 'sensor_data' as data_type FROM stream WHERE temperature > 20"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		// Verify that it is a non-aggregated query
		assert.False(t, ssql.IsAggregationQuery())

		// Set AddSink callbacks to collect asynchronous results
		var sinkCallCount int32
		var sinkResults []any
		var sinkResultsMux sync.Mutex // Protect sinkResults access
		ssql.AddSink(func(result []map[string]any) {
			atomic.AddInt32(&sinkCallCount, 1)
			sinkResultsMux.Lock()
			sinkResults = append(sinkResults, result)
			sinkResultsMux.Unlock()
		})

		// Test data
		testData := []map[string]any{
			{"temperature": 25.0, "humidity": 60.0}, // Meet the requirements
			{"temperature": 15.0, "humidity": 70.0}, // Filtered
			{"temperature": 30.0, "humidity": 80.0}, // Meet the requirements
		}

		var syncResults []map[string]any

		// Processing test data
		for _, data := range testData {
			// Synchronized processing
			result, err := ssql.EmitSync(data)
			require.NoError(t, err)

			if result != nil {
				syncResults = append(syncResults, result)
			}
		}

		// Wait for the asynchronous callback to complete
		time.Sleep(100 * time.Millisecond)

		// Verify synchronization results
		assert.Equal(t, 2, len(syncResults), "应该有2条同步结果（温度>20）")

		// Safely read asynchronous callback results
		sinkResultsMux.Lock()
		finalSinkResults := make([]any, len(sinkResults))
		copy(finalSinkResults, sinkResults)
		sinkResultsMux.Unlock()

		// Verify the asynchronous callback results
		finalSinkCallCount := atomic.LoadInt32(&sinkCallCount)
		assert.Equal(t, int32(2), finalSinkCallCount, "AddSink应该被调用2次")
		assert.Equal(t, 2, len(finalSinkResults), "应该收集到2条异步结果")

		// Verify the consistency between synchronous and asynchronous results
		if len(syncResults) > 0 && len(finalSinkResults) > 0 {
			// Convert the results into comparable formats
			syncTemperatures := make([]float64, 0, len(syncResults))
			syncHumidities := make([]float64, 0, len(syncResults))
			asyncTemperatures := make([]float64, 0, len(finalSinkResults))
			asyncHumidities := make([]float64, 0, len(finalSinkResults))

			// Collect synchronized results
			for _, result := range syncResults {
				syncResult := result
				syncTemperatures = append(syncTemperatures, syncResult["temperature"].(float64))
				syncHumidities = append(syncHumidities, syncResult["humidity"].(float64))

				// Validate string constant fields
				assert.Equal(t, "normal", syncResult["status"], "status字段应该是常量'normal'")
				assert.Equal(t, "sensor_data", syncResult["data_type"], "data_type字段应该是常量'sensor_data'")

				// Verify the mathematical operations of the backtick field
				expectedFahrenheit := syncResult["temperature"].(float64)*1.8 + 32
				assert.InDelta(t, expectedFahrenheit, syncResult["temp_fahrenheit"].(float64), 0.01, "华氏温度转换应该正确")

				// The validation result includes all expected fields
				assert.Contains(t, syncResult, "temperature", "应该包含temperature字段")
				assert.Contains(t, syncResult, "humidity", "应该包含humidity字段")
				assert.Contains(t, syncResult, "temp_fahrenheit", "应该包含temp_fahrenheit字段")
				assert.Contains(t, syncResult, "status", "应该包含status字段")
				assert.Contains(t, syncResult, "data_type", "应该包含data_type字段")
			}

			// Collect asynchronous results
			for _, result := range finalSinkResults {
				if sinkResultArray, ok := result.([]map[string]any); ok && len(sinkResultArray) > 0 {
					sinkResult := sinkResultArray[0]
					asyncTemperatures = append(asyncTemperatures, sinkResult["temperature"].(float64))
					asyncHumidities = append(asyncHumidities, sinkResult["humidity"].(float64))
				}
			}

			// Verify whether the result set is consistent (regardless of order)
			assert.ElementsMatch(t, syncTemperatures, asyncTemperatures, "温度值集合应该一致")
			assert.ElementsMatch(t, syncHumidities, asyncHumidities, "湿度值集合应该一致")

			// Verify whether the expected values are fully realized
			assert.Contains(t, syncTemperatures, 25.0, "同步结果应包含25.0")
			assert.Contains(t, syncTemperatures, 30.0, "同步结果应包含30.0")
			assert.Contains(t, asyncTemperatures, 25.0, "异步结果应包含25.0")
			assert.Contains(t, asyncTemperatures, 30.0, "异步结果应包含30.0")
		}
	})

	t.Run("聚合查询不支持EmitSync", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Perform aggregated queries
		sql := "SELECT AVG(temperature) as avg_temp FROM stream GROUP BY TumblingWindow('1s')"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		// Validation is an aggregated query
		assert.True(t, ssql.IsAggregationQuery())

		// Attempting synchronization should return an error
		data := map[string]any{"temperature": 25.0}
		result, err := ssql.EmitSync(data)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "synchronous mode only supports non-aggregation queries, use Emit() method for aggregation queries")
	})

	t.Run("多个AddSink回调都被触发", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Perform non-aggregated queries
		sql := "SELECT temperature FROM stream"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		// Add multiple AddSink callbacks and use atomic operations to ensure thread safety
		var sink1Count, sink2Count, sink3Count int32

		ssql.AddSink(func(result []map[string]any) {
			atomic.AddInt32(&sink1Count, 1)
		})

		ssql.AddSink(func(result []map[string]any) {
			atomic.AddInt32(&sink2Count, 1)
		})

		ssql.AddSink(func(result []map[string]any) {
			atomic.AddInt32(&sink3Count, 1)
		})

		// Processing a single piece of data
		data := map[string]any{"temperature": 25.0}
		result, err := ssql.EmitSync(data)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Waiting for an asynchronous pullback
		time.Sleep(100 * time.Millisecond)

		// Verify that all callbacks are triggered
		assert.Equal(t, int32(1), atomic.LoadInt32(&sink1Count))
		assert.Equal(t, int32(1), atomic.LoadInt32(&sink2Count))
		assert.Equal(t, int32(1), atomic.LoadInt32(&sink3Count))
	})

	t.Run("过滤条件不匹配时AddSink不触发", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// Execute queries with filtering conditions
		sql := "SELECT temperature FROM stream WHERE temperature > 30"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		// Add AddSink callbacks
		var sinkCallCount int32
		ssql.AddSink(func(result []map[string]any) {
			atomic.AddInt32(&sinkCallCount, 1)
		})

		// Handling data that does not meet the criteria
		data := map[string]any{"temperature": 20.0} // Does not meet the conditions of > 30
		result, err := ssql.EmitSync(data)
		require.NoError(t, err)
		assert.Nil(t, result, "不符合过滤条件应该返回nil")

		// Waiting for possible asynchronous pullbacks
		time.Sleep(100 * time.Millisecond)

		// Verify that AddSink is not triggered
		assert.Equal(t, int32(0), atomic.LoadInt32(&sinkCallCount), "过滤掉的数据不应触发AddSink")
	})

	// New test: complex mixed usage of string constants and inquotation fields
	t.Run("字符串常量与反引号字段混合用法", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

		// The test contains SQL queries containing various string constants
		sql := "SELECT `temperature` as temp, 'celsius' as unit, 'high' as level, `humidity`, 'percent' as humidity_unit FROM stream WHERE temperature > 20"
		err := ssql.Execute(sql)
		require.NoError(t, err)

		// Test data
		testData := map[string]any{
			"temperature": 25.5,
			"humidity":    65.0,
		}

		// Synchronized processing
		result, err := ssql.EmitSync(testData)
		require.NoError(t, err)
		require.NotNil(t, result)
		syncResult := result
		// Validate the backquoted field
		assert.Equal(t, 25.5, syncResult["temp"], "温度字段应该正确")
		assert.Equal(t, 65.0, syncResult["humidity"], "湿度字段应该正确")

		// Validate string constant fields
		assert.Equal(t, "celsius", syncResult["unit"], "单位应该是celsius")
		assert.Equal(t, "high", syncResult["level"], "级别应该是high")
		assert.Equal(t, "percent", syncResult["humidity_unit"], "湿度单位应该是percent")
	})
}

// TestEmitSyncPerformance Tests EmitSync performance (including AddSink triggers)
func TestEmitSyncPerformance(t *testing.T) {
	t.Parallel()
	ssql := streamsql.New()
	defer ssql.Stop()

	sql := "SELECT temperature, humidity FROM stream WHERE temperature > 0"
	err := ssql.Execute(sql)
	require.NoError(t, err)

	// Add AddSink callbacks and use atomic operations to ensure thread safety
	var sinkCallCount int32
	ssql.AddSink(func(result []map[string]any) {
		atomic.AddInt32(&sinkCallCount, 1)
	})

	// Performance testing
	testCount := 1000

	start := time.Now()
	for i := 0; i < testCount; i++ {
		data := map[string]any{
			"temperature": float64(20 + i%20),
			"humidity":    float64(50 + i%30),
		}

		result, err := ssql.EmitSync(data)
		require.NoError(t, err)
		require.NotNil(t, result)
	}
	duration := time.Since(start)

	// Wait for all asynchronous callbacks to complete
	time.Sleep(200 * time.Millisecond)

	// Verify performance and consistency
	assert.Less(t, duration, 1*time.Second, "性能应该足够好")
	assert.Equal(t, int32(testCount), atomic.LoadInt32(&sinkCallCount), "所有数据都应触发AddSink")
}
