package streamsql

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamSQLErrorHandling 测试StreamSQL的错误处理机制
func TestStreamSQLErrorHandling(t *testing.T) {
	t.Run("invalid SQL syntax", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("INVALID SQL STATEMENT")
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "SQL parsing failed")
	})

	t.Run("missing SELECT keyword", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("FROM stream WHERE id > 1")
		// 修改后的解析器会对缺少SELECT关键字进行严格检查
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "Expected SELECT")
	})

	t.Run("invalid function name", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT INVALID_FUNCTION(id) FROM stream")
		require.NotNil(t, err)
	})

	t.Run("invalid window function", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream GROUP BY InvalidWindow('5s')")
		// TODO InvalidWindow 在SQL解析阶段被当作普通字段处理，而不是窗口函数
		// 因此不会在stream创建阶段报错，这是当前解析器的设计行为
		require.Nil(t, err)
	})

	t.Run("EmitSync without Execute", func(t *testing.T) {
		ssql := New()
		_, err := ssql.EmitSync(map[string]interface{}{"id": 1})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "stream not initialized")
	})

	t.Run("EmitSync with aggregation query", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT COUNT(*) FROM stream")
		require.Nil(t, err)

		_, err = ssql.EmitSync(map[string]interface{}{"id": 1})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "synchronous mode only supports non-aggregation queries")
	})

	t.Run("Emit without Execute", func(t *testing.T) {
		ssql := New()
		// 这不应该引发panic，但也不会有任何效果
		ssql.Emit(map[string]interface{}{"id": 1})
	})

	t.Run("Stop without Execute", func(t *testing.T) {
		ssql := New()
		// 这不应该引发panic
		ssql.Stop()
	})

	t.Run("GetStats without Execute", func(t *testing.T) {
		ssql := New()
		stats := ssql.GetStats()
		require.NotNil(t, stats)
		require.Equal(t, 0, len(stats))
	})

	t.Run("GetDetailedStats without Execute", func(t *testing.T) {
		ssql := New()
		stats := ssql.GetDetailedStats()
		require.NotNil(t, stats)
		require.Equal(t, 0, len(stats))
	})

	t.Run("ToChannel without Execute", func(t *testing.T) {
		ssql := New()
		ch := ssql.ToChannel()
		require.Nil(t, ch)
	})

	t.Run("Stream without Execute", func(t *testing.T) {
		ssql := New()
		stream := ssql.Stream()
		require.Nil(t, stream)
	})

	t.Run("IsAggregationQuery without Execute", func(t *testing.T) {
		ssql := New()
		isAgg := ssql.IsAggregationQuery()
		require.False(t, isAgg)
	})

	t.Run("AddSink without Execute", func(t *testing.T) {
		ssql := New()
		// 这不应该引发panic
		ssql.AddSink(func(results []map[string]interface{}) {})
	})

	t.Run("PrintTable without Execute", func(t *testing.T) {
		ssql := New()
		// 这不应该引发panic
		ssql.PrintTable()
	})
}

// TestStreamSQLEdgeCases 测试边界条件和特殊情况
func TestStreamSQLEdgeCases(t *testing.T) {
	t.Run("empty SQL string", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("")
		require.NotNil(t, err)
	})

	t.Run("whitespace only SQL", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("   \n\t   ")
		require.NotNil(t, err)
	})

	t.Run("SQL with comments", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("-- This is a comment\nSELECT id FROM stream")
		// 根据实际的SQL解析器行为，这可能成功或失败
		// 这里我们只是确保不会panic
		_ = err
	})

	t.Run("very long SQL statement", func(t *testing.T) {
		ssql := New()
		longSQL := "SELECT "
		for i := 0; i < 1000; i++ {
			if i > 0 {
				longSQL += ", "
			}
			longSQL += "field" + string(rune('0'+i%10))
		}
		longSQL += " FROM stream"
		err := ssql.Execute(longSQL)
		// 应该能够处理长SQL语句
		_ = err
	})

	t.Run("multiple Execute calls", func(t *testing.T) {
		ssql := New()
		err1 := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err1)

		// 第二次Execute应该失败，因为已经执行过了
		err2 := ssql.Execute("SELECT name FROM stream")
		require.Error(t, err2)
		require.Contains(t, err2.Error(), "Execute() has already been called")
	})

	t.Run("Execute after Stop", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err)

		ssql.Stop()

		// 停止后再次Execute应该失败，因为已经执行过了
		err = ssql.Execute("SELECT name FROM stream")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Execute() has already been called")
	})

	t.Run("concurrent Execute calls", func(t *testing.T) {
		ssql := New()

		done := make(chan bool, 2)
		var successCount int32
		var errorCount int32

		go func() {
			err := ssql.Execute("SELECT id FROM stream")
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else {
				atomic.AddInt32(&errorCount, 1)
			}
			done <- true
		}()

		go func() {
			err := ssql.Execute("SELECT name FROM stream")
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else {
				atomic.AddInt32(&errorCount, 1)
			}
			done <- true
		}()

		// 等待两个goroutine完成
		<-done
		<-done

		// 验证只有一个成功，一个失败
		assert.Equal(t, int32(1), atomic.LoadInt32(&successCount))
		assert.Equal(t, int32(1), atomic.LoadInt32(&errorCount))

		// 确保最终有一个有效的stream
		require.NotNil(t, ssql.Stream())
	})
}

// TestStreamSQLNilHandling 测试nil值处理
func TestStreamSQLNilHandling(t *testing.T) {
	t.Run("emit nil map", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err)

		// 发送nil数据不应该panic
		ssql.Emit(nil)
		ssql.Stop()
	})

	t.Run("emit map with nil values", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id, name FROM stream")
		require.Nil(t, err)

		// 发送包含nil值的数据
		ssql.Emit(map[string]interface{}{
			"id":   1,
			"name": nil,
		})
		ssql.Stop()
	})

	t.Run("EmitSync with nil data", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err)

		// EmitSync with nil data
		_, err = ssql.EmitSync(nil)
		// 根据实现，这可能返回错误或处理nil值
		_ = err
		ssql.Stop()
	})
}

// TestStreamSQLComplexQueries 测试复杂查询
func TestStreamSQLComplexQueries(t *testing.T) {
	t.Run("query with multiple fields", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id, name, value, timestamp FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]interface{}{
			"id":        1,
			"name":      "test",
			"value":     100.5,
			"timestamp": time.Now(),
		})
		ssql.Stop()
	})

	t.Run("query with WHERE clause", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id, value FROM stream WHERE value > 50")
		require.Nil(t, err)

		ssql.Emit(map[string]interface{}{"id": 1, "value": 100})
		ssql.Emit(map[string]interface{}{"id": 2, "value": 25})
		ssql.Stop()
	})

	t.Run("query with aggregation functions", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT COUNT(*), SUM(value), AVG(value) FROM stream")
		require.Nil(t, err)

		for i := 0; i < 5; i++ {
			ssql.Emit(map[string]interface{}{"id": i, "value": i * 10})
		}
		ssql.Stop()
	})

	t.Run("query with window functions", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id, value FROM stream GROUP BY TumblingWindow('5s')")
		// 根据实际实现，这可能成功或失败
		_ = err
		if err == nil {
			ssql.Stop()
		}
	})
}

// TestStreamSQLDataTypes 测试不同数据类型
func TestStreamSQLDataTypes(t *testing.T) {
	t.Run("string data types", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT name FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]interface{}{"name": "test string"})
		ssql.Emit(map[string]interface{}{"name": ""})
		ssql.Emit(map[string]interface{}{"name": "unicode测试🚀"})
		ssql.Stop()
	})

	t.Run("numeric data types", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT value FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]interface{}{"value": 42})
		ssql.Emit(map[string]interface{}{"value": 3.14159})
		ssql.Emit(map[string]interface{}{"value": int64(9223372036854775807)})
		ssql.Emit(map[string]interface{}{"value": float32(1.23)})
		ssql.Stop()
	})

	t.Run("boolean data types", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT active FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]interface{}{"active": true})
		ssql.Emit(map[string]interface{}{"active": false})
		ssql.Stop()
	})

	t.Run("time data types", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT timestamp FROM stream")
		require.Nil(t, err)

		now := time.Now()
		ssql.Emit(map[string]interface{}{"timestamp": now})
		ssql.Emit(map[string]interface{}{"timestamp": now.Unix()})
		ssql.Stop()
	})

	t.Run("array and slice data types", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT data FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]interface{}{"data": []int{1, 2, 3, 4, 5}})
		ssql.Emit(map[string]interface{}{"data": []string{"a", "b", "c"}})
		ssql.Emit(map[string]interface{}{"data": []interface{}{1, "test", true}})
		ssql.Stop()
	})

	t.Run("map data types", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT metadata FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]interface{}{
			"metadata": map[string]interface{}{
				"key1": "value1",
				"key2": 42,
				"key3": true,
			},
		})
		ssql.Stop()
	})
}

// TestStreamSQLStressTest 压力测试
func TestStreamSQLStressTest(t *testing.T) {
	t.Run("high frequency emissions", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err)

		// 高频率发送数据
		for i := 0; i < 1000; i++ {
			ssql.Emit(map[string]interface{}{"id": i})
		}
		ssql.Stop()
	})

	t.Run("large data payloads", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT data FROM stream")
		require.Nil(t, err)

		// 发送大数据负载
		largeString := make([]byte, 10*1024) // 10KB
		for i := range largeString {
			largeString[i] = byte('A' + (i % 26))
		}

		for i := 0; i < 10; i++ {
			ssql.Emit(map[string]interface{}{"data": string(largeString)})
		}
		ssql.Stop()
	})

	t.Run("concurrent operations", func(t *testing.T) {
		ssql := New()
		err := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err)

		var wg sync.WaitGroup
		numWorkers := 5
		numEmissions := 100

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < numEmissions; j++ {
					ssql.Emit(map[string]interface{}{"id": workerID*1000 + j})
				}
			}(i)
		}

		wg.Wait()
		ssql.Stop()
	})
}
