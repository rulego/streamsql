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

// TestStreamSQLErrorHandling tests StreamSQL's error handling mechanism
func TestStreamSQLErrorHandling(t *testing.T) {
	t.Parallel()
	t.Run("invalid SQL syntax", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("INVALID SQL STATEMENT")
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "SQL parsing failed")
	})

	t.Run("missing SELECT keyword", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("FROM stream WHERE id > 1")
		// The modified parser will strictly check for missing SELECT keywords
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "Expected SELECT")
	})

	t.Run("invalid function name", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT INVALID_FUNCTION(id) FROM stream")
		require.NotNil(t, err)
	})

	t.Run("invalid window function", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT id FROM stream GROUP BY InvalidWindow('5s')")
		// Unknown/misspelled window functions are rejected at parse time.
		// (Previously InvalidWindow was silently treated as a plain GROUP BY
		// field, running the query with no window and wrong grouping — no error.)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown window function")
	})

	t.Run("EmitSync without Execute", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		_, err := ssql.EmitSync(map[string]any{"id": 1})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "stream not initialized")
	})

	t.Run("EmitSync with aggregation query", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT COUNT(*) FROM stream")
		require.Nil(t, err)

		_, err = ssql.EmitSync(map[string]any{"id": 1})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "synchronous mode only supports non-aggregation queries")
	})

	t.Run("Emit without Execute", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		// This shouldn't trigger panic, but it won't have any effect either
		ssql.Emit(map[string]any{"id": 1})
	})

	t.Run("Stop without Execute", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		// This should not trigger panic
		ssql.Stop()
	})

	t.Run("GetStats without Execute", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		stats := ssql.GetStats()
		require.NotNil(t, stats)
		require.Equal(t, 0, len(stats))
	})

	t.Run("GetDetailedStats without Execute", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		stats := ssql.GetDetailedStats()
		require.NotNil(t, stats)
		require.Equal(t, 0, len(stats))
	})

	t.Run("ToChannel without Execute", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		ch := ssql.ToChannel()
		require.Nil(t, ch)
	})

	t.Run("Stream without Execute", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		stream := ssql.Stream()
		require.Nil(t, stream)
	})

	t.Run("IsAggregationQuery without Execute", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		isAgg := ssql.IsAggregationQuery()
		require.False(t, isAgg)
	})

	t.Run("AddSink without Execute", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		// This should not trigger panic
		ssql.AddSink(func(results []map[string]any) {})
	})

	t.Run("PrintTable without Execute", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		// This should not trigger panic
		ssql.PrintTable()
	})
}

// TestStreamSQLEdgeCases tests boundary conditions and special cases
func TestStreamSQLEdgeCases(t *testing.T) {
	t.Parallel()
	t.Run("empty SQL string", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("")
		require.NotNil(t, err)
	})

	t.Run("whitespace only SQL", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("   \n\t   ")
		require.NotNil(t, err)
	})

	t.Run("SQL with comments", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("-- This is a comment\nSELECT id FROM stream")
		// Depending on the actual SQL parser behavior, this may succeed or fail
		// Here, we just make sure there is no panic
		_ = err
	})

	t.Run("very long SQL statement", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		longSQL := "SELECT "
		for i := 0; i < 1000; i++ {
			if i > 0 {
				longSQL += ", "
			}
			longSQL += "field" + string(rune('0'+i%10))
		}
		longSQL += " FROM stream"
		err := ssql.Execute(longSQL)
		// It should be able to handle long SQL statements
		_ = err
	})

	t.Run("multiple Execute calls", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err1 := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err1)

		// The second execute should fail because it has already been executed
		err2 := ssql.Execute("SELECT name FROM stream")
		require.Error(t, err2)
		require.Contains(t, err2.Error(), "Execute() has already been called")
	})

	t.Run("Execute after Stop", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err)

		ssql.Stop()

		// After stopping, execute should fail again because it has already been executed
		err = ssql.Execute("SELECT name FROM stream")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Execute() has already been called")
	})

	t.Run("concurrent Execute calls", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()

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

		// Wait for both goroutines to complete
		<-done
		<-done

		// Validation is only one success, one failure
		assert.Equal(t, int32(1), atomic.LoadInt32(&successCount))
		assert.Equal(t, int32(1), atomic.LoadInt32(&errorCount))

		// Make sure you end up with an effective stream
		require.NotNil(t, ssql.Stream())
	})
}

// TestStreamSQLNilHandling tests nil value processing
func TestStreamSQLNilHandling(t *testing.T) {
	t.Parallel()
	t.Run("emit nil map", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err)

		// Sending nil data should not be panicked
		ssql.Emit(nil)
		ssql.Stop()
	})

	t.Run("emit map with nil values", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT id, name FROM stream")
		require.Nil(t, err)

		// Send data containing the nil value
		ssql.Emit(map[string]any{
			"id":   1,
			"name": nil,
		})
		ssql.Stop()
	})

	t.Run("EmitSync with nil data", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err)

		// EmitSync with nil data
		_, err = ssql.EmitSync(nil)
		// Depending on the implementation, this may return errors or handle nil values
		_ = err
		ssql.Stop()
	})
}

// TestStreamSQLComplexQueries tests complex queries
func TestStreamSQLComplexQueries(t *testing.T) {
	t.Parallel()
	t.Run("query with multiple fields", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT id, name, value, timestamp FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]any{
			"id":        1,
			"name":      "test",
			"value":     100.5,
			"timestamp": time.Now(),
		})
		ssql.Stop()
	})

	t.Run("query with WHERE clause", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT id, value FROM stream WHERE value > 50")
		require.Nil(t, err)

		ssql.Emit(map[string]any{"id": 1, "value": 100})
		ssql.Emit(map[string]any{"id": 2, "value": 25})
		ssql.Stop()
	})

	t.Run("query with aggregation functions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT COUNT(*), SUM(value), AVG(value) FROM stream")
		require.Nil(t, err)

		for i := 0; i < 5; i++ {
			ssql.Emit(map[string]any{"id": i, "value": i * 10})
		}
		ssql.Stop()
	})

	t.Run("query with window functions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT id, value FROM stream GROUP BY TumblingWindow('5s')")
		// Depending on the actual implementation, this may succeed or fail
		_ = err
		if err == nil {
			ssql.Stop()
		}
	})
}

// TestStreamSQLDataTypes tests different data types
func TestStreamSQLDataTypes(t *testing.T) {
	t.Parallel()
	t.Run("string data types", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT name FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]any{"name": "test string"})
		ssql.Emit(map[string]any{"name": ""})
		ssql.Emit(map[string]any{"name": "unicode测试🚀"})
		ssql.Stop()
	})

	t.Run("numeric data types", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT value FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]any{"value": 42})
		ssql.Emit(map[string]any{"value": 3.14159})
		ssql.Emit(map[string]any{"value": int64(9223372036854775807)})
		ssql.Emit(map[string]any{"value": float32(1.23)})
		ssql.Stop()
	})

	t.Run("boolean data types", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT active FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]any{"active": true})
		ssql.Emit(map[string]any{"active": false})
		ssql.Stop()
	})

	t.Run("time data types", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT timestamp FROM stream")
		require.Nil(t, err)

		now := time.Now()
		ssql.Emit(map[string]any{"timestamp": now})
		ssql.Emit(map[string]any{"timestamp": now.Unix()})
		ssql.Stop()
	})

	t.Run("array and slice data types", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT data FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]any{"data": []int{1, 2, 3, 4, 5}})
		ssql.Emit(map[string]any{"data": []string{"a", "b", "c"}})
		ssql.Emit(map[string]any{"data": []any{1, "test", true}})
		ssql.Stop()
	})

	t.Run("map data types", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT metadata FROM stream")
		require.Nil(t, err)

		ssql.Emit(map[string]any{
			"metadata": map[string]any{
				"key1": "value1",
				"key2": 42,
				"key3": true,
			},
		})
		ssql.Stop()
	})
}

// TestStreamSQLStressTest Stress testing
func TestStreamSQLStressTest(t *testing.T) {
	t.Parallel()
	t.Run("high frequency emissions", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT id FROM stream")
		require.Nil(t, err)

		// Data is transmitted at high frequencies
		for i := 0; i < 1000; i++ {
			ssql.Emit(map[string]any{"id": i})
		}
		ssql.Stop()
	})

	t.Run("large data payloads", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
		err := ssql.Execute("SELECT data FROM stream")
		require.Nil(t, err)

		// Send large data loads
		largeString := make([]byte, 10*1024) // 10KB
		for i := range largeString {
			largeString[i] = byte('A' + (i % 26))
		}

		for i := 0; i < 10; i++ {
			ssql.Emit(map[string]any{"data": string(largeString)})
		}
		ssql.Stop()
	})

	t.Run("concurrent operations", func(t *testing.T) {
		ssql := streamsql.New()
		defer ssql.Stop()
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
					ssql.Emit(map[string]any{"id": workerID*1000 + j})
				}
			}(i)
		}

		wg.Wait()
		ssql.Stop()
	})
}
