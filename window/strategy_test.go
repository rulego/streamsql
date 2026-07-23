package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOverflowStrategies tests different buffer overflow strategies
func TestOverflowStrategies(t *testing.T) {
	t.Run("CountingWindow_StrategyBlock_Timeout", func(t *testing.T) {
		// Configuration: window size 1 (triggered once per data entry), output buffer 1, blocking policy, timeout 100ms
		config := types.WindowConfig{
			Type:   "CountingWindow",
			Params: []any{1}, // Threshold = 1
			PerformanceConfig: types.PerformanceConfig{
				BufferConfig: types.BufferConfig{
					WindowOutputSize: 1,
				},
				OverflowConfig: types.OverflowConfig{
					Strategy:      types.OverflowStrategyBlock,
					BlockTimeout:  100 * time.Millisecond,
					AllowDataLoss: true, // Allow discard statistics
				},
			},
		}

		win, err := NewCountingWindow(config)
		require.NoError(t, err)
		win.Start()
		defer win.Stop()

		// 1. Send the first data entry, trigger the window, and fill outputChan (capacity 1)
		win.Add(map[string]any{"id": 1})

		// Waiting for processing
		time.Sleep(50 * time.Millisecond)
		stats := win.GetStats()
		assert.Equal(t, int64(1), stats["sentCount"])
		assert.Equal(t, int64(1), stats["bufferUsed"]) // It should still be in the buffer zone because no one is reading it

		// 2. Send the second data entry to trigger the window
		// At this point, outputChan is full, and sendResult should block 100ms before being discarded after timeout
		win.Add(map[string]any{"id": 2})

		// Wait timeout (100ms) + processing time
		time.Sleep(200 * time.Millisecond)

		stats = win.GetStats()
		// Article 1 is still in the buffer zone (because no one reads it)
		// Article 2 is discarded due to blocked timeout
		assert.Equal(t, int64(1), stats["bufferUsed"])
		assert.Equal(t, int64(1), stats["droppedCount"])

		// 3. Read data from the buffer to free up space
		select {
		case <-win.OutputChan():
			// Read out the first item
		default:
			t.Fatal("expected data in output channel")
		}

		// 4. Send data for item 3
		win.Add(map[string]any{"id": 3})
		time.Sleep(50 * time.Millisecond)

		stats = win.GetStats()
		assert.Equal(t, int64(2), stats["sentCount"])    // Items 1 and 3 were successfully sent
		assert.Equal(t, int64(1), stats["droppedCount"]) // Article 2: Discard
	})

	t.Run("SessionWindow_StrategyBlock_Timeout", func(t *testing.T) {
		// Configuration: Session timeout 50ms, output buffer 1, blocking policy, timeout 50ms
		config := types.WindowConfig{
			Type:   "SessionWindow",
			Params: []any{"50ms"},
			PerformanceConfig: types.PerformanceConfig{
				BufferConfig: types.BufferConfig{
					WindowOutputSize: 1,
				},
				OverflowConfig: types.OverflowConfig{
					Strategy:      types.OverflowStrategyBlock,
					BlockTimeout:  50 * time.Millisecond,
					AllowDataLoss: true,
				},
			},
		}

		win, err := NewSessionWindow(config)
		require.NoError(t, err)
		win.Start()
		defer win.Stop()

		// 1. Send data and start a session
		win.Add(map[string]any{"id": 1})

		// 2. Wait for session timeout (50ms) + check cycle (timeout/2 = 25ms)
		// Ensure the session is triggered and sent to outputChan
		time.Sleep(100 * time.Millisecond)

		stats := win.GetStats()
		assert.Equal(t, int64(1), stats["sentCount"])
		assert.Equal(t, int64(1), stats["bufferUsed"])

		// 3. Send data to start the second session (because the previous session has already ended)
		win.Add(map[string]any{"id": 2})

		// 4. Wait for session timeout
		// At this point, outputChan is full, so it should be blocked and discarded
		time.Sleep(150 * time.Millisecond)

		stats = win.GetStats()
		assert.Equal(t, int64(1), stats["bufferUsed"])
		assert.Equal(t, int64(1), stats["droppedCount"])
	})

	t.Run("CountingWindow_StrategyDrop", func(t *testing.T) {
		// Configuration: window size 1, output buffer 1, discard policy
		config := types.WindowConfig{
			Type:   "CountingWindow",
			Params: []any{1},
			PerformanceConfig: types.PerformanceConfig{
				BufferConfig: types.BufferConfig{
					WindowOutputSize: 1,
				},
				OverflowConfig: types.OverflowConfig{
					Strategy: types.OverflowStrategyDrop,
				},
			},
		}

		win, err := NewCountingWindow(config)
		require.NoError(t, err)
		win.Start()
		defer win.Stop()

		// 1. Send the first data entry and fill in outputChan
		win.Add(map[string]any{"id": 1})
		time.Sleep(50 * time.Millisecond)

		// 2. Send the second data
		// outputChan is full, and StrategyDrop will try to discard the old data (outputChan header) to add new data
		win.Add(map[string]any{"id": 2})
		time.Sleep(50 * time.Millisecond)

		stats := win.GetStats()
		assert.Equal(t, int64(2), stats["sentCount"])

		// Verify that the buffer now contains data number 2
		select {
		case data := <-win.OutputChan():
			assert.Len(t, data, 1)
			assert.Equal(t, 2, cast.ToInt(data[0].Data.(map[string]any)["id"]))
		default:
			t.Fatal("expected data in output channel")
		}
	})

	t.Run("TumblingWindow_StrategyBlock_Timeout", func(t *testing.T) {
		// Configuration: window size 50ms, output buffer 1, blocking policy, timeout 50ms
		config := types.WindowConfig{
			Type:   "TumblingWindow",
			Params: []any{"50ms"},
			PerformanceConfig: types.PerformanceConfig{
				BufferConfig: types.BufferConfig{
					WindowOutputSize: 1,
				},
				OverflowConfig: types.OverflowConfig{
					Strategy:      types.OverflowStrategyBlock,
					BlockTimeout:  50 * time.Millisecond,
					AllowDataLoss: true,
				},
			},
		}

		win, err := NewTumblingWindow(config)
		require.NoError(t, err)
		win.Start()
		defer win.Stop()

		// 1. Send data to trigger the first window
		win.Add(map[string]any{"id": 1})
		// Wait for window to trigger (50ms)
		time.Sleep(100 * time.Millisecond)

		stats := win.GetStats()
		assert.Equal(t, int64(1), stats["sentCount"])
		assert.Equal(t, int64(1), stats["bufferUsed"])

		// 2. Send data to trigger the second window
		// Since outputChan was not read, the second window should be blocked and then timed out when triggered
		win.Add(map[string]any{"id": 2})
		// Wait window trigger (50ms) + block timeout (50ms)
		time.Sleep(150 * time.Millisecond)

		stats = win.GetStats()
		assert.Equal(t, int64(1), stats["bufferUsed"])   // Still only data from window 1
		assert.Equal(t, int64(1), stats["droppedCount"]) // The second window result is discarded
	})
}
