package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCountingWindowStateTTL_ReapsIdleKeys: TTL>0, dead keys that are not fully saved and exceed TTL are cleared.
// Use large TTL to avoid background ticker interference; directly call reapIdleKeys to precisely verify the cleaning logic.
func TestCountingWindowStateTTL_ReapsIdleKeys(t *testing.T) {
	config := types.WindowConfig{
		Type:          TypeCounting,
		Params:        []any{10},
		GroupByKeys:   []string{"deviceId"},
		CountStateTTL: time.Minute,
	}
	cw, err := NewCountingWindow(config)
	require.NoError(t, err)
	cw.Start()
	defer cw.Stop()

	cw.Add(map[string]any{"deviceId": "d1", "v": 1})
	cw.Add(map[string]any{"deviceId": "d2", "v": 1})
	time.Sleep(80 * time.Millisecond) // let rows enter the Start goroutine

	cw.mu.Lock()
	require.Len(t, cw.keyedBuffer, 2, "two keys buffered before reap")
	d1Last := cw.lastActive["d1"]
	cw.mu.Unlock()
	require.False(t, d1Last.IsZero(), "lastActive recorded for d1")

	cw.reapIdleKeys(d1Last.Add(2 * time.Minute)) // now well past TTL

	cw.mu.Lock()
	assert.Empty(t, cw.keyedBuffer, "idle keys reaped")
	assert.Empty(t, cw.lastActive, "lastActive cleared")
	cw.mu.Unlock()
}

// TestCountingWindowStateTTL_KeepsActiveKeys: TTL>0, active keys within TTL are not mistakenly cleared.
func TestCountingWindowStateTTL_KeepsActiveKeys(t *testing.T) {
	config := types.WindowConfig{
		Type:          TypeCounting,
		Params:        []any{10},
		GroupByKeys:   []string{"deviceId"},
		CountStateTTL: time.Minute,
	}
	cw, err := NewCountingWindow(config)
	require.NoError(t, err)
	cw.Start()
	defer cw.Stop()

	cw.Add(map[string]any{"deviceId": "d1", "v": 1})
	time.Sleep(80 * time.Millisecond)

	cw.mu.Lock()
	d1Last := cw.lastActive["d1"]
	cw.mu.Unlock()

	cw.reapIdleKeys(d1Last.Add(10 * time.Second)) // 10s < 1min TTL: still active

	cw.mu.Lock()
	assert.Len(t, cw.keyedBuffer, 1, "active key kept")
	assert.Contains(t, cw.lastActive, "d1")
	cw.mu.Unlock()
}

// TestCountingWindowStateTTL_DisabledByDefault: TTL=0 (default) does not start cleanup, but keyedBuffer accumulates normally.
func TestCountingWindowStateTTL_DisabledByDefault(t *testing.T) {
	config := types.WindowConfig{
		Type:        TypeCounting,
		Params:      []any{10},
		GroupByKeys: []string{"deviceId"},
	}
	cw, err := NewCountingWindow(config)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), cw.countStateTTL, "default TTL is 0 (disabled)")

	cw.Start()
	defer cw.Stop()

	cw.Add(map[string]any{"deviceId": "d1", "v": 1})
	cw.Add(map[string]any{"deviceId": "d2", "v": 1})
	time.Sleep(200 * time.Millisecond)

	cw.mu.Lock()
	assert.Len(t, cw.keyedBuffer, 2, "no auto-reap with TTL=0 (default)")
	cw.mu.Unlock()
}
