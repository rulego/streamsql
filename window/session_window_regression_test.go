package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSessionWindowReapsTriggeredSessionsPT lock fix: After a session expires in ProcessingTime mode
// Retention of delayed data for allowedLateness such as triggeredSessions; closeTime must be cleaned up after expiration,
// Otherwise, high base sessions + allowedLateness>0 will cause triggeredSessions to grow unbounded.
// Fixed missed call on checkExpiredSessions (PT path) before closeExpiredSessions (only on ET path).
func TestSessionWindowReapsTriggeredSessionsPT(t *testing.T) {
	config := types.WindowConfig{
		Type:            TypeSession,
		Params:          []any{60 * time.Millisecond},
		AllowedLateness: 100 * time.Millisecond,
	}
	sw, err := NewSessionWindow(config)
	require.NoError(t, err)
	sw.Start()
	defer sw.Stop()

	sw.Add(map[string]any{"user_id": "u1", "value": 1})

	// Etc. Session expired (60ms) + Retention expired (100ms) + ticker (timeout/2=30ms) Multiple checks + remaining balance.
	time.Sleep(500 * time.Millisecond)

	sw.mu.Lock()
	n := len(sw.triggeredSessions)
	sw.mu.Unlock()
	assert.Equal(t, 0, n, "triggeredSessions should be reaped after closeTime (PT + allowedLateness>0)")
}
