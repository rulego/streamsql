package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSessionWindowReapsTriggeredSessionsPT 锁定修复：ProcessingTime 模式下会话过期后
// 保留在 triggeredSessions 等 allowedLateness 迟到数据；closeTime 过期后必须被清理，
// 否则高基数会话 + allowedLateness>0 会让 triggeredSessions 无界增长。
// 修复前 checkExpiredSessions（PT 路径）漏调 closeExpiredSessions（仅 ET 路径调）。
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

	// 等 会话过期(60ms) + 保留期过期(100ms) + ticker(timeout/2=30ms)多次检查 + 余量。
	time.Sleep(500 * time.Millisecond)

	sw.mu.Lock()
	n := len(sw.triggeredSessions)
	sw.mu.Unlock()
	assert.Equal(t, 0, n, "triggeredSessions should be reaped after closeTime (PT + allowedLateness>0)")
}
