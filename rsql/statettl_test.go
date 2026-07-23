package rsql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseStateTTL: STATETTL='24h' Resolves to WindowConfig.CountStateTTL.
func TestParseStateTTL(t *testing.T) {
	config, _, err := Parse("SELECT deviceId, COUNT(*) FROM stream GROUP BY deviceId, CountingWindow(10) WITH(STATETTL='24h')")
	require.NoError(t, err)
	require.NotNil(t, config)
	assert.Equal(t, 24*time.Hour, config.WindowConfig.CountStateTTL)
}

// TestParseStateTTL_DefaultZero: Defaults to 0 (disabled) when STATETTL is absent.
func TestParseStateTTL_DefaultZero(t *testing.T) {
	config, _, err := Parse("SELECT deviceId, COUNT(*) FROM stream GROUP BY deviceId, CountingWindow(10)")
	require.NoError(t, err)
	require.NotNil(t, config)
	assert.Equal(t, time.Duration(0), config.WindowConfig.CountStateTTL)
}
