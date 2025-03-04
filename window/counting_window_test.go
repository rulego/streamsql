package window

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCountingWindow(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test case 1: Normal operation
	cw := NewCountingWindow(3, func(results []interface{}) {
		t.Logf("Received results: %v", results)
	})
	go cw.Start()

	for i := 0; i < 3; i++ {
		cw.Add(i)
		time.Sleep(100 * time.Millisecond)
	}

	// Trigger one more element to check threshold
	cw.Add(3)

	results := make(chan []interface{})
	go func() {
		for res := range cw.OutputChan() {
			results <- res
		}
	}()

	select {
	case res := <-results:
		assert.Len(t, res, 3)
		assert.Contains(t, res, 0)
		assert.Contains(t, res, 1)
		assert.Contains(t, res, 2)
	case <-time.After(2 * time.Second):
		t.Error("No results received within timeout")
	}

	// Test case 2: Reset
	cw.Reset()
	assert.Len(t, cw.dataBuffer, 0)
}

func TestCountingWindowBadThreshold(t *testing.T) {
	_, err := CreateWindow("counting", map[string]interface{}{
		"count": 0,
	})
	require.Error(t, err)
}
