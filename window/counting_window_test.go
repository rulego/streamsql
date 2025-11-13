package window

import (
	"context"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestCountingWindow(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test case 1: Normal operation
	cw, _ := NewCountingWindow(types.WindowConfig{
		Params: []interface{}{3},
		Callback: func(results []types.Row) {
				t.Logf("Received results: %v", results)
		},
	})
	go cw.Start()

	for i := 0; i < 3; i++ {
		cw.Add(i)
		time.Sleep(100 * time.Millisecond)
	}

	// Trigger one more element to check threshold
	cw.Add(3)

	resultsChan := cw.OutputChan()
	//results := make(chan []types.Row)
	// go func() {
	// 	for res := range cw.OutputChan() {
	// 		results <- res
	// 	}
	// }()

	select {
	case res := <-resultsChan:
		assert.Len(t, res, 3)
		assert.Equal(t, 0, res[0].Data, "First element should be 0")
		assert.Equal(t, 1, res[1].Data, "Second element should be 1")
		assert.Equal(t, 2, res[2].Data, "Third element should be 2")
	case <-time.After(2 * time.Second):
		t.Error("No results received within timeout")
	}
	// Verify window state: After adding 4th data, first window has triggered, remaining 1 data (value 3)
	// Continue adding 2 more data, should trigger again
	cw.Add(4) // Add 5th data
	cw.Add(5) // Add 6th data, should trigger again (3,4,5)

	// Wait for second trigger
	select {
	case res := <-resultsChan:
		assert.Len(t, res, 3)
		assert.Equal(t, 3, res[0].Data, "First element of second batch should be 3")
		assert.Equal(t, 4, res[1].Data, "Second element of second batch should be 4")
		assert.Equal(t, 5, res[2].Data, "Third element of second batch should be 5")
	case <-time.After(2 * time.Second):
		t.Error("No second results received within timeout")
	}

	// Test case 2: Reset
	cw.Reset()
	// Add data after reset to verify reset was successful
	cw.Add(100)
	cw.Add(101)
	cw.Add(102)
	select {
	case res := <-resultsChan:
		assert.Len(t, res, 3)
		assert.Equal(t, 100, res[0].Data, "First element after reset should be 100")
	case <-time.After(2 * time.Second):
		t.Error("No results after reset received within timeout")
	}
}

func TestCountingWindowBadThreshold(t *testing.T) {
	_, err := CreateWindow(types.WindowConfig{
		Type: "counting",
		Params: []interface{}{0},
	})
	require.Error(t, err)
}
