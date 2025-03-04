package window

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestTumblingWindow(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	tw := NewTumblingWindow(2 * time.Second)
	tw.SetCallback(func(results []interface{}) {
		// Process results
	})
	go tw.Start()

	// Add data every 500ms
	for i := 0; i < 5; i++ {
		tw.Add(i)
		time.Sleep(1100 * time.Millisecond)
	}

	// Check output channel
	resultsChan := tw.OutputChan()
	var results []interface{}
	select {
	case results = <-resultsChan:
	case <-time.After(3 * time.Second):
		t.Fatal("No results received within timeout")
	}

	// Verify that data is sent every 2 seconds
	require.Len(t, results, 2)
	require.Equal(t, []interface{}{0, 1}, results)

	// Verify next batch
	select {
	case results = <-resultsChan:
		require.Len(t, results, 2)
		require.Equal(t, []interface{}{2, 3}, results)
	case <-time.After(3 * time.Second):
		t.Fatal("No results received within timeout")
	}

	//time.Sleep(1100 * time.Millisecond)
	//results = <-resultsChan

	// Verify reset and final batch
	tw.Reset()
	tw.Add(99)
	cancel()

	select {
	case results = <-resultsChan:
		require.Len(t, results, 1)
		require.Equal(t, []interface{}{99}, results)
	case <-time.After(3 * time.Second):
		t.Fatal("No results received within timeout")
	}
}
