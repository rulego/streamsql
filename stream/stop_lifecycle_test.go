package stream

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/require"
)

// TestStream_StopWaitsForInflightSink asserts that Stop joins the sink worker
// running an in-flight task: it must not return while a sink callback is still
// executing, and it must return promptly once that callback completes.
func TestStream_StopWaitsForInflightSink(t *testing.T) {
	config := types.Config{SimpleFields: []string{"name"}}
	stream, err := NewStream(config)
	require.NoError(t, err)

	sinkStarted := make(chan struct{})
	sinkProceed := make(chan struct{})
	stream.AddSink(func(results []map[string]interface{}) {
		close(sinkStarted)
		<-sinkProceed // block until the test releases the sink
	})

	stream.Start()
	stream.Emit(map[string]interface{}{"name": "x"})

	// Wait until a worker has picked up the task and entered the sink.
	select {
	case <-sinkStarted:
	case <-time.After(time.Second):
		t.Fatal("sink callback did not start")
	}

	stopDone := make(chan struct{})
	go func() {
		stream.Stop()
		close(stopDone)
	}()

	// Stop must stay blocked while the in-flight sink runs.
	select {
	case <-stopDone:
		t.Fatal("Stop returned before the in-flight sink finished; join not working")
	case <-time.After(100 * time.Millisecond):
		// expected: Stop is still joining
	}

	// Releasing the sink lets the worker exit, so Stop returns.
	close(sinkProceed)
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return after the in-flight sink finished")
	}
}

// TestStream_StopReturnsPromptly ensures the bounded join does not add latency
// for well-behaved sinks: Stop must return near-instantly, never hitting the
// grace ceiling.
func TestStream_StopReturnsPromptly(t *testing.T) {
	config := types.Config{SimpleFields: []string{"name"}}
	stream, err := NewStream(config)
	require.NoError(t, err)

	stream.AddSink(func(results []map[string]interface{}) {})
	stream.Start()
	for i := 0; i < 20; i++ {
		stream.Emit(map[string]interface{}{"name": "x"})
	}

	start := time.Now()
	stream.Stop()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Stop took %v with well-behaved sinks; expected near-instant join", d)
	}
}

// TestStream_StopIdempotentWithJoin ensures repeated Stop calls stay safe now
// that Stop joins goroutines.
func TestStream_StopIdempotentWithJoin(t *testing.T) {
	config := types.Config{SimpleFields: []string{"name"}}
	stream, err := NewStream(config)
	require.NoError(t, err)
	stream.AddSink(func(results []map[string]interface{}) {})
	stream.Start()
	stream.Emit(map[string]interface{}{"name": "x"})

	stream.Stop()
	stream.Stop() // second call must be a no-op, not panic or hang
	stream.Stop()
}
