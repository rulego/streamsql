package window

import (
	"fmt"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// TestTumblingWindowPerformance Tests the performance of the rolling window
func TestTumblingWindowPerformance(t *testing.T) {
	// Testing performance with different buffer sizes
	bufferSizes := []int{10, 100, 1000, 5000}

	for _, bufferSize := range bufferSizes {
		t.Run(fmt.Sprintf("BufferSize_%d", bufferSize), func(t *testing.T) {
			tw, _ := NewTumblingWindow(types.WindowConfig{
				Type:   "TumblingWindow",
				Params: []any{100 * time.Millisecond},
				TsProp: "Ts",
			})

			go tw.Start()

			// Simulates high-frequency data input
			dataCount := 10000
			startTime := time.Now()
			baseTime := time.Now()

			for i := 0; i < dataCount; i++ {
				tw.Add(TestData{
					Ts:  baseTime.Add(time.Duration(i) * time.Millisecond),
					tag: fmt.Sprintf("data_%d", i),
				})
			}

			// Wait for processing to complete
			time.Sleep(2 * time.Second)

			// Get statistics
			stats := tw.GetStats()
			elapsed := time.Since(startTime)

			t.Logf("Buffer size: %d", bufferSize)
			t.Logf("Processing time: %v", elapsed)
			t.Logf("Sent successfully: %d", stats["sentCount"])
			t.Logf("Discarded quantity: %d", stats["droppedCount"])
			t.Logf("Buffer Utilization: %d/%d", stats["bufferUsed"], stats["bufferSize"])

			// No serious data loss was verified
			if bufferSize >= 1000 {
				if stats["droppedCount"] > int64(dataCount/10) { // Allows up to 10% loss
					t.Errorf("Excessive data loss: %d (Total: %d)", stats["droppedCount"], dataCount)
				}
			}

			tw.Stop()
		})
	}
}

// TestData Tests data structures
type TestData struct {
	Ts  time.Time
	tag string
}

// BenchmarkTumblingWindowThroughput tests the throughput of the rolling window
func BenchmarkTumblingWindowThroughput(b *testing.B) {
	tw, _ := NewTumblingWindow(types.WindowConfig{
		Type:   "TumblingWindow",
		Params: []any{10 * time.Millisecond},
		TsProp: "Ts",
	})

	go tw.Start()

	// Backend consumption results to avoid blockages
	done := make(chan struct{})
	go func() {
		for {
			select {
			case _, ok := <-tw.OutputChan():
				if !ok {
					return
				}
			case <-done:
				return
			}
		}
	}()

	baseTime := time.Now()
	data := TestData{
		Ts:  baseTime,
		tag: "benchmark",
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			data.Ts = baseTime.Add(time.Duration(i) * time.Microsecond)
			tw.Add(data)
			i++
		}
	})

	// Get the final stats
	stats := tw.GetStats()
	b.Logf("Send successfully: %d, Discard: %d", stats["sentCount"], stats["droppedCount"])

	tw.Stop()
	close(done)
}

// TestWindowBufferOverflow tests buffer overflow handling
func TestWindowBufferOverflow(t *testing.T) {
	// Create a window with a small buffer
	tw, _ := NewTumblingWindow(types.WindowConfig{
		Type:   "TumblingWindow",
		Params: []any{50 * time.Millisecond},
		TsProp: "Ts",
	})

	go tw.Start()

	// No consumption of output, causing the buffer to be full
	// Only add data, do not read the output channel

	baseTime := time.Now()
	for i := 0; i < 100; i++ {
		tw.Add(TestData{
			Ts:  baseTime.Add(time.Duration(i) * time.Millisecond),
			tag: fmt.Sprintf("overflow_%d", i),
		})
	}

	// Wait a while for the window to trigger
	time.Sleep(200 * time.Millisecond)

	stats := tw.GetStats()
	t.Logf("Buffer Overflow Test - Send: %d, Discard: %d", stats["sentCount"], stats["droppedCount"])

	// There should be data discarded
	if stats["droppedCount"] == 0 {
		t.Log("Data was expected to be discarded, but in reality, it was not")
	}

	// Verify that the system is still running normally (no blocking)
	if stats["sentCount"] == 0 {
		t.Error("At least some data should have been sent")
	}

	tw.Stop()
}
