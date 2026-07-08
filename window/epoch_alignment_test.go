package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
)

// TestPTWindowEpochAlignment verifies processing-time windows align to epoch
// boundaries (D1): a window's start lands on a size/slide boundary regardless of
// when the first data arrived. Matches Flink TumblingProcessingTimeWindows
// (getWindowStartWithOffset) and eKuiper ("align to the nature time ...
// regardless of the rule start time").
func TestPTWindowEpochAlignment(t *testing.T) {
	// First-data time with a sub-second offset that is NOT on any boundary.
	odd := time.Date(2025, 4, 7, 16, 46, 57, 789000000, time.UTC) // xx:57.789

	t.Run("tumbling aligns to size", func(t *testing.T) {
		tw, _ := NewTumblingWindow(types.WindowConfig{
			Params: []any{2 * time.Second},
			TsProp: "Ts",
		})
		defer tw.Stop()
		tw.Add(TestData{Ts: odd, tag: "x"})

		want := odd.Truncate(2 * time.Second) // xx:56.000
		assert.Equal(t, want, *tw.currentSlot.Start, "PT tumbling start must align to 2s epoch boundary")
		assert.Equal(t, want.Add(2*time.Second), *tw.currentSlot.End)
	})

	t.Run("sliding aligns to slide", func(t *testing.T) {
		sw, _ := NewSlidingWindow(types.WindowConfig{
			Params: []any{2 * time.Second, time.Second}, // size=2s, slide=1s
			TsProp: "Ts",
		})
		defer sw.Stop()
		sw.Add(TestData{Ts: odd, tag: "x"})

		want := odd.Truncate(time.Second) // xx:57.000
		assert.Equal(t, want, *sw.currentSlot.Start, "PT sliding start must align to 1s slide boundary")
		assert.Equal(t, want.Add(2*time.Second), *sw.currentSlot.End) // slot spans size
	})
}
