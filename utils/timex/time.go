package timex

import (
	"time"
)

// AlignTimeToWindow aligns time to window start time
func AlignTimeToWindow(t time.Time, size time.Duration) time.Time {
	// Handle zero time
	if t.IsZero() {
		return t
	}
	offset := t.UnixNano() % int64(size)
	return t.Add(time.Duration(-offset))
}

// AlignTime aligns time to specified time unit. When roundUp is true, rounds up; when false, rounds down
func AlignTime(t time.Time, timeUnit time.Duration, roundUp bool) time.Time {
	trunc := t.Truncate(timeUnit)
	if roundUp && !t.Equal(trunc) {
		return trunc.Add(timeUnit)
	}
	return trunc
}
