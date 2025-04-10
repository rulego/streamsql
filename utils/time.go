package timex

import (
	"time"
)

// AlignTimeToWindow 将时间对齐到窗口的起始时间。
func AlignTimeToWindow(t time.Time, size time.Duration) time.Time {
	offset := t.UnixNano() % int64(size)
	return t.Add(time.Duration(-offset))
}

// AlignTime 将时间对齐到指定的时间单位。 roundUp 为 true 时向上截断，为 false 时向下截断。
func AlignTime(t time.Time, timeUnit time.Duration, roundUp bool) time.Time {
	trunc := t.Truncate(timeUnit)
	if !roundUp {
		return trunc.Add(timeUnit)
	}
	return trunc
}
