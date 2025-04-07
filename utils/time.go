package timex

import "time"

func AlignTimeToWindow(t time.Time, size time.Duration) time.Time {
	offset := t.UnixNano() % int64(size)
	return t.Add(time.Duration(-offset))
}
