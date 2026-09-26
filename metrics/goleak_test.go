package metrics

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain 在全部测试结束后做 goroutine 泄漏检测。
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
