package window

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSlidingWindow(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	sw := NewSlidingWindow(2*time.Second, 1*time.Second)
	sw.SetCallback(func(results []interface{}) {
		t.Logf("Received results: %v", results)
	})
	sw.Start()

	// 添加数据
	now := time.Now()
	sw.Add(now.Add(-3 * time.Second))
	sw.Add(now.Add(-2 * time.Second))
	sw.Add(now.Add(-1 * time.Second))
	sw.Add(now)

	// 等待一段时间，触发窗口
	time.Sleep(3 * time.Second)

	// 检查结果
	resultsChan := sw.OutputChan()
	var results []interface{}
	select {
	case results = <-resultsChan:
	case <-time.After(1 * time.Second):
		t.Fatal("No results received within timeout")
	}

	// 预期结果：保留最近 2 秒内的数据
	assert.Len(t, results, 2)
	assert.Contains(t, results, now.Add(-1*time.Second))
	assert.Contains(t, results, now)
}
