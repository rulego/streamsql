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
		Params: map[string]interface{}{
			"count": 3,
			"callback": func(results []interface{}) {
				t.Logf("Received results: %v", results)
			},
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
		assert.Equal(t, 0, res[0].Data, "第一个元素应该是0")
		assert.Equal(t, 1, res[1].Data, "第二个元素应该是1")
		assert.Equal(t, 2, res[2].Data, "第三个元素应该是2")
	case <-time.After(2 * time.Second):
		t.Error("No results received within timeout")
	}
	// 验证窗口状态：添加第4个数据后，第一个窗口已触发，剩余1个数据(值为3)
	// 继续添加2个数据，应该再次触发
	cw.Add(4) // 添加第5个数据
	cw.Add(5) // 添加第6个数据，应该再次触发(3,4,5)
	
	// 等待第二次触发
	select {
	case res := <-resultsChan:
		assert.Len(t, res, 3)
		assert.Equal(t, 3, res[0].Data, "第二批第一个元素应该是3")
		assert.Equal(t, 4, res[1].Data, "第二批第二个元素应该是4")
		assert.Equal(t, 5, res[2].Data, "第二批第三个元素应该是5")
	case <-time.After(2 * time.Second):
		t.Error("No second results received within timeout")
	}
	
	// Test case 2: Reset
	cw.Reset()
	// Reset后添加数据验证重置是否成功
	cw.Add(100)
	cw.Add(101)
	cw.Add(102)
	select {
	case res := <-resultsChan:
		assert.Len(t, res, 3)
		assert.Equal(t, 100, res[0].Data, "重置后第一个元素应该是100")
	case <-time.After(2 * time.Second):
		t.Error("No results after reset received within timeout")
	}
}

func TestCountingWindowBadThreshold(t *testing.T) {
	_, err := CreateWindow(types.WindowConfig{
		Type: "counting",
		Params: map[string]interface{}{
			"count": 0,
		},
	})
	require.Error(t, err)
}
