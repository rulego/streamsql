package window

import (
	"fmt"
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// TestTumblingWindowPerformance 测试滚动窗口的性能
func TestTumblingWindowPerformance(t *testing.T) {
	// 测试不同缓冲区大小的性能
	bufferSizes := []int{10, 100, 1000, 5000}

	for _, bufferSize := range bufferSizes {
		t.Run(fmt.Sprintf("BufferSize_%d", bufferSize), func(t *testing.T) {
			tw, _ := NewTumblingWindow(types.WindowConfig{
				Type: "TumblingWindow",
				Params: map[string]interface{}{
					"size":             "100ms",
					"outputBufferSize": bufferSize,
				},
				TsProp: "Ts",
			})

			go tw.Start()

			// 模拟高频数据输入
			dataCount := 10000
			startTime := time.Now()
			baseTime := time.Now()

			for i := 0; i < dataCount; i++ {
				tw.Add(TestData{
					Ts:  baseTime.Add(time.Duration(i) * time.Millisecond),
					tag: fmt.Sprintf("data_%d", i),
				})
			}

			// 等待处理完成
			time.Sleep(2 * time.Second)

			// 获取统计信息
			stats := tw.GetStats()
			elapsed := time.Since(startTime)

			t.Logf("缓冲区大小: %d", bufferSize)
			t.Logf("处理时间: %v", elapsed)
			t.Logf("发送成功: %d", stats["sent_count"])
			t.Logf("丢弃数量: %d", stats["dropped_count"])
			t.Logf("缓冲区利用率: %d/%d", stats["buffer_used"], stats["buffer_size"])

			// 验证没有严重的数据丢失
			if bufferSize >= 1000 {
				if stats["dropped_count"] > int64(dataCount/10) { // 允许最多10%的丢失
					t.Errorf("丢失数据过多: %d (总数: %d)", stats["dropped_count"], dataCount)
				}
			}

			tw.Stop()
		})
	}
}

// TestData 测试数据结构
type TestData struct {
	Ts  time.Time
	tag string
}

// BenchmarkTumblingWindowThroughput 测试滚动窗口的吞吐量
func BenchmarkTumblingWindowThroughput(b *testing.B) {
	tw, _ := NewTumblingWindow(types.WindowConfig{
		Type: "TumblingWindow",
		Params: map[string]interface{}{
			"size":             "10ms",
			"outputBufferSize": 5000,
		},
		TsProp: "Ts",
	})

	go tw.Start()

	// 在后台消费结果，避免阻塞
	go func() {
		for range tw.OutputChan() {
			// 消费结果
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

	// 获取最终统计
	stats := tw.GetStats()
	b.Logf("发送成功: %d, 丢弃: %d", stats["sent_count"], stats["dropped_count"])

	tw.Stop()
}

// TestWindowBufferOverflow 测试缓冲区溢出处理
func TestWindowBufferOverflow(t *testing.T) {
	// 创建一个小缓冲区的窗口
	tw, _ := NewTumblingWindow(types.WindowConfig{
		Type: "TumblingWindow",
		Params: map[string]interface{}{
			"size":             "50ms",
			"outputBufferSize": 5, // 很小的缓冲区
		},
		TsProp: "Ts",
	})

	go tw.Start()

	// 不消费输出，导致缓冲区满
	// 只添加数据，不读取输出通道

	baseTime := time.Now()
	for i := 0; i < 100; i++ {
		tw.Add(TestData{
			Ts:  baseTime.Add(time.Duration(i) * time.Millisecond),
			tag: fmt.Sprintf("overflow_%d", i),
		})
	}

	// 等待一段时间让窗口触发
	time.Sleep(200 * time.Millisecond)

	stats := tw.GetStats()
	t.Logf("缓冲区溢出测试 - 发送: %d, 丢弃: %d", stats["sent_count"], stats["dropped_count"])

	// 应该有数据被丢弃
	if stats["dropped_count"] == 0 {
		t.Log("预期会有数据丢弃，但实际没有丢弃")
	}

	// 验证系统仍然运行正常（没有阻塞）
	if stats["sent_count"] == 0 {
		t.Error("应该至少发送了一些数据")
	}

	tw.Stop()
}
