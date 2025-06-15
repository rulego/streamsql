package window

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/types"
)

// TestTumblingWindowUnifiedConfig 测试滚动窗口的统一配置
func TestTumblingWindowUnifiedConfig(t *testing.T) {
	tests := []struct {
		name               string
		performanceConfig  types.PerformanceConfig
		expectedBufferSize int
	}{
		{
			name:               "默认配置",
			performanceConfig:  types.DefaultPerformanceConfig(),
			expectedBufferSize: 1000,
		},
		{
			name:               "高性能配置",
			performanceConfig:  types.HighPerformanceConfig(),
			expectedBufferSize: 5000,
		},
		{
			name:               "低延迟配置",
			performanceConfig:  types.LowLatencyConfig(),
			expectedBufferSize: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windowConfig := types.WindowConfig{
				Type: TypeTumbling,
				Params: map[string]interface{}{
					"size":              "2s",
					"performanceConfig": tt.performanceConfig,
				},
			}

			tw, err := NewTumblingWindow(windowConfig)
			if err != nil {
				t.Fatalf("创建滚动窗口失败: %v", err)
			}

			actualBufferSize := cap(tw.outputChan)
			if actualBufferSize != tt.expectedBufferSize {
				t.Errorf("期望缓冲区大小 %d，实际得到 %d", tt.expectedBufferSize, actualBufferSize)
			}

			tw.Stop()
		})
	}
}

// TestSlidingWindowUnifiedConfig 测试滑动窗口的统一配置
func TestSlidingWindowUnifiedConfig(t *testing.T) {
	performanceConfig := types.HighPerformanceConfig()

	windowConfig := types.WindowConfig{
		Type: TypeSliding,
		Params: map[string]interface{}{
			"size":              "10s",
			"slide":             "5s",
			"performanceConfig": performanceConfig,
		},
	}

	sw, err := NewSlidingWindow(windowConfig)
	if err != nil {
		t.Fatalf("创建滑动窗口失败: %v", err)
	}

	expectedBufferSize := 5000 // 高性能配置
	actualBufferSize := cap(sw.outputChan)
	if actualBufferSize != expectedBufferSize {
		t.Errorf("期望缓冲区大小 %d，实际得到 %d", expectedBufferSize, actualBufferSize)
	}

	sw.Stop()
}

// TestCountingWindowUnifiedConfig 测试计数窗口的统一配置
func TestCountingWindowUnifiedConfig(t *testing.T) {
	performanceConfig := types.HighPerformanceConfig()

	windowConfig := types.WindowConfig{
		Type: TypeCounting,
		Params: map[string]interface{}{
			"count":             10,
			"performanceConfig": performanceConfig,
		},
	}

	cw, err := NewCountingWindow(windowConfig)
	if err != nil {
		t.Fatalf("创建计数窗口失败: %v", err)
	}

	expectedBufferSize := 500 // 5000 / 10
	actualBufferSize := cap(cw.outputChan)
	if actualBufferSize != expectedBufferSize {
		t.Errorf("期望缓冲区大小 %d，实际得到 %d", expectedBufferSize, actualBufferSize)
	}
}

// TestSessionWindowUnifiedConfig 测试会话窗口的统一配置
func TestSessionWindowUnifiedConfig(t *testing.T) {
	performanceConfig := types.ZeroDataLossConfig()

	windowConfig := types.WindowConfig{
		Type: TypeSession,
		Params: map[string]interface{}{
			"timeout":           "30s",
			"performanceConfig": performanceConfig,
		},
	}

	sw, err := NewSessionWindow(windowConfig)
	if err != nil {
		t.Fatalf("创建会话窗口失败: %v", err)
	}

	expectedBufferSize := 200 // 2000 / 10
	actualBufferSize := cap(sw.outputChan)
	if actualBufferSize != expectedBufferSize {
		t.Errorf("期望缓冲区大小 %d，实际得到 %d", expectedBufferSize, actualBufferSize)
	}

	sw.Stop()
}

// TestWindowWithoutPerformanceConfig 测试没有性能配置时的默认行为
func TestWindowWithoutPerformanceConfig(t *testing.T) {
	windowConfig := types.WindowConfig{
		Type: TypeTumbling,
		Params: map[string]interface{}{
			"size": "3s",
			// 不添加 performanceConfig
		},
	}

	tw, err := NewTumblingWindow(windowConfig)
	if err != nil {
		t.Fatalf("创建窗口失败: %v", err)
	}

	expectedBufferSize := 1000 // 默认值
	actualBufferSize := cap(tw.outputChan)
	if actualBufferSize != expectedBufferSize {
		t.Errorf("期望默认缓冲区大小 %d，实际得到 %d", expectedBufferSize, actualBufferSize)
	}

	tw.Stop()
}

// TestWindowFactoryWithUnifiedConfig 测试窗口工厂与统一配置的集成
func TestWindowFactoryWithUnifiedConfig(t *testing.T) {
	performanceConfig := types.PerformanceConfig{
		BufferConfig: types.BufferConfig{
			WindowOutputSize: 1500,
		},
	}

	// 测试滚动窗口
	windowConfig := types.WindowConfig{
		Type: TypeTumbling,
		Params: map[string]interface{}{
			"size":              "5s",
			"performanceConfig": performanceConfig,
		},
	}

	window, err := CreateWindow(windowConfig)
	if err != nil {
		t.Fatalf("创建窗口失败: %v", err)
	}

	tw, ok := window.(*TumblingWindow)
	if !ok {
		t.Fatalf("期望得到TumblingWindow，实际得到 %T", window)
	}

	expectedBufferSize := 1500
	actualBufferSize := cap(tw.outputChan)
	if actualBufferSize != expectedBufferSize {
		t.Errorf("期望缓冲区大小 %d，实际得到 %d", expectedBufferSize, actualBufferSize)
	}

	tw.Stop()
}

// TestWindowUnifiedConfigIntegration 集成测试：验证窗口配置与实际数据处理的集成
func TestWindowUnifiedConfigIntegration(t *testing.T) {
	performanceConfig := types.HighPerformanceConfig()

	windowConfig := types.WindowConfig{
		Type: TypeTumbling,
		Params: map[string]interface{}{
			"size":              "1s",
			"performanceConfig": performanceConfig,
		},
	}

	tw, err := NewTumblingWindow(windowConfig)
	if err != nil {
		t.Fatalf("创建窗口失败: %v", err)
	}
	defer tw.Stop()

	// 验证缓冲区大小
	expectedBufferSize := 5000 // 高性能配置的WindowOutputSize
	actualBufferSize := cap(tw.outputChan)
	if actualBufferSize != expectedBufferSize {
		t.Errorf("期望缓冲区大小 %d，实际得到 %d", expectedBufferSize, actualBufferSize)
	}

	// 启动窗口
	tw.Start()

	// 发送一些测试数据
	for i := 0; i < 10; i++ {
		tw.Add(map[string]interface{}{
			"id":    i,
			"value": i * 10,
		})
	}

	// 等待窗口触发
	time.Sleep(1200 * time.Millisecond)

	// 验证窗口能正常工作（应该收到输出）
	select {
	case data := <-tw.OutputChan():
		if len(data) == 0 {
			t.Error("期望接收到窗口数据，但为空")
		}
		t.Logf("成功接收到窗口数据，数量: %d", len(data))
	case <-time.After(500 * time.Millisecond):
		t.Error("超时未接收到窗口输出")
	}
}
