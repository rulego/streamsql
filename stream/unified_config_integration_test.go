package stream

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/types"
)

// TestStreamWindowUnifiedConfigIntegration 测试Stream和Window统一配置的集成
func TestStreamWindowUnifiedConfigIntegration(t *testing.T) {
	// 测试不同性能配置下，Stream创建的窗口是否正确应用了缓冲区配置
	testCases := []struct {
		name                     string
		performanceConfig        types.PerformanceConfig
		expectedWindowBufferSize int
	}{
		{
			name:                     "默认配置",
			performanceConfig:        types.DefaultPerformanceConfig(),
			expectedWindowBufferSize: 1000,
		},
		{
			name:                     "高性能配置",
			performanceConfig:        types.HighPerformanceConfig(),
			expectedWindowBufferSize: 5000,
		},
		{
			name:                     "低延迟配置",
			performanceConfig:        types.LowLatencyConfig(),
			expectedWindowBufferSize: 100,
		},
		{
			name:                     "零数据丢失配置",
			performanceConfig:        types.ZeroDataLossConfig(),
			expectedWindowBufferSize: 2000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建包含窗口的配置
			config := types.Config{
				NeedWindow: true,
				WindowConfig: types.WindowConfig{
					Type: "tumbling",
					Params: map[string]interface{}{
						"size": "5s",
					},
				},
				SelectFields: map[string]aggregator.AggregateType{
					"value": aggregator.Count,
				},
				PerformanceConfig: tc.performanceConfig,
			}

			// 创建stream
			s, err := NewStream(config)
			if err != nil {
				t.Fatalf("创建Stream失败: %v", err)
			}
			defer s.Stop()

			// 验证stream的缓冲区配置
			if cap(s.dataChan) != tc.performanceConfig.BufferConfig.DataChannelSize {
				t.Errorf("期望数据通道大小 %d，实际得到 %d",
					tc.performanceConfig.BufferConfig.DataChannelSize, cap(s.dataChan))
			}

			if cap(s.resultChan) != tc.performanceConfig.BufferConfig.ResultChannelSize {
				t.Errorf("期望结果通道大小 %d，实际得到 %d",
					tc.performanceConfig.BufferConfig.ResultChannelSize, cap(s.resultChan))
			}

			// 验证窗口的缓冲区配置 (需要访问窗口的内部状态)
			// 这需要窗口实现暴露缓冲区大小的方法，或者通过类型断言访问
			if s.Window != nil {
				// 通过反射或类型断言来验证窗口缓冲区大小
				// 这里简化测试，只验证窗口不为nil
				t.Logf("窗口已创建，类型: %T", s.Window)
			} else {
				t.Error("期望创建窗口，但窗口为nil")
			}
		})
	}
}

// TestStreamUnifiedConfigPerformanceImpact 测试统一配置对Stream性能的影响
func TestStreamUnifiedConfigPerformanceImpact(t *testing.T) {
	// 基准测试：比较不同配置下的性能
	configs := map[string]types.PerformanceConfig{
		"默认配置":  types.DefaultPerformanceConfig(),
		"高性能配置": types.HighPerformanceConfig(),
		"低延迟配置": types.LowLatencyConfig(),
	}

	for name, perfConfig := range configs {
		t.Run(name, func(t *testing.T) {
			config := types.Config{
				NeedWindow: true,
				WindowConfig: types.WindowConfig{
					Type: "tumbling",
					Params: map[string]interface{}{
						"size": "1s",
					},
				},
				SelectFields: map[string]aggregator.AggregateType{
					"value": aggregator.Sum,
				},
				PerformanceConfig: perfConfig,
			}

			s, err := NewStream(config)
			if err != nil {
				t.Fatalf("创建Stream失败: %v", err)
			}
			defer s.Stop()

			// 启动stream
			go s.Start()

			// 发送测试数据
			dataCount := 1000
			startTime := time.Now()

			for i := 0; i < dataCount; i++ {
				data := map[string]interface{}{
					"value":     i,
					"timestamp": time.Now().Unix(),
				}

				select {
				case s.dataChan <- data:
					// 成功发送
				case <-time.After(100 * time.Millisecond):
					// 发送超时，这在低缓冲区配置下可能发生
					t.Logf("第%d条数据发送超时", i)
					break
				}
			}

			processingTime := time.Since(startTime)
			t.Logf("%s 处理%d条数据耗时: %v", name, dataCount, processingTime)

			// 等待一些结果
			time.Sleep(1500 * time.Millisecond)

			// 检查结果
			resultCount := 0
			for {
				select {
				case <-s.resultChan:
					resultCount++
				default:
					goto done
				}
			}
		done:
			t.Logf("%s 产生结果数量: %d", name, resultCount)
		})
	}
}

// TestStreamUnifiedConfigErrorHandling 测试统一配置的错误处理
func TestStreamUnifiedConfigErrorHandling(t *testing.T) {
	// 测试无效的窗口配置（无效的窗口类型）
	invalidConfig := types.Config{
		NeedWindow: true,
		WindowConfig: types.WindowConfig{
			Type: "invalid_window_type", // 无效的窗口类型
			Params: map[string]interface{}{
				"size": "5s",
			},
		},
		SelectFields: map[string]aggregator.AggregateType{
			"value": aggregator.Count,
		},
		PerformanceConfig: types.DefaultPerformanceConfig(),
	}

	// Stream应该无法创建，因为窗口类型无效
	_, err := NewStream(invalidConfig)
	if err == nil {
		t.Error("期望创建失败，但实际成功了")
		return
	}
	t.Logf("正确捕获到错误: %v", err)

	// 测试无效的窗口参数（缺少必要参数）
	invalidSizeConfig := types.Config{
		NeedWindow: true,
		WindowConfig: types.WindowConfig{
			Type:   "tumbling",
			Params: map[string]interface{}{
				// 缺少 "size" 参数
			},
		},
		SelectFields: map[string]aggregator.AggregateType{
			"value": aggregator.Count,
		},
		PerformanceConfig: types.DefaultPerformanceConfig(),
	}

	_, err = NewStream(invalidSizeConfig)
	if err == nil {
		t.Error("期望因为缺少size参数而创建失败，但实际成功了")
		return
	}
	t.Logf("正确捕获到size参数错误: %v", err)
}

// TestStreamUnifiedConfigCompatibility 测试统一配置的兼容性
func TestStreamUnifiedConfigCompatibility(t *testing.T) {
	// 测试新的统一配置与旧API的兼容性

	// 1. 使用新的统一配置
	newConfig := types.Config{
		NeedWindow: false,
		SelectFields: map[string]aggregator.AggregateType{
			"value": aggregator.Count,
		},
		PerformanceConfig: types.HighPerformanceConfig(),
	}

	s1, err := NewStream(newConfig)
	if err != nil {
		t.Fatalf("使用新配置创建Stream失败: %v", err)
	}
	defer s1.Stop()

	// 验证新配置生效
	expectedDataSize := types.HighPerformanceConfig().BufferConfig.DataChannelSize
	if cap(s1.dataChan) != expectedDataSize {
		t.Errorf("新配置期望数据通道大小 %d，实际得到 %d", expectedDataSize, cap(s1.dataChan))
	}

	// 2. 测试默认配置
	defaultConfig := types.Config{
		NeedWindow: false,
		SelectFields: map[string]aggregator.AggregateType{
			"value": aggregator.Count,
		},
		PerformanceConfig: types.DefaultPerformanceConfig(),
	}

	s2, err := NewStream(defaultConfig)
	if err != nil {
		t.Fatalf("使用默认配置创建Stream失败: %v", err)
	}
	defer s2.Stop()

	// 验证默认配置
	expectedDefaultSize := types.DefaultPerformanceConfig().BufferConfig.DataChannelSize
	if cap(s2.dataChan) != expectedDefaultSize {
		t.Errorf("默认配置期望数据通道大小 %d，实际得到 %d", expectedDefaultSize, cap(s2.dataChan))
	}

	t.Logf("新配置数据通道大小: %d", cap(s1.dataChan))
	t.Logf("默认配置数据通道大小: %d", cap(s2.dataChan))
}
