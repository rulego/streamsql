package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamUnifiedConfigIntegration 测试Stream和Window统一配置的集成
func TestStreamUnifiedConfigIntegration(t *testing.T) {
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

			s, err := NewStream(config)
			require.NoError(t, err)
			defer s.Stop()

			// 验证stream的缓冲区配置
			assert.Equal(t, tc.performanceConfig.BufferConfig.DataChannelSize, cap(s.dataChan),
				"数据通道大小不匹配")
			assert.Equal(t, tc.performanceConfig.BufferConfig.ResultChannelSize, cap(s.resultChan),
				"结果通道大小不匹配")

			// 验证窗口创建成功
			assert.NotNil(t, s.Window, "窗口应该被创建")
			t.Logf("窗口已创建，类型: %T", s.Window)
		})
	}
}

// TestStreamUnifiedConfigPerformanceImpact 测试统一配置对Stream性能的影响
func TestStreamUnifiedConfigPerformanceImpact(t *testing.T) {
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
			require.NoError(t, err)
			defer s.Stop()

			go s.Start()

			// 发送测试数据并测量性能
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
					// 发送超时
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
			// 性能测试主要关注处理时间，结果数量可能因窗口触发时机而变化
		})
	}
}

// TestStreamUnifiedConfigErrorHandling 测试统一配置的错误处理
func TestStreamUnifiedConfigErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		config      types.Config
		expectError bool
		description string
	}{
		{
			name: "无效窗口类型",
			config: types.Config{
				NeedWindow: true,
				WindowConfig: types.WindowConfig{
					Type: "invalid_window_type",
					Params: map[string]interface{}{
						"size": "5s",
					},
				},
				SelectFields: map[string]aggregator.AggregateType{
					"value": aggregator.Count,
				},
				PerformanceConfig: types.DefaultPerformanceConfig(),
			},
			expectError: true,
			description: "无效的窗口类型应该导致创建失败",
		},
		{
			name: "缺少窗口大小参数",
			config: types.Config{
				NeedWindow: true,
				WindowConfig: types.WindowConfig{
					Type:   "tumbling",
					Params: map[string]interface{}{},
				},
				SelectFields: map[string]aggregator.AggregateType{
					"value": aggregator.Count,
				},
				PerformanceConfig: types.DefaultPerformanceConfig(),
			},
			expectError: true,
			description: "缺少size参数应该导致创建失败",
		},
		{
			name: "有效配置",
			config: types.Config{
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
				PerformanceConfig: types.DefaultPerformanceConfig(),
			},
			expectError: false,
			description: "有效配置应该创建成功",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, err := NewStream(tt.config)
			if tt.expectError {
				assert.Error(t, err, tt.description)
				assert.Nil(t, stream)
			} else {
				assert.NoError(t, err, tt.description)
				assert.NotNil(t, stream)
				if stream != nil {
					defer stream.Stop()
				}
			}
		})
	}
}

// TestStreamUnifiedConfigCompatibility 测试统一配置的兼容性
func TestStreamUnifiedConfigCompatibility(t *testing.T) {
	// 测试新的统一配置
	newConfig := types.Config{
		NeedWindow: false,
		SelectFields: map[string]aggregator.AggregateType{
			"value": aggregator.Count,
		},
		PerformanceConfig: types.HighPerformanceConfig(),
	}

	s1, err := NewStream(newConfig)
	require.NoError(t, err)
	defer s1.Stop()

	// 验证新配置生效
	expectedDataSize := types.HighPerformanceConfig().BufferConfig.DataChannelSize
	assert.Equal(t, expectedDataSize, cap(s1.dataChan), "高性能配置的数据通道大小不匹配")

	// 测试默认配置
	defaultConfig := types.Config{
		NeedWindow: false,
		SelectFields: map[string]aggregator.AggregateType{
			"value": aggregator.Count,
		},
		PerformanceConfig: types.DefaultPerformanceConfig(),
	}

	s2, err := NewStream(defaultConfig)
	require.NoError(t, err)
	defer s2.Stop()

	// 验证默认配置
	expectedDefaultSize := types.DefaultPerformanceConfig().BufferConfig.DataChannelSize
	assert.Equal(t, expectedDefaultSize, cap(s2.dataChan), "默认配置的数据通道大小不匹配")

	t.Logf("高性能配置数据通道大小: %d", cap(s1.dataChan))
	t.Logf("默认配置数据通道大小: %d", cap(s2.dataChan))
}

// TestStatsManager 测试统计管理器
func TestStatsManager(t *testing.T) {
	config := types.Config{
		NeedWindow:   false,
		SimpleFields: []string{"value"},
	}

	stream, err := NewStream(config)
	require.NoError(t, err)
	defer stream.Stop()

	// 启动流处理
	stream.Start()

	// 发送一些数据来生成统计信息
	for i := 0; i < 10; i++ {
		stream.Emit(map[string]interface{}{"value": i})
	}

	// 等待处理完成
	time.Sleep(100 * time.Millisecond)

	// 测试基本统计
	stats := stream.GetStats()
	assert.Equal(t, int64(10), stats[InputCount], "输入计数不匹配")
	assert.GreaterOrEqual(t, stats[OutputCount], int64(1), "输出计数应该大于等于1")

	// 测试重置统计
	stream.ResetStats()
	stats = stream.GetStats()
	assert.Equal(t, int64(0), stats[InputCount], "重置后输入计数应该为0")
}

// TestDataHandler 测试数据处理器
func TestDataHandler(t *testing.T) {
	tests := []struct {
		name           string
		performanceConfig types.PerformanceConfig
		dataCount      int
		expectedDrops  bool
	}{
		{
			name:             "高性能配置 - 无丢弃",
			performanceConfig: types.HighPerformanceConfig(),
			dataCount:        100,
			expectedDrops:    false,
		},
		{
			name:             "低延迟配置 - 可能丢弃",
			performanceConfig: types.LowLatencyConfig(),
			dataCount:        1000,
			expectedDrops:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := types.Config{
				NeedWindow:        false,
				SimpleFields:      []string{"value"},
				PerformanceConfig: tt.performanceConfig,
			}

			stream, err := NewStream(config)
			require.NoError(t, err)
			defer stream.Stop()

			stream.Start()

			// 快速发送大量数据
			for i := 0; i < tt.dataCount; i++ {
				stream.Emit(map[string]interface{}{"value": i})
			}

			time.Sleep(100 * time.Millisecond)

			stats := stream.GetStats()
			droppedCount := stats[DroppedCount]

			if tt.expectedDrops {
				// 在高负载下可能会有丢弃
				t.Logf("%s: 输入 %d, 丢弃 %d", tt.name, stats[InputCount], droppedCount)
			} else {
				// 高性能配置应该能处理所有数据
				assert.Equal(t, int64(0), droppedCount, "高性能配置不应该丢弃数据")
			}
		})
	}
}

// TestResultHandler 测试结果处理器
func TestResultHandler(t *testing.T) {
	config := types.Config{
		NeedWindow:   false,
		SimpleFields: []string{"value"},
	}

	stream, err := NewStream(config)
	require.NoError(t, err)
	defer stream.Stop()

	// 测试Sink功能
	var mu sync.Mutex
	var receivedResults []interface{}

	stream.AddSink(func(result interface{}) {
		mu.Lock()
		defer mu.Unlock()
		receivedResults = append(receivedResults, result)
	})

	stream.Start()

	// 发送测试数据
	testData := []map[string]interface{}{
		{"value": 1},
		{"value": 2},
		{"value": 3},
	}

	for _, data := range testData {
		stream.Emit(data)
	}

	time.Sleep(100 * time.Millisecond)

	// 验证结果
	mu.Lock()
	defer mu.Unlock()

	assert.GreaterOrEqual(t, len(receivedResults), len(testData), "应该接收到所有结果")

	// 验证结果格式
	for _, result := range receivedResults {
		assert.IsType(t, []map[string]interface{}{}, result, "结果应该是map切片类型")
		resultSlice := result.([]map[string]interface{})
		assert.Greater(t, len(resultSlice), 0, "结果切片不应该为空")
	}
}

// TestPerformanceConfigurations 测试不同性能配置的效果
func TestPerformanceConfigurations(t *testing.T) {
	configs := map[string]types.PerformanceConfig{
		"Default":        types.DefaultPerformanceConfig(),
		"HighPerformance": types.HighPerformanceConfig(),
		"LowLatency":     types.LowLatencyConfig(),
		"ZeroDataLoss":   types.ZeroDataLossConfig(),
	}

	for name, perfConfig := range configs {
		t.Run(name, func(t *testing.T) {
			config := types.Config{
				NeedWindow:        false,
				SimpleFields:      []string{"value"},
				PerformanceConfig: perfConfig,
			}

			stream, err := NewStream(config)
			require.NoError(t, err)
			defer stream.Stop()

			// 验证缓冲区大小
			assert.Equal(t, perfConfig.BufferConfig.DataChannelSize, cap(stream.dataChan))
			assert.Equal(t, perfConfig.BufferConfig.ResultChannelSize, cap(stream.resultChan))

			// 验证工作池配置
			assert.Equal(t, perfConfig.WorkerConfig.SinkPoolSize, cap(stream.sinkWorkerPool))

			t.Logf("%s配置: 数据通道=%d, 结果通道=%d, Sink池=%d",
				name,
				cap(stream.dataChan),
				cap(stream.resultChan),
				cap(stream.sinkWorkerPool))
		})
	}
}