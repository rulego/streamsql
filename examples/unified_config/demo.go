package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/rulego/streamsql/stream"
	"github.com/rulego/streamsql/types"
)

func main() {
	fmt.Println("=== StreamSQL 统一配置系统演示 ===")

	// 1. 使用新的配置API创建默认配置Stream
	fmt.Println("\n1. 默认配置Stream:")
	defaultConfig := types.NewConfig()
	defaultConfig.SimpleFields = []string{"temperature", "humidity", "location"}

	defaultStream, err := stream.NewStream(defaultConfig)
	if err != nil {
		log.Fatal(err)
	}
	printStreamStats("默认配置", defaultStream)

	// 2. 使用高性能预设配置
	fmt.Println("\n2. 高性能配置Stream:")
	highPerfConfig := types.NewConfigWithPerformance(types.HighPerformanceConfig())
	highPerfConfig.SimpleFields = []string{"temperature", "humidity", "location"}

	highPerfStream, err := stream.NewStreamWithHighPerformance(highPerfConfig)
	if err != nil {
		log.Fatal(err)
	}
	printStreamStats("高性能配置", highPerfStream)

	// 3. 使用低延迟预设配置
	fmt.Println("\n3. 低延迟配置Stream:")
	lowLatencyConfig := types.NewConfigWithPerformance(types.LowLatencyConfig())
	lowLatencyConfig.SimpleFields = []string{"temperature", "humidity", "location"}

	lowLatencyStream, err := stream.NewStreamWithLowLatency(lowLatencyConfig)
	if err != nil {
		log.Fatal(err)
	}
	printStreamStats("低延迟配置", lowLatencyStream)

	// 4. 使用零数据丢失预设配置
	fmt.Println("\n4. 零数据丢失配置Stream:")
	zeroLossConfig := types.NewConfigWithPerformance(types.ZeroDataLossConfig())
	zeroLossConfig.SimpleFields = []string{"temperature", "humidity", "location"}

	zeroLossStream, err := stream.NewStreamWithZeroDataLoss(zeroLossConfig)
	if err != nil {
		log.Fatal(err)
	}
	printStreamStats("零数据丢失配置", zeroLossStream)

	// 5. 使用持久化预设配置
	fmt.Println("\n5. 持久化配置Stream:")
	persistConfig := types.NewConfigWithPerformance(types.PersistencePerformanceConfig())
	persistConfig.SimpleFields = []string{"temperature", "humidity", "location"}

	persistStream, err := stream.NewStreamWithCustomPerformance(persistConfig, types.PersistencePerformanceConfig())
	if err != nil {
		log.Fatal(err)
	}
	printStreamStats("持久化配置", persistStream)

	// 6. 创建完全自定义的配置
	fmt.Println("\n6. 自定义配置Stream:")
	customPerfConfig := types.PerformanceConfig{
		BufferConfig: types.BufferConfig{
			DataChannelSize:     30000,
			ResultChannelSize:   25000,
			WindowOutputSize:    3000,
			EnableDynamicResize: true,
			MaxBufferSize:       200000,
			UsageThreshold:      0.85,
		},
		OverflowConfig: types.OverflowConfig{
			Strategy:      "expand",
			BlockTimeout:  15 * time.Second,
			AllowDataLoss: false,
			ExpansionConfig: types.ExpansionConfig{
				GrowthFactor:     2.0,
				MinIncrement:     2000,
				TriggerThreshold: 0.9,
				ExpansionTimeout: 3 * time.Second,
			},
		},
		WorkerConfig: types.WorkerConfig{
			SinkPoolSize:     800,
			SinkWorkerCount:  12,
			MaxRetryRoutines: 10,
		},
		MonitoringConfig: types.MonitoringConfig{
			EnableMonitoring:    true,
			StatsUpdateInterval: 500 * time.Millisecond,
			EnableDetailedStats: true,
			WarningThresholds: types.WarningThresholds{
				DropRateWarning:     5.0,
				DropRateCritical:    15.0,
				BufferUsageWarning:  75.0,
				BufferUsageCritical: 90.0,
			},
		},
	}

	customConfig := types.NewConfigWithPerformance(customPerfConfig)
	customConfig.SimpleFields = []string{"temperature", "humidity", "location"}

	customStream, err := stream.NewStreamWithCustomPerformance(customConfig, customPerfConfig)
	if err != nil {
		log.Fatal(err)
	}
	printStreamStats("自定义配置", customStream)

	// 7. 配置比较演示
	fmt.Println("\n7. 配置比较:")
	compareConfigurations()

	// 8. 实时数据处理演示
	fmt.Println("\n8. 实时数据处理演示:")
	demonstrateRealTimeProcessing(defaultStream)

	// 9. 窗口统一配置演示
	fmt.Println("\n9. 窗口统一配置演示:")
	demonstrateWindowConfig()

	// 清理资源
	fmt.Println("\n10. 清理资源...")
	defaultStream.Stop()
	highPerfStream.Stop()
	lowLatencyStream.Stop()
	zeroLossStream.Stop()
	persistStream.Stop()
	customStream.Stop()

	fmt.Println("\n=== 演示完成 ===")
}

func printStreamStats(name string, s *stream.Stream) {
	stats := s.GetStats()
	detailedStats := s.GetDetailedStats()

	fmt.Printf("【%s】统计信息:\n", name)
	fmt.Printf("  数据通道: %d/%d (使用率: %.1f%%)\n",
		stats["data_chan_len"], stats["data_chan_cap"],
		detailedStats["data_chan_usage"])
	fmt.Printf("  结果通道: %d/%d (使用率: %.1f%%)\n",
		stats["result_chan_len"], stats["result_chan_cap"],
		detailedStats["result_chan_usage"])
	fmt.Printf("  工作池: %d/%d (使用率: %.1f%%)\n",
		stats["sink_pool_len"], stats["sink_pool_cap"],
		detailedStats["sink_pool_usage"])
	fmt.Printf("  性能等级: %s\n", detailedStats["performance_level"])
}

func compareConfigurations() {
	configs := map[string]types.PerformanceConfig{
		"默认配置":  types.DefaultPerformanceConfig(),
		"高性能配置": types.HighPerformanceConfig(),
		"低延迟配置": types.LowLatencyConfig(),
		"零丢失配置": types.ZeroDataLossConfig(),
		"持久化配置": types.PersistencePerformanceConfig(),
	}

	fmt.Printf("%-12s %-10s %-10s %-10s %-10s %-15s\n",
		"配置类型", "数据缓冲", "结果缓冲", "工作池", "工作线程", "溢出策略")
	fmt.Println(strings.Repeat("-", 75))

	for name, config := range configs {
		fmt.Printf("%-12s %-10d %-10d %-10d %-10d %-15s\n",
			name,
			config.BufferConfig.DataChannelSize,
			config.BufferConfig.ResultChannelSize,
			config.WorkerConfig.SinkPoolSize,
			config.WorkerConfig.SinkWorkerCount,
			config.OverflowConfig.Strategy)
	}
}

func demonstrateRealTimeProcessing(s *stream.Stream) {
	// 设置数据接收器
	s.AddSink(func(data interface{}) {
		fmt.Printf("  接收到处理结果: %v\n", data)
	})

	// 启动流处理
	s.Start()

	// 模拟发送数据
	for i := 0; i < 3; i++ {
		data := map[string]interface{}{
			"temperature": 20.0 + float64(i)*2.5,
			"humidity":    60.0 + float64(i)*5,
			"location":    fmt.Sprintf("sensor_%d", i+1),
			"timestamp":   time.Now().Unix(),
		}

		fmt.Printf("  发送数据: %v\n", data)
		s.Emit(data)
		time.Sleep(100 * time.Millisecond)
	}

	// 等待处理完成
	time.Sleep(200 * time.Millisecond)

	// 显示最终统计
	finalStats := s.GetDetailedStats()
	fmt.Printf("  最终统计 - 输入: %d, 输出: %d, 丢弃: %d, 处理率: %.1f%%\n",
		finalStats["basic_stats"].(map[string]int64)["input_count"],
		finalStats["basic_stats"].(map[string]int64)["output_count"],
		finalStats["basic_stats"].(map[string]int64)["dropped_count"],
		finalStats["process_rate"])
}
