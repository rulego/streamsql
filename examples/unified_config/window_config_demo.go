package main

import (
	"fmt"
	"time"

	"github.com/rulego/streamsql"
	"github.com/rulego/streamsql/types"
)

// demonstrateWindowConfig 演示窗口统一配置的使用
func demonstrateWindowConfig() {
	fmt.Println("=== 窗口统一配置演示 ===")

	// 1. 测试默认配置的窗口
	fmt.Println("\n1. 默认配置窗口测试")
	testWindowWithConfig("默认配置", streamsql.New())

	// 2. 测试高性能配置的窗口
	fmt.Println("\n2. 高性能配置窗口测试")
	testWindowWithConfig("高性能配置", streamsql.New(streamsql.WithHighPerformance()))

	// 3. 测试低延迟配置的窗口
	fmt.Println("\n3. 低延迟配置窗口测试")
	testWindowWithConfig("低延迟配置", streamsql.New(streamsql.WithLowLatency()))

	// 4. 测试自定义配置的窗口
	fmt.Println("\n4. 自定义配置窗口测试")
	customConfig := types.DefaultPerformanceConfig()
	customConfig.BufferConfig.WindowOutputSize = 2000 // 自定义窗口输出缓冲区大小
	testWindowWithConfig("自定义配置", streamsql.New(streamsql.WithCustomPerformance(customConfig)))

	fmt.Println("\n=== 窗口配置演示完成 ===")
}

func testWindowWithConfig(configName string, ssql *streamsql.Streamsql) {
	// 执行一个简单的滚动窗口查询
	sql := "SELECT deviceId, AVG(temperature) as avg_temp FROM stream GROUP BY deviceId, TumblingWindow('2s')"

	err := ssql.Execute(sql)
	if err != nil {
		fmt.Printf("❌ %s - 执行SQL失败: %v\n", configName, err)
		return
	}

	// 添加结果处理器
	stream := ssql.Stream()
	if stream != nil {
		stream.AddSink(func(result interface{}) {
			fmt.Printf("📊 %s - 窗口结果: %v\n", configName, result)
		})

		// 发送测试数据
		for i := 0; i < 5; i++ {
			data := map[string]interface{}{
				"deviceId":    fmt.Sprintf("device_%d", i%2),
				"temperature": 20.0 + float64(i),
				"timestamp":   time.Now(),
			}
			ssql.Emit(data)
		}

		// 等待处理完成
		time.Sleep(3 * time.Second)

		// 获取统计信息
		stats := ssql.GetDetailedStats()
		fmt.Printf("📈 %s - 统计信息: %v\n", configName, stats)
	}

	// 停止流处理
	ssql.Stop()
	fmt.Printf("✅ %s - 测试完成\n", configName)
}
