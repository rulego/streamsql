package streamsql

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkStreamSQLCore 核心性能基准测试
func BenchmarkStreamSQLCore(b *testing.B) {
	tests := []struct {
		name      string
		sql       string
		hasWindow bool
		waitTime  time.Duration
	}{
		{
			name:      "SimpleFilter",
			sql:       "SELECT deviceId, temperature FROM stream WHERE temperature > 20",
			hasWindow: false,
			waitTime:  50 * time.Millisecond,
		},
		{
			name:      "WindowAggregation",
			sql:       "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('100ms')",
			hasWindow: true,
			waitTime:  200 * time.Millisecond,
		},
		{
			name:      "ComplexQuery",
			sql:       "SELECT deviceId, AVG(temperature), COUNT(*) FROM stream WHERE humidity > 50 GROUP BY deviceId, TumblingWindow('100ms')",
			hasWindow: true,
			waitTime:  250 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			// 使用默认配置进行基准测试
			ssql := New()
			defer ssql.Stop()

			err := ssql.Execute(tt.sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			var resultReceived int64

			// 添加结果处理器
			ssql.AddSink(func(result interface{}) {
				atomic.AddInt64(&resultReceived, 1)
			})

			// 异步消费结果通道防止阻塞
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				for {
					select {
					case <-ssql.Stream().GetResultsChan():
					case <-ctx.Done():
						return
					}
				}
			}()

			// 生成测试数据
			testData := generateTestData(5)

			// 重置统计
			ssql.Stream().ResetStats()

			b.ResetTimer()

			// 执行基准测试
			start := time.Now()
			for i := 0; i < b.N; i++ {
				ssql.Emit(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			// 等待处理完成
			time.Sleep(tt.waitTime)
			cancel()

			// 获取统计信息
			stats := ssql.Stream().GetStats()
			received := atomic.LoadInt64(&resultReceived)

			// 计算性能指标
			inputThroughput := float64(b.N) / inputDuration.Seconds()
			processedCount := stats["output_count"]
			droppedCount := stats["dropped_count"]
			processRate := float64(processedCount) / float64(b.N) * 100
			dropRate := float64(droppedCount) / float64(b.N) * 100

			b.ReportMetric(inputThroughput, "ops/sec")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dropRate, "drop_rate_%")
			b.ReportMetric(float64(received), "results")

			b.Logf("%s - 吞吐量: %.0f ops/sec, 处理率: %.1f%%, 丢弃率: %.2f%%",
				tt.name, inputThroughput, processRate, dropRate)
		})
	}
}

// BenchmarkConfigComparison 配置对比基准测试
func BenchmarkConfigComparison(b *testing.B) {
	tests := []struct {
		name      string
		setupFunc func() *Streamsql
	}{
		{
			name: "Default",
			setupFunc: func() *Streamsql {
				return New()
			},
		},
		{
			name: "HighPerformance",
			setupFunc: func() *Streamsql {
				return New(WithHighPerformance())
			},
		},
		{
			name: "Lightweight",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(5000, 5000, 250))
			},
		},
	}

	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			ssql := tt.setupFunc()
			defer ssql.Stop()

			err := ssql.Execute(sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			var resultCount int64
			ssql.AddSink(func(result interface{}) {
				atomic.AddInt64(&resultCount, 1)
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				for {
					select {
					case <-ssql.Stream().GetResultsChan():
					case <-ctx.Done():
						return
					}
				}
			}()

			testData := generateTestData(3)
			ssql.Stream().ResetStats()

			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				ssql.Emit(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			time.Sleep(50 * time.Millisecond)
			cancel()

			stats := ssql.Stream().GetStats()

			inputThroughput := float64(b.N) / inputDuration.Seconds()
			processedCount := stats["output_count"]
			droppedCount := stats["dropped_count"]
			processRate := float64(processedCount) / float64(b.N) * 100
			dropRate := float64(droppedCount) / float64(b.N) * 100

			b.ReportMetric(inputThroughput, "ops/sec")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dropRate, "drop_rate_%")

			b.Logf("%s配置 - 吞吐量: %.0f ops/sec, 处理率: %.1f%%, 丢弃率: %.2f%%",
				tt.name, inputThroughput, processRate, dropRate)
		})
	}
}

// BenchmarkPureInput 纯输入性能基准测试
func BenchmarkPureInput(b *testing.B) {
	ssql := New(WithHighPerformance())
	defer ssql.Stop()

	sql := "SELECT deviceId FROM stream"
	err := ssql.Execute(sql)
	if err != nil {
		b.Fatal(err)
	}

	// 启动结果消费者防止阻塞
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case <-ssql.Stream().GetResultsChan():
			case <-ctx.Done():
				return
			}
		}
	}()

	// 预生成数据
	data := map[string]interface{}{
		"deviceId":    "device1",
		"temperature": 25.0,
	}

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		ssql.Emit(data)
	}

	b.StopTimer()
	duration := time.Since(start)
	throughput := float64(b.N) / duration.Seconds()
	b.ReportMetric(throughput, "pure_input_ops/sec")

	b.Logf("纯输入性能: %.0f ops/sec (%.1f万 ops/sec)", throughput, throughput/10000)
}

// generateTestData 生成测试数据
func generateTestData(count int) []map[string]interface{} {
	data := make([]map[string]interface{}, count)
	devices := []string{"device1", "device2", "device3", "device4", "device5"}

	for i := 0; i < count; i++ {
		data[i] = map[string]interface{}{
			"deviceId":    devices[rand.Intn(len(devices))],
			"temperature": 15.0 + rand.Float64()*20, // 15-35度
			"humidity":    30.0 + rand.Float64()*40, // 30-70%
			"timestamp":   time.Now().UnixNano(),
		}
	}
	return data
}

// BenchmarkConfigurationComparison 不同配置性能对比基准测试
func BenchmarkConfigurationComparison(b *testing.B) {
	tests := []struct {
		name        string
		setupFunc   func() *Streamsql
		description string
	}{
		{
			name: "轻量配置",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(5000, 5000, 250))
			},
			description: "5K数据缓冲，5K结果缓冲，250 sink池",
		},
		{
			name: "默认配置（中等场景）",
			setupFunc: func() *Streamsql {
				return New()
			},
			description: "20K数据缓冲，20K结果缓冲，800 sink池",
		},
		{
			name: "重负载配置",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(35000, 35000, 1200))
			},
			description: "35K数据缓冲，35K结果缓冲，1.2K sink池",
		},
		{
			name: "高性能配置",
			setupFunc: func() *Streamsql {
				return New(WithHighPerformance())
			},
			description: "50K数据缓冲，50K结果缓冲，1K sink池",
		},
		{
			name: "超大缓冲配置",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(100000, 100000, 2000))
			},
			description: "100K数据缓冲，100K结果缓冲，2K sink池",
		},
	}

	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			ssql := tt.setupFunc()
			defer ssql.Stop()

			err := ssql.Execute(sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			var resultCount int64

			// 添加轻量级sink
			ssql.AddSink(func(result interface{}) {
				atomic.AddInt64(&resultCount, 1)
			})

			// 异步消费resultChan
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				for {
					select {
					case <-ssql.Stream().GetResultsChan():
						// 快速消费
					case <-ctx.Done():
						return
					}
				}
			}()

			// 测试数据
			testData := generateTestData(3)

			b.ResetTimer()

			// 执行基准测试
			start := time.Now()
			for i := 0; i < b.N; i++ {
				ssql.Emit(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			// 等待处理完成
			time.Sleep(100 * time.Millisecond)

			cancel()

			// 获取统计信息
			detailedStats := ssql.Stream().GetDetailedStats()
			results := atomic.LoadInt64(&resultCount)

			// 性能指标
			inputThroughput := float64(b.N) / inputDuration.Seconds()
			processRate := detailedStats["process_rate"].(float64)
			dropRate := detailedStats["drop_rate"].(float64)
			perfLevel := detailedStats["performance_level"].(string)

			b.ReportMetric(inputThroughput, "input_ops/sec")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dropRate, "drop_rate_%")
			b.ReportMetric(float64(results), "results_count")

			// 详细报告
			b.Logf("配置: %s", tt.description)
			b.Logf("性能等级: %s", perfLevel)
			b.Logf("处理效率: %.2f%%, 丢弃率: %.2f%%", processRate, dropRate)

			// 缓冲区使用情况
			dataChanUsage := detailedStats["data_chan_usage"].(float64)
			resultChanUsage := detailedStats["result_chan_usage"].(float64)
			sinkPoolUsage := detailedStats["sink_pool_usage"].(float64)

			b.Logf("缓冲区使用率 - 数据: %.1f%%, 结果: %.1f%%, Sink池: %.1f%%",
				dataChanUsage, resultChanUsage, sinkPoolUsage)
		})
	}
}

// TestMemoryUsageComparison 内存使用对比测试
//func TestMemoryUsageComparison(t *testing.T) {
//	tests := []struct {
//		name        string
//		setupFunc   func() *Streamsql
//		description string
//		expectedMB  float64 // 预期内存使用(MB)
//	}{
//		{
//			name: "轻量配置",
//			setupFunc: func() *Streamsql {
//				return New(WithBufferSizes(5000, 5000, 250))
//			},
//			description: "5K数据 + 5K结果 + 250sink池",
//			expectedMB:  1.0, // 预期约1MB
//		},
//		{
//			name: "默认配置（中等场景）",
//			setupFunc: func() *Streamsql {
//				return New()
//			},
//			description: "20K数据 + 20K结果 + 800sink池",
//			expectedMB:  3.0, // 预期约3MB
//		},
//		{
//			name: "高性能配置",
//			setupFunc: func() *Streamsql {
//				return New(WithHighPerformance())
//			},
//			description: "50K数据 + 50K结果 + 1Ksinki池",
//			expectedMB:  12.0, // 预期约12MB
//		},
//		{
//			name: "超大缓冲配置",
//			setupFunc: func() *Streamsql {
//				return New(WithBufferSizes(100000, 100000, 2000))
//			},
//			description: "100K数据缓冲，100K结果缓冲，2Ksinki池",
//			expectedMB:  25.0, // 预期约25MB
//		},
//	}
//
//	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			// 获取开始内存
//			var startMem runtime.MemStats
//			runtime.GC()
//			runtime.ReadMemStats(&startMem)
//
//			// 创建Stream
//			ssql := tt.setupFunc()
//			err := ssql.Execute(sql)
//			if err != nil {
//				t.Fatalf("SQL执行失败: %v", err)
//			}
//
//			// 等待初始化完成
//			time.Sleep(10 * time.Millisecond)
//
//			// 获取创建后内存
//			var afterCreateMem runtime.MemStats
//			runtime.GC()
//			runtime.ReadMemStats(&afterCreateMem)
//
//			createUsage := float64(afterCreateMem.Alloc-startMem.Alloc) / 1024 / 1024
//
//			// 添加一些数据测试内存增长
//			testData := generateTestData(3)
//			for i := 0; i < 1000; i++ {
//				ssql.Emit(testData[i%len(testData)])
//			}
//
//			time.Sleep(50 * time.Millisecond)
//
//			// 获取使用后内存
//			var afterUseMem runtime.MemStats
//			runtime.GC()
//			runtime.ReadMemStats(&afterUseMem)
//
//			totalUsage := float64(afterUseMem.Alloc-startMem.Alloc) / 1024 / 1024
//
//			// 获取详细统计
//			detailedStats := ssql.Stream().GetDetailedStats()
//			basicStats := detailedStats["basic_stats"].(map[string]int64)
//
//			ssql.Stop()
//
//			t.Logf("=== %s 内存使用分析 ===", tt.name)
//			t.Logf("配置: %s", tt.description)
//			t.Logf("创建开销: %.2f MB", createUsage)
//			t.Logf("总内存使用: %.2f MB", totalUsage)
//			t.Logf("缓冲区配置:")
//			t.Logf("  数据通道: %d", basicStats["data_chan_cap"])
//			t.Logf("  结果通道: %d", basicStats["result_chan_cap"])
//			t.Logf("  Sink池: %d", basicStats["sink_pool_cap"])
//
//			// 计算理论内存使用 (每个接口槽位约24字节)
//			dataChanMem := float64(basicStats["data_chan_cap"]) * 24 / 1024 / 1024
//			resultChanMem := float64(basicStats["result_chan_cap"]) * 24 / 1024 / 1024
//			sinkPoolMem := float64(basicStats["sink_pool_cap"]) * 8 / 1024 / 1024 // 函数指针
//
//			theoreticalMem := dataChanMem + resultChanMem + sinkPoolMem
//
//			t.Logf("理论内存分配:")
//			t.Logf("  数据通道: %.2f MB", dataChanMem)
//			t.Logf("  结果通道: %.2f MB", resultChanMem)
//			t.Logf("  Sink池: %.2f MB", sinkPoolMem)
//			t.Logf("  理论总计: %.2f MB", theoreticalMem)
//
//			// 内存效率分析
//			if totalUsage > tt.expectedMB*2 {
//				t.Logf("警告: 内存使用超过预期2倍 (%.2f MB > %.2f MB)", totalUsage, tt.expectedMB*2)
//			} else if totalUsage > tt.expectedMB*1.5 {
//				t.Logf("注意: 内存使用超过预期50%% (%.2f MB > %.2f MB)", totalUsage, tt.expectedMB*1.5)
//			} else {
//				t.Logf("✓ 内存使用在合理范围内 (%.2f MB)", totalUsage)
//			}
//		})
//	}
//}

// BenchmarkLightweightVsDefaultComparison 轻量 vs 默认配置基准测试
func BenchmarkLightweightVsDefaultComparison(b *testing.B) {
	tests := []struct {
		name      string
		setupFunc func() *Streamsql
	}{
		{
			name: "轻量配置5K",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(5000, 5000, 250))
			},
		},
		{
			name: "默认配置20K",
			setupFunc: func() *Streamsql {
				return New()
			},
		},
	}

	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			ssql := tt.setupFunc()
			defer ssql.Stop()

			err := ssql.Execute(sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			var resultCount int64
			ssql.AddSink(func(result interface{}) {
				atomic.AddInt64(&resultCount, 1)
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				for {
					select {
					case <-ssql.Stream().GetResultsChan():
					case <-ctx.Done():
						return
					}
				}
			}()

			testData := generateTestData(3)

			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				ssql.Emit(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			time.Sleep(50 * time.Millisecond)
			cancel()

			detailedStats := ssql.Stream().GetDetailedStats()
			results := atomic.LoadInt64(&resultCount)

			inputThroughput := float64(b.N) / inputDuration.Seconds()
			processRate := detailedStats["process_rate"].(float64)
			dropRate := detailedStats["drop_rate"].(float64)
			dataChanUsage := detailedStats["data_chan_usage"].(float64)

			b.ReportMetric(inputThroughput, "input_ops/sec")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dropRate, "drop_rate_%")
			b.ReportMetric(dataChanUsage, "data_chan_usage_%")
			b.ReportMetric(float64(results), "results_count")

			basicStats := detailedStats["basic_stats"].(map[string]int64)
			b.Logf("缓冲区配置: 数据通道 %d, 结果通道 %d, Sink池 %d",
				basicStats["data_chan_cap"],
				basicStats["result_chan_cap"],
				basicStats["sink_pool_cap"])
			b.Logf("性能指标: %.0f ops/sec, 处理率 %.1f%%, 丢弃率 %.2f%%, 通道使用率 %.1f%%",
				inputThroughput, processRate, dropRate, dataChanUsage)
		})
	}
}

// BenchmarkStreamSQLRealistic 现实的性能基准测试
func BenchmarkStreamSQLRealistic(b *testing.B) {
	tests := []struct {
		name      string
		sql       string
		hasWindow bool
		waitTime  time.Duration
	}{
		{
			name:      "SimpleFilter",
			sql:       "SELECT deviceId, temperature FROM stream WHERE temperature > 20",
			hasWindow: false,
			waitTime:  50 * time.Millisecond,
		},
		{
			name:      "BasicAggregation",
			sql:       "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('100ms')",
			hasWindow: true,
			waitTime:  200 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			// 使用默认配置，避免异常大的缓冲区
			ssql := New()
			defer ssql.Stop()

			err := ssql.Execute(tt.sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			var processedCount int64
			var actualResultCount int64

			// 测量实际的处理完成
			ssql.AddSink(func(result interface{}) {
				atomic.AddInt64(&actualResultCount, 1)
			})

			// 不使用异步消费resultChan，让系统自然处理
			testData := generateTestData(3)

			// 限制测试规模，避免过度膨胀
			maxIterations := min(b.N, 10000) // 最多1万次

			ssql.Stream().ResetStats()
			b.ResetTimer()

			// 受控的输入，测量真实处理性能
			start := time.Now()
			for i := 0; i < maxIterations; i++ {
				// 直接使用AddData，如果系统处理不过来会自然阻塞或丢弃
				ssql.Emit(testData[i%len(testData)])
				atomic.AddInt64(&processedCount, 1)

				// 每100条数据稍微停顿，模拟真实的数据流
				if i > 0 && i%100 == 0 {
					time.Sleep(1 * time.Millisecond)
				}
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			// 等待处理完成
			time.Sleep(tt.waitTime)

			processed := atomic.LoadInt64(&processedCount)
			results := atomic.LoadInt64(&actualResultCount)
			stats := ssql.Stream().GetStats()

			// 计算真实的处理吞吐量
			realThroughput := float64(processed) / inputDuration.Seconds()

			b.ReportMetric(realThroughput, "realistic_ops/sec")
			b.ReportMetric(float64(results), "actual_results")
			b.ReportMetric(float64(stats["dropped_count"]), "dropped_data")

			// 输出合理的性能数据范围
			b.Logf("实际输入: %d 条, 实际结果: %d 个", processed, results)
			b.Logf("真实吞吐量: %.0f ops/sec (%.1f万 ops/sec)", realThroughput, realThroughput/10000)

			if dropped := stats["dropped_count"]; dropped > 0 {
				b.Logf("丢弃数据: %d 条", dropped)
			}
		})
	}
}

// min 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// BenchmarkPurePerformance 纯性能基准测试（无等待，无限制）
func BenchmarkPurePerformance(b *testing.B) {
	ssql := New(WithHighPerformance())
	defer ssql.Stop()

	sql := "SELECT deviceId FROM stream"
	err := ssql.Execute(sql)
	if err != nil {
		b.Fatal(err)
	}

	// 启动结果消费者
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case <-ssql.Stream().GetResultsChan():
			case <-ctx.Done():
				return
			}
		}
	}()

	// 预生成单条数据
	data := map[string]interface{}{
		"deviceId":    "device1",
		"temperature": 25.0,
	}

	b.ResetTimer()
	start := time.Now()

	// 纯输入性能测试
	for i := 0; i < b.N; i++ {
		ssql.Emit(data)
	}

	b.StopTimer()
	duration := time.Since(start)
	throughput := float64(b.N) / duration.Seconds()
	cancel()

	b.ReportMetric(throughput, "pure_ops/sec")
	b.Logf("纯输入性能: %.0f ops/sec (%.1f万 ops/sec)", throughput, throughput/10000)
}

// BenchmarkEndToEndProcessing 端到端处理性能基准测试
func BenchmarkEndToEndProcessing(b *testing.B) {
	tests := []struct {
		name         string
		sql          string
		batchSize    int
		expectOutput bool
	}{
		{
			name:         "EndToEndFilter",
			sql:          "SELECT deviceId, temperature FROM stream WHERE temperature > 20",
			batchSize:    1000,
			expectOutput: true,
		},
		{
			name:         "EndToEndAggregation",
			sql:          "SELECT deviceId, COUNT(*) as count FROM stream GROUP BY deviceId, TumblingWindow('100ms')",
			batchSize:    500,
			expectOutput: true,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			ssql := New()
			defer ssql.Stop()

			err := ssql.Execute(tt.sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			// 计算需要的批次数
			batches := (b.N + tt.batchSize - 1) / tt.batchSize

			var totalProcessed int64
			var totalDuration time.Duration

			testData := generateTestData(3)

			b.ResetTimer()

			// 分批处理，每批次测量完整的处理时间
			for batch := 0; batch < batches; batch++ {
				currentBatchSize := tt.batchSize
				if batch == batches-1 {
					// 最后一批可能不满
					currentBatchSize = b.N - batch*tt.batchSize
				}

				var resultsReceived int64
				resultChan := make(chan bool, currentBatchSize)

				// 设置sink来捕获结果
				ssql.AddSink(func(result interface{}) {
					count := atomic.AddInt64(&resultsReceived, 1)
					if count <= int64(currentBatchSize) {
						resultChan <- true
					}
				})

				// 记录开始时间
				start := time.Now()

				// 输入数据
				for i := 0; i < currentBatchSize; i++ {
					ssql.Emit(testData[i%len(testData)])
				}

				// 等待所有结果处理完成（对于非聚合查询）
				if tt.expectOutput {
					expectedResults := currentBatchSize
					if tt.name == "EndToEndAggregation" {
						// 聚合查询的结果数量较少，等待至少1个结果
						expectedResults = 1
					}

					receivedCount := 0
					timeout := time.After(5 * time.Second)
					for receivedCount < expectedResults {
						select {
						case <-resultChan:
							receivedCount++
							if tt.name == "EndToEndAggregation" && receivedCount >= 1 {
								// 聚合查询收到1个结果就算完成
								goto batchDone
							}
						case <-timeout:
							// 超时，记录实际收到的结果
							goto batchDone
						}
					}
				}

			batchDone:
				// 记录这批次的处理时间
				batchDuration := time.Since(start)
				totalDuration += batchDuration
				totalProcessed += int64(currentBatchSize)

				// 注意：没有ClearSinks方法，所以每次测试使用新的Stream实例
			}

			b.StopTimer()

			// 计算真实的端到端吞吐量
			realThroughput := float64(totalProcessed) / totalDuration.Seconds()

			b.ReportMetric(realThroughput, "end_to_end_ops/sec")
			b.Logf("端到端测试 - 处理: %d 条, 总耗时: %v", totalProcessed, totalDuration)
			b.Logf("端到端吞吐量: %.0f ops/sec (%.1f万 ops/sec)", realThroughput, realThroughput/10000)
		})
	}
}

// BenchmarkSustainedProcessing 持续处理性能基准测试
func BenchmarkSustainedProcessing(b *testing.B) {
	ssql := New()
	defer ssql.Stop()

	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"
	err := ssql.Execute(sql)
	if err != nil {
		b.Fatal(err)
	}

	var processedResults int64
	var lastResultTime time.Time

	// 设置结果处理器
	ssql.AddSink(func(result interface{}) {
		atomic.AddInt64(&processedResults, 1)
		lastResultTime = time.Now()
	})

	testData := generateTestData(3)

	b.ResetTimer()
	start := time.Now()

	// 持续输入数据
	for i := 0; i < b.N; i++ {
		ssql.Emit(testData[i%len(testData)])

		// 每1000条检查一次处理进度
		if i > 0 && i%1000 == 0 {
			time.Sleep(1 * time.Millisecond) // 让系统有时间处理
		}
	}

	inputEnd := time.Now()
	inputDuration := inputEnd.Sub(start)

	// 等待所有结果处理完成
	for {
		current := atomic.LoadInt64(&processedResults)
		if current >= int64(b.N) {
			break
		}
		if time.Since(lastResultTime) > 2*time.Second {
			// 2秒没有新结果，认为处理完成
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	totalDuration := time.Since(start)
	final := atomic.LoadInt64(&processedResults)

	b.StopTimer()

	inputThroughput := float64(b.N) / inputDuration.Seconds()
	sustainedThroughput := float64(final) / totalDuration.Seconds()

	b.ReportMetric(inputThroughput, "input_rate_ops/sec")
	b.ReportMetric(sustainedThroughput, "sustained_ops/sec")
	b.ReportMetric(float64(final), "processed_count")

	b.Logf("持续处理测试:")
	b.Logf("  输入速率: %.0f ops/sec (%.1f万 ops/sec)", inputThroughput, inputThroughput/10000)
	b.Logf("  持续处理速率: %.0f ops/sec (%.1f万 ops/sec)", sustainedThroughput, sustainedThroughput/10000)
	b.Logf("  处理完成率: %.1f%% (%d/%d)", float64(final)/float64(b.N)*100, final, b.N)
}
