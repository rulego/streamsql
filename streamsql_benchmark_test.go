package streamsql

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkStreamSQL StreamSQL基准测试
func BenchmarkStreamSQL(b *testing.B) {
	tests := []struct {
		name     string
		sql      string
		waitTime time.Duration
	}{
		{
			name:     "SimpleFilter",
			sql:      "SELECT deviceId, temperature FROM stream WHERE temperature > 20",
			waitTime: 50 * time.Millisecond,
		},
		{
			name:     "BasicAggregation",
			sql:      "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('100ms')",
			waitTime: 150 * time.Millisecond,
		},
		{
			name:     "ComplexQuery",
			sql:      "SELECT deviceId, AVG(temperature), COUNT(*) FROM stream WHERE humidity > 50 GROUP BY deviceId, TumblingWindow('100ms')",
			waitTime: 200 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			// 使用默认配置
			ssql := New()
			defer ssql.Stop()

			err := ssql.Execute(tt.sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			var resultCount int64
			ssql.AddSink(func(result []map[string]interface{}) {
				atomic.AddInt64(&resultCount, 1)
			})

			// 异步消费结果防止阻塞
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
			testData := generateOptimizedTestData(5)
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
			results := atomic.LoadInt64(&resultCount)

			// 计算核心性能指标
			throughput := float64(b.N) / inputDuration.Seconds()
			processedCount := stats["output_count"]
			droppedCount := stats["dropped_count"]
			processRate := float64(processedCount) / float64(b.N) * 100
			dropRate := float64(droppedCount) / float64(b.N) * 100

			// 报告指标
			b.ReportMetric(throughput, "ops/sec")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dropRate, "drop_rate_%")
			b.ReportMetric(float64(results), "results")

			// 输出可读的性能报告
			b.Logf("性能报告 - %s:", tt.name)
			b.Logf("  吞吐量: %.0f ops/sec (%.1f万 ops/sec)", throughput, throughput/10000)
			b.Logf("  处理率: %.1f%%, 丢弃率: %.2f%%", processRate, dropRate)
			b.Logf("  结果数: %d", results)
		})
	}
}

// BenchmarkConfigurationOptimized 优化后的配置对比基准测试
func BenchmarkConfigurationOptimized(b *testing.B) {
	configs := []struct {
		name      string
		setupFunc func() *Streamsql
	}{
		{
			name: "Lightweight",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(5000, 5000, 250))
			},
		},
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
	}

	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"

	for _, config := range configs {
		b.Run(config.name, func(b *testing.B) {
			ssql := config.setupFunc()
			defer ssql.Stop()

			err := ssql.Execute(sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			var resultCount int64
			ssql.AddSink(func(result []map[string]interface{}) {
				atomic.AddInt64(&resultCount, 1)
			})

			// 异步消费结果
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

			testData := generateOptimizedTestData(3)
			ssql.Stream().ResetStats()

			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				ssql.Emit(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			time.Sleep(100 * time.Millisecond)
			cancel()

			// 获取详细统计
			detailedStats := ssql.Stream().GetDetailedStats()
			results := atomic.LoadInt64(&resultCount)

			throughput := float64(b.N) / inputDuration.Seconds()
			processRate := detailedStats["process_rate"].(float64)
			dropRate := detailedStats["drop_rate"].(float64)

			b.ReportMetric(throughput, "ops/sec")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dropRate, "drop_rate_%")

			b.Logf("%s配置性能:", config.name)
			b.Logf("  吞吐量: %.0f ops/sec (%.1f万 ops/sec)", throughput, throughput/10000)
			b.Logf("  处理率: %.1f%%, 丢弃率: %.2f%%", processRate, dropRate)
			b.Logf("  结果数: %d", results)
		})
	}
}

// BenchmarkPureInputOptimized 优化后的纯输入性能测试
func BenchmarkPureInputOptimized(b *testing.B) {
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

	b.ReportMetric(throughput, "pure_ops/sec")
	b.Logf("纯输入性能: %.0f ops/sec (%.1f万 ops/sec)", throughput, throughput/10000)
}

// BenchmarkPostAggregationPerformance 后聚合性能基准测试
func BenchmarkPostAggregationPerformance(b *testing.B) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "SimpleAggregation",
			sql:  "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('100ms')",
		},
		{
			name: "PostAggregationSimple",
			sql:  "SELECT deviceId, AVG(temperature) + 10 as adjusted_temp FROM stream GROUP BY deviceId, TumblingWindow('100ms')",
		},
		{
			name: "PostAggregationComplex",
			sql:  "SELECT deviceId, CEIL(AVG(temperature) * 1.8 + 32) as fahrenheit FROM stream GROUP BY deviceId, TumblingWindow('100ms')",
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

			var resultCount int64
			ssql.AddSink(func(result []map[string]interface{}) {
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

			testData := generateOptimizedTestData(5)
			ssql.Stream().ResetStats()

			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				ssql.Emit(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			time.Sleep(200 * time.Millisecond)
			cancel()

			stats := ssql.Stream().GetStats()
			results := atomic.LoadInt64(&resultCount)

			throughput := float64(b.N) / inputDuration.Seconds()
			processedCount := stats["output_count"]
			droppedCount := stats["dropped_count"]
			processRate := float64(processedCount) / float64(b.N) * 100
			dropRate := float64(droppedCount) / float64(b.N) * 100

			b.ReportMetric(throughput, "ops/sec")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dropRate, "drop_rate_%")
			b.ReportMetric(float64(results), "results")

			b.Logf("%s性能:", tt.name)
			b.Logf("  吞吐量: %.0f ops/sec (%.1f万 ops/sec)", throughput, throughput/10000)
			b.Logf("  处理率: %.1f%%, 丢弃率: %.2f%%", processRate, dropRate)
			b.Logf("  结果数: %d", results)
		})
	}
}

// generateOptimizedTestData 生成优化的测试数据
func generateOptimizedTestData(count int) []map[string]interface{} {
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

// BenchmarkMemoryEfficiency 内存效率基准测试
func BenchmarkMemoryEfficiency(b *testing.B) {
	configs := []struct {
		name      string
		setupFunc func() *Streamsql
	}{
		{
			name: "Lightweight5K",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(5000, 5000, 250))
			},
		},
		{
			name: "Default20K",
			setupFunc: func() *Streamsql {
				return New()
			},
		},
		{
			name: "HighPerf50K",
			setupFunc: func() *Streamsql {
				return New(WithHighPerformance())
			},
		},
	}

	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"

	for _, config := range configs {
		b.Run(config.name, func(b *testing.B) {
			ssql := config.setupFunc()
			defer ssql.Stop()

			err := ssql.Execute(sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			var resultCount int64
			ssql.AddSink(func(result []map[string]interface{}) {
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

			testData := generateOptimizedTestData(3)

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

			throughput := float64(b.N) / inputDuration.Seconds()
			processRate := detailedStats["process_rate"].(float64)
			dataChanUsage := detailedStats["data_chan_usage"].(float64)
			resultChanUsage := detailedStats["result_chan_usage"].(float64)

			b.ReportMetric(throughput, "ops/sec")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dataChanUsage, "data_chan_usage_%")
			b.ReportMetric(resultChanUsage, "result_chan_usage_%")
			b.ReportMetric(float64(results), "results")

			basicStats := detailedStats["basic_stats"].(map[string]int64)
			b.Logf("%s配置效率:", config.name)
			b.Logf("  缓冲区: 数据%d/结果%d/Sink%d",
				basicStats["data_chan_cap"],
				basicStats["result_chan_cap"],
				basicStats["sink_pool_cap"])
			b.Logf("  吞吐量: %.0f ops/sec, 处理率: %.1f%%, 结果数: %d", throughput, processRate, results)
			b.Logf("  通道使用率: 数据%.1f%%, 结果%.1f%%", dataChanUsage, resultChanUsage)
		})
	}
}
