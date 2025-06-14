package streamsql

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/streamsql/stream"
	"github.com/rulego/streamsql/types"
)

// BenchmarkStreamSQLPerformance 综合性能基准测试（优化版本）
func BenchmarkStreamSQLPerformance(b *testing.B) {
	tests := []struct {
		name      string
		sql       string
		hasWindow bool
		waitTime  time.Duration
		config    string // 配置描述
	}{
		{
			name:      "SimpleFilter",
			sql:       "SELECT deviceId, temperature FROM stream WHERE temperature > 20",
			hasWindow: false,
			waitTime:  50 * time.Millisecond,
			config:    "基准测试专用",
		},
		{
			name:      "BasicAggregation",
			sql:       "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('200ms')",
			hasWindow: true,
			waitTime:  400 * time.Millisecond,
			config:    "基准测试专用",
		},
		{
			name:      "ComplexQuery",
			sql:       "SELECT deviceId, AVG(temperature), COUNT(*) FROM stream WHERE humidity > 50 GROUP BY deviceId, TumblingWindow('100ms')",
			hasWindow: true,
			waitTime:  300 * time.Millisecond,
			config:    "基准测试专用",
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			// 使用超大缓冲区专门针对基准测试优化
			// 基准测试需要处理大量迭代，需要更大的缓冲区
			bufferSize := max(int64(100000), int64(b.N/10)) // 至少10万，或者迭代数的1/10
			ssql := New(WithBufferSizes(int(bufferSize), int(bufferSize), 2000))
			defer ssql.Stop()

			err := ssql.Execute(tt.sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			var resultReceived int64

			// 添加非阻塞sink处理结果
			ssql.Stream().AddSink(func(result interface{}) {
				atomic.AddInt64(&resultReceived, 1)
			})

			// 使用context控制生命周期
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// 异步消费resultChan，确保通道不被填满
			go func() {
				for {
					select {
					case <-ssql.Stream().GetResultsChan():
						// 快速消费，避免通道阻塞
					case <-ctx.Done():
						return
					}
				}
			}()

			// 测试数据 - 减少数据种类避免过度生成
			testData := generateTestData(3)

			// 重置统计，获得准确的基准测试数据
			ssql.Stream().ResetStats()

			b.ResetTimer()

			// 执行基准测试 - 添加节流以避免瞬间填满缓冲区
			start := time.Now()
			batchSize := 1000 // 分批处理，避免瞬间打满缓冲区
			for i := 0; i < b.N; i++ {
				ssql.AddData(testData[i%len(testData)])

				// 每处理一批数据，稍微暂停，让系统有时间处理
				if i > 0 && i%batchSize == 0 {
					time.Sleep(10 * time.Microsecond) // 极短暂停
				}
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			// 等待处理完成
			time.Sleep(tt.waitTime)

			cancel() // 停止结果处理goroutine

			// 获取详细统计信息
			detailedStats := ssql.Stream().GetDetailedStats()
			received := atomic.LoadInt64(&resultReceived)

			// 计算性能指标
			inputThroughput := float64(b.N) / inputDuration.Seconds()
			processRate := detailedStats["process_rate"].(float64)
			dropRate := detailedStats["drop_rate"].(float64)
			perfLevel := detailedStats["performance_level"].(string)

			b.ReportMetric(inputThroughput, "input_ops/sec")
			b.ReportMetric(float64(received), "results_received")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dropRate, "drop_rate_%")

			// 性能分析报告
			b.Logf("%s配置 (缓冲区: %d) - 性能等级: %s", tt.config, bufferSize, perfLevel)
			b.Logf("处理效率: %.2f%%, 丢弃率: %.2f%%", processRate, dropRate)
			b.Logf("缓冲区使用: 数据通道 %.1f%%, 结果通道 %.1f%%",
				detailedStats["data_chan_usage"], detailedStats["result_chan_usage"])

			if dropRate > 5 { // 降低警告阈值
				b.Logf("警告: 丢弃率 %.2f%% - 建议增加缓冲区大小", dropRate)
			}

			if !tt.hasWindow && received == 0 {
				b.Logf("警告: 非聚合查询未收到结果")
			}

			// 性能建议
			if dropRate > 10 {
				b.Logf("建议: 使用更大缓冲区配置，当前缓冲区可能不足")
			} else if processRate == 100.0 && dropRate == 0.0 {
				b.Logf("✓ 优秀: 完美的处理效率，无数据丢失")
			}
		})
	}
}

// BenchmarkStreamSQLFixed 修复版本的基准测试
func BenchmarkStreamSQLFixed(b *testing.B) {
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
			waitTime:  10 * time.Millisecond,
		},
		{
			name:      "BasicAggregation",
			sql:       "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('200ms')",
			hasWindow: true,
			waitTime:  300 * time.Millisecond,
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

			var processedCount int64

			// 使用非阻塞的sink避免阻塞
			ssql.Stream().AddSink(func(result interface{}) {
				// 非阻塞计数，不做任何可能阻塞的操作
				atomic.AddInt64(&processedCount, 1)
			})

			// 启动一个goroutine异步消费resultChan，防止填满
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				for {
					select {
					case <-ssql.Stream().GetResultsChan():
						// 快速消费，不做处理
					case <-ctx.Done():
						return
					}
				}
			}()

			// 测试数据
			testData := generateTestData(5) // 减少数据量避免过载

			b.ResetTimer()

			// 执行基准测试
			start := time.Now()
			for i := 0; i < b.N; i++ {
				ssql.AddData(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			// 等待处理完成
			time.Sleep(tt.waitTime)

			// 计算性能指标
			inputThroughput := float64(b.N) / inputDuration.Seconds()
			processed := atomic.LoadInt64(&processedCount)

			b.ReportMetric(inputThroughput, "input_ops/sec")
			b.ReportMetric(float64(processed), "processed_results")
		})
	}
}

// BenchmarkPureInputPerformance 纯输入性能基准测试（避免结果处理的影响）
func BenchmarkPureInputPerformance(b *testing.B) {
	ssql := New()
	defer ssql.Stop()

	// 最简单的查询
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
				// 快速丢弃结果
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

	// 测量纯输入吞吐量
	for i := 0; i < b.N; i++ {
		ssql.AddData(data)
	}

	b.StopTimer()
	duration := time.Since(start)
	throughput := float64(b.N) / duration.Seconds()
	b.ReportMetric(throughput, "pure_input_ops/sec")
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

// generateIoTData 生成更真实的IoT设备数据
func generateIoTData(count int) []map[string]interface{} {
	data := make([]map[string]interface{}, count)
	devices := []string{"sensor001", "sensor002", "sensor003", "gateway001", "gateway002"}
	locations := []string{"building_a", "building_b", "outdoor", "warehouse"}

	for i := 0; i < count; i++ {
		baseTemp := 20.0
		if rand.Float64() < 0.1 { // 10%概率产生异常值
			baseTemp = 40.0
		}

		data[i] = map[string]interface{}{
			"deviceId":    devices[rand.Intn(len(devices))],
			"location":    locations[rand.Intn(len(locations))],
			"temperature": baseTemp + rand.Float64()*10,
			"humidity":    40.0 + rand.Float64()*30,
			"pressure":    1000.0 + rand.Float64()*100,
			"battery":     rand.Float64() * 100,
			"signal":      -30.0 - rand.Float64()*50,
			"timestamp":   time.Now().UnixNano(),
		}
	}
	return data
}

// max 辅助函数
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// TestStreamSQLPerformanceAnalysis 性能分析测试
func TestStreamSQLPerformanceAnalysis(t *testing.T) {
	scenarios := []struct {
		name          string
		sql           string
		dataCount     int
		duration      time.Duration
		expectResults bool
	}{
		{
			name:          "高频非聚合查询",
			sql:           "SELECT deviceId, temperature FROM stream WHERE temperature > 20",
			dataCount:     1000,
			duration:      100 * time.Millisecond,
			expectResults: true,
		},
		{
			name:          "窗口聚合查询",
			sql:           "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('50ms')",
			dataCount:     500,
			duration:      200 * time.Millisecond,
			expectResults: true,
		},
		{
			name:          "复杂聚合查询",
			sql:           "SELECT deviceId, AVG(temperature), MAX(humidity), COUNT(*) FROM stream WHERE temperature > 15 GROUP BY deviceId, TumblingWindow('100ms')",
			dataCount:     300,
			duration:      300 * time.Millisecond,
			expectResults: true,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ssql := New()
			defer ssql.Stop()

			err := ssql.Execute(scenario.sql)
			if err != nil {
				t.Fatalf("SQL执行失败: %v", err)
			}

			var inputCount int64
			var resultCount int64

			// 结果监听
			ctx, cancel := context.WithTimeout(context.Background(), scenario.duration*2)
			defer cancel()

			go func() {
				for {
					select {
					case <-ssql.Stream().GetResultsChan():
						atomic.AddInt64(&resultCount, 1)
					case <-ctx.Done():
						return
					}
				}
			}()

			// 生成和输入数据
			testData := generateTestData(20)
			start := time.Now()

			for i := 0; i < scenario.dataCount; i++ {
				ssql.AddData(testData[i%len(testData)])
				atomic.AddInt64(&inputCount, 1)

				// 控制输入频率，避免过快
				if i%100 == 0 {
					time.Sleep(1 * time.Millisecond)
				}
			}

			inputDuration := time.Since(start)

			// 等待处理完成
			time.Sleep(scenario.duration)

			input := atomic.LoadInt64(&inputCount)
			results := atomic.LoadInt64(&resultCount)

			inputRate := float64(input) / inputDuration.Seconds()

			t.Logf("场景: %s", scenario.name)
			t.Logf("输入数据: %d 条, 耗时: %v", input, inputDuration)
			t.Logf("输入速率: %.2f ops/sec", inputRate)
			t.Logf("生成结果: %d 个", results)

			if scenario.expectResults && results == 0 {
				t.Logf("警告: 预期有结果但未收到任何结果")
			}

			// 基本性能验证
			if inputRate < 1000 {
				t.Logf("注意: 输入速率较低 (%.2f ops/sec)", inputRate)
			}

			if input != int64(scenario.dataCount) {
				t.Errorf("输入数据不完整: 期望 %d, 实际 %d", scenario.dataCount, input)
			}
		})
	}
}

// TestDiagnoseBenchmarkIssues 诊断基准测试阻塞问题的测试用例
func TestDiagnoseBenchmarkIssues(t *testing.T) {
	t.Run("基础功能测试", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		// 使用最简单的查询
		sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"
		err := ssql.Execute(sql)
		if err != nil {
			t.Fatalf("SQL执行失败: %v", err)
		}

		// 检查流是否正确创建
		if ssql.Stream() == nil {
			t.Fatal("流创建失败")
		}

		var resultCount int64
		var lastResult interface{}

		// 添加结果回调
		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&resultCount, 1)
			lastResult = result
			t.Logf("收到结果 #%d: %v", atomic.LoadInt64(&resultCount), result)
		})

		// 添加测试数据
		testData := map[string]interface{}{
			"deviceId":    "device1",
			"temperature": 25.0,
			"humidity":    60.0,
		}

		t.Logf("添加数据: %v", testData)
		ssql.AddData(testData)

		// 等待结果
		time.Sleep(100 * time.Millisecond)

		count := atomic.LoadInt64(&resultCount)
		t.Logf("处理结果数量: %d", count)
		if count > 0 {
			t.Logf("最后结果: %v", lastResult)
		}

		// 验证非聚合查询应该立即返回结果
		if count == 0 {
			t.Error("非聚合查询没有返回任何结果")
		}
	})

	t.Run("窗口聚合测试", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		// 使用滚动窗口
		sql := "SELECT deviceId, AVG(temperature) as avg_temp FROM stream GROUP BY deviceId, TumblingWindow('200ms')"
		err := ssql.Execute(sql)
		if err != nil {
			t.Fatalf("SQL执行失败: %v", err)
		}

		var resultCount int64
		var lastResult interface{}

		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&resultCount, 1)
			lastResult = result
			t.Logf("窗口结果 #%d: %v", atomic.LoadInt64(&resultCount), result)
		})

		// 添加多条数据
		for i := 0; i < 5; i++ {
			testData := map[string]interface{}{
				"deviceId":    "device1",
				"temperature": 20.0 + float64(i),
				"humidity":    60.0,
			}
			t.Logf("添加数据 #%d: %v", i+1, testData)
			ssql.AddData(testData)
			time.Sleep(10 * time.Millisecond) // 小间隔
		}

		// 等待窗口触发
		t.Log("等待窗口触发...")
		time.Sleep(300 * time.Millisecond)

		count := atomic.LoadInt64(&resultCount)
		t.Logf("窗口结果数量: %d", count)
		if count > 0 {
			t.Logf("最后窗口结果: %v", lastResult)
		}

		if count == 0 {
			t.Error("窗口聚合没有返回任何结果")
		}
	})

	t.Run("高频数据测试", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		sql := "SELECT deviceId, COUNT(*) as count FROM stream GROUP BY deviceId, TumblingWindow('100ms')"
		err := ssql.Execute(sql)
		if err != nil {
			t.Fatalf("SQL执行失败: %v", err)
		}

		var resultCount int64
		var totalDataPoints int64

		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&resultCount, 1)
			if resultSlice, ok := result.([]map[string]interface{}); ok {
				for _, r := range resultSlice {
					if count, exists := r["count"]; exists {
						if countVal, ok := count.(int64); ok {
							atomic.AddInt64(&totalDataPoints, countVal)
						}
					}
				}
			}
			t.Logf("高频结果 #%d: %v", atomic.LoadInt64(&resultCount), result)
		})

		// 高频添加数据
		start := time.Now()
		dataCount := 50
		for i := 0; i < dataCount; i++ {
			testData := map[string]interface{}{
				"deviceId":    fmt.Sprintf("device%d", (i%3)+1),
				"temperature": 20.0 + rand.Float64()*10,
				"humidity":    50.0 + rand.Float64()*20,
			}
			ssql.AddData(testData)
		}
		inputDuration := time.Since(start)

		// 等待窗口处理
		time.Sleep(200 * time.Millisecond)

		windows := atomic.LoadInt64(&resultCount)
		processed := atomic.LoadInt64(&totalDataPoints)

		t.Logf("输入 %d 条数据，用时 %v", dataCount, inputDuration)
		t.Logf("生成 %d 个窗口，处理 %d 条数据", windows, processed)
		t.Logf("输入速率: %.2f ops/sec", float64(dataCount)/inputDuration.Seconds())

		if processed != int64(dataCount) {
			t.Logf("警告: 处理数据量 (%d) 与输入数据量 (%d) 不匹配", processed, dataCount)
		}

		if windows == 0 {
			t.Error("高频数据测试没有生成任何窗口")
		}
	})

	t.Run("性能基准预测试", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		sql := "SELECT deviceId, temperature, humidity FROM stream WHERE temperature > 20"
		err := ssql.Execute(sql)
		if err != nil {
			t.Fatalf("SQL执行失败: %v", err)
		}

		var processedCount int64
		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&processedCount, 1)
		})

		testData := generateTestData(10)

		// 模拟基准测试的执行方式
		start := time.Now()
		iterations := 1000
		for i := 0; i < iterations; i++ {
			ssql.AddData(testData[i%len(testData)])
		}
		duration := time.Since(start)

		time.Sleep(20 * time.Millisecond) // 等待处理完成

		processed := atomic.LoadInt64(&processedCount)
		throughput := float64(iterations) / duration.Seconds()

		t.Logf("执行 %d 次迭代，用时 %v", iterations, duration)
		t.Logf("处理 %d 条数据", processed)
		t.Logf("吞吐量: %.2f ops/sec", throughput)

		if processed == 0 {
			t.Error("性能基准预测试没有处理任何数据")
		}

		// 检查是否会阻塞
		if duration > 5*time.Second {
			t.Error("执行时间过长，可能存在阻塞问题")
		}
	})
}

// TestStreamOptimizations 测试Stream优化效果
func TestStreamOptimizations(t *testing.T) {
	t.Run("非阻塞性能测试", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		// 使用简单查询测试
		sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"
		err := ssql.Execute(sql)
		if err != nil {
			t.Fatalf("SQL执行失败: %v", err)
		}

		// 添加多个sink模拟实际使用场景
		var sink1Count, sink2Count, sink3Count int64

		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&sink1Count, 1)
			time.Sleep(1 * time.Millisecond) // 模拟处理延迟
		})

		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&sink2Count, 1)
			time.Sleep(2 * time.Millisecond) // 模拟较慢的sink
		})

		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&sink3Count, 1)
		})

		// 快速输入大量数据
		testData := generateTestData(10)
		inputCount := 1000

		start := time.Now()
		for i := 0; i < inputCount; i++ {
			ssql.AddData(testData[i%len(testData)])
		}
		inputDuration := time.Since(start)

		// 等待处理完成
		time.Sleep(200 * time.Millisecond)

		// 获取统计信息
		stats := ssql.Stream().GetStats()

		t.Logf("输入 %d 条数据，耗时: %v", inputCount, inputDuration)
		t.Logf("输入速率: %.2f ops/sec", float64(inputCount)/inputDuration.Seconds())
		t.Logf("统计信息: %+v", stats)
		t.Logf("Sink计数: sink1=%d, sink2=%d, sink3=%d",
			atomic.LoadInt64(&sink1Count),
			atomic.LoadInt64(&sink2Count),
			atomic.LoadInt64(&sink3Count))

		// 验证性能指标
		if inputDuration > 100*time.Millisecond {
			t.Errorf("输入耗时过长: %v", inputDuration)
		}

		if stats["dropped_count"] > int64(inputCount/10) {
			t.Errorf("丢弃数据过多: %d", stats["dropped_count"])
		}

		// 验证非阻塞性
		throughput := float64(inputCount) / inputDuration.Seconds()
		if throughput < 5000 { // 最低5K ops/sec
			t.Errorf("吞吐量过低: %.2f ops/sec", throughput)
		}
	})

	t.Run("窗口聚合优化测试", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		sql := "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('100ms')"
		err := ssql.Execute(sql)
		if err != nil {
			t.Fatalf("SQL执行失败: %v", err)
		}

		var resultCount int64
		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&resultCount, 1)
			// 模拟慢sink
			time.Sleep(5 * time.Millisecond)
		})

		// 快速输入数据
		testData := generateTestData(5)
		inputCount := 500

		start := time.Now()
		for i := 0; i < inputCount; i++ {
			ssql.AddData(testData[i%len(testData)])
		}
		inputDuration := time.Since(start)

		// 等待窗口触发
		time.Sleep(200 * time.Millisecond)

		stats := ssql.Stream().GetStats()
		results := atomic.LoadInt64(&resultCount)

		t.Logf("窗口聚合测试 - 输入: %d, 耗时: %v", inputCount, inputDuration)
		t.Logf("输入速率: %.2f ops/sec", float64(inputCount)/inputDuration.Seconds())
		t.Logf("生成窗口结果: %d", results)
		t.Logf("统计信息: %+v", stats)

		// 验证窗口功能正常
		if results == 0 {
			t.Error("窗口聚合未生成任何结果")
		}

		// 验证非阻塞性
		if inputDuration > 100*time.Millisecond {
			t.Errorf("窗口模式输入耗时过长: %v", inputDuration)
		}
	})

	t.Run("高负载压力测试", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		sql := "SELECT deviceId, temperature FROM stream"
		err := ssql.Execute(sql)
		if err != nil {
			t.Fatalf("SQL执行失败: %v", err)
		}

		// 添加会阻塞的sink
		ssql.Stream().AddSink(func(result interface{}) {
			time.Sleep(10 * time.Millisecond) // 故意阻塞
		})

		// 高频输入
		testData := generateTestData(3)
		inputCount := 2000 // 增加输入量

		start := time.Now()
		for i := 0; i < inputCount; i++ {
			ssql.AddData(testData[i%len(testData)])
		}
		inputDuration := time.Since(start)

		// 短暂等待
		time.Sleep(50 * time.Millisecond)

		stats := ssql.Stream().GetStats()

		t.Logf("高负载测试 - 输入: %d, 耗时: %v", inputCount, inputDuration)
		t.Logf("输入速率: %.2f ops/sec", float64(inputCount)/inputDuration.Seconds())
		t.Logf("统计信息: %+v", stats)

		// 即使有阻塞的sink，系统也应该保持响应
		if inputDuration > 200*time.Millisecond {
			t.Errorf("高负载下输入耗时过长: %v", inputDuration)
		}

		// 验证系统没有完全阻塞
		throughput := float64(inputCount) / inputDuration.Seconds()
		if throughput < 1000 {
			t.Errorf("高负载下吞吐量过低: %.2f ops/sec", throughput)
		}
	})
}

// TestStreamOptimizationsImproved 测试Stream改进后的优化效果
func TestStreamOptimizationsImproved(t *testing.T) {
	t.Run("改进的非阻塞性能测试", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		// 使用简单查询测试
		sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"
		err := ssql.Execute(sql)
		if err != nil {
			t.Fatalf("SQL执行失败: %v", err)
		}

		// 添加多个sink模拟真实场景
		var sink1Count, sink2Count int64

		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&sink1Count, 1)
			time.Sleep(2 * time.Millisecond) // 模拟处理延迟
		})

		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&sink2Count, 1)
			time.Sleep(1 * time.Millisecond) // 模拟较快的sink
		})

		// 重置统计信息
		ssql.Stream().ResetStats()

		// 快速输入大量数据
		testData := generateTestData(10)
		inputCount := 2000 // 增加输入量测试

		start := time.Now()
		for i := 0; i < inputCount; i++ {
			ssql.AddData(testData[i%len(testData)])
		}
		inputDuration := time.Since(start)

		// 等待处理完成
		time.Sleep(300 * time.Millisecond)

		// 获取统计信息
		stats := ssql.Stream().GetStats()

		t.Logf("改进测试 - 输入 %d 条数据，耗时: %v", inputCount, inputDuration)
		t.Logf("输入速率: %.2f ops/sec", float64(inputCount)/inputDuration.Seconds())
		t.Logf("统计信息: %+v", stats)
		t.Logf("Sink计数: sink1=%d, sink2=%d",
			atomic.LoadInt64(&sink1Count),
			atomic.LoadInt64(&sink2Count))

		// 计算处理效率
		inputTotal := stats["input_count"]
		outputTotal := stats["output_count"]
		droppedTotal := stats["dropped_count"]

		if inputTotal > 0 {
			processRate := float64(outputTotal) / float64(inputTotal) * 100
			dropRate := float64(droppedTotal) / float64(inputTotal) * 100

			t.Logf("处理效率: %.2f%%, 丢弃率: %.2f%%", processRate, dropRate)

			// 验证改进效果
			if dropRate > 50 { // 丢弃率不应超过50%
				t.Errorf("丢弃率过高: %.2f%%", dropRate)
			}
		}

		// 验证非阻塞性
		throughput := float64(inputCount) / inputDuration.Seconds()
		if throughput < 10000 { // 期望至少10K ops/sec
			t.Logf("注意: 吞吐量较低: %.2f ops/sec", throughput)
		}

		// 验证系统没有完全阻塞
		if inputDuration > 500*time.Millisecond {
			t.Errorf("输入耗时过长: %v", inputDuration)
		}
	})

	t.Run("超高负载压力测试", func(t *testing.T) {
		ssql := New()
		defer ssql.Stop()

		sql := "SELECT deviceId, temperature FROM stream"
		err := ssql.Execute(sql)
		if err != nil {
			t.Fatalf("SQL执行失败: %v", err)
		}

		// 添加会严重阻塞的sink
		var sinkCount int64
		ssql.Stream().AddSink(func(result interface{}) {
			atomic.AddInt64(&sinkCount, 1)
			time.Sleep(20 * time.Millisecond) // 故意制造严重阻塞
		})

		// 重置统计
		ssql.Stream().ResetStats()

		// 超高频输入
		testData := generateTestData(3)
		inputCount := 5000 // 大幅增加输入量

		start := time.Now()
		for i := 0; i < inputCount; i++ {
			ssql.AddData(testData[i%len(testData)])
		}
		inputDuration := time.Since(start)

		// 短暂等待
		time.Sleep(100 * time.Millisecond)

		stats := ssql.Stream().GetStats()
		sinks := atomic.LoadInt64(&sinkCount)

		t.Logf("超高负载测试 - 输入: %d, 耗时: %v", inputCount, inputDuration)
		t.Logf("输入速率: %.2f ops/sec", float64(inputCount)/inputDuration.Seconds())
		t.Logf("Sink处理数: %d", sinks)
		t.Logf("统计信息: %+v", stats)

		// 即使有严重阻塞的sink，系统仍应保持响应
		throughput := float64(inputCount) / inputDuration.Seconds()

		// 验证系统没有完全卡死
		if inputDuration > 1*time.Second {
			t.Errorf("超高负载下输入耗时过长: %v", inputDuration)
		}

		if throughput < 5000 {
			t.Logf("注意: 超高负载下吞吐量: %.2f ops/sec", throughput)
		} else {
			t.Logf("优秀: 超高负载下仍保持高吞吐量: %.2f ops/sec", throughput)
		}

		// 验证背压控制有效
		inputTotal := stats["input_count"]
		droppedTotal := stats["dropped_count"]
		if inputTotal > 0 {
			dropRate := float64(droppedTotal) / float64(inputTotal) * 100
			t.Logf("背压控制 - 丢弃率: %.2f%%", dropRate)
		}
	})

	t.Run("性能对比测试", func(t *testing.T) {
		// 测试不同负载下的性能表现
		testCases := []struct {
			name        string
			inputCount  int
			sinkDelay   time.Duration
			maxDropRate float64
		}{
			{"轻负载", 500, 1 * time.Millisecond, 10.0},
			{"中负载", 1500, 3 * time.Millisecond, 25.0},
			{"重负载", 3000, 5 * time.Millisecond, 40.0},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ssql := New()
				defer ssql.Stop()

				sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 15"
				err := ssql.Execute(sql)
				if err != nil {
					t.Fatalf("SQL执行失败: %v", err)
				}

				var sinkCount int64
				ssql.Stream().AddSink(func(result interface{}) {
					atomic.AddInt64(&sinkCount, 1)
					time.Sleep(tc.sinkDelay)
				})

				ssql.Stream().ResetStats()

				testData := generateTestData(5)
				start := time.Now()

				for i := 0; i < tc.inputCount; i++ {
					ssql.AddData(testData[i%len(testData)])
				}

				inputDuration := time.Since(start)
				time.Sleep(150 * time.Millisecond)

				stats := ssql.Stream().GetStats()
				sinks := atomic.LoadInt64(&sinkCount)

				throughput := float64(tc.inputCount) / inputDuration.Seconds()

				inputTotal := stats["input_count"]
				outputTotal := stats["output_count"]
				droppedTotal := stats["dropped_count"]

				var processRate, dropRate float64
				if inputTotal > 0 {
					processRate = float64(outputTotal) / float64(inputTotal) * 100
					dropRate = float64(droppedTotal) / float64(inputTotal) * 100
				}

				t.Logf("%s结果:", tc.name)
				t.Logf("  输入速率: %.2f ops/sec", throughput)
				t.Logf("  处理效率: %.2f%%", processRate)
				t.Logf("  丢弃率: %.2f%%", dropRate)
				t.Logf("  Sink处理: %d", sinks)
				t.Logf("  统计: %+v", stats)

				// 验证性能标准
				if dropRate > tc.maxDropRate {
					t.Errorf("%s: 丢弃率过高 %.2f%% > %.2f%%", tc.name, dropRate, tc.maxDropRate)
				}

				if throughput < 1000 {
					t.Errorf("%s: 吞吐量过低 %.2f ops/sec", tc.name, throughput)
				}
			})
		}
	})
}

// TestMassiveBufferOptimization 测试超大缓冲区优化效果
func TestMassiveBufferOptimization(t *testing.T) {
	t.Run("标准配置vs高性能配置对比", func(t *testing.T) {
		// 测试标准配置
		t.Run("标准配置", func(t *testing.T) {
			ssql := New()
			defer ssql.Stop()

			sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"
			err := ssql.Execute(sql)
			if err != nil {
				t.Fatalf("SQL执行失败: %v", err)
			}

			var sinkCount int64
			ssql.Stream().AddSink(func(result interface{}) {
				atomic.AddInt64(&sinkCount, 1)
				time.Sleep(1 * time.Millisecond) // 模拟处理延迟
			})

			ssql.Stream().ResetStats()

			// 大量数据输入
			testData := generateTestData(5)
			inputCount := 10000

			start := time.Now()
			for i := 0; i < inputCount; i++ {
				ssql.AddData(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			time.Sleep(300 * time.Millisecond)

			detailedStats := ssql.Stream().GetDetailedStats()
			sinks := atomic.LoadInt64(&sinkCount)

			t.Logf("标准配置结果:")
			t.Logf("  输入速率: %.2f ops/sec", float64(inputCount)/inputDuration.Seconds())
			t.Logf("  处理效率: %.2f%%", detailedStats["process_rate"])
			t.Logf("  丢弃率: %.2f%%", detailedStats["drop_rate"])
			t.Logf("  性能等级: %s", detailedStats["performance_level"])
			t.Logf("  数据通道使用率: %.2f%%", detailedStats["data_chan_usage"])
			t.Logf("  Sink处理数: %d", sinks)
			t.Logf("  详细统计: %+v", detailedStats["basic_stats"])
		})

		// 测试高性能配置
		t.Run("高性能配置", func(t *testing.T) {
			// 直接创建高性能Stream（绕过StreamSQL包装）
			config := types.Config{
				SimpleFields: []string{"deviceId", "temperature"},
			}

			stream, err := stream.NewHighPerformanceStream(config)
			if err != nil {
				t.Fatalf("高性能Stream创建失败: %v", err)
			}
			defer stream.Stop()

			err = stream.RegisterFilter("temperature > 20")
			if err != nil {
				t.Fatalf("过滤器注册失败: %v", err)
			}

			stream.Start()

			var sinkCount int64
			stream.AddSink(func(result interface{}) {
				atomic.AddInt64(&sinkCount, 1)
				time.Sleep(1 * time.Millisecond) // 模拟处理延迟
			})

			stream.ResetStats()

			// 大量数据输入
			testData := generateTestData(5)
			inputCount := 10000

			start := time.Now()
			for i := 0; i < inputCount; i++ {
				stream.AddData(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			time.Sleep(300 * time.Millisecond)

			detailedStats := stream.GetDetailedStats()
			sinks := atomic.LoadInt64(&sinkCount)

			t.Logf("高性能配置结果:")
			t.Logf("  输入速率: %.2f ops/sec", float64(inputCount)/inputDuration.Seconds())
			t.Logf("  处理效率: %.2f%%", detailedStats["process_rate"])
			t.Logf("  丢弃率: %.2f%%", detailedStats["drop_rate"])
			t.Logf("  性能等级: %s", detailedStats["performance_level"])
			t.Logf("  数据通道使用率: %.2f%%", detailedStats["data_chan_usage"])
			t.Logf("  Sink处理数: %d", sinks)
			t.Logf("  详细统计: %+v", detailedStats["basic_stats"])
		})
	})

	t.Run("超高负载抗压测试", func(t *testing.T) {
		// 使用最大缓冲区配置测试极限情况
		config := types.Config{
			SimpleFields: []string{"deviceId", "temperature"},
		}

		// 自定义超大缓冲区：100K输入，100K结果，2K sink池
		stream, err := stream.NewStreamWithBuffers(config, 100000, 100000, 2000)
		if err != nil {
			t.Fatalf("超大缓冲区Stream创建失败: %v", err)
		}
		defer stream.Stop()

		err = stream.RegisterFilter("temperature > 15")
		if err != nil {
			t.Fatalf("过滤器注册失败: %v", err)
		}

		stream.Start()

		// 添加多个慢速sink模拟极端场景
		var sink1Count, sink2Count, sink3Count int64

		stream.AddSink(func(result interface{}) {
			atomic.AddInt64(&sink1Count, 1)
			time.Sleep(3 * time.Millisecond) // 慢速sink
		})

		stream.AddSink(func(result interface{}) {
			atomic.AddInt64(&sink2Count, 1)
			time.Sleep(5 * time.Millisecond) // 更慢的sink
		})

		stream.AddSink(func(result interface{}) {
			atomic.AddInt64(&sink3Count, 1)
			time.Sleep(1 * time.Millisecond) // 相对快速的sink
		})

		stream.ResetStats()

		// 超大量数据输入
		testData := generateTestData(3)
		inputCount := 50000 // 5万条数据

		t.Logf("开始超高负载测试：输入 %d 条数据", inputCount)

		start := time.Now()
		for i := 0; i < inputCount; i++ {
			stream.AddData(testData[i%len(testData)])

			// 偶尔检查状态，避免测试超时
			if i%10000 == 0 && i > 0 {
				t.Logf("已输入 %d 条数据", i)
			}
		}
		inputDuration := time.Since(start)

		t.Logf("数据输入完成，耗时: %v", inputDuration)

		// 等待处理完成
		time.Sleep(500 * time.Millisecond)

		detailedStats := stream.GetDetailedStats()

		t.Logf("超高负载测试结果:")
		t.Logf("  输入速率: %.2f ops/sec", float64(inputCount)/inputDuration.Seconds())
		t.Logf("  处理效率: %.2f%%", detailedStats["process_rate"])
		t.Logf("  丢弃率: %.2f%%", detailedStats["drop_rate"])
		t.Logf("  性能等级: %s", detailedStats["performance_level"])
		t.Logf("  数据通道使用率: %.2f%%", detailedStats["data_chan_usage"])
		t.Logf("  结果通道使用率: %.2f%%", detailedStats["result_chan_usage"])
		t.Logf("  Sink池使用率: %.2f%%", detailedStats["sink_pool_usage"])

		sinks := []int64{
			atomic.LoadInt64(&sink1Count),
			atomic.LoadInt64(&sink2Count),
			atomic.LoadInt64(&sink3Count),
		}
		t.Logf("  Sink处理数: %v", sinks)
		t.Logf("  详细统计: %+v", detailedStats["basic_stats"])

		// 验证性能指标
		if detailedStats["drop_rate"].(float64) > 30 {
			t.Logf("注意: 丢弃率较高 %.2f%%，但系统未阻塞", detailedStats["drop_rate"])
		}

		throughput := float64(inputCount) / inputDuration.Seconds()
		if throughput < 50000 { // 期望至少5万ops/sec
			t.Logf("注意: 吞吐量 %.2f ops/sec，但在超高负载下属于正常范围", throughput)
		} else {
			t.Logf("优秀: 超高负载下仍保持高吞吐量 %.2f ops/sec", throughput)
		}

		// 验证系统没有完全阻塞
		if inputDuration > 2*time.Second {
			t.Errorf("超高负载下输入耗时过长: %v", inputDuration)
		}

		// 验证缓冲区配置有效
		basicStats := detailedStats["basic_stats"].(map[string]int64)
		t.Logf("缓冲区配置验证:")
		t.Logf("  数据通道: %d/%d", basicStats["data_chan_len"], basicStats["data_chan_cap"])
		t.Logf("  结果通道: %d/%d", basicStats["result_chan_len"], basicStats["result_chan_cap"])
		t.Logf("  Sink池: %d/%d", basicStats["sink_pool_len"], basicStats["sink_pool_cap"])
	})
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
			ssql.Stream().AddSink(func(result interface{}) {
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
				ssql.AddData(testData[i%len(testData)])
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

// BenchmarkOptimizedPerformance 优化后的单项性能测试
func BenchmarkOptimizedPerformance(b *testing.B) {
	b.Run("纯输入性能-高性能配置", func(b *testing.B) {
		ssql := New(WithHighPerformance())
		defer ssql.Stop()

		sql := "SELECT deviceId FROM stream"
		err := ssql.Execute(sql)
		if err != nil {
			b.Fatal(err)
		}

		// 消费者防止阻塞
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
			ssql.AddData(data)
		}

		b.StopTimer()
		duration := time.Since(start)
		throughput := float64(b.N) / duration.Seconds()

		// 获取统计
		detailedStats := ssql.Stream().GetDetailedStats()
		dropRate := detailedStats["drop_rate"].(float64)

		b.ReportMetric(throughput, "pure_input_ops/sec")
		b.ReportMetric(dropRate, "drop_rate_%")

		b.Logf("高性能配置下丢弃率: %.2f%%", dropRate)
	})

	b.Run("窗口聚合性能-超大缓冲", func(b *testing.B) {
		ssql := New(WithBufferSizes(50000, 50000, 1500))
		defer ssql.Stop()

		sql := "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('100ms')"
		err := ssql.Execute(sql)
		if err != nil {
			b.Fatal(err)
		}

		var resultCount int64

		ssql.Stream().AddSink(func(result interface{}) {
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

		testData := generateTestData(2)

		b.ResetTimer()

		start := time.Now()
		for i := 0; i < b.N; i++ {
			ssql.AddData(testData[i%len(testData)])
		}
		inputDuration := time.Since(start)

		b.StopTimer()

		time.Sleep(200 * time.Millisecond)
		cancel()

		results := atomic.LoadInt64(&resultCount)
		detailedStats := ssql.Stream().GetDetailedStats()

		inputThroughput := float64(b.N) / inputDuration.Seconds()
		processRate := detailedStats["process_rate"].(float64)
		dropRate := detailedStats["drop_rate"].(float64)

		b.ReportMetric(inputThroughput, "input_ops/sec")
		b.ReportMetric(processRate, "process_rate_%")
		b.ReportMetric(dropRate, "drop_rate_%")
		b.ReportMetric(float64(results), "window_results")

		b.Logf("窗口聚合 - 处理效率: %.2f%%, 丢弃率: %.2f%%", processRate, dropRate)
	})
}

// TestMemoryUsageComparison 内存使用对比测试
func TestMemoryUsageComparison(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func() *Streamsql
		description string
		expectedMB  float64 // 预期内存使用(MB)
	}{
		{
			name: "轻量配置",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(5000, 5000, 250))
			},
			description: "5K数据 + 5K结果 + 250sink池",
			expectedMB:  1.0, // 预期约1MB
		},
		{
			name: "默认配置（中等场景）",
			setupFunc: func() *Streamsql {
				return New()
			},
			description: "20K数据 + 20K结果 + 800sink池",
			expectedMB:  3.0, // 预期约3MB
		},
		{
			name: "高性能配置",
			setupFunc: func() *Streamsql {
				return New(WithHighPerformance())
			},
			description: "50K数据 + 50K结果 + 1Ksinki池",
			expectedMB:  12.0, // 预期约12MB
		},
		{
			name: "超大缓冲配置",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(100000, 100000, 2000))
			},
			description: "100K数据缓冲，100K结果缓冲，2Ksinki池",
			expectedMB:  25.0, // 预期约25MB
		},
	}

	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 获取开始内存
			var startMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&startMem)

			// 创建Stream
			ssql := tt.setupFunc()
			err := ssql.Execute(sql)
			if err != nil {
				t.Fatalf("SQL执行失败: %v", err)
			}

			// 等待初始化完成
			time.Sleep(10 * time.Millisecond)

			// 获取创建后内存
			var afterCreateMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&afterCreateMem)

			createUsage := float64(afterCreateMem.Alloc-startMem.Alloc) / 1024 / 1024

			// 添加一些数据测试内存增长
			testData := generateTestData(3)
			for i := 0; i < 1000; i++ {
				ssql.AddData(testData[i%len(testData)])
			}

			time.Sleep(50 * time.Millisecond)

			// 获取使用后内存
			var afterUseMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&afterUseMem)

			totalUsage := float64(afterUseMem.Alloc-startMem.Alloc) / 1024 / 1024

			// 获取详细统计
			detailedStats := ssql.Stream().GetDetailedStats()
			basicStats := detailedStats["basic_stats"].(map[string]int64)

			ssql.Stop()

			t.Logf("=== %s 内存使用分析 ===", tt.name)
			t.Logf("配置: %s", tt.description)
			t.Logf("创建开销: %.2f MB", createUsage)
			t.Logf("总内存使用: %.2f MB", totalUsage)
			t.Logf("缓冲区配置:")
			t.Logf("  数据通道: %d", basicStats["data_chan_cap"])
			t.Logf("  结果通道: %d", basicStats["result_chan_cap"])
			t.Logf("  Sink池: %d", basicStats["sink_pool_cap"])

			// 计算理论内存使用 (每个接口槽位约24字节)
			dataChanMem := float64(basicStats["data_chan_cap"]) * 24 / 1024 / 1024
			resultChanMem := float64(basicStats["result_chan_cap"]) * 24 / 1024 / 1024
			sinkPoolMem := float64(basicStats["sink_pool_cap"]) * 8 / 1024 / 1024 // 函数指针

			theoreticalMem := dataChanMem + resultChanMem + sinkPoolMem

			t.Logf("理论内存分配:")
			t.Logf("  数据通道: %.2f MB", dataChanMem)
			t.Logf("  结果通道: %.2f MB", resultChanMem)
			t.Logf("  Sink池: %.2f MB", sinkPoolMem)
			t.Logf("  理论总计: %.2f MB", theoreticalMem)

			// 内存效率分析
			if totalUsage > tt.expectedMB*2 {
				t.Logf("警告: 内存使用超过预期2倍 (%.2f MB > %.2f MB)", totalUsage, tt.expectedMB*2)
			} else if totalUsage > tt.expectedMB*1.5 {
				t.Logf("注意: 内存使用超过预期50%% (%.2f MB > %.2f MB)", totalUsage, tt.expectedMB*1.5)
			} else {
				t.Logf("✓ 内存使用在合理范围内 (%.2f MB)", totalUsage)
			}
		})
	}
}

// TestResourceCostAnalysis 资源成本分析测试
func TestResourceCostAnalysis(t *testing.T) {
	scenarios := []struct {
		name         string
		setup        func() *Streamsql
		workload     int
		description  string
		costCategory string
	}{
		{
			name:         "轻量场景",
			setup:        func() *Streamsql { return New(WithBufferSizes(5000, 5000, 250)) },
			workload:     1000,
			description:  "资源受限环境，轻量业务",
			costCategory: "低成本",
		},
		{
			name:         "中等场景（默认）",
			setup:        func() *Streamsql { return New() },
			workload:     10000,
			description:  "生产环境，正常峰值",
			costCategory: "中等成本",
		},
		{
			name:         "高负载场景",
			setup:        func() *Streamsql { return New(WithHighPerformance()) },
			workload:     50000,
			description:  "高并发，极端负载",
			costCategory: "高成本",
		},
	}

	sql := "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('100ms')"

	t.Log("=== 资源成本对比分析 ===")

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// 内存监控
			var beforeMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&beforeMem)

			// CPU和goroutine监控
			beforeGoroutines := runtime.NumGoroutine()
			start := time.Now()

			// 创建实例
			ssql := scenario.setup()
			err := ssql.Execute(sql)
			if err != nil {
				t.Fatalf("SQL执行失败: %v", err)
			}

			// 运行负载
			testData := generateTestData(5)
			for i := 0; i < scenario.workload; i++ {
				ssql.AddData(testData[i%len(testData)])
			}

			// 等待处理完成
			time.Sleep(200 * time.Millisecond)

			// 获取统计
			detailedStats := ssql.Stream().GetDetailedStats()
			basicStats := detailedStats["basic_stats"].(map[string]int64)

			// 资源使用测量
			duration := time.Since(start)
			var afterMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&afterMem)
			afterGoroutines := runtime.NumGoroutine()

			memUsage := float64(afterMem.Alloc-beforeMem.Alloc) / 1024 / 1024
			goroutineIncrease := afterGoroutines - beforeGoroutines

			ssql.Stop()

			// 成本分析报告
			t.Logf("--- %s 成本分析 ---", scenario.name)
			t.Logf("场景: %s (%s)", scenario.description, scenario.costCategory)
			t.Logf("负载: %d 条数据", scenario.workload)
			t.Logf("执行时间: %v", duration)

			t.Logf("资源消耗:")
			t.Logf("  内存: %.2f MB", memUsage)
			t.Logf("  Goroutine增加: %d", goroutineIncrease)
			t.Logf("  处理速率: %.2f ops/sec", float64(scenario.workload)/duration.Seconds())

			t.Logf("缓冲区开销:")
			t.Logf("  数据通道容量: %d", basicStats["data_chan_cap"])
			t.Logf("  结果通道容量: %d", basicStats["result_chan_cap"])
			t.Logf("  Sink池容量: %d", basicStats["sink_pool_cap"])

			t.Logf("性能指标:")
			t.Logf("  处理效率: %.2f%%", detailedStats["process_rate"])
			t.Logf("  丢弃率: %.2f%%", detailedStats["drop_rate"])
			t.Logf("  性能等级: %s", detailedStats["performance_level"])

			// 成本效益分析
			throughput := float64(scenario.workload) / duration.Seconds()
			memEfficiency := throughput / memUsage // ops/sec per MB

			t.Logf("成本效益:")
			t.Logf("  内存效率: %.2f ops/sec/MB", memEfficiency)

			// 推荐使用场景
			switch scenario.costCategory {
			case "低成本":
				if throughput > 5000 {
					t.Logf("✓ 推荐: 适合日常业务、开发测试、资源受限环境")
				}
			case "中等成本":
				if throughput > 20000 {
					t.Logf("✓ 推荐: 适合生产环境、中等负载、平衡性能和成本")
				}
			case "高成本":
				if throughput > 100000 {
					t.Logf("✓ 推荐: 适合极高负载、关键业务、性能优先场景")
				} else {
					t.Logf("⚠ 注意: 高成本但吞吐量未达到预期，可能配置过度")
				}
			}
		})
	}

	// 配置选择建议
	t.Log("\n=== 配置选择建议 ===")
	t.Log("1. 默认配置: 适合大多数场景，内存占用低（~2MB），goroutine开销小")
	t.Log("2. 中等配置: 适合中等负载，内存占用中等（~5MB），平衡性能和成本")
	t.Log("3. 高性能配置: 适合极高负载，内存占用高（~12MB），最大化吞吐量")
	t.Log("4. 自定义配置: 根据具体业务需求精确调优，避免资源浪费")
}

// TestHighPerformanceCostAnalysis 高性能模式代价深度分析
func TestHighPerformanceCostAnalysis(t *testing.T) {
	t.Log("=== 高性能模式 vs 默认配置深度对比 ===")

	configs := []struct {
		name     string
		setup    func() *Streamsql
		category string
	}{
		{
			name:     "默认配置",
			setup:    func() *Streamsql { return New() },
			category: "基准",
		},
		{
			name:     "高性能配置",
			setup:    func() *Streamsql { return New(WithHighPerformance()) },
			category: "优化",
		},
	}

	sql := "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('50ms')"
	workload := 10000

	var results []map[string]interface{}

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			// 详细资源监控
			var beforeMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&beforeMem)
			beforeGoroutines := runtime.NumGoroutine()

			// 创建并启动Stream
			ssql := config.setup()
			err := ssql.Execute(sql)
			if err != nil {
				t.Fatalf("执行失败: %v", err)
			}

			// 等待完全初始化
			time.Sleep(20 * time.Millisecond)

			// 测量初始化开销
			var afterInitMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&afterInitMem)
			afterInitGoroutines := runtime.NumGoroutine()

			initMemCost := float64(afterInitMem.Alloc-beforeMem.Alloc) / 1024 / 1024
			initGoroutineCost := afterInitGoroutines - beforeGoroutines

			// 运行负载测试
			testData := generateTestData(3)
			start := time.Now()

			for i := 0; i < workload; i++ {
				ssql.AddData(testData[i%len(testData)])
			}

			inputDuration := time.Since(start)
			time.Sleep(100 * time.Millisecond) // 等待处理完成

			// 最终资源测量
			var finalMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&finalMem)
			finalGoroutines := runtime.NumGoroutine()

			totalMemUsage := float64(finalMem.Alloc-beforeMem.Alloc) / 1024 / 1024
			runtimeMemUsage := totalMemUsage - initMemCost

			// 获取详细统计
			detailedStats := ssql.Stream().GetDetailedStats()
			basicStats := detailedStats["basic_stats"].(map[string]int64)

			ssql.Stop()

			// 分析结果
			result := map[string]interface{}{
				"name":               config.name,
				"category":           config.category,
				"init_memory_mb":     initMemCost,
				"runtime_memory_mb":  runtimeMemUsage,
				"total_memory_mb":    totalMemUsage,
				"init_goroutines":    initGoroutineCost,
				"total_goroutines":   finalGoroutines - beforeGoroutines,
				"input_duration_ms":  float64(inputDuration.Nanoseconds()) / 1e6,
				"throughput_ops_sec": float64(workload) / inputDuration.Seconds(),
				"data_chan_cap":      basicStats["data_chan_cap"],
				"result_chan_cap":    basicStats["result_chan_cap"],
				"sink_pool_cap":      basicStats["sink_pool_cap"],
				"process_rate":       detailedStats["process_rate"],
				"drop_rate":          detailedStats["drop_rate"],
				"performance_level":  detailedStats["performance_level"],
			}

			results = append(results, result)

			// 详细报告
			t.Logf("=== %s 详细分析 ===", config.name)
			t.Logf("初始化开销:")
			t.Logf("  内存: %.2f MB", initMemCost)
			t.Logf("  Goroutine: %d 个", initGoroutineCost)

			t.Logf("运行时开销:")
			t.Logf("  额外内存: %.2f MB", runtimeMemUsage)
			t.Logf("  总内存: %.2f MB", totalMemUsage)
			t.Logf("  总Goroutine: %d 个", finalGoroutines-beforeGoroutines)

			t.Logf("性能表现:")
			t.Logf("  输入耗时: %.2f ms", float64(inputDuration.Nanoseconds())/1e6)
			t.Logf("  吞吐量: %.2f ops/sec", float64(workload)/inputDuration.Seconds())
			t.Logf("  处理效率: %.2f%%", detailedStats["process_rate"])
			t.Logf("  丢弃率: %.2f%%", detailedStats["drop_rate"])

			t.Logf("缓冲区配置:")
			t.Logf("  数据通道: %d", basicStats["data_chan_cap"])
			t.Logf("  结果通道: %d", basicStats["result_chan_cap"])
			t.Logf("  Sink池: %d", basicStats["sink_pool_cap"])
		})
	}

	// 对比分析
	if len(results) == 2 {
		defaultResult := results[0]
		highPerfResult := results[1]

		t.Log("\n=== 对比分析总结 ===")

		// 内存开销对比
		memMultiplier := highPerfResult["total_memory_mb"].(float64) / defaultResult["total_memory_mb"].(float64)
		t.Logf("内存开销倍数: %.1fx (%.2f MB vs %.2f MB)",
			memMultiplier,
			highPerfResult["total_memory_mb"],
			defaultResult["total_memory_mb"])

		// 性能提升对比
		perfMultiplier := highPerfResult["throughput_ops_sec"].(float64) / defaultResult["throughput_ops_sec"].(float64)
		t.Logf("性能提升倍数: %.1fx (%.0f ops/sec vs %.0f ops/sec)",
			perfMultiplier,
			highPerfResult["throughput_ops_sec"],
			defaultResult["throughput_ops_sec"])

		// 缓冲区容量对比
		dataCapMultiplier := float64(highPerfResult["data_chan_cap"].(int64)) / float64(defaultResult["data_chan_cap"].(int64))
		t.Logf("缓冲区容量倍数: %.1fx", dataCapMultiplier)

		// 成本效益分析
		memEfficiencyDefault := defaultResult["throughput_ops_sec"].(float64) / defaultResult["total_memory_mb"].(float64)
		memEfficiencyHighPerf := highPerfResult["throughput_ops_sec"].(float64) / highPerfResult["total_memory_mb"].(float64)

		t.Logf("内存效率对比:")
		t.Logf("  默认配置: %.0f ops/sec/MB", memEfficiencyDefault)
		t.Logf("  高性能配置: %.0f ops/sec/MB", memEfficiencyHighPerf)
		t.Logf("  效率比: %.2fx", memEfficiencyHighPerf/memEfficiencyDefault)

		// 代价分析
		t.Log("\n=== 高性能模式代价分析 ===")
		t.Logf("✓ 性能收益: %.1fx 吞吐量提升", perfMultiplier)
		t.Logf("✗ 内存代价: %.1fx 内存消耗增长", memMultiplier)
		t.Logf("✗ 缓冲区代价: %.1fx 缓冲区容量增长", dataCapMultiplier)

		if memMultiplier < perfMultiplier {
			t.Log("✓ 结论: 高性能模式性价比较高，内存增长小于性能提升")
		} else {
			t.Log("⚠ 结论: 高性能模式需要权衡，内存增长超过性能提升比例")
		}
	}
}

// TestLightweightVsDefaultPerformanceAnalysis 专门分析轻量配置vs默认配置性能差异的测试
func TestLightweightVsDefaultPerformanceAnalysis(t *testing.T) {
	configs := []struct {
		name         string
		setupFunc    func() *Streamsql
		description  string
		expectedPerf string
	}{
		{
			name: "轻量配置(5K)",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(5000, 5000, 250))
			},
			description:  "5K数据缓冲，5K结果缓冲，250sink池",
			expectedPerf: "高吞吐，低内存",
		},
		{
			name: "默认配置(20K)",
			setupFunc: func() *Streamsql {
				return New()
			},
			description:  "20K数据缓冲，20K结果缓冲，800sink池",
			expectedPerf: "平衡性能",
		},
		{
			name: "中等配置(10K)",
			setupFunc: func() *Streamsql {
				return New(WithBufferSizes(10000, 10000, 400))
			},
			description:  "10K数据缓冲，10K结果缓冲，400sink池",
			expectedPerf: "介于两者之间",
		},
	}

	sql := "SELECT deviceId, temperature FROM stream WHERE temperature > 20"

	t.Log("=== 轻量配置 vs 默认配置深度对比分析 ===")

	var results []map[string]interface{}

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			// 内存和性能监控
			var beforeMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&beforeMem)

			ssql := config.setupFunc()
			defer ssql.Stop()

			err := ssql.Execute(sql)
			if err != nil {
				t.Fatalf("SQL执行失败: %v", err)
			}

			// 测量初始化后内存
			var afterInitMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&afterInitMem)
			initMemory := float64(afterInitMem.Alloc-beforeMem.Alloc) / 1024 / 1024

			var resultCount int64
			ssql.Stream().AddSink(func(result interface{}) {
				atomic.AddInt64(&resultCount, 1)
			})

			// 消费resultChan防止阻塞
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

			// 重置统计
			ssql.Stream().ResetStats()

			// 执行性能测试
			testData := generateTestData(3)
			iterations := 50000 // 固定迭代次数便于对比

			start := time.Now()
			for i := 0; i < iterations; i++ {
				ssql.AddData(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			// 等待处理完成
			time.Sleep(100 * time.Millisecond)

			// 获取详细统计
			detailedStats := ssql.Stream().GetDetailedStats()
			basicStats := detailedStats["basic_stats"].(map[string]int64)

			// 最终内存测量
			var finalMem runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&finalMem)
			totalMemory := float64(finalMem.Alloc-beforeMem.Alloc) / 1024 / 1024

			// 计算指标
			inputThroughput := float64(iterations) / inputDuration.Seconds()
			processRate := detailedStats["process_rate"].(float64)
			dropRate := detailedStats["drop_rate"].(float64)
			perfLevel := detailedStats["performance_level"].(string)
			dataChanUsage := detailedStats["data_chan_usage"].(float64)
			memEfficiency := inputThroughput / totalMemory // ops/sec per MB

			result := map[string]interface{}{
				"name":              config.name,
				"description":       config.description,
				"data_chan_cap":     basicStats["data_chan_cap"],
				"result_chan_cap":   basicStats["result_chan_cap"],
				"sink_pool_cap":     basicStats["sink_pool_cap"],
				"init_memory_mb":    initMemory,
				"total_memory_mb":   totalMemory,
				"input_throughput":  inputThroughput,
				"process_rate":      processRate,
				"drop_rate":         dropRate,
				"performance_level": perfLevel,
				"data_chan_usage":   dataChanUsage,
				"mem_efficiency":    memEfficiency,
				"input_duration_ms": float64(inputDuration.Nanoseconds()) / 1e6,
			}
			results = append(results, result)

			// 详细报告
			t.Logf("=== %s 分析报告 ===", config.name)
			t.Logf("配置: %s", config.description)
			t.Logf("预期: %s", config.expectedPerf)

			t.Logf("缓冲区配置:")
			t.Logf("  数据通道: %d", basicStats["data_chan_cap"])
			t.Logf("  结果通道: %d", basicStats["result_chan_cap"])
			t.Logf("  Sink池: %d", basicStats["sink_pool_cap"])

			t.Logf("内存使用:")
			t.Logf("  初始化: %.2f MB", initMemory)
			t.Logf("  总计: %.2f MB", totalMemory)

			t.Logf("性能指标:")
			t.Logf("  输入速率: %.0f ops/sec", inputThroughput)
			t.Logf("  输入耗时: %.2f ms", float64(inputDuration.Nanoseconds())/1e6)
			t.Logf("  处理效率: %.2f%%", processRate)
			t.Logf("  丢弃率: %.2f%%", dropRate)
			t.Logf("  性能等级: %s", perfLevel)
			t.Logf("  数据通道使用率: %.1f%%", dataChanUsage)
			t.Logf("  内存效率: %.0f ops/sec/MB", memEfficiency)

			// 性能分析
			if dropRate > 10 {
				t.Logf("⚠ 警告: 丢弃率较高 (%.2f%%)", dropRate)
			} else if dropRate < 1 {
				t.Logf("✓ 优秀: 丢弃率很低 (%.2f%%)", dropRate)
			}

			if dataChanUsage > 80 {
				t.Logf("⚠ 警告: 数据通道使用率过高 (%.1f%%)", dataChanUsage)
			} else if dataChanUsage < 50 {
				t.Logf("✓ 良好: 数据通道使用率适中 (%.1f%%)", dataChanUsage)
			}
		})
	}

	// 对比分析
	if len(results) >= 2 {
		t.Log("\n=== 对比分析结论 ===")

		lightweight := results[0] // 轻量配置
		defaultCfg := results[1]  // 默认配置

		// 性能对比
		perfRatio := lightweight["input_throughput"].(float64) / defaultCfg["input_throughput"].(float64)
		memRatio := lightweight["total_memory_mb"].(float64) / defaultCfg["total_memory_mb"].(float64)
		memEffRatio := lightweight["mem_efficiency"].(float64) / defaultCfg["mem_efficiency"].(float64)

		t.Logf("性能倍数: %.2fx (轻量 %.0f vs 默认 %.0f ops/sec)",
			perfRatio,
			lightweight["input_throughput"].(float64),
			defaultCfg["input_throughput"].(float64))

		t.Logf("内存倍数: %.2fx (轻量 %.2f vs 默认 %.2f MB)",
			memRatio,
			lightweight["total_memory_mb"].(float64),
			defaultCfg["total_memory_mb"].(float64))

		t.Logf("内存效率倍数: %.2fx (轻量 %.0f vs 默认 %.0f ops/sec/MB)",
			memEffRatio,
			lightweight["mem_efficiency"].(float64),
			defaultCfg["mem_efficiency"].(float64))

		// 分析原因
		t.Log("\n=== 轻量配置性能更高的可能原因 ===")

		lightUsage := lightweight["data_chan_usage"].(float64)
		defaultUsage := defaultCfg["data_chan_usage"].(float64)

		t.Logf("1. 缓冲区压力差异:")
		t.Logf("   轻量配置数据通道使用率: %.1f%%", lightUsage)
		t.Logf("   默认配置数据通道使用率: %.1f%%", defaultUsage)

		if lightUsage < defaultUsage {
			t.Log("   → 轻量配置缓冲区压力更小，减少了队列等待时间")
		}

		lightCapacity := lightweight["data_chan_cap"].(int64)
		defaultCapacity := defaultCfg["data_chan_cap"].(int64)
		capacityRatio := float64(defaultCapacity) / float64(lightCapacity)

		t.Logf("2. 缓冲区容量差异:")
		t.Logf("   容量倍数: %.1fx (轻量 %d vs 默认 %d)",
			capacityRatio, lightCapacity, defaultCapacity)
		t.Log("   → 大缓冲区可能导致更多内存分配和GC压力")

		t.Logf("3. 内存分配模式:")
		t.Logf("   轻量配置总内存: %.2f MB", lightweight["total_memory_mb"].(float64))
		t.Logf("   默认配置总内存: %.2f MB", defaultCfg["total_memory_mb"].(float64))
		t.Log("   → 轻量配置内存占用更少，减少GC频率和暂停时间")

		t.Log("\n=== 技术解释 ===")
		t.Log("轻量配置吞吐量更高的核心原因:")
		t.Log("1. **内存局部性更好**: 小缓冲区提高CPU缓存命中率")
		t.Log("2. **GC压力更小**: 减少垃圾回收的暂停时间")
		t.Log("3. **队列效率更高**: 小队列减少锁竞争和等待时间")
		t.Log("4. **资源竞争减少**: 更少的内存分配减少系统调用开销")
		t.Log("5. **适合高频小数据**: 本测试场景正好符合轻量配置的优势区间")

		if perfRatio > 1.1 {
			t.Log("\n✓ 结论: 轻量配置在此场景下确实具有性能优势")
			t.Log("  推荐: 对于高频率、小数据量的场景，优先考虑轻量配置")
		} else {
			t.Log("\n→ 结论: 性能差异不显著，可能存在测试误差")
		}
	}
}

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
			ssql.Stream().AddSink(func(result interface{}) {
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
				ssql.AddData(testData[i%len(testData)])
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
			ssql.Stream().AddSink(func(result interface{}) {
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
				ssql.AddData(testData[i%len(testData)])
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

// BenchmarkStreamSQLOptimized 优化的高性能基准测试
func BenchmarkStreamSQLOptimized(b *testing.B) {
	tests := []struct {
		name      string
		sql       string
		hasWindow bool
		waitTime  time.Duration
	}{
		{
			name:      "HighThroughputFilter",
			sql:       "SELECT deviceId, temperature FROM stream WHERE temperature > 20",
			hasWindow: false,
			waitTime:  10 * time.Millisecond,
		},
		{
			name:      "HighThroughputAggregation",
			sql:       "SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('50ms')",
			hasWindow: true,
			waitTime:  100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			// 使用高性能配置
			ssql := New(WithHighPerformance())
			defer ssql.Stop()

			err := ssql.Execute(tt.sql)
			if err != nil {
				b.Fatalf("SQL执行失败: %v", err)
			}

			var actualResultCount int64

			// 极简的sink，避免任何额外开销
			ssql.Stream().AddSink(func(result interface{}) {
				atomic.AddInt64(&actualResultCount, 1)
			})

			// 异步消费resultChan，避免阻塞
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				for {
					select {
					case <-ssql.Stream().GetResultsChan():
						// 立即丢弃，避免处理开销
					case <-ctx.Done():
						return
					}
				}
			}()

			// 预生成测试数据，避免重复生成开销
			testData := generateTestData(5)

			ssql.Stream().ResetStats()
			b.ResetTimer()

			// 纯粹的性能测试：无限制，无延迟
			start := time.Now()
			for i := 0; i < b.N; i++ {
				ssql.AddData(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			// 最小等待时间
			time.Sleep(tt.waitTime)
			cancel()

			results := atomic.LoadInt64(&actualResultCount)
			stats := ssql.Stream().GetStats()

			// 计算纯输入吞吐量
			inputThroughput := float64(b.N) / inputDuration.Seconds()

			b.ReportMetric(inputThroughput, "optimized_ops/sec")
			b.ReportMetric(float64(results), "actual_results")
			b.ReportMetric(float64(stats["dropped_count"]), "dropped_data")

			// 输出性能数据
			b.Logf("优化测试 - 输入: %d 条, 结果: %d 个", b.N, results)
			b.Logf("优化吞吐量: %.0f ops/sec (%.1f万 ops/sec)", inputThroughput, inputThroughput/10000)

			if dropped := stats["dropped_count"]; dropped > 0 {
				dropRate := float64(dropped) / float64(b.N) * 100
				b.Logf("丢弃率: %.2f%% (%d/%d)", dropRate, dropped, b.N)
			}
		})
	}
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
		ssql.AddData(data)
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
				ssql.Stream().AddSink(func(result interface{}) {
					count := atomic.AddInt64(&resultsReceived, 1)
					if count <= int64(currentBatchSize) {
						resultChan <- true
					}
				})

				// 记录开始时间
				start := time.Now()

				// 输入数据
				for i := 0; i < currentBatchSize; i++ {
					ssql.AddData(testData[i%len(testData)])
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
	ssql.Stream().AddSink(func(result interface{}) {
		atomic.AddInt64(&processedResults, 1)
		lastResultTime = time.Now()
	})

	testData := generateTestData(3)

	b.ResetTimer()
	start := time.Now()

	// 持续输入数据
	for i := 0; i < b.N; i++ {
		ssql.AddData(testData[i%len(testData)])

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
