package streamsql

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkStreamSQL StreamSQL benchmarking
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
			// Use the default configuration
			ssql := New()
			defer ssql.Stop()

			err := ssql.Execute(tt.sql)
			if err != nil {
				b.Fatalf("SQL execution failure: %v", err)
			}

			var resultCount int64
			ssql.AddSink(func(result []map[string]any) {
				atomic.AddInt64(&resultCount, 1)
			})

			// Asynchronous consumption results prevent blockages
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

			// Generate test data
			testData := generateOptimizedTestData(5)
			ssql.Stream().ResetStats()

			b.ResetTimer()

			// Benchmark the test
			start := time.Now()
			for i := 0; i < b.N; i++ {
				ssql.Emit(testData[i%len(testData)])
			}
			inputDuration := time.Since(start)

			b.StopTimer()

			// Wait for processing to complete
			time.Sleep(tt.waitTime)
			cancel()

			// Get statistics
			stats := ssql.Stream().GetStats()
			results := atomic.LoadInt64(&resultCount)

			// Calculate core performance metrics
			throughput := float64(b.N) / inputDuration.Seconds()
			processedCount := stats["output_count"]
			droppedCount := stats["dropped_count"]
			processRate := float64(processedCount) / float64(b.N) * 100
			dropRate := float64(droppedCount) / float64(b.N) * 100

			// Reporting indicators
			b.ReportMetric(throughput, "ops/sec")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dropRate, "drop_rate_%")
			b.ReportMetric(float64(results), "results")

			// Output readable performance reports
			b.Logf("Performance Report - %s:", tt.name)
			b.Logf("Throughput: %.0f ops/sec (%.1f million ops/sec)", throughput, throughput/10000)
			b.Logf("Processing rate: %.1f%%, Disposal rate: %.2f%%", processRate, dropRate)
			b.Logf("Number of results: %d", results)
		})
	}
}

// BenchmarkConfigurationOptimized configuration comparison benchmark
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
				b.Fatalf("SQL execution failure: %v", err)
			}

			var resultCount int64
			ssql.AddSink(func(result []map[string]any) {
				atomic.AddInt64(&resultCount, 1)
			})

			// Asynchronous consumption results
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

			// Get detailed statistics
			detailedStats := ssql.Stream().GetDetailedStats()
			results := atomic.LoadInt64(&resultCount)

			throughput := float64(b.N) / inputDuration.Seconds()
			processRate := detailedStats["process_rate"].(float64)
			dropRate := detailedStats["drop_rate"].(float64)

			b.ReportMetric(throughput, "ops/sec")
			b.ReportMetric(processRate, "process_rate_%")
			b.ReportMetric(dropRate, "drop_rate_%")

			b.Logf("%s Configuration Performance:", config.name)
			b.Logf("Throughput: %.0f ops/sec (%.1f million ops/sec)", throughput, throughput/10000)
			b.Logf("Processing rate: %.1f%%, Disposal rate: %.2f%%", processRate, dropRate)
			b.Logf("Number of results: %d", results)
		})
	}
}

// BenchmarkPureInputOptimized: Optimized pure input performance testing
func BenchmarkPureInputOptimized(b *testing.B) {
	ssql := New(WithHighPerformance())
	defer ssql.Stop()

	sql := "SELECT deviceId FROM stream"
	err := ssql.Execute(sql)
	if err != nil {
		b.Fatal(err)
	}

	// Launch results for consumers
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

	// Pre-generated data
	data := map[string]any{
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
	b.Logf("Pure input performance: %.0f ops/sec (%.1f million ops/sec)", throughput, throughput/10000)
}

// BenchmarkPostAggregationPerformance followed by aggregated performance benchmarking
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
				b.Fatalf("SQL execution failure: %v", err)
			}

			var resultCount int64
			ssql.AddSink(func(result []map[string]any) {
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

			b.Logf("%s Performance:", tt.name)
			b.Logf("Throughput: %.0f ops/sec (%.1f million ops/sec)", throughput, throughput/10000)
			b.Logf("Processing rate: %.1f%%, Disposal rate: %.2f%%", processRate, dropRate)
			b.Logf("Number of results: %d", results)
		})
	}
}

// generateOptimizedTestData generates optimized test data
func generateOptimizedTestData(count int) []map[string]any {
	data := make([]map[string]any, count)
	devices := []string{"device1", "device2", "device3", "device4", "device5"}

	for i := 0; i < count; i++ {
		data[i] = map[string]any{
			"deviceId":    devices[rand.Intn(len(devices))],
			"temperature": 15.0 + rand.Float64()*20, // 15-35 degrees
			"humidity":    30.0 + rand.Float64()*40, // 30-70%
			"timestamp":   time.Now().UnixNano(),
		}
	}
	return data
}

// BenchmarkMemoryEfficiency Memory Efficiency Benchmark
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
				b.Fatalf("SQL execution failure: %v", err)
			}

			var resultCount int64
			ssql.AddSink(func(result []map[string]any) {
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
			b.Logf("%s Configuration Efficiency:", config.name)
			b.Logf("Buffer: Data %d / Result %d / Sink%d",
				basicStats["data_chan_cap"],
				basicStats["result_chan_cap"],
				basicStats["sink_pool_cap"])
			b.Logf("Throughput: %.0f ops/sec, Throughput: %.1f%%, Number of Results: %d", throughput, processRate, results)
			b.Logf("Channel Utilization: Data %.1f%%, results %.1f%%", dataChanUsage, resultChanUsage)
		})
	}
}
