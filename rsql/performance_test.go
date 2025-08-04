package rsql

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestConcurrentAccess 测试并发访问
func TestConcurrentAccess(t *testing.T) {
	const numGoroutines = 10
	const numIterations = 10

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()
			for j := 0; j < numIterations; j++ {
				sql := "SELECT * FROM table"
				parser := NewParser(sql)
				_, err := parser.Parse()
				if err != nil {
					t.Errorf("Goroutine %d iteration %d failed: %v", id, j, err)
				}
			}
		}(i)
	}

	// 等待所有goroutines完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// TestMemoryUsage 测试内存使用情况
func TestMemoryUsage(t *testing.T) {
	// 记录初始内存使用
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	// 测试大量解析操作不会导致内存泄漏
	for i := 0; i < 1000; i++ {
		sql := "SELECT field1, field2, field3 FROM table WHERE condition = 'value'"
		parser := NewParser(sql)
		_, err := parser.Parse()
		if err != nil {
			t.Errorf("Iteration %d failed: %v", i, err)
			break
		}
	}

	// 强制垃圾回收并检查内存使用
	runtime.GC()
	runtime.ReadMemStats(&m2)

	// 检查内存增长是否合理（使用int64避免溢出）
	memoryIncrease := int64(m2.Alloc) - int64(m1.Alloc)
	if memoryIncrease < 0 {
		// 如果是负数，说明内存实际减少了，这是正常的
		t.Logf("Memory usage decreased by %d bytes", -memoryIncrease)
	} else if memoryIncrease > 10*1024*1024 { // 10MB
		t.Errorf("Memory usage increased by %d bytes, which may indicate a memory leak", memoryIncrease)
	} else {
		t.Logf("Memory usage increased by %d bytes (within acceptable range)", memoryIncrease)
	}
}

// TestParserConcurrencySafety 测试解析器并发安全性
func TestParserConcurrencySafety(t *testing.T) {
	const numWorkers = 20
	const numOperations = 50

	var wg sync.WaitGroup
	errorChan := make(chan error, numWorkers*numOperations)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				sql := "SELECT temperature, humidity FROM sensors WHERE deviceId = 'device1'"
				parser := NewParser(sql)
				stmt, err := parser.Parse()
				if err != nil {
					errorChan <- err
					return
				}

				// 测试ToStreamConfig的并发调用
				_, _, err = stmt.ToStreamConfig()
				if err != nil {
					errorChan <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// 检查是否有错误
	for err := range errorChan {
		t.Errorf("Concurrent operation failed: %v", err)
	}
}

// TestLexerConcurrency 测试词法分析器并发安全性
func TestLexerConcurrency(t *testing.T) {
	const numWorkers = 15
	const numOperations = 100

	var wg sync.WaitGroup
	errorChan := make(chan error, numWorkers*numOperations)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				sql := "SELECT * FROM events WHERE status = 'active' AND priority > 5"
				lexer := NewLexer(sql)

				// 读取所有token
				for {
					token := lexer.NextToken()
					if token.Type == TokenEOF {
						break
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// 检查是否有错误
	for err := range errorChan {
		t.Errorf("Concurrent lexer operation failed: %v", err)
	}
}

// TestHighLoadParsing 测试高负载解析
func TestHighLoadParsing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high load test in short mode")
	}

	const numOperations = 10000
	start := time.Now()

	for i := 0; i < numOperations; i++ {
		sql := "SELECT COUNT(*), AVG(temperature), MAX(humidity) FROM sensors GROUP BY deviceId HAVING COUNT(*) > 10"
		parser := NewParser(sql)
		stmt, err := parser.Parse()
		if err != nil {
			t.Errorf("High load parsing failed at iteration %d: %v", i, err)
			break
		}

		// 测试转换为流配置
		_, _, err = stmt.ToStreamConfig()
		if err != nil {
			t.Errorf("High load stream config conversion failed at iteration %d: %v", i, err)
			break
		}
	}

	duration := time.Since(start)
	operationsPerSecond := float64(numOperations) / duration.Seconds()

	t.Logf("Completed %d operations in %v (%.2f ops/sec)", numOperations, duration, operationsPerSecond)

	// 性能基准：应该能够每秒处理至少1000个操作
	if operationsPerSecond < 1000 {
		t.Errorf("Performance below threshold: %.2f ops/sec (expected >= 1000)", operationsPerSecond)
	}
}

// TestMemoryLeakDetection 测试内存泄漏检测
func TestMemoryLeakDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory leak test in short mode")
	}

	// 预热
	for i := 0; i < 100; i++ {
		parser := NewParser("SELECT * FROM table")
		_, _ = parser.Parse()
	}

	// 记录基准内存
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	// 执行大量操作
	for i := 0; i < 5000; i++ {
		sql := "SELECT temperature, humidity, pressure FROM sensors WHERE deviceId = 'device1' AND timestamp > '2023-01-01'"
		parser := NewParser(sql)
		stmt, err := parser.Parse()
		if err != nil {
			t.Errorf("Memory leak test parsing failed: %v", err)
			break
		}
		_, _, _ = stmt.ToStreamConfig()
	}

	// 强制垃圾回收
	runtime.GC()
	runtime.GC() // 两次GC确保清理完成

	// 检查内存使用
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	memoryIncrease := int64(final.Alloc) - int64(baseline.Alloc)
	t.Logf("Memory increase: %d bytes", memoryIncrease)

	// 内存增长不应超过5MB
	if memoryIncrease < 0 {
		// 如果是负数，说明内存实际减少了，这是正常的
		t.Logf("Memory usage decreased by %d bytes (good)", -memoryIncrease)
	} else if memoryIncrease > 5*1024*1024 {
		t.Errorf("Potential memory leak detected: memory increased by %d bytes", memoryIncrease)
	} else {
		t.Logf("Memory increase within acceptable range: %d bytes", memoryIncrease)
	}
}

// TestConcurrentErrorHandling 测试并发错误处理
func TestConcurrentErrorHandling(t *testing.T) {
	const numWorkers = 10
	const numOperations = 50

	var wg sync.WaitGroup
	errorCount := make(chan int, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			errors := 0
			for j := 0; j < numOperations; j++ {
				// 故意使用无效SQL来测试错误处理
				invalidSQL := "SELECT FROM WHERE"
				parser := NewParser(invalidSQL)
				_, err := parser.Parse()
				if err != nil || parser.HasErrors() {
					errors++
				}
			}
			errorCount <- errors
		}(i)
	}

	wg.Wait()
	close(errorCount)

	// 验证所有worker都正确处理了错误
	totalErrors := 0
	for count := range errorCount {
		totalErrors += count
		if count != numOperations {
			t.Errorf("Expected %d errors per worker, got %d", numOperations, count)
		}
	}

	expectedTotalErrors := numWorkers * numOperations
	if totalErrors != expectedTotalErrors {
		t.Errorf("Expected %d total errors, got %d", expectedTotalErrors, totalErrors)
	}
}

// BenchmarkParsing 解析性能基准测试
func BenchmarkParsing(b *testing.B) {
	sql := "SELECT temperature, humidity FROM sensors WHERE deviceId = 'device1'"
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		parser := NewParser(sql)
		_, err := parser.Parse()
		if err != nil {
			b.Errorf("Benchmark parsing failed: %v", err)
		}
	}
}

// BenchmarkLexing 词法分析性能基准测试
func BenchmarkLexing(b *testing.B) {
	sql := "SELECT temperature, humidity FROM sensors WHERE deviceId = 'device1' AND timestamp > '2023-01-01'"
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lexer := NewLexer(sql)
		for {
			token := lexer.NextToken()
			if token.Type == TokenEOF {
				break
			}
		}
	}
}

// BenchmarkStreamConfig 流配置转换性能基准测试
func BenchmarkStreamConfig(b *testing.B) {
	sql := "SELECT COUNT(*), AVG(temperature) FROM sensors GROUP BY deviceId"
	parser := NewParser(sql)
	stmt, err := parser.Parse()
	if err != nil {
		b.Fatalf("Failed to parse SQL for benchmark: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := stmt.ToStreamConfig()
		if err != nil {
			b.Errorf("Benchmark stream config conversion failed: %v", err)
		}
	}
}

// BenchmarkComplexQuery 复杂查询性能基准测试
func BenchmarkComplexQuery(b *testing.B) {
	sql := "SELECT COUNT(*), AVG(temperature), MAX(humidity), MIN(pressure) FROM sensors WHERE deviceId IN ('device1', 'device2', 'device3') AND timestamp > '2023-01-01' GROUP BY deviceId, location HAVING COUNT(*) > 10 ORDER BY COUNT(*) DESC LIMIT 100"
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		parser := NewParser(sql)
		stmt, err := parser.Parse()
		if err != nil {
			b.Errorf("Benchmark complex query parsing failed: %v", err)
			continue
		}
		_, _, err = stmt.ToStreamConfig()
		if err != nil {
			b.Errorf("Benchmark complex query stream config failed: %v", err)
		}
	}
}
