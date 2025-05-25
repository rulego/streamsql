package streamsql

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"

	"github.com/rulego/streamsql/functions"
)

func main() {
	fmt.Println("⚡ 增量计算 vs 批量计算性能对比测试")
	fmt.Println("=====================================")

	// 注册优化的函数
	functions.Register(functions.NewOptimizedStdDevFunction())
	functions.Register(functions.NewOptimizedVarFunction())
	functions.Register(functions.NewOptimizedVarSFunction())
	functions.Register(functions.NewOptimizedStdDevSFunction())

	// 测试不同数据量
	dataSizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range dataSizes {
		fmt.Printf("\n🔬 测试数据量: %d 个数据点\n", size)
		fmt.Println("================================")

		// 生成测试数据
		data := generateTestData(size)

		// 测试各种聚合函数
		testSumPerformance(data)
		testAvgPerformance(data)
		testStdDevPerformance(data)
		testOptimizedStdDevPerformance(data)
	}

	// 内存使用对比
	fmt.Println("\n💾 内存使用对比")
	fmt.Println("================")
	testMemoryUsage()
}

func generateTestData(size int) []float64 {
	data := make([]float64, size)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < size; i++ {
		data[i] = rand.Float64() * 100
	}
	return data
}

func testSumPerformance(data []float64) {
	fmt.Printf("\n📊 SUM 函数性能测试 (数据量: %d)\n", len(data))

	// 增量计算
	start := time.Now()
	sumFunc := functions.NewSumFunction()
	aggFunc := sumFunc.New()
	for _, val := range data {
		aggFunc.Add(val)
	}
	result1 := aggFunc.Result()
	incrementalTime := time.Since(start)

	// 批量计算
	start = time.Now()
	args := make([]interface{}, len(data))
	for i, val := range data {
		args[i] = val
	}
	result2, _ := sumFunc.Execute(&functions.FunctionContext{}, args)
	batchTime := time.Since(start)

	fmt.Printf("  🚀 增量计算: %v (结果: %.2f)\n", incrementalTime, result1)
	fmt.Printf("  📊 批量计算: %v (结果: %.2f)\n", batchTime, result2)
	fmt.Printf("  📈 性能提升: %.1fx\n", float64(batchTime)/float64(incrementalTime))
}

func testAvgPerformance(data []float64) {
	fmt.Printf("\n📊 AVG 函数性能测试 (数据量: %d)\n", len(data))

	// 增量计算
	start := time.Now()
	avgFunc := functions.NewAvgFunction()
	aggFunc := avgFunc.New()
	for _, val := range data {
		aggFunc.Add(val)
	}
	result1 := aggFunc.Result()
	incrementalTime := time.Since(start)

	// 批量计算
	start = time.Now()
	args := make([]interface{}, len(data))
	for i, val := range data {
		args[i] = val
	}
	result2, _ := avgFunc.Execute(&functions.FunctionContext{}, args)
	batchTime := time.Since(start)

	fmt.Printf("  🚀 增量计算: %v (结果: %.2f)\n", incrementalTime, result1)
	fmt.Printf("  📊 批量计算: %v (结果: %.2f)\n", batchTime, result2)
	fmt.Printf("  📈 性能提升: %.1fx\n", float64(batchTime)/float64(incrementalTime))
}

func testStdDevPerformance(data []float64) {
	fmt.Printf("\n📊 STDDEV 函数性能测试 (数据量: %d)\n", len(data))

	// 增量计算（原版本，存储所有值）
	start := time.Now()
	stddevFunc := functions.NewStdDevAggregatorFunction()
	aggFunc := stddevFunc.New()
	for _, val := range data {
		aggFunc.Add(val)
	}
	result1 := aggFunc.Result()
	incrementalTime := time.Since(start)

	// 批量计算
	start = time.Now()
	args := make([]interface{}, len(data))
	for i, val := range data {
		args[i] = val
	}
	result2, _ := stddevFunc.Execute(&functions.FunctionContext{}, args)
	batchTime := time.Since(start)

	fmt.Printf("  🚀 增量计算(原版): %v (结果: %.6f)\n", incrementalTime, result1)
	fmt.Printf("  📊 批量计算: %v (结果: %.6f)\n", batchTime, result2)
	fmt.Printf("  📈 性能提升: %.1fx\n", float64(batchTime)/float64(incrementalTime))
}

func testOptimizedStdDevPerformance(data []float64) {
	fmt.Printf("\n📊 STDDEV 优化版本性能测试 (数据量: %d)\n", len(data))

	// 获取优化版本
	fn, exists := functions.Get("stddev_optimized")
	if !exists {
		fmt.Printf("  ❌ 优化版本未找到\n")
		return
	}

	optimizedFunc, ok := fn.(functions.AggregatorFunction)
	if !ok {
		fmt.Printf("  ❌ 不是聚合函数\n")
		return
	}

	// 增量计算（优化版本，韦尔福德算法）
	start := time.Now()
	aggFunc := optimizedFunc.New()
	for _, val := range data {
		aggFunc.Add(val)
	}
	result1 := aggFunc.Result()
	optimizedTime := time.Since(start)

	// 批量计算
	start = time.Now()
	args := make([]interface{}, len(data))
	for i, val := range data {
		args[i] = val
	}
	result2, _ := fn.Execute(&functions.FunctionContext{}, args)
	batchTime := time.Since(start)

	// 与原版本对比
	stddevFunc := functions.NewStdDevAggregatorFunction()
	start = time.Now()
	originalAggFunc := stddevFunc.New()
	for _, val := range data {
		originalAggFunc.Add(val)
	}
	result3 := originalAggFunc.Result()
	originalTime := time.Since(start)

	fmt.Printf("  🚀 优化增量计算: %v (结果: %.6f)\n", optimizedTime, result1)
	fmt.Printf("  ⚠️  原版增量计算: %v (结果: %.6f)\n", originalTime, result3)
	fmt.Printf("  📊 批量计算: %v (结果: %.6f)\n", batchTime, result2)
	fmt.Printf("  📈 优化版性能提升: %.1fx (vs 原版)\n", float64(originalTime)/float64(optimizedTime))
	fmt.Printf("  📈 优化版性能提升: %.1fx (vs 批量)\n", float64(batchTime)/float64(optimizedTime))
}

func testMemoryUsage() {
	dataSize := 100000
	data := generateTestData(dataSize)

	fmt.Printf("测试数据量: %d 个 float64 值\n", dataSize)
	fmt.Printf("理论数据大小: %.2f MB\n", float64(dataSize*8)/(1024*1024))

	// 测试批量计算内存使用
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// 批量计算 - 需要存储所有数据
	args := make([]interface{}, len(data))
	for i, val := range data {
		args[i] = val
	}
	sumFunc := functions.NewSumFunction()
	sumFunc.Execute(&functions.FunctionContext{}, args)

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	batchMemory := m2.Alloc - m1.Alloc

	// 测试增量计算内存使用
	runtime.GC()
	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)

	// 增量计算 - 只存储聚合状态
	aggFunc := sumFunc.New()
	for _, val := range data {
		aggFunc.Add(val)
	}
	aggFunc.Result()

	runtime.GC()
	var m4 runtime.MemStats
	runtime.ReadMemStats(&m4)

	incrementalMemory := m4.Alloc - m3.Alloc

	fmt.Printf("\n💾 内存使用对比:\n")
	fmt.Printf("  📊 批量计算内存使用: %.2f MB\n", float64(batchMemory)/(1024*1024))
	fmt.Printf("  🚀 增量计算内存使用: %.2f KB\n", float64(incrementalMemory)/1024)
	if batchMemory > 0 && incrementalMemory > 0 {
		fmt.Printf("  📈 内存节省: %.1fx\n", float64(batchMemory)/float64(incrementalMemory))
	}

	// 测试优化版本的内存使用
	fmt.Printf("\n🔬 详细内存分析:\n")

	testFunctionMemory("SUM (O(1)空间)", functions.NewSumFunction(), data)
	testFunctionMemory("AVG (O(1)空间)", functions.NewAvgFunction(), data)
	testFunctionMemory("STDDEV 原版 (O(n)空间)", functions.NewStdDevAggregatorFunction(), data)

	if fn, exists := functions.Get("stddev_optimized"); exists {
		if aggFn, ok := fn.(functions.AggregatorFunction); ok {
			testFunctionMemoryOptimized("STDDEV 优化版 (O(1)空间)", aggFn, data)
		}
	}
}

func testFunctionMemory(name string, fn functions.AggregatorFunction, data []float64) {
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	aggFunc := fn.New()
	for _, val := range data {
		aggFunc.Add(val)
	}
	aggFunc.Result()

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	memory := m2.Alloc - m1.Alloc
	fmt.Printf("  %s: %.2f KB\n", name, float64(memory)/1024)
}

func testFunctionMemoryOptimized(name string, fn functions.AggregatorFunction, data []float64) {
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	aggFunc := fn.New()
	for _, val := range data {
		aggFunc.Add(val)
	}
	aggFunc.Result()

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	memory := m2.Alloc - m1.Alloc
	fmt.Printf("  %s: %.2f KB\n", name, float64(memory)/1024)
}
