package functions

import (
	"testing"
)

func TestAggregatorFunctionInterface(t *testing.T) {
	// 测试Sum聚合函数
	sumFunc := NewSumFunction()

	// 测试创建新实例
	aggInstance := sumFunc.New()
	if aggInstance == nil {
		t.Fatal("Failed to create new aggregator instance")
	}

	// 测试增量计算
	aggInstance.Add(10.0)
	aggInstance.Add(20.0)
	aggInstance.Add(30.0)

	result := aggInstance.Result()
	if result != 60.0 {
		t.Errorf("Expected 60.0, got %v", result)
	}

	// 测试重置
	aggInstance.Reset()
	result = aggInstance.Result()
	if result != nil {
		t.Errorf("Expected nil after reset (SQL standard: SUM with no rows returns NULL), got %v", result)
	}

	// 测试克隆
	aggInstance.Add(15.0)
	cloned := aggInstance.Clone()
	cloned.Add(25.0)

	originalResult := aggInstance.Result()
	clonedResult := cloned.Result()

	if originalResult != 15.0 {
		t.Errorf("Expected original result 15.0, got %v", originalResult)
	}
	if clonedResult != 40.0 {
		t.Errorf("Expected cloned result 40.0, got %v", clonedResult)
	}
}

func TestAnalyticalFunctionInterface(t *testing.T) {
	// 测试Lag分析函数
	lagFunc := NewLagFunction()

	ctx := &FunctionContext{
		Data: make(map[string]interface{}),
	}

	// 测试第一个值（应该返回默认值nil）
	result, err := lagFunc.Execute(ctx, []interface{}{10})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil for first value, got %v", result)
	}

	// 测试第二个值（应该返回第一个值）
	result, err = lagFunc.Execute(ctx, []interface{}{20})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != 10 {
		t.Errorf("Expected 10, got %v", result)
	}

	// 测试克隆
	cloned := lagFunc.Clone()
	if cloned == nil {
		t.Fatal("Failed to clone analytical function")
	}

	// 测试重置
	lagFunc.Reset()
	result, err = lagFunc.Execute(ctx, []interface{}{30})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil after reset, got %v", result)
	}
}

func TestCreateAggregator(t *testing.T) {
	// 测试创建已注册的聚合器
	aggFunc, err := CreateAggregator("sum")
	if err != nil {
		t.Fatalf("Failed to create sum aggregator: %v", err)
	}
	if aggFunc == nil {
		t.Fatal("Created aggregator is nil")
	}

	// 测试创建不存在的聚合器
	_, err = CreateAggregator("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent aggregator")
	}
}

func TestCreateAnalytical(t *testing.T) {
	// 测试创建已注册的分析函数
	analFunc, err := CreateAnalytical("lag")
	if err != nil {
		t.Fatalf("Failed to create lag analytical function: %v", err)
	}
	if analFunc == nil {
		t.Fatal("Created analytical function is nil")
	}

	// 测试创建不存在的分析函数
	_, err = CreateAnalytical("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent analytical function")
	}
}

func TestAggregatorAdapter(t *testing.T) {
	// 测试聚合器适配器
	adapter, err := NewAggregatorAdapter("sum")
	if err != nil {
		t.Fatalf("Failed to create aggregator adapter: %v", err)
	}

	// 测试创建新实例
	newInstance := adapter.New()
	if newInstance == nil {
		t.Fatal("Failed to create new adapter instance")
	}

	newAdapter, ok := newInstance.(*AggregatorAdapter)
	if !ok {
		t.Fatal("New instance is not an AggregatorAdapter")
	}

	// 测试添加值和获取结果
	newAdapter.Add(5.0)
	newAdapter.Add(10.0)

	result := newAdapter.Result()
	if result != 15.0 {
		t.Errorf("Expected 15.0, got %v", result)
	}
}

func TestAnalyticalAdapter(t *testing.T) {
	// 测试分析函数适配器
	adapter, err := NewAnalyticalAdapter("latest")
	if err != nil {
		t.Fatalf("Failed to create analytical adapter: %v", err)
	}

	ctx := &FunctionContext{
		Data: make(map[string]interface{}),
	}

	// 测试执行
	result, err := adapter.Execute(ctx, []interface{}{"test_value"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != "test_value" {
		t.Errorf("Expected 'test_value', got %v", result)
	}

	// 测试克隆
	cloned := adapter.Clone()
	if cloned == nil {
		t.Fatal("Failed to clone analytical adapter")
	}

	// 测试重置
	adapter.Reset()
}

func TestCustomFunctionRegistration(t *testing.T) {
	// 注册自定义函数示例
	RegisterCustomFunctions()

	// 测试自定义聚合函数
	productFunc, exists := Get("product")
	if !exists {
		t.Fatal("Custom product function not registered")
	}

	if productFunc.GetType() != TypeAggregation {
		t.Error("Product function should be aggregation type")
	}

	// 测试自定义分析函数
	movingAvgFunc, exists := Get("moving_avg")
	if !exists {
		t.Fatal("Custom moving average function not registered")
	}

	if movingAvgFunc.GetType() != TypeAnalytical {
		t.Error("Moving average function should be analytical type")
	}

	// 测试简单自定义函数
	doubleFunc, exists := Get("double")
	if !exists {
		t.Fatal("Custom double function not registered")
	}

	ctx := &FunctionContext{
		Data: make(map[string]interface{}),
	}

	result, err := doubleFunc.Execute(ctx, []interface{}{5.0})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != 10.0 {
		t.Errorf("Expected 10.0, got %v", result)
	}
}

func TestFunctionRegistry(t *testing.T) {
	// 测试函数注册表
	allFunctions := ListAll()
	if len(allFunctions) == 0 {
		t.Error("No functions registered")
	}

	// 测试按类型获取函数
	aggFunctions := GetByType(TypeAggregation)
	if len(aggFunctions) == 0 {
		t.Error("No aggregation functions found")
	}

	analFunctions := GetByType(TypeAnalytical)
	if len(analFunctions) == 0 {
		t.Error("No analytical functions found")
	}

	// 验证一些内置函数存在
	expectedFunctions := []string{"sum", "avg", "min", "max", "count", "lag", "latest"}
	for _, funcName := range expectedFunctions {
		if _, exists := Get(funcName); !exists {
			t.Errorf("Expected function %s not found", funcName)
		}
	}
}

func BenchmarkAggregatorIncremental(b *testing.B) {
	sumFunc := NewSumFunction()
	aggInstance := sumFunc.New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggInstance.Add(float64(i))
	}
	_ = aggInstance.Result()
}

func BenchmarkAggregatorBatch(b *testing.B) {
	sumFunc := NewSumFunction()
	ctx := &FunctionContext{
		Data: make(map[string]interface{}),
	}

	// 准备测试数据
	args := make([]interface{}, b.N)
	for i := 0; i < b.N; i++ {
		args[i] = float64(i)
	}

	b.ResetTimer()
	_, _ = sumFunc.Execute(ctx, args)
}
