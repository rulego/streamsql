package functions

import (
	"testing"
)

func TestFunctionsAggregatorIntegration(t *testing.T) {
	// 测试聚合函数的增量计算
	t.Run("SumAggregator", func(t *testing.T) {
		sumFunc := NewSumFunction()
		aggInstance := sumFunc.New()

		// 测试增量计算
		aggInstance.Add(10.0)
		aggInstance.Add(20.0)
		aggInstance.Add(30.0)

		result := aggInstance.Result()
		if result != 60.0 {
			t.Errorf("Expected 60.0, got %v", result)
		}
	})

	t.Run("AvgAggregator", func(t *testing.T) {
		avgFunc := NewAvgFunction()
		aggInstance := avgFunc.New()

		aggInstance.Add(10.0)
		aggInstance.Add(20.0)
		aggInstance.Add(30.0)

		result := aggInstance.Result()
		if result != 20.0 {
			t.Errorf("Expected 20.0, got %v", result)
		}
	})

	t.Run("CountAggregator", func(t *testing.T) {
		countFunc := NewCountFunction()
		aggInstance := countFunc.New()

		aggInstance.Add("a")
		aggInstance.Add("b")
		aggInstance.Add("c")

		result := aggInstance.Result()
		if result != 3.0 {
			t.Errorf("Expected 3.0, got %v", result)
		}
	})

	t.Run("MinAggregator", func(t *testing.T) {
		minFunc := NewMinFunction()
		aggInstance := minFunc.New()

		aggInstance.Add(30.0)
		aggInstance.Add(10.0)
		aggInstance.Add(20.0)

		result := aggInstance.Result()
		if result != 10.0 {
			t.Errorf("Expected 10.0, got %v", result)
		}
	})

	t.Run("MaxAggregator", func(t *testing.T) {
		maxFunc := NewMaxFunction()
		aggInstance := maxFunc.New()

		aggInstance.Add(10.0)
		aggInstance.Add(30.0)
		aggInstance.Add(20.0)

		result := aggInstance.Result()
		if result != 30.0 {
			t.Errorf("Expected 30.0, got %v", result)
		}
	})
}

func TestAnalyticalFunctionsIntegration(t *testing.T) {
	t.Run("LagFunction", func(t *testing.T) {
		lagFunc := NewLagFunction()
		ctx := &FunctionContext{
			Data: make(map[string]interface{}),
		}

		// 第一个值应该返回默认值nil
		result, err := lagFunc.Execute(ctx, []interface{}{10})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if result != nil {
			t.Errorf("Expected nil for first value, got %v", result)
		}

		// 第二个值应该返回第一个值
		result, err = lagFunc.Execute(ctx, []interface{}{20})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if result != 10 {
			t.Errorf("Expected 10, got %v", result)
		}
	})

	t.Run("LatestFunction", func(t *testing.T) {
		latestFunc := NewLatestFunction()
		ctx := &FunctionContext{
			Data: make(map[string]interface{}),
		}

		result, err := latestFunc.Execute(ctx, []interface{}{"test_value"})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if result != "test_value" {
			t.Errorf("Expected 'test_value', got %v", result)
		}
	})

	t.Run("HadChangedFunction", func(t *testing.T) {
		hadChangedFunc := NewHadChangedFunction()
		ctx := &FunctionContext{
			Data: make(map[string]interface{}),
		}

		// 第一次调用应该返回true
		result, err := hadChangedFunc.Execute(ctx, []interface{}{10})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if result != true {
			t.Errorf("Expected true for first call, got %v", result)
		}

		// 相同值应该返回false
		result, err = hadChangedFunc.Execute(ctx, []interface{}{10})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if result != false {
			t.Errorf("Expected false for same value, got %v", result)
		}

		// 不同值应该返回true
		result, err = hadChangedFunc.Execute(ctx, []interface{}{20})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if result != true {
			t.Errorf("Expected true for different value, got %v", result)
		}
	})
}

func TestWindowFunctions(t *testing.T) {
	t.Run("WindowStartFunction", func(t *testing.T) {
		windowStartFunc := NewWindowStartFunction()

		// 测试增量计算接口
		aggInstance := windowStartFunc.New()
		aggInstance.Add(1000)

		result := aggInstance.Result()
		if result != 1000 {
			t.Errorf("Expected 1000, got %v", result)
		}
	})

	t.Run("WindowEndFunction", func(t *testing.T) {
		windowEndFunc := NewWindowEndFunction()

		// 测试增量计算接口
		aggInstance := windowEndFunc.New()
		aggInstance.Add(2000)

		result := aggInstance.Result()
		if result != 2000 {
			t.Errorf("Expected 2000, got %v", result)
		}
	})
}

func TestComplexAggregators(t *testing.T) {
	t.Run("StdDevAggregator", func(t *testing.T) {
		stddevFunc := NewStdDevAggregatorFunction()
		aggInstance := stddevFunc.New()

		aggInstance.Add(1.0)
		aggInstance.Add(2.0)
		aggInstance.Add(3.0)
		aggInstance.Add(4.0)
		aggInstance.Add(5.0)

		result := aggInstance.Result()
		// 标准差应该约为1.58
		if result.(float64) < 1.5 || result.(float64) > 1.7 {
			t.Errorf("Expected stddev around 1.58, got %v", result)
		}
	})

	t.Run("MedianAggregator", func(t *testing.T) {
		medianFunc := NewMedianAggregatorFunction()
		aggInstance := medianFunc.New()

		aggInstance.Add(1.0)
		aggInstance.Add(3.0)
		aggInstance.Add(2.0)
		aggInstance.Add(5.0)
		aggInstance.Add(4.0)

		result := aggInstance.Result()
		if result != 3.0 {
			t.Errorf("Expected 3.0, got %v", result)
		}
	})

	t.Run("CollectAggregator", func(t *testing.T) {
		collectFunc := NewCollectAggregatorFunction()
		aggInstance := collectFunc.New()

		aggInstance.Add("a")
		aggInstance.Add("b")
		aggInstance.Add("c")

		result := aggInstance.Result()
		values, ok := result.([]interface{})
		if !ok {
			t.Fatalf("Expected []interface{}, got %T", result)
		}

		if len(values) != 3 {
			t.Errorf("Expected 3 values, got %d", len(values))
		}

		if values[0] != "a" || values[1] != "b" || values[2] != "c" {
			t.Errorf("Expected [a, b, c], got %v", values)
		}
	})

	t.Run("DeduplicateAggregator", func(t *testing.T) {
		dedupeFunc := NewDeduplicateAggregatorFunction()
		aggInstance := dedupeFunc.New()

		aggInstance.Add("a")
		aggInstance.Add("b")
		aggInstance.Add("a") // 重复
		aggInstance.Add("c")
		aggInstance.Add("b") // 重复

		result := aggInstance.Result()
		values, ok := result.([]interface{})
		if !ok {
			t.Fatalf("Expected []interface{}, got %T", result)
		}

		if len(values) != 3 {
			t.Errorf("Expected 3 unique values, got %d", len(values))
		}
	})
}

func TestAdapterFunctions(t *testing.T) {
	t.Run("AggregatorAdapter", func(t *testing.T) {
		adapter, err := NewAggregatorAdapter("sum")
		if err != nil {
			t.Fatalf("Failed to create aggregator adapter: %v", err)
		}

		newInstance := adapter.New()
		newAdapter, ok := newInstance.(*AggregatorAdapter)
		if !ok {
			t.Fatalf("New instance is not an AggregatorAdapter")
		}

		newAdapter.Add(10.0)
		newAdapter.Add(20.0)

		result := newAdapter.Result()
		if result != 30.0 {
			t.Errorf("Expected 30.0, got %v", result)
		}
	})

	t.Run("AnalyticalAggregatorAdapter", func(t *testing.T) {
		adapter, err := NewAnalyticalAggregatorAdapter("latest")
		if err != nil {
			t.Fatalf("Failed to create analytical aggregator adapter: %v", err)
		}

		newInstance := adapter.New()
		newAdapter, ok := newInstance.(*AnalyticalAggregatorAdapter)
		if !ok {
			t.Fatalf("New instance is not an AnalyticalAggregatorAdapter")
		}

		newAdapter.Add("test_value")
		result := newAdapter.Result()
		if result != "test_value" {
			t.Errorf("Expected 'test_value', got %v", result)
		}
	})
}
