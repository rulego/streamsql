package functions

import (
	"testing"
)

func TestFunctionsAggregatorIntegration(t *testing.T) {
	// Test the incremental calculation of the aggregate function
	t.Run("SumAggregator", func(t *testing.T) {
		sumFunc := NewSumFunction()
		aggInstance := sumFunc.New()

		// Test incremental calculations
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

func TestWindowFunctions(t *testing.T) {
	t.Run("WindowStartFunction", func(t *testing.T) {
		windowStartFunc := NewWindowStartFunction()

		// Test incremental computing interface
		aggInstance := windowStartFunc.New()
		aggInstance.Add(1000)

		result := aggInstance.Result()
		if result != 1000 {
			t.Errorf("Expected 1000, got %v", result)
		}
	})

	t.Run("WindowEndFunction", func(t *testing.T) {
		windowEndFunc := NewWindowEndFunction()

		// Test incremental computing interface
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
		// The standard deviation should be about 1.58
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
		values, ok := result.([]any)
		if !ok {
			t.Fatalf("Expected []any, got %T", result)
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
		aggInstance.Add("a") // Repeat
		aggInstance.Add("c")
		aggInstance.Add("b") // Repeat

		result := aggInstance.Result()
		values, ok := result.([]any)
		if !ok {
			t.Fatalf("Expected []any, got %T", result)
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
}
