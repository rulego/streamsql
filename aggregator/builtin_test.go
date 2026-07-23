package aggregator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostAggregationPlaceholder Tests the full functionality of aggregated placeholders
func TestPostAggregationPlaceholder(t *testing.T) {
	t.Run("测试PostAggregationPlaceholder基本功能", func(t *testing.T) {
		// Create a PostAggregationPlaceholder instance
		placeholder := &PostAggregationPlaceholder{}
		require.NotNil(t, placeholder)

		// Test the new method
		newPlaceholder := placeholder.New()
		require.NotNil(t, newPlaceholder)
		assert.IsType(t, &PostAggregationPlaceholder{}, newPlaceholder)

		// Test the Add method (no action should be done)
		placeholder.Add(10)
		placeholder.Add("test")
		placeholder.Add(nil)
		placeholder.Add([]int{1, 2, 3})

		// Test the Result method (should return nil)
		result := placeholder.Result()
		assert.Nil(t, result)
	})

	t.Run("测试通过CreateBuiltinAggregator创建PostAggregationPlaceholder", func(t *testing.T) {
		// Use CreateBuiltinAggregator to create aggregators of post_aggregation type
		aggregator := CreateBuiltinAggregator(PostAggregation)
		require.NotNil(t, aggregator)
		assert.IsType(t, &PostAggregationPlaceholder{}, aggregator)

		// Test the aggregator features you create
		newAgg := aggregator.New()
		require.NotNil(t, newAgg)
		assert.IsType(t, &PostAggregationPlaceholder{}, newAgg)

		// Test adds various types of values
		newAgg.Add(100)
		newAgg.Add("string_value")
		newAgg.Add(map[string]any{"key": "value"})

		// The verification result is always nil
		result := newAgg.Result()
		assert.Nil(t, result)
	})

	t.Run("测试PostAggregationPlaceholder的多实例独立性", func(t *testing.T) {
		// Create multiple instances
		placeholder1 := &PostAggregationPlaceholder{}
		placeholder2 := placeholder1.New()
		placeholder3 := placeholder1.New()

		// Verify that the instance type is correct
		assert.IsType(t, &PostAggregationPlaceholder{}, placeholder1)
		assert.IsType(t, &PostAggregationPlaceholder{}, placeholder2)
		assert.IsType(t, &PostAggregationPlaceholder{}, placeholder3)

		// Each instance should return nil
		assert.Nil(t, placeholder1.Result())
		assert.Nil(t, placeholder2.Result())
		assert.Nil(t, placeholder3.Result())

		// Verifying that the Add operation does not affect the result (since it is a placeholder)
		placeholder1.Add("test1")
		placeholder2.Add("test2")
		placeholder3.Add("test3")
		assert.Nil(t, placeholder1.Result())
		assert.Nil(t, placeholder2.Result())
		assert.Nil(t, placeholder3.Result())
	})

	t.Run("测试PostAggregationPlaceholder在聚合场景中的使用", func(t *testing.T) {
		// Create an aggregate field containing PostAggregationPlaceholder
		groupFields := []string{"category"}
		aggFields := []AggregationField{
			{InputField: "value", AggregateType: Sum, OutputAlias: "sum_value"},
			{InputField: "placeholder_field", AggregateType: PostAggregation, OutputAlias: "post_agg_field"},
		}

		// Create a packet aggregator
		agg := NewGroupAggregator(groupFields, aggFields)
		require.NotNil(t, agg)

		// Add test data
		testData := []map[string]any{
			{"category": "A", "value": 10, "placeholder_field": "should_be_ignored"},
			{"category": "A", "value": 20, "placeholder_field": "also_ignored"},
			{"category": "B", "value": 30, "placeholder_field": 999},
		}

		for _, data := range testData {
			err := agg.Add(data)
			assert.NoError(t, err)
		}

		// Get results
		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, 2)

		// The result of verifying the PostAggregationPlaceholder field is nil
		for _, result := range results {
			assert.Contains(t, result, "post_agg_field")
			assert.Nil(t, result["post_agg_field"])
			// Verify that the aggregated fields are working properly
			assert.Contains(t, result, "sum_value")
			assert.NotNil(t, result["sum_value"])
		}
	})
}

// TestCreateBuiltinAggregatorPostAggregation Test CreateBuiltinAggregator's handling of post_aggregation types
func TestCreateBuiltinAggregatorPostAggregation(t *testing.T) {
	t.Run("测试post_aggregation类型聚合器创建", func(t *testing.T) {
		aggregator := CreateBuiltinAggregator("post_aggregation")
		require.NotNil(t, aggregator)
		assert.IsType(t, &PostAggregationPlaceholder{}, aggregator)
	})

	t.Run("测试PostAggregation常量", func(t *testing.T) {
		// Verify the PostAggregation constant value
		assert.Equal(t, AggregateType("post_aggregation"), PostAggregation)

		// Create aggregators using constants
		aggregator := CreateBuiltinAggregator(PostAggregation)
		require.NotNil(t, aggregator)
		assert.IsType(t, &PostAggregationPlaceholder{}, aggregator)
	})

	t.Run("测试与其他聚合类型的区别", func(t *testing.T) {
		// Create different types of aggregators
		sumAgg := CreateBuiltinAggregator(Sum)
		countAgg := CreateBuiltinAggregator(Count)
		postAgg := CreateBuiltinAggregator(PostAggregation)

		// Different types of verification
		assert.NotEqual(t, sumAgg, postAgg)
		assert.NotEqual(t, countAgg, postAgg)
		assert.IsType(t, &PostAggregationPlaceholder{}, postAgg)
	})
}
