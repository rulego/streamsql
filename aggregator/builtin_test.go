package aggregator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostAggregationPlaceholder 测试后聚合占位符的完整功能
func TestPostAggregationPlaceholder(t *testing.T) {
	t.Run("测试PostAggregationPlaceholder基本功能", func(t *testing.T) {
		// 创建PostAggregationPlaceholder实例
		placeholder := &PostAggregationPlaceholder{}
		require.NotNil(t, placeholder)

		// 测试New方法
		newPlaceholder := placeholder.New()
		require.NotNil(t, newPlaceholder)
		assert.IsType(t, &PostAggregationPlaceholder{}, newPlaceholder)

		// 测试Add方法（应该不做任何操作）
		placeholder.Add(10)
		placeholder.Add("test")
		placeholder.Add(nil)
		placeholder.Add([]int{1, 2, 3})

		// 测试Result方法（应该返回nil）
		result := placeholder.Result()
		assert.Nil(t, result)
	})

	t.Run("测试通过CreateBuiltinAggregator创建PostAggregationPlaceholder", func(t *testing.T) {
		// 使用CreateBuiltinAggregator创建post_aggregation类型的聚合器
		aggregator := CreateBuiltinAggregator(PostAggregation)
		require.NotNil(t, aggregator)
		assert.IsType(t, &PostAggregationPlaceholder{}, aggregator)

		// 测试创建的聚合器功能
		newAgg := aggregator.New()
		require.NotNil(t, newAgg)
		assert.IsType(t, &PostAggregationPlaceholder{}, newAgg)

		// 测试添加各种类型的值
		newAgg.Add(100)
		newAgg.Add("string_value")
		newAgg.Add(map[string]interface{}{"key": "value"})

		// 验证结果始终为nil
		result := newAgg.Result()
		assert.Nil(t, result)
	})

	t.Run("测试PostAggregationPlaceholder的多实例独立性", func(t *testing.T) {
		// 创建多个实例
		placeholder1 := &PostAggregationPlaceholder{}
		placeholder2 := placeholder1.New()
		placeholder3 := placeholder1.New()

		// 验证实例类型正确
		assert.IsType(t, &PostAggregationPlaceholder{}, placeholder1)
		assert.IsType(t, &PostAggregationPlaceholder{}, placeholder2)
		assert.IsType(t, &PostAggregationPlaceholder{}, placeholder3)

		// 每个实例都应该返回nil
		assert.Nil(t, placeholder1.Result())
		assert.Nil(t, placeholder2.Result())
		assert.Nil(t, placeholder3.Result())

		// 验证Add操作不会影响结果（因为是占位符）
		placeholder1.Add("test1")
		placeholder2.Add("test2")
		placeholder3.Add("test3")
		assert.Nil(t, placeholder1.Result())
		assert.Nil(t, placeholder2.Result())
		assert.Nil(t, placeholder3.Result())
	})

	t.Run("测试PostAggregationPlaceholder在聚合场景中的使用", func(t *testing.T) {
		// 创建包含PostAggregationPlaceholder的聚合字段
		groupFields := []string{"category"}
		aggFields := []AggregationField{
			{InputField: "value", AggregateType: Sum, OutputAlias: "sum_value"},
			{InputField: "placeholder_field", AggregateType: PostAggregation, OutputAlias: "post_agg_field"},
		}

		// 创建分组聚合器
		agg := NewGroupAggregator(groupFields, aggFields)
		require.NotNil(t, agg)

		// 添加测试数据
		testData := []map[string]interface{}{
			{"category": "A", "value": 10, "placeholder_field": "should_be_ignored"},
			{"category": "A", "value": 20, "placeholder_field": "also_ignored"},
			{"category": "B", "value": 30, "placeholder_field": 999},
		}

		for _, data := range testData {
			err := agg.Add(data)
			assert.NoError(t, err)
		}

		// 获取结果
		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, 2)

		// 验证PostAggregationPlaceholder字段的结果为nil
		for _, result := range results {
			assert.Contains(t, result, "post_agg_field")
			assert.Nil(t, result["post_agg_field"])
			// 验证正常聚合字段工作正常
			assert.Contains(t, result, "sum_value")
			assert.NotNil(t, result["sum_value"])
		}
	})
}

// TestCreateBuiltinAggregatorPostAggregation 测试CreateBuiltinAggregator对post_aggregation类型的处理
func TestCreateBuiltinAggregatorPostAggregation(t *testing.T) {
	t.Run("测试post_aggregation类型聚合器创建", func(t *testing.T) {
		aggregator := CreateBuiltinAggregator("post_aggregation")
		require.NotNil(t, aggregator)
		assert.IsType(t, &PostAggregationPlaceholder{}, aggregator)
	})

	t.Run("测试PostAggregation常量", func(t *testing.T) {
		// 验证PostAggregation常量值
		assert.Equal(t, AggregateType("post_aggregation"), PostAggregation)

		// 使用常量创建聚合器
		aggregator := CreateBuiltinAggregator(PostAggregation)
		require.NotNil(t, aggregator)
		assert.IsType(t, &PostAggregationPlaceholder{}, aggregator)
	})

	t.Run("测试与其他聚合类型的区别", func(t *testing.T) {
		// 创建不同类型的聚合器
		sumAgg := CreateBuiltinAggregator(Sum)
		countAgg := CreateBuiltinAggregator(Count)
		postAgg := CreateBuiltinAggregator(PostAggregation)

		// 验证类型不同
		assert.NotEqual(t, sumAgg, postAgg)
		assert.NotEqual(t, countAgg, postAgg)
		assert.IsType(t, &PostAggregationPlaceholder{}, postAgg)
	})
}