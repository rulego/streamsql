package aggregator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testData struct {
	Device      string
	temperature float64
	humidity    float64
}

func TestGroupAggregator_MultiFieldSum(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "temperature_sum",
			},
			{
				InputField:    "humidity",
				AggregateType: Sum,
				OutputAlias:   "humidity_sum",
			},
		},
	)

	testData := []map[string]interface{}{
		{"Device": "aa", "temperature": 25.5, "humidity": 60.0},
		{"Device": "aa", "temperature": 26.8, "humidity": 55.0},
		{"Device": "bb", "temperature": 22.3, "humidity": 65.0},
		{"Device": "bb", "temperature": 23.5, "humidity": 70.0},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]interface{}{
		{"Device": "aa", "temperature_sum": 52.3, "humidity_sum": 115.0},
		{"Device": "bb", "temperature_sum": 45.8, "humidity_sum": 135.0},
	}

	results, _ := agg.GetResults()
	assert.ElementsMatch(t, expected, results)
}

// TestGroupAggregator_Put 测试Put方法
func TestGroupAggregator_Put(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "temperature_sum",
			},
		},
	)

	// 测试Put方法
	err := agg.Put("test_key", "test_value")
	assert.NoError(t, err)

	// 测试多次Put
	err = agg.Put("key1", 123)
	assert.NoError(t, err)
	err = agg.Put("key2", 456.78)
	assert.NoError(t, err)
}

// TestGroupAggregator_RegisterExpression 测试表达式注册
func TestGroupAggregator_RegisterExpression(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "temperature_sum",
			},
		},
	)

	// 注册表达式
	evaluator := func(data interface{}) (interface{}, error) {
		if dataMap, ok := data.(map[string]interface{}); ok {
			if temp, exists := dataMap["temperature"]; exists {
				if tempFloat, ok := temp.(float64); ok {
					return tempFloat * 1.8 + 32, nil // 摄氏度转华氏度
				}
			}
		}
		return nil, errors.New("invalid data")
	}

	agg.RegisterExpression("fahrenheit", "temperature * 1.8 + 32", []string{"temperature"}, evaluator)

	// 验证表达式已注册
	assert.NotNil(t, agg.expressions["fahrenheit"])
	assert.Equal(t, "fahrenheit", agg.expressions["fahrenheit"].Field)
	assert.Equal(t, "temperature * 1.8 + 32", agg.expressions["fahrenheit"].Expression)
	assert.Equal(t, []string{"temperature"}, agg.expressions["fahrenheit"].Fields)
}

// TestGroupAggregator_Reset 测试Reset方法
func TestGroupAggregator_Reset(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "temperature_sum",
			},
		},
	)

	// 添加一些数据
	testData := []map[string]interface{}{
		{"Device": "test", "temperature": 25.5},
		{"Device": "test", "temperature": 26.8},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	// 验证有数据
	results, _ := agg.GetResults()
	assert.Len(t, results, 1)

	// 重置
	agg.Reset()

	// 验证数据已清空
	results, _ = agg.GetResults()
	assert.Len(t, results, 0)
}

// TestGroupAggregator_ErrorHandling 测试错误处理
func TestGroupAggregator_ErrorHandling(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "temperature_sum",
			},
		},
	)

	// 测试添加无效数据
	err := agg.Add(nil)
	assert.Error(t, err)

	// 测试添加非map类型数据
	err = agg.Add("invalid data")
	assert.Error(t, err)

	// 测试添加缺少分组字段的数据
	err = agg.Add(map[string]interface{}{"temperature": 25.5})
	assert.Error(t, err)
}

// TestGroupAggregator_DifferentAggregateTypes 测试不同聚合类型
func TestGroupAggregator_DifferentAggregateTypes(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"category"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Count,
				OutputAlias:   "count",
			},
			{
				InputField:    "score",
				AggregateType: Avg,
				OutputAlias:   "avg_score",
			},
			{
				InputField:    "score",
				AggregateType: Max,
				OutputAlias:   "max_score",
			},
			{
				InputField:    "score",
				AggregateType: Min,
				OutputAlias:   "min_score",
			},
		},
	)

	testData := []map[string]interface{}{
		{"category": "A", "value": 1, "score": 85.5},
		{"category": "A", "value": 2, "score": 92.0},
		{"category": "A", "value": 3, "score": 78.5},
		{"category": "B", "value": 4, "score": 88.0},
		{"category": "B", "value": 5, "score": 95.5},
	}

	for _, d := range testData {
		err := agg.Add(d)
		assert.NoError(t, err)
	}

	results, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 2)

	// 验证结果
	for _, result := range results {
		category := result["category"]
		if category == "A" {
			assert.Equal(t, float64(3), result["count"])
			assert.InDelta(t, 85.33, result["avg_score"], 0.1)
			assert.Equal(t, 92.0, result["max_score"])
			assert.Equal(t, 78.5, result["min_score"])
		} else if category == "B" {
			assert.Equal(t, float64(2), result["count"])
			assert.InDelta(t, 91.75, result["avg_score"], 0.1)
			assert.Equal(t, 95.5, result["max_score"])
			assert.Equal(t, 88.0, result["min_score"])
		}
	}
}

// TestGroupAggregator_MultipleGroupFields 测试多个分组字段
func TestGroupAggregator_MultipleGroupFields(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"region", "category"},
		[]AggregationField{
			{
				InputField:    "sales",
				AggregateType: Sum,
				OutputAlias:   "total_sales",
			},
		},
	)

	testData := []map[string]interface{}{
		{"region": "North", "category": "A", "sales": 100.0},
		{"region": "North", "category": "A", "sales": 150.0},
		{"region": "North", "category": "B", "sales": 200.0},
		{"region": "South", "category": "A", "sales": 120.0},
		{"region": "South", "category": "B", "sales": 180.0},
	}

	for _, d := range testData {
		err := agg.Add(d)
		assert.NoError(t, err)
	}

	results, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 4)

	// 验证每个组合的结果
	expected := map[string]float64{
		"North-A": 250.0,
		"North-B": 200.0,
		"South-A": 120.0,
		"South-B": 180.0,
	}

	for _, result := range results {
		key := result["region"].(string) + "-" + result["category"].(string)
		expectedSales, exists := expected[key]
		assert.True(t, exists, "Unexpected group key: %s", key)
		assert.Equal(t, expectedSales, result["total_sales"])
	}
}

// TestGroupAggregator_EmptyData 测试空数据处理
func TestGroupAggregator_EmptyData(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "temperature_sum",
			},
		},
	)

	// 不添加任何数据，直接获取结果
	results, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 0)
}

// TestGroupAggregator_NilValues 测试空值处理
func TestGroupAggregator_NilValues(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "temperature_sum",
			},
		},
	)

	testData := []map[string]interface{}{
		{"Device": "test", "temperature": 25.5},
		{"Device": "test", "temperature": nil}, // 空值
		{"Device": "test", "temperature": 30.0},
	}

	for _, d := range testData {
		err := agg.Add(d)
		assert.NoError(t, err)
	}

	results, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// 空值应该被忽略，只计算非空值
	expected := 55.5 // 25.5 + 30.0
	assert.Equal(t, expected, results[0]["temperature_sum"])
}

// TestGroupAggregator_ConcurrentAccess 测试并发访问
func TestGroupAggregator_ConcurrentAccess(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "temperature_sum",
			},
		},
	)

	// 并发添加数据
	go func() {
		for i := 0; i < 10; i++ {
			agg.Add(map[string]interface{}{"Device": "A", "temperature": float64(i)})
		}
	}()

	go func() {
		for i := 0; i < 10; i++ {
			agg.Add(map[string]interface{}{"Device": "B", "temperature": float64(i * 2)})
		}
	}()

	// 并发注册表达式
	go func() {
		evaluator := func(data interface{}) (interface{}, error) {
			return 1.0, nil
		}
		agg.RegisterExpression("test_expr", "1", []string{}, evaluator)
	}()

	// 并发Put操作
	go func() {
		for i := 0; i < 5; i++ {
			agg.Put("key"+string(rune(i)), i)
		}
	}()

	// 等待一段时间确保所有goroutine完成
	// 注意：这不是最佳的同步方式，但对于测试来说足够了
	// 在实际应用中应该使用sync.WaitGroup
	for i := 0; i < 100; i++ {
		// 尝试获取结果，测试并发读取
		_, _ = agg.GetResults()
	}
}

// TestCreateBuiltinAggregator 测试内置聚合器创建
func TestCreateBuiltinAggregator(t *testing.T) {
	tests := []struct {
		name    string
		aggType AggregateType
	}{
		{"Sum聚合器", Sum},
		{"Count聚合器", Count},
		{"Avg聚合器", Avg},
		{"Max聚合器", Max},
		{"Min聚合器", Min},
		{"Expression聚合器", Expression},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregator := CreateBuiltinAggregator(tt.aggType)
			assert.NotNil(t, aggregator)

			// 测试New方法
			newAgg := aggregator.New()
			assert.NotNil(t, newAgg)
		})
	}
}

// TestExpressionAggregatorWrapper 测试表达式聚合器包装器
func TestExpressionAggregatorWrapper(t *testing.T) {
	wrapper := CreateBuiltinAggregator(Expression)
	require.NotNil(t, wrapper)

	// 测试类型断言
	exprWrapper, ok := wrapper.(*ExpressionAggregatorWrapper)
	assert.True(t, ok)
	assert.NotNil(t, exprWrapper.function)

	// 测试New方法
	newWrapper := wrapper.New()
	assert.NotNil(t, newWrapper)

	// 测试Add和Result方法
	wrapper.Add(10.0)
	wrapper.Add(20.0)
	result := wrapper.Result()
	assert.NotNil(t, result)
}

func TestGroupAggregator_SingleField(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "temperature_sum",
			},
		},
	)

	testData := []map[string]interface{}{
		{"Device": "cc", "temperature": 24.5},
		{"Device": "cc", "temperature": 27.8},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]interface{}{
		{"Device": "cc", "temperature_sum": 52.3},
	}

	results, _ := agg.GetResults()
	assert.ElementsMatch(t, expected, results)
}

func TestGroupAggregator_MultipleAggregators(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "temperature_sum",
			},
			{
				InputField:    "humidity",
				AggregateType: Avg,
				OutputAlias:   "humidity_avg",
			},
			{
				InputField:    "presure",
				AggregateType: Max,
				OutputAlias:   "presure_max",
			},
			{
				InputField:    "PM10",
				AggregateType: Min,
				OutputAlias:   "PM10_min",
			},
		},
	)

	testData := []map[string]interface{}{
		{"Device": "cc", "temperature": 25.5, "humidity": 65.5, "presure": 1008, "PM10": 35},
		{"Device": "cc", "temperature": 27.8, "humidity": 60.5, "presure": 1012, "PM10": 28},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]interface{}{
		{
			"Device":          "cc",
			"temperature_sum": 53.3,
			"humidity_avg":    63.0,
			"presure_max":     1012.0,
			"PM10_min":        28.0,
		},
	}

	results, _ := agg.GetResults()
	assert.ElementsMatch(t, expected, results)
}

func TestGroupAggregator_NoAlias(t *testing.T) {
	// Test case where no alias is specified, should use input field name as output field name
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				// OutputAlias left empty, should use InputField
			},
		},
	)

	testData := []map[string]interface{}{
		{"Device": "dd", "temperature": 10.0},
		{"Device": "dd", "temperature": 15.0},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]interface{}{
		{"Device": "dd", "temperature": 25.0},
	}

	results, _ := agg.GetResults()
	assert.ElementsMatch(t, expected, results)
}
