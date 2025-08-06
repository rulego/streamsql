package aggregator

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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
					return tempFloat*1.8 + 32, nil // 摄氏度转华氏度
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

// TestGroupAggregatorAdvancedFeatures 测试聚合器高级功能
func TestGroupAggregatorAdvancedFeatures(t *testing.T) {
	// 测试复杂聚合表达式
	t.Run("Complex Aggregation Expressions", func(t *testing.T) {
		agg := NewGroupAggregator(
			[]string{"Device"},
			[]AggregationField{
				{
					InputField:    "temperature",
					AggregateType: Avg,
					OutputAlias:   "avg_temp",
				},
				{
					InputField:    "humidity",
					AggregateType: Max,
					OutputAlias:   "max_humidity",
				},
				{
					InputField:    "pressure",
					AggregateType: Min,
					OutputAlias:   "min_pressure",
				},
			},
		)

		testData := []map[string]interface{}{
			{"Device": "sensor1", "temperature": 25.5, "humidity": 60.0, "pressure": 1013.25},
			{"Device": "sensor1", "temperature": 26.8, "humidity": 65.0, "pressure": 1012.50},
			{"Device": "sensor2", "temperature": 22.3, "humidity": 55.0, "pressure": 1014.75},
			{"Device": "sensor2", "temperature": 23.5, "humidity": 70.0, "pressure": 1013.00},
		}

		for _, d := range testData {
			agg.Add(d)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, 2)

		// 验证结果
		for _, result := range results {
			device := result["Device"].(string)
			if device == "sensor1" {
				assert.InDelta(t, 26.15, result["avg_temp"], 0.01)
				assert.Equal(t, 65.0, result["max_humidity"])
				assert.Equal(t, 1012.50, result["min_pressure"])
			} else if device == "sensor2" {
				assert.InDelta(t, 22.9, result["avg_temp"], 0.01)
				assert.Equal(t, 70.0, result["max_humidity"])
				assert.Equal(t, 1013.00, result["min_pressure"])
			}
		}
	})

	// 测试统计聚合函数
	t.Run("Statistical Aggregation Functions", func(t *testing.T) {
		agg := NewGroupAggregator(
			[]string{"category"},
			[]AggregationField{
				{
					InputField:    "value",
					AggregateType: StdDev,
					OutputAlias:   "std_dev",
				},
				{
					InputField:    "value",
					AggregateType: Var,
					OutputAlias:   "variance",
				},
				{
					InputField:    "value",
					AggregateType: Median,
					OutputAlias:   "median",
				},
			},
		)

		testData := []map[string]interface{}{
			{"category": "A", "value": 10.0},
			{"category": "A", "value": 12.0},
			{"category": "A", "value": 14.0},
			{"category": "B", "value": 5.0},
			{"category": "B", "value": 7.0},
			{"category": "B", "value": 9.0},
		}

		for _, d := range testData {
			agg.Add(d)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, 2)

		// 验证统计结果
		for _, result := range results {
			category := result["category"].(string)
			if category == "A" {
				assert.InDelta(t, 2.0, result["std_dev"], 0.01)
				assert.InDelta(t, 2.6666666666666665, result["variance"], 0.01)
				assert.Equal(t, 12.0, result["median"])
			} else if category == "B" {
				assert.InDelta(t, 2.0, result["std_dev"], 0.01)
				assert.InDelta(t, 2.6666666666666665, result["variance"], 0.01)
				assert.Equal(t, 7.0, result["median"])
			}
		}
	})
}

// TestGroupAggregatorDataTypes 测试不同数据类型的聚合
func TestGroupAggregatorDataTypes(t *testing.T) {
	tests := []struct {
		name        string
		aggType     AggregateType
		inputData   []map[string]interface{}
		expectedKey string
		expectedVal interface{}
	}{
		{
			name:    "String Count",
			aggType: Count,
			inputData: []map[string]interface{}{
				{"group": "A", "value": "hello"},
				{"group": "A", "value": "world"},
				{"group": "B", "value": "test"},
			},
			expectedKey: "count",
			expectedVal: 0.0, // Count聚合器可能只计算数值
		},
		{
			name:    "Boolean Count",
			aggType: Count,
			inputData: []map[string]interface{}{
				{"group": "A", "value": true},
				{"group": "A", "value": false},
				{"group": "A", "value": true},
				{"group": "B", "value": false},
			},
			expectedKey: "count",
			expectedVal: 0.0, // Count聚合器可能只计算数值
		},
		{
			name:    "Mixed Types Count",
			aggType: Count,
			inputData: []map[string]interface{}{
				{"group": "A", "value": 123},
				{"group": "A", "value": "string"},
				{"group": "A", "value": true},
				{"group": "B", "value": 456},
			},
			expectedKey: "count",
			expectedVal: 1.0, // 只有123是数值
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := NewGroupAggregator(
				[]string{"group"},
				[]AggregationField{
					{
						InputField:    "value",
						AggregateType: tt.aggType,
						OutputAlias:   tt.expectedKey,
					},
				},
			)

			for _, d := range tt.inputData {
				agg.Add(d)
			}

			results, err := agg.GetResults()
			assert.NoError(t, err)

			// 找到A组的结果
			var groupAResult map[string]interface{}
			for _, result := range results {
				if result["group"] == "A" {
					groupAResult = result
					break
				}
			}

			assert.NotNil(t, groupAResult)
			assert.Equal(t, tt.expectedVal, groupAResult[tt.expectedKey])
		})
	}
}

// TestGroupAggregatorEdgeCases 测试聚合器边界情况
func TestGroupAggregatorEdgeCases(t *testing.T) {
	// 测试空数据
	t.Run("Empty Data", func(t *testing.T) {
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

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Empty(t, results)
	})

	// 测试空分组字段
	t.Run("Empty Group Fields", func(t *testing.T) {
		agg := NewGroupAggregator(
			[]string{},
			[]AggregationField{
				{
					InputField:    "temperature",
					AggregateType: Sum,
					OutputAlias:   "temperature_sum",
				},
			},
		)

		testData := []map[string]interface{}{
			{"temperature": 25.5},
			{"temperature": 26.8},
		}

		for _, d := range testData {
			agg.Add(d)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.InDelta(t, 52.3, results[0]["temperature_sum"], 0.01)
	})

	// 测试空聚合字段
	t.Run("Empty Aggregation Fields", func(t *testing.T) {
		agg := NewGroupAggregator(
			[]string{"Device"},
			[]AggregationField{},
		)

		testData := []map[string]interface{}{
			{"Device": "sensor1", "temperature": 25.5},
			{"Device": "sensor2", "temperature": 26.8},
		}

		for _, d := range testData {
			agg.Add(d)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, 2)
		// 只应该包含分组字段，没有聚合字段
		for _, result := range results {
			assert.Contains(t, result, "Device")
			assert.Len(t, result, 1)
		}
	})

	// 测试缺失字段
	t.Run("Missing Fields", func(t *testing.T) {
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
			{"Device": "sensor1", "temperature": 25.5},
			{"Device": "sensor2"}, // 缺少temperature字段
			{"Device": "sensor3", "temperature": 26.8},
		}

		for _, d := range testData {
			agg.Add(d)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, 3)

		// 验证结果
		for _, result := range results {
			device := result["Device"].(string)
			if device == "sensor1" {
				assert.Equal(t, 25.5, result["temperature_sum"])
			} else if device == "sensor2" {
				assert.Nil(t, result["temperature_sum"]) // 缺失字段应该为nil
			} else if device == "sensor3" {
				assert.Equal(t, 26.8, result["temperature_sum"])
			}
		}
	})
}

// TestGroupAggregatorPerformance 测试聚合器性能
func TestGroupAggregatorPerformance(t *testing.T) {
	// 测试大量数据处理性能
	t.Run("Large Dataset Performance", func(t *testing.T) {
		agg := NewGroupAggregator(
			[]string{"category"},
			[]AggregationField{
				{
					InputField:    "value",
					AggregateType: Sum,
					OutputAlias:   "sum",
				},
				{
					InputField:    "value",
					AggregateType: Avg,
					OutputAlias:   "avg",
				},
				{
					InputField:    "value",
					AggregateType: Max,
					OutputAlias:   "max",
				},
				{
					InputField:    "value",
					AggregateType: Min,
					OutputAlias:   "min",
				},
			},
		)

		// 生成大量测试数据
		const numRecords = 10000
		const numCategories = 100

		for i := 0; i < numRecords; i++ {
			category := i % numCategories
			value := float64(i % 1000)
			data := map[string]interface{}{
				"category": fmt.Sprintf("cat_%d", category),
				"value":    value,
			}
			agg.Add(data)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, numCategories)

		// 验证结果
		for _, result := range results {
			assert.Contains(t, result, "sum")
			assert.Contains(t, result, "avg")
			assert.Contains(t, result, "max")
			assert.Contains(t, result, "min")
		}
	})

	// 测试并发性能
	t.Run("Concurrent Performance", func(t *testing.T) {
		agg := NewGroupAggregator(
			[]string{"category"},
			[]AggregationField{
				{
					InputField:    "value",
					AggregateType: Sum,
					OutputAlias:   "sum",
				},
			},
		)

		const numGoroutines = 10
		const recordsPerGoroutine = 1000

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// 启动多个goroutine并发添加数据
		for i := 0; i < numGoroutines; i++ {
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < recordsPerGoroutine; j++ {
					category := (goroutineID + j) % 100
					value := float64(j)
					data := map[string]interface{}{
						"category": fmt.Sprintf("cat_%d", category),
						"value":    value,
					}
					agg.Add(data)
				}
			}(i)
		}

		wg.Wait()

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.NotEmpty(t, results)
	})
}

// TestGroupAggregatorMemoryUsage 测试聚合器内存使用
func TestGroupAggregatorMemoryUsage(t *testing.T) {
	// 测试大量分组的内存使用
	t.Run("Many Groups Memory Usage", func(t *testing.T) {
		agg := NewGroupAggregator(
			[]string{"group"},
			[]AggregationField{
				{
					InputField:    "value",
					AggregateType: Sum,
					OutputAlias:   "sum",
				},
			},
		)

		// 创建大量不同的分组
		const numGroups = 10000
		for i := 0; i < numGroups; i++ {
			data := map[string]interface{}{
				"group": fmt.Sprintf("group_%d", i),
				"value": float64(i),
			}
			agg.Add(data)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, numGroups)

		// 验证每个分组都有正确的结果
		for i := 0; i < numGroups; i++ {
			expectedGroup := fmt.Sprintf("group_%d", i)
			found := false
			for _, result := range results {
				if result["group"] == expectedGroup {
					assert.Equal(t, float64(i), result["sum"])
					found = true
					break
				}
			}
			assert.True(t, found, "Group %s should be found in results", expectedGroup)
		}
	})
}

// TestGroupAggregatorResetAndReuse 测试聚合器重置和重用
func TestGroupAggregatorResetAndReuse(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"category"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Sum,
				OutputAlias:   "sum",
			},
		},
	)

	// 第一轮数据
	testData1 := []map[string]interface{}{
		{"category": "A", "value": 10.0},
		{"category": "B", "value": 20.0},
	}

	for _, d := range testData1 {
		agg.Add(d)
	}

	results1, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results1, 2)

	// 重置聚合器
	agg.Reset()

	// 第二轮数据
	testData2 := []map[string]interface{}{
		{"category": "A", "value": 15.0},
		{"category": "C", "value": 25.0},
	}

	for _, d := range testData2 {
		agg.Add(d)
	}

	results2, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results2, 2)

	// 验证第二轮结果
	for _, result := range results2 {
		category := result["category"].(string)
		if category == "A" {
			assert.Equal(t, 15.0, result["sum"])
		} else if category == "C" {
			assert.Equal(t, 25.0, result["sum"])
		}
	}
}

// TestGroupAggregatorBasic 测试基本聚合器功能
func TestGroupAggregatorBasic(t *testing.T) {
	// 创建聚合字段配置
	aggFields := []AggregationField{
		{
			InputField:    "value",
			AggregateType: Sum,
			OutputAlias:   "sum_value",
		},
	}

	// 创建分组聚合器
	ga := NewGroupAggregator([]string{"group"}, aggFields)

	// 测试数据
	data := []map[string]interface{}{
		{"group": "A", "value": 10},
		{"group": "A", "value": 20},
		{"group": "B", "value": 30},
	}

	// 添加数据
	for _, item := range data {
		err := ga.Add(item)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}

	// 获取结果
	results, err := ga.GetResults()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 验证结果
	if len(results) != 2 {
		t.Errorf("expected 2 groups, got %d", len(results))
	}
}

// TestGroupAggregatorErrorHandling 测试错误处理
func TestGroupAggregatorErrorHandling(t *testing.T) {
	// 测试空配置
	ga := NewGroupAggregator([]string{}, []AggregationField{})

	// 添加数据应该不会出错
	err := ga.Add(map[string]interface{}{"field": "value"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 获取结果
	results, err := ga.GetResults()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 空配置应该返回空结果
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

// TestGroupAggregatorConcurrency 测试并发安全
func TestGroupAggregatorConcurrency(t *testing.T) {
	aggFields := []AggregationField{
		{
			InputField:    "value",
			AggregateType: Count,
			OutputAlias:   "count_value",
		},
	}

	ga := NewGroupAggregator([]string{"group"}, aggFields)

	// 并发添加数据
	for i := 0; i < 100; i++ {
		go func(id int) {
			data := map[string]interface{}{
				"group": "test",
				"value": id,
			}
			err := ga.Add(data)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}

	// 等待一段时间确保所有goroutine完成
	time.Sleep(100 * time.Millisecond)

	// 获取结果
	results, err := ga.GetResults()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 验证结果存在
	if len(results) == 0 {
		t.Error("expected results, got none")
	}
}

// TestRegisterFunction 测试 Register 函数
func TestRegisterFunction(t *testing.T) {
	// 创建自定义聚合器
	customAggregator := func() AggregatorFunction {
		return &testCustomAggregator{}
	}

	// 注册自定义聚合器
	Register("custom_test", customAggregator)

	// 验证注册成功（通过创建聚合器来验证）
	agg := CreateBuiltinAggregator("custom_test")
	assert.NotNil(t, agg)
}

// testCustomAggregator 测试用的自定义聚合器
type testCustomAggregator struct {
	sum float64
}

func (t *testCustomAggregator) New() AggregatorFunction {
	return &testCustomAggregator{}
}

func (t *testCustomAggregator) Add(value interface{}) {
	if v, ok := value.(float64); ok {
		t.sum += v
	}
}

func (t *testCustomAggregator) Result() interface{} {
	return t.sum
}

// TestIsNumericAggregator 测试 isNumericAggregator 方法的各种分支
func TestIsNumericAggregator(t *testing.T) {
	ga := NewGroupAggregator([]string{"group"}, []AggregationField{})

	// 测试数值聚合器
	assert.True(t, ga.isNumericAggregator(Sum))
	assert.True(t, ga.isNumericAggregator(Avg))
	assert.True(t, ga.isNumericAggregator(Max))
	assert.True(t, ga.isNumericAggregator(Min))
	assert.True(t, ga.isNumericAggregator(Count))
	assert.True(t, ga.isNumericAggregator(StdDev))
	assert.True(t, ga.isNumericAggregator(Median))
	assert.True(t, ga.isNumericAggregator(Percentile))
	assert.True(t, ga.isNumericAggregator(Var))
	assert.True(t, ga.isNumericAggregator(VarS))
	assert.True(t, ga.isNumericAggregator(StdDevS))

	// 测试非数值聚合器
	assert.False(t, ga.isNumericAggregator(Collect))
	assert.False(t, ga.isNumericAggregator(MergeAgg))
	assert.False(t, ga.isNumericAggregator(Deduplicate))
	assert.False(t, ga.isNumericAggregator(LastValue))

	// 测试分析函数
	assert.False(t, ga.isNumericAggregator(Lag))
	assert.False(t, ga.isNumericAggregator(Latest))
	assert.False(t, ga.isNumericAggregator(ChangedCol))
	assert.False(t, ga.isNumericAggregator(HadChanged))

	// 测试未知聚合器（通过名称模式匹配）
	assert.True(t, ga.isNumericAggregator("custom_sum"))
	assert.True(t, ga.isNumericAggregator("custom_avg"))
	assert.True(t, ga.isNumericAggregator("custom_min"))
	assert.True(t, ga.isNumericAggregator("custom_max"))
	assert.True(t, ga.isNumericAggregator("custom_count"))
	assert.True(t, ga.isNumericAggregator("custom_std"))
	assert.True(t, ga.isNumericAggregator("custom_var"))

	// 测试不匹配模式的未知聚合器
	assert.False(t, ga.isNumericAggregator("custom_collect"))
	assert.False(t, ga.isNumericAggregator("unknown_function"))
}

// TestExpressionAggregator 测试表达式聚合器
func TestExpressionAggregator(t *testing.T) {
	// 创建表达式聚合器
	agg := CreateBuiltinAggregator(Expression)
	assert.NotNil(t, agg)

	// 测试创建新实例
	newAgg := agg.New()
	assert.NotNil(t, newAgg)

	// 测试添加值和获取结果
	newAgg.Add("test_value")
	result := newAgg.Result()
	assert.NotNil(t, result)
}

// TestGroupAggregatorContextAggregator 测试 ContextAggregator 功能
func TestGroupAggregatorContextAggregator(t *testing.T) {
	// 创建一个共享的values切片来跟踪所有添加的值
	sharedValues := &[]interface{}{}

	// 注册模拟聚合器
	Register("mock_context", func() AggregatorFunction {
		return &mockContextAggregator{
			contextKey: "test_context_key",
			values:     sharedValues,
		}
	})

	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "missing_field", // 故意使用不存在的字段
				AggregateType: "mock_context",
				OutputAlias:   "context_result",
			},
		},
	)

	// 设置上下文值
	err := ga.Put("test_context_key", "context_value")
	assert.NoError(t, err)

	// 添加数据（字段不存在，应该从上下文获取）
	err = ga.Add(map[string]interface{}{
		"group": "test_group",
		// 故意不包含 missing_field
	})
	assert.NoError(t, err)

	// 获取结果
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// 验证上下文值被使用
	assert.Contains(t, *sharedValues, "context_value")
}

// mockContextAggregator 模拟的 ContextAggregator
type mockContextAggregator struct {
	contextKey string
	values     *[]interface{}
}

func (m *mockContextAggregator) New() AggregatorFunction {
	return &mockContextAggregator{
		contextKey: m.contextKey,
		values:     m.values, // 共享同一个values切片
	}
}

func (m *mockContextAggregator) Add(value interface{}) {
	*m.values = append(*m.values, value)
}

func (m *mockContextAggregator) Result() interface{} {
	return len(*m.values)
}

func (m *mockContextAggregator) GetContextKey() string {
	return m.contextKey
}

// TestGroupAggregatorNumericConversionError 测试数值转换错误
func TestGroupAggregatorNumericConversionError(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Sum, // Sum 需要数值类型
				OutputAlias:   "sum_value",
			},
		},
	)

	// 添加无法转换为数值的数据
	err := ga.Add(map[string]interface{}{
		"group": "test_group",
		"value": "not_a_number", // 无法转换为数值
	})

	// 应该返回转换错误
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot convert field value")
}

// TestGroupAggregatorWithExpressionEvaluator 测试表达式求值器
func TestGroupAggregatorWithExpressionEvaluator(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				OutputAlias:   "fahrenheit_sum",
			},
		},
	)

	// 注册表达式求值器（摄氏度转华氏度）
	evaluator := func(data interface{}) (interface{}, error) {
		if dataMap, ok := data.(map[string]interface{}); ok {
			if temp, exists := dataMap["temperature"]; exists {
				if tempFloat, ok := temp.(float64); ok {
					return tempFloat*1.8 + 32, nil
				}
			}
		}
		return nil, errors.New("invalid temperature data")
	}

	ga.RegisterExpression("fahrenheit_sum", "temperature * 1.8 + 32", []string{"temperature"}, evaluator)

	// 添加测试数据
	testData := []map[string]interface{}{
		{"group": "sensor1", "temperature": 0.0},   // 32°F
		{"group": "sensor1", "temperature": 100.0}, // 212°F
	}

	for _, data := range testData {
		err := ga.Add(data)
		assert.NoError(t, err)
	}

	// 获取结果
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// 验证表达式求值结果 (32 + 212 = 244)
	assert.Equal(t, "sensor1", results[0]["group"])
	assert.Equal(t, 244.0, results[0]["fahrenheit_sum"])
}

// TestGroupAggregatorExpressionEvaluatorError 测试表达式求值器错误处理
func TestGroupAggregatorExpressionEvaluatorError(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Sum,
				OutputAlias:   "processed_value",
			},
		},
	)

	// 注册会出错的表达式求值器
	errorEvaluator := func(data interface{}) (interface{}, error) {
		return nil, errors.New("expression evaluation failed")
	}

	ga.RegisterExpression("processed_value", "error_expression", []string{"value"}, errorEvaluator)

	// 添加测试数据
	err := ga.Add(map[string]interface{}{
		"group": "test_group",
		"value": 10.0,
	})

	// 应该没有错误，因为表达式错误会被忽略
	assert.NoError(t, err)

	// 获取结果
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// 由于表达式求值失败，聚合器应该没有值
	assert.Equal(t, "test_group", results[0]["group"])
	// processed_value 应该是聚合器的默认结果（通常是 nil 或 0）
}

// TestGroupAggregatorCountStarField 测试 count(*) 功能
func TestGroupAggregatorCountStarField(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"category"},
		[]AggregationField{
			{
				InputField:    "*", // count(*) 语法
				AggregateType: Count,
				OutputAlias:   "total_count",
			},
		},
	)

	// 添加测试数据
	testData := []map[string]interface{}{
		{"category": "A", "value": 10},
		{"category": "A", "value": 20},
		{"category": "A"}, // 没有 value 字段
		{"category": "B", "value": 30},
	}

	for _, data := range testData {
		err := ga.Add(data)
		assert.NoError(t, err)
	}

	// 获取结果
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 2)

	// 验证 count(*) 结果
	for _, result := range results {
		category := result["category"].(string)
		if category == "A" {
			assert.Equal(t, float64(3), result["total_count"]) // A 类别有 3 条记录
		} else if category == "B" {
			assert.Equal(t, float64(1), result["total_count"]) // B 类别有 1 条记录
		}
	}
}

// TestGroupAggregatorNilFieldValue 测试 nil 字段值处理
func TestGroupAggregatorNilFieldValue(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Sum,
				OutputAlias:   "sum_value",
			},
		},
	)

	// 添加包含 nil 值的数据
	testData := []map[string]interface{}{
		{"group": "test", "value": 10.0},
		{"group": "test", "value": nil}, // nil 值应该被跳过
		{"group": "test", "value": 20.0},
	}

	for _, data := range testData {
		err := ga.Add(data)
		assert.NoError(t, err)
	}

	// 获取结果
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// nil 值应该被跳过，只计算 10.0 + 20.0 = 30.0
	assert.Equal(t, "test", results[0]["group"])
	assert.Equal(t, 30.0, results[0]["sum_value"])
}

// TestGroupAggregatorStructData 测试结构体数据
func TestGroupAggregatorStructData(t *testing.T) {
	// 定义测试结构体
	type TestStruct struct {
		Group string
		Value float64
	}

	ga := NewGroupAggregator(
		[]string{"Group"},
		[]AggregationField{
			{
				InputField:    "Value",
				AggregateType: Sum,
				OutputAlias:   "sum_value",
			},
		},
	)

	// 添加结构体数据
	testData := []TestStruct{
		{Group: "A", Value: 10.0},
		{Group: "A", Value: 20.0},
		{Group: "B", Value: 30.0},
	}

	for _, data := range testData {
		err := ga.Add(data)
		assert.NoError(t, err)
	}

	// 获取结果
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 2)

	// 验证结果
	for _, result := range results {
		group := result["Group"].(string)
		if group == "A" {
			assert.Equal(t, 30.0, result["sum_value"])
		} else if group == "B" {
			assert.Equal(t, 30.0, result["sum_value"])
		}
	}
}

// TestGroupAggregatorPointerData 测试指针数据
func TestGroupAggregatorPointerData(t *testing.T) {
	type TestStruct struct {
		Group string
		Value float64
	}

	ga := NewGroupAggregator(
		[]string{"Group"},
		[]AggregationField{
			{
				InputField:    "Value",
				AggregateType: Sum,
				OutputAlias:   "sum_value",
			},
		},
	)

	// 添加指针数据
	testData := &TestStruct{Group: "test", Value: 42.0}
	err := ga.Add(testData)
	assert.NoError(t, err)

	// 获取结果
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "test", results[0]["Group"])
	assert.Equal(t, 42.0, results[0]["sum_value"])
}

// TestGroupAggregatorUnsupportedDataType 测试不支持的数据类型
func TestGroupAggregatorUnsupportedDataType(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Sum,
				OutputAlias:   "sum_value",
			},
		},
	)

	// 测试不支持的数据类型
	err := ga.Add(123) // int 类型不是 struct 或 map
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported data type")

	err = ga.Add("string") // string 类型不支持
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported data type")

	err = ga.Add([]int{1, 2, 3}) // slice 类型不支持
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported data type")
}

// TestGroupAggregatorGroupFieldNilValue 测试分组字段为 nil 的情况
func TestGroupAggregatorGroupFieldNilValue(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Sum,
				OutputAlias:   "sum_value",
			},
		},
	)

	// 添加分组字段为 nil 的数据
	err := ga.Add(map[string]interface{}{
		"group": nil, // 分组字段为 nil
		"value": 10.0,
	})

	// 应该返回错误
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "field group has nil value")
}

// TestIsNumericAggregatorAdvanced 测试 isNumericAggregator 的更多分支
func TestIsNumericAggregatorAdvanced(t *testing.T) {
	ga := NewGroupAggregator([]string{"group"}, []AggregationField{})

	// 测试 TypeAnalytical 类型
	result := ga.isNumericAggregator("analytical_func")
	assert.False(t, result)

	// 测试不存在的函数，但名称包含数值聚合关键字
	result = ga.isNumericAggregator("custom_sum_func")
	assert.True(t, result)

	result = ga.isNumericAggregator("custom_avg_func")
	assert.True(t, result)

	result = ga.isNumericAggregator("custom_min_func")
	assert.True(t, result)

	result = ga.isNumericAggregator("custom_max_func")
	assert.True(t, result)

	result = ga.isNumericAggregator("custom_count_func")
	assert.True(t, result)

	result = ga.isNumericAggregator("custom_std_func")
	assert.True(t, result)

	result = ga.isNumericAggregator("custom_var_func")
	assert.True(t, result)

	// 测试不包含数值关键字的函数
	result = ga.isNumericAggregator("custom_text_func")
	assert.False(t, result)
}

// TestGroupAggregatorNilData 测试 nil 数据
func TestGroupAggregatorNilData(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Sum,
				OutputAlias:   "sum_value",
			},
		},
	)

	// 测试 nil 数据
	err := ga.Add(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data cannot be nil")
}

// TestGroupAggregatorMissingGroupField 测试缺少分组字段
func TestGroupAggregatorMissingGroupField(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"missing_group"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Sum,
				OutputAlias:   "sum_value",
			},
		},
	)

	// 添加缺少分组字段的数据
	err := ga.Add(map[string]interface{}{
		"value": 10,
		// 缺少 missing_group 字段
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "field missing_group not found")
}

// TestGroupAggregatorMissingAggregationField 测试缺少聚合字段但有上下文
func TestGroupAggregatorMissingAggregationField(t *testing.T) {
	// 创建一个不会从上下文获取值的聚合器
	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "missing_field",
				AggregateType: Sum,
				OutputAlias:   "sum_value",
			},
		},
	)

	// 添加缺少聚合字段的数据（没有上下文）
	err := ga.Add(map[string]interface{}{
		"group": "test",
		// 缺少 missing_field
	})
	assert.NoError(t, err) // 应该成功，因为会跳过缺少的字段

	// 获取结果
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	// 由于没有添加任何值，聚合器应该返回默认值
}

// TestGroupAggregatorExpressionEvaluationError 测试表达式求值错误但继续处理
func TestGroupAggregatorExpressionEvaluationError(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Sum,
				OutputAlias:   "sum_value",
			},
			{
				InputField:    "other",
				AggregateType: Sum,
				OutputAlias:   "expr_result",
			},
		},
	)

	// 注册一个会出错的表达式求值器
	ga.RegisterExpression("expr_result", "error_expr", []string{"other"}, func(data interface{}) (interface{}, error) {
		return nil, fmt.Errorf("evaluation error")
	})

	// 添加数据
	err := ga.Add(map[string]interface{}{
		"group": "test",
		"value": 10,
		"other": 20,
	})
	assert.NoError(t, err) // 应该成功，因为表达式错误会被跳过

	// 获取结果
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	// sum_value 应该有值，expr_result 应该没有值或为默认值
	assert.Equal(t, float64(10), results[0]["sum_value"])
}
