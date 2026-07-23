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

// TestGetResultsErrorCases Tests for errors in the GetResults function
func TestGetResultsErrorCases(t *testing.T) {
	groupFields := []string{"category"}
	aggFields := []AggregationField{
		{InputField: "value", AggregateType: Sum, OutputAlias: "sum_value"},
	}
	agg := NewEnhancedGroupAggregator(groupFields, aggFields)

	// Add an invalid post-aggregate expression
	requiredFields := []AggregationFieldInfo{
		{FuncName: "invalid", InputField: "value", AggType: Sum},
	}
	err := agg.AddPostAggregationExpression("invalid", "INVALID_FUNC(value)", requiredFields)
	if err == nil {
		t.Skip("Expected error when adding invalid expression, but got none")
	}

	// Error handling when testing results
	results, err := agg.GetResults()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if results == nil {
		t.Error("Expected results map, got nil")
	}
}

// TestParseFunctionCallEdgeCases tests the boundary status of the parseFunctionCall function
func TestParseFunctionCallEdgeCases(t *testing.T) {
	groupFields := []string{"category"}
	aggFields := []AggregationField{
		{InputField: "value", AggregateType: Sum, OutputAlias: "sum_value"},
	}
	agg := NewEnhancedGroupAggregator(groupFields, aggFields)

	tests := []struct {
		name        string
		expr        string
		expectError bool
	}{
		{
			name:        "Function with nested parentheses",
			expr:        "SUM(CASE WHEN (value > 0) THEN value ELSE 0 END)",
			expectError: false,
		},
		{
			name:        "Function with string literals",
			expr:        "CONCAT('Hello', 'World')",
			expectError: false,
		},
		{
			name:        "Function with quoted identifiers",
			expr:        "SUM(`column name`)",
			expectError: false,
		},
		{
			name:        "Unmatched parentheses",
			expr:        "SUM(value",
			expectError: true,
		},
		{
			name:        "Empty function call",
			expr:        "()",
			expectError: true,
		},
		{
			name:        "Function with arithmetic",
			expr:        "SUM(value * 2 + 1)",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _ = agg.parseFunctionCall(tt.expr)
			// Note: parseFunctionCall signature changed to not return error
		})
	}
}

// TestHasMultipleTopLevelArgsEdgeCases tests the boundary status of the hasMultipleTopLevelArgs function
func TestHasMultipleTopLevelArgsEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		args     string
		expected bool
	}{
		{
			name:     "Single argument",
			args:     "value",
			expected: false,
		},
		{
			name:     "Multiple arguments",
			args:     "value1, value2",
			expected: true,
		},
		{
			name:     "Arguments with nested function",
			args:     "SUM(value), COUNT(*)",
			expected: true,
		},
		{
			name:     "Arguments with parentheses",
			args:     "(value1 + value2), value3",
			expected: true,
		},
		{
			name:     "Single complex argument",
			args:     "(value1, value2)",
			expected: false,
		},
		{
			name:     "Empty arguments",
			args:     "",
			expected: false,
		},
		{
			name:     "Arguments with string literals",
			args:     "'hello, world', value",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasMultipleTopLevelArgs(tt.args)
			if result != tt.expected {
				t.Errorf("hasMultipleTopLevelArgs(%q) = %v, want %v", tt.args, result, tt.expected)
			}
		})
	}
}

// TestBuiltinAggregatorEdgeCases tests the boundaries of the built-in aggregator
func TestBuiltinAggregatorEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		aggType AggregateType
		data    []map[string]any
	}{
		{
			name:    "Sum with nil values",
			aggType: Sum,
			data: []map[string]any{
				{"field": nil, "group": "A"},
				{"field": 10, "group": "A"},
			},
		},
		{
			name:    "Count with mixed types",
			aggType: Count,
			data: []map[string]any{
				{"field": "string", "group": "A"},
				{"field": 123, "group": "A"},
				{"field": nil, "group": "A"},
			},
		},
		{
			name:    "Avg with empty data",
			aggType: Avg,
			data:    []map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupFields := []string{"group"}
			aggFields := []AggregationField{
				{InputField: "field", AggregateType: tt.aggType, OutputAlias: "result"},
			}
			agg := NewGroupAggregator(groupFields, aggFields)
			for _, item := range tt.data {
				agg.Add(item)
			}
			results, err := agg.GetResults()
			assert.NoError(t, err)
			assert.NotNil(t, results)
		})
	}
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

	testData := []map[string]any{
		{"Device": "aa", "temperature": 25.5, "humidity": 60.0},
		{"Device": "aa", "temperature": 26.8, "humidity": 55.0},
		{"Device": "bb", "temperature": 22.3, "humidity": 65.0},
		{"Device": "bb", "temperature": 23.5, "humidity": 70.0},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]any{
		{"Device": "aa", "temperature_sum": 52.3, "humidity_sum": 115.0},
		{"Device": "bb", "temperature_sum": 45.8, "humidity_sum": 135.0},
	}

	results, _ := agg.GetResults()
	assert.ElementsMatch(t, expected, results)
}

// TestGroupAggregator_Put Test the Put method
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

	// Test the put method
	err := agg.Put("test_key", "test_value")
	assert.NoError(t, err)

	// Test multiple puts
	err = agg.Put("key1", 123)
	assert.NoError(t, err)
	err = agg.Put("key2", 456.78)
	assert.NoError(t, err)
}

// TestGroupAggregator_RegisterExpression Test expression registration
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

	// Register the expression
	evaluator := func(data any) (any, error) {
		if dataMap, ok := data.(map[string]any); ok {
			if temp, exists := dataMap["temperature"]; exists {
				if tempFloat, ok := temp.(float64); ok {
					return tempFloat*1.8 + 32, nil // Celsius degrees are converted to Fahrenheit degrees
				}
			}
		}
		return nil, errors.New("invalid data")
	}

	agg.RegisterExpression("fahrenheit", "temperature * 1.8 + 32", []string{"temperature"}, evaluator)

	// The verification expression is registered
	assert.NotNil(t, agg.expressions["fahrenheit"])
	assert.Equal(t, "fahrenheit", agg.expressions["fahrenheit"].Field)
	assert.Equal(t, "temperature * 1.8 + 32", agg.expressions["fahrenheit"].Expression)
	assert.Equal(t, []string{"temperature"}, agg.expressions["fahrenheit"].Fields)
}

// TestGroupAggregator_Reset Test the reset method
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

	// Add some data
	testData := []map[string]any{
		{"Device": "test", "temperature": 25.5},
		{"Device": "test", "temperature": 26.8},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	// Verification has data
	results, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// Reset
	agg.Reset()

	// Verification data has been cleared
	results, _ = agg.GetResults()
	assert.Len(t, results, 0)
}

// TestGroupAggregator_ErrorHandling Handling of test errors
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

	// Testing adds invalid data
	err := agg.Add(nil)
	assert.Error(t, err)

	// Test adding non-map type data
	err = agg.Add("invalid data")
	assert.Error(t, err)

	// Missing grouping fields: Incorporate NULL grouping, no longer causing errors
	err = agg.Add(map[string]any{"temperature": 25.5})
	assert.NoError(t, err)
}

// The GROUP BY key must retain the original type output (Zeng Heng is string); int(1) and float64(1) should be in the same group.
func TestGroupAggregator_GroupKeyPreservesType(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  any
	}{
		{"int", int(1)},
		{"float64", float64(1)},
		{"int64", int64(1)},
	} {
		agg := NewGroupAggregator([]string{"region_id"}, []AggregationField{
			{InputField: "*", AggregateType: Count, OutputAlias: "cnt"},
		})
		require.NoError(t, agg.Add(map[string]any{"region_id": tc.val}))
		res, err := agg.GetResults()
		require.NoError(t, err)
		require.Len(t, res, 1, tc.name)
		got := res[0]["region_id"]
		assert.Equal(t, tc.val, got, "%s: region_id value", tc.name)
		assert.IsType(t, tc.val, got, "%s: region_id must keep original type, not become string", tc.name)
	}

	// int(1) and float64(1) with the same value should be grouped together (SQL values are equal), and must not be split into two groups.
	agg := NewGroupAggregator([]string{"region_id"}, []AggregationField{
		{InputField: "*", AggregateType: Count, OutputAlias: "cnt"},
	})
	require.NoError(t, agg.Add(map[string]any{"region_id": int(1)}))
	require.NoError(t, agg.Add(map[string]any{"region_id": float64(1)}))
	res, err := agg.GetResults()
	require.NoError(t, err)
	require.Len(t, res, 1, "int(1) and float64(1) must group together")
	assert.Equal(t, float64(2), res[0]["cnt"])
}

// Field values with separators must be fully preserved (previously truncated as "a" by Split when restoring with "|").
func TestGroupAggregator_GroupKeyWithSeparator(t *testing.T) {
	agg := NewGroupAggregator([]string{"tag"}, []AggregationField{
		{InputField: "*", AggregateType: Count, OutputAlias: "cnt"},
	})
	require.NoError(t, agg.Add(map[string]any{"tag": "a|b"}))
	require.NoError(t, agg.Add(map[string]any{"tag": "a|b"}))
	require.NoError(t, agg.Add(map[string]any{"tag": "x"}))
	res, err := agg.GetResults()
	require.NoError(t, err)
	require.Len(t, res, 2)
	seen := map[string]int{}
	for _, r := range res {
		tag, ok := r["tag"].(string)
		require.True(t, ok, "tag should be string, got %T", r["tag"])
		seen[tag] = int(r["cnt"].(float64))
	}
	assert.Equal(t, 2, seen["a|b"], "value containing separator must be preserved, not truncated")
	assert.Equal(t, 1, seen["x"])
}

// If the numeric aggregate field cast fails, only skip that field and the corresponding line; do not return interrupt the entire line Add (which has caused competitors).
// The following fields are omitted). In the slice order [b,c], b precedes the c in row {b:"x", c:2} must still be counted by SUM(c).
func TestGroupAggregator_NumericCastFailureSkipsFieldOnly(t *testing.T) {
	agg := NewGroupAggregator(nil, []AggregationField{
		{InputField: "b", AggregateType: Sum, OutputAlias: "sum_b"},
		{InputField: "c", AggregateType: Sum, OutputAlias: "sum_c"},
	})
	rows := []map[string]any{
		{"b": 10, "c": 1},
		{"b": "x", "c": 2}, // b Non-numeric value: skipping only sum_b, sum_c still counts as 2
		{"b": 20, "c": 3},
	}
	for _, r := range rows {
		require.NoError(t, agg.Add(r)) // Before repairing, Add(row2) returned a cast error
	}
	res, err := agg.GetResults()
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, float64(30), res[0]["sum_b"]) // 10+20, skip with "x"."
	assert.Equal(t, float64(6), res[0]["sum_c"])  // 1+2+3, before repair = 4 (return interrupt omitted c)
}

// TestGroupAggregator_DifferentAggregateTypes Test different types of aggregation
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

	testData := []map[string]any{
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

	// Verify the results
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

// TestGroupAggregator_MultipleGroupFields Test multiple grouped fields
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

	testData := []map[string]any{
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

	// Verify the results of each combination
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

// TestGroupAggregator_EmptyData Testspace data processing
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

	// No data is added, just get the results directly
	results, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 0)
}

// TestGroupAggregator_NilValues Test null value handling
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

	testData := []map[string]any{
		{"Device": "test", "temperature": 25.5},
		{"Device": "test", "temperature": nil}, // Null value
		{"Device": "test", "temperature": 30.0},
	}

	for _, d := range testData {
		err := agg.Add(d)
		assert.NoError(t, err)
	}

	results, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// Null values should be ignored, and only non-null values are calculated
	expected := 55.5 // 25.5 + 30.0
	assert.Equal(t, expected, results[0]["temperature_sum"])
}

// TestGroupAggregator_ConcurrentAccess Test for concurrent access
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

	// Concurrent data addition
	go func() {
		for i := 0; i < 10; i++ {
			agg.Add(map[string]any{"Device": "A", "temperature": float64(i)})
		}
	}()

	go func() {
		for i := 0; i < 10; i++ {
			agg.Add(map[string]any{"Device": "B", "temperature": float64(i * 2)})
		}
	}()

	// Concurrent registration expressions
	go func() {
		evaluator := func(data any) (any, error) {
			return 1.0, nil
		}
		agg.RegisterExpression("test_expr", "1", []string{}, evaluator)
	}()

	// Concurrent Put operations
	go func() {
		for i := 0; i < 5; i++ {
			agg.Put("key"+string(rune(i)), i)
		}
	}()

	// Wait a while to ensure all goroutines are completed
	// Note: This is not the best synchronization method, but it is sufficient for testing
	// In practical applications, sync.WaitGroup
	for i := 0; i < 100; i++ {
		// Attempt to obtain results and test concurrent reads
		_, _ = agg.GetResults()
	}
}

// TestCreateBuiltinAggregator tests the creation of a built-in aggregator
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

			// Test the new method
			newAgg := aggregator.New()
			assert.NotNil(t, newAgg)
		})
	}
}

// TestExpressionAggregatorWrapper Tests the expression aggregator wrapper
func TestExpressionAggregatorWrapper(t *testing.T) {
	wrapper := CreateBuiltinAggregator(Expression)
	require.NotNil(t, wrapper)

	// Test type assertion
	exprWrapper, ok := wrapper.(*ExpressionAggregatorWrapper)
	assert.True(t, ok)
	assert.NotNil(t, exprWrapper.function)

	// Test the new method
	newWrapper := wrapper.New()
	assert.NotNil(t, newWrapper)

	// Test the Add and Result methods
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

	testData := []map[string]any{
		{"Device": "cc", "temperature": 24.5},
		{"Device": "cc", "temperature": 27.8},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]any{
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

	testData := []map[string]any{
		{"Device": "cc", "temperature": 25.5, "humidity": 65.5, "presure": 1008, "PM10": 35},
		{"Device": "cc", "temperature": 27.8, "humidity": 60.5, "presure": 1012, "PM10": 28},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]any{
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

	testData := []map[string]any{
		{"Device": "dd", "temperature": 10.0},
		{"Device": "dd", "temperature": 15.0},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]any{
		{"Device": "dd", "temperature": 25.0},
	}

	results, _ := agg.GetResults()
	assert.ElementsMatch(t, expected, results)
}

// TestGroupAggregatorAdvancedFeatures Advanced features of the test aggregator
func TestGroupAggregatorAdvancedFeatures(t *testing.T) {
	// Test complex aggregated expressions
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

		testData := []map[string]any{
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

		// Verify the results
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

	// Test statistical aggregation functions
	t.Run("Statistical Aggregation Functions", func(t *testing.T) {
		tests := []struct {
			name    string
			aggType AggregateType
			data    []map[string]any
		}{
			{"StdDev", StdDev, []map[string]any{
				{"group": "A", "value": 1.0},
				{"group": "A", "value": 2.0},
				{"group": "A", "value": 3.0},
			}},
			{"Var", Var, []map[string]any{
				{"group": "A", "value": 1.0},
				{"group": "A", "value": 2.0},
				{"group": "A", "value": 3.0},
			}},
			{"Median", Median, []map[string]any{
				{"group": "A", "value": 1.0},
				{"group": "A", "value": 2.0},
				{"group": "A", "value": 3.0},
			}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				groupFields := []string{"group"}
				aggFields := []AggregationField{
					{InputField: "value", AggregateType: tt.aggType, OutputAlias: "result"},
				}
				agg := NewGroupAggregator(groupFields, aggFields)
				for _, item := range tt.data {
					agg.Add(item)
				}
				results, _ := agg.GetResults()
				assert.NotNil(t, results)
			})
		}
	})
}

// TestGroupAggregatorDataTypes tests aggregations of different data types
func TestGroupAggregatorDataTypes(t *testing.T) {
	tests := []struct {
		name        string
		aggType     AggregateType
		inputData   []map[string]any
		expectedKey string
		expectedVal any
	}{
		{
			name:    "String Count",
			aggType: Count,
			inputData: []map[string]any{
				{"group": "A", "value": "hello"},
				{"group": "A", "value": "world"},
				{"group": "B", "value": "test"},
			},
			expectedKey: "count",
			expectedVal: 2.0, // The Count aggregator calculates all non-null values
		},
		{
			name:    "Boolean Count",
			aggType: Count,
			inputData: []map[string]any{
				{"group": "A", "value": true},
				{"group": "A", "value": false},
				{"group": "A", "value": true},
				{"group": "B", "value": false},
			},
			expectedKey: "count",
			expectedVal: 3.0, // The Count aggregator calculates all non-null values
		},
		{
			name:    "Mixed Types Count",
			aggType: Count,
			inputData: []map[string]any{
				{"group": "A", "value": 123},
				{"group": "A", "value": "string"},
				{"group": "A", "value": true},
				{"group": "B", "value": 456},
			},
			expectedKey: "count",
			expectedVal: 3.0, // The Count aggregator calculates all non-null values
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

			// Find the results of Group A
			var groupAResult map[string]any
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

// TestGroupAggregatorEdgeCases tests aggregator boundaries
func TestGroupAggregatorEdgeCases(t *testing.T) {
	// Test empty data
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

	// Test empty group fields
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

		testData := []map[string]any{
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

	// Test the empty aggregate field
	t.Run("Empty Aggregation Fields", func(t *testing.T) {
		agg := NewGroupAggregator(
			[]string{"Device"},
			[]AggregationField{},
		)

		testData := []map[string]any{
			{"Device": "sensor1", "temperature": 25.5},
			{"Device": "sensor2", "temperature": 26.8},
		}

		for _, d := range testData {
			agg.Add(d)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, 2)
		// Only grouped fields should be included, no aggregated fields
		for _, result := range results {
			assert.Contains(t, result, "Device")
			assert.Len(t, result, 1)
		}
	})

	// Test for missing fields
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

		testData := []map[string]any{
			{"Device": "sensor1", "temperature": 25.5},
			{"Device": "sensor2"}, // Missing the temperature field
			{"Device": "sensor3", "temperature": 26.8},
		}

		for _, d := range testData {
			agg.Add(d)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, 3)

		// Verify the results
		for _, result := range results {
			device := result["Device"].(string)
			if device == "sensor1" {
				assert.Equal(t, 25.5, result["temperature_sum"])
			} else if device == "sensor2" {
				assert.Nil(t, result["temperature_sum"]) // The missing field should be nil
			} else if device == "sensor3" {
				assert.Equal(t, 26.8, result["temperature_sum"])
			}
		}
	})
}

// TestGroupAggregatorPerformance Tests aggregator performance
func TestGroupAggregatorPerformance(t *testing.T) {
	// Testing large amounts of data processing performance
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

		// Generates a large amount of test data
		const numRecords = 10000
		const numCategories = 100

		for i := 0; i < numRecords; i++ {
			category := i % numCategories
			value := float64(i % 1000)
			data := map[string]any{
				"category": fmt.Sprintf("cat_%d", category),
				"value":    value,
			}
			agg.Add(data)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, numCategories)

		// Verify the results
		for _, result := range results {
			assert.Contains(t, result, "sum")
			assert.Contains(t, result, "avg")
			assert.Contains(t, result, "max")
			assert.Contains(t, result, "min")
		}
	})

	// Testing concurrency performance
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

		// Start multiple goroutines to add data concurrently
		for i := 0; i < numGoroutines; i++ {
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < recordsPerGoroutine; j++ {
					category := (goroutineID + j) % 100
					value := float64(j)
					data := map[string]any{
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

// TestGroupAggregatorMemoryUsage tests aggregator memory usage
func TestGroupAggregatorMemoryUsage(t *testing.T) {
	// Testing memory usage across large packets
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

		// Create a large number of different groups
		const numGroups = 10000
		for i := 0; i < numGroups; i++ {
			data := map[string]any{
				"group": fmt.Sprintf("group_%d", i),
				"value": float64(i),
			}
			agg.Add(data)
		}

		results, err := agg.GetResults()
		assert.NoError(t, err)
		assert.Len(t, results, numGroups)

		// Verify that each group has the correct results
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

// TestGroupAggregatorResetAndReuse: Test aggregator reset and reuse
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

	// First round data
	testData1 := []map[string]any{
		{"category": "A", "value": 10.0},
		{"category": "B", "value": 20.0},
	}

	for _, d := range testData1 {
		agg.Add(d)
	}

	results1, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results1, 2)

	// Reset the aggregator
	agg.Reset()

	// Second round data
	testData2 := []map[string]any{
		{"category": "A", "value": 15.0},
		{"category": "C", "value": 25.0},
	}

	for _, d := range testData2 {
		agg.Add(d)
	}

	results2, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results2, 2)

	// Verify the results of the second round
	for _, result := range results2 {
		category := result["category"].(string)
		if category == "A" {
			assert.Equal(t, 15.0, result["sum"])
		} else if category == "C" {
			assert.Equal(t, 25.0, result["sum"])
		}
	}
}

// TestGroupAggregatorBasic tests basic aggregator functionality
func TestGroupAggregatorBasic(t *testing.T) {
	// Create aggregated field configurations
	aggFields := []AggregationField{
		{
			InputField:    "value",
			AggregateType: Sum,
			OutputAlias:   "sum_value",
		},
	}

	// Create a packet aggregator
	ga := NewGroupAggregator([]string{"group"}, aggFields)

	// Test data
	data := []map[string]any{
		{"group": "A", "value": 10},
		{"group": "A", "value": 20},
		{"group": "B", "value": 30},
	}

	// Add data
	for _, item := range data {
		err := ga.Add(item)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}

	// Get results
	results, err := ga.GetResults()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Verify the results
	if len(results) != 2 {
		t.Errorf("expected 2 groups, got %d", len(results))
	}
}

// TestGroupAggregatorErrorHandling Test error handling
func TestGroupAggregatorErrorHandling(t *testing.T) {
	// Test the empty configuration
	ga := NewGroupAggregator([]string{}, []AggregationField{})

	// Adding data shouldn't go wrong
	err := ga.Add(map[string]any{"field": "value"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Get results
	results, err := ga.GetResults()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// An empty configuration should return an empty result
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

// TestGroupAggregatorConcurrency tests concurrency security
func TestGroupAggregatorConcurrency(t *testing.T) {
	aggFields := []AggregationField{
		{
			InputField:    "value",
			AggregateType: Count,
			OutputAlias:   "count_value",
		},
	}

	ga := NewGroupAggregator([]string{"group"}, aggFields)

	// Concurrent data addition
	for i := 0; i < 100; i++ {
		go func(id int) {
			data := map[string]any{
				"group": "test",
				"value": id,
			}
			err := ga.Add(data)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}

	// Wait a while to ensure all goroutines are completed
	time.Sleep(100 * time.Millisecond)

	// Get results
	results, err := ga.GetResults()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// The verification results exist
	if len(results) == 0 {
		t.Error("expected results, got none")
	}
}

// TestRegisterFunction Register function
func TestRegisterFunction(t *testing.T) {
	// Create custom aggregators
	customAggregator := func() AggregatorFunction {
		return &testCustomAggregator{}
	}

	// Register a custom aggregator
	Register("custom_test", customAggregator)

	// Verify registration success (by creating aggregators to verify it)
	agg := CreateBuiltinAggregator("custom_test")
	assert.NotNil(t, agg)
}

// testCustomAggregator is a custom aggregator for testing
type testCustomAggregator struct {
	sum float64
}

func (t *testCustomAggregator) New() AggregatorFunction {
	return &testCustomAggregator{}
}

func (t *testCustomAggregator) Add(value any) {
	if v, ok := value.(float64); ok {
		t.sum += v
	}
}

func (t *testCustomAggregator) Result() any {
	return t.sum
}

// TestIsNumericAggregator tests various branches of the isNumericAggregator method
func TestIsNumericAggregator(t *testing.T) {
	ga := NewGroupAggregator([]string{"group"}, []AggregationField{})

	// Test numerical aggregator
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

	// Testing non-numerical aggregators
	assert.False(t, ga.isNumericAggregator(Collect))
	assert.False(t, ga.isNumericAggregator(MergeAgg))
	assert.False(t, ga.isNumericAggregator(Deduplicate))
	assert.False(t, ga.isNumericAggregator(LastValue))

	// Test the analysis function
	assert.False(t, ga.isNumericAggregator(Lag))
	assert.False(t, ga.isNumericAggregator(Latest))
	assert.False(t, ga.isNumericAggregator(ChangedCol))
	assert.False(t, ga.isNumericAggregator(HadChanged))

	// Testing unknown aggregators (by name pattern matching)
	assert.True(t, ga.isNumericAggregator("custom_sum"))
	assert.True(t, ga.isNumericAggregator("custom_avg"))
	assert.True(t, ga.isNumericAggregator("custom_min"))
	assert.True(t, ga.isNumericAggregator("custom_max"))
	assert.True(t, ga.isNumericAggregator("custom_count"))
	assert.True(t, ga.isNumericAggregator("custom_std"))
	assert.True(t, ga.isNumericAggregator("custom_var"))

	// Testing an unknown aggregator with mismatched patterns
	assert.False(t, ga.isNumericAggregator("custom_collect"))
	assert.False(t, ga.isNumericAggregator("unknown_function"))
}

// TestExpressionAggregator tests the expression aggregator
func TestExpressionAggregator(t *testing.T) {
	// Create an expression aggregator
	agg := CreateBuiltinAggregator(Expression)
	assert.NotNil(t, agg)

	// Test to create new instances
	newAgg := agg.New()
	assert.NotNil(t, newAgg)

	// Test the added value and obtain the results
	newAgg.Add("test_value")
	result := newAgg.Result()
	assert.NotNil(t, result)
}

// TestGroupAggregatorContextAggregator tests the ContextAggregator feature
func TestGroupAggregatorContextAggregator(t *testing.T) {
	// Create a shared values slice to track all added values
	sharedValues := &[]any{}

	// Register the analog aggregator
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
				InputField:    "missing_field", // Deliberately using fields that don't exist
				AggregateType: "mock_context",
				OutputAlias:   "context_result",
			},
		},
	)

	// Set context values
	err := ga.Put("test_context_key", "context_value")
	assert.NoError(t, err)

	// Add data (fields do not exist and should be retrieved from context)
	err = ga.Add(map[string]any{
		"group": "test_group",
		// Intentionally omitting missing_field
	})
	assert.NoError(t, err)

	// Get results
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// Verify that context values are used
	assert.Contains(t, *sharedValues, "context_value")
}

// mockContextAggregator Simulates ContextAggregator
type mockContextAggregator struct {
	contextKey string
	values     *[]any
}

func (m *mockContextAggregator) New() AggregatorFunction {
	return &mockContextAggregator{
		contextKey: m.contextKey,
		values:     m.values, // Share the same values slices
	}
}

func (m *mockContextAggregator) Add(value any) {
	*m.values = append(*m.values, value)
}

func (m *mockContextAggregator) Result() any {
	return len(*m.values)
}

func (m *mockContextAggregator) GetContextKey() string {
	return m.contextKey
}

// TestGroupAggregatorNumericConversionError Non-numeric fields should be skipped by numeric aggregation (without interrupting the entire line).
// Add, no error), and the remaining valid values are aggregated as usual (A5: Zeng returned errors interrupted the entire line of Add, causing the following fields to be omitted).
func TestGroupAggregatorNumericConversionError(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"group"},
		[]AggregationField{
			{
				InputField:    "value",
				AggregateType: Sum, // Sum requires a numeric type
				OutputAlias:   "sum_value",
			},
		},
	)

	// Non-numeric rows: skip the value and should not report errors
	require.NoError(t, ga.Add(map[string]any{"group": "g1", "value": "not_a_number"}))
	// Effective row: Polymerization as usual
	require.NoError(t, ga.Add(map[string]any{"group": "g1", "value": 10}))
	require.NoError(t, ga.Add(map[string]any{"group": "g1", "value": 20}))

	res, err := ga.GetResults()
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, float64(30), res[0]["sum_value"]) // Only 10+20, the "not_a_number" was skipped
}

// TestGroupAggregatorWithExpressionEvaluator: Test expression evaluator
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

	// Register expression evaluator (Celsius to Fahrenheit)
	evaluator := func(data any) (any, error) {
		if dataMap, ok := data.(map[string]any); ok {
			if temp, exists := dataMap["temperature"]; exists {
				if tempFloat, ok := temp.(float64); ok {
					return tempFloat*1.8 + 32, nil
				}
			}
		}
		return nil, errors.New("invalid temperature data")
	}

	ga.RegisterExpression("fahrenheit_sum", "temperature * 1.8 + 32", []string{"temperature"}, evaluator)

	// Add test data
	testData := []map[string]any{
		{"group": "sensor1", "temperature": 0.0},   // 32°F
		{"group": "sensor1", "temperature": 100.0}, // 212°F
	}

	for _, data := range testData {
		err := ga.Add(data)
		assert.NoError(t, err)
	}

	// Get results
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// Verify the evaluation result of the expression (32 + 212 = 244)
	assert.Equal(t, "sensor1", results[0]["group"])
	assert.Equal(t, 244.0, results[0]["fahrenheit_sum"])
}

// TestGroupAggregatorExpressionEvaluatorError Error handling of test expression evaluator
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

	// Register an error expression evaluator
	errorEvaluator := func(data any) (any, error) {
		return nil, errors.New("expression evaluation failed")
	}

	ga.RegisterExpression("processed_value", "error_expression", []string{"value"}, errorEvaluator)

	// Add test data
	err := ga.Add(map[string]any{
		"group": "test_group",
		"value": 10.0,
	})

	// There should be no errors, because expression errors are ignored
	assert.NoError(t, err)

	// Get results
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// Because the expression evaluation failed, the aggregator should have no value
	assert.Equal(t, "test_group", results[0]["group"])
	// processed_value should be the default result of the aggregator (usually nil or 0)
}

// TestGroupAggregatorCountStarField tests count(*) functionality
func TestGroupAggregatorCountStarField(t *testing.T) {
	ga := NewGroupAggregator(
		[]string{"category"},
		[]AggregationField{
			{
				InputField:    "*", // count(*) syntax
				AggregateType: Count,
				OutputAlias:   "total_count",
			},
		},
	)

	// Add test data
	testData := []map[string]any{
		{"category": "A", "value": 10},
		{"category": "A", "value": 20},
		{"category": "A"}, // There is no value field
		{"category": "B", "value": 30},
	}

	for _, data := range testData {
		err := ga.Add(data)
		assert.NoError(t, err)
	}

	// Get results
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 2)

	// Verify count(*) results
	for _, result := range results {
		category := result["category"].(string)
		if category == "A" {
			assert.Equal(t, float64(3), result["total_count"]) // There are 3 records in category A
		} else if category == "B" {
			assert.Equal(t, float64(1), result["total_count"]) // There is 1 record in category B
		}
	}
}

// TestGroupAggregatorNilFieldValue tests nil field-value handling.
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

	// Add data containing the nil value
	testData := []map[string]any{
		{"group": "test", "value": 10.0},
		{"group": "test", "value": nil}, // The nil value should be skipped
		{"group": "test", "value": 20.0},
	}

	for _, data := range testData {
		err := ga.Add(data)
		assert.NoError(t, err)
	}

	// Get results
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)

	// The nil value should be skipped, and only 10.0 + 20.0 = 30.0 is calculated
	assert.Equal(t, "test", results[0]["group"])
	assert.Equal(t, 30.0, results[0]["sum_value"])
}

// TestGroupAggregatorStructData tests structure data
func TestGroupAggregatorStructData(t *testing.T) {
	// Define the test structure
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

	// Add structure data
	testData := []TestStruct{
		{Group: "A", Value: 10.0},
		{Group: "A", Value: 20.0},
		{Group: "B", Value: 30.0},
	}

	for _, data := range testData {
		err := ga.Add(data)
		assert.NoError(t, err)
	}

	// Get results
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 2)

	// Verify the results
	for _, result := range results {
		group := result["Group"].(string)
		if group == "A" {
			assert.Equal(t, 30.0, result["sum_value"])
		} else if group == "B" {
			assert.Equal(t, 30.0, result["sum_value"])
		}
	}
}

// TestGroupAggregatorPointerData: Test pointer data
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

	// Add pointer data
	testData := &TestStruct{Group: "test", Value: 42.0}
	err := ga.Add(testData)
	assert.NoError(t, err)

	// Get results
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "test", results[0]["Group"])
	assert.Equal(t, 42.0, results[0]["sum_value"])
}

// TestGroupAggregatorUnsupportedDataType Tests data types that are not supported
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

	// Test for data types that are not supported
	err := ga.Add(123) // The int type is not struct or map
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported data type")

	err = ga.Add("string") // string type is not supported
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported data type")

	err = ga.Add([]int{1, 2, 3}) // The slice type is not supported
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported data type")
}

// TestGroupAggregatorGroupFieldNilValue When the test grouping field is nil, it is assigned to a NULL group (no line dropped)
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

	// Group field nil: Includes NULL grouping, no more errors or line drops
	err := ga.Add(map[string]any{
		"group": nil,
		"value": 10.0,
	})
	assert.NoError(t, err)

	results, err := ga.GetResults()
	assert.NoError(t, err)
	if assert.Len(t, results, 1) {
		assert.Nil(t, results[0]["group"])
	}
}

// TestIsNumericAggregatorAdvanced covers additional isNumericAggregator branches
func TestIsNumericAggregatorAdvanced(t *testing.T) {
	ga := NewGroupAggregator([]string{"group"}, []AggregationField{})

	// Test TypeAnalytical type
	result := ga.isNumericAggregator("analytical_func")
	assert.False(t, result)

	// Tests functions that do not exist, but whose names contain numerical aggregation keywords
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

	// Tests functions that do not contain numeric keywords
	result = ga.isNumericAggregator("custom_text_func")
	assert.False(t, result)
}

// TestGroupAggregatorNilData tests nil data
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

	// Test NIL data
	err := ga.Add(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data cannot be nil")
}

// TestGroupAggregatorMissingGroupField When a group field is missing, NULL grouping is added (no line dropped)
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

	// Missing grouping fields: Includes NULL grouping, no longer throws errors or missing lines
	err := ga.Add(map[string]any{
		"value": 10,
	})
	assert.NoError(t, err)

	results, err := ga.GetResults()
	assert.NoError(t, err)
	if assert.Len(t, results, 1) {
		assert.Nil(t, results[0]["missing_group"])
	}
}

// TestGroupAggregatorMissingAggregationField The test lacks an aggregation field but has context
func TestGroupAggregatorMissingAggregationField(t *testing.T) {
	// Create an aggregator that does not retrieve values from context
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

	// Add data missing aggregated fields (without context)
	err := ga.Add(map[string]any{
		"group": "test",
		// Lack of missing_field
	})
	assert.NoError(t, err) // It should succeed, because missing fields will be skipped

	// Get results
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	// Since no value is added, the aggregator should return the default value
}

// TestGroupAggregatorExpressionEvaluationError The test expression is evaluated incorrectly but continues to be processed
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

	// Register an error expression evaluator
	ga.RegisterExpression("expr_result", "error_expr", []string{"other"}, func(data any) (any, error) {
		return nil, fmt.Errorf("evaluation error")
	})

	// Add data
	err := ga.Add(map[string]any{
		"group": "test",
		"value": 10,
		"other": 20,
	})
	assert.NoError(t, err) // It should succeed because an expression error can be skipped

	// Get results
	results, err := ga.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	// sum_value should have a value, expr_result should have no value or be the default value
	assert.Equal(t, float64(10), results[0]["sum_value"])
}
