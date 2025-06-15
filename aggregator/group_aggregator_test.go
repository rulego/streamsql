package aggregator

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	// 测试没有指定别名的情况，应该使用输入字段名作为输出字段名
	agg := NewGroupAggregator(
		[]string{"Device"},
		[]AggregationField{
			{
				InputField:    "temperature",
				AggregateType: Sum,
				// OutputAlias 留空，应该使用 InputField
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
