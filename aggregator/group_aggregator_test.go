package aggregator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testData struct {
	Device string
	Data1  float64
	Data2  float64
}

func TestGroupAggregator_MultiFieldSum(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		map[string]AggregateType{
			"Data1": Sum,
			"Data2": Sum,
		},
		map[string]string{
			"Data1": "Data1_sum",
			"Data2": "Data2_sum",
		},
	)

	testData := []map[string]interface{}{
		{"Device": "aa", "Data1": 20, "Data2": 30},
		{"Device": "aa", "Data1": 21, "Data2": 0},
		{"Device": "bb", "Data1": 15, "Data2": 20},
		{"Device": "bb", "Data1": 16, "Data2": 20},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]interface{}{
		{"Device": "aa", "Data1_sum": 41.0, "Data2_sum": 30.0},
		{"Device": "bb", "Data1_sum": 31.0, "Data2_sum": 40.0},
	}

	results, _ := agg.GetResults()
	assert.ElementsMatch(t, expected, results)
}

func TestGroupAggregator_SingleField(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		map[string]AggregateType{
			"Data1": Sum,
		},
		map[string]string{
			"Data1": "Data1_sum",
		},
	)

	testData := []map[string]interface{}{
		{"Device": "cc", "Data1": 10},
		{"Device": "cc", "Data1": 20},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]interface{}{
		{"Device": "cc", "Data1_sum": 30.0},
	}

	results, _ := agg.GetResults()
	assert.ElementsMatch(t, expected, results)
}

func TestGroupAggregator_MultipleAggregators(t *testing.T) {
	agg := NewGroupAggregator(
		[]string{"Device"},
		map[string]AggregateType{
			"Data1": Sum,
			"Data2": Avg,
			"Data3": Max,
			"Data4": Min,
		},
		map[string]string{
			"Data1": "Data1_sum",
			"Data2": "Data2_avg",
			"Data3": "Data3_max",
			"Data4": "Data4_min",
		},
	)

	testData := []map[string]interface{}{
		{"Device": "cc", "Data1": 10, "Data2": 5.5, "Data3": 8, "Data4": 3},
		{"Device": "cc", "Data1": 20, "Data2": 4.5, "Data3": 12, "Data4": 2},
	}

	for _, d := range testData {
		agg.Add(d)
	}

	expected := []map[string]interface{}{
		{
			"Device":    "cc",
			"Data1_sum": 30.0,
			"Data2_avg": 5.0,
			"Data3_max": 12.0,
			"Data4_min": 2.0,
		},
	}

	results, _ := agg.GetResults()
	assert.ElementsMatch(t, expected, results)
}
