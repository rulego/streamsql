package functions

import (
	"testing"
)

// TestAggregateTypeConstants Tests the aggregation type constants
func TestAggregateTypeConstants(t *testing.T) {
	tests := []struct {
		name     string
		aggType  AggregateType
		expected string
	}{
		{"Sum", Sum, "sum"},
		{"Count", Count, "count"},
		{"Avg", Avg, "avg"},
		{"Max", Max, "max"},
		{"Min", Min, "min"},
		{"Median", Median, "median"},
		{"Percentile", Percentile, "percentile"},
		{"WindowStart", WindowStart, "window_start"},
		{"WindowEnd", WindowEnd, "window_end"},
		{"Collect", Collect, "collect"},
		{"LastValue", LastValue, "last_value"},
		{"MergeAgg", MergeAgg, "merge_agg"},
		{"StdDev", StdDev, "stddev"},
		{"StdDevS", StdDevS, "stddevs"},
		{"Deduplicate", Deduplicate, "deduplicate"},
		{"Var", Var, "var"},
		{"VarS", VarS, "vars"},
		{"Lag", Lag, "lag"},
		{"Latest", Latest, "latest"},
		{"ChangedCol", ChangedCol, "changed_col"},
		{"HadChanged", HadChanged, "had_changed"},
		{"Expression", Expression, "expression"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.aggType) != tt.expected {
				t.Errorf("AggregateType %s = %s, want %s", tt.name, string(tt.aggType), tt.expected)
			}
		})
	}
}

// TestStringConstants tests string constants
func TestStringConstants(t *testing.T) {
	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{"SumStr", SumStr, "sum"},
		{"CountStr", CountStr, "count"},
		{"AvgStr", AvgStr, "avg"},
		{"MaxStr", MaxStr, "max"},
		{"MinStr", MinStr, "min"},
		{"MedianStr", MedianStr, "median"},
		{"PercentileStr", PercentileStr, "percentile"},
		{"WindowStartStr", WindowStartStr, "window_start"},
		{"WindowEndStr", WindowEndStr, "window_end"},
		{"CollectStr", CollectStr, "collect"},
		{"LastValueStr", LastValueStr, "last_value"},
		{"MergeAggStr", MergeAggStr, "merge_agg"},
		{"StdDevStr", StdDevStr, "stddev"},
		{"StdDevSStr", StdDevSStr, "stddevs"},
		{"DeduplicateStr", DeduplicateStr, "deduplicate"},
		{"VarStr", VarStr, "var"},
		{"VarSStr", VarSStr, "vars"},
		{"LagStr", LagStr, "lag"},
		{"LatestStr", LatestStr, "latest"},
		{"ChangedColStr", ChangedColStr, "changed_col"},
		{"HadChangedStr", HadChangedStr, "had_changed"},
		{"ExpressionStr", ExpressionStr, "expression"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.constant != tt.expected {
				t.Errorf("Constant %s = %s, want %s", tt.name, tt.constant, tt.expected)
			}
		})
	}
}

// TestRegisterLegacyAggregator tests the Legacy aggregator
func TestRegisterLegacyAggregator(t *testing.T) {
	// Create a test aggregator constructor
	constructor := func() LegacyAggregatorFunction {
		return &TestLegacyAggregator{}
	}

	// Register aggregators
	RegisterLegacyAggregator("test_agg", constructor)

	// Verify registration successfully
	legacyRegistryMutex.RLock()
	_, exists := legacyAggregatorRegistry["test_agg"]
	legacyRegistryMutex.RUnlock()

	if !exists {
		t.Error("Failed to register legacy aggregator")
	}

	// Test the creation of aggregators
	createdAgg := CreateLegacyAggregator("test_agg")
	if createdAgg == nil {
		t.Error("Failed to create legacy aggregator")
	}

	// Test the aggregator function
	createdAgg.Add(10)
	createdAgg.Add(20)
	result := createdAgg.Result()
	if result != 30 {
		t.Errorf("Expected result 30, got %v", result)
	}
}

// TestCreateLegacyAggregatorUnsupportedReturnsNil verifies an unsupported
// aggregator type returns nil instead of panicking (so a registration gap
// degrades to an empty field rather than crashing the engine). Unknown aggregate
// types are rejected at parse time.
func TestCreateLegacyAggregatorUnsupportedReturnsNil(t *testing.T) {
	agg := CreateLegacyAggregator("nonexistent_aggregator")
	if agg != nil {
		t.Errorf("expected nil for unsupported aggregator type, got %T", agg)
	}
}

// TestCreateLegacyAggregatorPercentile verifies percentile aggregates without
// panicking (H9: percentile was registered as a plain function that did not
// implement AggregatorFunction, so CreateLegacyAggregator panicked).
func TestCreateLegacyAggregatorPercentile(t *testing.T) {
	agg := CreateLegacyAggregator(Percentile)
	if agg == nil {
		t.Fatal("CreateLegacyAggregator(percentile) returned nil")
	}
	agg.Add(1.0)
	agg.Add(2.0)
	agg.Add(3.0)
	if r := agg.Result(); r == nil {
		t.Error("percentile Result() returned nil")
	}
}

// TestFunctionAggregatorWrapper is a test function aggregator wrapper
func TestFunctionAggregatorWrapper(t *testing.T) {
	// Create a test aggregator function
	testAgg := &TestAggregatorFunction{}

	// Create a test adapter
	adapter := &AggregatorAdapter{
		aggFunc: testAgg,
	}
	wrapper := &FunctionAggregatorWrapper{adapter: adapter}

	// Test the new method
	newWrapper := wrapper.New()
	if newWrapper == nil {
		t.Error("New() should return a new wrapper")
	}

	// Test the GetContextKey method
	contextKey := wrapper.GetContextKey()
	if contextKey != "" {
		t.Logf("Context key: %s", contextKey)
	}
}

// TestLegacyAggregator implementation for testing
type TestLegacyAggregator struct {
	sum int
}

// New: Create a new aggregator instance
func (t *TestLegacyAggregator) New() LegacyAggregatorFunction {
	return &TestLegacyAggregator{}
}

// Add value
func (t *TestLegacyAggregator) Add(value any) {
	if v, ok := value.(int); ok {
		t.sum += v
	}
}

// Result: Returns the aggregated result
func (t *TestLegacyAggregator) Result() any {
	return t.sum
}

// TestAggregatorFunction is an aggregator function implemented for testing
type TestAggregatorFunction struct {
	sum int
}

// New: Create a new aggregator instance
func (t *TestAggregatorFunction) New() AggregatorFunction {
	return &TestAggregatorFunction{}
}

// Add value
func (t *TestAggregatorFunction) Add(value any) {
	if v, ok := value.(int); ok {
		t.sum += v
	}
}

// Result: Returns the aggregated result
func (t *TestAggregatorFunction) Result() any {
	return t.sum
}

// Reset: Resets the aggregator state
func (t *TestAggregatorFunction) Reset() {
	t.sum = 0
}

// Clone cloning aggregator
func (t *TestAggregatorFunction) Clone() AggregatorFunction {
	return &TestAggregatorFunction{sum: t.sum}
}

// GetName returns the function name
func (t *TestAggregatorFunction) GetName() string {
	return "test_aggregator"
}

// GetType returns the function type
func (t *TestAggregatorFunction) GetType() FunctionType {
	return TypeAggregation
}

// GetCategory returns the function classification
func (t *TestAggregatorFunction) GetCategory() string {
	return "test"
}

// GetDescription returns the function description
func (t *TestAggregatorFunction) GetDescription() string {
	return "Test aggregator function"
}

// GetAliases returns the function alias
func (t *TestAggregatorFunction) GetAliases() []string {
	return []string{}
}

// Validate the parameters
func (t *TestAggregatorFunction) Validate(args []any) error {
	return nil
}

// Execute the function
func (t *TestAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return t.Result(), nil
}

// GetMinArgs returns the minimum number of parameters
func (t *TestAggregatorFunction) GetMinArgs() int {
	return 1
}

// GetMaxArgs returns the maximum number of parameters
func (t *TestAggregatorFunction) GetMaxArgs() int {
	return 1
}
