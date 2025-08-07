package functions

import "fmt"

// AggregatorFunction defines the interface for aggregator functions that support incremental computation
type AggregatorFunction interface {
	Function
	// New creates a new aggregator instance
	New() AggregatorFunction
	// Add adds a value for incremental computation
	Add(value interface{})
	// Result returns the aggregation result
	Result() interface{}
	// Reset resets the aggregator state
	Reset()
	// Clone clones the aggregator (used for window functions and similar scenarios)
	Clone() AggregatorFunction
}

// AnalyticalFunction defines the interface for analytical functions with state management
// Now inherits from AggregatorFunction to support incremental computation
type AnalyticalFunction interface {
	AggregatorFunction
}

// CreateAggregator creates an aggregator instance
func CreateAggregator(name string) (AggregatorFunction, error) {
	fn, exists := Get(name)
	if !exists {
		return nil, fmt.Errorf("aggregator function %s not found", name)
	}

	if aggFn, ok := fn.(AggregatorFunction); ok {
		return aggFn.New(), nil
	}

	return nil, fmt.Errorf("function %s is not an aggregator function", name)
}

// CreateAnalytical creates an analytical function instance
func CreateAnalytical(name string) (AnalyticalFunction, error) {
	fn, exists := Get(name)
	if !exists {
		return nil, fmt.Errorf("analytical function %s not found", name)
	}

	if analFn, ok := fn.(AnalyticalFunction); ok {
		return analFn.New().(AnalyticalFunction), nil
	}

	return nil, fmt.Errorf("function %s is not an analytical function", name)
}
