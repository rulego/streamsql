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

// ParameterizedFunction defines the interface for functions that need parameter initialization
type ParameterizedFunction interface {
	AggregatorFunction
	// Init initializes the function with parsed arguments
	Init(args []interface{}) error
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

// CreateParameterizedAggregator creates a parameterized aggregator instance with initialization
func CreateParameterizedAggregator(name string, args []interface{}) (AggregatorFunction, error) {
	fn, exists := Get(name)
	if !exists {
		return nil, fmt.Errorf("aggregator function %s not found", name)
	}

	// Check if it's a parameterized function
	if paramFn, ok := fn.(ParameterizedFunction); ok {
		newInstance := paramFn.New()
		if paramNewInstance, ok := newInstance.(ParameterizedFunction); ok {
			if err := paramNewInstance.Init(args); err != nil {
				return nil, fmt.Errorf("failed to initialize parameterized function %s: %v", name, err)
			}
			return newInstance, nil
		}
	}

	// Fallback to regular aggregator creation
	if aggFn, ok := fn.(AggregatorFunction); ok {
		return aggFn.New(), nil
	}

	return nil, fmt.Errorf("function %s is not an aggregator function", name)
}

// IsAggregatorFunction checks if a function name is an aggregator function
func IsAggregatorFunction(name string) bool {
	fn, exists := Get(name)
	if !exists {
		return false
	}
	_, ok := fn.(AggregatorFunction)
	return ok
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
