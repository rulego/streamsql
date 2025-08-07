package functions

import (
	"sync"
)

// AggregatorAdapter provides adapter for aggregator functions, compatible with legacy aggregator interface
type AggregatorAdapter struct {
	aggFunc AggregatorFunction
}

// NewAggregatorAdapter creates an aggregator adapter
func NewAggregatorAdapter(name string) (*AggregatorAdapter, error) {
	aggFunc, err := CreateAggregator(name)
	if err != nil {
		return nil, err
	}

	return &AggregatorAdapter{
		aggFunc: aggFunc,
	}, nil
}

// New creates a new aggregator instance
func (a *AggregatorAdapter) New() interface{} {
	return &AggregatorAdapter{
		aggFunc: a.aggFunc.New(),
	}
}

// Add adds a value
func (a *AggregatorAdapter) Add(value interface{}) {
	a.aggFunc.Add(value)
}

// Result returns the result
func (a *AggregatorAdapter) Result() interface{} {
	return a.aggFunc.Result()
}

// GetFunctionName returns the underlying function name for context mechanism support
func (a *AggregatorAdapter) GetFunctionName() string {
	if a.aggFunc != nil {
		return a.aggFunc.GetName()
	}
	return ""
}

// AnalyticalAdapter provides adapter for analytical functions
type AnalyticalAdapter struct {
	analFunc AnalyticalFunction
}

// NewAnalyticalAdapter creates an analytical function adapter
func NewAnalyticalAdapter(name string) (*AnalyticalAdapter, error) {
	analFunc, err := CreateAnalytical(name)
	if err != nil {
		return nil, err
	}

	return &AnalyticalAdapter{
		analFunc: analFunc,
	}, nil
}

// Execute executes the analytical function
func (a *AnalyticalAdapter) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return a.analFunc.Execute(ctx, args)
}

// Reset resets the state
func (a *AnalyticalAdapter) Reset() {
	a.analFunc.Reset()
}

// Clone clones the instance
func (a *AnalyticalAdapter) Clone() *AnalyticalAdapter {
	return &AnalyticalAdapter{
		analFunc: a.analFunc.Clone(),
	}
}

// Global adapter registry
var (
	aggregatorAdapters = make(map[string]func() interface{})
	analyticalAdapters = make(map[string]func() *AnalyticalAdapter)
	adapterMutex       sync.RWMutex
)

// RegisterAggregatorAdapter registers an aggregator adapter
func RegisterAggregatorAdapter(name string) error {
	adapterMutex.Lock()
	defer adapterMutex.Unlock()

	aggregatorAdapters[name] = func() interface{} {
		adapter, err := NewAggregatorAdapter(name)
		if err != nil {
			return nil
		}
		return adapter
	}
	return nil
}

// RegisterAnalyticalAdapter registers analytical function adapter
func RegisterAnalyticalAdapter(name string) error {
	adapterMutex.Lock()
	defer adapterMutex.Unlock()

	analyticalAdapters[name] = func() *AnalyticalAdapter {
		adapter, err := NewAnalyticalAdapter(name)
		if err != nil {
			return nil
		}
		return adapter
	}
	return nil
}

// GetAggregatorAdapter gets aggregator adapter
func GetAggregatorAdapter(name string) (func() interface{}, bool) {
	adapterMutex.RLock()
	defer adapterMutex.RUnlock()

	constructor, exists := aggregatorAdapters[name]
	return constructor, exists
}

// GetAnalyticalAdapter gets analytical function adapter
func GetAnalyticalAdapter(name string) (func() *AnalyticalAdapter, bool) {
	adapterMutex.RLock()
	defer adapterMutex.RUnlock()

	constructor, exists := analyticalAdapters[name]
	return constructor, exists
}

// CreateBuiltinAggregatorFromFunctions creates aggregator from functions module
func CreateBuiltinAggregatorFromFunctions(aggType string) interface{} {
	// First try to get from adapter registry
	if constructor, exists := GetAggregatorAdapter(aggType); exists {
		return constructor()
	}

	// If not found, try to create directly
	adapter, err := NewAggregatorAdapter(aggType)
	if err != nil {
		return nil
	}

	return adapter
}

// CreateAnalyticalFromFunctions creates analytical function from functions module
func CreateAnalyticalFromFunctions(funcType string) *AnalyticalAdapter {
	// First try to get from adapter registry
	if constructor, exists := GetAnalyticalAdapter(funcType); exists {
		return constructor()
	}

	// If not found, try to create directly
	adapter, err := NewAnalyticalAdapter(funcType)
	if err != nil {
		return nil
	}

	return adapter
}
