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
func (a *AggregatorAdapter) New() any {
	return &AggregatorAdapter{
		aggFunc: a.aggFunc.New(),
	}
}

// Add adds a value
func (a *AggregatorAdapter) Add(value any) {
	a.aggFunc.Add(value)
}

// Result returns the result
func (a *AggregatorAdapter) Result() any {
	return a.aggFunc.Result()
}

// GetFunctionName returns the underlying function name for context mechanism support
func (a *AggregatorAdapter) GetFunctionName() string {
	if a.aggFunc != nil {
		return a.aggFunc.GetName()
	}
	return ""
}

// Global adapter registry
var (
	aggregatorAdapters = make(map[string]func() any)
	adapterMutex       sync.RWMutex
)

// RegisterAggregatorAdapter 注册聚合器适配器。通常无需手动调用：
// functions.Register 注册 TypeAggregation 函数时会自动调用本函数（见 registry.go）。
// 仅在需要为已注册函数单独补建适配器时才显式调用。
func RegisterAggregatorAdapter(name string) error {
	adapterMutex.Lock()
	defer adapterMutex.Unlock()

	aggregatorAdapters[name] = func() any {
		adapter, err := NewAggregatorAdapter(name)
		if err != nil {
			return nil
		}
		return adapter
	}
	return nil
}

// GetAggregatorAdapter gets aggregator adapter
func GetAggregatorAdapter(name string) (func() any, bool) {
	adapterMutex.RLock()
	defer adapterMutex.RUnlock()

	constructor, exists := aggregatorAdapters[name]
	return constructor, exists
}

// CreateBuiltinAggregatorFromFunctions creates aggregator from functions module
func CreateBuiltinAggregatorFromFunctions(aggType string) any {
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
