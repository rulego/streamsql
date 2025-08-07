package functions

import (
	"sync"
)

// AggregateType defines aggregate types, migrated from aggregator.AggregateType
type AggregateType string

const (
	Sum         AggregateType = "sum"
	Count       AggregateType = "count"
	Avg         AggregateType = "avg"
	Max         AggregateType = "max"
	Min         AggregateType = "min"
	Median      AggregateType = "median"
	Percentile  AggregateType = "percentile"
	WindowStart AggregateType = "window_start"
	WindowEnd   AggregateType = "window_end"
	Collect     AggregateType = "collect"
	LastValue   AggregateType = "last_value"
	MergeAgg    AggregateType = "merge_agg"
	StdDev      AggregateType = "stddev"
	StdDevS     AggregateType = "stddevs"
	Deduplicate AggregateType = "deduplicate"
	Var         AggregateType = "var"
	VarS        AggregateType = "vars"
	// Analytical functions
	Lag        AggregateType = "lag"
	Latest     AggregateType = "latest"
	ChangedCol AggregateType = "changed_col"
	HadChanged AggregateType = "had_changed"
	// Expression aggregator for handling custom functions
	Expression AggregateType = "expression"
)

// String constant versions for convenience
const (
	SumStr         = string(Sum)
	CountStr       = string(Count)
	AvgStr         = string(Avg)
	MaxStr         = string(Max)
	MinStr         = string(Min)
	MedianStr      = string(Median)
	PercentileStr  = string(Percentile)
	WindowStartStr = string(WindowStart)
	WindowEndStr   = string(WindowEnd)
	CollectStr     = string(Collect)
	LastValueStr   = string(LastValue)
	MergeAggStr    = string(MergeAgg)
	StdStr         = "std"
	StdDevStr      = string(StdDev)
	StdDevSStr     = string(StdDevS)
	DeduplicateStr = string(Deduplicate)
	VarStr         = string(Var)
	VarSStr        = string(VarS)
	// Analytical functions
	LagStr        = string(Lag)
	LatestStr     = string(Latest)
	ChangedColStr = string(ChangedCol)
	HadChangedStr = string(HadChanged)
	// Expression aggregator
	ExpressionStr = string(Expression)
)

// LegacyAggregatorFunction defines aggregator function interface compatible with legacy aggregator interface
// Maintains compatibility with original interface for backward compatibility
type LegacyAggregatorFunction interface {
	New() LegacyAggregatorFunction
	Add(value interface{})
	Result() interface{}
}

// ContextAggregator defines aggregator interface that supports context mechanism
type ContextAggregator interface {
	GetContextKey() string
}

var (
	legacyAggregatorRegistry = make(map[string]func() LegacyAggregatorFunction)
	legacyRegistryMutex      sync.RWMutex
)

// RegisterLegacyAggregator registers legacy aggregator to global registry
func RegisterLegacyAggregator(name string, constructor func() LegacyAggregatorFunction) {
	legacyRegistryMutex.Lock()
	defer legacyRegistryMutex.Unlock()
	legacyAggregatorRegistry[name] = constructor
}

// CreateLegacyAggregator creates legacy aggregator, prioritizing functions module
func CreateLegacyAggregator(aggType AggregateType) LegacyAggregatorFunction {
	// First try to create aggregator from functions module
	if aggFunc := CreateBuiltinAggregatorFromFunctions(string(aggType)); aggFunc != nil {
		if adapter, ok := aggFunc.(*AggregatorAdapter); ok {
			return &FunctionAggregatorWrapper{adapter: adapter}
		}
	}

	// Try to create analytical function aggregator from functions module
	if analFunc := CreateAnalyticalAggregatorFromFunctions(string(aggType)); analFunc != nil {
		if adapter, ok := analFunc.(*AnalyticalAggregatorAdapter); ok {
			return &AnalyticalAggregatorWrapper{adapter: adapter}
		}
	}

	// Check custom registry
	legacyRegistryMutex.RLock()
	constructor, exists := legacyAggregatorRegistry[string(aggType)]
	legacyRegistryMutex.RUnlock()
	if exists {
		return constructor()
	}

	// If none found, throw error
	panic("unsupported aggregator type: " + aggType)
}

// FunctionAggregatorWrapper wraps functions module aggregator to make it compatible with original interface
type FunctionAggregatorWrapper struct {
	adapter *AggregatorAdapter
}

func (w *FunctionAggregatorWrapper) New() LegacyAggregatorFunction {
	newAdapter := w.adapter.New().(*AggregatorAdapter)
	return &FunctionAggregatorWrapper{adapter: newAdapter}
}

func (w *FunctionAggregatorWrapper) Add(value interface{}) {
	w.adapter.Add(value)
}

func (w *FunctionAggregatorWrapper) Result() interface{} {
	return w.adapter.Result()
}

// Implements ContextAggregator interface, supports context mechanism for window functions
func (w *FunctionAggregatorWrapper) GetContextKey() string {
	// Check if underlying function is a window function
	if w.adapter != nil {
		switch w.adapter.GetFunctionName() {
		case "window_start":
			return "window_start"
		case "window_end":
			return "window_end"
		}
	}
	return ""
}

// AnalyticalAggregatorWrapper wraps functions module analytical function aggregator to make it compatible with original interface
type AnalyticalAggregatorWrapper struct {
	adapter *AnalyticalAggregatorAdapter
}

func (w *AnalyticalAggregatorWrapper) New() LegacyAggregatorFunction {
	newAdapter := w.adapter.New().(*AnalyticalAggregatorAdapter)
	return &AnalyticalAggregatorWrapper{adapter: newAdapter}
}

func (w *AnalyticalAggregatorWrapper) Add(value interface{}) {
	w.adapter.Add(value)
}

func (w *AnalyticalAggregatorWrapper) Result() interface{} {
	return w.adapter.Result()
}
