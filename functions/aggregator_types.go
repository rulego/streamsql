package functions

import (
	"sync"
)

// AggregateType 聚合类型，从 aggregator.AggregateType 迁移而来
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
	// 分析函数
	Lag        AggregateType = "lag"
	Latest     AggregateType = "latest"
	ChangedCol AggregateType = "changed_col"
	HadChanged AggregateType = "had_changed"
	// 表达式聚合器，用于处理自定义函数
	Expression AggregateType = "expression"
)

// 为了方便使用，提供字符串常量版本
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
	// 分析函数
	LagStr        = string(Lag)
	LatestStr     = string(Latest)
	ChangedColStr = string(ChangedCol)
	HadChangedStr = string(HadChanged)
	// 表达式聚合器
	ExpressionStr = string(Expression)
)

// LegacyAggregatorFunction 兼容原有aggregator接口的聚合器函数接口
// 保持与原有接口兼容，用于向后兼容
type LegacyAggregatorFunction interface {
	New() LegacyAggregatorFunction
	Add(value interface{})
	Result() interface{}
}

// ContextAggregator 支持context机制的聚合器接口
type ContextAggregator interface {
	GetContextKey() string
}

var (
	legacyAggregatorRegistry = make(map[string]func() LegacyAggregatorFunction)
	legacyRegistryMutex      sync.RWMutex
)

// RegisterLegacyAggregator 注册传统聚合器到全局注册表
func RegisterLegacyAggregator(name string, constructor func() LegacyAggregatorFunction) {
	legacyRegistryMutex.Lock()
	defer legacyRegistryMutex.Unlock()
	legacyAggregatorRegistry[name] = constructor
}

// CreateLegacyAggregator 创建传统聚合器，优先使用functions模块
func CreateLegacyAggregator(aggType AggregateType) LegacyAggregatorFunction {
	// 首先尝试从functions模块创建聚合器
	if aggFunc := CreateBuiltinAggregatorFromFunctions(string(aggType)); aggFunc != nil {
		if adapter, ok := aggFunc.(*AggregatorAdapter); ok {
			return &FunctionAggregatorWrapper{adapter: adapter}
		}
	}

	// 尝试从functions模块创建分析函数聚合器
	if analFunc := CreateAnalyticalAggregatorFromFunctions(string(aggType)); analFunc != nil {
		if adapter, ok := analFunc.(*AnalyticalAggregatorAdapter); ok {
			return &AnalyticalAggregatorWrapper{adapter: adapter}
		}
	}

	// 检查自定义注册表
	legacyRegistryMutex.RLock()
	constructor, exists := legacyAggregatorRegistry[string(aggType)]
	legacyRegistryMutex.RUnlock()
	if exists {
		return constructor()
	}

	// 如果都没有找到，抛出错误
	panic("unsupported aggregator type: " + aggType)
}

// FunctionAggregatorWrapper 包装functions模块的聚合器，使其兼容原有接口
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

// 实现ContextAggregator接口，支持窗口函数的context机制
func (w *FunctionAggregatorWrapper) GetContextKey() string {
	// 检查底层函数是否是窗口函数
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

// AnalyticalAggregatorWrapper 包装functions模块的分析函数聚合器，使其兼容原有接口
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
