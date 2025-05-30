package functions

import (
	"sync"
)

// AggregatorAdapter 聚合器适配器，兼容原有的aggregator接口
type AggregatorAdapter struct {
	aggFunc AggregatorFunction
}

// NewAggregatorAdapter 创建聚合器适配器
func NewAggregatorAdapter(name string) (*AggregatorAdapter, error) {
	aggFunc, err := CreateAggregator(name)
	if err != nil {
		return nil, err
	}

	return &AggregatorAdapter{
		aggFunc: aggFunc,
	}, nil
}

// New 创建新的聚合器实例
func (a *AggregatorAdapter) New() interface{} {
	return &AggregatorAdapter{
		aggFunc: a.aggFunc.New(),
	}
}

// Add 添加值
func (a *AggregatorAdapter) Add(value interface{}) {
	a.aggFunc.Add(value)
}

// Result 获取结果
func (a *AggregatorAdapter) Result() interface{} {
	return a.aggFunc.Result()
}

// GetFunctionName 获取底层函数名称，用于支持context机制
func (a *AggregatorAdapter) GetFunctionName() string {
	if a.aggFunc != nil {
		return a.aggFunc.GetName()
	}
	return ""
}

// AnalyticalAdapter 分析函数适配器
type AnalyticalAdapter struct {
	analFunc AnalyticalFunction
}

// NewAnalyticalAdapter 创建分析函数适配器
func NewAnalyticalAdapter(name string) (*AnalyticalAdapter, error) {
	analFunc, err := CreateAnalytical(name)
	if err != nil {
		return nil, err
	}

	return &AnalyticalAdapter{
		analFunc: analFunc,
	}, nil
}

// Execute 执行分析函数
func (a *AnalyticalAdapter) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return a.analFunc.Execute(ctx, args)
}

// Reset 重置状态
func (a *AnalyticalAdapter) Reset() {
	a.analFunc.Reset()
}

// Clone 克隆实例
func (a *AnalyticalAdapter) Clone() *AnalyticalAdapter {
	return &AnalyticalAdapter{
		analFunc: a.analFunc.Clone(),
	}
}

// 全局适配器注册表
var (
	aggregatorAdapters = make(map[string]func() interface{})
	analyticalAdapters = make(map[string]func() *AnalyticalAdapter)
	adapterMutex       sync.RWMutex
)

// RegisterAggregatorAdapter 注册聚合器适配器
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

// RegisterAnalyticalAdapter 注册分析函数适配器
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

// GetAggregatorAdapter 获取聚合器适配器
func GetAggregatorAdapter(name string) (func() interface{}, bool) {
	adapterMutex.RLock()
	defer adapterMutex.RUnlock()

	constructor, exists := aggregatorAdapters[name]
	return constructor, exists
}

// GetAnalyticalAdapter 获取分析函数适配器
func GetAnalyticalAdapter(name string) (func() *AnalyticalAdapter, bool) {
	adapterMutex.RLock()
	defer adapterMutex.RUnlock()

	constructor, exists := analyticalAdapters[name]
	return constructor, exists
}

// CreateBuiltinAggregatorFromFunctions 从functions模块创建聚合器
func CreateBuiltinAggregatorFromFunctions(aggType string) interface{} {
	// 首先尝试从适配器注册表获取
	if constructor, exists := GetAggregatorAdapter(aggType); exists {
		return constructor()
	}

	// 如果没有找到，尝试直接创建
	adapter, err := NewAggregatorAdapter(aggType)
	if err != nil {
		return nil
	}

	return adapter
}

// CreateAnalyticalFromFunctions 从functions模块创建分析函数
func CreateAnalyticalFromFunctions(funcType string) *AnalyticalAdapter {
	// 首先尝试从适配器注册表获取
	if constructor, exists := GetAnalyticalAdapter(funcType); exists {
		return constructor()
	}

	// 如果没有找到，尝试直接创建
	adapter, err := NewAnalyticalAdapter(funcType)
	if err != nil {
		return nil
	}

	return adapter
}
