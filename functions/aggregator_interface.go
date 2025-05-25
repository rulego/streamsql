package functions

import "fmt"

// AggregatorFunction 聚合器函数接口，支持增量计算
type AggregatorFunction interface {
	Function
	// New 创建新的聚合器实例
	New() AggregatorFunction
	// Add 添加值进行增量计算
	Add(value interface{})
	// Result 获取聚合结果
	Result() interface{}
	// Reset 重置聚合器状态
	Reset()
	// Clone 克隆聚合器（用于窗口函数等场景）
	Clone() AggregatorFunction
}

// AnalyticalFunction 分析函数接口，支持状态管理
// 现在继承自AggregatorFunction，支持增量计算
type AnalyticalFunction interface {
	AggregatorFunction
}

// CreateAggregator 创建聚合器实例
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

// CreateAnalytical 创建分析函数实例
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
