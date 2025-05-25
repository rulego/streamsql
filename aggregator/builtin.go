package aggregator

import (
	"github.com/rulego/streamsql/functions"
)

// 为了向后兼容，重新导出functions模块中的类型和函数

// AggregateType 聚合类型，重新导出functions.AggregateType
type AggregateType = functions.AggregateType

// 重新导出所有聚合类型常量
const (
	Sum         = functions.Sum
	Count       = functions.Count
	Avg         = functions.Avg
	Max         = functions.Max
	Min         = functions.Min
	StdDev      = functions.StdDev
	Median      = functions.Median
	Percentile  = functions.Percentile
	WindowStart = functions.WindowStart
	WindowEnd   = functions.WindowEnd
	Collect     = functions.Collect
	LastValue   = functions.LastValue
	MergeAgg    = functions.MergeAgg
	StdDevS     = functions.StdDevS
	Deduplicate = functions.Deduplicate
	Var         = functions.Var
	VarS        = functions.VarS
	// 分析函数
	Lag        = functions.Lag
	Latest     = functions.Latest
	ChangedCol = functions.ChangedCol
	HadChanged = functions.HadChanged
	// 表达式聚合器，用于处理自定义函数
	Expression = functions.Expression
)

// AggregatorFunction 聚合器函数接口，重新导出functions.LegacyAggregatorFunction
type AggregatorFunction = functions.LegacyAggregatorFunction

// ContextAggregator 支持context机制的聚合器接口，重新导出functions.ContextAggregator
type ContextAggregator = functions.ContextAggregator

// Register 添加自定义聚合器到全局注册表，重新导出functions.RegisterLegacyAggregator
func Register(name string, constructor func() AggregatorFunction) {
	functions.RegisterLegacyAggregator(name, constructor)
}

// CreateBuiltinAggregator 创建内置聚合器，重新导出functions.CreateLegacyAggregator
func CreateBuiltinAggregator(aggType AggregateType) AggregatorFunction {
	// 特殊处理expression类型
	if aggType == "expression" {
		return &ExpressionAggregatorWrapper{
			function: functions.NewExpressionAggregatorFunction(),
		}
	}

	return functions.CreateLegacyAggregator(aggType)
}

// ExpressionAggregatorWrapper 包装表达式聚合器，使其兼容LegacyAggregatorFunction接口
type ExpressionAggregatorWrapper struct {
	function *functions.ExpressionAggregatorFunction
}

func (w *ExpressionAggregatorWrapper) New() AggregatorFunction {
	return &ExpressionAggregatorWrapper{
		function: w.function.New().(*functions.ExpressionAggregatorFunction),
	}
}

func (w *ExpressionAggregatorWrapper) Add(value interface{}) {
	w.function.Add(value)
}

func (w *ExpressionAggregatorWrapper) Result() interface{} {
	return w.function.Result()
}
