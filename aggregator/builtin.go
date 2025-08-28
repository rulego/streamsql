package aggregator

import (
	"github.com/rulego/streamsql/functions"
)

// AggregateType aggregate type, re-exports functions.AggregateType
type AggregateType = functions.AggregateType

// Re-export all aggregate type constants
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
	FirstValue  = functions.FirstValue
	LastValue   = functions.LastValue
	MergeAgg    = functions.MergeAgg
	StdDevS     = functions.StdDevS
	Deduplicate = functions.Deduplicate
	Var         = functions.Var
	VarS        = functions.VarS
	// Analytical functions
	Lag        = functions.Lag
	Latest     = functions.Latest
	ChangedCol = functions.ChangedCol
	HadChanged = functions.HadChanged
	// Expression aggregator for handling custom functions
	Expression = functions.Expression
	// Post-aggregation marker
	PostAggregation = functions.PostAggregation
)

// AggregatorFunction aggregator function interface, re-exports functions.LegacyAggregatorFunction
type AggregatorFunction = functions.LegacyAggregatorFunction

// ContextAggregator aggregator interface supporting context mechanism, re-exports functions.ContextAggregator
type ContextAggregator = functions.ContextAggregator

// Register adds custom aggregator to global registry, re-exports functions.RegisterLegacyAggregator
func Register(name string, constructor func() AggregatorFunction) {
	functions.RegisterLegacyAggregator(name, constructor)
}

// CreateBuiltinAggregator creates built-in aggregator, re-exports functions.CreateLegacyAggregator
func CreateBuiltinAggregator(aggType AggregateType) AggregatorFunction {
	// Special handling for expression type
	if aggType == "expression" {
		return &ExpressionAggregatorWrapper{
			function: functions.NewExpressionAggregatorFunction(),
		}
	}

	// Special handling for post-aggregation type (placeholder aggregator)
	if aggType == "post_aggregation" {
		return &PostAggregationPlaceholder{}
	}

	return functions.CreateLegacyAggregator(aggType)
}

// PostAggregationPlaceholder is a placeholder aggregator for post-aggregation fields
type PostAggregationPlaceholder struct{}

func (p *PostAggregationPlaceholder) New() AggregatorFunction {
	return &PostAggregationPlaceholder{}
}

func (p *PostAggregationPlaceholder) Add(value interface{}) {
	// Do nothing - this is just a placeholder
}

func (p *PostAggregationPlaceholder) Result() interface{} {
	// Return nil - actual result will be computed in post-processing
	return nil
}

// ExpressionAggregatorWrapper wraps expression aggregator to make it compatible with LegacyAggregatorFunction interface
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
