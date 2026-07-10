package aggregator

import (
	"fmt"

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

// perRowWindowFunctions compute a value per row. The engine's aggregation
// model produces one result per group, so these cannot be evaluated: row_number
// has no AggregatorFunction (would crash), lead implements it but its Result is
// a nil-returning stub. Reject both up front with a clear error instead.
var perRowWindowFunctions = map[string]bool{
	"row_number": true,
	"lead":       true,
}

// ValidateAggregateType reports whether aggType can be evaluated in an
// aggregation query. It rejects per-row window functions (row_number, lead) the
// per-group model cannot compute, so they fail at Execute with a clear error
// instead of crashing on the data path or silently returning nil. Other types
// are allowed: genuine aggregators resolve via CreateBuiltinAggregator, and
// scalar functions that appear in SelectFields for non-aggregation queries are
// evaluated through the field-expression path (not the aggregator path), so
// rejecting them here would break legitimate scalar-function SELECTs.
func ValidateAggregateType(aggType AggregateType) error {
	if perRowWindowFunctions[string(aggType)] {
		return fmt.Errorf("%s() is a per-row window function and is not supported in aggregation queries; the engine computes one result per group (per-row OVER-window support is planned)", aggType)
	}
	return nil
}

// PostAggregationPlaceholder is a placeholder aggregator for post-aggregation fields
type PostAggregationPlaceholder struct{}

func (p *PostAggregationPlaceholder) New() AggregatorFunction {
	return &PostAggregationPlaceholder{}
}

func (p *PostAggregationPlaceholder) Add(value any) {
	// Do nothing - this is just a placeholder
}

func (p *PostAggregationPlaceholder) Result() any {
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

func (w *ExpressionAggregatorWrapper) Add(value any) {
	w.function.Add(value)
}

func (w *ExpressionAggregatorWrapper) Result() any {
	return w.function.Result()
}
