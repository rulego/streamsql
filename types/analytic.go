package types

import "strconv"

// AnalyticSelfToken is a placeholder in the "expression package analysis function" regeneration template (e.g., ts - __analytic_self__):
// During the evaluation period, the analysis function results are written into the key of the row, and the entire outer expression is then calculated.
const AnalyticSelfToken = "__analytic_self__"

// AnalyticSelfTokenN Returns the placeholder identifier used in the parse template for the i-th analysis call in the expression within the expression.
// i==0 uses AnalyticSelfToken (single calls are backward compatible), i>0 uses __analytic_self_<i>__.
// When the same expression contains multiple parse calls (e.g., acc_max(v) - acc_min(v)), each call is assigned a placeholder.
func AnalyticSelfTokenN(i int) string {
	if i == 0 {
		return AnalyticSelfToken
	}
	return "__analytic_self_" + strconv.Itoa(i) + "__"
}

// AnalyticCall describes a single parsing call in the "Expression Package Analysis Function" (a field can contain multiple entries, e.g., acc_max(v) - acc_min(v)).
type AnalyticCall struct {
	FuncName string   // Function name, such as "acc_max"
	BareCall string   // Full call text (excluding OVER), such as "acc_max(v)"
	Args     []string // Fragments of original parameter expressions (not evaluated), such as ["v"]
}

// OverSpec describes the OVER clause of the analysis function.
// It only supports PARTITION BY and WHEN, not ORDER BY / ROWS frames (which is the Flink model).
type OverSpec struct {
	PartitionBy []string // Partitioned fields maintain their status independently by partition
	When        string   // WHEN conditional expression; The status is updated only when the condition is satisfied; otherwise, the old value is reused
}

// AnalyticField describes the number segment of the analysis function in SELECT (with optional OVER).
// Uses a direct connection path (EmitSync), evaluating each item by the stream-level state machine without entering the aggregation path.
type AnalyticField struct {
	FuncName   string    // Function name, such as "lag"
	Args       []string  // Original parameter expression fragments (not evaluated), such as ["temp","1"]
	Expression string    // Full call text (excluding OVER), such as "lag(temp, 1)"
	Alias      string    // Output column names (multi-column functions only serve as internal handles; actual column names are determined by the results)
	Over       *OverSpec // In the OVER clause, nil means none
	// MultiColumn Flags the multi-column dynamic output function (changed_cols): its evaluation result is map[colname]value,
	// During projection, fan out multiple output columns by pressing prefix+colname. Only SELECT.
	MultiColumn bool
	// WrapperExpr Outer Arithmetic/Expression Regeneration Template: When the field is an "expression package analysis function" (e.g., ts - lag(ts))
	// the analysis call substring is replaced with a placeholder __analytic_self__, resulting in "ts - __analytic_self__".
	// During the evaluation period, first calculate the analysis function value, substitute it into the placeholder, and then compute the entire expression. Empty indicates a pure analysis field.
	WrapperExpr string
	// All Analytic calls within the Calls expression (in order of occurrence). Pure single call field length is 1;
	// Multiple calls (acc_max(v) - acc_min(v)) length ≥ 2, each call corresponds to a WrapperExpr
	// types.AnalyticSelfTokenN(i) occupies a position. FuncName/Args/Expression still retains the first call value for the old path.
	Calls []AnalyticCall
	// In the InlineAggDisplay window query, analyze the override mapping of function parameter inline aggregation: hide key → display name.
	// For example, changed_cols("t", true, avg(temperature)) in window queries is extracted as
	// Hide the calculation field __winagg_0__, record {"__winagg_0__":"avg"}, and set the output column name to display name (→ tavg).
	InlineAggDisplay map[string]string
}

// WhereAnalyticCall describes the analysis function call that appears in the WHERE condition.
// During the parsing phase, extract from the WHERE text and assign placeholders; The value is calculated before WHERE during the calculation period,
// Injecting dataMap[Placeholder], the original call in the WHERE text is replaced with placeholders.
type WhereAnalyticCall struct {
	Placeholder string    // Composite bonds, such as "__analytic_0__"
	FuncName    string    // Function name
	Args        []string  // Original parameter fragments
	Expression  string    // Full call text (excluding OVER)
	Over        *OverSpec // In the OVER clause, nil means none
}
