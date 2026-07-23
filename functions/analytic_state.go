package functions

import (
	"reflect"
	"strings"
)

// AnalyticState analyzes the flow level state machine of the function. Each event is adjusted to apply:
// Update the status with ARGS parsed from the current row and return the current result value. Each PARTITION holds one copy
// Independent state. Unlike AggregatorFunction's batch Add/Result, this applies one item at a time.
type AnalyticState interface {
	// Apply: Use args (args[0] as the main parameter, followed by offset/default/ignoreNull, etc.)
	// Update status, returning the analysis result for the current row.
	Apply(args []any) any
	// Reset: Resets the state (called during Stop or state cleanup).
	Reset()
}

// StatefulAnalytic is implemented by stateful analysis functions (lag/latest/had_changed/changed_col/acc_*).
// NewState creates an independent state for the state machine manager to hold for each PARTITION.
type StatefulAnalytic interface {
	NewState() AnalyticState
}

// MultiColumnState is implemented by outputting multi-column analysis functions (changed_cols). The engine is compatible with MultiColumn
// Fields go through ApplyColumns: Pass prefix/ignoreNull and {column_name: current value}, return {prefix+column_name: new}
// Only columns with changes are included. Single-column function continues with Apply.
type MultiColumnState interface {
	ApplyColumns(prefix string, ignoreNull bool, cols map[string]any) map[string]any
}

// NamedRowState is implemented by an analysis function that compares entire rows by column name (had_changed using '*'). Compare by list name
// Prevents misalignment when row schema changes (column addition, deletion, or disorder order).
type NamedRowState interface {
	ApplyNamed(ignoreNull bool, cols map[string]any) any
}

// analyticToInt: Fault-tolerant integer conversion: parameters like lag offset may be obtained after parseFunctionArgs
// int/int64/float64, uniformly converted to int.
func analyticToInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}

// AnalyticToBool fault-tolerant Boolean transform: The ignoreNull parameters of had_changed/changed_col(s) are used
// parseFunctionArgs may be bool or string "true"/"false" (unquoted true falls into the string branch).
func AnalyticToBool(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	case string:
		return strings.EqualFold(b, "true")
	}
	return false
}

// analyticEqual values are equal, numbers cross-type (int vs float64) are equal, the rest reflect.DeepEqual.
func analyticEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if af, ok := toFloat64Generic(a); ok {
		if bf, ok2 := toFloat64Generic(b); ok2 {
			return af == bf
		}
	}
	return reflect.DeepEqual(a, b)
}

// toFloat64Generic unifies numeric types to float64 for equality/comparison.
func toFloat64Generic(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}
