package aggregator

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/rulego/streamsql/utils/fieldpath"
)

// nullGroupKeyMarker is the group-key segment for a missing/nil group field
// (e.g. a LEFT JOIN row with no match). Rows sharing it collapse into one NULL
// group; GetResults maps it back to nil. The \x00 byte avoids collisions with
// realistic field values.
const nullGroupKeyMarker = "\x00NULL"

// groupKeySep separates the fields of the grouping key. \x1f (cell separator) rarely appears in real data to avoid field values containing
// Key collisions caused by delimiters (previously used with "|": Values containing "|" will be truncated during the restore phase, and multiple fields will be misaligned).
const groupKeySep = "\x1f"

// Aggregator aggregator interface
type Aggregator interface {
	Add(data any) error
	Put(key string, val any) error
	GetResults() ([]map[string]any, error)
	Reset()
	// RegisterExpression registers expression evaluator
	RegisterExpression(field, expression string, fields []string, evaluator func(data any) (any, error))
}

// AggregationField defines configuration for a single aggregation field
type AggregationField struct {
	InputField    string        // Input field name (e.g., "temperature")
	AggregateType AggregateType // Aggregation type (e.g., Sum, Avg)
	OutputAlias   string        // Output alias (e.g., "temp_sum")
}

type GroupAggregator struct {
	aggregationFields []AggregationField
	groupFields       []string
	aggregators       map[string]AggregatorFunction
	groups            map[string]map[string]AggregatorFunction
	groupKeyVals      map[string][]any // Each group key corresponds to the original type grouping field value, which GetResults restores (avoiding serialized type loss)
	mu                sync.RWMutex
	context           map[string]any
	// Expression evaluators
	expressions map[string]*ExpressionEvaluator
}

// ExpressionEvaluator wraps expression evaluation functionality
type ExpressionEvaluator struct {
	Expression   string   // Complete expression
	Field        string   // Primary field name
	Fields       []string // All fields referenced in expression
	evaluateFunc func(data any) (any, error)
}

// NewGroupAggregator creates a new group aggregator
func NewGroupAggregator(groupFields []string, aggregationFields []AggregationField) *GroupAggregator {
	aggregators := make(map[string]AggregatorFunction)

	// Create aggregator for each aggregation field
	for i := range aggregationFields {
		if aggregationFields[i].OutputAlias == "" {
			// If no alias specified, use input field name
			aggregationFields[i].OutputAlias = aggregationFields[i].InputField
		}
		aggregators[aggregationFields[i].OutputAlias] = CreateBuiltinAggregator(aggregationFields[i].AggregateType)
	}

	return &GroupAggregator{
		aggregationFields: aggregationFields,
		groupFields:       groupFields,
		aggregators:       aggregators,
		groups:            make(map[string]map[string]AggregatorFunction),
		groupKeyVals:      make(map[string][]any),
		expressions:       make(map[string]*ExpressionEvaluator),
	}
}

// RegisterExpression registers expression evaluator
func (ga *GroupAggregator) RegisterExpression(field, expression string, fields []string, evaluator func(data any) (any, error)) {
	ga.mu.Lock()
	defer ga.mu.Unlock()

	ga.expressions[field] = &ExpressionEvaluator{
		Expression:   expression,
		Field:        field,
		Fields:       fields,
		evaluateFunc: evaluator,
	}
}

func (ga *GroupAggregator) Put(key string, val any) error {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	if ga.context == nil {
		ga.context = make(map[string]any)
	}
	ga.context[key] = val
	return nil
}

// isNumericAggregator checks if aggregator requires numeric type input
func (ga *GroupAggregator) isNumericAggregator(aggType AggregateType) bool {
	// Dynamically check function type through functions module
	if fn, exists := functions.Get(string(aggType)); exists {
		switch fn.GetType() {
		case functions.TypeMath:
			// Math functions usually require numeric input
			return true
		case functions.TypeAggregation:
			// Check if it's a numeric aggregation function
			switch string(aggType) {
			case functions.SumStr, functions.AvgStr, functions.MinStr, functions.MaxStr, functions.CountStr,
				functions.StdDevStr, functions.MedianStr, functions.PercentileStr,
				functions.VarStr, functions.VarSStr, functions.StdDevSStr:
				return true
			case functions.CollectStr, functions.MergeAggStr, functions.DeduplicateStr, functions.LastValueStr:
				// These functions can handle any type
				return false
			default:
				// For unknown aggregation functions, try to check function name patterns
				funcName := string(aggType)
				if strings.Contains(funcName, functions.SumStr) || strings.Contains(funcName, functions.AvgStr) ||
					strings.Contains(funcName, functions.MinStr) || strings.Contains(funcName, functions.MaxStr) ||
					strings.Contains(funcName, functions.StdStr) || strings.Contains(funcName, functions.VarStr) {
					return true
				}
				return false
			}
		case functions.TypeAnalytical:
			// Analytical functions can usually handle any type
			return false
		default:
			// For other types of functions, conservatively assume no numeric conversion needed
			return false
		}
	}

	// If function doesn't exist, judge by name pattern
	funcName := string(aggType)
	if strings.Contains(funcName, functions.SumStr) || strings.Contains(funcName, functions.AvgStr) ||
		strings.Contains(funcName, functions.MinStr) || strings.Contains(funcName, functions.MaxStr) ||
		strings.Contains(funcName, functions.CountStr) || strings.Contains(funcName, functions.StdStr) ||
		strings.Contains(funcName, functions.VarStr) {
		return true
	}
	return false
}

// shouldAllowNullValues determines whether the aggregator should allow NULL values
func (ga *GroupAggregator) shouldAllowNullValues(aggType AggregateType) bool {
	// FIRST_VALUE and LAST_VALUE functions should allow NULL values because they need to record the first/last value, even if it is NULL
	return aggType == FirstValue || aggType == LastValue
}

func (ga *GroupAggregator) Add(data any) error {
	ga.mu.Lock()
	defer ga.mu.Unlock()

	// Check if the data is nil
	if data == nil {
		return fmt.Errorf("data cannot be nil")
	}

	var v reflect.Value

	switch data.(type) {
	case map[string]any:
		dataMap := data.(map[string]any)
		v = reflect.ValueOf(dataMap)
	default:
		v = reflect.ValueOf(data)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		// Check if the data type is supported
		if v.Kind() != reflect.Struct && v.Kind() != reflect.Map {
			return fmt.Errorf("unsupported data type: %T, expected struct or map", data)
		}
	}

	key := ""
	keyVals := make([]any, 0, len(ga.groupFields))
	for _, field := range ga.groupFields {
		var fieldVal any
		var found bool

		// Check if it's a nested field
		if fieldpath.IsNestedField(field) {
			fieldVal, found = fieldpath.GetNestedField(data, field)
		} else {
			// Original field access logic
			var f reflect.Value
			if v.Kind() == reflect.Map {
				keyVal := reflect.ValueOf(field)
				f = v.MapIndex(keyVal)
			} else {
				f = v.FieldByName(field)
			}

			if f.IsValid() {
				fieldVal = f.Interface()
				found = true
			}
		}

		// Missing or nil group field (e.g. a LEFT JOIN row with no match)
		// collapses into a single NULL group keyed by the sentinel; GetResults
		// maps it back to nil. Avoids dropping the whole row on a nullable key.
		if !found || fieldVal == nil {
			key += nullGroupKeyMarker + groupKeySep
			keyVals = append(keyVals, nil)
			continue
		}

		if str, ok := fieldVal.(string); ok {
			key += str + groupKeySep
		} else {
			key += fmt.Sprintf("%v", fieldVal) + groupKeySep
		}
		keyVals = append(keyVals, fieldVal)
	}

	if _, exists := ga.groups[key]; !exists {
		ga.groups[key] = make(map[string]AggregatorFunction)
		ga.groupKeyVals[key] = keyVals
	}

	// Create aggregator instances for each field
	for outputAlias, agg := range ga.aggregators {
		if _, exists := ga.groups[key][outputAlias]; !exists {
			ga.groups[key][outputAlias] = agg.New()
		}
	}

	// Process each aggregation field
	for _, aggField := range ga.aggregationFields {
		outputAlias := aggField.OutputAlias
		if outputAlias == "" {
			outputAlias = aggField.InputField
		}

		// Check if there's an expression evaluator
		if expr, hasExpr := ga.expressions[outputAlias]; hasExpr {
			result, err := expr.evaluateFunc(data)
			if err != nil {
				continue
			}

			if groupAgg, exists := ga.groups[key][outputAlias]; exists {
				groupAgg.Add(result)
			}
			continue
		}

		inputField := aggField.InputField

		// Special handling for count(*) case
		if inputField == "*" {
			// For count(*), directly add 1 without getting specific field value
			if groupAgg, exists := ga.groups[key][outputAlias]; exists {
				groupAgg.Add(1)
			}
			continue
		}

		// Get field value - supports nested fields
		var fieldVal any
		var found bool

		if fieldpath.IsNestedField(inputField) {
			fieldVal, found = fieldpath.GetNestedField(data, inputField)
		} else {
			// Original field access logic
			var f reflect.Value
			if v.Kind() == reflect.Map {
				keyVal := reflect.ValueOf(inputField)
				f = v.MapIndex(keyVal)
			} else {
				f = v.FieldByName(inputField)
			}

			if f.IsValid() {
				fieldVal = f.Interface()
				found = true
			}
		}

		if !found {
			// Try to get from context
			if ga.context != nil {
				if groupAgg, exists := ga.groups[key][outputAlias]; exists {
					if contextAgg, ok := groupAgg.(ContextAggregator); ok {
						contextKey := contextAgg.GetContextKey()
						if val, exists := ga.context[contextKey]; exists {
							groupAgg.Add(val)
						}
					}
				}
			}
			continue
		}

		aggType := aggField.AggregateType

		// Skip nil values for most aggregation functions, but allow FIRST_VALUE and LAST_VALUE to handle them
		if fieldVal == nil && !ga.shouldAllowNullValues(aggType) {
			continue
		}

		// Special handling for Count aggregator - it can handle any type
		if aggType == Count {
			// Count can handle any non-null value
			if groupAgg, exists := ga.groups[key][outputAlias]; exists {
				groupAgg.Add(fieldVal)
			}
		} else if ga.isNumericAggregator(aggType) {
			// For numeric aggregation functions, try to convert to numeric type
			if numVal, err := cast.ToFloat64E(fieldVal); err == nil {
				if groupAgg, exists := ga.groups[key][outputAlias]; exists {

					groupAgg.Add(numVal)
				}
			} else {
				// Non-numeric values skip the field without interrupting the entire line of Add.
				continue
			}
		} else {
			// For non-numeric aggregation functions, pass original value directly
			if groupAgg, exists := ga.groups[key][outputAlias]; exists {

				groupAgg.Add(fieldVal)
			}
		}
	}

	return nil
}

func (ga *GroupAggregator) GetResults() ([]map[string]any, error) {
	ga.mu.RLock()
	defer ga.mu.RUnlock()

	// If there are no grouping fields or aggregate fields, but data has been added, return an empty result row
	if len(ga.aggregationFields) == 0 && len(ga.groupFields) == 0 {
		if len(ga.groups) > 0 {
			return []map[string]any{{}}, nil
		}
		return []map[string]any{}, nil
	}

	result := make([]map[string]any, 0, len(ga.groups))
	for key, aggregators := range ga.groups {
		group := make(map[string]any)
		keyVals := ga.groupKeyVals[key]
		for i, field := range ga.groupFields {
			if i < len(keyVals) {
				group[field] = keyVals[i] // The NULL group here is nil
			}
		}
		for field, agg := range aggregators {
			result := agg.Result()
			group[field] = result
			// Debug: log aggregator results (can be removed in production)
			// if strings.HasPrefix(field, "__") {
			//	fmt.Printf("Aggregator %s result: %v (%T)\n", field, result, result)
			// }
		}
		result = append(result, group)
	}
	return result, nil
}

func (ga *GroupAggregator) Reset() {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	ga.groups = make(map[string]map[string]AggregatorFunction)
	ga.groupKeyVals = make(map[string][]any)
}
