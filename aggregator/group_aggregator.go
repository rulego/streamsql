package aggregator

import (
	"container/list"
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

// groupKeySep 分隔分组键各字段。\x1f（单元分隔符）在真实数据中极少出现，避免字段值含
// 分隔符导致的键碰撞（曾用 "|"：含 "|" 的值会被还原阶段截断、多字段还会错位）。
const groupKeySep = "\x1f"

// defaultMaxPartitions bounds the number of GROUP BY partitions (distinct group
// keys) kept at once, mirroring the analytic-function and CEP caps so the three
// high-cardinality-key paths have consistent memory bounds. Above the cap the
// least-recently-used group is evicted, discarding its accumulated aggregation
// state. Set via SetMaxPartitions / WithGroupMaxPartitions; 0 keeps the default.
const defaultMaxPartitions = 10000

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
	groupKeyVals      map[string][]any // 每个 group key 对应的原始类型分组字段值，供 GetResults 还原（避免序列化丢类型）
	mu                sync.RWMutex
	context           map[string]any
	// Expression evaluators
	expressions map[string]*ExpressionEvaluator

	// LRU 分区上限：groups/groupKeyVals 在高基数 GROUP BY（如万级 deviceId）下会无界增长，
	// 与分析函数/CEP 的有界分区不一致（见 V1.2.0 审计 P1-5）。maxPartitions>0 时，
	// 新建分组超过上限即淘汰最久未用的分组（连同其聚合状态）。
	maxPartitions int
	// groupOrder 维护分组的 LRU 顺序：front=最近 Add 命中，back=待淘汰。
	groupOrder *list.List
	// groupElems 把 group key 映射到其在 groupOrder 中的节点，O(1) 提升与淘汰。
	groupElems map[string]*list.Element
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
		groupOrder:        list.New(),
		groupElems:        make(map[string]*list.Element),
	}
}

// SetMaxPartitions overrides the GROUP BY partition cap. A value <=0 keeps the
// default (defaultMaxPartitions). Safe to call before the aggregator receives
// data; calling after data has been added only affects future eviction.
func (ga *GroupAggregator) SetMaxPartitions(n int) {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	if n > 0 {
		ga.maxPartitions = n
	} else {
		ga.maxPartitions = 0 // 0 means "use default" resolved in effectiveMaxPartitions
	}
}

// effectiveMaxPartitions returns the active cap, resolving 0 to the default.
// Caller must hold ga.mu (or call under lock).
func (ga *GroupAggregator) effectiveMaxPartitions() int {
	if ga.maxPartitions > 0 {
		return ga.maxPartitions
	}
	return defaultMaxPartitions
}

// evictIfNeeded drops the least-recently-used group (and all its aggregation
// state) while the partition count exceeds the cap. Must be called under ga.mu.
// Eviction is silent by design — mirroring analytic/CEP partition eviction —
// and resets that group's aggregates to empty the next time it appears.
func (ga *GroupAggregator) evictIfNeeded() {
	cap := ga.effectiveMaxPartitions()
	for ga.groupOrder.Len() > cap {
		oldest := ga.groupOrder.Back()
		if oldest == nil {
			return
		}
		key := oldest.Value.(string)
		ga.groupOrder.Remove(oldest)
		delete(ga.groupElems, key)
		delete(ga.groups, key)
		delete(ga.groupKeyVals, key)
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

// shouldAllowNullValues 判断聚合函数是否应该允许NULL值
func (ga *GroupAggregator) shouldAllowNullValues(aggType AggregateType) bool {
	// FIRST_VALUE和LAST_VALUE函数应该允许NULL值，因为它们需要记录第一个/最后一个值，即使是NULL
	return aggType == FirstValue || aggType == LastValue
}

func (ga *GroupAggregator) Add(data any) error {
	ga.mu.Lock()
	defer ga.mu.Unlock()

	// 检查数据是否为nil
	if data == nil {
		return fmt.Errorf("data cannot be nil")
	}

	var v reflect.Value
	// dataMap is set for the common map[string]any path so that field access can
	// use a direct map lookup instead of reflect.ValueOf/MapIndex (which allocate
	// a reflect.Value for the key on every field of every row). v is still needed
	// for the struct path below.
	var dataMap map[string]any

	switch dm := data.(type) {
	case map[string]any:
		dataMap = dm
		// v is intentionally left invalid for the map path; the direct-lookup
		// branch below handles it without reflection.
	default:
		v = reflect.ValueOf(data)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		// 检查是否为支持的数据类型
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
		} else if dataMap != nil {
			// Hot path: map[string]any with a flat key. Direct lookup avoids two
			// reflect.Value allocations per field that the MapIndex path costs.
			fieldVal, found = dataMap[field]
		} else {
			// Struct (or non-string-keyed map) path: field access via reflection.
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
		// Track new group as most-recently-used, then enforce the cap.
		ga.groupElems[key] = ga.groupOrder.PushFront(key)
		ga.evictIfNeeded()
	} else {
		// Existing group: promote to most-recently-used so it is evicted last.
		ga.groupOrder.MoveToFront(ga.groupElems[key])
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
		} else if dataMap != nil {
			// Hot path: map[string]any with a flat key. Direct lookup avoids two
			// reflect.Value allocations per field that the MapIndex path costs.
			fieldVal, found = dataMap[inputField]
		} else {
			// Struct (or non-string-keyed map) path: field access via reflection.
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
				// 非数值跳过该字段，不中断整行 Add。
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

	// 如果既没有分组字段又没有聚合字段，但有数据被添加过，返回一个空的结果行
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
				group[field] = keyVals[i] // NULL 组此处即 nil
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
	// Reset LRU tracking too: stale list/elems after a Reset would dangle and
	// cause evictIfNeeded to delete already-cleared map keys (harmless but
	// confusing), and would keep the cap applied to stale counts.
	ga.groupOrder = list.New()
	ga.groupElems = make(map[string]*list.Element)
}
