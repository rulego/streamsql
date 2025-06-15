package aggregator

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/cast"
)

type Aggregator interface {
	Add(data interface{}) error
	Put(key string, val interface{}) error
	GetResults() ([]map[string]interface{}, error)
	Reset()
	// RegisterExpression 注册表达式计算器
	RegisterExpression(field, expression string, fields []string, evaluator func(data interface{}) (interface{}, error))
}

// AggregationField 定义单个聚合字段的配置
type AggregationField struct {
	InputField    string        // 输入字段名（如 "temperature"）
	AggregateType AggregateType // 聚合类型（如 Sum, Avg）
	OutputAlias   string        // 输出别名（如 "temp_sum"）
}

type GroupAggregator struct {
	aggregationFields []AggregationField
	groupFields       []string
	aggregators       map[string]AggregatorFunction
	groups            map[string]map[string]AggregatorFunction
	mu                sync.RWMutex
	context           map[string]interface{}
	// 表达式计算器
	expressions map[string]*ExpressionEvaluator
}

// ExpressionEvaluator 包装表达式计算功能
type ExpressionEvaluator struct {
	Expression   string   // 完整表达式
	Field        string   // 主字段名
	Fields       []string // 表达式中引用的所有字段
	evaluateFunc func(data interface{}) (interface{}, error)
}

// NewGroupAggregator 创建新的分组聚合器
func NewGroupAggregator(groupFields []string, aggregationFields []AggregationField) *GroupAggregator {
	aggregators := make(map[string]AggregatorFunction)

	// 为每个聚合字段创建聚合器
	for i := range aggregationFields {
		if aggregationFields[i].OutputAlias == "" {
			// 如果没有指定别名，使用输入字段名
			aggregationFields[i].OutputAlias = aggregationFields[i].InputField
		}
		aggregators[aggregationFields[i].OutputAlias] = CreateBuiltinAggregator(aggregationFields[i].AggregateType)
	}

	return &GroupAggregator{
		aggregationFields: aggregationFields,
		groupFields:       groupFields,
		aggregators:       aggregators,
		groups:            make(map[string]map[string]AggregatorFunction),
		expressions:       make(map[string]*ExpressionEvaluator),
	}
}

// RegisterExpression 注册表达式计算器
func (ga *GroupAggregator) RegisterExpression(field, expression string, fields []string, evaluator func(data interface{}) (interface{}, error)) {
	ga.mu.Lock()
	defer ga.mu.Unlock()

	ga.expressions[field] = &ExpressionEvaluator{
		Expression:   expression,
		Field:        field,
		Fields:       fields,
		evaluateFunc: evaluator,
	}
}

func (ga *GroupAggregator) Put(key string, val interface{}) error {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	if ga.context == nil {
		ga.context = make(map[string]interface{})
	}
	ga.context[key] = val
	return nil
}

// isNumericAggregator 检查聚合器是否需要数值类型输入
func (ga *GroupAggregator) isNumericAggregator(aggType AggregateType) bool {
	// 通过functions模块动态检查函数类型
	if fn, exists := functions.Get(string(aggType)); exists {
		switch fn.GetType() {
		case functions.TypeMath:
			// 数学函数通常需要数值输入
			return true
		case functions.TypeAggregation:
			// 检查是否是数值聚合函数
			switch string(aggType) {
			case functions.SumStr, functions.AvgStr, functions.MinStr, functions.MaxStr, functions.CountStr,
				functions.StdDevStr, functions.MedianStr, functions.PercentileStr,
				functions.VarStr, functions.VarSStr, functions.StdDevSStr:
				return true
			case functions.CollectStr, functions.MergeAggStr, functions.DeduplicateStr, functions.LastValueStr:
				// 这些函数可以处理任意类型
				return false
			default:
				// 对于未知的聚合函数，尝试检查函数名称模式
				funcName := string(aggType)
				if strings.Contains(funcName, functions.SumStr) || strings.Contains(funcName, functions.AvgStr) ||
					strings.Contains(funcName, functions.MinStr) || strings.Contains(funcName, functions.MaxStr) ||
					strings.Contains(funcName, functions.StdStr) || strings.Contains(funcName, functions.VarStr) {
					return true
				}
				return false
			}
		case functions.TypeAnalytical:
			// 分析函数通常可以处理任意类型
			return false
		default:
			// 其他类型的函数，保守起见认为不需要数值转换
			return false
		}
	}

	// 如果函数不存在，根据名称模式判断
	funcName := string(aggType)
	if strings.Contains(funcName, functions.SumStr) || strings.Contains(funcName, functions.AvgStr) ||
		strings.Contains(funcName, functions.MinStr) || strings.Contains(funcName, functions.MaxStr) ||
		strings.Contains(funcName, functions.CountStr) || strings.Contains(funcName, functions.StdStr) ||
		strings.Contains(funcName, functions.VarStr) {
		return true
	}
	return false
}

func (ga *GroupAggregator) Add(data interface{}) error {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	var v reflect.Value

	switch data.(type) {
	case map[string]interface{}:
		dataMap := data.(map[string]interface{})
		v = reflect.ValueOf(dataMap)
	default:
		v = reflect.ValueOf(data)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
	}

	key := ""
	for _, field := range ga.groupFields {
		var f reflect.Value

		if v.Kind() == reflect.Map {
			keyVal := reflect.ValueOf(field)
			f = v.MapIndex(keyVal)
		} else {
			f = v.FieldByName(field)
		}

		if !f.IsValid() {
			return fmt.Errorf("field %s not found", field)
		}

		keyVal := f.Interface()
		if keyVal == nil {
			return fmt.Errorf("field %s has nil value", field)
		}

		if str, ok := keyVal.(string); ok {
			key += fmt.Sprintf("%s|", str)
		} else {
			key += fmt.Sprintf("%v|", keyVal)
		}
	}

	if _, exists := ga.groups[key]; !exists {
		ga.groups[key] = make(map[string]AggregatorFunction)
	}

	// 为每个字段创建聚合器实例
	for outputAlias, agg := range ga.aggregators {
		if _, exists := ga.groups[key][outputAlias]; !exists {
			ga.groups[key][outputAlias] = agg.New()
		}
	}

	// 处理每个聚合字段
	for _, aggField := range ga.aggregationFields {
		outputAlias := aggField.OutputAlias
		if outputAlias == "" {
			outputAlias = aggField.InputField
		}

		// 检查是否有表达式计算器
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

		// 特殊处理count(*)的情况
		if inputField == "*" {
			// 对于count(*)，直接添加1，不需要获取具体字段值
			if groupAgg, exists := ga.groups[key][outputAlias]; exists {
				groupAgg.Add(1)
			}
			continue
		}

		// 获取字段值
		var f reflect.Value
		if v.Kind() == reflect.Map {
			keyVal := reflect.ValueOf(inputField)
			f = v.MapIndex(keyVal)
		} else {
			f = v.FieldByName(inputField)
		}

		if !f.IsValid() {
			// 尝试从context中获取
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

		fieldVal := f.Interface()
		aggType := aggField.AggregateType

		// 动态检查是否需要数值转换
		if ga.isNumericAggregator(aggType) {
			// 对于数值聚合函数，尝试转换为数值类型
			if numVal, err := cast.ToFloat64E(fieldVal); err == nil {
				if groupAgg, exists := ga.groups[key][outputAlias]; exists {
					groupAgg.Add(numVal)
				}
			} else {
				return fmt.Errorf("cannot convert field %s value %v to numeric type for aggregator %s", inputField, fieldVal, aggType)
			}
		} else {
			// 对于非数值聚合函数，直接传递原始值
			if groupAgg, exists := ga.groups[key][outputAlias]; exists {
				groupAgg.Add(fieldVal)
			}
		}
	}

	return nil
}

func (ga *GroupAggregator) GetResults() ([]map[string]interface{}, error) {
	ga.mu.RLock()
	defer ga.mu.RUnlock()
	result := make([]map[string]interface{}, 0, len(ga.groups))
	for key, aggregators := range ga.groups {
		group := make(map[string]interface{})
		fields := strings.Split(key, "|")
		for i, field := range ga.groupFields {
			if i < len(fields) {
				group[field] = fields[i]
			}
		}
		for field, agg := range aggregators {
			group[field] = agg.Result()
		}
		result = append(result, group)
	}
	return result, nil
}

func (ga *GroupAggregator) Reset() {
	ga.mu.Lock()
	defer ga.mu.Unlock()
	ga.groups = make(map[string]map[string]AggregatorFunction)
}
