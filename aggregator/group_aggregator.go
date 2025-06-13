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

type GroupAggregator struct {
	fieldMap    map[string]AggregateType
	groupFields []string
	aggregators map[string]AggregatorFunction
	groups      map[string]map[string]AggregatorFunction
	mu          sync.RWMutex
	context     map[string]interface{}
	fieldAlias  map[string]string
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

func NewGroupAggregator(groupFields []string, fieldMap map[string]AggregateType, fieldAlias map[string]string) *GroupAggregator {
	aggregators := make(map[string]AggregatorFunction)

	// 重新组织映射关系
	// fieldMap: 输入字段名 -> 聚合类型
	// fieldAlias: 输入字段名 -> 输出别名
	// 需要转换为：输出别名 -> 聚合类型，输出别名 -> 输入字段名

	newFieldMap := make(map[string]AggregateType) // 输出字段名 -> 聚合类型
	newFieldAlias := make(map[string]string)      // 输出字段名 -> 输入字段名

	for inputField, aggType := range fieldMap {
		outputField := inputField // 默认输出字段名 = 输入字段名
		if alias, exists := fieldAlias[inputField]; exists {
			outputField = alias // 如果有别名，使用别名作为输出字段名
		}

		newFieldMap[outputField] = aggType
		newFieldAlias[outputField] = inputField
		aggregators[outputField] = CreateBuiltinAggregator(aggType)
	}

	return &GroupAggregator{
		fieldMap:    newFieldMap, // 输出字段名 -> 聚合类型
		groupFields: groupFields,
		aggregators: aggregators,
		groups:      make(map[string]map[string]AggregatorFunction),
		fieldAlias:  newFieldAlias, // 输出字段名 -> 输入字段名
		expressions: make(map[string]*ExpressionEvaluator),
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

// prepareDataValue 准备数据的反射值
func (ga *GroupAggregator) prepareDataValue(data interface{}) reflect.Value {
	switch data.(type) {
	case map[string]interface{}:
		return reflect.ValueOf(data.(map[string]interface{}))
	default:
		v := reflect.ValueOf(data)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		return v
	}
}

// buildGroupKey 构建分组键
func (ga *GroupAggregator) buildGroupKey(v reflect.Value) (string, error) {
	key := ""
	for _, field := range ga.groupFields {
		fieldValue, err := ga.getFieldValue(v, field)
		if err != nil {
			return "", err
		}

		if fieldValue == nil {
			return "", fmt.Errorf("field %s has nil value", field)
		}

		if str, ok := fieldValue.(string); ok {
			key += fmt.Sprintf("%s|", str)
		} else {
			key += fmt.Sprintf("%v|", fieldValue)
		}
	}
	return key, nil
}

// getFieldValue 获取字段值
func (ga *GroupAggregator) getFieldValue(v reflect.Value, fieldName string) (interface{}, error) {
	var f reflect.Value
	if v.Kind() == reflect.Map {
		keyVal := reflect.ValueOf(fieldName)
		f = v.MapIndex(keyVal)
	} else {
		f = v.FieldByName(fieldName)
	}

	if !f.IsValid() {
		return nil, fmt.Errorf("field %s not found", fieldName)
	}

	return f.Interface(), nil
}

// ensureAggregators 确保聚合器实例存在
func (ga *GroupAggregator) ensureAggregators(key string) {
	if _, exists := ga.groups[key]; !exists {
		ga.groups[key] = make(map[string]AggregatorFunction)
	}

	// 为每个字段创建聚合器实例
	for field, agg := range ga.aggregators {
		if _, exists := ga.groups[key][field]; !exists {
			ga.groups[key][field] = agg.New()
		}
	}
}

// processFieldAggregation 处理字段聚合
func (ga *GroupAggregator) processFieldAggregation(key, field string, data interface{}, v reflect.Value) error {
	// 检查是否有表达式计算器
	if expr, hasExpr := ga.expressions[field]; hasExpr {
		return ga.processExpressionField(key, field, expr, data)
	}

	// 获取实际的输入字段名
	inputFieldName := ga.getInputFieldName(field)

	// 特殊处理count(*)的情况
	if inputFieldName == "*" {
		return ga.addValueToAggregator(key, field, 1)
	}

	// 获取字段值并处理
	return ga.processRegularField(key, field, inputFieldName, v)
}

// processExpressionField 处理表达式字段
func (ga *GroupAggregator) processExpressionField(key, field string, expr *ExpressionEvaluator, data interface{}) error {
	result, err := expr.evaluateFunc(data)
	if err != nil {
		return nil // 继续处理其他字段
	}
	return ga.addValueToAggregator(key, field, result)
}

// getInputFieldName 获取输入字段名
func (ga *GroupAggregator) getInputFieldName(field string) string {
	if mappedField, exists := ga.fieldAlias[field]; exists {
		return mappedField
	}
	return field
}

// processRegularField 处理常规字段
func (ga *GroupAggregator) processRegularField(key, field, inputFieldName string, v reflect.Value) error {
	fieldVal, err := ga.getFieldValue(v, inputFieldName)
	if err != nil {
		// 尝试从context中获取
		return ga.tryContextAggregation(key, field)
	}

	aggType := ga.fieldMap[field]
	if ga.isNumericAggregator(aggType) {
		return ga.processNumericField(key, field, inputFieldName, fieldVal, aggType)
	}

	return ga.addValueToAggregator(key, field, fieldVal)
}

// tryContextAggregation 尝试从context中获取值进行聚合
func (ga *GroupAggregator) tryContextAggregation(key, field string) error {
	if ga.context == nil {
		return nil
	}

	if groupAgg, exists := ga.groups[key][field]; exists {
		if contextAgg, ok := groupAgg.(ContextAggregator); ok {
			contextKey := contextAgg.GetContextKey()
			if val, exists := ga.context[contextKey]; exists {
				groupAgg.Add(val)
			}
		}
	}
	return nil
}

// processNumericField 处理数值字段
func (ga *GroupAggregator) processNumericField(key, field, inputFieldName string, fieldVal interface{}, aggType AggregateType) error {
	numVal, err := cast.ToFloat64E(fieldVal)
	if err != nil {
		return fmt.Errorf("cannot convert field %s value %v to numeric type for aggregator %s", inputFieldName, fieldVal, aggType)
	}
	return ga.addValueToAggregator(key, field, numVal)
}

// addValueToAggregator 向聚合器添加值
func (ga *GroupAggregator) addValueToAggregator(key, field string, value interface{}) error {
	if groupAgg, exists := ga.groups[key][field]; exists {
		groupAgg.Add(value)
	}
	return nil
}

func (ga *GroupAggregator) Add(data interface{}) error {
	ga.mu.Lock()
	defer ga.mu.Unlock()

	v := ga.prepareDataValue(data)

	key, err := ga.buildGroupKey(v)
	if err != nil {
		return err
	}

	ga.ensureAggregators(key)

	for field := range ga.fieldMap {
		if err := ga.processFieldAggregation(key, field, data, v); err != nil {
			return err
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
