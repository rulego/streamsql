package aggregator

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type Aggregator interface {
	Add(data interface{}) error
	GetResults() ([]map[string]interface{}, error)
	Reset()
}

type GroupAggregator struct {
	fieldMap    map[string]AggregateType
	groupFields []string
	aggregators map[string]AggregatorFunction
	groups      map[string]map[string]AggregatorFunction
	mu          sync.RWMutex
}

func NewGroupAggregator(groupFields []string, fieldMap map[string]AggregateType) *GroupAggregator {
	aggregators := make(map[string]AggregatorFunction)

	for field, aggType := range fieldMap {
		aggregators[field] = CreateBuiltinAggregator(aggType)
	}

	return &GroupAggregator{
		fieldMap:    fieldMap,
		groupFields: groupFields,
		aggregators: aggregators,
		groups:      make(map[string]map[string]AggregatorFunction),
	}
}
func (ga *GroupAggregator) Add(data interface{}) error {
	ga.mu.Lock()         // 获取写锁
	defer ga.mu.Unlock() // 确保函数返回时释放锁
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
			// 处理 map 类型
			keyVal := reflect.ValueOf(field)
			f = v.MapIndex(keyVal)
		} else {
			// 处理结构体类型
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

	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// 去除最后的 | 符号
	key = key[:len(key)-1]

	if _, exists := ga.groups[key]; !exists {
		ga.groups[key] = make(map[string]AggregatorFunction)
	}
	// field级别的聚合可以分批创建
	for field, agg := range ga.aggregators {
		if _, exists := ga.groups[key][field]; !exists {
			// 创建新的聚合器实例
			ga.groups[key][field] = agg.New()
			//fmt.Printf("groups by %s : %v \n", key, ga.groups[key])
		}
	}

	for field := range ga.fieldMap {
		var f reflect.Value

		if v.Kind() == reflect.Map {
			// 处理 map 类型
			keyVal := reflect.ValueOf(field)
			f = v.MapIndex(keyVal)
		} else {
			// 处理结构体类型
			f = v.FieldByName(field)
		}

		if !f.IsValid() {
			//return fmt.Errorf("field %s not found", field)
			//fmt.Printf("field %s not found in %v \n ", field, data)
			continue
		}

		fieldVal := f.Interface()
		var value float64
		switch vType := fieldVal.(type) {
		case float64:
			value = vType
		case int, int32, int64:
			value = float64(vType.(int))
		case float32:
			value = float64(vType)
		default:
			return fmt.Errorf("unsupported type for field %s: %T", field, fieldVal)
		}
		if groupAgg, exists := ga.groups[key][field]; exists {
			groupAgg.Add(value)
			//fmt.Printf("add agg group by %s:%s , %v  \n", key, field, value)
		} else {

		}
	}

	return nil
}

func (ga *GroupAggregator) GetResults() ([]map[string]interface{}, error) {
	ga.mu.RLock()         // 获取读锁，允许并发读取
	defer ga.mu.RUnlock() // 确保函数返回时释放锁
	result := make([]map[string]interface{}, 0, len(ga.groups))
	for key, aggregators := range ga.groups {
		group := make(map[string]interface{})
		fields := strings.Split(key, "|")
		for i, field := range ga.groupFields {
			group[field] = fields[i]
		}
		for field, agg := range aggregators {
			group[field+"_"+string(ga.fieldMap[field])] = agg.Result()
		}
		result = append(result, group)
	}
	return result, nil
}

func (ga *GroupAggregator) Reset() {
	ga.mu.Lock()         // 获取写锁
	defer ga.mu.Unlock() // 确保函数返回时释放锁
	ga.groups = make(map[string]map[string]AggregatorFunction)
}
