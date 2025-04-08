package aggregator

import (
	"math"
	"sort"
	"strconv"
	"sync"
)

type AggregateType string

const (
	Sum         AggregateType = "sum"
	Count       AggregateType = "count"
	Avg         AggregateType = "avg"
	Max         AggregateType = "max"
	Min         AggregateType = "min"
	StdDev      AggregateType = "stddev"
	Median      AggregateType = "median"
	Percentile  AggregateType = "percentile"
	WindowStart AggregateType = "window_start"
	WindowEnd   AggregateType = "window_end"
)

type AggregatorFunction interface {
	New() AggregatorFunction
	Add(value interface{})
	Result() interface{}
}

type SumAggregator struct {
	value float64
}

func (s *SumAggregator) New() AggregatorFunction {
	return &SumAggregator{}
}

func (s *SumAggregator) Add(v interface{}) {
	var vv float64 = ConvertToFloat64(v, 0)
	s.value += vv
}

func (s *SumAggregator) Result() interface{} {
	return s.value
}

type CountAggregator struct {
	count int
}

func (s *CountAggregator) New() AggregatorFunction {
	return &CountAggregator{}
}

func (c *CountAggregator) Add(_ interface{}) {
	c.count++
}

func (c *CountAggregator) Result() interface{} {
	return float64(c.count)
}

type AvgAggregator struct {
	sum   float64
	count int
}

func (a *AvgAggregator) New() AggregatorFunction {
	return &AvgAggregator{}
}

func (a *AvgAggregator) Add(v interface{}) {
	var vv float64 = ConvertToFloat64(v, 0)
	a.sum += vv
	a.count++
}

func (a *AvgAggregator) Result() interface{} {
	if a.count == 0 {
		return 0
	}
	return a.sum / float64(a.count)
}

var (
	aggregatorRegistry = make(map[string]func() AggregatorFunction)
	registryMutex      sync.RWMutex
)

// Register 添加自定义聚合器到全局注册表
func Register(name string, constructor func() AggregatorFunction) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	aggregatorRegistry[name] = constructor
}

func CreateBuiltinAggregator(aggType AggregateType) AggregatorFunction {
	registryMutex.RLock()
	constructor, exists := aggregatorRegistry[string(aggType)]
	registryMutex.RUnlock()
	if exists {
		return constructor()
	}

	switch aggType {
	case Sum:
		return &SumAggregator{}
	case Count:
		return &CountAggregator{}
	case Avg:
		return &AvgAggregator{}
	case Min:
		return &MinAggregator{}
	case Max:
		return &MaxAggregator{}
	case StdDev:
		return &StdDevAggregator{}
	//case "var":
	//	return &VarAggregator{}
	case Median:
		return &MedianAggregator{}
	case Percentile:
		return &PercentileAggregator{p: 0.95}
	case WindowStart:
		return &WindowStartAggregator{}
	case WindowEnd:
		return &WindowEndAggregator{}
	default:
		panic("unsupported aggregator type: " + aggType)
	}
}

type StdDevAggregator struct {
	values []float64
}

func (s *StdDevAggregator) New() AggregatorFunction {
	return &StdDevAggregator{}
}

func calculateVariance(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	avg := calculateAverage(values)
	var sum float64
	for _, v := range values {
		sum += (v - avg) * (v - avg)
	}
	return sum / float64(len(values)-1)
}

type MedianAggregator struct {
	values []float64
}

func (m *MedianAggregator) New() AggregatorFunction {
	return &MedianAggregator{}
}

func (m *MedianAggregator) Add(val interface{}) {
	var vv float64 = ConvertToFloat64(val, 0)
	m.values = append(m.values, vv)
}

func (m *MedianAggregator) Result() interface{} {
	sort.Float64s(m.values)
	return m.values[len(m.values)/2]
}

type PercentileAggregator struct {
	values []float64
	p      float64
}

func (p *PercentileAggregator) New() AggregatorFunction {
	return &PercentileAggregator{}
}

func (p *PercentileAggregator) Add(v interface{}) {
	vv := ConvertToFloat64(v, 0)
	p.values = append(p.values, vv)
}

type MinAggregator struct {
	value float64
	first bool
}

func (s *MinAggregator) New() AggregatorFunction {
	return &MinAggregator{
		first: true,
	}
}

func (m *MinAggregator) Add(v interface{}) {
	var vv float64 = ConvertToFloat64(v, math.MaxFloat64)
	if m.first || vv < m.value {
		m.value = vv
		m.first = false
	}
}

func (m *MinAggregator) Result() interface{} {
	return m.value
}

type MaxAggregator struct {
	value float64
	first bool
}

func (m *MaxAggregator) New() AggregatorFunction {
	return &MaxAggregator{}
}

func (m *MaxAggregator) Add(v interface{}) {
	var vv float64 = ConvertToFloat64(v, 0)
	if m.first || vv > m.value {
		m.value = vv
		m.first = false
	}
}

func (m *MaxAggregator) Result() interface{} {
	return m.value
}

func (s *StdDevAggregator) Add(v interface{}) {
	var vv float64 = ConvertToFloat64(v, 0)
	s.values = append(s.values, vv)
}

func (s *StdDevAggregator) Result() interface{} {
	if len(s.values) < 2 {
		return 0
	}
	avg := calculateAverage(s.values)
	var sum float64
	for _, v := range s.values {
		sum += (v - avg) * (v - avg)
	}
	return math.Sqrt(sum / float64(len(s.values)-1))
}

func (p *PercentileAggregator) Result() interface{} {
	if len(p.values) == 0 {
		return 0
	}
	sort.Float64s(p.values)
	index := p.p * float64(len(p.values)-1)
	return p.values[int(index)]
}

func calculateAverage(values []float64) float64 {
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func ConvertToFloat64(v interface{}, defaultVal float64) float64 {
	var vv float64 = defaultVal
	switch val := v.(type) {
	case float64:
		vv = val
	case float32:
		vv = float64(val)
	case int:
		vv = float64(val)
	case int32:
		vv = float64(val)
	case int64:
		vv = float64(val)
	case uint:
		vv = float64(val)
	case uint32:
		vv = float64(val)
	case uint64:
		vv = float64(val)
	case string:
		// 处理字符串类型的转换
		if floatValue, err := strconv.ParseFloat(val, 64); err == nil {
			vv = floatValue
		} else {
			panic("unsupported type for sum aggregator")
		}
	default:
		panic("unsupported type for sum aggregator")
	}
	return vv
}
