package aggregator

import (
	"math"
	"sort"
	"sync"
)

type AggregateType string

const (
	Sum        AggregateType = "sum"
	Count      AggregateType = "count"
	Avg        AggregateType = "avg"
	Max        AggregateType = "max"
	Min        AggregateType = "min"
	StdDev     AggregateType = "stddev"
	Median     AggregateType = "median"
	Percentile AggregateType = "percentile"
)

type AggregatorFunction interface {
	New() AggregatorFunction
	Add(value float64)
	Result() float64
}

type SumAggregator struct {
	value float64
}

func (s *SumAggregator) New() AggregatorFunction {
	return &SumAggregator{}
}

func (s *SumAggregator) Add(v float64) {
	s.value += v
}

func (s *SumAggregator) Result() float64 {
	return s.value
}

type CountAggregator struct {
	count int
}

func (s *CountAggregator) New() AggregatorFunction {
	return &CountAggregator{}
}

func (c *CountAggregator) Add(_ float64) {
	c.count++
}

func (c *CountAggregator) Result() float64 {
	return float64(c.count)
}

type AvgAggregator struct {
	sum   float64
	count int
}

func (a *AvgAggregator) New() AggregatorFunction {
	return &AvgAggregator{}
}

func (a *AvgAggregator) Add(v float64) {
	a.sum += v
	a.count++
}

func (a *AvgAggregator) Result() float64 {
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

func (m *MedianAggregator) Add(val float64) {
	m.values = append(m.values, val)
}

func (m *MedianAggregator) Result() float64 {
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

func (p *PercentileAggregator) Add(v float64) {
	p.values = append(p.values, v)
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

func (m *MinAggregator) Add(v float64) {
	if m.first || v < m.value {
		m.value = v
		m.first = false
	}
}

func (m *MinAggregator) Result() float64 {
	return m.value
}

type MaxAggregator struct {
	value float64
	first bool
}

func (m *MaxAggregator) New() AggregatorFunction {
	return &MaxAggregator{}
}

func (m *MaxAggregator) Add(v float64) {
	if m.first || v > m.value {
		m.value = v
		m.first = false
	}
}

func (m *MaxAggregator) Result() float64 {
	return m.value
}

func (s *StdDevAggregator) Add(v float64) {
	s.values = append(s.values, v)
}

func (s *StdDevAggregator) Result() float64 {
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

func (p *PercentileAggregator) Result() float64 {
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
