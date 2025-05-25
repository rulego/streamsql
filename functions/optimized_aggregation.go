package functions

import (
	"math"

	"github.com/rulego/streamsql/utils/cast"
)

// OptimizedStdDevFunction 优化的标准差函数，使用韦尔福德算法实现O(1)空间复杂度
type OptimizedStdDevFunction struct {
	*BaseFunction
	count int
	mean  float64
	m2    float64 // 平方差的累计值
}

func NewOptimizedStdDevFunction() *OptimizedStdDevFunction {
	return &OptimizedStdDevFunction{
		BaseFunction: NewBaseFunction("stddev_optimized", TypeAggregation, "优化聚合函数", "使用韦尔福德算法计算标准差", 1, -1),
	}
}

func (f *OptimizedStdDevFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *OptimizedStdDevFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 批量执行模式，回退到传统算法
	sum := 0.0
	count := 0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		sum += val
		count++
	}
	if count == 0 {
		return 0.0, nil
	}
	mean := sum / float64(count)
	variance := 0.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		variance += math.Pow(val-mean, 2)
	}
	return math.Sqrt(variance / float64(count)), nil
}

// 实现AggregatorFunction接口 - 韦尔福德算法
func (f *OptimizedStdDevFunction) New() AggregatorFunction {
	return &OptimizedStdDevFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *OptimizedStdDevFunction) Add(value interface{}) {
	val, err := cast.ToFloat64E(value)
	if err != nil {
		return
	}

	f.count++
	delta := val - f.mean
	f.mean += delta / float64(f.count)
	delta2 := val - f.mean
	f.m2 += delta * delta2
}

func (f *OptimizedStdDevFunction) Result() interface{} {
	if f.count < 1 {
		return 0.0
	}
	variance := f.m2 / float64(f.count)
	return math.Sqrt(variance)
}

func (f *OptimizedStdDevFunction) Reset() {
	f.count = 0
	f.mean = 0
	f.m2 = 0
}

func (f *OptimizedStdDevFunction) Clone() AggregatorFunction {
	return &OptimizedStdDevFunction{
		BaseFunction: f.BaseFunction,
		count:        f.count,
		mean:         f.mean,
		m2:           f.m2,
	}
}

// OptimizedVarFunction 优化的方差函数，使用韦尔福德算法实现O(1)空间复杂度
type OptimizedVarFunction struct {
	*BaseFunction
	count int
	mean  float64
	m2    float64
}

func NewOptimizedVarFunction() *OptimizedVarFunction {
	return &OptimizedVarFunction{
		BaseFunction: NewBaseFunction("var_optimized", TypeAggregation, "优化聚合函数", "使用韦尔福德算法计算总体方差", 1, -1),
	}
}

func (f *OptimizedVarFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *OptimizedVarFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 批量执行模式
	sum := 0.0
	count := 0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		sum += val
		count++
	}
	if count == 0 {
		return 0.0, nil
	}
	mean := sum / float64(count)
	variance := 0.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		variance += math.Pow(val-mean, 2)
	}
	return variance / float64(count), nil
}

// 实现AggregatorFunction接口 - 韦尔福德算法
func (f *OptimizedVarFunction) New() AggregatorFunction {
	return &OptimizedVarFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *OptimizedVarFunction) Add(value interface{}) {
	val, err := cast.ToFloat64E(value)
	if err != nil {
		return
	}

	f.count++
	delta := val - f.mean
	f.mean += delta / float64(f.count)
	delta2 := val - f.mean
	f.m2 += delta * delta2
}

func (f *OptimizedVarFunction) Result() interface{} {
	if f.count < 1 {
		return 0.0
	}
	return f.m2 / float64(f.count)
}

func (f *OptimizedVarFunction) Reset() {
	f.count = 0
	f.mean = 0
	f.m2 = 0
}

func (f *OptimizedVarFunction) Clone() AggregatorFunction {
	return &OptimizedVarFunction{
		BaseFunction: f.BaseFunction,
		count:        f.count,
		mean:         f.mean,
		m2:           f.m2,
	}
}

// OptimizedVarSFunction 优化的样本方差函数
type OptimizedVarSFunction struct {
	*BaseFunction
	count int
	mean  float64
	m2    float64
}

func NewOptimizedVarSFunction() *OptimizedVarSFunction {
	return &OptimizedVarSFunction{
		BaseFunction: NewBaseFunction("vars_optimized", TypeAggregation, "优化聚合函数", "使用韦尔福德算法计算样本方差", 1, -1),
	}
}

func (f *OptimizedVarSFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *OptimizedVarSFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 批量执行模式
	sum := 0.0
	count := 0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		sum += val
		count++
	}
	if count <= 1 {
		return 0.0, nil
	}
	mean := sum / float64(count)
	variance := 0.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		variance += math.Pow(val-mean, 2)
	}
	return variance / float64(count-1), nil
}

// 实现AggregatorFunction接口 - 韦尔福德算法
func (f *OptimizedVarSFunction) New() AggregatorFunction {
	return &OptimizedVarSFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *OptimizedVarSFunction) Add(value interface{}) {
	val, err := cast.ToFloat64E(value)
	if err != nil {
		return
	}

	f.count++
	delta := val - f.mean
	f.mean += delta / float64(f.count)
	delta2 := val - f.mean
	f.m2 += delta * delta2
}

func (f *OptimizedVarSFunction) Result() interface{} {
	if f.count < 2 {
		return 0.0
	}
	return f.m2 / float64(f.count-1)
}

func (f *OptimizedVarSFunction) Reset() {
	f.count = 0
	f.mean = 0
	f.m2 = 0
}

func (f *OptimizedVarSFunction) Clone() AggregatorFunction {
	return &OptimizedVarSFunction{
		BaseFunction: f.BaseFunction,
		count:        f.count,
		mean:         f.mean,
		m2:           f.m2,
	}
}

// OptimizedStdDevSFunction 优化的样本标准差函数
type OptimizedStdDevSFunction struct {
	*BaseFunction
	count int
	mean  float64
	m2    float64
}

func NewOptimizedStdDevSFunction() *OptimizedStdDevSFunction {
	return &OptimizedStdDevSFunction{
		BaseFunction: NewBaseFunction("stddevs_optimized", TypeAggregation, "优化聚合函数", "使用韦尔福德算法计算样本标准差", 1, -1),
	}
}

func (f *OptimizedStdDevSFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *OptimizedStdDevSFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 批量执行模式
	sum := 0.0
	count := 0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		sum += val
		count++
	}
	if count <= 1 {
		return 0.0, nil
	}
	mean := sum / float64(count)
	variance := 0.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		variance += math.Pow(val-mean, 2)
	}
	return math.Sqrt(variance / float64(count-1)), nil
}

// 实现AggregatorFunction接口 - 韦尔福德算法
func (f *OptimizedStdDevSFunction) New() AggregatorFunction {
	return &OptimizedStdDevSFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *OptimizedStdDevSFunction) Add(value interface{}) {
	val, err := cast.ToFloat64E(value)
	if err != nil {
		return
	}

	f.count++
	delta := val - f.mean
	f.mean += delta / float64(f.count)
	delta2 := val - f.mean
	f.m2 += delta * delta2
}

func (f *OptimizedStdDevSFunction) Result() interface{} {
	if f.count < 2 {
		return 0.0
	}
	variance := f.m2 / float64(f.count-1)
	return math.Sqrt(variance)
}

func (f *OptimizedStdDevSFunction) Reset() {
	f.count = 0
	f.mean = 0
	f.m2 = 0
}

func (f *OptimizedStdDevSFunction) Clone() AggregatorFunction {
	return &OptimizedStdDevSFunction{
		BaseFunction: f.BaseFunction,
		count:        f.count,
		mean:         f.mean,
		m2:           f.m2,
	}
}
