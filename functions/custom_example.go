package functions

import (
	"fmt"
	"math"

	"github.com/rulego/streamsql/utils/cast"
)

// CustomProductFunction 自定义乘积聚合函数示例
type CustomProductFunction struct {
	*BaseFunction
	product float64
	first   bool
}

func NewCustomProductFunction() *CustomProductFunction {
	return &CustomProductFunction{
		BaseFunction: NewBaseFunction("product", TypeAggregation, "自定义聚合函数", "计算数值乘积", 1, -1),
		product:      1.0,
		first:        true,
	}
}

func (f *CustomProductFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CustomProductFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	product := 1.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			return nil, err
		}
		product *= val
	}
	return product, nil
}

// 实现AggregatorFunction接口
func (f *CustomProductFunction) New() AggregatorFunction {
	return &CustomProductFunction{
		BaseFunction: f.BaseFunction,
		product:      1.0,
		first:        true,
	}
}

func (f *CustomProductFunction) Add(value interface{}) {
	if val, err := cast.ToFloat64E(value); err == nil {
		if f.first {
			f.product = val
			f.first = false
		} else {
			f.product *= val
		}
	}
}

func (f *CustomProductFunction) Result() interface{} {
	if f.first {
		return 0.0
	}
	return f.product
}

func (f *CustomProductFunction) Reset() {
	f.product = 1.0
	f.first = true
}

func (f *CustomProductFunction) Clone() AggregatorFunction {
	return &CustomProductFunction{
		BaseFunction: f.BaseFunction,
		product:      f.product,
		first:        f.first,
	}
}

// CustomMovingAverageFunction 自定义移动平均分析函数示例
type CustomMovingAverageFunction struct {
	*BaseFunction
	values     []float64
	windowSize int
}

func NewCustomMovingAverageFunction(windowSize int) *CustomMovingAverageFunction {
	return &CustomMovingAverageFunction{
		BaseFunction: NewBaseFunction("moving_avg", TypeAnalytical, "自定义分析函数",
			fmt.Sprintf("计算窗口大小为%d的移动平均", windowSize), 1, 1),
		windowSize: windowSize,
		values:     make([]float64, 0),
	}
}

func (f *CustomMovingAverageFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CustomMovingAverageFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}

	// 添加新值
	f.values = append(f.values, val)

	// 保持窗口大小
	if len(f.values) > f.windowSize {
		f.values = f.values[1:]
	}

	// 计算移动平均
	sum := 0.0
	for _, v := range f.values {
		sum += v
	}

	return sum / float64(len(f.values)), nil
}

// 实现AnalyticalFunction接口
func (f *CustomMovingAverageFunction) Reset() {
	f.values = make([]float64, 0)
}

// 实现AggregatorFunction接口 - 增量计算支持
func (f *CustomMovingAverageFunction) New() AggregatorFunction {
	return &CustomMovingAverageFunction{
		BaseFunction: f.BaseFunction,
		windowSize:   f.windowSize,
		values:       make([]float64, 0),
	}
}

func (f *CustomMovingAverageFunction) Add(value interface{}) {
	if val, err := cast.ToFloat64E(value); err == nil {
		// 添加新值
		f.values = append(f.values, val)
		// 保持窗口大小
		if len(f.values) > f.windowSize {
			f.values = f.values[1:]
		}
	}
}

func (f *CustomMovingAverageFunction) Result() interface{} {
	if len(f.values) == 0 {
		return 0.0
	}
	// 计算移动平均
	sum := 0.0
	for _, v := range f.values {
		sum += v
	}
	return sum / float64(len(f.values))
}

func (f *CustomMovingAverageFunction) Clone() AggregatorFunction {
	clone := &CustomMovingAverageFunction{
		BaseFunction: f.BaseFunction,
		windowSize:   f.windowSize,
		values:       make([]float64, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// CustomGeometricMeanFunction 自定义几何平均聚合函数示例
type CustomGeometricMeanFunction struct {
	*BaseFunction
	product float64
	count   int
}

func NewCustomGeometricMeanFunction() *CustomGeometricMeanFunction {
	return &CustomGeometricMeanFunction{
		BaseFunction: NewBaseFunction("geomean", TypeAggregation, "自定义聚合函数", "计算几何平均数", 1, -1),
		product:      1.0,
		count:        0,
	}
}

func (f *CustomGeometricMeanFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CustomGeometricMeanFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	product := 1.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			return nil, err
		}
		if val <= 0 {
			return nil, fmt.Errorf("geometric mean requires positive values")
		}
		product *= val
	}
	return math.Pow(product, 1.0/float64(len(args))), nil
}

// 实现AggregatorFunction接口
func (f *CustomGeometricMeanFunction) New() AggregatorFunction {
	return &CustomGeometricMeanFunction{
		BaseFunction: f.BaseFunction,
		product:      1.0,
		count:        0,
	}
}

func (f *CustomGeometricMeanFunction) Add(value interface{}) {
	if val, err := cast.ToFloat64E(value); err == nil && val > 0 {
		f.product *= val
		f.count++
	}
}

func (f *CustomGeometricMeanFunction) Result() interface{} {
	if f.count == 0 {
		return 0.0
	}
	return math.Pow(f.product, 1.0/float64(f.count))
}

func (f *CustomGeometricMeanFunction) Reset() {
	f.product = 1.0
	f.count = 0
}

func (f *CustomGeometricMeanFunction) Clone() AggregatorFunction {
	return &CustomGeometricMeanFunction{
		BaseFunction: f.BaseFunction,
		product:      f.product,
		count:        f.count,
	}
}

// RegisterCustomFunctions 注册自定义函数的示例
func RegisterCustomFunctions() {
	// 注册自定义聚合函数
	Register(NewCustomProductFunction())
	Register(NewCustomGeometricMeanFunction())

	// 注册自定义分析函数
	Register(NewCustomMovingAverageFunction(5)) // 5个值的移动平均

	// 注册适配器
	RegisterAggregatorAdapter("product")
	RegisterAggregatorAdapter("geomean")
	RegisterAnalyticalAdapter("moving_avg")

	// 使用RegisterCustomFunction的方式注册简单函数
	RegisterCustomFunction("double", TypeAggregation, "自定义函数", "将值乘以2", 1, 1,
		func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
			val, err := cast.ToFloat64E(args[0])
			if err != nil {
				return nil, err
			}
			return val * 2, nil
		})
}
