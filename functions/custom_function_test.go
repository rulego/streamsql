package functions

import (
	"fmt"
	"math"
	"testing"

	"github.com/rulego/streamsql/utils/cast"
)

func TestAggregatorFunctionInterface(t *testing.T) {
	// Test Sum aggregation function
	sumFunc := NewSumFunction()

	// Test creating new instance
	aggInstance := sumFunc.New()
	if aggInstance == nil {
		t.Fatal("Failed to create new aggregator instance")
	}

	// Test incremental calculation
	aggInstance.Add(10.0)
	aggInstance.Add(20.0)
	aggInstance.Add(30.0)

	result := aggInstance.Result()
	if result != 60.0 {
		t.Errorf("Expected 60.0, got %v", result)
	}

	// Test reset
	aggInstance.Reset()
	result = aggInstance.Result()
	if result != nil {
		t.Errorf("Expected nil after reset (SQL standard: SUM with no rows returns NULL), got %v", result)
	}

	// Test clone
	aggInstance.Add(15.0)
	cloned := aggInstance.Clone()
	cloned.Add(25.0)

	originalResult := aggInstance.Result()
	clonedResult := cloned.Result()

	if originalResult != 15.0 {
		t.Errorf("Expected original result 15.0, got %v", originalResult)
	}
	if clonedResult != 40.0 {
		t.Errorf("Expected cloned result 40.0, got %v", clonedResult)
	}
}

func TestCreateAggregator(t *testing.T) {
	// Test creating registered aggregator
	aggFunc, err := CreateAggregator("sum")
	if err != nil {
		t.Fatalf("Failed to create sum aggregator: %v", err)
	}
	if aggFunc == nil {
		t.Fatal("Created aggregator is nil")
	}

	// Test creating non-existent aggregator
	_, err = CreateAggregator("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent aggregator")
	}
}

func TestAggregatorAdapter(t *testing.T) {
	// 测试聚合器适配器
	adapter, err := NewAggregatorAdapter("sum")
	if err != nil {
		t.Fatalf("Failed to create aggregator adapter: %v", err)
	}

	// 测试创建新实例
	newInstance := adapter.New()
	if newInstance == nil {
		t.Fatal("Failed to create new adapter instance")
	}

	newAdapter, ok := newInstance.(*AggregatorAdapter)
	if !ok {
		t.Fatal("New instance is not an AggregatorAdapter")
	}

	// 测试添加值和获取结果
	newAdapter.Add(5.0)
	newAdapter.Add(10.0)

	result := newAdapter.Result()
	if result != 15.0 {
		t.Errorf("Expected 15.0, got %v", result)
	}
}

func TestCustomFunctionRegistration(t *testing.T) {
	// 注册自定义函数示例
	RegisterCustomFunctions()

	// 测试自定义聚合函数
	productFunc, exists := Get("product")
	if !exists {
		t.Fatal("Custom product function not registered")
	}

	if productFunc.GetType() != TypeAggregation {
		t.Error("Product function should be aggregation type")
	}

	// 测试自定义分析函数
	movingAvgFunc, exists := Get("moving_avg")
	if !exists {
		t.Fatal("Custom moving average function not registered")
	}

	if movingAvgFunc.GetType() != TypeAnalytical {
		t.Error("Moving average function should be analytical type")
	}

	// 测试简单自定义函数
	doubleFunc, exists := Get("double")
	if !exists {
		t.Fatal("Custom double function not registered")
	}

	ctx := &FunctionContext{
		Data: make(map[string]any),
	}

	result, err := doubleFunc.Execute(ctx, []any{5.0})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result != 10.0 {
		t.Errorf("Expected 10.0, got %v", result)
	}
}

func TestFunctionRegistry(t *testing.T) {
	// 测试函数注册表
	allFunctions := ListAll()
	if len(allFunctions) == 0 {
		t.Error("No functions registered")
	}

	// 测试按类型获取函数
	aggFunctions := GetByType(TypeAggregation)
	if len(aggFunctions) == 0 {
		t.Error("No aggregation functions found")
	}

	analFunctions := GetByType(TypeAnalytical)
	if len(analFunctions) == 0 {
		t.Error("No analytical functions found")
	}

	// 验证一些内置函数存在
	expectedFunctions := []string{"sum", "avg", "min", "max", "count", "lag", "latest"}
	for _, funcName := range expectedFunctions {
		if _, exists := Get(funcName); !exists {
			t.Errorf("Expected function %s not found", funcName)
		}
	}
}

func BenchmarkAggregatorIncremental(b *testing.B) {
	sumFunc := NewSumFunction()
	aggInstance := sumFunc.New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggInstance.Add(float64(i))
	}
	_ = aggInstance.Result()
}

func BenchmarkAggregatorBatch(b *testing.B) {
	sumFunc := NewSumFunction()
	ctx := &FunctionContext{
		Data: make(map[string]any),
	}

	// 准备测试数据
	args := make([]any, b.N)
	for i := 0; i < b.N; i++ {
		args[i] = float64(i)
	}

	b.ResetTimer()
	_, _ = sumFunc.Execute(ctx, args)
}

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

func (f *CustomProductFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *CustomProductFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

func (f *CustomProductFunction) Add(value any) {
	if val, err := cast.ToFloat64E(value); err == nil {
		if f.first {
			f.product = val
			f.first = false
		} else {
			f.product *= val
		}
	}
}

func (f *CustomProductFunction) Result() any {
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

// CustomMovingAverageFunction 自定义移动平均分析函数示例。
// 演示自定义分析函数的正确写法：实现 StatefulAnalytic（NewState→Apply），跨行状态
// 放在独立 State 里，由 AnalyticEngine 为每个 PARTITION 各持一份逐条求值。
type CustomMovingAverageFunction struct {
	*BaseFunction
	windowSize int
}

func NewCustomMovingAverageFunction(windowSize int) *CustomMovingAverageFunction {
	return &CustomMovingAverageFunction{
		BaseFunction: NewBaseFunction("moving_avg", TypeAnalytical, "自定义分析函数",
			fmt.Sprintf("计算窗口大小为%d的移动平均", windowSize), 1, 1),
		windowSize: windowSize,
	}
}

func (f *CustomMovingAverageFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

// Execute 标量路径禁用：分析函数需跨行状态，由 AnalyticEngine 的状态机求值。
func (f *CustomMovingAverageFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

// NewState 实现 StatefulAnalytic：每个 PARTITION 一份独立窗口状态。
func (f *CustomMovingAverageFunction) NewState() AnalyticState {
	return &movingAvgState{windowSize: f.windowSize}
}

// movingAvgState 维护最近 windowSize 个值，Apply 返回当前窗口均值。
type movingAvgState struct {
	windowSize int
	values     []float64
}

func (s *movingAvgState) Apply(args []any) any {
	if len(args) == 0 {
		return nil
	}
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil
	}
	s.values = append(s.values, val)
	if len(s.values) > s.windowSize {
		s.values = s.values[len(s.values)-s.windowSize:]
	}
	sum := 0.0
	for _, v := range s.values {
		sum += v
	}
	return sum / float64(len(s.values))
}

func (s *movingAvgState) Reset() { s.values = nil }

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

func (f *CustomGeometricMeanFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *CustomGeometricMeanFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

func (f *CustomGeometricMeanFunction) Add(value any) {
	if val, err := cast.ToFloat64E(value); err == nil && val > 0 {
		f.product *= val
		f.count++
	}
}

func (f *CustomGeometricMeanFunction) Result() any {
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
	_ = Register(NewCustomProductFunction())
	_ = Register(NewCustomGeometricMeanFunction())

	// 注册自定义分析函数
	_ = Register(NewCustomMovingAverageFunction(5)) // 5个值的移动平均

	// 注册适配器
	RegisterAggregatorAdapter("product")
	RegisterAggregatorAdapter("geomean")

	// 使用RegisterCustomFunction的方式注册简单函数
	RegisterCustomFunction("double", TypeMath, "自定义函数", "将值乘以2", 1, 1,
		func(ctx *FunctionContext, args []any) (any, error) {
			val, err := cast.ToFloat64E(args[0])
			if err != nil {
				return nil, err
			}
			return val * 2, nil
		})
}
