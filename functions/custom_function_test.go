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
	// Test the aggregator adapter
	adapter, err := NewAggregatorAdapter("sum")
	if err != nil {
		t.Fatalf("Failed to create aggregator adapter: %v", err)
	}

	// Test to create new instances
	newInstance := adapter.New()
	if newInstance == nil {
		t.Fatal("Failed to create new adapter instance")
	}

	newAdapter, ok := newInstance.(*AggregatorAdapter)
	if !ok {
		t.Fatal("New instance is not an AggregatorAdapter")
	}

	// Test the added value and obtain the results
	newAdapter.Add(5.0)
	newAdapter.Add(10.0)

	result := newAdapter.Result()
	if result != 15.0 {
		t.Errorf("Expected 15.0, got %v", result)
	}
}

func TestCustomFunctionRegistration(t *testing.T) {
	// Register custom function examples
	RegisterCustomFunctions()

	// Test custom aggregate functions
	productFunc, exists := Get("product")
	if !exists {
		t.Fatal("Custom product function not registered")
	}

	if productFunc.GetType() != TypeAggregation {
		t.Error("Product function should be aggregation type")
	}

	// Test the custom analysis function
	movingAvgFunc, exists := Get("moving_avg")
	if !exists {
		t.Fatal("Custom moving average function not registered")
	}

	if movingAvgFunc.GetType() != TypeAnalytical {
		t.Error("Moving average function should be analytical type")
	}

	// Test simple custom functions
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
	// Test the function registry
	allFunctions := ListAll()
	if len(allFunctions) == 0 {
		t.Error("No functions registered")
	}

	// Test to get functions by type
	aggFunctions := GetByType(TypeAggregation)
	if len(aggFunctions) == 0 {
		t.Error("No aggregation functions found")
	}

	analFunctions := GetByType(TypeAnalytical)
	if len(analFunctions) == 0 {
		t.Error("No analytical functions found")
	}

	// Verify the existence of some built-in functions
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

	// Prepare test data
	args := make([]any, b.N)
	for i := 0; i < b.N; i++ {
		args[i] = float64(i)
	}

	b.ResetTimer()
	_, _ = sumFunc.Execute(ctx, args)
}

// Example of a custom product aggregation function with CustomProductFunction
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

// Implement the AggregatorFunction interface
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

// CustomMovingAverageFunction: Example of a custom moving average analysis function.
// Demonstrates the correct way to write custom analysis functions: implement StatefulAnalytic(NewState→Apply) and span lines
// Placed in independent states, AnalyticEngine holds a copy for each PARTITION and evaluates each entry.
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

// Execute scalar path disabled: The analysis function needs to be cross-line state, evaluated by the state machine of AnalyticEngine.
func (f *CustomMovingAverageFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER, not in a scalar expression", f.GetName())
}

// NewState implements StatefulAnalytic: Each PARTITION has its own independent window state.
func (f *CustomMovingAverageFunction) NewState() AnalyticState {
	return &movingAvgState{windowSize: f.windowSize}
}

// movingAvgState maintains the value of the most recent windowSize, and Apply returns the current window's average.
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

// CustomGeometricMeanFunction is an example custom geometric-mean aggregation function.
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

// Implement the AggregatorFunction interface
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

// RegisterCustomFunctions Example of registering custom functions
func RegisterCustomFunctions() {
	// Register custom aggregation functions
	_ = Register(NewCustomProductFunction())
	_ = Register(NewCustomGeometricMeanFunction())

	// Register custom analysis functions
	_ = Register(NewCustomMovingAverageFunction(5)) // Moving average of 5 values

	// Register the adapter
	RegisterAggregatorAdapter("product")
	RegisterAggregatorAdapter("geomean")

	// Register simple functions using the RegisterCustomFunction method
	RegisterCustomFunction("double", TypeMath, "自定义函数", "将值乘以2", 1, 1,
		func(ctx *FunctionContext, args []any) (any, error) {
			val, err := cast.ToFloat64E(args[0])
			if err != nil {
				return nil, err
			}
			return val * 2, nil
		})
}
