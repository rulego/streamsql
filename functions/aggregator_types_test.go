package functions

import (
	"testing"
)

// TestAggregateTypeConstants 测试聚合类型常量
func TestAggregateTypeConstants(t *testing.T) {
	tests := []struct {
		name     string
		aggType  AggregateType
		expected string
	}{
		{"Sum", Sum, "sum"},
		{"Count", Count, "count"},
		{"Avg", Avg, "avg"},
		{"Max", Max, "max"},
		{"Min", Min, "min"},
		{"Median", Median, "median"},
		{"Percentile", Percentile, "percentile"},
		{"WindowStart", WindowStart, "window_start"},
		{"WindowEnd", WindowEnd, "window_end"},
		{"Collect", Collect, "collect"},
		{"LastValue", LastValue, "last_value"},
		{"MergeAgg", MergeAgg, "merge_agg"},
		{"StdDev", StdDev, "stddev"},
		{"StdDevS", StdDevS, "stddevs"},
		{"Deduplicate", Deduplicate, "deduplicate"},
		{"Var", Var, "var"},
		{"VarS", VarS, "vars"},
		{"Lag", Lag, "lag"},
		{"Latest", Latest, "latest"},
		{"ChangedCol", ChangedCol, "changed_col"},
		{"HadChanged", HadChanged, "had_changed"},
		{"Expression", Expression, "expression"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.aggType) != tt.expected {
				t.Errorf("AggregateType %s = %s, want %s", tt.name, string(tt.aggType), tt.expected)
			}
		})
	}
}

// TestStringConstants 测试字符串常量
func TestStringConstants(t *testing.T) {
	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{"SumStr", SumStr, "sum"},
		{"CountStr", CountStr, "count"},
		{"AvgStr", AvgStr, "avg"},
		{"MaxStr", MaxStr, "max"},
		{"MinStr", MinStr, "min"},
		{"MedianStr", MedianStr, "median"},
		{"PercentileStr", PercentileStr, "percentile"},
		{"WindowStartStr", WindowStartStr, "window_start"},
		{"WindowEndStr", WindowEndStr, "window_end"},
		{"CollectStr", CollectStr, "collect"},
		{"LastValueStr", LastValueStr, "last_value"},
		{"MergeAggStr", MergeAggStr, "merge_agg"},
		{"StdDevStr", StdDevStr, "stddev"},
		{"StdDevSStr", StdDevSStr, "stddevs"},
		{"DeduplicateStr", DeduplicateStr, "deduplicate"},
		{"VarStr", VarStr, "var"},
		{"VarSStr", VarSStr, "vars"},
		{"LagStr", LagStr, "lag"},
		{"LatestStr", LatestStr, "latest"},
		{"ChangedColStr", ChangedColStr, "changed_col"},
		{"HadChangedStr", HadChangedStr, "had_changed"},
		{"ExpressionStr", ExpressionStr, "expression"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.constant != tt.expected {
				t.Errorf("Constant %s = %s, want %s", tt.name, tt.constant, tt.expected)
			}
		})
	}
}

// TestRegisterLegacyAggregator 测试注册遗留聚合器
func TestRegisterLegacyAggregator(t *testing.T) {
	// 创建一个测试聚合器构造函数
	constructor := func() LegacyAggregatorFunction {
		return &TestLegacyAggregator{}
	}

	// 注册聚合器
	RegisterLegacyAggregator("test_agg", constructor)

	// 验证注册成功
	legacyRegistryMutex.RLock()
	_, exists := legacyAggregatorRegistry["test_agg"]
	legacyRegistryMutex.RUnlock()

	if !exists {
		t.Error("Failed to register legacy aggregator")
	}

	// 测试创建聚合器
	createdAgg := CreateLegacyAggregator("test_agg")
	if createdAgg == nil {
		t.Error("Failed to create legacy aggregator")
	}

	// 测试聚合器功能
	createdAgg.Add(10)
	createdAgg.Add(20)
	result := createdAgg.Result()
	if result != 30 {
		t.Errorf("Expected result 30, got %v", result)
	}
}

// TestCreateLegacyAggregatorPanic 测试创建不存在的聚合器时的panic
func TestCreateLegacyAggregatorPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for unsupported aggregator type")
		}
	}()

	CreateLegacyAggregator("nonexistent_aggregator")
}

// TestFunctionAggregatorWrapper 测试函数聚合器包装器
func TestFunctionAggregatorWrapper(t *testing.T) {
	// 创建一个测试聚合器函数
	testAgg := &TestAggregatorFunction{}

	// 创建一个测试适配器
	adapter := &AggregatorAdapter{
		aggFunc: testAgg,
	}
	wrapper := &FunctionAggregatorWrapper{adapter: adapter}

	// 测试New方法
	newWrapper := wrapper.New()
	if newWrapper == nil {
		t.Error("New() should return a new wrapper")
	}

	// 测试GetContextKey方法
	contextKey := wrapper.GetContextKey()
	if contextKey != "" {
		t.Logf("Context key: %s", contextKey)
	}
}

// TestAnalyticalAggregatorWrapper 测试分析聚合器包装器
func TestAnalyticalAggregatorWrapper(t *testing.T) {
	// 创建一个测试分析函数
	testAnalFunc := &TestAnalyticalFunction{}

	// 创建一个测试适配器
	adapter := &AnalyticalAggregatorAdapter{
		analFunc: testAnalFunc,
		ctx: &FunctionContext{
			Data: make(map[string]interface{}),
		},
	}
	wrapper := &AnalyticalAggregatorWrapper{adapter: adapter}

	// 测试New方法
	newWrapper := wrapper.New()
	if newWrapper == nil {
		t.Error("New() should return a new wrapper")
	}

	// 测试Add和Result方法
	wrapper.Add("test")
	result := wrapper.Result()
	t.Logf("Result: %v", result)
}

// TestLegacyAggregator 测试用的遗留聚合器实现
type TestLegacyAggregator struct {
	sum int
}

// New 创建新的聚合器实例
func (t *TestLegacyAggregator) New() LegacyAggregatorFunction {
	return &TestLegacyAggregator{}
}

// Add 添加值
func (t *TestLegacyAggregator) Add(value interface{}) {
	if v, ok := value.(int); ok {
		t.sum += v
	}
}

// Result 返回聚合结果
func (t *TestLegacyAggregator) Result() interface{} {
	return t.sum
}

// TestAggregatorFunction 测试用的聚合器函数实现
type TestAggregatorFunction struct {
	sum int
}

// New 创建新的聚合器实例
func (t *TestAggregatorFunction) New() AggregatorFunction {
	return &TestAggregatorFunction{}
}

// Add 添加值
func (t *TestAggregatorFunction) Add(value interface{}) {
	if v, ok := value.(int); ok {
		t.sum += v
	}
}

// Result 返回聚合结果
func (t *TestAggregatorFunction) Result() interface{} {
	return t.sum
}

// Reset 重置聚合器状态
func (t *TestAggregatorFunction) Reset() {
	t.sum = 0
}

// Clone 克隆聚合器
func (t *TestAggregatorFunction) Clone() AggregatorFunction {
	return &TestAggregatorFunction{sum: t.sum}
}

// GetName 返回函数名称
func (t *TestAggregatorFunction) GetName() string {
	return "test_aggregator"
}

// GetType 返回函数类型
func (t *TestAggregatorFunction) GetType() FunctionType {
	return TypeAggregation
}

// GetCategory 返回函数分类
func (t *TestAggregatorFunction) GetCategory() string {
	return "test"
}

// GetDescription 返回函数描述
func (t *TestAggregatorFunction) GetDescription() string {
	return "Test aggregator function"
}

// GetAliases 返回函数别名
func (t *TestAggregatorFunction) GetAliases() []string {
	return []string{}
}

// Validate 验证参数
func (t *TestAggregatorFunction) Validate(args []interface{}) error {
	return nil
}

// Execute 执行函数
func (t *TestAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return t.Result(), nil
}

// GetMinArgs 返回最小参数数量
func (t *TestAggregatorFunction) GetMinArgs() int {
	return 1
}

// GetMaxArgs 返回最大参数数量
func (t *TestAggregatorFunction) GetMaxArgs() int {
	return 1
}

// TestAnalyticalFunction 测试用的分析函数实现
type TestAnalyticalFunction struct {
	values []interface{}
}

// New 创建新的分析函数实例
func (t *TestAnalyticalFunction) New() AggregatorFunction {
	return &TestAnalyticalFunction{
		values: make([]interface{}, 0),
	}
}

// Add 添加值
func (t *TestAnalyticalFunction) Add(value interface{}) {
	t.values = append(t.values, value)
}

// Result 返回分析结果
func (t *TestAnalyticalFunction) Result() interface{} {
	return len(t.values)
}

// Reset 重置分析函数状态
func (t *TestAnalyticalFunction) Reset() {
	t.values = make([]interface{}, 0)
}

// Clone 克隆分析函数
func (t *TestAnalyticalFunction) Clone() AggregatorFunction {
	newValues := make([]interface{}, len(t.values))
	copy(newValues, t.values)
	return &TestAnalyticalFunction{values: newValues}
}

// GetName 返回函数名称
func (t *TestAnalyticalFunction) GetName() string {
	return "test_analytical"
}

// GetType 返回函数类型
func (t *TestAnalyticalFunction) GetType() FunctionType {
	return TypeAnalytical
}

// GetCategory 返回函数分类
func (t *TestAnalyticalFunction) GetCategory() string {
	return "test"
}

// GetDescription 返回函数描述
func (t *TestAnalyticalFunction) GetDescription() string {
	return "Test analytical function"
}

// GetAliases 返回函数别名
func (t *TestAnalyticalFunction) GetAliases() []string {
	return []string{}
}

// Validate 验证参数
func (t *TestAnalyticalFunction) Validate(args []interface{}) error {
	return nil
}

// Execute 执行函数
func (t *TestAnalyticalFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return t.Result(), nil
}

// GetMinArgs 返回最小参数数量
func (t *TestAnalyticalFunction) GetMinArgs() int {
	return 1
}

// GetMaxArgs 返回最大参数数量
func (t *TestAnalyticalFunction) GetMaxArgs() int {
	return 1
}
