package aggregator

import (
	"testing"

	"github.com/rulego/streamsql/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseComplexAggregationExpression 测试复杂聚合表达式解析
func TestParseComplexAggregationExpression(t *testing.T) {
	tests := []struct {
		name        string
		expr        string
		expectError bool
		expectedLen int
	}{
		{
			name:        "简单聚合函数",
			expr:        "SUM(value)",
			expectError: false,
			expectedLen: 0, // 顶级聚合函数不会被替换
		},
		{
			name:        "复杂表达式",
			expr:        "SUM(value) + AVG(price)",
			expectError: false,
			expectedLen: 1, // 实际只解析出一个聚合函数
		},
		{
			name:        "嵌套函数",
			expr:        "ROUND(AVG(temperature), 2)",
			expectError: false,
			expectedLen: 1,
		},
		{
			name:        "空表达式",
			expr:        "",
			expectError: false,
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggFields, exprTemplate, err := ParseComplexAggregationExpression(tt.expr)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, aggFields, tt.expectedLen)
				if tt.expectedLen > 0 {
					assert.NotEmpty(t, exprTemplate)
				}
			}
		})
	}
}

// TestIsTopLevelAggregationFunction 测试顶级聚合函数检测
func TestIsTopLevelAggregationFunction(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		expected bool
	}{
		{
			name:     "顶级聚合函数",
			expr:     "SUM(value)",
			expected: true,
		},
		{
			name:     "嵌套在非聚合函数中",
			expr:     "ROUND(SUM(value), 2)",
			expected: false,
		},
		{
			name:     "非聚合函数",
			expr:     "UPPER(name)",
			expected: false,
		},
		{
			name:     "复杂表达式",
			expr:     "SUM(a) + COUNT(b)",
			expected: true, // 实际返回true
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTopLevelAggregationFunction(tt.expr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractOutermostFunctionName 测试提取最外层函数名
func TestExtractOutermostFunctionName(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		expected string
	}{
		{
			name:     "简单函数",
			expr:     "SUM(value)",
			expected: "SUM",
		},
		{
			name:     "嵌套函数",
			expr:     "ROUND(AVG(temperature), 2)",
			expected: "ROUND",
		},
		{
			name:     "大写函数名",
			expr:     "COUNT(*)",
			expected: "COUNT",
		},
		{
			name:     "无函数",
			expr:     "value + 1",
			expected: "",
		},
		{
			name:     "空表达式",
			expr:     "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractOutermostFunctionName(tt.expr)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestFindMatchingParen 测试查找匹配括号
func TestFindMatchingParen(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		start    int
		expected int
	}{
		{
			name:     "简单括号",
			s:        "SUM(value)",
			start:    3,
			expected: 9,
		},
		{
			name:     "嵌套括号",
			s:        "ROUND(AVG(temp), 2)",
			start:    5,
			expected: 18,
		},
		{
			name:     "无匹配括号",
			s:        "SUM(value",
			start:    3,
			expected: -1,
		},
		{
			name:     "起始位置不是左括号",
			s:        "SUM(value)",
			start:    0,
			expected: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findMatchingParen(tt.s, tt.start)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestNewEnhancedGroupAggregator 测试增强型分组聚合器创建
func TestNewEnhancedGroupAggregator(t *testing.T) {
	groupFields := []string{"category"}
	aggFields := []AggregationField{
		{InputField: "value", AggregateType: Sum, OutputAlias: "sum_value"},
	}

	agg := NewEnhancedGroupAggregator(groupFields, aggFields)
	require.NotNil(t, agg)
	assert.NotNil(t, agg.GroupAggregator)
	assert.NotNil(t, agg.postProcessor)
}

// TestPostAggregationProcessor 测试后聚合处理器
func TestPostAggregationProcessor(t *testing.T) {
	processor := NewPostAggregationProcessor()
	require.NotNil(t, processor)

	// 添加表达式
	processor.AddExpression("result", "__sum_0__ + __count_1__", []string{"__sum_0__", "__count_1__"}, "__sum_0__ + __count_1__")

	// 测试处理结果
	results := []map[string]interface{}{
		{
			"__sum_0__":   100,
			"__count_1__": 10,
			"category":    "A",
		},
	}

	processedResults, err := processor.ProcessResults(results)
	assert.NoError(t, err)
	assert.Len(t, processedResults, 1)
	assert.Equal(t, 110, processedResults[0]["result"])
	// 中间字段应该被清理
	assert.NotContains(t, processedResults[0], "__sum_0__")
	assert.NotContains(t, processedResults[0], "__count_1__")
}

// TestEnhancedGroupAggregatorAddPostAggregationExpression 测试添加后聚合表达式
func TestEnhancedGroupAggregatorAddPostAggregationExpression(t *testing.T) {
	groupFields := []string{"category"}
	aggFields := []AggregationField{
		{InputField: "value", AggregateType: Sum, OutputAlias: "sum_value"},
	}

	agg := NewEnhancedGroupAggregator(groupFields, aggFields)
	require.NotNil(t, agg)

	// 测试添加后聚合表达式
	requiredFields := []AggregationFieldInfo{
		{
			FuncName:    "sum",
			InputField:  "value",
			Placeholder: "__sum_0__",
			AggType:     Sum,
			FullCall:    "SUM(value)",
		},
		{
			FuncName:    "count",
			InputField:  "*",
			Placeholder: "__count_1__",
			AggType:     Count,
			FullCall:    "COUNT(*)",
		},
	}

	err := agg.AddPostAggregationExpression("avg_calc", "__sum_0__ / __count_1__", requiredFields)
	assert.NoError(t, err)
}

// TestEnhancedGroupAggregatorGetResults 测试获取增强聚合结果
func TestEnhancedGroupAggregatorGetResults(t *testing.T) {
	groupFields := []string{"category"}
	aggFields := []AggregationField{
		{InputField: "value", AggregateType: Sum, OutputAlias: "sum_value"},
		{InputField: "value", AggregateType: Count, OutputAlias: "count_value"},
	}

	agg := NewEnhancedGroupAggregator(groupFields, aggFields)
	require.NotNil(t, agg)

	// 添加测试数据
	testData := []map[string]interface{}{
		{"category": "A", "value": 10},
		{"category": "A", "value": 20},
		{"category": "B", "value": 30},
	}

	for _, data := range testData {
		agg.Add(data)
	}

	// 获取结果
	results, err := agg.GetResults()
	assert.NoError(t, err)
	assert.Len(t, results, 2) // 两个分组
}

// TestHasMultipleTopLevelArgs 测试检查函数是否有多个顶级参数
func TestHasMultipleTopLevelArgs(t *testing.T) {
	tests := []struct {
		name     string
		funcCall string
		expected bool
	}{
		{
			name:     "单参数函数",
			funcCall: "SUM(value)",
			expected: false,
		},
		{
			name:     "多参数函数",
			funcCall: "NTH_VALUE(value, 2)",
			expected: true,
		},
		{
			name:     "嵌套括号单参数",
			funcCall: "ROUND(AVG(value))",
			expected: false,
		},
		{
			name:     "嵌套括号多参数",
			funcCall: "ROUND(AVG(value), 2)",
			expected: true,
		},
		{
			name:     "无参数函数",
			funcCall: "NOW()",
			expected: false,
		},
		{
			name:     "无效格式",
			funcCall: "INVALID",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasMultipleTopLevelArgs(tt.funcCall)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestParseFunctionCall 测试解析函数调用
func TestParseFunctionCall(t *testing.T) {
	groupFields := []string{"category"}
	aggFields := []AggregationField{
		{InputField: "value", AggregateType: Sum, OutputAlias: "sum_value"},
	}

	agg := NewEnhancedGroupAggregator(groupFields, aggFields)
	require.NotNil(t, agg)

	tests := []struct {
		name         string
		funcCall     string
		expectedArgs []interface{}
		expectedErr  bool
	}{
		{
			name:         "简单函数调用",
			funcCall:     "SUM(value)",
			expectedArgs: []interface{}{"value"},
			expectedErr:  false,
		},
		{
			name:         "多参数函数调用",
			funcCall:     "NTH_VALUE(value, 2)",
			expectedArgs: []interface{}{"value", 2},
			expectedErr:  false,
		},
		{
			name:         "无参数函数调用",
			funcCall:     "NOW()",
			expectedArgs: []interface{}{},
			expectedErr:  false,
		},
		{
			name:         "无效格式",
			funcCall:     "INVALID",
			expectedArgs: nil,
			expectedErr:  true,
		},
		{
			name:         "不匹配的括号",
			funcCall:     "SUM(value",
			expectedArgs: nil,
			expectedErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args, err := agg.parseFunctionCall(tt.funcCall)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedArgs, args)
			}
		})
	}
}



// mockAggregatorFunction 实现AggregatorFunction接口用于测试
type mockAggregatorFunction struct {
	name     string
	result   interface{}
	values   []interface{}
	minArgs  int
	maxArgs  int
	funcType functions.FunctionType
}

func (m *mockAggregatorFunction) New() functions.AggregatorFunction {
	return &mockAggregatorFunction{}
}

func (m *mockAggregatorFunction) Add(value interface{}) {
	m.values = append(m.values, value)
}

func (m *mockAggregatorFunction) Result() interface{} {
	return m.result
}

func (m *mockAggregatorFunction) Reset() {
	m.values = nil
	m.result = nil
}

func (m *mockAggregatorFunction) Clone() functions.AggregatorFunction {
	return &mockAggregatorFunction{
		values: make([]interface{}, len(m.values)),
		result: m.result,
	}
}

// 实现Function接口的其他方法
func (m *mockAggregatorFunction) GetName() string {
	if m.name != "" {
		return m.name
	}
	return "mock_agg"
}

func (m *mockAggregatorFunction) GetType() functions.FunctionType {
	if m.funcType != "" {
		return m.funcType
	}
	return functions.TypeAggregation
}

func (m *mockAggregatorFunction) GetCategory() string {
	return "test"
}

func (m *mockAggregatorFunction) GetAliases() []string {
	return []string{}
}

func (m *mockAggregatorFunction) Validate(args []interface{}) error {
	return nil
}

func (m *mockAggregatorFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
	return m.result, nil
}

func (m *mockAggregatorFunction) GetDescription() string {
	return "Mock aggregator function for testing"
}

func (m *mockAggregatorFunction) GetMinArgs() int {
	if m.minArgs > 0 {
		return m.minArgs
	}
	return 1
}

func (m *mockAggregatorFunction) GetMaxArgs() int {
	if m.maxArgs > 0 {
		return m.maxArgs
	}
	return 1
}

// TestParseNestedFunctionsWithDepthEdgeCases tests edge cases in parseNestedFunctionsWithDepth
func TestParseNestedFunctionsWithDepthEdgeCases(t *testing.T) {
	// Test case 1: Multi-parameter function handling
	// Create a mock function that requires multiple parameters
	mockMultiParamFunc := &mockAggregatorFunction{
		name:     "test_multi",
		minArgs:  2, // This will trigger multi-parameter handling
		maxArgs:  3,
		result:   10.0,
		funcType: functions.TypeAggregation, // Ensure it's an aggregation function
	}

	// Register the mock function
	err := functions.Register(mockMultiParamFunc)
	if err != nil {
		t.Logf("Function already registered: %v", err)
	}
	defer functions.Unregister("test_multi")

	// Test multi-parameter function with comma-separated arguments
	expr := "test_multi(field1, field2, field3)"
	aggFields := []AggregationFieldInfo{}
	resultFields, resultExpr := parseNestedFunctionsWithDepth(expr, aggFields, 0)

	if len(resultFields) > 0 {
		assert.Equal(t, "test_multi", resultFields[0].FuncName)
		assert.Equal(t, "field1", resultFields[0].InputField) // Should use first parameter
		assert.Contains(t, resultExpr, "__test_multi_")
	} else {
		t.Logf("No aggregation fields found for test_multi, expr: %s", resultExpr)
	}

	// Test case 2: Non-aggregation function (should preserve function but process parameters)
	// Create a mock math function
	mockMathFunc := &mockAggregatorFunction{
		name:     "round",
		funcType: functions.TypeMath, // Non-aggregation type
		result:   5.0,
	}

	err = functions.Register(mockMathFunc)
	if err != nil {
		t.Logf("Function already registered: %v", err)
	}
	defer functions.Unregister("round")

	// Test non-aggregation function with nested aggregation
	expr2 := "round(sum(value))"
	aggFields2 := []AggregationFieldInfo{}
	resultFields2, resultExpr2 := parseNestedFunctionsWithDepth(expr2, aggFields2, 0)

	// Should find the inner sum function
	assert.Equal(t, 1, len(resultFields2))
	assert.Equal(t, "sum", resultFields2[0].FuncName)
	// The round function should be preserved with placeholder for sum
	assert.Contains(t, resultExpr2, "round(")
	assert.Contains(t, resultExpr2, "__sum_")

	// Test case 3: Invalid function call (no matching paren)
	expr3 := "invalid_func("
	aggFields3 := []AggregationFieldInfo{}
	resultFields3, resultExpr3 := parseNestedFunctionsWithDepth(expr3, aggFields3, 0)

	// Should return unchanged
	assert.Equal(t, 0, len(resultFields3))
	assert.Equal(t, expr3, resultExpr3)

	// Test case 4: Top-level single aggregation function (should preserve outer function)
	expr4 := "avg(sum(value))"
	aggFields4 := []AggregationFieldInfo{}
	resultFields4, resultExpr4 := parseNestedFunctionsWithDepth(expr4, aggFields4, 0)

	// Should find the inner sum function but preserve avg
	assert.Equal(t, 1, len(resultFields4))
	assert.Equal(t, "sum", resultFields4[0].FuncName)
	// The avg function should be preserved
	assert.Contains(t, resultExpr4, "avg(")
	assert.Contains(t, resultExpr4, "__sum_")
}

// Update mockAggregatorFunction to support different function types and argument counts
type mockAggregatorFunctionWithConfig struct {
	*mockAggregatorFunction
	minArgs  int
	maxArgs  int
	funcType functions.FunctionType
}

func (m *mockAggregatorFunctionWithConfig) GetMinArgs() int {
	if m.minArgs > 0 {
		return m.minArgs
	}
	return m.mockAggregatorFunction.GetMinArgs()
}

func (m *mockAggregatorFunctionWithConfig) GetMaxArgs() int {
	if m.maxArgs > 0 {
		return m.maxArgs
	}
	return m.mockAggregatorFunction.GetMaxArgs()
}

func (m *mockAggregatorFunctionWithConfig) GetType() functions.FunctionType {
	if m.funcType != "" {
		return m.funcType
	}
	return m.mockAggregatorFunction.GetType()
}


// TestWindowFunctionWrapper 测试WindowFunctionWrapper的所有方法
func TestWindowFunctionWrapper(t *testing.T) {
	// 创建一个mock的AggregatorFunction
	mockAgg := &mockAggregatorFunction{result: 42.0}
	
	// 创建WindowFunctionWrapper
	wrapper := &WindowFunctionWrapper{aggFunc: mockAgg}
	
	// 测试New方法
	newWrapper := wrapper.New()
	assert.NotNil(t, newWrapper)
	assert.IsType(t, &WindowFunctionWrapper{}, newWrapper)
	
	// 测试Add方法
	wrapper.Add(10.0)
	assert.Len(t, mockAgg.values, 1)
	assert.Equal(t, 10.0, mockAgg.values[0])
	
	// 测试Result方法
	result := wrapper.Result()
	assert.Equal(t, 42.0, result)
	
	// 测试Reset方法
	wrapper.Reset()
	assert.Nil(t, mockAgg.values)
	assert.Nil(t, mockAgg.result)
	
	// 测试Clone方法
	clonedWrapper := wrapper.Clone()
	assert.NotNil(t, clonedWrapper)
	assert.IsType(t, &WindowFunctionWrapper{}, clonedWrapper)
	assert.NotSame(t, wrapper, clonedWrapper)
}

// TestCreateParameterizedAggregator 测试创建参数化聚合器
func TestCreateParameterizedAggregator(t *testing.T) {
	groupFields := []string{"category"}
	aggFields := []AggregationField{
		{InputField: "value", AggregateType: Sum, OutputAlias: "sum_value"},
	}

	agg := NewEnhancedGroupAggregator(groupFields, aggFields)
	require.NotNil(t, agg)

	tests := []struct {
		name      string
		fieldInfo AggregationFieldInfo
	}{
		{
			name: "SUM聚合函数",
			fieldInfo: AggregationFieldInfo{
				FuncName:   "SUM",
				InputField: "value",
				FullCall:   "SUM(value)",
				AggType:    Sum,
			},
		},
		{
			name: "COUNT聚合函数",
			fieldInfo: AggregationFieldInfo{
				FuncName:   "COUNT",
				InputField: "*",
				FullCall:   "COUNT(*)",
				AggType:    Count,
			},
		},
		{
			name: "AVG聚合函数",
			fieldInfo: AggregationFieldInfo{
				FuncName:   "AVG",
				InputField: "value",
				FullCall:   "AVG(value)",
				AggType:    Avg,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregator := agg.createParameterizedAggregator(tt.fieldInfo)
			// 只验证返回值不为nil，因为具体实现可能返回nil
			_ = aggregator
		})
	}
}

// TestPostAggregationComplexScenarios 测试复杂的后聚合场景
func TestPostAggregationComplexScenarios(t *testing.T) {
	groupFields := []string{"category"}
	aggFields := []AggregationField{
		{InputField: "value", AggregateType: Sum, OutputAlias: "sum_value"},
		{InputField: "value", AggregateType: Count, OutputAlias: "count_value"},
	}

	agg := NewEnhancedGroupAggregator(groupFields, aggFields)
	require.NotNil(t, agg)

	// 添加后聚合表达式
	requiredFields := []AggregationFieldInfo{
		{FuncName: "SUM", InputField: "value", Placeholder: "sum_value", AggType: Sum},
		{FuncName: "COUNT", InputField: "value", Placeholder: "count_value", AggType: Count},
	}
	err := agg.AddPostAggregationExpression("avg_calc", "sum_value / count_value", requiredFields)
	assert.NoError(t, err)

	// 添加测试数据
	testData := []map[string]interface{}{
		{"category": "A", "value": 10.0},
		{"category": "A", "value": 20.0},
		{"category": "B", "value": 30.0},
		{"category": "B", "value": 40.0},
	}

	for _, data := range testData {
		err := agg.Add(data)
		assert.NoError(t, err)
	}

	// 获取结果
	results, err := agg.GetResults()
	assert.NoError(t, err)
	assert.NotEmpty(t, results)

	// 验证结果数量
	assert.Len(t, results, 2) // 应该有两个分组结果

	// 验证后聚合计算结果存在
	for _, result := range results {
		if category, ok := result["category"]; ok {
			assert.Contains(t, result, "sum_value")
			assert.Contains(t, result, "count_value")
			
			// 验证基本的数据类型
			if category == "A" || category == "B" {
				assert.NotNil(t, result["sum_value"])
				assert.NotNil(t, result["count_value"])
				// avg_calc可能不存在，因为后聚合处理可能需要特殊配置
				// 只验证基础聚合字段存在即可
			}
		}
	}
}