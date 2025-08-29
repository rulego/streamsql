package functions

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockParameterizedAggregator 实现ParameterizedFunction接口用于测试
type mockParameterizedAggregator struct {
	name        string
	args        []interface{}
	initialized bool
	addedValues []interface{}
	result      interface{}
}

// 实现Function接口
func (m *mockParameterizedAggregator) GetName() string {
	return m.name
}

func (m *mockParameterizedAggregator) GetMinArgs() int {
	return 1
}

func (m *mockParameterizedAggregator) GetMaxArgs() int {
	return 3
}

func (m *mockParameterizedAggregator) GetType() FunctionType {
	return TypeAggregation
}

func (m *mockParameterizedAggregator) GetCategory() string {
	return "test"
}

func (m *mockParameterizedAggregator) GetAliases() []string {
	return []string{}
}

func (m *mockParameterizedAggregator) Validate(args []interface{}) error {
	return nil
}

func (m *mockParameterizedAggregator) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return m.result, nil
}

func (m *mockParameterizedAggregator) GetDescription() string {
	return "Mock parameterized aggregator"
}

// 实现AggregatorFunction接口
func (m *mockParameterizedAggregator) New() AggregatorFunction {
	return &mockParameterizedAggregator{
		name:        m.name,
		addedValues: make([]interface{}, 0),
	}
}

func (m *mockParameterizedAggregator) Add(value interface{}) {
	m.addedValues = append(m.addedValues, value)
}

func (m *mockParameterizedAggregator) Result() interface{} {
	if m.result != nil {
		return m.result
	}
	// 默认返回值的数量
	return len(m.addedValues)
}

func (m *mockParameterizedAggregator) Reset() {
	m.addedValues = make([]interface{}, 0)
	m.result = nil
}

func (m *mockParameterizedAggregator) Clone() AggregatorFunction {
	return &mockParameterizedAggregator{
		name:        m.name,
		args:        m.args,
		initialized: m.initialized,
		addedValues: make([]interface{}, len(m.addedValues)),
		result:      m.result,
	}
}

// 实现ParameterizedFunction接口
func (m *mockParameterizedAggregator) Init(args []interface{}) error {
	m.args = args
	m.initialized = true
	// 根据参数设置结果
	if len(args) > 0 {
		if val, ok := args[0].(int); ok {
			m.result = val * 10 // 简单的计算逻辑
		}
	}
	return nil
}

// mockSimpleAggregator 实现AggregatorFunction接口但不实现ParameterizedFunction
type mockSimpleAggregator struct {
	name   string
	values []interface{}
}

// 实现Function接口
func (m *mockSimpleAggregator) GetName() string {
	return m.name
}

func (m *mockSimpleAggregator) GetMinArgs() int {
	return 1
}

func (m *mockSimpleAggregator) GetMaxArgs() int {
	return 1
}

func (m *mockSimpleAggregator) GetType() FunctionType {
	return TypeAggregation
}

func (m *mockSimpleAggregator) GetCategory() string {
	return "test"
}

func (m *mockSimpleAggregator) GetAliases() []string {
	return []string{}
}

func (m *mockSimpleAggregator) Validate(args []interface{}) error {
	return nil
}

func (m *mockSimpleAggregator) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return len(m.values), nil
}

func (m *mockSimpleAggregator) GetDescription() string {
	return "Mock simple aggregator"
}

// 实现AggregatorFunction接口
func (m *mockSimpleAggregator) New() AggregatorFunction {
	return &mockSimpleAggregator{
		name:   m.name,
		values: make([]interface{}, 0),
	}
}

func (m *mockSimpleAggregator) Add(value interface{}) {
	m.values = append(m.values, value)
}

func (m *mockSimpleAggregator) Result() interface{} {
	return len(m.values)
}

func (m *mockSimpleAggregator) Reset() {
	m.values = make([]interface{}, 0)
}

func (m *mockSimpleAggregator) Clone() AggregatorFunction {
	return &mockSimpleAggregator{
		name:   m.name,
		values: make([]interface{}, len(m.values)),
	}
}

// TestCreateParameterizedAggregator 测试CreateParameterizedAggregator函数的完整功能
func TestCreateParameterizedAggregator(t *testing.T) {
	t.Run("测试参数化聚合器创建和初始化", func(t *testing.T) {
		// 注册测试用的参数化聚合器
		mockParamAgg := &mockParameterizedAggregator{name: "test_param_agg"}
		err := Register(mockParamAgg)
		require.NoError(t, err)

		// 测试创建参数化聚合器
		args := []interface{}{5, "test", 3.14}
		aggregator, err := CreateParameterizedAggregator("test_param_agg", args)
		require.NoError(t, err)
		require.NotNil(t, aggregator)

		// 验证聚合器类型
		paramAgg, ok := aggregator.(*mockParameterizedAggregator)
		require.True(t, ok)
		assert.True(t, paramAgg.initialized)
		assert.Equal(t, args, paramAgg.args)
		assert.Equal(t, 50, paramAgg.result) // 5 * 10 = 50

		// 清理
		Unregister("test_param_agg")
	})

	t.Run("测试非参数化聚合器的回退处理", func(t *testing.T) {
		// 注册测试用的简单聚合器
		mockSimpleAgg := &mockSimpleAggregator{name: "test_simple_agg"}
		err := Register(mockSimpleAgg)
		require.NoError(t, err)

		// 测试创建非参数化聚合器（应该回退到常规创建）
		args := []interface{}{1, 2, 3}
		aggregator, err := CreateParameterizedAggregator("test_simple_agg", args)
		require.NoError(t, err)
		require.NotNil(t, aggregator)

		// 验证聚合器类型
		simpleAgg, ok := aggregator.(*mockSimpleAggregator)
		require.True(t, ok)
		assert.Equal(t, "test_simple_agg", simpleAgg.name)

		// 清理
		Unregister("test_simple_agg")
	})

	t.Run("测试不存在的聚合器函数", func(t *testing.T) {
		// 测试创建不存在的聚合器
		args := []interface{}{1, 2, 3}
		aggregator, err := CreateParameterizedAggregator("non_existent_agg", args)
		assert.Error(t, err)
		assert.Nil(t, aggregator)
		assert.Contains(t, err.Error(), "aggregator function non_existent_agg not found")
	})

	t.Run("测试参数化聚合器初始化失败", func(t *testing.T) {
		// 创建一个会初始化失败的参数化聚合器
		failingAgg := &mockParameterizedAggregatorWithFailingInit{
			mockParameterizedAggregator: mockParameterizedAggregator{name: "failing_agg"},
		}
		err := Register(failingAgg)
		require.NoError(t, err)

		// 测试创建时初始化失败
		args := []interface{}{"invalid"}
		aggregator, err := CreateParameterizedAggregator("failing_agg", args)
		assert.Error(t, err)
		assert.Nil(t, aggregator)
		assert.Contains(t, err.Error(), "failed to initialize parameterized function")

		// 清理
		Unregister("failing_agg")
	})

	t.Run("测试空参数列表", func(t *testing.T) {
		// 注册测试用的参数化聚合器
		mockParamAgg := &mockParameterizedAggregator{name: "test_empty_args"}
		err := Register(mockParamAgg)
		require.NoError(t, err)

		// 测试空参数列表
		args := []interface{}{}
		aggregator, err := CreateParameterizedAggregator("test_empty_args", args)
		require.NoError(t, err)
		require.NotNil(t, aggregator)

		// 验证聚合器已初始化
		paramAgg, ok := aggregator.(*mockParameterizedAggregator)
		require.True(t, ok)
		assert.True(t, paramAgg.initialized)
		assert.Equal(t, args, paramAgg.args)

		// 清理
		Unregister("test_empty_args")
	})

	t.Run("测试聚合器功能", func(t *testing.T) {
		// 注册测试用的参数化聚合器
		mockParamAgg := &mockParameterizedAggregator{name: "test_functionality"}
		err := Register(mockParamAgg)
		require.NoError(t, err)

		// 创建聚合器
		args := []interface{}{3}
		aggregator, err := CreateParameterizedAggregator("test_functionality", args)
		require.NoError(t, err)
		require.NotNil(t, aggregator)

		// 测试聚合器功能
		aggregator.Add(10)
		aggregator.Add(20)
		aggregator.Add(30)

		// 验证结果（应该是初始化时设置的值：3 * 10 = 30）
		result := aggregator.Result()
		assert.Equal(t, 30, result)

		// 清理
		Unregister("test_functionality")
	})
}

// mockParameterizedAggregatorWithFailingInit 用于测试初始化失败的情况
type mockParameterizedAggregatorWithFailingInit struct {
	mockParameterizedAggregator
}

func (m *mockParameterizedAggregatorWithFailingInit) Init(args []interface{}) error {
	return errors.New("initialization failed") // 返回一个错误
}

func (m *mockParameterizedAggregatorWithFailingInit) New() AggregatorFunction {
	return &mockParameterizedAggregatorWithFailingInit{
		mockParameterizedAggregator: mockParameterizedAggregator{
			name:        m.mockParameterizedAggregator.name,
			addedValues: make([]interface{}, 0),
			result:      m.mockParameterizedAggregator.result,
		},
	}
}

// TestCreateAggregatorInterface 测试CreateAggregator函数
func TestCreateAggregatorInterface(t *testing.T) {
	t.Run("测试创建存在的聚合器", func(t *testing.T) {
		// 注册测试聚合器
		mockAgg := &mockSimpleAggregator{name: "test_create_agg"}
		err := Register(mockAgg)
		require.NoError(t, err)

		// 创建聚合器
		aggregator, err := CreateAggregator("test_create_agg")
		require.NoError(t, err)
		require.NotNil(t, aggregator)
		assert.IsType(t, &mockSimpleAggregator{}, aggregator)

		// 清理
		Unregister("test_create_agg")
	})

	t.Run("测试创建不存在的聚合器", func(t *testing.T) {
		aggregator, err := CreateAggregator("non_existent")
		assert.Error(t, err)
		assert.Nil(t, aggregator)
		assert.Contains(t, err.Error(), "aggregator function non_existent not found")
	})
}

// TestIsAggregatorFunction 测试IsAggregatorFunction函数
func TestIsAggregatorFunction(t *testing.T) {
	t.Run("测试已注册的聚合器函数", func(t *testing.T) {
		// 注册测试聚合器
		mockAgg := &mockSimpleAggregator{name: "test_is_agg"}
		err := Register(mockAgg)
		require.NoError(t, err)

		// 测试IsAggregatorFunction
		isAgg := IsAggregatorFunction("test_is_agg")
		assert.True(t, isAgg)

		// 清理
		Unregister("test_is_agg")
	})

	t.Run("测试不存在的函数", func(t *testing.T) {
		isAgg := IsAggregatorFunction("non_existent_function")
		assert.False(t, isAgg)
	})
}