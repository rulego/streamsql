package functions

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockParameterizedAggregator implements the ParameterizedFunction interface for testing
type mockParameterizedAggregator struct {
	name        string
	args        []any
	initialized bool
	addedValues []any
	result      any
}

// Implement the Function interface
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

func (m *mockParameterizedAggregator) Validate(args []any) error {
	return nil
}

func (m *mockParameterizedAggregator) Execute(ctx *FunctionContext, args []any) (any, error) {
	return m.result, nil
}

func (m *mockParameterizedAggregator) GetDescription() string {
	return "Mock parameterized aggregator"
}

// Implement the AggregatorFunction interface
func (m *mockParameterizedAggregator) New() AggregatorFunction {
	return &mockParameterizedAggregator{
		name:        m.name,
		addedValues: make([]any, 0),
	}
}

func (m *mockParameterizedAggregator) Add(value any) {
	m.addedValues = append(m.addedValues, value)
}

func (m *mockParameterizedAggregator) Result() any {
	if m.result != nil {
		return m.result
	}
	// The default number of return values
	return len(m.addedValues)
}

func (m *mockParameterizedAggregator) Reset() {
	m.addedValues = make([]any, 0)
	m.result = nil
}

func (m *mockParameterizedAggregator) Clone() AggregatorFunction {
	return &mockParameterizedAggregator{
		name:        m.name,
		args:        m.args,
		initialized: m.initialized,
		addedValues: make([]any, len(m.addedValues)),
		result:      m.result,
	}
}

// Implement the ParameterizedFunction interface
func (m *mockParameterizedAggregator) Init(args []any) error {
	m.args = args
	m.initialized = true
	// Set the results according to the parameters
	if len(args) > 0 {
		if val, ok := args[0].(int); ok {
			m.result = val * 10 // Simple calculation logic
		}
	}
	return nil
}

// mockSimpleAggregator implements the AggregatorFunction interface but does not implement ParameterizedFunction
type mockSimpleAggregator struct {
	name   string
	values []any
}

// Implement the Function interface
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

func (m *mockSimpleAggregator) Validate(args []any) error {
	return nil
}

func (m *mockSimpleAggregator) Execute(ctx *FunctionContext, args []any) (any, error) {
	return len(m.values), nil
}

func (m *mockSimpleAggregator) GetDescription() string {
	return "Mock simple aggregator"
}

// Implement the AggregatorFunction interface
func (m *mockSimpleAggregator) New() AggregatorFunction {
	return &mockSimpleAggregator{
		name:   m.name,
		values: make([]any, 0),
	}
}

func (m *mockSimpleAggregator) Add(value any) {
	m.values = append(m.values, value)
}

func (m *mockSimpleAggregator) Result() any {
	return len(m.values)
}

func (m *mockSimpleAggregator) Reset() {
	m.values = make([]any, 0)
}

func (m *mockSimpleAggregator) Clone() AggregatorFunction {
	return &mockSimpleAggregator{
		name:   m.name,
		values: make([]any, len(m.values)),
	}
}

// TestCreateParameterizedAggregator Test the full functionality of the CreateParameterizedAggregator function
func TestCreateParameterizedAggregator(t *testing.T) {
	t.Run("测试参数化聚合器创建和初始化", func(t *testing.T) {
		// Parametric aggregators for registration testing
		mockParamAgg := &mockParameterizedAggregator{name: "test_param_agg"}
		err := Register(mockParamAgg)
		require.NoError(t, err)

		// Test the creation of parametric aggregators
		args := []any{5, "test", 3.14}
		aggregator, err := CreateParameterizedAggregator("test_param_agg", args)
		require.NoError(t, err)
		require.NotNil(t, aggregator)

		// Verify aggregator types
		paramAgg, ok := aggregator.(*mockParameterizedAggregator)
		require.True(t, ok)
		assert.True(t, paramAgg.initialized)
		assert.Equal(t, args, paramAgg.args)
		assert.Equal(t, 50, paramAgg.result) // 5 * 10 = 50

		// Cleanup
		Unregister("test_param_agg")
	})

	t.Run("测试非参数化聚合器的回退处理", func(t *testing.T) {
		// A simple aggregator for registration testing
		mockSimpleAgg := &mockSimpleAggregator{name: "test_simple_agg"}
		err := Register(mockSimpleAgg)
		require.NoError(t, err)

		// Test creating a non-parametric aggregator (should rollback to regular creation)
		args := []any{1, 2, 3}
		aggregator, err := CreateParameterizedAggregator("test_simple_agg", args)
		require.NoError(t, err)
		require.NotNil(t, aggregator)

		// Verify aggregator types
		simpleAgg, ok := aggregator.(*mockSimpleAggregator)
		require.True(t, ok)
		assert.Equal(t, "test_simple_agg", simpleAgg.name)

		// Cleanup
		Unregister("test_simple_agg")
	})

	t.Run("测试不存在的聚合器函数", func(t *testing.T) {
		// Test to create aggregators that don't exist
		args := []any{1, 2, 3}
		aggregator, err := CreateParameterizedAggregator("non_existent_agg", args)
		assert.Error(t, err)
		assert.Nil(t, aggregator)
		assert.Contains(t, err.Error(), "aggregator function non_existent_agg not found")
	})

	t.Run("测试参数化聚合器初始化失败", func(t *testing.T) {
		// Create a parameterized aggregator that will initialize failure
		failingAgg := &mockParameterizedAggregatorWithFailingInit{
			mockParameterizedAggregator: mockParameterizedAggregator{name: "failing_agg"},
		}
		err := Register(failingAgg)
		require.NoError(t, err)

		// Initialization fails when the test is created
		args := []any{"invalid"}
		aggregator, err := CreateParameterizedAggregator("failing_agg", args)
		assert.Error(t, err)
		assert.Nil(t, aggregator)
		assert.Contains(t, err.Error(), "failed to initialize parameterized function")

		// Cleanup
		Unregister("failing_agg")
	})

	t.Run("测试空参数列表", func(t *testing.T) {
		// Parametric aggregators for registration testing
		mockParamAgg := &mockParameterizedAggregator{name: "test_empty_args"}
		err := Register(mockParamAgg)
		require.NoError(t, err)

		// Test null parameter list
		args := []any{}
		aggregator, err := CreateParameterizedAggregator("test_empty_args", args)
		require.NoError(t, err)
		require.NotNil(t, aggregator)

		// Verify that the aggregator has been initialized
		paramAgg, ok := aggregator.(*mockParameterizedAggregator)
		require.True(t, ok)
		assert.True(t, paramAgg.initialized)
		assert.Equal(t, args, paramAgg.args)

		// Cleanup
		Unregister("test_empty_args")
	})

	t.Run("测试聚合器功能", func(t *testing.T) {
		// Parametric aggregators for registration testing
		mockParamAgg := &mockParameterizedAggregator{name: "test_functionality"}
		err := Register(mockParamAgg)
		require.NoError(t, err)

		// Create aggregators
		args := []any{3}
		aggregator, err := CreateParameterizedAggregator("test_functionality", args)
		require.NoError(t, err)
		require.NotNil(t, aggregator)

		// Test the aggregator function
		aggregator.Add(10)
		aggregator.Add(20)
		aggregator.Add(30)

		// Verification result (should be the value set at initialization: 3 * 10 = 30)
		result := aggregator.Result()
		assert.Equal(t, 30, result)

		// Cleanup
		Unregister("test_functionality")
	})
}

// mockParameterizedAggregatorWithFailingInit is used to test for initialization failures
type mockParameterizedAggregatorWithFailingInit struct {
	mockParameterizedAggregator
}

func (m *mockParameterizedAggregatorWithFailingInit) Init(args []any) error {
	return errors.New("initialization failed") // Returns an error
}

func (m *mockParameterizedAggregatorWithFailingInit) New() AggregatorFunction {
	return &mockParameterizedAggregatorWithFailingInit{
		mockParameterizedAggregator: mockParameterizedAggregator{
			name:        m.mockParameterizedAggregator.name,
			addedValues: make([]any, 0),
			result:      m.mockParameterizedAggregator.result,
		},
	}
}

// TestCreateAggregatorInterface tests the CreateAggregator function
func TestCreateAggregatorInterface(t *testing.T) {
	t.Run("测试创建存在的聚合器", func(t *testing.T) {
		// Register for the test aggregator
		mockAgg := &mockSimpleAggregator{name: "test_create_agg"}
		err := Register(mockAgg)
		require.NoError(t, err)

		// Create aggregators
		aggregator, err := CreateAggregator("test_create_agg")
		require.NoError(t, err)
		require.NotNil(t, aggregator)
		assert.IsType(t, &mockSimpleAggregator{}, aggregator)

		// Cleanup
		Unregister("test_create_agg")
	})

	t.Run("测试创建不存在的聚合器", func(t *testing.T) {
		aggregator, err := CreateAggregator("non_existent")
		assert.Error(t, err)
		assert.Nil(t, aggregator)
		assert.Contains(t, err.Error(), "aggregator function non_existent not found")
	})
}

// TestIsAggregatorFunction Test the IsAggregatorFunction function
func TestIsAggregatorFunction(t *testing.T) {
	t.Run("测试已注册的聚合器函数", func(t *testing.T) {
		// Register for the test aggregator
		mockAgg := &mockSimpleAggregator{name: "test_is_agg"}
		err := Register(mockAgg)
		require.NoError(t, err)

		// Test IsAggregatorFunction
		isAgg := IsAggregatorFunction("test_is_agg")
		assert.True(t, isAgg)

		// Cleanup
		Unregister("test_is_agg")
	})

	t.Run("测试不存在的函数", func(t *testing.T) {
		isAgg := IsAggregatorFunction("non_existent_function")
		assert.False(t, isAgg)
	})
}
