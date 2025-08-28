package functions

import (
	"testing"
)

// TestNewAnalyticalAggregatorAdapter 测试创建分析聚合器适配器
func TestNewAnalyticalAggregatorAdapter(t *testing.T) {
	tests := []struct {
		name        string
		funcName    string
		expectError bool
	}{
		{
			name:        "valid analytical function",
			funcName:    "lag",
			expectError: false,
		},
		{
			name:        "invalid function name",
			funcName:    "nonexistent_function",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, err := NewAnalyticalAggregatorAdapter(tt.funcName)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				if adapter != nil {
					t.Error("Expected nil adapter but got one")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if adapter == nil {
					t.Error("Expected adapter but got nil")
				}
				if adapter != nil {
					if adapter.analFunc == nil {
						t.Error("Expected analytical function but got nil")
					}
					if adapter.ctx == nil {
						t.Error("Expected context but got nil")
					}
				}
			}
		})
	}
}

// TestAnalyticalAggregatorAdapterNew 测试New方法
func TestAnalyticalAggregatorAdapterNew(t *testing.T) {
	// 创建一个测试适配器
	adapter, err := NewAnalyticalAggregatorAdapter("lag")
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	// 测试New方法
	newAdapter := adapter.New()
	if newAdapter == nil {
		t.Error("New() should return a new adapter")
	}

	// 验证返回的是正确的类型
	if _, ok := newAdapter.(*AnalyticalAggregatorAdapter); !ok {
		t.Error("New() should return AnalyticalAggregatorAdapter")
	}

	// 验证新适配器有独立的上下文
	newAdapterTyped := newAdapter.(*AnalyticalAggregatorAdapter)
	if newAdapterTyped.ctx == adapter.ctx {
		t.Error("New adapter should have independent context")
	}
}

// TestAnalyticalAggregatorAdapterAdd 测试Add方法
func TestAnalyticalAggregatorAdapterAdd(t *testing.T) {
	adapter, err := NewAnalyticalAggregatorAdapter("lag")
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	// 测试Add方法
	adapter.Add(10)
	adapter.Add(20)
	adapter.Add(30)

	// 验证没有panic
	t.Log("Add method executed successfully")
}

// TestAnalyticalAggregatorAdapterResult 测试Result方法
func TestAnalyticalAggregatorAdapterResult(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
	}{
		{"lag function", "lag"},
		{"latest function", "latest"},
		{"had_changed function", "had_changed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, err := NewAnalyticalAggregatorAdapter(tt.funcName)
			if err != nil {
				t.Skipf("Function %s not available: %v", tt.funcName, err)
				return
			}

			// 添加一些数据
			adapter.Add(10)
			adapter.Add(20)

			// 获取结果
			result := adapter.Result()
			t.Logf("Result for %s: %v", tt.funcName, result)
		})
	}
}

// TestCreateAnalyticalAggregatorFromFunctions 测试从函数模块创建分析聚合器
func TestCreateAnalyticalAggregatorFromFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcType string
		expected bool // 是否期望创建成功
	}{
		{
			name:     "valid analytical function",
			funcType: "lag",
			expected: true,
		},
		{
			name:     "another valid function",
			funcType: "latest",
			expected: true,
		},
		{
			name:     "invalid function",
			funcType: "nonexistent",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateAnalyticalAggregatorFromFunctions(tt.funcType)
			if tt.expected {
				if result == nil {
					t.Errorf("Expected to create aggregator for %s but got nil", tt.funcType)
				} else {
					// 验证返回的是正确的类型
					if _, ok := result.(*AnalyticalAggregatorAdapter); !ok {
						t.Errorf("Expected AnalyticalAggregatorAdapter but got %T", result)
					}
				}
			} else {
				if result != nil {
					t.Errorf("Expected nil for %s but got %v", tt.funcType, result)
				}
			}
		})
	}
}

// TestAnalyticalAggregatorAdapterWithMockFunction 测试使用模拟函数的适配器
func TestAnalyticalAggregatorAdapterWithMockFunction(t *testing.T) {
	// 创建模拟分析函数
	mockFunc := &MockAnalyticalFunction{
		name:   "mock_analytical",
		values: []interface{}{},
	}

	// 创建适配器
	adapter := &AnalyticalAggregatorAdapter{
		analFunc: mockFunc,
		ctx: &FunctionContext{
			Data: make(map[string]interface{}),
		},
	}

	// 测试Add方法
	adapter.Add("test1")
	adapter.Add("test2")

	// 验证值被添加
	if len(mockFunc.values) != 2 {
		t.Errorf("Expected 2 values, got %d", len(mockFunc.values))
	}

	// 测试Result方法
	result := adapter.Result()
	if result != "mock_result" {
		t.Errorf("Expected 'mock_result', got %v", result)
	}

	// 测试New方法
	newAdapter := adapter.New()
	if newAdapter == nil {
		t.Error("New() should return a new adapter")
	}
}

// MockAnalyticalFunction 模拟分析函数用于测试
type MockAnalyticalFunction struct {
	name   string
	values []interface{}
}

// GetName 返回函数名称
func (m *MockAnalyticalFunction) GetName() string {
	return m.name
}

// GetType 返回函数类型
func (m *MockAnalyticalFunction) GetType() FunctionType {
	return TypeAnalytical
}

// GetCategory 返回函数分类
func (m *MockAnalyticalFunction) GetCategory() string {
	return "mock"
}

// GetDescription 返回函数描述
func (m *MockAnalyticalFunction) GetDescription() string {
	return "Mock analytical function for testing"
}

// GetAliases 返回函数别名
func (m *MockAnalyticalFunction) GetAliases() []string {
	return []string{}
}

// GetMinArgs 返回最小参数数量
func (m *MockAnalyticalFunction) GetMinArgs() int {
	return 1
}

// GetMaxArgs 返回最大参数数量
func (m *MockAnalyticalFunction) GetMaxArgs() int {
	return 1
}

// Validate 验证参数
func (m *MockAnalyticalFunction) Validate(args []interface{}) error {
	return nil
}

// Execute 执行函数
func (m *MockAnalyticalFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if len(args) > 0 {
		m.values = append(m.values, args[0])
	}
	return "mock_result", nil
}

// Add 添加值到聚合器
func (m *MockAnalyticalFunction) Add(value interface{}) {
	m.values = append(m.values, value)
}

// Result 返回聚合结果
func (m *MockAnalyticalFunction) Result() interface{} {
	return len(m.values)
}

// Reset 重置聚合器
func (m *MockAnalyticalFunction) Reset() {
	m.values = make([]interface{}, 0)
}

// New 创建新的聚合器实例
func (m *MockAnalyticalFunction) New() AggregatorFunction {
	newMock := &MockAnalyticalFunction{
		name:   m.name,
		values: make([]interface{}, 0),
	}
	return newMock
}

// Clone 克隆函数 - 返回AggregatorFunction以满足接口要求
func (m *MockAnalyticalFunction) Clone() AggregatorFunction {
	newMock := &MockAnalyticalFunction{
		name:   m.name,
		values: make([]interface{}, len(m.values)),
	}
	copy(newMock.values, m.values)
	return newMock
}
