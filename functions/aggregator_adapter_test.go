package functions

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAggregatorAdapterBasic 测试聚合器适配器基本功能
func TestAggregatorAdapterBasic(t *testing.T) {
	// 测试创建聚合器适配器
	adapter, err := NewAggregatorAdapter("sum")
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// 测试GetFunctionName
	funcName := adapter.GetFunctionName()
	assert.Equal(t, "sum", funcName)

	// 测试New方法
	newAdapter := adapter.New()
	assert.NotNil(t, newAdapter)

	// 测试Add和Result方法
	adapter.Add(10)
	adapter.Add(20)
	adapter.Add(30)
	result := adapter.Result()
	assert.Equal(t, 60.0, result)

	// 测试创建不存在的聚合器
	_, err = NewAggregatorAdapter("nonexistent")
	assert.Error(t, err)
}

// TestAggregatorAdapterWithNilFunction 测试聚合器适配器的nil函数情况
func TestAggregatorAdapterWithNilFunction(t *testing.T) {
	adapter := &AggregatorAdapter{aggFunc: nil}
	funcName := adapter.GetFunctionName()
	assert.Equal(t, "", funcName)
}

// TestAnalyticalAdapterBasic 测试分析函数适配器基本功能
func TestAnalyticalAdapterBasic(t *testing.T) {
	// 测试创建分析函数适配器
	adapter, err := NewAnalyticalAdapter("latest")
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// 测试Execute方法
	ctx := &FunctionContext{
		Data: map[string]interface{}{"test": "value"},
	}
	result, err := adapter.Execute(ctx, []interface{}{"test_value"})
	assert.NoError(t, err)
	assert.Equal(t, "test_value", result)

	// 测试Reset方法
	adapter.Reset()

	// 测试Clone方法
	clonedAdapter := adapter.Clone()
	assert.NotNil(t, clonedAdapter)
	assert.NotSame(t, adapter, clonedAdapter)

	// 测试创建不存在的分析函数
	_, err = NewAnalyticalAdapter("nonexistent")
	assert.Error(t, err)
}

// TestRegisterAggregatorAdapter 测试注册聚合器适配器
func TestRegisterAggregatorAdapter(t *testing.T) {
	// 注册聚合器适配器
	err := RegisterAggregatorAdapter("sum")
	assert.NoError(t, err)

	// 获取聚合器适配器
	constructor, exists := GetAggregatorAdapter("sum")
	assert.True(t, exists)
	assert.NotNil(t, constructor)

	// 使用构造函数创建实例
	instance := constructor()
	assert.NotNil(t, instance)

	// 测试不存在的适配器
	_, exists = GetAggregatorAdapter("nonexistent")
	assert.False(t, exists)
}

// TestRegisterAnalyticalAdapter 测试注册分析函数适配器
func TestRegisterAnalyticalAdapter(t *testing.T) {
	// 注册分析函数适配器
	err := RegisterAnalyticalAdapter("latest")
	assert.NoError(t, err)

	// 获取分析函数适配器
	constructor, exists := GetAnalyticalAdapter("latest")
	assert.True(t, exists)
	assert.NotNil(t, constructor)

	// 使用构造函数创建实例
	instance := constructor()
	assert.NotNil(t, instance)

	// 测试不存在的适配器
	_, exists = GetAnalyticalAdapter("nonexistent")
	assert.False(t, exists)
}

// TestCreateBuiltinAggregatorFromFunctions 测试从函数模块创建内置聚合器
func TestCreateBuiltinAggregatorFromFunctions(t *testing.T) {
	// 先注册一个适配器
	err := RegisterAggregatorAdapter("sum")
	assert.NoError(t, err)

	// 从注册的适配器创建
	aggregator := CreateBuiltinAggregatorFromFunctions("sum")
	assert.NotNil(t, aggregator)

	// 从未注册的函数直接创建
	aggregator2 := CreateBuiltinAggregatorFromFunctions("avg")
	assert.NotNil(t, aggregator2)

	// 测试不存在的聚合器
	aggregator3 := CreateBuiltinAggregatorFromFunctions("nonexistent")
	assert.Nil(t, aggregator3)
}

// TestCreateAnalyticalFromFunctions 测试从函数模块创建分析函数
func TestCreateAnalyticalFromFunctions(t *testing.T) {
	// 先注册一个适配器
	err := RegisterAnalyticalAdapter("latest")
	assert.NoError(t, err)

	// 从注册的适配器创建
	analytical := CreateAnalyticalFromFunctions("latest")
	assert.NotNil(t, analytical)

	// 从未注册的函数直接创建
	analytical2 := CreateAnalyticalFromFunctions("had_changed")
	assert.NotNil(t, analytical2)

	// 测试不存在的分析函数
	analytical3 := CreateAnalyticalFromFunctions("nonexistent")
	assert.Nil(t, analytical3)
}

// TestAggregatorAdapterErrorHandling 测试聚合器适配器错误处理
func TestAggregatorAdapterErrorHandling(t *testing.T) {
	// 注册一个会失败的适配器（使用不存在的函数名）
	err := RegisterAggregatorAdapter("invalid_function")
	assert.NoError(t, err)

	// 获取并尝试创建实例
	constructor, exists := GetAggregatorAdapter("invalid_function")
	assert.True(t, exists)

	// 创建实例应该返回nil（因为函数不存在）
	instance := constructor()
	assert.Nil(t, instance)
}

// TestAnalyticalAdapterErrorHandling 测试分析函数适配器错误处理
func TestAnalyticalAdapterErrorHandling(t *testing.T) {
	// 注册一个会失败的适配器（使用不存在的函数名）
	err := RegisterAnalyticalAdapter("invalid_analytical")
	assert.NoError(t, err)

	// 获取并尝试创建实例
	constructor, exists := GetAnalyticalAdapter("invalid_analytical")
	assert.True(t, exists)

	// 创建实例应该返回nil（因为函数不存在）
	instance := constructor()
	assert.Nil(t, instance)
}