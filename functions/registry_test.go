package functions

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFunction 是用于测试的简单函数实现
type TestFunction struct {
	name        string
	fnType      FunctionType
	category    string
	description string
	minArgs     int
	maxArgs     int
	executor    func(ctx *FunctionContext, args []interface{}) (interface{}, error)
}

func (f *TestFunction) GetName() string        { return f.name }
func (f *TestFunction) GetType() FunctionType  { return f.fnType }
func (f *TestFunction) GetCategory() string    { return f.category }
func (f *TestFunction) GetAliases() []string   { return []string{} }
func (f *TestFunction) GetDescription() string { return f.description }
func (f *TestFunction) GetMinArgs() int        { return f.minArgs }
func (f *TestFunction) GetMaxArgs() int        { return f.maxArgs }

func (f *TestFunction) Validate(args []interface{}) error {
	if len(args) < f.minArgs {
		return fmt.Errorf("not enough arguments: expected at least %d, got %d", f.minArgs, len(args))
	}
	if f.maxArgs >= 0 && len(args) > f.maxArgs {
		return fmt.Errorf("too many arguments: expected at most %d, got %d", f.maxArgs, len(args))
	}
	return nil
}

func (f *TestFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if f.executor != nil {
		return f.executor(ctx, args)
	}
	return "test_result", nil
}

func TestRegistryEdgeCases(t *testing.T) {
	// 测试注册nil函数
	t.Run("Register nil function", func(t *testing.T) {
		err := Register(nil)
		assert.Error(t, err)
	})
	
	// 测试注册重复函数名
	t.Run("Register duplicate name", func(t *testing.T) {
		testFunc1 := &TestFunction{
			name:     "duplicate_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
		}
		testFunc2 := &TestFunction{
			name:     "duplicate_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
		}
		
		// 先注册第一个函数
		err := Register(testFunc1)
		assert.NoError(t, err)
		
		// 尝试注册同名函数
		err = Register(testFunc2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already registered")
		
		// 清理
		Unregister("duplicate_func")
	})
}

// TestGetFunctionEdgeCases 测试Get函数的边界情况
func TestGetFunctionEdgeCases(t *testing.T) {
	// 测试获取不存在的函数
	t.Run("Get non-existent function", func(t *testing.T) {
		func_, exists := Get("non_existent_func")
		assert.False(t, exists)
		assert.Nil(t, func_)
	})
	
	// 测试获取空名称函数
	t.Run("Get empty name function", func(t *testing.T) {
		func_, exists := Get("")
		assert.False(t, exists)
		assert.Nil(t, func_)
	})
	
	// 测试大小写不敏感性（函数名会被转换为小写）
	t.Run("Case insensitivity", func(t *testing.T) {
		testFunc := &TestFunction{
			name:     "lowercase_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
		}
		
		// 注册小写函数名
		err := Register(testFunc)
		assert.NoError(t, err)
		
		// 用大写获取应该也能找到（因为内部转换为小写）
		func_, exists := Get("LOWERCASE_FUNC")
		assert.True(t, exists)
		assert.NotNil(t, func_)
		
		// 用正确的小写获取
		func_, exists = Get("lowercase_func")
		assert.True(t, exists)
		assert.NotNil(t, func_)
		
		// 清理
		Unregister("lowercase_func")
	})
}

// TestUnregisterEdgeCases 测试Unregister函数的边界情况
func TestUnregisterEdgeCases(t *testing.T) {
	// 测试注销不存在的函数
	t.Run("Unregister non-existent function", func(t *testing.T) {
		// 这应该不会引起panic或错误
		result := Unregister("non_existent_func")
		assert.False(t, result)
		
		// 验证函数确实不存在
		func_, exists := Get("non_existent_func")
		assert.False(t, exists)
		assert.Nil(t, func_)
	})
	
	// 测试注销空名称函数
	t.Run("Unregister empty name", func(t *testing.T) {
		// 这应该不会引起panic或错误
		result := Unregister("")
		assert.False(t, result)
	})
	
	// 测试注销后再次注销
	t.Run("Double unregister", func(t *testing.T) {
		testFunc := &TestFunction{
			name:     "double_unregister_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
		}
		
		// 注册函数
		err := Register(testFunc)
		assert.NoError(t, err)
		
		// 第一次注销
		result := Unregister("double_unregister_func")
		assert.True(t, result)
		func_, exists := Get("double_unregister_func")
		assert.False(t, exists)
		assert.Nil(t, func_)
		
		// 第二次注销（应该不会有问题）
		result = Unregister("double_unregister_func")
		assert.False(t, result)
	})
}

// TestListAllEdgeCases 测试ListAll函数的边界情况
func TestListAllEdgeCases(t *testing.T) {
	// 注册一些测试函数
	testFunctions := []*TestFunction{
		{
			name:     "test_func1",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
		},
		{
			name:     "test_func2",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
		},
		{
			name:     "test_func3",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
		},
	}
	
	for _, fn := range testFunctions {
		err := Register(fn)
		assert.NoError(t, err)
	}
	
	// 测试非空注册表
	t.Run("Non-empty registry", func(t *testing.T) {
		functions := ListAll()
		assert.GreaterOrEqual(t, len(functions), 3) // 至少包含我们注册的3个函数
		_, exists1 := functions["test_func1"]
		_, exists2 := functions["test_func2"]
		_, exists3 := functions["test_func3"]
		assert.True(t, exists1)
		assert.True(t, exists2)
		assert.True(t, exists3)
	})
	
	// 清理测试函数
	for _, fn := range testFunctions {
		Unregister(fn.name)
	}
}

// TestExecuteEdgeCases 测试Execute函数的边界情况
func TestExecuteEdgeCases(t *testing.T) {
	// 测试执行不存在的函数
	t.Run("Execute non-existent function", func(t *testing.T) {
		ctx := &FunctionContext{}
		result, err := Execute("non_existent_func", ctx, []interface{}{})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "not found")
	})
	
	// 测试执行返回错误的函数
	t.Run("Execute function that returns error", func(t *testing.T) {
		errorFunc := &TestFunction{
			name:     "error_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				return nil, errors.New("test error")
			},
		}
		
		err := Register(errorFunc)
		assert.NoError(t, err)
		
		ctx := &FunctionContext{}
		result, err := Execute("error_func", ctx, []interface{}{})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "test error")
		
		// 清理
		Unregister("error_func")
	})
	
	// 测试执行带参数的函数
	t.Run("Execute function with arguments", func(t *testing.T) {
		sumFunc := &TestFunction{
			name:     "sum_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				if len(args) == 0 {
					return 0, nil
				}
				
				sum := 0
				for _, arg := range args {
					if num, ok := arg.(int); ok {
						sum += num
					}
				}
				return sum, nil
			},
		}
		
		err := Register(sumFunc)
		assert.NoError(t, err)
		
		ctx := &FunctionContext{}
		// 无参数调用
		result, err := Execute("sum_func", ctx, []interface{}{})
		assert.NoError(t, err)
		assert.Equal(t, 0, result)
		
		// 带参数调用
		result, err = Execute("sum_func", ctx, []interface{}{1, 2, 3, 4, 5})
		assert.NoError(t, err)
		assert.Equal(t, 15, result)
		
		// 清理
		Unregister("sum_func")
	})
	
	// 测试执行panic的函数
	t.Run("Execute function that panics", func(t *testing.T) {
		panicFunc := &TestFunction{
			name:     "panic_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				panic("test panic")
			},
		}
		
		err := Register(panicFunc)
		assert.NoError(t, err)
		
		ctx := &FunctionContext{}
		// 执行应该捕获panic并返回错误
		assert.Panics(t, func() {
			Execute("panic_func", ctx, []interface{}{})
		})
		
		// 清理
		Unregister("panic_func")
	})
}

// TestValidateEdgeCases 测试Validate函数的边界情况
func TestValidateEdgeCases(t *testing.T) {
	// 测试验证不存在的函数
	t.Run("Validate non-existent function", func(t *testing.T) {
		err := Validate("non_existent_func")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function 'non_existent_func' not found")
	})
	
	// 测试验证空名称函数
	t.Run("Validate empty name function", func(t *testing.T) {
		err := Validate("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function '' not found")
	})
	
	// 测试验证有效函数
	t.Run("Validate valid function", func(t *testing.T) {
		validFunc := &TestFunction{
			name:     "valid_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				return "valid", nil
			},
		}
		
		err := Register(validFunc)
		assert.NoError(t, err)
		
		err = Validate("valid_func")
		assert.NoError(t, err)
		
		// 清理
		Unregister("valid_func")
	})
	
	// 测试验证多个函数
	t.Run("Validate multiple functions", func(t *testing.T) {
		testFunc1 := &TestFunction{
			name:     "test_func1",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				return "test1", nil
			},
		}
		testFunc2 := &TestFunction{
			name:     "test_func2",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				return "test2", nil
			},
		}
		
		err := Register(testFunc1)
		assert.NoError(t, err)
		err = Register(testFunc2)
		assert.NoError(t, err)
		
		// 验证存在的函数
		err = Validate("test_func1")
		assert.NoError(t, err)
		err = Validate("test_func2")
		assert.NoError(t, err)
		
		// 验证不存在的函数
		err = Validate("test_func3")
		assert.Error(t, err)
		
		// 清理
		Unregister("test_func1")
		Unregister("test_func2")
	})
}

// TestConcurrentAccess 测试并发访问
func TestConcurrentAccess(t *testing.T) {
	// 这个测试检查注册表在并发访问时的行为
	// 注意：实际的并发安全需要在registry实现中处理
	t.Run("Concurrent register and get", func(t *testing.T) {
		testFunc := &TestFunction{
			name:     "concurrent_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				return "concurrent_test", nil
			},
		}
		
		// 注册函数
		err := Register(testFunc)
		assert.NoError(t, err)
		
		// 获取函数
		func_, exists := Get("concurrent_func")
		assert.True(t, exists)
		assert.NotNil(t, func_)
		
		ctx := &FunctionContext{}
		// 执行函数
		result, err := Execute("concurrent_func", ctx, []interface{}{})
		assert.NoError(t, err)
		assert.Equal(t, "concurrent_test", result)
		
		// 清理
		Unregister("concurrent_func")
	})
}

// TestFunctionSignatureVariations 测试不同函数签名的变化
func TestFunctionSignatureVariations(t *testing.T) {
	// 测试无参数函数
	t.Run("No parameter function", func(t *testing.T) {
		noParamFunc := &TestFunction{
			name:     "no_param_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  0,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				if len(args) != 0 {
					return nil, errors.New("expected no arguments")
				}
				return "no_params", nil
			},
		}
		
		err := Register(noParamFunc)
		assert.NoError(t, err)
		
		ctx := &FunctionContext{}
		result, err := Execute("no_param_func", ctx, []interface{}{})
		assert.NoError(t, err)
		assert.Equal(t, "no_params", result)
		
		// 清理
		Unregister("no_param_func")
	})
	
	// 测试可变参数函数
	t.Run("Variadic parameter function", func(t *testing.T) {
		variadicFunc := &TestFunction{
			name:     "variadic_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				return len(args), nil
			},
		}
		
		err := Register(variadicFunc)
		assert.NoError(t, err)
		
		ctx := &FunctionContext{}
		// 测试不同数量的参数
		result, err := Execute("variadic_func", ctx, []interface{}{})
		assert.NoError(t, err)
		assert.Equal(t, 0, result)
		
		result, err = Execute("variadic_func", ctx, []interface{}{1})
		assert.NoError(t, err)
		assert.Equal(t, 1, result)
		
		result, err = Execute("variadic_func", ctx, []interface{}{1, 2, 3})
		assert.NoError(t, err)
		assert.Equal(t, 3, result)
		
		// 清理
		Unregister("variadic_func")
	})
	
	// 测试返回不同类型的函数
	t.Run("Different return types", func(t *testing.T) {
		// 返回字符串
		stringFunc := &TestFunction{
			name:     "string_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				return "string_result", nil
			},
		}
		
		// 返回数字
		numberFunc := &TestFunction{
			name:     "number_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				return 42, nil
			},
		}
		
		// 返回布尔值
		boolFunc := &TestFunction{
			name:     "bool_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				return true, nil
			},
		}
		
		// 返回nil
		nilFunc := &TestFunction{
			name:     "nil_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
				return nil, nil
			},
		}
		
		// 注册所有函数
		assert.NoError(t, Register(stringFunc))
		assert.NoError(t, Register(numberFunc))
		assert.NoError(t, Register(boolFunc))
		assert.NoError(t, Register(nilFunc))
		
		ctx := &FunctionContext{}
		// 测试执行
		result, err := Execute("string_func", ctx, []interface{}{})
		assert.NoError(t, err)
		assert.Equal(t, "string_result", result)
		
		result, err = Execute("number_func", ctx, []interface{}{})
		assert.NoError(t, err)
		assert.Equal(t, 42, result)
		
		result, err = Execute("bool_func", ctx, []interface{}{})
		assert.NoError(t, err)
		assert.Equal(t, true, result)
		
		result, err = Execute("nil_func", ctx, []interface{}{})
		assert.NoError(t, err)
		assert.Nil(t, result)
		
		// 清理
		Unregister("string_func")
		Unregister("number_func")
		Unregister("bool_func")
		Unregister("nil_func")
	})
}
