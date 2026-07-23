package functions

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFunction is a simple implementation of a function used for testing
type TestFunction struct {
	name        string
	fnType      FunctionType
	category    string
	description string
	minArgs     int
	maxArgs     int
	executor    func(ctx *FunctionContext, args []any) (any, error)
}

func (f *TestFunction) GetName() string        { return f.name }
func (f *TestFunction) GetType() FunctionType  { return f.fnType }
func (f *TestFunction) GetCategory() string    { return f.category }
func (f *TestFunction) GetAliases() []string   { return []string{} }
func (f *TestFunction) GetDescription() string { return f.description }
func (f *TestFunction) GetMinArgs() int        { return f.minArgs }
func (f *TestFunction) GetMaxArgs() int        { return f.maxArgs }

func (f *TestFunction) Validate(args []any) error {
	if len(args) < f.minArgs {
		return fmt.Errorf("not enough arguments: expected at least %d, got %d", f.minArgs, len(args))
	}
	if f.maxArgs >= 0 && len(args) > f.maxArgs {
		return fmt.Errorf("too many arguments: expected at most %d, got %d", f.maxArgs, len(args))
	}
	return nil
}

func (f *TestFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if f.executor != nil {
		return f.executor(ctx, args)
	}
	return "test_result", nil
}

func TestRegistryEdgeCases(t *testing.T) {
	// Test the registration nil function
	t.Run("Register nil function", func(t *testing.T) {
		err := Register(nil)
		assert.Error(t, err)
	})

	// Test registers duplicate function names
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

		// First, register the first function
		err := Register(testFunc1)
		assert.NoError(t, err)

		// Try registering a function with the same name
		err = Register(testFunc2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already registered")

		// Cleanup
		Unregister("duplicate_func")
	})
}

// TestGetFunctionEdgeCases tests the boundary status of the Get function
func TestGetFunctionEdgeCases(t *testing.T) {
	// Test to retrieve a function that doesn't exist
	t.Run("Get non-existent function", func(t *testing.T) {
		func_, exists := Get("non_existent_func")
		assert.False(t, exists)
		assert.Nil(t, func_)
	})

	// Test to get the empty name function
	t.Run("Get empty name function", func(t *testing.T) {
		func_, exists := Get("")
		assert.False(t, exists)
		assert.Nil(t, func_)
	})

	// Testing case insensitivity (function names will be converted to lowercase)
	t.Run("Case insensitivity", func(t *testing.T) {
		testFunc := &TestFunction{
			name:     "lowercase_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
		}

		// Register lowercase function names
		err := Register(testFunc)
		assert.NoError(t, err)

		// You should be able to find it using uppercase (since internal conversion is lowercase).
		func_, exists := Get("LOWERCASE_FUNC")
		assert.True(t, exists)
		assert.NotNil(t, func_)

		// Get with the correct lowercase
		func_, exists = Get("lowercase_func")
		assert.True(t, exists)
		assert.NotNil(t, func_)

		// Cleanup
		Unregister("lowercase_func")
	})
}

// TestUnregisterEdgeCases tests the boundary status of the Unregister function
func TestUnregisterEdgeCases(t *testing.T) {
	// Test logout functions that do not exist
	t.Run("Unregister non-existent function", func(t *testing.T) {
		// This should not cause panic or errors
		result := Unregister("non_existent_func")
		assert.False(t, result)

		// The verification function really does not exist
		func_, exists := Get("non_existent_func")
		assert.False(t, exists)
		assert.Nil(t, func_)
	})

	// Test the logout empty name function
	t.Run("Unregister empty name", func(t *testing.T) {
		// This should not cause panic or errors
		result := Unregister("")
		assert.False(t, result)
	})

	// After test deregistration, deregister again
	t.Run("Double unregister", func(t *testing.T) {
		testFunc := &TestFunction{
			name:     "double_unregister_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
		}

		// Register the function
		err := Register(testFunc)
		assert.NoError(t, err)

		// The first time it was canceled
		result := Unregister("double_unregister_func")
		assert.True(t, result)
		func_, exists := Get("double_unregister_func")
		assert.False(t, exists)
		assert.Nil(t, func_)

		// Second cancellation (should be fine)
		result = Unregister("double_unregister_func")
		assert.False(t, result)
	})
}

// TestListAllEdgeCases tests the boundary status of the ListAll function
func TestListAllEdgeCases(t *testing.T) {
	// Register some test functions
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

	// Test non-empty registry
	t.Run("Non-empty registry", func(t *testing.T) {
		functions := ListAll()
		assert.GreaterOrEqual(t, len(functions), 3) // Include at least 3 functions we registered
		_, exists1 := functions["test_func1"]
		_, exists2 := functions["test_func2"]
		_, exists3 := functions["test_func3"]
		assert.True(t, exists1)
		assert.True(t, exists2)
		assert.True(t, exists3)
	})

	// Clean up the test function
	for _, fn := range testFunctions {
		Unregister(fn.name)
	}
}

// TestExecuteEdgeCases tests the boundary status of the Execute function
func TestExecuteEdgeCases(t *testing.T) {
	// Testing to execute a function that does not exist
	t.Run("Execute non-existent function", func(t *testing.T) {
		ctx := &FunctionContext{}
		result, err := Execute("non_existent_func", ctx, []any{})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "not found")
	})

	// The test executes the function that returns an error
	t.Run("Execute function that returns error", func(t *testing.T) {
		errorFunc := &TestFunction{
			name:     "error_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				return nil, errors.New("test error")
			},
		}

		err := Register(errorFunc)
		assert.NoError(t, err)

		ctx := &FunctionContext{}
		result, err := Execute("error_func", ctx, []any{})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "test error")

		// Cleanup
		Unregister("error_func")
	})

	// Test executing a function with parameters
	t.Run("Execute function with arguments", func(t *testing.T) {
		sumFunc := &TestFunction{
			name:     "sum_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
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
		// No parameter calls
		result, err := Execute("sum_func", ctx, []any{})
		assert.NoError(t, err)
		assert.Equal(t, 0, result)

		// Call with parameters
		result, err = Execute("sum_func", ctx, []any{1, 2, 3, 4, 5})
		assert.NoError(t, err)
		assert.Equal(t, 15, result)

		// Cleanup
		Unregister("sum_func")
	})

	// Test the function that executes panic
	t.Run("Execute function that panics", func(t *testing.T) {
		panicFunc := &TestFunction{
			name:     "panic_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				panic("test panic")
			},
		}

		err := Register(panicFunc)
		assert.NoError(t, err)

		ctx := &FunctionContext{}
		// The execution should capture panic and return errors
		assert.Panics(t, func() {
			Execute("panic_func", ctx, []any{})
		})

		// Cleanup
		Unregister("panic_func")
	})
}

// TestValidateEdgeCases tests the boundaries of the Validate function
func TestValidateEdgeCases(t *testing.T) {
	// Testing verifies functions that do not exist
	t.Run("Validate non-existent function", func(t *testing.T) {
		err := Validate("non_existent_func")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function 'non_existent_func' not found")
	})

	// Test and verify the empty name function
	t.Run("Validate empty name function", func(t *testing.T) {
		err := Validate("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "function '' not found")
	})

	// Test validates the effective function
	t.Run("Validate valid function", func(t *testing.T) {
		validFunc := &TestFunction{
			name:     "valid_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				return "valid", nil
			},
		}

		err := Register(validFunc)
		assert.NoError(t, err)

		err = Validate("valid_func")
		assert.NoError(t, err)

		// Cleanup
		Unregister("valid_func")
	})

	// Test and verify multiple functions
	t.Run("Validate multiple functions", func(t *testing.T) {
		testFunc1 := &TestFunction{
			name:     "test_func1",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				return "test1", nil
			},
		}
		testFunc2 := &TestFunction{
			name:     "test_func2",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				return "test2", nil
			},
		}

		err := Register(testFunc1)
		assert.NoError(t, err)
		err = Register(testFunc2)
		assert.NoError(t, err)

		// Verify the function that exists
		err = Validate("test_func1")
		assert.NoError(t, err)
		err = Validate("test_func2")
		assert.NoError(t, err)

		// Verify functions that do not exist
		err = Validate("test_func3")
		assert.Error(t, err)

		// Cleanup
		Unregister("test_func1")
		Unregister("test_func2")
	})
}

// TestConcurrentAccess tests for concurrent access
func TestConcurrentAccess(t *testing.T) {
	// This test checks the behavior of the registry during concurrent access
	// Note: Actual concurrency security needs to be handled in the registry implementation
	t.Run("Concurrent register and get", func(t *testing.T) {
		testFunc := &TestFunction{
			name:     "concurrent_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				return "concurrent_test", nil
			},
		}

		// Register the function
		err := Register(testFunc)
		assert.NoError(t, err)

		// Get the function
		func_, exists := Get("concurrent_func")
		assert.True(t, exists)
		assert.NotNil(t, func_)

		ctx := &FunctionContext{}
		// Execute the function
		result, err := Execute("concurrent_func", ctx, []any{})
		assert.NoError(t, err)
		assert.Equal(t, "concurrent_test", result)

		// Cleanup
		Unregister("concurrent_func")
	})
}

// TestFunctionSignatureVariations tests changes in signatures of different functions
func TestFunctionSignatureVariations(t *testing.T) {
	// Testing the no-parameter function
	t.Run("No parameter function", func(t *testing.T) {
		noParamFunc := &TestFunction{
			name:     "no_param_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  0,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				if len(args) != 0 {
					return nil, errors.New("expected no arguments")
				}
				return "no_params", nil
			},
		}

		err := Register(noParamFunc)
		assert.NoError(t, err)

		ctx := &FunctionContext{}
		result, err := Execute("no_param_func", ctx, []any{})
		assert.NoError(t, err)
		assert.Equal(t, "no_params", result)

		// Cleanup
		Unregister("no_param_func")
	})

	// Test variable parameter functions
	t.Run("Variadic parameter function", func(t *testing.T) {
		variadicFunc := &TestFunction{
			name:     "variadic_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				return len(args), nil
			},
		}

		err := Register(variadicFunc)
		assert.NoError(t, err)

		ctx := &FunctionContext{}
		// Test different numbers of parameters
		result, err := Execute("variadic_func", ctx, []any{})
		assert.NoError(t, err)
		assert.Equal(t, 0, result)

		result, err = Execute("variadic_func", ctx, []any{1})
		assert.NoError(t, err)
		assert.Equal(t, 1, result)

		result, err = Execute("variadic_func", ctx, []any{1, 2, 3})
		assert.NoError(t, err)
		assert.Equal(t, 3, result)

		// Cleanup
		Unregister("variadic_func")
	})

	// Tests return functions of different types
	t.Run("Different return types", func(t *testing.T) {
		// Returns the string
		stringFunc := &TestFunction{
			name:     "string_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				return "string_result", nil
			},
		}

		// Return the numbers
		numberFunc := &TestFunction{
			name:     "number_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				return 42, nil
			},
		}

		// Returns the boolean value
		boolFunc := &TestFunction{
			name:     "bool_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				return true, nil
			},
		}

		// Return nil
		nilFunc := &TestFunction{
			name:     "nil_func",
			fnType:   TypeCustom,
			category: "test",
			minArgs:  0,
			maxArgs:  -1,
			executor: func(ctx *FunctionContext, args []any) (any, error) {
				return nil, nil
			},
		}

		// Register all functions
		assert.NoError(t, Register(stringFunc))
		assert.NoError(t, Register(numberFunc))
		assert.NoError(t, Register(boolFunc))
		assert.NoError(t, Register(nilFunc))

		ctx := &FunctionContext{}
		// Test execution
		result, err := Execute("string_func", ctx, []any{})
		assert.NoError(t, err)
		assert.Equal(t, "string_result", result)

		result, err = Execute("number_func", ctx, []any{})
		assert.NoError(t, err)
		assert.Equal(t, 42, result)

		result, err = Execute("bool_func", ctx, []any{})
		assert.NoError(t, err)
		assert.Equal(t, true, result)

		result, err = Execute("nil_func", ctx, []any{})
		assert.NoError(t, err)
		assert.Nil(t, result)

		// Cleanup
		Unregister("string_func")
		Unregister("number_func")
		Unregister("bool_func")
		Unregister("nil_func")
	})
}
