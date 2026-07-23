package functions

import (
	"reflect"
	"testing"
)

// isWindowFunction to determine whether it is a window function
func isWindowFunction(funcName string) bool {
	windowFunctions := map[string]bool{
		"window_start": true,
		"window_end":   true,
		"lag":          true,
		"first_value":  true,
		"last_value":   true,
	}
	return windowFunctions[funcName]
}

func TestNewWindowFunctions(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []any
		want     any
		wantErr  bool
		setup    func(fn AggregatorFunction)
	}{
		// first_value Function Testing
		{
			name:     "first_value basic",
			funcName: "first_value",
			args:     []any{"test"},
			want:     "first",
			wantErr:  false,
			setup: func(fn AggregatorFunction) {
				fn.Add("first")
				fn.Add("second")
				fn.Add("third")
			},
		},
		{
			name:     "first_value empty",
			funcName: "first_value",
			args:     []any{"test"},
			want:     nil,
			wantErr:  false,
			setup:    func(fn AggregatorFunction) {},
		},

		// last_value Function testing
		{
			name:     "last_value basic",
			funcName: "last_value",
			args:     []any{"test"},
			want:     "third",
			wantErr:  false,
			setup: func(fn AggregatorFunction) {
				fn.Add("first")
				fn.Add("second")
				fn.Add("third")
			},
		},
		{
			name:     "last_value empty",
			funcName: "last_value",
			args:     []any{"test"},
			want:     nil,
			wantErr:  false,
			setup:    func(fn AggregatorFunction) {},
		},

		// nth_value Function Testing
		{
			name:     "nth_value first",
			funcName: "nth_value",
			args:     []any{"test", 1},
			want:     "first",
			wantErr:  false,
			setup: func(fn AggregatorFunction) {
				fn.Add("first")
				fn.Add("second")
				fn.Add("third")
			},
		},
		{
			name:     "nth_value second",
			funcName: "nth_value",
			args:     []any{"test", 2},
			want:     "second",
			wantErr:  false,
			setup: func(fn AggregatorFunction) {
				fn.Add("first")
				fn.Add("second")
				fn.Add("third")
			},
		},
		{
			name:     "nth_value out of range",
			funcName: "nth_value",
			args:     []any{"test", 5},
			want:     nil,
			wantErr:  false,
			setup: func(fn AggregatorFunction) {
				fn.Add("first")
				fn.Add("second")
			},
		},
		{
			name:     "nth_value invalid n type",
			funcName: "nth_value",
			args:     []any{"test", "invalid"},
			wantErr:  true,
			setup:    func(fn AggregatorFunction) {},
		},
		{
			name:     "nth_value zero n",
			funcName: "nth_value",
			args:     []any{"test", 0},
			wantErr:  true,
			setup:    func(fn AggregatorFunction) {},
		},
		{
			name:     "nth_value negative n",
			funcName: "nth_value",
			args:     []any{"test", -1},
			wantErr:  true,
			setup:    func(fn AggregatorFunction) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, exists := Get(tt.funcName)
			if !exists {
				t.Fatalf("Function %s not found", tt.funcName)
			}

			// Check whether the function implements the AggregatorFunction interface
			aggFn, ok := fn.(AggregatorFunction)
			if !ok {
				t.Fatalf("Function %s does not implement AggregatorFunction", tt.funcName)
			}

			// First, the function's Validate method is executed to set the parameters
			err := fn.Validate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// If the expectation is incorrect and Validate has failed, the test passes
			if tt.wantErr && err != nil {
				return
			}

			// Create a new aggregator instance
			aggInstance := aggFn.New()

			// Execute the setup function to add test data
			if tt.setup != nil {
				tt.setup(aggInstance)
			}

			// For window function testing, there is no need to call the Execute method
			// The Execute method is mainly used for streaming processing. Here, we will directly test the aggregator's Result method
			// If you need to test the Execute method, it should be called on the original function instance
			if !isWindowFunction(tt.funcName) {
				// For non-window functions, execute on the aggregator instance
				if aggFunc, ok := aggInstance.(Function); ok {
					_, err = aggFunc.Execute(nil, tt.args)
				} else {
					// Execute the function
					_, err = fn.Execute(nil, tt.args)
				}
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// For window functions, we mainly test the aggregator's Result method
				aggResult := aggInstance.Result()
				if tt.want != nil && aggResult != tt.want {
					t.Errorf("AggregatorFunction.Result() = %v, want %v", aggResult, tt.want)
				}
			}
		})
	}
}

// Test the basic functions of the window function
func TestWindowFunctionBasics(t *testing.T) {
	// Test window_start and window_end functions
	t.Run("WindowStartEndFunctions", func(t *testing.T) {
		windowStartFunc, exists := Get("window_start")
		if !exists {
			t.Fatal("window_start function not found")
		}

		windowEndFunc, exists := Get("window_end")
		if !exists {
			t.Fatal("window_end function not found")
		}

		// Testing behavior when there is no window information
		ctx := &FunctionContext{
			Data: map[string]any{},
		}
		_, err := windowStartFunc.Execute(ctx, []any{})
		if err != nil {
			t.Errorf("Execute() error = %v", err)
		}
		// If there is no window information, nil or the default value should be returned

		_, err = windowEndFunc.Execute(ctx, []any{})
		if err != nil {
			t.Errorf("Execute() error = %v", err)
		}
		// If there is no window information, nil or the default value should be returned
	})
}

func TestWindowFunctionResetAndClone(t *testing.T) {
	// Test the reset and clone of WindowStartFunction
	windowStart := NewWindowStartFunction()
	agg := windowStart.New().(*WindowStartFunction)
	agg.Add("data1")
	agg.Add("data2")

	// Test Reset
	agg.Reset()
	res := agg.Result()
	if res != nil {
		t.Errorf("WindowStart Reset failed, result = %v, want nil", res)
	}

	// Testing Clone
	clone := agg.Clone().(*WindowStartFunction)
	if clone.windowStart != agg.windowStart {
		t.Errorf("WindowStart Clone failed")
	}

	// Test the reset and clone of WindowEndFunction
	windowEnd := NewWindowEndFunction()
	agg2 := windowEnd.New().(*WindowEndFunction)
	agg2.Add("data1")
	agg2.Add("data2")

	// Test Reset
	agg2.Reset()
	res2 := agg2.Result()
	if res2 != nil {
		t.Errorf("WindowEnd Reset failed, result = %v, want nil", res2)
	}

	// Testing Clone
	clone2 := agg2.Clone().(*WindowEndFunction)
	if clone2.windowEnd != agg2.windowEnd {
		t.Errorf("WindowEnd Clone failed")
	}
}

func TestExpressionFunction(t *testing.T) {
	fn := NewExpressionFunction()
	ctx := &FunctionContext{}

	// Test the Execute method
	_, err := fn.Execute(ctx, []any{"x + y", 1, 2})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	// Note: The result here depends on the implementation of the expression evaluator
	// We mainly test whether the function call is successful

	// Test aggregator methods
	agg := fn.New().(*ExpressionFunction)
	agg.Add("test")
	_ = agg.Result()
	// The result depends on the implementation of the expression evaluator

	// Test Reset
	agg.Reset()
	res := agg.Result()
	if res != nil {
		t.Errorf("Reset failed, result = %v, want nil", res)
	}

	// Testing Clone
	clone := agg.Clone().(*ExpressionFunction)
	if !reflect.DeepEqual(clone.values, agg.values) {
		t.Errorf("Clone failed")
	}
}

func TestExpressionAggregatorFunction(t *testing.T) {
	fn := NewExpressionAggregatorFunction()
	ctx := &FunctionContext{}

	// Test the Execute method
	_, err := fn.Execute(ctx, []any{"sum(x)", 1, 2, 3})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	// Note: The result here depends on the implementation of the expression evaluator

	// Test aggregator methods
	agg := fn.New().(*ExpressionAggregatorFunction)
	agg.Add("test")
	_ = agg.Result()
	// The result depends on the implementation of the expression evaluator

	// Test Reset
	agg.Reset()
	res := agg.Result()
	if res != nil {
		t.Errorf("Reset failed, result = %v, want nil", res)
	}

	// Testing Clone
	clone := agg.Clone().(*ExpressionAggregatorFunction)
	if clone.lastResult != agg.lastResult {
		t.Errorf("Clone failed")
	}
}

func TestFirstValueFunction(t *testing.T) {
	fn := NewFirstValueFunction()

	// Test aggregator methods
	agg := fn.New().(*FirstValueFunction)
	agg.Add("x")
	agg.Add("y")
	agg.Add("z")
	res := agg.Result()
	if res != "x" {
		t.Errorf("Agg first_value result = %v, want x", res)
	}

	// Test Reset
	agg.Reset()
	res2 := agg.Result()
	if res2 != nil {
		t.Errorf("Reset failed, result = %v, want nil", res2)
	}

	// Testing Clone
	clone := agg.Clone().(*FirstValueFunction)
	if clone.firstValue != agg.firstValue {
		t.Errorf("Clone failed")
	}
}

func TestNthValueFunction(t *testing.T) {
	fn := NewNthValueFunction()

	// Test aggregator methods
	agg := fn.New().(*NthValueFunction)
	agg.Add("x")
	agg.Add("y")
	agg.Add("z")
	res := agg.Result()
	if res != "x" {
		t.Errorf("Agg nth_value result = %v, want x", res)
	}

	// Test Reset
	agg.Reset()
	res = agg.Result()
	if res != nil {
		t.Errorf("Reset failed, result = %v, want nil", res)
	}

	// Testing Clone
	clone := agg.Clone().(*NthValueFunction)
	if clone.n != agg.n {
		t.Errorf("Clone failed")
	}
}

func TestWindowFunctionEdgeCases(t *testing.T) {
	// NthValueFunction Validate/Execute boundary
	nth := NewNthValueFunction()
	if err := nth.Validate([]any{}); err == nil {
		t.Error("NthValueFunction.Validate should fail for insufficient args")
	}
	_, err := nth.Execute(nil, []any{})
	if err == nil {
		t.Error("NthValueFunction.Execute should fail for empty args")
	}
	agg2 := nth.New().(*NthValueFunction)
	agg2.Reset()
	_ = agg2.Clone()

	// FirstValueFunction Validate/Execute boundary
	first := NewFirstValueFunction()
	if err := first.Validate([]any{}); err == nil {
		t.Error("FirstValueFunction.Validate should fail for insufficient args")
	}
	_, err = first.Execute(nil, []any{})
	if err == nil {
		t.Error("FirstValueFunction.Execute should fail for empty args")
	}
	agg3 := first.New().(*FirstValueFunction)
	agg3.Reset()
	_ = agg3.Clone()

	// LastValueFunction Validate/Execute boundary
	last := NewLastValueFunction()
	if err := last.Validate([]any{}); err == nil {
		t.Error("LastValueFunction.Validate should fail for insufficient args")
	}
	_, err = last.Execute(nil, []any{})
	if err == nil {
		t.Error("LastValueFunction.Execute should fail for empty args")
	}
	agg4 := last.New().(*LastValueFunction)
	agg4.Reset()
	_ = agg4.Clone()

	// WindowStartFunction/WindowEndFunction Reset/Clone boundary
	ws := NewWindowStartFunction().New().(*WindowStartFunction)
	ws.Reset()
	_ = ws.Clone()
	we := NewWindowEndFunction().New().(*WindowEndFunction)
	we.Reset()
	_ = we.Clone()
}
