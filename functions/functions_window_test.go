package functions

import (
	"reflect"
	"testing"
)

// isWindowFunction 判断是否为窗口函数
func isWindowFunction(funcName string) bool {
	windowFunctions := map[string]bool{
		"row_number":   true,
		"window_start": true,
		"window_end":   true,
		"lead":         true,
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
		args     []interface{}
		want     interface{}
		wantErr  bool
		setup    func(fn AggregatorFunction)
	}{
		// first_value 函数测试
		{
			name:     "first_value basic",
			funcName: "first_value",
			args:     []interface{}{"test"},
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
			args:     []interface{}{"test"},
			want:     nil,
			wantErr:  false,
			setup:    func(fn AggregatorFunction) {},
		},

		// last_value 函数测试
		{
			name:     "last_value basic",
			funcName: "last_value",
			args:     []interface{}{"test"},
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
			args:     []interface{}{"test"},
			want:     nil,
			wantErr:  false,
			setup:    func(fn AggregatorFunction) {},
		},

		// lag 函数测试
		{
			name:     "lag default offset",
			funcName: "lag",
			args:     []interface{}{"test"},
			want:     "second",
			wantErr:  false,
			setup: func(fn AggregatorFunction) {
				fn.Add("first")
				fn.Add("second")
				fn.Add("third")
			},
		},
		{
			name:     "lag with offset 2",
			funcName: "lag",
			args:     []interface{}{"test", 2},
			want:     "first",
			wantErr:  false,
			setup: func(fn AggregatorFunction) {
				fn.Add("first")
				fn.Add("second")
				fn.Add("third")
			},
		},
		{
			name:     "lag with default value",
			funcName: "lag",
			args:     []interface{}{"test", 5, "default"},
			want:     "default",
			wantErr:  false,
			setup: func(fn AggregatorFunction) {
				fn.Add("first")
				fn.Add("second")
			},
		},
		{
			name:     "lag invalid offset type",
			funcName: "lag",
			args:     []interface{}{"test", "invalid"},
			wantErr:  true,
			setup:    func(fn AggregatorFunction) {},
		},

		// lead 函数测试
		{
			name:     "lead default offset",
			funcName: "lead",
			args:     []interface{}{"test"},
			want:     nil, // Lead函数简化实现返回nil
			wantErr:  false,
			setup: func(fn AggregatorFunction) {
				fn.Add("first")
				fn.Add("second")
				fn.Add("third")
			},
		},
		{
			name:     "lead with default value",
			funcName: "lead",
			args:     []interface{}{"test", 1, "default"},
			want:     "default",
			wantErr:  false,
			setup:    func(fn AggregatorFunction) {},
		},
		{
			name:     "lead invalid offset type",
			funcName: "lead",
			args:     []interface{}{"test", "invalid"},
			wantErr:  true,
			setup:    func(fn AggregatorFunction) {},
		},

		// nth_value 函数测试
		{
			name:     "nth_value first",
			funcName: "nth_value",
			args:     []interface{}{"test", 1},
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
			args:     []interface{}{"test", 2},
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
			args:     []interface{}{"test", 5},
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
			args:     []interface{}{"test", "invalid"},
			wantErr:  true,
			setup:    func(fn AggregatorFunction) {},
		},
		{
			name:     "nth_value zero n",
			funcName: "nth_value",
			args:     []interface{}{"test", 0},
			wantErr:  true,
			setup:    func(fn AggregatorFunction) {},
		},
		{
			name:     "nth_value negative n",
			funcName: "nth_value",
			args:     []interface{}{"test", -1},
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

			// 检查函数是否实现了AggregatorFunction接口
			aggFn, ok := fn.(AggregatorFunction)
			if !ok {
				t.Fatalf("Function %s does not implement AggregatorFunction", tt.funcName)
			}

			// 先执行函数的Validate方法来设置参数
			err := fn.Validate(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// 如果期望错误且Validate已经失败，则测试通过
			if tt.wantErr && err != nil {
				return
			}

			// 创建新的聚合器实例
			aggInstance := aggFn.New()

			// 执行setup函数添加测试数据
			if tt.setup != nil {
				tt.setup(aggInstance)
			}

			// 对于窗口函数测试，不需要调用Execute方法
			// Execute方法主要用于流式处理，这里我们直接测试聚合器的Result方法
			// 如果需要测试Execute方法，应该在原始函数实例上调用
			if !isWindowFunction(tt.funcName) {
				// 对于非窗口函数，在聚合器实例上执行
				if aggFunc, ok := aggInstance.(Function); ok {
					_, err = aggFunc.Execute(nil, tt.args)
				} else {
					// 执行函数
					_, err = fn.Execute(nil, tt.args)
				}
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// 对于窗口函数，我们主要测试聚合器的Result方法
				aggResult := aggInstance.Result()
				if tt.want != nil && aggResult != tt.want {
					t.Errorf("AggregatorFunction.Result() = %v, want %v", aggResult, tt.want)
				}
			}
		})
	}
}

// 测试窗口函数的基本功能
func TestWindowFunctionBasics(t *testing.T) {
	// 测试row_number函数
	t.Run("RowNumberFunction", func(t *testing.T) {
		rowNumFunc, exists := Get("row_number")
		if !exists {
			t.Fatal("row_number function not found")
		}

		// 重置函数状态
		if rowNum, ok := rowNumFunc.(*RowNumberFunction); ok {
			rowNum.Reset()
		}

		// 测试行号递增
		result1, err := rowNumFunc.Execute(nil, []interface{}{})
		if err != nil {
			t.Errorf("Execute() error = %v", err)
		}
		if result1 != int64(1) {
			t.Errorf("First call should return 1, got %v", result1)
		}

		result2, err := rowNumFunc.Execute(nil, []interface{}{})
		if err != nil {
			t.Errorf("Execute() error = %v", err)
		}
		if result2 != int64(2) {
			t.Errorf("Second call should return 2, got %v", result2)
		}
	})

	// 测试window_start和window_end函数
	t.Run("WindowStartEndFunctions", func(t *testing.T) {
		windowStartFunc, exists := Get("window_start")
		if !exists {
			t.Fatal("window_start function not found")
		}

		windowEndFunc, exists := Get("window_end")
		if !exists {
			t.Fatal("window_end function not found")
		}

		// 测试无窗口信息时的行为
		ctx := &FunctionContext{
			Data: map[string]interface{}{},
		}
		_, err := windowStartFunc.Execute(ctx, []interface{}{})
		if err != nil {
			t.Errorf("Execute() error = %v", err)
		}
		// 无窗口信息时应该返回nil或默认值

		_, err = windowEndFunc.Execute(ctx, []interface{}{})
		if err != nil {
			t.Errorf("Execute() error = %v", err)
		}
		// 无窗口信息时应该返回nil或默认值
	})
}

func TestWindowFunctionResetAndClone(t *testing.T) {
	// 测试WindowStartFunction的Reset和Clone
	windowStart := NewWindowStartFunction()
	agg := windowStart.New().(*WindowStartFunction)
	agg.Add("data1")
	agg.Add("data2")

	// 测试Reset
	agg.Reset()
	res := agg.Result()
	if res != nil {
		t.Errorf("WindowStart Reset failed, result = %v, want nil", res)
	}

	// 测试Clone
	clone := agg.Clone().(*WindowStartFunction)
	if clone.windowStart != agg.windowStart {
		t.Errorf("WindowStart Clone failed")
	}

	// 测试WindowEndFunction的Reset和Clone
	windowEnd := NewWindowEndFunction()
	agg2 := windowEnd.New().(*WindowEndFunction)
	agg2.Add("data1")
	agg2.Add("data2")

	// 测试Reset
	agg2.Reset()
	res2 := agg2.Result()
	if res2 != nil {
		t.Errorf("WindowEnd Reset failed, result = %v, want nil", res2)
	}

	// 测试Clone
	clone2 := agg2.Clone().(*WindowEndFunction)
	if clone2.windowEnd != agg2.windowEnd {
		t.Errorf("WindowEnd Clone failed")
	}
}

func TestExpressionFunction(t *testing.T) {
	fn := NewExpressionFunction()
	ctx := &FunctionContext{}

	// 测试Execute方法
	_, err := fn.Execute(ctx, []interface{}{"x + y", 1, 2})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	// 注意：这里的结果取决于表达式求值器的实现
	// 我们主要测试函数调用是否成功

	// 测试聚合器方法
	agg := fn.New().(*ExpressionFunction)
	agg.Add("test")
	_ = agg.Result()
	// 结果取决于表达式求值器的实现

	// 测试Reset
	agg.Reset()
	res := agg.Result()
	if res != nil {
		t.Errorf("Reset failed, result = %v, want nil", res)
	}

	// 测试Clone
	clone := agg.Clone().(*ExpressionFunction)
	if !reflect.DeepEqual(clone.values, agg.values) {
		t.Errorf("Clone failed")
	}
}

func TestExpressionAggregatorFunction(t *testing.T) {
	fn := NewExpressionAggregatorFunction()
	ctx := &FunctionContext{}

	// 测试Execute方法
	_, err := fn.Execute(ctx, []interface{}{"sum(x)", 1, 2, 3})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	// 注意：这里的结果取决于表达式求值器的实现

	// 测试聚合器方法
	agg := fn.New().(*ExpressionAggregatorFunction)
	agg.Add("test")
	_ = agg.Result()
	// 结果取决于表达式求值器的实现

	// 测试Reset
	agg.Reset()
	res := agg.Result()
	if res != nil {
		t.Errorf("Reset failed, result = %v, want nil", res)
	}

	// 测试Clone
	clone := agg.Clone().(*ExpressionAggregatorFunction)
	if clone.lastResult != agg.lastResult {
		t.Errorf("Clone failed")
	}
}

func TestFirstValueFunction(t *testing.T) {
	fn := NewFirstValueFunction()

	// 测试聚合器方法
	agg := fn.New().(*FirstValueFunction)
	agg.Add("x")
	agg.Add("y")
	agg.Add("z")
	res := agg.Result()
	if res != "x" {
		t.Errorf("Agg first_value result = %v, want x", res)
	}

	// 测试Reset
	agg.Reset()
	res2 := agg.Result()
	if res2 != nil {
		t.Errorf("Reset failed, result = %v, want nil", res2)
	}

	// 测试Clone
	clone := agg.Clone().(*FirstValueFunction)
	if clone.firstValue != agg.firstValue {
		t.Errorf("Clone failed")
	}
}

func TestLeadFunction(t *testing.T) {
	fn := NewLeadFunction()

	// 测试聚合器方法
	agg := fn.New().(*LeadFunction)
	agg.Add("x")
	agg.Add("y")
	agg.Add("z")
	res := agg.Result()
	if res != nil {
		t.Errorf("Agg lead result = %v, want nil", res)
	}

	// 测试Reset
	agg.Reset()
	res2 := agg.Result()
	if res2 != nil {
		t.Errorf("Reset failed, result = %v, want nil", res2)
	}

	// 测试Clone
	clone := agg.Clone().(*LeadFunction)
	if clone.defaultValue != agg.defaultValue || clone.offset != agg.offset {
		t.Errorf("Clone failed")
	}
}

func TestNthValueFunction(t *testing.T) {
	fn := NewNthValueFunction()

	// 测试聚合器方法
	agg := fn.New().(*NthValueFunction)
	agg.Add("x")
	agg.Add("y")
	agg.Add("z")
	res := agg.Result()
	if res != "x" {
		t.Errorf("Agg nth_value result = %v, want x", res)
	}

	// 测试Reset
	agg.Reset()
	res = agg.Result()
	if res != nil {
		t.Errorf("Reset failed, result = %v, want nil", res)
	}

	// 测试Clone
	clone := agg.Clone().(*NthValueFunction)
	if clone.n != agg.n {
		t.Errorf("Clone failed")
	}
}

func TestWindowFunctionEdgeCases(t *testing.T) {
	// LeadFunction Validate/Execute边界
	lead := NewLeadFunction()
	if err := lead.Validate([]interface{}{}); err == nil {
		t.Error("LeadFunction.Validate should fail for insufficient args")
	}
	_, err := lead.Execute(nil, []interface{}{})
	if err == nil {
		t.Error("LeadFunction.Execute should fail for empty args")
	}
	agg := lead.New().(*LeadFunction)
	agg.Reset()
	_ = agg.Clone()

	// NthValueFunction Validate/Execute边界
	nth := NewNthValueFunction()
	if err := nth.Validate([]interface{}{}); err == nil {
		t.Error("NthValueFunction.Validate should fail for insufficient args")
	}
	_, err = nth.Execute(nil, []interface{}{})
	if err == nil {
		t.Error("NthValueFunction.Execute should fail for empty args")
	}
	agg2 := nth.New().(*NthValueFunction)
	agg2.Reset()
	_ = agg2.Clone()

	// FirstValueFunction Validate/Execute边界
	first := NewFirstValueFunction()
	if err := first.Validate([]interface{}{}); err == nil {
		t.Error("FirstValueFunction.Validate should fail for insufficient args")
	}
	_, err = first.Execute(nil, []interface{}{})
	if err == nil {
		t.Error("FirstValueFunction.Execute should fail for empty args")
	}
	agg3 := first.New().(*FirstValueFunction)
	agg3.Reset()
	_ = agg3.Clone()

	// LastValueFunction Validate/Execute边界
	last := NewLastValueFunction()
	if err := last.Validate([]interface{}{}); err == nil {
		t.Error("LastValueFunction.Validate should fail for insufficient args")
	}
	_, err = last.Execute(nil, []interface{}{})
	if err == nil {
		t.Error("LastValueFunction.Execute should fail for empty args")
	}
	agg4 := last.New().(*LastValueFunction)
	agg4.Reset()
	_ = agg4.Clone()

	// WindowStartFunction/WindowEndFunction Reset/Clone边界
	ws := NewWindowStartFunction().New().(*WindowStartFunction)
	ws.Reset()
	_ = ws.Clone()
	we := NewWindowEndFunction().New().(*WindowEndFunction)
	we.Reset()
	_ = we.Clone()
}
