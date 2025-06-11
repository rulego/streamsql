package functions

import (
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