package functions

// AnalyticalAggregatorAdapter 分析函数到聚合器的适配器
type AnalyticalAggregatorAdapter struct {
	analFunc AnalyticalFunction
	ctx      *FunctionContext
}

// NewAnalyticalAggregatorAdapter 创建分析函数聚合器适配器
func NewAnalyticalAggregatorAdapter(name string) (*AnalyticalAggregatorAdapter, error) {
	analFunc, err := CreateAnalytical(name)
	if err != nil {
		return nil, err
	}

	return &AnalyticalAggregatorAdapter{
		analFunc: analFunc,
		ctx: &FunctionContext{
			Data: make(map[string]interface{}),
		},
	}, nil
}

// New 创建新的适配器实例
func (a *AnalyticalAggregatorAdapter) New() interface{} {
	// 对于实现了AggregatorFunction接口的函数，使用其New方法
	if aggFunc, ok := a.analFunc.(AggregatorFunction); ok {
		newAnalFunc := aggFunc.New().(AnalyticalFunction)
		return &AnalyticalAggregatorAdapter{
			analFunc: newAnalFunc,
			ctx: &FunctionContext{
				Data: make(map[string]interface{}),
			},
		}
	}

	// 对于其他分析函数，使用Clone方法
	return &AnalyticalAggregatorAdapter{
		analFunc: a.analFunc.Clone(),
		ctx: &FunctionContext{
			Data: make(map[string]interface{}),
		},
	}
}

// Add 添加值
func (a *AnalyticalAggregatorAdapter) Add(value interface{}) {
	// 对于实现了AggregatorFunction接口的函数，直接调用Add方法
	if aggFunc, ok := a.analFunc.(AggregatorFunction); ok {
		aggFunc.Add(value)
		return
	}

	// 对于其他分析函数，执行分析函数
	args := []interface{}{value}
	a.analFunc.Execute(a.ctx, args)
}

// Result 获取结果
func (a *AnalyticalAggregatorAdapter) Result() interface{} {
	// 对于LatestFunction，直接返回LatestValue
	if latestFunc, ok := a.analFunc.(*LatestFunction); ok {
		return latestFunc.LatestValue
	}

	// 对于HadChangedFunction，返回当前状态
	if hadChangedFunc, ok := a.analFunc.(*HadChangedFunction); ok {
		return hadChangedFunc.IsSet
	}

	// 对于LagFunction，调用其Result方法
	if lagFunc, ok := a.analFunc.(*LagFunction); ok {
		return lagFunc.Result()
	}

	// 对于其他分析函数，尝试执行一次来获取当前状态的结果
	// 这里传入nil作为参数，表示获取当前状态
	result, _ := a.analFunc.Execute(a.ctx, []interface{}{nil})
	return result
}

// CreateAnalyticalAggregatorFromFunctions 从functions模块创建分析函数聚合器
func CreateAnalyticalAggregatorFromFunctions(funcType string) interface{} {
	// 首先尝试从适配器注册表获取
	if constructor, exists := GetAnalyticalAdapter(funcType); exists {
		adapter := constructor()
		if adapter != nil {
			return &AnalyticalAggregatorAdapter{
				analFunc: adapter.analFunc,
				ctx: &FunctionContext{
					Data: make(map[string]interface{}),
				},
			}
		}
	}

	// 如果没有找到，尝试直接创建
	adapter, err := NewAnalyticalAggregatorAdapter(funcType)
	if err != nil {
		return nil
	}

	return adapter
}
