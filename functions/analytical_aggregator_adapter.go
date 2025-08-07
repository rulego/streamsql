package functions

// AnalyticalAggregatorAdapter provides adapter from analytical functions to aggregators
type AnalyticalAggregatorAdapter struct {
	analFunc AnalyticalFunction
	ctx      *FunctionContext
}

// NewAnalyticalAggregatorAdapter creates an analytical function aggregator adapter
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

// New creates a new adapter instance
func (a *AnalyticalAggregatorAdapter) New() interface{} {
	// For functions that implement AggregatorFunction interface, use their New method
	if aggFunc, ok := a.analFunc.(AggregatorFunction); ok {
		newAnalFunc := aggFunc.New().(AnalyticalFunction)
		return &AnalyticalAggregatorAdapter{
			analFunc: newAnalFunc,
			ctx: &FunctionContext{
				Data: make(map[string]interface{}),
			},
		}
	}

	// For other analytical functions, use Clone method
	return &AnalyticalAggregatorAdapter{
		analFunc: a.analFunc.Clone(),
		ctx: &FunctionContext{
			Data: make(map[string]interface{}),
		},
	}
}

// Add adds a value
func (a *AnalyticalAggregatorAdapter) Add(value interface{}) {
	// For functions that implement AggregatorFunction interface, call Add method directly
	if aggFunc, ok := a.analFunc.(AggregatorFunction); ok {
		aggFunc.Add(value)
		return
	}

	// For other analytical functions, execute the analytical function
	args := []interface{}{value}
	_, _ = a.analFunc.Execute(a.ctx, args)
}

// Result returns the result
func (a *AnalyticalAggregatorAdapter) Result() interface{} {
	// For LatestFunction, return LatestValue directly
	if latestFunc, ok := a.analFunc.(*LatestFunction); ok {
		return latestFunc.LatestValue
	}

	// For HadChangedFunction, return current state
	if hadChangedFunc, ok := a.analFunc.(*HadChangedFunction); ok {
		return hadChangedFunc.IsSet
	}

	// For LagFunction, call its Result method
	if lagFunc, ok := a.analFunc.(*LagFunction); ok {
		return lagFunc.Result()
	}

	// For other analytical functions, try to execute once to get current state result
	// Pass nil as parameter to indicate getting current state
	result, _ := a.analFunc.Execute(a.ctx, []interface{}{nil})
	return result
}

// CreateAnalyticalAggregatorFromFunctions creates analytical function aggregator from functions module
func CreateAnalyticalAggregatorFromFunctions(funcType string) interface{} {
	// First try to get from adapter registry
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

	// If not found, try to create directly
	adapter, err := NewAnalyticalAggregatorAdapter(funcType)
	if err != nil {
		return nil
	}

	return adapter
}
