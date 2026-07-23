# StreamSQL Functions Module expansion

## Overview

This extension implements unified management of aggregate and analysis functions, simplifying the extension process for custom functions. Now, you only need to implement the function in the `functions` module to automatically use it in the `aggregator` module.

## Major improvements

### 1. Unified function interface

- **AggregatorFunction**: Aggregate function interface supporting incremental computation
- **AnalyticalFunction**: Interface for analysis functions supporting state management
- **Function**: Basic function interface

### 2. Automatic adapter

- **AggregatorAdapter**: Adapt the aggregation function of the functions module to the aggregator module
- **AnalyticalAdapter**: Adapt the analysis functions of the functions module to the aggregator module

### 3. Simplified expansion process

Now, to add a custom function, you only need:
1. Implement functions in the functions module
2. Register functions and adapters
3. No need to modify aggregator modules

## How to use

### Create a custom aggregate function

```go
// 1. Define the function structure
type CustomSumFunction struct {
    *BaseFunction
    sum float64
}

// 2. Implement basic interfaces
func (f *CustomSumFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
    // Implement functional logic
}

// 3. Implement AggregatorFunction interfaces
func (f *CustomSumFunction) New() AggregatorFunction {
    return &CustomSumFunction{BaseFunction: f.BaseFunction}
}

func (f *CustomSumFunction) Add(value interface{}) {
    // Incremental calculation logic
}

func (f *CustomSumFunction) Result() interface{} {
    return f.sum
}

func (f *CustomSumFunction) Reset() {
    f.sum = 0
}

func (f *CustomSumFunction) Clone() AggregatorFunction {
    return &CustomSumFunction{BaseFunction: f.BaseFunction, sum: f.sum}
}

// 4. Register Function (TypeAggregation Automatically connect the aggregation adapter, no manual RegisterAggregatorAdapter required)
func init() {
    Register(NewCustomSumFunction())
}
```

### Create a custom analysis function

```go
// 1. Define the function structure (analyze that the function itself has no mutable state; the interline state is placed in the State below)
type CustomAnalyticalFunction struct {
    *BaseFunction
}

// 2. Implement basic interfaces (Execute Scalar path disabled: analysis functions must cross lines and be evaluated by state machines)
func (f *CustomAnalyticalFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
    return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER", f.GetName())
}

// 3. Implement StatefulAnalytic: NewState returns a single independent state, with the engine holding one copy for each PARTITION
func (f *CustomAnalyticalFunction) NewState() AnalyticState {
    return &customAnalyticalState{}
}

type customAnalyticalState struct {
    prev any
}

// Apply Each event call: Update the status with the current line parameter and return the current result (here returns the value of the previous line, i.e., lag semantics)
func (s *customAnalyticalState) Apply(args []any) any {
    cur := args[0]
    result := s.prev
    s.prev = cur
    return result
}

func (s *customAnalyticalState) Reset() { s.prev = nil }

// 4. Register functions (analyze function Register, no need to RegisterAnalyticalAdapter)
func init() {
    Register(NewCustomAnalyticalFunction())
}
```

### Use a simplified registration method

```go
// Register simple custom functions
RegisterCustomFunction("double", TypeAggregation, "数学函数", "将值乘以2", 1, 1,
    func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
        val, err := cast.ToFloat64E(args[0])
        if err != nil {
            return nil, err
        }
        return val * 2, nil
    })
```

## Built-in functions

### Aggregate Function
- `sum`: Seek harmony
- `avg`: Average
- `min`: Minimum value
- `max`: Maximum value
- `count`: Count
- `stddev`: Standard deviation
- `median`: Median
- `percentile`: percentile
- `collect`: Collect all values
- `last_value`: The last value
- `merge_agg`: Merge and aggregate
- `stddevs`: Standard deviation of the sample
- `deduplicate`: Reduce deduplication
- `var`: Population variance
- `vars`: Sample variance

### Analyze the function
- `lag`: The lag function
- `latest`: Latest value
- `changed_col`: Change column
- `had_changed`: Whether it changes

## Custom function example

Refer to the example from the `custom_example.go` file:
- `CustomProductFunction`: Product aggregation function
- `CustomGeometricMeanFunction`: Geometric mean aggregation function
- `CustomMovingAverageFunction`: Moving average analysis function
