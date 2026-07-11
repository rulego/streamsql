# StreamSQL Functions 模块扩展

## 概述

本次扩展实现了统一的聚合函数和分析函数管理，简化了自定义函数的扩展过程。现在只需要在 `functions` 模块中实现函数，就可以自动在 `aggregator` 模块中使用。

## 主要改进

### 1. 统一的函数接口

- **AggregatorFunction**: 支持增量计算的聚合函数接口
- **AnalyticalFunction**: 支持状态管理的分析函数接口
- **Function**: 基础函数接口

### 2. 自动适配器

- **AggregatorAdapter**: 将 functions 模块的聚合函数适配到 aggregator 模块
- **AnalyticalAdapter**: 将 functions 模块的分析函数适配到 aggregator 模块

### 3. 简化的扩展流程

现在添加自定义函数只需要：
1. 在 functions 模块中实现函数
2. 注册函数和适配器
3. 无需修改 aggregator 模块

## 使用方法

### 创建自定义聚合函数

```go
// 1. 定义函数结构
type CustomSumFunction struct {
    *BaseFunction
    sum float64
}

// 2. 实现基础接口
func (f *CustomSumFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
    // 实现函数逻辑
}

// 3. 实现AggregatorFunction接口
func (f *CustomSumFunction) New() AggregatorFunction {
    return &CustomSumFunction{BaseFunction: f.BaseFunction}
}

func (f *CustomSumFunction) Add(value interface{}) {
    // 增量计算逻辑
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

// 4. 注册函数（TypeAggregation 自动接通聚合适配器，无需手动 RegisterAggregatorAdapter）
func init() {
    Register(NewCustomSumFunction())
}
```

### 创建自定义分析函数

```go
// 1. 定义函数结构（分析函数本身无可变状态，跨行状态放在下面的 State 里）
type CustomAnalyticalFunction struct {
    *BaseFunction
}

// 2. 实现基础接口（Execute 标量路径禁用：分析函数需跨行状态，由状态机求值）
func (f *CustomAnalyticalFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
    return nil, fmt.Errorf("analytic function %q must be used as a field or with OVER", f.GetName())
}

// 3. 实现 StatefulAnalytic：NewState 返回一份独立状态，引擎为每个 PARTITION 各持一份
func (f *CustomAnalyticalFunction) NewState() AnalyticState {
    return &customAnalyticalState{}
}

type customAnalyticalState struct {
    prev any
}

// Apply 每条事件调用：用当前行参数更新状态，返回当前结果（这里返回上一行的值，即 lag 语义）
func (s *customAnalyticalState) Apply(args []any) any {
    cur := args[0]
    result := s.prev
    s.prev = cur
    return result
}

func (s *customAnalyticalState) Reset() { s.prev = nil }

// 4. 注册函数（分析函数 Register 即可，无需 RegisterAnalyticalAdapter）
func init() {
    Register(NewCustomAnalyticalFunction())
}
```

### 使用简化的注册方式

```go
// 注册简单的自定义函数
RegisterCustomFunction("double", TypeAggregation, "数学函数", "将值乘以2", 1, 1,
    func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
        val, err := cast.ToFloat64E(args[0])
        if err != nil {
            return nil, err
        }
        return val * 2, nil
    })
```

## 内置函数

### 聚合函数
- `sum`: 求和
- `avg`: 平均值
- `min`: 最小值
- `max`: 最大值
- `count`: 计数
- `stddev`: 标准差
- `median`: 中位数
- `percentile`: 百分位数
- `collect`: 收集所有值
- `last_value`: 最后一个值
- `merge_agg`: 合并聚合
- `stddevs`: 样本标准差
- `deduplicate`: 去重
- `var`: 总体方差
- `vars`: 样本方差

### 分析函数
- `lag`: 滞后函数
- `latest`: 最新值
- `changed_col`: 变化列
- `had_changed`: 是否变化

## 自定义函数示例

参考 `custom_example.go` 文件中的示例：
- `CustomProductFunction`: 乘积聚合函数
- `CustomGeometricMeanFunction`: 几何平均聚合函数
- `CustomMovingAverageFunction`: 移动平均分析函数
