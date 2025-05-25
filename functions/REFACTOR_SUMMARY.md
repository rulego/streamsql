# StreamSQL Functions 模块重构总结

## 重构目标

将所有函数计算相关的逻辑都迁移到 `functions` 模块，让 `aggregator` 模块只负责调用 `functions` 模块，简化自定义函数的扩展过程。

## 重构成果

### 1. 统一的函数管理

- **所有聚合函数和分析函数都在 `functions` 模块中实现**
- **`aggregator` 模块只保留接口定义和适配器逻辑**
- **新增自定义函数只需要在 `functions` 模块中添加，无需修改多个模块**

### 2. 支持增量计算的聚合函数

所有聚合函数都实现了 `AggregatorFunction` 接口，支持：
- `New()`: 创建新实例
- `Add(value)`: 增量添加值
- `Result()`: 获取聚合结果
- `Reset()`: 重置状态
- `Clone()`: 克隆实例

### 3. 支持状态管理的分析函数

所有分析函数都实现了 `AnalyticalFunction` 接口，支持：
- `Reset()`: 重置函数状态
- `Clone()`: 克隆函数实例
- 状态保持和历史数据管理

### 4. 自动适配器机制

- **AggregatorAdapter**: 将 functions 模块的聚合函数适配到 aggregator 模块
- **AnalyticalAdapter**: 将 functions 模块的分析函数适配到 aggregator 模块
- **AnalyticalAggregatorAdapter**: 将分析函数适配为聚合器接口

## 已实现的函数

### 聚合函数 (支持增量计算)
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

### 分析函数 (支持状态管理)
- `lag`: 滞后函数
- `latest`: 最新值
- `changed_col`: 变化列
- `had_changed`: 是否变化

### 窗口函数
- `window_start`: 窗口开始时间
- `window_end`: 窗口结束时间
- `expression`: 表达式函数

## 使用方法

### 1. 创建聚合器实例

```go
// 通过 aggregator 模块（推荐）
agg := aggregator.CreateBuiltinAggregator(aggregator.Sum)

// 直接通过 functions 模块
sumFunc := functions.NewSumFunction()
aggInstance := sumFunc.New()
```

### 2. 增量计算

```go
agg.Add(10.0)
agg.Add(20.0)
agg.Add(30.0)
result := agg.Result() // 60.0
```

### 3. 分析函数使用

```go
lagFunc := functions.NewLagFunction()
ctx := &functions.FunctionContext{
    Data: make(map[string]interface{}),
}

// 第一个值返回默认值 nil
result1, _ := lagFunc.Execute(ctx, []interface{}{10})

// 第二个值返回第一个值 10
result2, _ := lagFunc.Execute(ctx, []interface{}{20})
```

### 4. 添加自定义函数

```go
// 1. 实现聚合函数
type CustomSumFunction struct {
    *functions.BaseFunction
    sum float64
}

// 2. 实现必要的接口方法
func (f *CustomSumFunction) New() functions.AggregatorFunction { ... }
func (f *CustomSumFunction) Add(value interface{}) { ... }
func (f *CustomSumFunction) Result() interface{} { ... }
// ... 其他方法

// 3. 注册函数
functions.Register(NewCustomSumFunction())
functions.RegisterAggregatorAdapter("custom_sum")
```

## 兼容性

- **完全兼容现有的 aggregator 模块接口**
- **现有代码无需修改**
- **新的函数会优先使用 functions 模块的实现**
- **保留了原有的注册机制作为后备**

## 性能优化

- **增量计算减少重复计算开销**
- **函数注册表提供快速查找**
- **适配器模式保持接口兼容性**
- **状态管理支持复杂分析场景**

## 测试覆盖

所有重构后的功能都有完整的测试覆盖：
- `TestFunctionsAggregatorIntegration`: 聚合函数集成测试
- `TestAnalyticalFunctionsIntegration`: 分析函数集成测试
- `TestComplexAggregators`: 复杂聚合器测试
- `TestWindowFunctions`: 窗口函数测试
- `TestAdapterFunctions`: 适配器功能测试

## 扩展建议

1. **SQL 解析器调整**: 在解析聚合函数时，优先查找 functions 模块中的注册函数
2. **动态函数发现**: 支持运行时动态加载函数
3. **函数组合**: 支持函数的组合和链式调用
4. **性能监控**: 添加函数执行性能监控和优化
5. **更多内置函数**: 基于新的架构添加更多统计和分析函数

## 文件结构

```
functions/
├── aggregator_interface.go      # 聚合器和分析函数接口定义
├── aggregator_adapter.go        # 适配器实现
├── analytical_aggregator_adapter.go # 分析函数聚合器适配器
├── functions_aggregation.go     # 聚合函数实现
├── functions_analytical.go      # 分析函数实现
├── functions_window.go          # 窗口函数实现
├── init.go                     # 函数注册
├── integration_test.go         # 集成测试
├── custom_example.go           # 自定义函数示例
└── README.md                   # 使用文档

aggregator/
├── builtin.go                  # 简化的聚合器接口和适配逻辑
└── analytical_aggregators.go   # 简化的分析聚合器占位符
```

这次重构成功实现了将所有函数计算逻辑统一到 `functions` 模块的目标，大大简化了自定义函数的扩展过程。 