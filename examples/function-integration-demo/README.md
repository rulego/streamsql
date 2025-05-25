# 函数集成演示

## 简介

展示自定义函数与StreamSQL各种特性的集成使用，包括窗口聚合、表达式计算、条件过滤等。

## 功能演示

- 🪟 **窗口集成**：自定义函数在不同窗口类型中的使用
- 🧮 **表达式集成**：函数与算术表达式的组合使用
- 🔍 **条件集成**：在WHERE、HAVING子句中使用自定义函数
- 📊 **聚合集成**：自定义函数与内置聚合函数的协同工作

## 运行方式

```bash
cd examples/function-integration-demo
go run main.go
```

## 代码亮点

### 1. 窗口函数集成
```sql
SELECT 
    device,
    AVG(custom_calc(temperature, pressure)) as avg_result,
    window_start() as start_time
FROM stream 
GROUP BY device, SlidingWindow('30s', '10s')
```

### 2. 复杂表达式集成
```sql
SELECT 
    device,
    custom_function(value * 1.8 + 32) as processed_value,
    SUM(another_function(field1, field2)) as total
FROM stream
GROUP BY device
```

### 3. 条件过滤集成
```sql
SELECT device, AVG(temperature) 
FROM stream 
WHERE custom_validator(status) = true
HAVING custom_threshold(AVG(temperature)) > 0
```

## 演示场景

1. **传感器数据处理** - 温度、湿度、压力的综合计算
2. **业务指标计算** - 自定义评分和分级函数
3. **数据清洗** - 自定义验证和转换函数
4. **实时监控** - 阈值检查和告警函数

## 适用场景

- 🏭 **工业物联网**：复杂传感器数据处理
- 💼 **业务分析**：自定义业务逻辑计算
- 🔧 **系统集成**：已有函数库的整合使用 