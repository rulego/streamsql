# StreamSQL 函数系统整合指南

本文档说明 StreamSQL 如何整合自定义函数系统，以提供更强大和丰富的表达式计算能力，包括强大的 CASE 条件表达式支持。

## 🏗️ 架构概述

### 双引擎架构
StreamSQL 现在支持两套表达式引擎：

1. **自定义 expr 引擎** (`expr/expression.go`)
   - 专门针对数值计算优化
   - 支持基本数学运算和函数
   - 轻量级，高性能

2. **expr-lang/expr 引擎** 
   - 功能强大的通用表达式语言
   - 支持复杂数据类型（数组、对象、字符串等）
   - 丰富的内置函数库

### 桥接系统
`functions/expr_bridge.go` 提供了统一的接口，自动选择最合适的引擎并整合两套函数系统。

### 条件表达式系统
StreamSQL 内置了强大的 CASE 表达式支持，能够智能选择表达式引擎：
- **简单条件** → 自定义 expr 引擎（高性能）
- **复杂嵌套** → expr-lang/expr 引擎（功能完整）

## 📚 可用函数

### StreamSQL 内置函数

#### 数学函数 (TypeMath)
| 函数            | 描述     | 示例                     |
|---------------|--------|------------------------|
| `abs(x)`      | 绝对值    | `abs(-5)` → `5`        |
| `sqrt(x)`     | 平方根    | `sqrt(16)` → `4`       |
| `acos(x)`     | 反余弦    | `acos(0.5)` → `1.047`  |
| `asin(x)`     | 反正弦    | `asin(0.5)` → `0.524`  |
| `atan(x)`     | 反正切    | `atan(1)` → `0.785`    |
| `atan2(y,x)`  | 双参数反正切 | `atan2(1,1)` → `0.785` |
| `bitand(a,b)` | 按位与    | `bitand(5,3)` → `1`    |
| `bitor(a,b)`  | 按位或    | `bitor(5,3)` → `7`     |
| `bitxor(a,b)` | 按位异或   | `bitxor(5,3)` → `6`    |
| `bitnot(x)`   | 按位非    | `bitnot(5)` → `-6`     |
| `ceiling(x)`  | 向上取整   | `ceiling(3.2)` → `4`   |
| `cos(x)`      | 余弦     | `cos(0)` → `1`         |
| `cosh(x)`     | 双曲余弦   | `cosh(0)` → `1`        |
| `exp(x)`      | e的x次幂  | `exp(1)` → `2.718`     |
| `floor(x)`    | 向下取整   | `floor(3.8)` → `3`     |
| `ln(x)`       | 自然对数   | `ln(2.718)` → `1`      |
| `power(x,y)`  | x的y次幂  | `power(2,3)` → `8`     |

#### 字符串函数 (TypeString)
| 函数                  | 描述    | 示例                                              |
|---------------------|-------|-------------------------------------------------|
| `concat(s1,s2,...)` | 字符串连接 | `concat("hello"," ","world")` → `"hello world"` |
| `length(s)`         | 字符串长度 | `length("hello")` → `5`                         |
| `upper(s)`          | 转大写   | `upper("hello")` → `"HELLO"`                    |
| `lower(s)`          | 转小写   | `lower("HELLO")` → `"hello"`                    |

#### 转换函数 (TypeConversion)
| 函数                     | 描述       | 示例                                         |
|------------------------|----------|--------------------------------------------|
| `cast(value, type)`    | 类型转换     | `cast("123", "int64")` → `123`             |
| `hex2dec(hex)`         | 十六进制转十进制 | `hex2dec("ff")` → `255`                    |
| `dec2hex(num)`         | 十进制转十六进制 | `dec2hex(255)` → `"ff"`                    |
| `encode(data, format)` | 编码       | `encode("hello", "base64")` → `"aGVsbG8="` |
| `decode(data, format)` | 解码       | `decode("aGVsbG8=", "base64")` → `"hello"` |

#### 时间日期函数 (TypeDateTime)
| 函数               | 描述               | 示例                                |
|------------------|------------------|-----------------------------------|
| `now()`          | 当前时间戳            | `now()` → `1640995200`            |
| `current_time()` | 当前时间(HH:MM:SS)   | `current_time()` → `"14:30:25"`   |
| `current_date()` | 当前日期(YYYY-MM-DD) | `current_date()` → `"2025-01-01"` |

#### 聚合函数 (TypeAggregation)
| 函数            | 描述  | 示例                        |
|---------------|-----|---------------------------|
| `sum(...)`    | 求和  | `sum(1,2,3)` → `6`        |
| `avg(...)`    | 平均值 | `avg(1,2,3)` → `2`        |
| `min(...)`    | 最小值 | `min(1,2,3)` → `1`        |
| `max(...)`    | 最大值 | `max(1,2,3)` → `3`        |
| `count(...)`  | 计数  | `count(1,2,3)` → `3`      |
| `stddev(...)` | 标准差 | `stddev(1,2,3)` → `0.816` |
| `median(...)` | 中位数 | `median(1,2,3)` → `2`     |

### expr-lang/expr 内置函数

#### 数学函数
| 函数         | 描述   | 示例                 |
|------------|------|--------------------|
| `abs(x)`   | 绝对值  | `abs(-5)` → `5`    |
| `ceil(x)`  | 向上取整 | `ceil(3.2)` → `4`  |
| `floor(x)` | 向下取整 | `floor(3.8)` → `3` |
| `round(x)` | 四舍五入 | `round(3.6)` → `4` |
| `max(a,b)` | 最大值  | `max(5,3)` → `5`   |
| `min(a,b)` | 最小值  | `min(5,3)` → `3`   |

#### 字符串函数
| 函数                     | 描述     | 示例                                       |
|------------------------|--------|------------------------------------------|
| `trim(s)`              | 去除首尾空格 | `trim("  hello  ")` → `"hello"`          |
| `upper(s)`             | 转大写    | `upper("hello")` → `"HELLO"`             |
| `lower(s)`             | 转小写    | `lower("HELLO")` → `"hello"`             |
| `split(s, delimiter)`  | 分割字符串  | `split("a,b,c", ",")` → `["a","b","c"]`  |
| `replace(s, old, new)` | 替换字符串  | `replace("hello", "l", "x")` → `"hexxo"` |
| `indexOf(s, sub)`      | 查找子串位置 | `indexOf("hello", "ll")` → `2`           |
| `hasPrefix(s, prefix)` | 检查前缀   | `hasPrefix("hello", "he")` → `true`      |
| `hasSuffix(s, suffix)` | 检查后缀   | `hasSuffix("hello", "lo")` → `true`      |

#### 数组/集合函数
| 函数                         | 描述        | 示例                                     |
|----------------------------|-----------|----------------------------------------|
| `all(array, predicate)`    | 所有元素满足条件  | `all([2,4,6], # % 2 == 0)` → `true`    |
| `any(array, predicate)`    | 任一元素满足条件  | `any([1,3,4], # % 2 == 0)` → `true`    |
| `filter(array, predicate)` | 过滤元素      | `filter([1,2,3,4], # > 2)` → `[3,4]`   |
| `map(array, expression)`   | 转换元素      | `map([1,2,3], # * 2)` → `[2,4,6]`      |
| `find(array, predicate)`   | 查找元素      | `find([1,2,3], # > 2)` → `3`           |
| `count(array, predicate)`  | 计数满足条件的元素 | `count([1,2,3,4], # > 2)` → `2`        |
| `concat(array1, array2)`   | 连接数组      | `concat([1,2], [3,4])` → `[1,2,3,4]`   |
| `flatten(array)`           | 展平数组      | `flatten([[1,2],[3,4]])` → `[1,2,3,4]` |
| `len(value)`               | 获取长度      | `len([1,2,3])` → `3`                   |

#### 时间函数
| 函数            | 描述    | 示例                            |
|---------------|-------|-------------------------------|
| `now()`       | 当前时间  | `now()` → `时间对象`              |
| `duration(s)` | 解析时间段 | `duration("1h30m")` → `时间段对象` |
| `date(s)`     | 解析日期  | `date("2023-12-01")` → `日期对象` |

#### 类型转换函数
| 函数 | 描述 | 示例 |
|------|------|------|
| `int(x)` | 转整数 | `int("123")` → `123` |
| `float(x)` | 转浮点数 | `float("123.45")` → `123.45` |
| `string(x)` | 转字符串 | `string(123)` → `"123"` |
| `type(x)` | 获取类型 | `type(123)` → `"int"` |

#### JSON/编码函数
| 函数              | 描述       | 示例                                   |
|-----------------|----------|--------------------------------------|
| `toJSON(x)`     | 转JSON    | `toJSON({"a":1})` → `'{"a":1}'`      |
| `fromJSON(s)`   | 解析JSON   | `fromJSON('{"a":1}')` → `{"a":1}`    |
| `toBase64(s)`   | Base64编码 | `toBase64("hello")` → `"aGVsbG8="`   |
| `fromBase64(s)` | Base64解码 | `fromBase64("aGVsbG8=")` → `"hello"` |

## 🎯 条件表达式

### CASE表达式

StreamSQL 支持强大的 CASE 条件表达式，用于实现复杂的条件逻辑判断。

#### 语法支持

**搜索CASE表达式**：
```sql
CASE 
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ...
    ELSE default_result
END
```

**简单CASE表达式**：
```sql
CASE expression
    WHEN value1 THEN result1
    WHEN value2 THEN result2
    ...
    ELSE default_result
END
```

#### 功能特性

| 特性 | 支持状态 | 描述 |
|------|----------|------|
| **基本条件判断** | ✅ | 支持 WHEN/THEN/ELSE 逻辑 |
| **多重条件** | ✅ | 支持多个 WHEN 子句 |
| **逻辑运算符** | ✅ | 支持 AND、OR、NOT 操作 |
| **比较操作符** | ✅ | 支持 >、<、>=、<=、=、!= 等 |
| **数学函数** | ✅ | 支持 ABS、ROUND、CEIL 等函数调用 |
| **算术表达式** | ✅ | 支持 +、-、*、/ 运算 |
| **字符串操作** | ✅ | 支持字符串字面量和函数 |
| **聚合集成** | ✅ | 可在 SUM、AVG、COUNT 等聚合函数中使用 |
| **字段引用** | ✅ | 支持动态字段提取和计算 |
| **嵌套CASE** | ⚠️ | 部分支持（回退到 expr-lang） |

#### 使用示例

**设备状态分类**：
```sql
SELECT deviceId,
    CASE 
        WHEN temperature > 30 AND humidity > 70 THEN 'CRITICAL'
        WHEN temperature > 25 OR humidity > 80 THEN 'WARNING'
        ELSE 'NORMAL'
    END as alert_level
FROM stream
```

**条件聚合统计**：
```sql
SELECT deviceId,
    COUNT(CASE WHEN temperature > 25 THEN 1 END) as high_temp_count,
    SUM(CASE WHEN status = 'active' THEN temperature ELSE 0 END) as active_temp_sum,
    AVG(CASE WHEN humidity > 50 THEN humidity END) as avg_high_humidity
FROM stream
GROUP BY deviceId, TumblingWindow('5s')
```

**数学函数和算术表达式**：
```sql
SELECT deviceId,
    CASE 
        WHEN ABS(temperature - 25) < 5 THEN 'NORMAL'
        WHEN temperature * 1.8 + 32 > 100 THEN 'HOT_F'
        WHEN ROUND(temperature) = 20 THEN 'EXACT_20'
        ELSE 'OTHER'
    END as temp_classification
FROM stream
```

**状态码映射**：
```sql
SELECT deviceId,
    CASE status
        WHEN 'active' THEN 1
        WHEN 'inactive' THEN 0
        WHEN 'maintenance' THEN -1
        ELSE -999
    END as status_code
FROM stream
```

#### 表达式引擎选择

CASE表达式的处理遵循以下规则：

1. **简单条件** → 使用自定义 expr 引擎（高性能）
2. **嵌套CASE或复杂表达式** → 自动回退到 expr-lang/expr（功能完整）
3. **混合函数调用** → 智能选择最合适的引擎

#### 性能优化

- **条件顺序**：将最常见的条件放在前面
- **函数调用**：避免在条件中重复调用相同函数
- **类型一致性**：保持THEN子句返回相同类型以避免转换开销

## 🔧 使用方法

### 基本使用

```go
import (
    "github.com/rulego/streamsql/functions"
    "github.com/rulego/streamsql/utils/cast"
)

// 直接使用桥接器评估表达式
result, err := functions.EvaluateWithBridge("abs(-5) + len([1,2,3])", map[string]interface{}{})
// result: 8 (5 + 3)

// CASE表达式示例
caseResult, err := functions.EvaluateWithBridge(
    "CASE WHEN temperature > 30 THEN 'HOT' ELSE 'NORMAL' END", 
    map[string]interface{}{"temperature": 35.0})
// caseResult: "HOT"
```

### 在 SQL 查询中使用

```sql
-- 使用 StreamSQL 函数
SELECT device, abs(temperature - 20) as deviation 
FROM stream;

-- 使用 expr-lang 函数
SELECT device, filter(measurements, # > 10) as high_values
FROM stream;

-- 混合使用
SELECT device, encode(concat(device, "_", string(now())), "base64") as device_id
FROM stream;
```

### 表达式引擎选择

表达式引擎会自动选择：

1. **简单数值表达式** → 使用自定义 expr 引擎（更快）
2. **复杂表达式或使用高级函数** → 使用 expr-lang/expr（更强大）

### 函数冲突解决

当两个系统有同名函数时：

1. **默认优先级**：expr-lang/expr > StreamSQL
2. **访问 StreamSQL 版本**：使用 `streamsql_` 前缀，如 `streamsql_abs(-5)`
3. **明确指定**：通过函数解析器手动选择

## 🛠️ 高级用法

### 获取所有可用函数

```go
info := functions.GetAllAvailableFunctions()
streamSQLFuncs := info["streamsql"]
exprLangFuncs := info["expr-lang"]
```

### 自定义函数注册

```go
// 注册到 StreamSQL 系统
err := functions.RegisterCustomFunction("celsius_to_fahrenheit", 
    functions.TypeMath, "温度转换", "摄氏度转华氏度", 1, 1,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        celsius, _ := cast.ToFloat64E(args[0])
        return celsius*1.8 + 32, nil
    })

// 函数会自动在两个引擎中可用
```

### 表达式编译和缓存

```go
bridge := functions.GetExprBridge()

// 编译表达式（可缓存）
program, err := bridge.CompileExpressionWithStreamSQLFunctions(
    "abs(temperature - 20) > 5", 
    map[string]interface{}{"temperature": 0.0})

// 重复执行（高性能）
result, err := expr.Run(program, map[string]interface{}{"temperature": 25.5})
```

## 🔍 性能考虑

### 选择合适的引擎

1. **纯数值计算**：优先使用自定义 expr 引擎
2. **字符串/数组操作**：使用 expr-lang/expr
3. **复杂逻辑表达式**：使用 expr-lang/expr

### 优化建议

1. **预编译表达式**：对于重复使用的表达式，预编译以提高性能
2. **函数选择**：优先使用性能更好的版本
3. **数据类型**：避免不必要的类型转换

## 📝 示例

### 温度监控

```sql
SELECT 
    device,
    temperature,
    abs(temperature - 20) as deviation,
    CASE 
        WHEN temperature > 30 THEN "hot"
        WHEN temperature < 10 THEN "cold" 
        ELSE "normal"
    END as status,
    encode(concat(device, "_", current_date()), "base64") as device_key
FROM temperature_stream 
WHERE abs(temperature - 20) > 5;
```

### 智能告警系统

```sql
SELECT 
    device_id,
    timestamp,
    temperature,
    humidity,
    pressure,
    -- 多级告警判断
    CASE 
        WHEN temperature > 40 AND humidity > 80 THEN 'CRITICAL_HEAT_HUMID'
        WHEN temperature > 35 OR humidity > 90 THEN 'WARNING_HIGH'
        WHEN temperature < 5 AND pressure < 950 THEN 'CRITICAL_COLD_LOW_PRESSURE'
        WHEN ABS(temperature - 25) < 2 AND humidity BETWEEN 40 AND 60 THEN 'OPTIMAL'
        ELSE 'NORMAL'
    END as alert_level,
    -- 设备状态映射
    CASE device_status
        WHEN 'online' THEN 1
        WHEN 'offline' THEN 0
        WHEN 'maintenance' THEN -1
        ELSE -999
    END as status_code,
    -- 条件计算
    CASE 
        WHEN temperature > 0 THEN ROUND(temperature * 1.8 + 32, 1)
        ELSE NULL
    END as fahrenheit_temp
FROM sensor_stream
WHERE device_id IS NOT NULL;
```

### 条件聚合分析

```sql
SELECT 
    device_type,
    location,
    -- 条件计数
    COUNT(CASE WHEN temperature > 30 THEN 1 END) as hot_readings,
    COUNT(CASE WHEN temperature < 10 THEN 1 END) as cold_readings,
    COUNT(CASE WHEN humidity > 70 THEN 1 END) as humid_readings,
    -- 条件求和
    SUM(CASE WHEN status = 'active' THEN power_consumption ELSE 0 END) as active_power_sum,
    -- 条件平均值
    AVG(CASE WHEN temperature BETWEEN 20 AND 30 THEN temperature END) as normal_temp_avg,
    -- 复杂条件统计
    COUNT(CASE 
        WHEN temperature > 25 AND humidity < 60 AND status = 'active' 
        THEN 1 
    END) as optimal_active_count
FROM device_stream
GROUP BY device_type, location, TumblingWindow('10m')
HAVING COUNT(*) > 100;
```

### 数据处理

```sql
SELECT 
    sensor_id,
    filter(readings, # > avg(readings)) as above_average,
    map(readings, round(#, 2)) as rounded_readings,
    len(readings) as reading_count
FROM sensor_data
WHERE len(readings) > 10;
```