# StreamSQL 函数使用指南

StreamSQL 具有丰富的内置函数，可以对数据执行各种计算。所有函数都支持在流式处理环境中使用，部分函数支持增量计算以提高性能。

## 📊 聚合函数

聚合函数对一组值执行计算并返回单个值。聚合函数只能用在以下表达式中：
- SELECT 语句的 SELECT 列表（子查询或外部查询）
- HAVING 子句

### SUM - 求和函数
**语法**: `sum(col)`  
**描述**: 返回组中数值的总和。空值不参与计算。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, sum(temperature) as total_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### AVG - 平均值函数
**语法**: `avg(col)`  
**描述**: 返回组中数值的平均值。空值不参与计算。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, avg(temperature) as avg_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### COUNT - 计数函数
**语法**: `count(*)`  
**描述**: 返回组中的行数。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, count(*) as record_count 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### MIN - 最小值函数
**语法**: `min(col)`  
**描述**: 返回组中数值的最小值。空值不参与计算。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, min(temperature) as min_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### MAX - 最大值函数
**语法**: `max(col)`  
**描述**: 返回组中数值的最大值。空值不参与计算。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, max(temperature) as max_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### COLLECT - 收集函数
**语法**: `collect(col)`  
**描述**: 获取当前窗口所有消息的列值组成的数组。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, collect(temperature) as temp_values 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### LAST_VALUE - 最后值函数
**语法**: `last_value(col)`  
**描述**: 返回组中最后一行的值。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, last_value(temperature) as last_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### MERGE_AGG - 合并聚合函数
**语法**: `merge_agg(col)`  
**描述**: 将组中的值合并为单个值。对于对象类型，合并所有键值对；对于其他类型，用逗号连接。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, merge_agg(status) as all_status 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### DEDUPLICATE - 去重函数
**语法**: `deduplicate(col, false)`  
**描述**: 返回当前组去重的结果，通常用在窗口中。第二个参数指定是否返回全部结果。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, deduplicate(temperature, true) as unique_temps 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### STDDEV - 标准差函数
**语法**: `stddev(col)`  
**描述**: 返回组中所有值的总体标准差。空值不参与计算。  
**增量计算**: ✅ 支持（使用韦尔福德算法优化）  
**示例**:
```sql
SELECT device, stddev(temperature) as temp_stddev 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### STDDEVS - 样本标准差函数
**语法**: `stddevs(col)`  
**描述**: 返回组中所有值的样本标准差。空值不参与计算。  
**增量计算**: ✅ 支持（使用韦尔福德算法优化）  
**示例**:
```sql
SELECT device, stddevs(temperature) as temp_sample_stddev 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### VAR - 方差函数
**语法**: `var(col)`  
**描述**: 返回组中所有值的总体方差。空值不参与计算。  
**增量计算**: ✅ 支持（使用韦尔福德算法优化）  
**示例**:
```sql
SELECT device, var(temperature) as temp_variance 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### VARS - 样本方差函数
**语法**: `vars(col)`  
**描述**: 返回组中所有值的样本方差。空值不参与计算。  
**增量计算**: ✅ 支持（使用韦尔福德算法优化）  
**示例**:
```sql
SELECT device, vars(temperature) as temp_sample_variance 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### MEDIAN - 中位数函数
**语法**: `median(col)`  
**描述**: 返回组中所有值的中位数。空值不参与计算。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, median(temperature) as temp_median 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### PERCENTILE - 百分位数函数
**语法**: `percentile(col, 0.5)`  
**描述**: 返回组中所有值的指定百分位数。第二个参数指定百分位数的值，取值范围为 0.0 ~ 1.0。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, percentile(temperature, 0.95) as temp_p95 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

## 🔍 分析函数

分析函数用于在数据流中进行复杂的分析计算，支持状态管理和历史数据访问。

### LAG - 滞后函数
**语法**: `lag(col, offset, default_value)`  
**描述**: 返回当前行之前的第N行的值。offset指定偏移量，default_value为默认值。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, temperature, lag(temperature, 1) as prev_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### LATEST - 最新值函数
**语法**: `latest(col)`  
**描述**: 返回指定列的最新值。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, latest(temperature) as current_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### CHANGED_COL - 变化列函数
**语法**: `changed_col(row_data)`  
**描述**: 返回发生变化的列名数组。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, changed_col(*) as changed_columns 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### HAD_CHANGED - 变化检测函数
**语法**: `had_changed(col)`  
**描述**: 判断指定列的值是否发生变化，返回布尔值。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, had_changed(status) as status_changed 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

## 🪟 窗口函数

窗口函数提供窗口相关的信息。

### WINDOW_START - 窗口开始时间
**语法**: `window_start()`  
**描述**: 返回当前窗口的开始时间。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, window_start() as window_begin, avg(temperature) as avg_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

### WINDOW_END - 窗口结束时间
**语法**: `window_end()`  
**描述**: 返回当前窗口的结束时间。  
**增量计算**: ✅ 支持  
**示例**:
```sql
SELECT device, window_end() as window_finish, avg(temperature) as avg_temp 
FROM stream 
GROUP BY device, TumblingWindow('10s')
```

## 🧮 数学函数

数学函数用于数值计算。

### ABS - 绝对值函数
**语法**: `abs(number)`  
**描述**: 返回数值的绝对值。  
**增量计算**: ❌ 不支持（单值函数）  

### SQRT - 平方根函数
**语法**: `sqrt(number)`  
**描述**: 返回数值的平方根。  
**增量计算**: ❌ 不支持（单值函数）  

### POWER - 幂函数
**语法**: `power(base, exponent)`  
**描述**: 返回底数的指定次幂。  
**增量计算**: ❌ 不支持（单值函数）  

## 📝 字符串函数

字符串函数用于文本处理。

### UPPER - 转大写函数
**语法**: `upper(str)`  
**描述**: 将字符串转换为大写。  
**增量计算**: ❌ 不支持（单值函数）  

### LOWER - 转小写函数
**语法**: `lower(str)`  
**描述**: 将字符串转换为小写。  
**增量计算**: ❌ 不支持（单值函数）  

### CONCAT - 字符串连接函数
**语法**: `concat(str1, str2, ...)`  
**描述**: 连接多个字符串。  
**增量计算**: ❌ 不支持（单值函数）  

## 🔄 类型转换函数

类型转换函数用于数据类型转换。

### CAST - 类型转换函数
**语法**: `cast(value as type)`  
**描述**: 将值转换为指定类型。  
**增量计算**: ❌ 不支持（单值函数）  

## ⚡ 增量计算性能优势

支持增量计算的函数具有以下性能优势：

### 内存效率
- **传统批量计算**: 需要存储窗口内所有数据，内存使用 O(n)
- **增量计算**: 只存储必要的状态信息，内存使用 O(1) 或 O(log n)

### 计算效率
- **传统批量计算**: 每次窗口触发都重新计算所有数据，时间复杂度 O(n)
- **增量计算**: 只处理新增数据，时间复杂度 O(1)

### 实时性
- **传统批量计算**: 只能在窗口结束时输出结果
- **增量计算**: 可以实时输出中间结果

### 性能测试结果
根据我们的性能测试：
- **计算速度**: 增量计算比批量计算快 2-3 倍
- **内存使用**: 减少 99.9% 以上的内存占用
- **实时性**: 支持流式处理，实时输出中间结果

## 💡 使用建议

1. **优先使用支持增量计算的函数**: 在大数据量和高频率数据流场景下，优先选择支持增量计算的函数。

2. **合理选择窗口大小**: 窗口大小影响计算精度和性能，需要根据业务需求平衡。

3. **组合使用函数**: 可以在同一个查询中组合使用多个函数，实现复杂的分析需求。

4. **注意数据类型**: 确保输入数据类型与函数要求匹配，避免类型转换错误。

## 🔧 自定义函数扩展

StreamSQL 支持自定义函数扩展，详见 `functions/custom_example.go` 中的示例。可以实现：
- 自定义聚合函数（支持增量计算）
- 自定义分析函数（支持状态管理）
- 自定义数学函数
- 自定义字符串函数

通过实现相应的接口，自定义函数可以无缝集成到 StreamSQL 的函数体系中。 