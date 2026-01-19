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

### SQRT - 平方根函数
**语法**: `sqrt(number)`  
**描述**: 返回数值的平方根。  

### POWER - 幂函数
**语法**: `power(base, exponent)`  
**描述**: 返回底数的指定次幂。  
 
### CEILING - 向上取整函数
**语法**: `ceiling(number)`  
**描述**: 返回大于或等于指定数值的最小整数。  

### FLOOR - 向下取整函数
**语法**: `floor(number)`  
**描述**: 返回小于或等于指定数值的最大整数。  
 
### ROUND - 四舍五入函数
**语法**: `round(number, [precision])`  
**描述**: 将数值四舍五入到指定的小数位数。  
 
### MOD - 取模函数
**语法**: `mod(dividend, divisor)`  
**描述**: 返回除法运算的余数。  
 
### RAND - 随机数函数
**语法**: `rand()`  
**描述**: 返回0到1之间的随机数。  

### SIGN - 符号函数
**语法**: `sign(number)`  
**描述**: 返回数值的符号（-1、0或1）。  
 
### 三角函数

#### SIN - 正弦函数
**语法**: `sin(number)`  
**描述**: 返回角度的正弦值（弧度制）。  

#### COS - 余弦函数
**语法**: `cos(number)`  
**描述**: 返回角度的余弦值（弧度制）。  
 
#### TAN - 正切函数
**语法**: `tan(number)`  
**描述**: 返回角度的正切值（弧度制）。  
 
#### ASIN - 反正弦函数
**语法**: `asin(number)`  
**描述**: 返回数值的反正弦值（弧度制）。  
 
#### ACOS - 反余弦函数
**语法**: `acos(number)`  
**描述**: 返回数值的反余弦值（弧度制）。  
 
#### ATAN - 反正切函数
**语法**: `atan(number)`  
**描述**: 返回数值的反正切值（弧度制）。  
 
#### ATAN2 - 双参数反正切函数
**语法**: `atan2(y, x)`  
**描述**: 返回y/x的反正切值（弧度制）。  
 
### 双曲函数

#### SINH - 双曲正弦函数
**语法**: `sinh(number)`  
**描述**: 返回数值的双曲正弦值。  
 
#### COSH - 双曲余弦函数
**语法**: `cosh(number)`  
**描述**: 返回数值的双曲余弦值。  
 
#### TANH - 双曲正切函数
**语法**: `tanh(number)`  
**描述**: 返回数值的双曲正切值。  
 
### 对数和指数函数

#### EXP - 指数函数
**语法**: `exp(number)`  
**描述**: 返回e的指定次幂。  
 
#### LN - 自然对数函数
**语法**: `ln(number)`  
**描述**: 返回数值的自然对数。  
 
#### LOG - 对数函数
**语法**: `log(base, number)`  
**描述**: 返回指定底数的对数。  
 
#### LOG10 - 常用对数函数
**语法**: `log10(number)`  
**描述**: 返回数值的常用对数（以10为底）。  
 
#### LOG2 - 二进制对数函数
**语法**: `log2(number)`  
**描述**: 返回数值的二进制对数（以2为底）。  
 
### 位运算函数

#### BIT_AND - 位与函数
**语法**: `bit_and(number1, number2)`  
**描述**: 对两个整数执行位与运算。  
 
#### BIT_OR - 位或函数
**语法**: `bit_or(number1, number2)`  
**描述**: 对两个整数执行位或运算。  
 
#### BIT_XOR - 位异或函数
**语法**: `bit_xor(number1, number2)`  
**描述**: 对两个整数执行位异或运算。  
 
#### BIT_NOT - 位非函数
**语法**: `bit_not(number)`  
**描述**: 对整数执行位非运算。  
 
## 📝 字符串函数

字符串函数用于文本处理。

### UPPER - 转大写函数
**语法**: `upper(str)`  
**描述**: 将字符串转换为大写。  
 
### LOWER - 转小写函数
**语法**: `lower(str)`  
**描述**: 将字符串转换为小写。  
 
### CONCAT - 字符串连接函数
**语法**: `concat(str1, str2, ...)`  
**描述**: 连接多个字符串。  
 
### LENGTH - 字符串长度函数
**语法**: `length(str)`  
**描述**: 返回字符串的长度。  
 
### SUBSTRING - 子字符串函数
**语法**: `substring(str, start, [length])`  
**描述**: 从字符串中提取子字符串。  
 
### TRIM - 去除空格函数
**语法**: `trim(str)`  
**描述**: 去除字符串两端的空格。  
 
### LTRIM - 去除左侧空格函数
**语法**: `ltrim(str)`  
**描述**: 去除字符串左侧的空格。  
 
### RTRIM - 去除右侧空格函数
**语法**: `rtrim(str)`  
**描述**: 去除字符串右侧的空格。  

### FORMAT - 格式化函数
**语法**: `format(format_str, ...)`  
**描述**: 按照指定格式格式化字符串。  

### ENDSWITH - 结尾检查函数
**语法**: `endswith(str, suffix)`  
**描述**: 检查字符串是否以指定后缀结尾。  

### STARTSWITH - 开头检查函数
**语法**: `startswith(str, prefix)`  
**描述**: 检查字符串是否以指定前缀开头。  

### INDEXOF - 查找位置函数
**语法**: `indexof(str, substring)`  
**描述**: 返回子字符串在字符串中的位置。  
 
### REPLACE - 替换函数
**语法**: `replace(str, old_str, new_str)`  
**描述**: 替换字符串中的指定内容。  

### SPLIT - 分割函数
**语法**: `split(str, delimiter)`  
**描述**: 按照分隔符分割字符串。  
 
### LPAD - 左填充函数
**语法**: `lpad(str, length, pad_str)`  
**描述**: 在字符串左侧填充字符到指定长度。  
 
### RPAD - 右填充函数
**语法**: `rpad(str, length, pad_str)`  
**描述**: 在字符串右侧填充字符到指定长度。  
 
### 正则表达式函数

#### REGEXP_MATCHES - 正则匹配函数
**语法**: `regexp_matches(str, pattern)`  
**描述**: 检查字符串是否匹配正则表达式。  

#### REGEXP_REPLACE - 正则替换函数
**语法**: `regexp_replace(str, pattern, replacement)`  
**描述**: 使用正则表达式替换字符串内容。  
 
#### REGEXP_SUBSTRING - 正则提取函数
**语法**: `regexp_substring(str, pattern)`  
**描述**: 使用正则表达式提取字符串内容。  

## 🔄 类型转换函数

类型转换函数用于数据类型转换。

### CAST - 类型转换函数
**语法**: `cast(value as type)`  
**描述**: 将值转换为指定类型。  
 
### HEX2DEC - 十六进制转十进制函数
**语法**: `hex2dec(hex_str)`  
**描述**: 将十六进制字符串转换为十进制数。  
 
### DEC2HEX - 十进制转十六进制函数
**语法**: `dec2hex(number)`  
**描述**: 将十进制数转换为十六进制字符串。  

### ENCODE - 编码函数
**语法**: `encode(str, encoding)`  
**描述**: 按照指定编码方式编码字符串。  
 
### DECODE - 解码函数
**语法**: `decode(str, encoding)`  
**描述**: 按照指定编码方式解码字符串。  
 
### CONVERT_TZ - 时区转换函数
**语法**: `convert_tz(datetime, from_tz, to_tz)`  
**描述**: 将日期时间从一个时区转换到另一个时区。  

### TO_SECONDS - 转换为秒函数
**语法**: `to_seconds(datetime)`  
**描述**: 将日期时间转换为秒数。  

### CHR - 字符函数
**语法**: `chr(number)`  
**描述**: 将ASCII码转换为字符。  
 
### TRUNC - 截断函数
**语法**: `trunc(number, [precision])`  
**描述**: 截断数值到指定精度。  
 
### URL_ENCODE - URL编码函数
**语法**: `url_encode(str)`  
**描述**: 对字符串进行URL编码。  
 
### URL_DECODE - URL解码函数
**语法**: `url_decode(str)`  
**描述**: 对字符串进行URL解码。  
 
## ⏰ 时间日期函数

时间日期函数用于处理时间和日期数据。

### NOW - 当前时间函数
**语法**: `now()`  
**描述**: 返回当前的日期和时间。  
 
### CURRENT_TIME - 当前时间函数
**语法**: `current_time()`  
**描述**: 返回当前时间。  
 
### CURRENT_DATE - 当前日期函数
**语法**: `current_date()`  
**描述**: 返回当前日期。  
 
## 🔗 JSON函数

JSON函数用于处理JSON数据。

### TO_JSON - 转换为JSON函数
**语法**: `to_json(value)`  
**描述**: 将值转换为JSON字符串。  
 
### FROM_JSON - 从JSON解析函数
**语法**: `from_json(json_str)`  
**描述**: 从JSON字符串解析值。  
 
### JSON_EXTRACT - JSON提取函数
**语法**: `json_extract(json_source, path)`  
**描述**: 从JSON字符串、Map或Array中提取指定路径的值。支持嵌套对象和数组索引。

**参数**:
- `json_source`: 输入数据，可以是JSON格式字符串，也可以是Map或Array类型对象
- `path`: 提取路径，支持 `.` 访问字段，`[]` 访问数组索引或Map Key

**示例**:
```sql
-- 提取基本字段
json_extract('{"name": "Alice"}', 'name') -- 返回 "Alice"
json_extract('{"name": "Alice"}', '$.name') -- 返回 "Alice"

-- 提取嵌套字段
json_extract('{"user": {"address": {"city": "New York"}}}', 'user.address.city') -- 返回 "New York"
json_extract('{"user": {"address": {"city": "New York"}}}', '$.user.address.city') -- 返回 "New York"

-- 提取数组元素
json_extract('[10, 20, 30]', '[1]') -- 返回 20
json_extract('[10, 20, 30]', '$[1]') -- 返回 20

-- 复杂嵌套提取
json_extract('{"users": [{"name": "Alice"}, {"name": "Bob"}]}', 'users[1].name') -- 返回 "Bob"
```
 
### JSON_VALID - JSON验证函数
**语法**: `json_valid(json_str)`  
**描述**: 验证字符串是否为有效的JSON。  

### JSON_TYPE - JSON类型函数
**语法**: `json_type(json_str)`  
**描述**: 返回JSON值的类型。  

### JSON_LENGTH - JSON长度函数
**语法**: `json_length(json_str)`  
**描述**: 返回JSON数组或对象的长度。  

## 🔐 哈希函数

哈希函数用于生成数据的哈希值。

### MD5 - MD5哈希函数
**语法**: `md5(str)`  
**描述**: 生成字符串的MD5哈希值。  
 
### SHA1 - SHA1哈希函数
**语法**: `sha1(str)`  
**描述**: 生成字符串的SHA1哈希值。  

### SHA256 - SHA256哈希函数
**语法**: `sha256(str)`  
**描述**: 生成字符串的SHA256哈希值。  

### SHA512 - SHA512哈希函数
**语法**: `sha512(str)`  
**描述**: 生成字符串的SHA512哈希值。  

## 📋 数组函数

数组函数用于处理数组数据。

### ARRAY_LENGTH - 数组长度函数
**语法**: `array_length(array)`  
**描述**: 返回数组的长度。  

### ARRAY_CONTAINS - 数组包含函数
**语法**: `array_contains(array, value)`  
**描述**: 检查数组是否包含指定值。  
 
### ARRAY_POSITION - 数组位置函数
**语法**: `array_position(array, value)`  
**描述**: 返回值在数组中的位置。  

### ARRAY_REMOVE - 数组移除函数
**语法**: `array_remove(array, value)`  
**描述**: 从数组中移除指定值。  
 

### ARRAY_DISTINCT - 数组去重函数
**语法**: `array_distinct(array)`  
**描述**: 返回数组的去重结果。  
 
### ARRAY_INTERSECT - 数组交集函数
**语法**: `array_intersect(array1, array2)`  
**描述**: 返回两个数组的交集。  
 
### ARRAY_UNION - 数组并集函数
**语法**: `array_union(array1, array2)`  
**描述**: 返回两个数组的并集。  
 
### ARRAY_EXCEPT - 数组差集函数
**语法**: `array_except(array1, array2)`  
**描述**: 返回两个数组的差集。  
 
## 🔍 类型检查函数

类型检查函数用于检查数据类型。

### IS_NULL - 空值检查函数
**语法**: `is_null(value)`  
**描述**: 检查值是否为NULL。  
 
### IS_NOT_NULL - 非空值检查函数
**语法**: `is_not_null(value)`  
**描述**: 检查值是否不为NULL。  
 

### IS_NUMERIC - 数值检查函数
**语法**: `is_numeric(value)`  
**描述**: 检查值是否为数值类型。  
 
### IS_STRING - 字符串检查函数
**语法**: `is_string(value)`  
**描述**: 检查值是否为字符串类型。  
 
### IS_BOOL - 布尔值检查函数
**语法**: `is_bool(value)`  
**描述**: 检查值是否为布尔类型。  
 
### IS_ARRAY - 数组检查函数
**语法**: `is_array(value)`  
**描述**: 检查值是否为数组类型。  
 
### IS_OBJECT - 对象检查函数
**语法**: `is_object(value)`  
**描述**: 检查值是否为对象类型。  
 
## ❓ 条件函数

条件函数用于条件判断和值选择。

### IF_NULL - 空值处理函数
**语法**: `if_null(value, default_value)`  
**描述**: 如果值为NULL，返回默认值，否则返回原值。  
 
### COALESCE - 合并函数
**语法**: `coalesce(value1, value2, ...)`  
**描述**: 返回第一个非NULL值。  

### NULL_IF - 空值转换函数
**语法**: `null_if(value1, value2)`  
**描述**: 如果两个值相等，返回NULL，否则返回第一个值。  

### GREATEST - 最大值函数
**语法**: `greatest(value1, value2, ...)`  
**描述**: 返回参数中的最大值。  

### LEAST - 最小值函数
**语法**: `least(value1, value2, ...)`  
**描述**: 返回参数中的最小值。  

### CASE_WHEN - 条件选择函数
**语法**: `case_when(condition, value_if_true, value_if_false)`  
**描述**: 根据条件返回不同的值。  

## 📊 多行函数

多行函数用于处理多行数据。

### UNNEST - 展开函数
**语法**: `unnest(array)`  
**描述**: 将数组展开为多行。  
 
## 🪟 扩展窗口函数

扩展窗口函数提供更多窗口相关功能。

### ROW_NUMBER - 行号函数
**语法**: `row_number() OVER (ORDER BY col)`  
**描述**: 为结果集中的每一行分配一个唯一的行号。  
**增量计算**: ✅ 支持  

### FIRST_VALUE - 首值函数
**语法**: `first_value(col) OVER (ORDER BY col)`  
**描述**: 返回窗口中第一行的值。  
**增量计算**: ✅ 支持  

### LEAD - 前导函数
**语法**: `lead(col, offset, default_value) OVER (ORDER BY col)`  
**描述**: 返回当前行之后第N行的值。  
**增量计算**: ✅ 支持  

### NTH_VALUE - 第N个值函数
**语法**: `nth_value(col, n) OVER (ORDER BY col)`  
**描述**: 返回窗口中第N行的值。  
**增量计算**: ✅ 支持  

## 🔧 表达式函数

表达式函数用于动态表达式计算。

### EXPRESSION - 表达式函数
**语法**: `expression(expr_str)`  
**描述**: 动态计算表达式字符串。  

### EXPR - 表达式简写函数
**语法**: `expr(expr_str)`  
**描述**: expression函数的简写形式。  
 
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

## 🔧 自定义函数扩展

StreamSQL 支持自定义函数扩展，详见 `functions/custom_example.go` 中的示例。可以实现：
- 自定义聚合函数（支持增量计算）
- 自定义分析函数（支持状态管理）
- 自定义数学函数
- 自定义字符串函数

通过实现相应的接口，自定义函数可以无缝集成到 StreamSQL 的函数体系中。