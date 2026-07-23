# StreamSQL Functional System Integration Guide

This document explains how StreamSQL integrates custom function systems to provide more powerful and rich expression computing capabilities, including robust CASE conditional expression support.

## 🏗️ Architecture Overview

### Dual-Engine Architecture
StreamSQL Now supports two sets of expression engines:

1. **Custom expr Engine** (`expr/expression.go`)
   - Optimized specifically for numerical calculations
   - Supports basic mathematical operations and functions
   - Lightweight, high performance

2. **expr-lang/expr Engine** 
   - A powerful general-purpose expression language
   - Supports complex data types (arrays, objects, strings, etc.)
   - Rich built-in function library

### Bridging Systems
`functions/expr_bridge.go` provides a unified interface, automatically selects the most suitable engine, and integrates two functional systems.

### Conditional Expression System
StreamSQL Built-in powerful CASE expression support, allowing intelligent selection of expression engines:
- **Simple conditions** → Custom expr engine (high performance)
- **Complex nested** → expr-lang/expr engine (fully featured)

## 📚 Available functions

### StreamSQL built-in functions

#### Mathematical Functions (TypeMath)
| Function            | Description     | Example                     |
|---------------|--------|------------------------|
| `abs(x)`      | Absolute value    | `abs(-5)` → `5`        |
| `sqrt(x)`     | square root    | `sqrt(16)` → `4`       |
| `acos(x)`     | Anticosine    | `acos(0.5)` → `1.047`  |
| `asin(x)`     | Opposite sine    | `asin(0.5)` → `0.524`  |
| `atan(x)`     | Arctangent         | `atan(1)` → `0.785`    |
| `atan2(y,x)`  | Dual-parameter arctangent | `atan2(1,1)` → `0.785` |
| `bitand(a,b)` | By bit and    | `bitand(5,3)` → `1`    |
| `bitor(a,b)`  | Bitwise or    | `bitor(5,3)` → `7`     |
| `bitxor(a,b)` | By bit or   | `bitxor(5,3)` → `6`    |
| `bitnot(x)`   | By bit, not    | `bitnot(5)` → `-6`     |
| `ceiling(x)`  | Round up   | `ceiling(3.2)` → `4`   |
| `cos(x)`      | Cosine     | `cos(0)` → `1`         |
| `cosh(x)`     | Hyperbolic cosine   | `cosh(0)` → `1`        |
| `exp(x)`      | e x power  | `exp(1)` → `2.718`     |
| `floor(x)`    | Round down   | `floor(3.8)` → `3`     |
| `ln(x)`       | Natural logarithm   | `ln(2.718)` → `1`      |
| `power(x,y)`  | x y power  | `power(2,3)` → `8`     |

#### String Function (TypeString)
| Function                  | Description    | Example                                              |
|---------------------|-------|-------------------------------------------------|
| `concat(s1,s2,.)` | String concatenation | `concat("hello"," ","world")` → `"hello world"` |
| `length(s)`         | String length | `length("hello")` → `5`                         |
| `upper(s)`          | Capitalize   | `upper("hello")` → `"HELLO"`                    |
| `lower(s)`          | Lowercase   | `lower("HELLO")` → `"hello"`                    |

#### Transformation Function (TypeConversion)
| Function                     | Description       | Example                                         |
|------------------------|----------|--------------------------------------------|
| `cast(value, type)`    | Type conversion     | `cast("123", "int64")` → `123`             |
| `hex2dec(hex)`         | Hexadecimal to decimal | `hex2dec("ff")` → `255`                    |
| `dec2hex(num)`         | Decimal to hexadecimal | `dec2hex(255)` → `"ff"`                    |
| `encode(data, format)` | Code:       | `encode("hello", "base64")` → `"aGVsbG8="` |
| `decode(data, format)` | Decoding       | `decode("aGVsbG8=", "base64")` → `"hello"` |

#### Time-Date Function (TypeDateTime)
| Function               | Description               | Example                                |
|------------------|------------------|-----------------------------------|
| `now()`          | Current timestamp            | `now()` → `1640995200`            |
| `current_time()` | Current time (HH:MM:SS)   | `current_time()` → `"14:30:25"`   |
| `current_date()` | Current date (YYYY-MM-DD) | `current_date()` → `"2025-01-01"` |

#### Aggregate Function (TypeAggregation)
| Function            | Description  | Example                        |
|---------------|-----|---------------------------|
| `sum(.)`    | Seeking peace  | `sum(1,2,3)` → `6`        |
| `avg(.)`    | Average value | `avg(1,2,3)` → `2`        |
| `min(.)`    | Minimum value | `min(1,2,3)` → `1`        |
| `max(.)`    | Maximum | `max(1,2,3)` → `3`        |
| `count(.)`  | Count  | `count(1,2,3)` → `3`      |
| `stddev(.)` | Standard deviation | `stddev(1,2,3)` → `0.816` |
| `median(.)` | Median | `median(1,2,3)` → `2`     |

### expr-lang/expr built-in functions

#### Mathematical Functions
| Function         | Description   | Example                 |
|------------|------|--------------------|
| `abs(x)`   | Absolute value  | `abs(-5)` → `5`    |
| `ceil(x)`  | Round up | `ceil(3.2)` → `4`  |
| `floor(x)` | Round down | `floor(3.8)` → `3` |
| `round(x)` | Rounding | `round(3.6)` → `4` |
| `max(a,b)` | Maximum  | `max(5,3)` → `5`   |
| `min(a,b)` | Minimum value  | `min(5,3)` → `3`   |

#### String function
| Function                     | Description     | Example                                       |
|------------------------|--------|------------------------------------------|
| `trim(s)`              | Remove the starting and ending spaces | `trim("hello")` → `"hello"`          |
| `upper(s)`             | Capitalize    | `upper("hello")` → `"HELLO"`             |
| `lower(s)`             | Lowercase    | `lower("HELLO")` → `"hello"`             |
| `split(s, delimiter)`  | Split string  | `split("a,b,c", ",")` → `["a","b","c"]`  |
| `replace(s, old, new)` | Replace string  | `replace("hello", "l", "x")` → `"hexxo"` |
| `indexOf(s, sub)`      | Find the position of the substring | `indexOf("hello", "ll")` → `2`           |
| `hasPrefix(s, prefix)` | Check the prefix   | `hasPrefix("hello", "he")` → `true`      |
| `hasSuffix(s, suffix)` | Check the suffix   | `hasSuffix("hello", "lo")` → `true`      |

#### Array/Set Functions
| Function                         | Description        | Example                                     |
|----------------------------|-----------|----------------------------------------|
| `all(array, predicate)`    | All elements satisfy the condition  | `all([2,4,6], # % 2 == 0)` → `true`    |
| `any(array, predicate)`    | Any element satisfies the condition  | `any([1,3,4], # % 2 == 0)` → `true`    |
| `filter(array, predicate)` | Filter element      | `filter([1,2,3,4], # > 2)` → `[3,4]`   |
| `map(array, expression)`   | Transform element      | `map([1,2,3], # * 2)` → `[2,4,6]`      |
| `find(array, predicate)`   | Find element      | `find([1,2,3], # > 2)` → `3`           |
| `count(array, predicate)`  | Count elements that meet the condition | `count([1,2,3,4], # > 2)` → `2`        |
| `concat(array1, array2)`   | Join array      | `concat([1,2], [3,4])` → `[1,2,3,4]`   |
| `flatten(array)`           | Spread array      | `flatten([[1,2],[3,4]])` → `[1,2,3,4]` |
| `len(value)`               | Get length      | `len([1,2,3])` → `3`                   |

#### Time Function
| Function            | Description    | Example                            |
|---------------|-------|-------------------------------|
| `now()`       | Current time  | `now()` → `time object`              |
| `duration(s)` | Parsing time period | `duration("1h30m")` → `duration object` |
| `date(s)`     | Date of resolution  | `date("2023-12-01")` → `date object` |

#### Type conversion function
| Function | Description | Example |
|------|------|------|
| `int(x)` | Convert to integer | `int("123")` → `123` |
| `float(x)` | Floating point number | `float("123.45")` → `123.45` |
| `string(x)` | Convert to string | `string(123)` → `"123"` |
| `type(x)` | Get type | `type(123)` → `"int"` |

#### JSON/ Encoding function
| Function              | Description       | Example                                   |
|-----------------|----------|--------------------------------------|
| `toJSON(x)`     | Convert JSON    | `toJSON({"a":1})` → `'{"a":1}'`      |
| `fromJSON(s)`   | Analysis: JSON   | `fromJSON('{"a":1}')` → `{"a":1}`    |
| `toBase64(s)`   | Base64 Encoding | `toBase64("hello")` → `"aGVsbG8="`   |
| `fromBase64(s)` | Base64 Decode | `fromBase64("aGVsbG8=")` → `"hello"` |

## 🎯 Conditional expressions

### CASE Expression

StreamSQL Supports powerful CASE conditional expressions for implementing complex conditional logical judgments.

#### Syntax support

**Search CASE Expressions**:
```sql
CASE 
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ...
    ELSE default_result
END
```

**Simple CASE Expressions**:
```sql
CASE expression
    WHEN value1 THEN result1
    WHEN value2 THEN result2
    ...
    ELSE default_result
END
```

#### Functional Features

| Features | Support status | Description |
|------|----------|------|
| **Basic condition judgment** | ✅ | Supports WHEN/THEN/ELSE logic |
| **Multiple conditions** | ✅ | Supports multiple WHEN clauses |
| **Logical Operator** | ✅ | Supports AND, OR, NOT operations |
| **Comparison Operator** | ✅ | Supports >, <、> =, <=, =,!= and other |
| **Mathematical Functions** | ✅ | Supports ABS, ROUND, CEIL, and other functions to call |
| **Arithmetic Expression** | ✅ | Supports +、-、*, / operations |
| **String Operations** | ✅ | Supports string literals and functions |
| **Aggregate Integration** | ✅ | You can use |SUM, AVG, COUNT, and other aggregate functions
| **Field Reference** | ✅ | Supports dynamic field extraction and calculation |
| **Nested CASE** | ⚠️ | Partial support (rollback to expr-lang) |

#### Usage examples

**Equipment Status Classification**:
```sql
SELECT deviceId,
    CASE 
        WHEN temperature > 30 AND humidity > 70 THEN 'CRITICAL'
        WHEN temperature > 25 OR humidity > 80 THEN 'WARNING'
        ELSE 'NORMAL'
    END as alert_level
FROM stream
```

**Conditional Aggregation Statistics**:
```sql
SELECT deviceId,
    COUNT(CASE WHEN temperature > 25 THEN 1 END) as high_temp_count,
    SUM(CASE WHEN status = 'active' THEN temperature ELSE 0 END) as active_temp_sum,
    AVG(CASE WHEN humidity > 50 THEN humidity END) as avg_high_humidity
FROM stream
GROUP BY deviceId, TumblingWindow('5s')
```

**Mathematical Functions and Arithmetic Expressions**:
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

**Status Code Mapping**:
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

#### Expression Engine Selection

CASE Expressions are handled according to the following rules:

1. **Simple Condition** → Use a custom expr engine (high performance)
2. **Nested CASE or complex expressions** → automatically falls back to expr-lang/expr (full functionality)
3. **Hybrid function calls** → Intelligently select the most suitable engine

#### Performance Optimization

- **Conditional order**: Put the most common conditions first
- **Function Call**: Prevents repeated calls to the same function in conditions
- **Type consistency**: Keep THEN clauses returning the same type to avoid conversion overhead

## 🔧 How to use

### Basic usage

```go
import (
    "github.com/rulego/streamsql/functions"
    "github.com/rulego/streamsql/utils/cast"
)

// Directly use the bridge to evaluate the expression
result, err := functions.EvaluateWithBridge("abs(-5) + len([1,2,3])", map[string]interface{}{})
// result: 8 (5 + 3)

// CASE example expression
caseResult, err := functions.EvaluateWithBridge(
    "CASE WHEN temperature > 30 THEN 'HOT' ELSE 'NORMAL' END", 
    map[string]interface{}{"temperature": 35.0})
// caseResult: "HOT"
```

### Used in SQL queries

```sql
-- Use the StreamSQL function
SELECT device, abs(temperature - 20) as deviation 
FROM stream;

-- Use the expr-lang function
SELECT device, filter(measurements, # > 10) as high_values
FROM stream;

-- Mixed use
SELECT device, encode(concat(device, "_", string(now())), "base64") as device_id
FROM stream;
```

### Expression Engine Selection

The expression engine will automatically select:

1. **Simple Numeric Expressions** → Using a Custom expr Engine (Faster)
2. **complex expressions or use advanced functions** → use expr-lang/expr (more powerful)

### Function Conflict Resolution

When two systems have functions with the same name:

1. **Default priority**: expr-lang/expr > StreamSQL
2. **access StreamSQL version**: Use `streamsql_` prefixes such as `streamsql_abs(-5)`
3. **Clearly specify**: Manually select via function parser

## 🛠️ Advanced usage

### Get all available functions

```go
info := functions.GetAllAvailableFunctions()
streamSQLFuncs := info["streamsql"]
exprLangFuncs := info["expr-lang"]
```

### Custom function registration

```go
// Register with the StreamSQL system
err := functions.RegisterCustomFunction("celsius_to_fahrenheit", 
    functions.TypeMath, "温度转换", "摄氏度转华氏度", 1, 1,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        celsius, _ := cast.ToFloat64E(args[0])
        return celsius*1.8 + 32, nil
    })

// The function will automatically be available in both engines
```

### Expression Compilation and Caching

```go
bridge := functions.GetExprBridge()

// Compiled expressions (cacheable)
program, err := bridge.CompileExpressionWithStreamSQLFunctions(
    "abs(temperature - 20) > 5", 
    map[string]interface{}{"temperature": 0.0})

// Repetitive execution (high performance)
result, err := expr.Run(program, map[string]interface{}{"temperature": 25.5})
```

## 🔍 Performance considerations

### Choose the Right Engine

1. **Pure Numerical Computation**: Prioritize using custom expr engines
2. **String/Array Operations**: Use expr-lang/expr
3. **Complex Logical Expressions**: Use expr-lang/expr

### Optimization Suggestions

1. **Precompiled Expressions**: For reused expressions, precompile them to improve performance
2. **Function Select**: Prioritize the version with better performance
3. **Data Type**: Avoid unnecessary type conversions

## 📝 Example

### Temperature monitoring

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

### Intelligent Alarm System

```sql
SELECT 
    device_id,
    timestamp,
    temperature,
    humidity,
    pressure,
    -- Multi-level alarm detection
    CASE 
        WHEN temperature > 40 AND humidity > 80 THEN 'CRITICAL_HEAT_HUMID'
        WHEN temperature > 35 OR humidity > 90 THEN 'WARNING_HIGH'
        WHEN temperature < 5 AND pressure < 950 THEN 'CRITICAL_COLD_LOW_PRESSURE'
        WHEN ABS(temperature - 25) < 2 AND humidity BETWEEN 40 AND 60 THEN 'OPTIMAL'
        ELSE 'NORMAL'
    END as alert_level,
    -- Device status mapping
    CASE device_status
        WHEN 'online' THEN 1
        WHEN 'offline' THEN 0
        WHEN 'maintenance' THEN -1
        ELSE -999
    END as status_code,
    -- Condition calculation
    CASE 
        WHEN temperature > 0 THEN ROUND(temperature * 1.8 + 32, 1)
        ELSE NULL
    END as fahrenheit_temp
FROM sensor_stream
WHERE device_id IS NOT NULL;
```

### Conditional Aggregation Analysis

```sql
SELECT 
    device_type,
    location,
    -- Conditional counting
    COUNT(CASE WHEN temperature > 30 THEN 1 END) as hot_readings,
    COUNT(CASE WHEN temperature < 10 THEN 1 END) as cold_readings,
    COUNT(CASE WHEN humidity > 70 THEN 1 END) as humid_readings,
    -- Conditional sum
    SUM(CASE WHEN status = 'active' THEN power_consumption ELSE 0 END) as active_power_sum,
    -- Conditional mean
    AVG(CASE WHEN temperature BETWEEN 20 AND 30 THEN temperature END) as normal_temp_avg,
    -- Statistics under complex conditions
    COUNT(CASE 
        WHEN temperature > 25 AND humidity < 60 AND status = 'active' 
        THEN 1 
    END) as optimal_active_count
FROM device_stream
GROUP BY device_type, location, TumblingWindow('10m')
HAVING COUNT(*) > 100;
```

### Data Processing

```sql
SELECT 
    sensor_id,
    filter(readings, # > avg(readings)) as above_average,
    map(readings, round(#, 2)) as rounded_readings,
    len(readings) as reading_count
FROM sensor_data
WHERE len(readings) > 10;
```
