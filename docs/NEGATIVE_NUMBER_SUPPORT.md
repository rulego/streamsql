# StreamSQL Negative support document

## Overview

StreamSQL Now fully supports the use of negative numbers in CASE expressions. This document summarizes the improvement status, scope of support, and usage recommendations for negative number support.

## ✅ Supported Negative Number Usage

### 1. Basic negative constants

```sql
-- CASE Negative constants in expressions
CASE WHEN temperature > 0 THEN 1 ELSE -1 END

-- Negative decimal
CASE WHEN temperature > 0 THEN 1.5 ELSE -2.5 END

-- Negative zero
CASE WHEN temperature = -0 THEN 1 ELSE 0 END
```

### 2. Compare the negative number after the operator

```sql
-- After comparing the operator, just follow the negative number
CASE WHEN temperature < -10 THEN 'FREEZING' ELSE 'NORMAL' END
CASE WHEN temperature >= -5.5 THEN 'ABOVE' ELSE 'BELOW' END
CASE WHEN temperature > -20 THEN 'WARM' ELSE 'COLD' END
```

### 3. A negative number in a simple CASE expression

```sql
-- In simple CASE, negative numbers are used as matching values
CASE temperature 
  WHEN -10 THEN 'FROZEN'
  WHEN -5 THEN 'COLD'
  WHEN 0 THEN 'ZERO'
  ELSE 'OTHER'
END
```

### 4. Negative numbers in arithmetic expressions

```sql
-- Negative number operations in parentheses
CASE WHEN temperature + (-10) > 0 THEN 1 ELSE 0 END
CASE WHEN (temperature * -1) > 10 THEN 1 ELSE 0 END
```

## ⚠️ Partial support or limitations

### 1. Negative number expressions in function parameters

```sql
-- Currently not fully supported: negative number variables in function parameters
CASE WHEN ABS(-temperature) > 10 THEN 1 ELSE 0 END  -- ❌ 

-- Recommended alternative: use parentheses or calculate first
CASE WHEN ABS(temperature * -1) > 10 THEN 1 ELSE 0 END  -- ✅
```

### 2. BETWEEN The range of negative numbers in a statement

```sql
-- Currently not supported: BETWEEN with negative number combinations
CASE WHEN temperature BETWEEN -20 AND -10 THEN 1 ELSE 0 END  -- ❌

-- Recommended alternative: Use comparison operators
CASE WHEN temperature >= -20 AND temperature <= -10 THEN 1 ELSE 0 END  -- ✅
```

### 3. Spaces in SQL separate negative numbers

```sql
-- Avoid using space-separated negative numbers in SQL
SELECT CASE WHEN temperature < - 10 THEN 'COLD' END  -- ❌ Analyze the problem

-- Recommended method: tightly connect or use parentheses
SELECT CASE WHEN temperature < -10 THEN 'COLD' END   -- ✅
SELECT CASE WHEN temperature < (-10) THEN 'COLD' END -- ✅
```

## 🔧 Technical Implementation

### Lexical analyzer enhancement

1. **Intelligent Negative Number Recognition**:
   - Identify negative numbers after comparison operators (`<`, `>`, `<=`, `>=`, `==`, `!=`)
   - Supports negative numbers after logical operators (`AND`, `OR`)
   - Supports negative numbers after CASE keywords (`WHEN`, `THEN`, `ELSE`)

2. **Continuous Operator Check Optimization**:
   - Allows legitimate combinations of comparison operators followed by negative numbers
   - Intelligently distinguishes between negative and minus operators

3. **Spaces**:
   - Properly handle negative number markers separated by spaces
   - Improved token process to support various negative number formats

### Expression Evaluation Enhancement

1. **Negative Number Constants Analysis**: Fully supports negative integers and negative decimals
2. **Type Conversion**: Correctly handle numerical conversion of negative numbers
3. **NULL Value Processing**: Correct interaction between negative and NULL values

## 📊 Test coverage

### Expression-level testing

- ✅ Negative constants in THEN/ELSE
- ✅ Negative constants in WHEN condition  
- ✅ Negative decimal support
- ✅ Negative numbers in arithmetic expressions
- ✅ Negative numbers in simple CASE
- ✅ Negative zero processing

### SQL Integration Testing

- ✅ Negative number support in full SQL statements
- ✅ Negative expressions in non-aggregated queries
- ✅ Handling negative numbers in aggregated queries

## 🎯 Usage Recommendations

### 1. Recommended ways to write negative numbers

```sql
-- ✅ Recommendation: Tightly connected negative numbers
CASE WHEN temperature < -10 THEN 'FREEZING' END

-- ✅ Recommendation: Negative numbers enclosed in parentheses (safest)
CASE WHEN temperature < (-10) THEN 'FREEZING' END

-- ✅ Recommended: Negative decimals
CASE WHEN temperature < -10.5 THEN 'FREEZING' END
```

### 2. Avoid writing it

```sql
-- ❌ Avoid: negative numbers separated by spaces
CASE WHEN temperature < - 10 THEN 'FREEZING' END

-- ❌ Avoid: Complex negative number expressions in functions
CASE WHEN ABS(-temperature) > 10 THEN 1 END
```

### 3. Best practices

1. **Use parentheses**: When negative number analysis is uncertain, always use parentheses to enclose negative numbers
2. **Avoid spaces**: Do not add spaces between the negative sign and the number
3. **Test and Verify**: Thoroughly test complex expressions containing negative numbers
4. **version compatible with**: Make sure the StreamSQL version you use supports the required negative number feature

## 🚀 Future Improvement Plans

1. **Fully supports negative number expressions** in function parameters
2. **supports** of negative numbers in BETWEEN statements
3. **Improvements SQL** parsers for spaces to separate negative numbers
4. **Expand negative numbers to support more mathematical and string function**

## Example code

```go
package main

import (
    "fmt"
    "github.com/rulego/streamsql"
)

func main() {
    // Create StreamSQL instances
    sql := streamsql.New()
    defer sql.Stop()

    // SQL queries containing negative numbers
    query := `
        SELECT deviceId,
               temperature,
               CASE 
                 WHEN temperature < -10 THEN 'FREEZING'
                 WHEN temperature < 0 THEN 'COLD'
                 WHEN temperature = 0 THEN 'ZERO'
                 ELSE 'POSITIVE'
               END as temp_category,
               CASE 
                 WHEN temperature > 0 THEN temperature 
                 ELSE (-1.0)
               END as adjusted_temp
        FROM stream
    `

    // Execute the query
    err := sql.Execute(query)
    if err != nil {
        fmt.Printf("执行失败: %v\n", err)
        return
    }

    // Add data processor
    sql.AddSink(func(result interface{}) {
        fmt.Printf("结果: %+v\n", result)
    })

    // Add test data
    testData := []map[string]interface{}{
        {"deviceId": "sensor1", "temperature": -15.0},
        {"deviceId": "sensor2", "temperature": -5.0},
        {"deviceId": "sensor3", "temperature": 0.0},
        {"deviceId": "sensor4", "temperature": 10.0},
    }

    for _, data := range testData {
        sql.AddData(data)
    }
}
```

---

**Last Updated**: 2025-06-17  
**Version**: StreamSQL v0.x  
**Author**: StreamSQL Development Team 
