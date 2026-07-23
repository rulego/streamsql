# Simple custom function example

## Introduction

This example demonstrates how to register and use simple custom functions using StreamSQL's plugin-based custom function system.

## Feature Demonstration

- ✅ Mathematical functions: square calculation, Fahrenheit to Celsius, circle area calculation
- ✅ Direct SQL query mode and aggregate query mode
- ✅ Function management functions: query, classification, statistics

## Operating Mode

```bash
cd examples/simple-custom-functions
go run main.go
```

## Code Highlights

### 1. Simple function registration
```go
functions.RegisterCustomFunction(
    "square",               // Function name
    functions.TypeMath,     // Function type
    "数学函数",             // Classification
    "计算平方",             // Description
    1, 1,                  // Number of parameters
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        val, _ := functions.ConvertToFloat64(args[0])
        return val * val, nil
    },
)
```

### 2. SQL to use directly
```sql
SELECT square(value) as squared_value FROM stream
```

### 3. Aggregated queries
```sql
SELECT AVG(square(value)) as avg_squared FROM stream GROUP BY device, TumblingWindow('1s')
```

## Applicable Scenarios

- 🔰 Beginner's Introduction StreamSQL Custom Functions
- 📚 Learn the plugin-based function registration mechanism
- 🧪 Quick verification function functionality 
