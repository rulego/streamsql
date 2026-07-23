# Function integration demonstration

## Introduction

Demonstrates the integration of custom functions with various StreamSQL features, including window aggregation, expression calculation, and conditional filtering.

## Feature Demonstration

- 🪟 **Window Integration**: Custom functions for use in different window types
- 🧮 **Expression Integration**: Combined use of functions and arithmetic expressions
- 🔍 **Conditional Integration**: Use custom functions in WHERE and HAVING clauses
- 📊 **Aggregation Integration**: Collaboration between custom functions and built-in aggregate functions

## Operating Mode

```bash
cd examples/function-integration-demo
go run main.go
```

## Code Highlights

### 1. Window function integration
```sql
SELECT 
    device,
    AVG(custom_calc(temperature, pressure)) as avg_result,
    window_start() as start_time
FROM stream 
GROUP BY device, SlidingWindow('30s', '10s')
```

### 2. Complex expression integration
```sql
SELECT 
    device,
    custom_function(value * 1.8 + 32) as processed_value,
    SUM(another_function(field1, field2)) as total
FROM stream
GROUP BY device
```

### 3. Conditional filtering integration
```sql
SELECT device, AVG(temperature) 
FROM stream 
WHERE custom_validator(status) = true
HAVING custom_threshold(AVG(temperature)) > 0
```

## Demonstration Scene

1. **Sensor Data Processing** - Comprehensive calculation of temperature, humidity, and pressure
2. **Business Metrics Calculation** - Custom scoring and grading functions
3. **Data Cleaning** - Custom validation and transformation functions
4. **Real-time monitoring** - threshold checks and alarm functions

## Applicable Scenarios

- 🏭 **Industrial IoT**: Processing of complex sensor data
- 💼 **Business Analysis**: Custom business logic calculations
- 🔧 **System Integration**: Integration and use of existing libraries 
