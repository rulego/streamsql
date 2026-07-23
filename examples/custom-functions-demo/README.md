# Complete demonstration of custom functions

## Introduction

This is a complete demonstration of the StreamSQL custom function system, covering all function types and advanced usages.

## Feature Demonstration

- 🔢 **Mathematical Functions**: Distance Calculation, Temperature Conversion, Circle Area Calculation
- 📝 **String function**: JSON extract, invert string, repeat string
- 🔄 **Conversion function**: IP Address translation and byte size formatting
- 📅 **Time-Date Function**: Time formatting, time difference calculation
- 📊 **Aggregate function**: Geometric mean and mode calculations
- 🔍 **Analysis Function**: Moving Average
- 🛠️ **Function Management**: Register, query, classify, and deregister

## Operating Mode

```bash
cd examples/custom-functions-demo
go run main.go
```

## Code Highlights

### 1. Full function type coverage
```go
// Mathematical function: distance calculation
functions.RegisterCustomFunction("distance", functions.TypeMath, ...)

// String function: JSON extraction  
functions.RegisterCustomFunction("json_extract", functions.TypeString, ...)

// Conversion function: IP conversion
functions.RegisterCustomFunction("ip_to_int", functions.TypeConversion, ...)
```

### 2. Custom aggregation functions
```go
type GeometricMeanFunction struct {
    *functions.BaseFunction
}

// Used together with an aggregator
aggregator.Register("geometric_mean", func() aggregator.AggregatorFunction {
    return &GeometricMeanAggregator{}
})
```

### 3. Complex SQL queries
```sql
SELECT 
    device,
    AVG(distance(x1, y1, x2, y2)) as avg_distance,
    json_extract(metadata, 'version') as version,
    format_bytes(memory_usage) as formatted_memory
FROM stream 
GROUP BY device, TumblingWindow('1s')
```

## Demonstration Process

1. **Function registration stage** - Register various types of functions
2. **SQL Testing Phase** - Test functions in different modes
3. **Management Feature Demonstration** - Showcase function discovery and management features

## Applicable Scenarios

- 🏢 **Enterprise-level application**: Understand the full range of features
- 🔬 **Functional Verification**: Test complex function combinations
- **Learning Reference**: Best practices and usage patterns 
