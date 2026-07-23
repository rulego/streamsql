# StreamSQL Custom Function Development Guide

## 🚀 Overview

StreamSQL provides a powerful and flexible custom function system, supporting users in extending various types of functions according to business needs, including mathematical functions, string functions, aggregation functions, analysis functions, and more.

## 📋 Classification of function types

### Built-in function types

```go
const (
    TypeAggregation FunctionType = "aggregation"  // Aggregate function
    TypeWindow      FunctionType = "window"       // Window function  
    TypeDateTime    FunctionType = "datetime"     // Date/time function
    TypeConversion  FunctionType = "conversion"   // Conversion function
    TypeMath        FunctionType = "math"         // Mathematical functions
    TypeString      FunctionType = "string"       // String function
    TypeAnalytical  FunctionType = "analytical"   // Analytical function
    TypeCustom      FunctionType = "custom"       // User-defined functions
)
```

## 🛠️ Custom Function Implementation

### Method 1: Quick Registration (Simple Recommended Function)

```go
import "github.com/rulego/streamsql/functions"

// Register a simple mathematical function
err := functions.RegisterCustomFunction(
    "double",                    // Function name
    functions.TypeMath,          // Function type
    "数学函数",                   // Classification description
    "将数值乘以2",                // Function description
    1,                          // Minimum number of parameters
    1,                          // Maximum number of parameters
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        val, err := cast.ToFloat64E(args[0])
        if err != nil {
            return nil, err
        }
        return val * 2, nil
    },
)
```

### Method 2: Complete Struct Implementation (Recommended Complex Functions)

```go
// 1. Define the function structure
type AdvancedMathFunction struct {
    *functions.BaseFunction
    // You can add status variables
    cache map[string]interface{}
}

// 2. Implement constructors
func NewAdvancedMathFunction() *AdvancedMathFunction {
    return &AdvancedMathFunction{
        BaseFunction: functions.NewBaseFunction(
            "advanced_calc",           // Function name
            functions.TypeMath,        // Function type
            "高级数学函数",             // Classification
            "高级数学计算",             // Description
            2,                        // Minimum parameters
            3,                        // The most parameters
        ),
        cache: make(map[string]interface{}),
    }
}

// 3. Implement verification methods (optional, if special verification requirements exist)
func (f *AdvancedMathFunction) Validate(args []interface{}) error {
    if err := f.ValidateArgCount(args); err != nil {
        return err
    }
    
    // Custom verification logic
    if len(args) >= 2 {
        if _, err := cast.ToFloat64E(args[0]); err != nil {
            return fmt.Errorf("第一个参数必须是数值")
        }
        if _, err := cast.ToFloat64E(args[1]); err != nil {
            return fmt.Errorf("第二个参数必须是数值")
        }
    }
    
    return nil
}

// 4. Implement the execution method
func (f *AdvancedMathFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    a, _ := cast.ToFloat64E(args[0])
    b, _ := cast.ToFloat64E(args[1])
    
    operation := "add" // Default operation
    if len(args) > 2 {
        op, err := cast.ToStringE(args[2])
        if err == nil {
            operation = op
        }
    }
    
    switch operation {
    case "add":
        return a + b, nil
    case "multiply":
        return a * b, nil
    case "power":
        return math.Pow(a, b), nil
    default:
        return nil, fmt.Errorf("不支持的操作: %s", operation)
    }
}

// 5. Register functions
func init() {
    functions.Register(NewAdvancedMathFunction())
}
```

## 🎯 Examples of Function Type Implementations

### 1. Mathematical function example

```go
// Distance calculation function
func RegisterDistanceFunction() error {
    return functions.RegisterCustomFunction(
        "distance",
        functions.TypeMath,
        "几何数学",
        "计算两点间距离",
        4, 4,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            x1, err := cast.ToFloat64E(args[0])
            if err != nil { return nil, err }
            y1, err := cast.ToFloat64E(args[1])
            if err != nil { return nil, err }
            x2, err := cast.ToFloat64E(args[2])
            if err != nil { return nil, err }
            y2, err := cast.ToFloat64E(args[3])
            if err != nil { return nil, err }
            
            distance := math.Sqrt(math.Pow(x2-x1, 2) + math.Pow(y2-y1, 2))
            return distance, nil
        },
    )
}

// SQL usage examples:
// SELECT device, distance(lat1, lon1, lat2, lon2) as dist FROM stream
```

### 2. Example of string function

```go
// JSON extraction function
func RegisterJsonExtractFunction() error {
    return functions.RegisterCustomFunction(
        "json_extract",
        functions.TypeString,
        "JSON处理",
        "从JSON字符串中提取字段值",
        2, 2,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            jsonStr, err := cast.ToStringE(args[0])
            if err != nil { return nil, err }
            
            path, err := cast.ToStringE(args[1])
            if err != nil { return nil, err }
            
            var data map[string]interface{}
            if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
                return nil, fmt.Errorf("invalid JSON: %v", err)
            }
            
            // Simple Path Extraction (Can Be Expanded to Complex JSONPath)
            value, exists := data[path]
            if !exists {
                return nil, nil
            }
            
            return value, nil
        },
    )
}

// SQL usage examples:
// SELECT device, json_extract(metadata, 'version') as version FROM stream
```

### 3. Example of the time-date function

```go
// Time formatting function
func RegisterDateFormatFunction() error {
    return functions.RegisterCustomFunction(
        "date_format",
        functions.TypeDateTime,
        "时间格式化",
        "格式化时间戳为指定格式",
        2, 2,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            timestamp, err := cast.ToInt64E(args[0])
            if err != nil { return nil, err }
            
            format, err := cast.ToStringE(args[1])
            if err != nil { return nil, err }
            
            t := time.Unix(timestamp, 0)
            
            // Supports common formats
            switch format {
            case "YYYY-MM-DD":
                return t.Format("2006-01-02"), nil
            case "YYYY-MM-DD HH:mm:ss":
                return t.Format("2006-01-02 15:04:05"), nil
            case "RFC3339":
                return t.Format(time.RFC3339), nil
            default:
                return t.Format(format), nil
            }
        },
    )
}

// SQL usage examples:
// SELECT device, date_format(timestamp, 'YYYY-MM-DD') as date FROM stream
```

### 4. Example of a conversion function

```go
// IP address translation function
func RegisterIpToIntFunction() error {
    return functions.RegisterCustomFunction(
        "ip_to_int",
        functions.TypeConversion,
        "网络转换",
        "将IP地址转换为整数",
        1, 1,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            ipStr, err := cast.ToStringE(args[0])
            if err != nil { return nil, err }
            
            ip := net.ParseIP(ipStr)
            if ip == nil {
                return nil, fmt.Errorf("invalid IP address: %s", ipStr)
            }
            
            // Convert to IPv4
            ip = ip.To4()
            if ip == nil {
                return nil, fmt.Errorf("not an IPv4 address: %s", ipStr)
            }
            
            return int64(ip[0])<<24 + int64(ip[1])<<16 + int64(ip[2])<<8 + int64(ip[3]), nil
        },
    )
}

// SQL usage examples:
// SELECT device, ip_to_int(client_ip) as ip_int FROM stream
```

### 5. Custom aggregator function example

The aggregator function implements `AggregatorFunction` interfaces (`New` / `Add` / `Result` / `Reset` / `Clone`), registering one place with `functions. Register` — adapters automatically connect without `aggregator. Register`.

```go
import (
    "sort"

    "github.com/rulego/streamsql/functions"
    "github.com/rulego/streamsql/utils/cast"
)

// MedianAgg Fully implement AggregatorFunction
type MedianAgg struct {
    *functions.BaseFunction
    values []float64
}

func NewMedianAgg() *MedianAgg {
    return &MedianAgg{BaseFunction: functions.NewBaseFunction(
        "median_agg", functions.TypeAggregation, "统计聚合", "计算中位数", 1, -1)}
}

func (f *MedianAgg) Validate(args []any) error                            { return f.ValidateArgCount(args) }
func (f *MedianAgg) Execute(ctx *functions.FunctionContext, args []any) (any, error) {
    return nil, nil // Aggregation follows Add/Result, Execute only meets interface requirements
}
func (f *MedianAgg) New() functions.AggregatorFunction { return &MedianAgg{BaseFunction: f.BaseFunction} }
func (f *MedianAgg) Add(value any) {
    if v, err := cast.ToFloat64E(value); err == nil {
        f.values = append(f.values, v)
    }
}
func (f *MedianAgg) Result() any {
    if len(f.values) == 0 {
        return 0.0
    }
    sort.Float64s(f.values)
    mid := len(f.values) / 2
    if len(f.values)%2 == 0 {
        return (f.values[mid-1] + f.values[mid]) / 2
    }
    return f.values[mid]
}
func (f *MedianAgg) Reset()                                   { f.values = nil }
func (f *MedianAgg) Clone() functions.AggregatorFunction {
    cp := make([]float64, len(f.values))
    copy(cp, f.values)
    return &MedianAgg{BaseFunction: f.BaseFunction, values: cp}
}

func init() {
    functions.Register(NewMedianAgg())
}

// SQL usage examples:
// SELECT device, median_agg(temperature) as median_temp FROM stream GROUP BY device
```

## 📊 Function Management Features

### Check registered functions

```go
// List all functions
allFunctions := functions.ListAll()
for name, fn := range allFunctions {
    fmt.Printf("函数名: %s, 类型: %s, 描述: %s\n", 
        name, fn.GetType(), fn.GetDescription())
}

// View functions by type
mathFunctions := functions.GetByType(functions.TypeMath)
for _, fn := range mathFunctions {
    fmt.Printf("数学函数: %s - %s\n", fn.GetName(), fn.GetDescription())
}

// Check if the function exists
if fn, exists := functions.Get("my_function"); exists {
    fmt.Printf("函数存在: %s\n", fn.GetDescription())
}
```

### Cancel Function

```go
// Delete custom functions
success := functions.Unregister("my_custom_function")
if success {
    fmt.Println("函数注销成功")
}
```

## 🎯 Best Practices

### 1. Error handling

```go
func (f *MyFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    // 1. Parameter validation
    if len(args) == 0 {
        return nil, fmt.Errorf("至少需要一个参数")
    }
    
    // 2. Type conversion
    val, err := cast.ToFloat64E(args[0])
    if err != nil {
        return nil, fmt.Errorf("参数类型错误: %v", err)
    }
    
    // 3. Business logic validation
    if val < 0 {
        return nil, fmt.Errorf("参数值必须为正数")
    }
    
    // 4. Computational logic
    result := math.Sqrt(val)
    
    return result, nil
}
```

### 2. Performance optimization

```go
type CachedFunction struct {
    *functions.BaseFunction
    cache   map[string]interface{}
    mutex   sync.RWMutex
}

func (f *CachedFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    // Generate cache key
    key := fmt.Sprintf("%v", args)
    
    // Check the cache
    f.mutex.RLock()
    if cached, exists := f.cache[key]; exists {
        f.mutex.RUnlock()
        return cached, nil
    }
    f.mutex.RUnlock()
    
    // Calculation results
    result := f.calculate(args)
    
    // Store to cache
    f.mutex.Lock()
    f.cache[key] = result
    f.mutex.Unlock()
    
    return result, nil
}
```

### 3. Status management

```go
type StatefulFunction struct {
    *functions.BaseFunction
    counter int64
    mutex   sync.Mutex
}

func (f *StatefulFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    f.mutex.Lock()
    defer f.mutex.Unlock()
    
    f.counter++
    return f.counter, nil
}
```

## 🚨 Notes

1. **Thread Safety**: Functions may execute concurrently in multithreaded environments to ensure thread safety
2. **Error handling**: Always returns meaningful error messages
3. **Type Conversion**: Use the conversion function provided by the framework to perform type conversion
4. **Performance Considerations**: Avoid time-consuming operations in functions and consider using caches
5. **Resource Management**: Pay attention to resource application and release
6. **Naming conventions**: Use clear and descriptive function names

## 📝 Test your custom function

```go
func TestMyCustomFunction(t *testing.T) {
    // Register the function
    err := functions.RegisterCustomFunction("test_func", /* ... */)
    assert.NoError(t, err)
    defer functions.Unregister("test_func")
    
    // Get the function
    fn, exists := functions.Get("test_func")
    assert.True(t, exists)
    
    // Test execution
    ctx := &functions.FunctionContext{
        Data: make(map[string]interface{}),
    }
    
    result, err := fn.Execute(ctx, []interface{}{10.0})
    assert.NoError(t, err)
    assert.Equal(t, expectedResult, result)
}
```

With this guide, you can easily extend StreamSQL's features and implement various custom functions to meet specific business needs. 
