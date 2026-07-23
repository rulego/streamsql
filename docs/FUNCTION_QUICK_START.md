# StreamSQL Quick Start with Custom Functions

## 🚀 Overview

StreamSQL offers a powerful custom function system, allowing you to easily extend the framework's functionality. This guide will help you get started quickly by creating and using custom functions.

## 📋 Get Started Quickly

### 1. Register simple functions

The simplest way is to use the `RegisterCustomFunction` method:

```go
import (
    "github.com/rulego/streamsql/functions"
    "github.com/rulego/streamsql/utils/cast"
)

// Register a square function
err := functions.RegisterCustomFunction(
    "square",                    // Function name
    functions.TypeMath,          // Function type
    "数学函数",                   // Classification
    "计算数值的平方",             // Description
    1,                          // Minimum number of parameters
    1,                          // Maximum number of parameters
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        // Parameter conversion
        val, err := cast.ToFloat64E(args[0])
        if err != nil {
            return nil, err
        }
        // Business logic
        return val * val, nil
    },
)
```

### 2. Used in SQL

```sql
SELECT device, square(value) as squared_value FROM stream
```

## 🎯 Function type

### Mathematical Functions (TypeMath)

```go
// Distance calculation function
functions.RegisterCustomFunction(
    "distance",
    functions.TypeMath,
    "几何数学",
    "计算两点间的欧几里得距离",
    4, 4,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        x1, _ := cast.ToFloat64E(args[0])
        y1, _ := cast.ToFloat64E(args[1])
        x2, _ := cast.ToFloat64E(args[2])
        y2, _ := cast.ToFloat64E(args[3])
        
        return math.Sqrt(math.Pow(x2-x1, 2) + math.Pow(y2-y1, 2)), nil
    },
)

// SQL use
// SELECT device, distance(lat1, lon1, lat2, lon2) as dist FROM stream
```

### String Function (TypeString)

```go
// String inversion function
functions.RegisterCustomFunction(
    "reverse",
    functions.TypeString,
    "字符串处理",
    "反转字符串",
    1, 1,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        str, err := cast.ToStringE(args[0])
        if err != nil {
            return nil, err
        }
        
        runes := []rune(str)
        for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
            runes[i], runes[j] = runes[j], runes[i]
        }
        
        return string(runes), nil
    },
)

// SQL use
// SELECT device, reverse(device_name) as reversed_name FROM stream
```

### Transformation Function (TypeConversion)

```go
// IP address to integer
functions.RegisterCustomFunction(
    "ip_to_int",
    functions.TypeConversion,
    "网络转换",
    "将IPv4地址转换为32位整数",
    1, 1,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        ipStr, err := cast.ToStringE(args[0])
        if err != nil {
            return nil, err
        }
        
        ip := net.ParseIP(ipStr).To4()
        if ip == nil {
            return nil, fmt.Errorf("invalid IPv4: %s", ipStr)
        }
        
        return int64(ip[0])<<24 + int64(ip[1])<<16 + int64(ip[2])<<8 + int64(ip[3]), nil
    },
)

// SQL use
// SELECT device, ip_to_int(client_ip) as ip_num FROM stream
```

### Time-Date Function (TypeDateTime)

```go
// Time formatting function
functions.RegisterCustomFunction(
    "format_time",
    functions.TypeDateTime,
    "时间格式化",
    "格式化Unix时间戳",
    2, 2,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        timestamp, err := cast.ToInt64E(args[0])
        if err != nil {
            return nil, err
        }
        
        format, err := cast.ToStringE(args[1])
        if err != nil {
            return nil, err
        }
        
        t := time.Unix(timestamp, 0)
        return t.Format(format), nil
    },
)

// SQL use
// SELECT device, format_time(timestamp, '2006-01-02 15:04:05') as formatted_time FROM stream
```

## 🏗️ Implementation of complex functions

For complex functions, it is recommended to use structs:

```go
// 1. Define the function structure
type StatefulFunction struct {
    *functions.BaseFunction
    counter int64
    mutex   sync.Mutex
}

// 2. Constructor
func NewStatefulFunction() *StatefulFunction {
    return &StatefulFunction{
        BaseFunction: functions.NewBaseFunction(
            "counter",
            functions.TypeCustom,
            "状态函数",
            "递增计数器",
            0, 0,
        ),
        counter: 0,
    }
}

// 3. Verification parameters (optional)
func (f *StatefulFunction) Validate(args []interface{}) error {
    return f.ValidateArgCount(args)
}

// 4. Execute the function
func (f *StatefulFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    f.mutex.Lock()
    defer f.mutex.Unlock()
    
    f.counter++
    return f.counter, nil
}

// 5. Register functions
func init() {
    functions.Register(NewStatefulFunction())
}
```

## 📊 Aggregation Function

The aggregator function implements `AggregatorFunction` interfaces (`New` / `Add` / `Result` / `Reset` / `Clone`), registering one place with `functions. Register` — adapters automatically connect without `aggregator. Register`.

```go
import (
    "math"

    "github.com/rulego/streamsql/functions"
    "github.com/rulego/streamsql/utils/cast"
)

// GeometricMean Fully implement AggregatorFunction
type GeometricMean struct {
    *functions.BaseFunction
    values []float64
}

func NewGeometricMean() *GeometricMean {
    return &GeometricMean{BaseFunction: functions.NewBaseFunction(
        "geometric_mean", functions.TypeAggregation, "统计聚合", "几何平均数", 1, -1)}
}

func (f *GeometricMean) Validate(args []any) error                            { return f.ValidateArgCount(args) }
func (f *GeometricMean) Execute(ctx *functions.FunctionContext, args []any) (any, error) {
    return nil, nil // Aggregation follows Add/Result, Execute only meets interface requirements
}
func (f *GeometricMean) New() functions.AggregatorFunction { return &GeometricMean{BaseFunction: f.BaseFunction} }
func (f *GeometricMean) Add(value any) {
    if v, err := cast.ToFloat64E(value); err == nil && v > 0 {
        f.values = append(f.values, v)
    }
}
func (f *GeometricMean) Result() any {
    if len(f.values) == 0 {
        return 0.0
    }
    product := 1.0
    for _, v := range f.values {
        product *= v
    }
    return math.Pow(product, 1.0/float64(len(f.values)))
}
func (f *GeometricMean) Reset()                                   { f.values = nil }
func (f *GeometricMean) Clone() functions.AggregatorFunction {
    cp := make([]float64, len(f.values))
    copy(cp, f.values)
    return &GeometricMean{BaseFunction: f.BaseFunction, values: cp}
}

func init() {
    functions.Register(NewGeometricMean())
}

// SQL use
// SELECT device, geometric_mean(value) AS geo_mean FROM stream GROUP BY device
```

## 🔧 Function Management

### Check the registered functions

```go
// List all functions
allFunctions := functions.ListAll()
for name, fn := range allFunctions {
    fmt.Printf("函数: %s (%s) - %s\n", name, fn.GetType(), fn.GetDescription())
}

// View by type
mathFunctions := functions.GetByType(functions.TypeMath)
for _, fn := range mathFunctions {
    fmt.Printf("数学函数: %s\n", fn.GetName())
}

// Find a specific function
if fn, exists := functions.Get("square"); exists {
    fmt.Printf("找到函数: %s\n", fn.GetDescription())
}
```

### Cancel Function

```go
// Cancel function
success := functions.Unregister("my_function")
if success {
    fmt.Println("函数注销成功")
}
```

## 🎯 Complete Example

### Creating a temperature conversion function

```go
package main

import (
    "fmt"
    "time"
    "github.com/rulego/streamsql"
    "github.com/rulego/streamsql/functions"
)

func main() {
    // 1. Register custom functions
    registerCustomFunctions()
    
    // 2. Create StreamSQL instance
    ssql := streamsql.New()
    defer ssql.Stop()
    
    // 3. Execute SQL
    sql := `
        SELECT 
            device,
            celsius_to_fahrenheit(temperature) as temp_f,
            format_temperature(temperature, 'C') as formatted_temp
        FROM stream
    `
    
    err := ssql.Execute(sql)
    if err != nil {
        panic(err)
    }
    
    // 4. Add result monitoring
    ssql.Stream().AddSink(func(result interface{}) {
        fmt.Printf("结果: %v\n", result)
    })
    
    // 5. Add data
    ssql.AddData(map[string]interface{}{
        "device": "thermometer1",
        "temperature": 25.0,
    })
    
    time.Sleep(time.Second)
}

func registerCustomFunctions() {
    // Celsius degrees are converted to Fahrenheit degrees
    functions.RegisterCustomFunction(
        "celsius_to_fahrenheit",
        functions.TypeMath,
        "温度转换",
        "摄氏度转华氏度",
        1, 1,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            celsius, err := cast.ToFloat64E(args[0])
            if err != nil {
                return nil, err
            }
            return celsius*9/5 + 32, nil
        },
    )
    
    // Temperature formatting
    functions.RegisterCustomFunction(
        "format_temperature",
        functions.TypeString,
        "格式化函数",
        "格式化温度显示",
        2, 2,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            temp, err := cast.ToFloat64E(args[0])
            if err != nil {
                return nil, err
            }
            
            unit, err := cast.ToStringE(args[1])
            if err != nil {
                return nil, err
            }
            
            return fmt.Sprintf("%.1f°%s", temp, unit), nil
        },
    )
}
```

## 🚨 Best Practices

### 1. Error handling

```go
func (f *MyFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    // Parameter quantity check
    if len(args) == 0 {
        return nil, fmt.Errorf("至少需要1个参数")
    }
    
    // Type conversion
    val, err := cast.ToFloat64E(args[0])
    if err != nil {
        return nil, fmt.Errorf("参数类型错误: %v", err)
    }
    
    // Business logic validation
    if val < 0 {
        return nil, fmt.Errorf("参数必须为非负数")
    }
    
    return math.Sqrt(val), nil
}
```

### 2. Performance optimization

```go
type CachedFunction struct {
    *functions.BaseFunction
    cache map[string]interface{}
    mutex sync.RWMutex
}

func (f *CachedFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    key := fmt.Sprintf("%v", args)
    
    // Check the cache
    f.mutex.RLock()
    if result, exists := f.cache[key]; exists {
        f.mutex.RUnlock()
        return result, nil
    }
    f.mutex.RUnlock()
    
    // Calculation results
    result := f.calculate(args)
    
    // Cache the results
    f.mutex.Lock()
    f.cache[key] = result
    f.mutex.Unlock()
    
    return result, nil
}
```

### 3. Thread safety

```go
type ThreadSafeFunction struct {
    *functions.BaseFunction
    state map[string]interface{}
    mutex sync.RWMutex
}

func (f *ThreadSafeFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    f.mutex.Lock()
    defer f.mutex.Unlock()
    
    // Safely modify the state
    f.state["counter"] = f.state["counter"].(int) + 1
    
    return f.state["counter"], nil
}
```

## 📝 Test your function

```go
func TestMyCustomFunction(t *testing.T) {
    // Register the function
    err := functions.RegisterCustomFunction("test_func", functions.TypeMath, "测试", "测试函数", 1, 1,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            val, err := cast.ToFloat64E(args[0])
            return val * 2, err
        })
    assert.NoError(t, err)
    defer functions.Unregister("test_func")
    
    // Get and test the function
    fn, exists := functions.Get("test_func")
    assert.True(t, exists)
    
    ctx := &functions.FunctionContext{Data: make(map[string]interface{})}
    result, err := fn.Execute(ctx, []interface{}{5.0})
    
    assert.NoError(t, err)
    assert.Equal(t, 10.0, result)
}
```

With this quick start guide, you have mastered the basic usage of StreamSQL custom functions. Now you can start creating your own functions to expand the framework's capabilities! 
