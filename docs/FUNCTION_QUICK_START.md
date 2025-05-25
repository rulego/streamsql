# StreamSQL 自定义函数快速入门

## 🚀 概述

StreamSQL 提供了强大的自定义函数系统，让你可以轻松扩展框架功能。本指南将帮你快速上手，创建和使用自定义函数。

## 📋 快速开始

### 1. 注册简单函数

最简单的方式是使用 `RegisterCustomFunction` 方法：

```go
import "github.com/rulego/streamsql/functions"

// 注册一个平方函数
err := functions.RegisterCustomFunction(
    "square",                    // 函数名
    functions.TypeMath,          // 函数类型
    "数学函数",                   // 分类
    "计算数值的平方",             // 描述
    1,                          // 最少参数数量
    1,                          // 最多参数数量
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        // 参数转换
        val, err := functions.ConvertToFloat64(args[0])
        if err != nil {
            return nil, err
        }
        // 业务逻辑
        return val * val, nil
    },
)
```

### 2. 在SQL中使用

```sql
SELECT device, square(value) as squared_value FROM stream
```

## 🎯 函数类型

### 数学函数 (TypeMath)

```go
// 距离计算函数
functions.RegisterCustomFunction(
    "distance",
    functions.TypeMath,
    "几何数学",
    "计算两点间的欧几里得距离",
    4, 4,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        x1, _ := functions.ConvertToFloat64(args[0])
        y1, _ := functions.ConvertToFloat64(args[1])
        x2, _ := functions.ConvertToFloat64(args[2])
        y2, _ := functions.ConvertToFloat64(args[3])
        
        return math.Sqrt(math.Pow(x2-x1, 2) + math.Pow(y2-y1, 2)), nil
    },
)

// SQL使用
// SELECT device, distance(lat1, lon1, lat2, lon2) as dist FROM stream
```

### 字符串函数 (TypeString)

```go
// 字符串反转函数
functions.RegisterCustomFunction(
    "reverse",
    functions.TypeString,
    "字符串处理",
    "反转字符串",
    1, 1,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        str, err := functions.ConvertToString(args[0])
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

// SQL使用
// SELECT device, reverse(device_name) as reversed_name FROM stream
```

### 转换函数 (TypeConversion)

```go
// IP地址转整数
functions.RegisterCustomFunction(
    "ip_to_int",
    functions.TypeConversion,
    "网络转换",
    "将IPv4地址转换为32位整数",
    1, 1,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        ipStr, err := functions.ConvertToString(args[0])
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

// SQL使用
// SELECT device, ip_to_int(client_ip) as ip_num FROM stream
```

### 时间日期函数 (TypeDateTime)

```go
// 时间格式化函数
functions.RegisterCustomFunction(
    "format_time",
    functions.TypeDateTime,
    "时间格式化",
    "格式化Unix时间戳",
    2, 2,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        timestamp, err := functions.ConvertToInt64(args[0])
        if err != nil {
            return nil, err
        }
        
        format, err := functions.ConvertToString(args[1])
        if err != nil {
            return nil, err
        }
        
        t := time.Unix(timestamp, 0)
        return t.Format(format), nil
    },
)

// SQL使用
// SELECT device, format_time(timestamp, '2006-01-02 15:04:05') as formatted_time FROM stream
```

## 🏗️ 复杂函数实现

对于复杂函数，建议使用结构体方式：

```go
// 1. 定义函数结构
type StatefulFunction struct {
    *functions.BaseFunction
    counter int64
    mutex   sync.Mutex
}

// 2. 构造函数
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

// 3. 验证参数（可选）
func (f *StatefulFunction) Validate(args []interface{}) error {
    return f.ValidateArgCount(args)
}

// 4. 执行函数
func (f *StatefulFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    f.mutex.Lock()
    defer f.mutex.Unlock()
    
    f.counter++
    return f.counter, nil
}

// 5. 注册函数
func init() {
    functions.Register(NewStatefulFunction())
}
```

## 📊 聚合函数

聚合函数需要同时实现函数和聚合器：

```go
// 1. 实现聚合函数
type GeometricMeanFunction struct {
    *functions.BaseFunction
}

func NewGeometricMeanFunction() *GeometricMeanFunction {
    return &GeometricMeanFunction{
        BaseFunction: functions.NewBaseFunction(
            "geometric_mean",
            functions.TypeAggregation,
            "统计聚合",
            "计算几何平均数",
            1, -1,
        ),
    }
}

func (f *GeometricMeanFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    return nil, nil // 逻辑在聚合器中
}

// 2. 实现聚合器
type GeometricMeanAggregator struct {
    values []float64
}

func (g *GeometricMeanAggregator) New() aggregator.AggregatorFunction {
    return &GeometricMeanAggregator{values: make([]float64, 0)}
}

func (g *GeometricMeanAggregator) Add(value interface{}) {
    if val, err := functions.ConvertToFloat64(value); err == nil && val > 0 {
        g.values = append(g.values, val)
    }
}

func (g *GeometricMeanAggregator) Result() interface{} {
    if len(g.values) == 0 {
        return 0.0
    }
    
    product := 1.0
    for _, v := range g.values {
        product *= v
    }
    
    return math.Pow(product, 1.0/float64(len(g.values)))
}

// 3. 注册
func init() {
    functions.Register(NewGeometricMeanFunction())
    aggregator.Register("geometric_mean", func() aggregator.AggregatorFunction {
        return &GeometricMeanAggregator{}
    })
}

// SQL使用
// SELECT device, geometric_mean(value) as geo_mean FROM stream GROUP BY device
```

## 🔧 函数管理

### 查看注册的函数

```go
// 列出所有函数
allFunctions := functions.ListAll()
for name, fn := range allFunctions {
    fmt.Printf("函数: %s (%s) - %s\n", name, fn.GetType(), fn.GetDescription())
}

// 按类型查看
mathFunctions := functions.GetByType(functions.TypeMath)
for _, fn := range mathFunctions {
    fmt.Printf("数学函数: %s\n", fn.GetName())
}

// 查找特定函数
if fn, exists := functions.Get("square"); exists {
    fmt.Printf("找到函数: %s\n", fn.GetDescription())
}
```

### 注销函数

```go
// 注销函数
success := functions.Unregister("my_function")
if success {
    fmt.Println("函数注销成功")
}
```

## 🎯 完整示例

### 创建温度转换函数

```go
package main

import (
    "fmt"
    "time"
    "github.com/rulego/streamsql"
    "github.com/rulego/streamsql/functions"
)

func main() {
    // 1. 注册自定义函数
    registerCustomFunctions()
    
    // 2. 创建StreamSQL实例
    ssql := streamsql.New()
    defer ssql.Stop()
    
    // 3. 执行SQL
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
    
    // 4. 添加结果监听
    ssql.Stream().AddSink(func(result interface{}) {
        fmt.Printf("结果: %v\n", result)
    })
    
    // 5. 添加数据
    ssql.AddData(map[string]interface{}{
        "device": "thermometer1",
        "temperature": 25.0,
    })
    
    time.Sleep(time.Second)
}

func registerCustomFunctions() {
    // 摄氏度转华氏度
    functions.RegisterCustomFunction(
        "celsius_to_fahrenheit",
        functions.TypeMath,
        "温度转换",
        "摄氏度转华氏度",
        1, 1,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            celsius, err := functions.ConvertToFloat64(args[0])
            if err != nil {
                return nil, err
            }
            return celsius*9/5 + 32, nil
        },
    )
    
    // 温度格式化
    functions.RegisterCustomFunction(
        "format_temperature",
        functions.TypeString,
        "格式化函数",
        "格式化温度显示",
        2, 2,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            temp, err := functions.ConvertToFloat64(args[0])
            if err != nil {
                return nil, err
            }
            
            unit, err := functions.ConvertToString(args[1])
            if err != nil {
                return nil, err
            }
            
            return fmt.Sprintf("%.1f°%s", temp, unit), nil
        },
    )
}
```

## 🚨 最佳实践

### 1. 错误处理

```go
func (f *MyFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    // 参数数量检查
    if len(args) == 0 {
        return nil, fmt.Errorf("至少需要1个参数")
    }
    
    // 类型转换
    val, err := functions.ConvertToFloat64(args[0])
    if err != nil {
        return nil, fmt.Errorf("参数类型错误: %v", err)
    }
    
    // 业务逻辑验证
    if val < 0 {
        return nil, fmt.Errorf("参数必须为非负数")
    }
    
    return math.Sqrt(val), nil
}
```

### 2. 性能优化

```go
type CachedFunction struct {
    *functions.BaseFunction
    cache map[string]interface{}
    mutex sync.RWMutex
}

func (f *CachedFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    key := fmt.Sprintf("%v", args)
    
    // 检查缓存
    f.mutex.RLock()
    if result, exists := f.cache[key]; exists {
        f.mutex.RUnlock()
        return result, nil
    }
    f.mutex.RUnlock()
    
    // 计算结果
    result := f.calculate(args)
    
    // 缓存结果
    f.mutex.Lock()
    f.cache[key] = result
    f.mutex.Unlock()
    
    return result, nil
}
```

### 3. 线程安全

```go
type ThreadSafeFunction struct {
    *functions.BaseFunction
    state map[string]interface{}
    mutex sync.RWMutex
}

func (f *ThreadSafeFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    f.mutex.Lock()
    defer f.mutex.Unlock()
    
    // 安全地修改状态
    f.state["counter"] = f.state["counter"].(int) + 1
    
    return f.state["counter"], nil
}
```

## 📝 测试你的函数

```go
func TestMyCustomFunction(t *testing.T) {
    // 注册函数
    err := functions.RegisterCustomFunction("test_func", functions.TypeMath, "测试", "测试函数", 1, 1,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            val, err := functions.ConvertToFloat64(args[0])
            return val * 2, err
        })
    assert.NoError(t, err)
    defer functions.Unregister("test_func")
    
    // 获取并测试函数
    fn, exists := functions.Get("test_func")
    assert.True(t, exists)
    
    ctx := &functions.FunctionContext{Data: make(map[string]interface{})}
    result, err := fn.Execute(ctx, []interface{}{5.0})
    
    assert.NoError(t, err)
    assert.Equal(t, 10.0, result)
}
```

通过这个快速入门指南，你已经掌握了StreamSQL自定义函数的基本用法。现在可以开始创建自己的函数来扩展框架功能！ 