# StreamSQL 自定义函数开发指南

## 🚀 概述

StreamSQL 提供了强大而灵活的自定义函数系统，支持用户根据业务需求扩展各种类型的函数，包括数学函数、字符串函数、聚合函数、分析函数等。

## 📋 函数类型分类

### 内置函数类型

```go
const (
    TypeAggregation FunctionType = "aggregation"  // 聚合函数
    TypeWindow      FunctionType = "window"       // 窗口函数  
    TypeDateTime    FunctionType = "datetime"     // 时间日期函数
    TypeConversion  FunctionType = "conversion"   // 转换函数
    TypeMath        FunctionType = "math"         // 数学函数
    TypeString      FunctionType = "string"       // 字符串函数
    TypeAnalytical  FunctionType = "analytical"   // 分析函数
    TypeCustom      FunctionType = "custom"       // 用户自定义函数
)
```

## 🛠️ 自定义函数实现方式

### 方式一：快速注册（推荐简单函数）

```go
import "github.com/rulego/streamsql/functions"

// 注册一个简单的数学函数
err := functions.RegisterCustomFunction(
    "double",                    // 函数名
    functions.TypeMath,          // 函数类型
    "数学函数",                   // 分类描述
    "将数值乘以2",                // 函数描述
    1,                          // 最少参数个数
    1,                          // 最多参数个数
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        val, err := functions.ConvertToFloat64(args[0])
        if err != nil {
            return nil, err
        }
        return val * 2, nil
    },
)
```

### 方式二：完整结构体实现（推荐复杂函数）

```go
// 1. 定义函数结构体
type AdvancedMathFunction struct {
    *functions.BaseFunction
    // 可以添加状态变量
    cache map[string]interface{}
}

// 2. 实现构造函数
func NewAdvancedMathFunction() *AdvancedMathFunction {
    return &AdvancedMathFunction{
        BaseFunction: functions.NewBaseFunction(
            "advanced_calc",           // 函数名
            functions.TypeMath,        // 函数类型
            "高级数学函数",             // 分类
            "高级数学计算",             // 描述
            2,                        // 最少参数
            3,                        // 最多参数
        ),
        cache: make(map[string]interface{}),
    }
}

// 3. 实现验证方法（可选，如有特殊验证需求）
func (f *AdvancedMathFunction) Validate(args []interface{}) error {
    if err := f.ValidateArgCount(args); err != nil {
        return err
    }
    
    // 自定义验证逻辑
    if len(args) >= 2 {
        if _, err := functions.ConvertToFloat64(args[0]); err != nil {
            return fmt.Errorf("第一个参数必须是数值")
        }
        if _, err := functions.ConvertToFloat64(args[1]); err != nil {
            return fmt.Errorf("第二个参数必须是数值")
        }
    }
    
    return nil
}

// 4. 实现执行方法
func (f *AdvancedMathFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    a, _ := functions.ConvertToFloat64(args[0])
    b, _ := functions.ConvertToFloat64(args[1])
    
    operation := "add" // 默认操作
    if len(args) > 2 {
        op, err := functions.ConvertToString(args[2])
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

// 5. 注册函数
func init() {
    functions.Register(NewAdvancedMathFunction())
}
```

## 🎯 各类型函数实现示例

### 1. 数学函数示例

```go
// 距离计算函数
func RegisterDistanceFunction() error {
    return functions.RegisterCustomFunction(
        "distance",
        functions.TypeMath,
        "几何数学",
        "计算两点间距离",
        4, 4,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            x1, err := functions.ConvertToFloat64(args[0])
            if err != nil { return nil, err }
            y1, err := functions.ConvertToFloat64(args[1])
            if err != nil { return nil, err }
            x2, err := functions.ConvertToFloat64(args[2])
            if err != nil { return nil, err }
            y2, err := functions.ConvertToFloat64(args[3])
            if err != nil { return nil, err }
            
            distance := math.Sqrt(math.Pow(x2-x1, 2) + math.Pow(y2-y1, 2))
            return distance, nil
        },
    )
}

// SQL使用示例:
// SELECT device, distance(lat1, lon1, lat2, lon2) as dist FROM stream
```

### 2. 字符串函数示例

```go
// JSON提取函数
func RegisterJsonExtractFunction() error {
    return functions.RegisterCustomFunction(
        "json_extract",
        functions.TypeString,
        "JSON处理",
        "从JSON字符串中提取字段值",
        2, 2,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            jsonStr, err := functions.ConvertToString(args[0])
            if err != nil { return nil, err }
            
            path, err := functions.ConvertToString(args[1])
            if err != nil { return nil, err }
            
            var data map[string]interface{}
            if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
                return nil, fmt.Errorf("invalid JSON: %v", err)
            }
            
            // 简单路径提取（可扩展为复杂JSONPath）
            value, exists := data[path]
            if !exists {
                return nil, nil
            }
            
            return value, nil
        },
    )
}

// SQL使用示例:
// SELECT device, json_extract(metadata, 'version') as version FROM stream
```

### 3. 时间日期函数示例

```go
// 时间格式化函数
func RegisterDateFormatFunction() error {
    return functions.RegisterCustomFunction(
        "date_format",
        functions.TypeDateTime,
        "时间格式化",
        "格式化时间戳为指定格式",
        2, 2,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            timestamp, err := functions.ConvertToInt64(args[0])
            if err != nil { return nil, err }
            
            format, err := functions.ConvertToString(args[1])
            if err != nil { return nil, err }
            
            t := time.Unix(timestamp, 0)
            
            // 支持常见格式
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

// SQL使用示例:
// SELECT device, date_format(timestamp, 'YYYY-MM-DD') as date FROM stream
```

### 4. 转换函数示例

```go
// IP地址转换函数
func RegisterIpToIntFunction() error {
    return functions.RegisterCustomFunction(
        "ip_to_int",
        functions.TypeConversion,
        "网络转换",
        "将IP地址转换为整数",
        1, 1,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            ipStr, err := functions.ConvertToString(args[0])
            if err != nil { return nil, err }
            
            ip := net.ParseIP(ipStr)
            if ip == nil {
                return nil, fmt.Errorf("invalid IP address: %s", ipStr)
            }
            
            // 转换为IPv4
            ip = ip.To4()
            if ip == nil {
                return nil, fmt.Errorf("not an IPv4 address: %s", ipStr)
            }
            
            return int64(ip[0])<<24 + int64(ip[1])<<16 + int64(ip[2])<<8 + int64(ip[3]), nil
        },
    )
}

// SQL使用示例:
// SELECT device, ip_to_int(client_ip) as ip_int FROM stream
```

### 5. 自定义聚合函数示例

对于聚合函数，需要同时实现函数和聚合器：

```go
// 1. 实现自定义聚合函数
type MedianAggFunction struct {
    *functions.BaseFunction
}

func NewMedianAggFunction() *MedianAggFunction {
    return &MedianAggFunction{
        BaseFunction: functions.NewBaseFunction(
            "median_agg",
            functions.TypeAggregation,
            "统计聚合",
            "计算中位数",
            1, -1,
        ),
    }
}

func (f *MedianAggFunction) Validate(args []interface{}) error {
    return f.ValidateArgCount(args)
}

func (f *MedianAggFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    // 聚合函数的Execute在这里可能不会被直接调用
    // 实际逻辑在聚合器中实现
    return nil, nil
}

// 2. 实现对应的聚合器
type MedianCustomAggregator struct {
    values []float64
}

func (m *MedianCustomAggregator) New() aggregator.AggregatorFunction {
    return &MedianCustomAggregator{
        values: make([]float64, 0),
    }
}

func (m *MedianCustomAggregator) Add(value interface{}) {
    if val, err := functions.ConvertToFloat64(value); err == nil {
        m.values = append(m.values, val)
    }
}

func (m *MedianCustomAggregator) Result() interface{} {
    if len(m.values) == 0 {
        return 0.0
    }
    
    sort.Float64s(m.values)
    mid := len(m.values) / 2
    
    if len(m.values)%2 == 0 {
        return (m.values[mid-1] + m.values[mid]) / 2
    }
    return m.values[mid]
}

// 3. 注册聚合器
func init() {
    // 注册函数
    functions.Register(NewMedianAggFunction())
    
    // 注册聚合器
    aggregator.Register("median_agg", func() aggregator.AggregatorFunction {
        return &MedianCustomAggregator{}
    })
}

// SQL使用示例:
// SELECT device, median_agg(temperature) as median_temp FROM stream GROUP BY device
```

## 📊 函数管理功能

### 查看已注册函数

```go
// 列出所有函数
allFunctions := functions.ListAll()
for name, fn := range allFunctions {
    fmt.Printf("函数名: %s, 类型: %s, 描述: %s\n", 
        name, fn.GetType(), fn.GetDescription())
}

// 按类型查看函数
mathFunctions := functions.GetByType(functions.TypeMath)
for _, fn := range mathFunctions {
    fmt.Printf("数学函数: %s - %s\n", fn.GetName(), fn.GetDescription())
}

// 检查函数是否存在
if fn, exists := functions.Get("my_function"); exists {
    fmt.Printf("函数存在: %s\n", fn.GetDescription())
}
```

### 注销函数

```go
// 注销自定义函数
success := functions.Unregister("my_custom_function")
if success {
    fmt.Println("函数注销成功")
}
```

## 🎯 最佳实践

### 1. 错误处理

```go
func (f *MyFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    // 1. 参数验证
    if len(args) == 0 {
        return nil, fmt.Errorf("至少需要一个参数")
    }
    
    // 2. 类型转换
    val, err := functions.ConvertToFloat64(args[0])
    if err != nil {
        return nil, fmt.Errorf("参数类型错误: %v", err)
    }
    
    // 3. 业务逻辑验证
    if val < 0 {
        return nil, fmt.Errorf("参数值必须为正数")
    }
    
    // 4. 计算逻辑
    result := math.Sqrt(val)
    
    return result, nil
}
```

### 2. 性能优化

```go
type CachedFunction struct {
    *functions.BaseFunction
    cache   map[string]interface{}
    mutex   sync.RWMutex
}

func (f *CachedFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    // 生成缓存key
    key := fmt.Sprintf("%v", args)
    
    // 检查缓存
    f.mutex.RLock()
    if cached, exists := f.cache[key]; exists {
        f.mutex.RUnlock()
        return cached, nil
    }
    f.mutex.RUnlock()
    
    // 计算结果
    result := f.calculate(args)
    
    // 存储到缓存
    f.mutex.Lock()
    f.cache[key] = result
    f.mutex.Unlock()
    
    return result, nil
}
```

### 3. 状态管理

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

## 🚨 注意事项

1. **线程安全**: 函数可能在多线程环境下并发执行，确保线程安全
2. **错误处理**: 总是返回有意义的错误信息
3. **类型转换**: 使用框架提供的转换函数进行类型转换
4. **性能考虑**: 避免在函数中执行耗时操作，考虑使用缓存
5. **资源管理**: 注意资源的申请和释放
6. **命名规范**: 使用清晰、描述性的函数名

## 📝 测试你的自定义函数

```go
func TestMyCustomFunction(t *testing.T) {
    // 注册函数
    err := functions.RegisterCustomFunction("test_func", /* ... */)
    assert.NoError(t, err)
    defer functions.Unregister("test_func")
    
    // 获取函数
    fn, exists := functions.Get("test_func")
    assert.True(t, exists)
    
    // 测试执行
    ctx := &functions.FunctionContext{
        Data: make(map[string]interface{}),
    }
    
    result, err := fn.Execute(ctx, []interface{}{10.0})
    assert.NoError(t, err)
    assert.Equal(t, expectedResult, result)
}
```

通过这个指南，你可以轻松扩展StreamSQL的功能，实现各种自定义函数来满足特定的业务需求。 