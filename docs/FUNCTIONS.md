# StreamSQL 函数系统

StreamSQL 现已支持强大的函数系统，允许在 SQL 查询中使用各种内置函数和自定义函数。

## 🚀 主要特性

### 1. 模块化函数架构
- **函数注册器**：统一的函数注册和管理系统
- **类型安全**：强类型参数验证和转换
- **可扩展性**：支持运行时注册自定义函数
- **分类管理**：按功能类型组织函数

### 2. 内置函数类别

#### 数学函数 (TypeMath)
- `ABS(x)` - 绝对值
- `SQRT(x)` - 平方根

#### 字符串函数 (TypeString)
- `CONCAT(str1, str2, ...)` - 字符串连接
- `LENGTH(str)` - 字符串长度
- `UPPER(str)` - 转大写
- `LOWER(str)` - 转小写

#### 转换函数 (TypeConversion)
- `CAST(value, type)` - 类型转换
- `HEX2DEC(hexStr)` - 十六进制转十进制
- `DEC2HEX(number)` - 十进制转十六进制

#### 时间日期函数 (TypeDateTime)
- `NOW()` - 当前时间戳

### 3. 表达式引擎增强
- 支持函数调用的复杂表达式
- 运算符优先级处理
- 括号分组支持
- 自动类型转换

## 📝 使用示例

### 基本函数使用

```sql
-- 数学函数
SELECT device, ABS(temperature - 20) as deviation 
FROM stream;

-- 字符串函数  
SELECT CONCAT(device, '_processed') as processed_name
FROM stream;

-- 表达式中的函数
SELECT device, AVG(ABS(temperature - 20)) as avg_deviation
FROM stream 
GROUP BY device, TumblingWindow('1s');
```

### 自定义函数注册

```go
import (
    "github.com/rulego/streamsql/functions"
    "github.com/rulego/streamsql/utils/cast"
)

// 注册华氏度转摄氏度函数
err := functions.RegisterCustomFunction(
    "fahrenheit_to_celsius", 
    functions.TypeCustom, 
    "温度转换", 
    "华氏度转摄氏度", 
    1, 1,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        fahrenheit, err := cast.ToFloat64E(args[0])
        if err != nil {
            return nil, err
        }
        celsius := (fahrenheit - 32) * 5 / 9
        return celsius, nil
    })

// 在 SQL 中使用
sql := `
    SELECT device, AVG(fahrenheit_to_celsius(temperature)) as avg_celsius
    FROM stream 
    GROUP BY device, TumblingWindow('2s')
`
```

### 复合表达式

```sql
-- 复杂的数学表达式
SELECT 
    device,
    AVG(ABS(temperature - 20) * 1.8 + 32) as complex_calc
FROM stream 
GROUP BY device, TumblingWindow('1s');
```

## 🛠️ 函数开发

### 实现自定义函数

```go
// 1. 定义函数结构
type MyCustomFunction struct {
    *functions.BaseFunction
}

// 2. 实现构造函数
func NewMyCustomFunction() *MyCustomFunction {
    return &MyCustomFunction{
        BaseFunction: functions.NewBaseFunction(
            "my_func", 
            functions.TypeCustom, 
            "自定义分类", 
            "函数描述", 
            1, 3, // 最少1个参数，最多3个参数
        ),
    }
}

// 3. 实现验证方法
func (f *MyCustomFunction) Validate(args []interface{}) error {
    return f.ValidateArgCount(args)
}

// 4. 实现执行方法
func (f *MyCustomFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    // 实现具体逻辑
    return result, nil
}

// 5. 注册函数
functions.Register(NewMyCustomFunction())
```

### 便捷注册方式

```go
// 使用便捷方法注册函数
err := functions.RegisterCustomFunction(
    "double", 
    functions.TypeCustom, 
    "数学运算", 
    "将数值乘以2", 
    1, 1,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        val, err := cast.ToFloat64E(args[0])
        if err != nil {
            return nil, err
        }
        return val * 2, nil
    })
```

## 🧪 测试

### 运行函数系统测试
```bash
go test ./functions -v
```

### 运行集成测试
```bash
go test -v -run TestExpressionInAggregation
```

## 📋 支持的数据类型

函数系统支持以下数据类型的自动转换：

- **数值类型**: `int`, `int32`, `int64`, `uint`, `uint32`, `uint64`, `float32`, `float64`
- **字符串类型**: `string`
- **布尔类型**: `bool`
- **自动转换**: 字符串数值自动转换为相应的数值类型

## 🔧 类型转换工具

```go
// 使用内置转换函数
val, err := cast.ToFloat64E(someValue)
str, err := cast.ToStringE(someValue)
num, err := cast.ToInt64E(someValue)
flag, err := cast.ToBoolE(someValue)
```

## 📈 性能考虑

- **函数注册**: 一次性注册，运行时无开销
- **类型转换**: 高效的类型检查和转换
- **表达式缓存**: 表达式解析结果可复用
- **并发安全**: 函数注册器支持并发访问

## 🌟 路线图

已实现的功能：
- ✅ SELECT DISTINCT
- ✅ LIMIT 子句  
- ✅ HAVING 子句
- ✅ SESSION 窗口
- ✅ 函数参数支持表达式运算
- ✅ 统一函数注册系统

待实现的功能：
- 🔄 更多聚合函数（MEDIAN、STDDEV 等）
- 🔄 窗口函数（ROW_NUMBER、RANK 等）
- 🔄 更多时间日期函数
- 🔄 正则表达式函数
- 🔄 JSON 处理函数

## 🤝 贡献

欢迎提交新的函数实现！请遵循以下步骤：

1. 在 `functions/` 目录中实现函数
2. 添加相应的测试用例
3. 更新文档
4. 提交 Pull Request

---

*StreamSQL 函数系统让流处理更加强大和灵活！* 🚀 