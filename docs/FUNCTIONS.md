# StreamSQL Function system

StreamSQL now supports a powerful function system, allowing the use of various built-in and custom functions in SQL queries.

## 🚀 Main Features

### 1. Modular function architecture
- **Function Registerer**: A unified system for function registration and management
- **Type Safety**: Strong type parameter validation and conversion
- **Scalability**: Supports runtime registration of custom functions
- **Category Management**: Organize functions by function type

### 2. Built-in function categories

#### Mathematical Functions (TypeMath)
- `ABS(x)` - absolute value
- `SQRT(x)` - square root

#### String Function (TypeString)
- `CONCAT(str1, str2,.)` - String concatenation
- `LENGTH(str)` - String length
- `UPPER(str)` - Capitalized
- `LOWER(str)` - lowercase

#### Transformation Function (TypeConversion)
- `CAST(value, type)` - Type conversion
- `HEX2DEC(hexStr)` - Hexadecimal converted to decimal
- `DEC2HEX(number)` - Decimal converted to hexadecimal

#### Time-Date Function (TypeDateTime)
- `NOW()` - Current timestamp

### 3. Expression engine enhancement
- Complex expressions that support function calls
- Operator priority handling
- Bracket grouping support
- Automatic type conversion

## 📝 Usage Examples

### Basic Function Usage

```sql
-- Mathematical functions
SELECT device, ABS(temperature - 20) as deviation 
FROM stream;

-- String function  
SELECT CONCAT(device, '_processed') as processed_name
FROM stream;

-- Functions in expressions
SELECT device, AVG(ABS(temperature - 20)) as avg_deviation
FROM stream 
GROUP BY device, TumblingWindow('1s');
```

### Custom function registration

```go
import (
    "github.com/rulego/streamsql/functions"
    "github.com/rulego/streamsql/utils/cast"
)

// Register the Fahrenheit to Celsius function
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

// Used in SQL
sql := `
    SELECT device, AVG(fahrenheit_to_celsius(temperature)) as avg_celsius
    FROM stream 
    GROUP BY device, TumblingWindow('2s')
`
```

### Compound Expressions

```sql
-- Complex mathematical expressions
SELECT 
    device,
    AVG(ABS(temperature - 20) * 1.8 + 32) as complex_calc
FROM stream 
GROUP BY device, TumblingWindow('1s');
```

## 🛠️ Function Development

### Implementing custom functions

```go
// 1. Define the function structure
type MyCustomFunction struct {
    *functions.BaseFunction
}

// 2. Implement constructors
func NewMyCustomFunction() *MyCustomFunction {
    return &MyCustomFunction{
        BaseFunction: functions.NewBaseFunction(
            "my_func", 
            functions.TypeCustom, 
            "自定义分类", 
            "函数描述", 
            1, 3, // Minimum 1 parameter, maximum 3 parameters
        ),
    }
}

// 3. Implement verification methods
func (f *MyCustomFunction) Validate(args []interface{}) error {
    return f.ValidateArgCount(args)
}

// 4. Implement the execution method
func (f *MyCustomFunction) Execute(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
    // Implement concrete logic
    return result, nil
}

// 5. Register functions
functions.Register(NewMyCustomFunction())
```

### Convenient Registration Methods

```go
// Use an easy way to register the function
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

## 🧪 Testing

### Running Function System Testing
```bash
go test ./functions -v
```

### Run Integration Testing
```bash
go test -v -run TestExpressionInAggregation
```

## 📋 Supported Data Types

The function system supports automatic conversion of the following data types:

- **Numeric types**: `int`, `int32`, `int64`, `uint`, `uint32`, `uint64`, `float32`, `float64`
- **String type**: `string`
- **Boolean type**: `bool`
- **Automatic Conversion**: Automatically converts string values into the corresponding value type

## 🔧 Type conversion tool

```go
// Use built-in conversion functions
val, err := cast.ToFloat64E(someValue)
str, err := cast.ToStringE(someValue)
num, err := cast.ToInt64E(someValue)
flag, err := cast.ToBoolE(someValue)
```

## 📈 Performance considerations

- **Function Registration**: One-time registration, no overhead at runtime
- **Type Conversion**: Efficient type checking and conversion
- **Expression cache**: Expression parsing results can be reused
- **Concurrency Security**: The function registerer supports concurrent access

## 🌟 Roadmap

Implemented features:
- ✅ SELECT DISTINCT
- ✅ LIMIT Clause  
- ✅ HAVING Clause
- ✅ SESSION window
- ✅ Function parameters support expression operations
- ✅ Unified function registration system

Functions to be implemented:
- 🔄 More aggregate functions (MEDIAN, STDDEV, etc.)
- 🔄 Window functions (ROW_NUMBER, RANK, etc.)
- 🔄 More time-date functions
- 🔄 Regular expression function
- 🔄 JSON Handling functions

## 🤝 Contribution

Welcome to submit new function implementations! Please follow these steps:

1. Implement the function in the `functions/` directory
2. Add the corresponding test cases
3. Update the documentation
4. Submit Pull Request

---

*StreamSQL Function system makes stream processing more powerful and flexible! * 🚀 
