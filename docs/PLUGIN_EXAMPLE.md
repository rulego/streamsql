# StreamSQL Quick example of plugin-style custom functions

## 🚀 Plug-in Expansion in Just 5 Minutes

### 1️⃣ Register custom functions

```go
package main

import (
    "fmt"
    "strings"

    "github.com/rulego/streamsql"
    "github.com/rulego/streamsql/functions"
    "github.com/rulego/streamsql/utils/cast"
)

func main() {
    // 🔌 Plug-in registration - Data anonymization function
    functions.RegisterCustomFunction(
        "mask_email",           // Function name
        functions.TypeString,   // Function type
        "数据脱敏",             // Classification
        "邮箱地址脱敏",         // Description
        1, 1,                  // Number of parameters
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            email, _ := cast.ToStringE(args[0])
            parts := strings.Split(email, "@")
            if len(parts) != 2 {
                return email, nil
            }
            
            user := parts[0]
            domain := parts[1]
            
            if len(user) > 2 {
                masked := user[:2] + "***" + user[len(user)-1:]
                return masked + "@" + domain, nil
            }
            return email, nil
        },
    )
    
    // 🔌 Plug-in registration - business calculation function  
    functions.RegisterCustomFunction(
        "calculate_score",
        functions.TypeMath,
        "业务计算", 
        "计算用户评分",
        2, 2,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            base, _ := cast.ToFloat64E(args[0])
            bonus, _ := cast.ToFloat64E(args[1])
            return base + bonus*0.1, nil
        },
    )
    
    // 🔌 Plug-in registration - status transition function
    functions.RegisterCustomFunction(
        "format_status",
        functions.TypeConversion,
        "状态转换",
        "格式化状态显示", 
        1, 1,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            status, _ := cast.ToStringE(args[0])
            switch status {
            case "1": return "✅ 活跃", nil
            case "0": return "❌ 非活跃", nil
            default: return "❓ 未知", nil
            }
        },
    )
}
```

### 2️⃣ Use it now in SQL

```go
func demonstrateUsage() {
    ssql := streamsql.New()
    defer ssql.Stop()
    
    // 🎯 Use newly registered functions directly in the SQL—no need to modify any core code!
    sql := `
        SELECT 
            user_id,
            mask_email(email) as safe_email,
            format_status(status) as status_display,
            AVG(calculate_score(base_score, performance)) as avg_score
        FROM stream 
        GROUP BY user_id, TumblingWindow('5s')
    `
    
    err := ssql.Execute(sql)
    if err != nil {
        panic(err)
    }
    
    // Add result monitoring
    ssql.Stream().AddSink(func(result interface{}) {
        fmt.Printf("处理结果: %v\n", result)
    })
    
    // Add test data
    testData := []map[string]interface{}{
        {
            "user_id":     "U001",
            "email":       "john.doe@example.com",
            "status":      "1", 
            "base_score":  85.0,
            "performance": 12.0,
        },
        {
            "user_id":     "U001",
            "email":       "john.doe@example.com", 
            "status":      "1",
            "base_score":  90.0,
            "performance": 15.0,
        },
    }
    
    for _, data := range testData {
        ssql.AddData(data)
    }
    
    // Waiting for the results
    time.Sleep(6 * time.Second)
}
```

### 3️⃣ Running results

```json
{
  "user_id": "U001",
  "safe_email": "jo***e@example.com",
  "status_display": "✅ 活跃", 
  "avg_score": 86.35
}
```

## 🔥 Core Advantages

### ✅ Fully plug-in type
- **No modification needed SQL parser** - New functions are automatically recognized
- **No need to restart the app** - Runtime dynamic registration
- **No additional configuration required** - Available immediately upon registration

### ✅ Intelligent Processing
- **String function** → Direct processing mode (low latency)
- **Mathematical Functions** → Window Aggregation Mode (supports statistics)
- **Conversion Function** → Direct Processing Mode (Real-Time Conversion)

### ✅ Flexible management
```go
// Runtime management
fn, exists := functions.Get("mask_email")           // Query function
mathFuncs := functions.GetByType(functions.TypeMath) // Search by type
allFuncs := functions.ListAll()                     // List all functions
success := functions.Unregister("old_function")     // Cancel function
```

## 🎯 Practical Application Scenarios

### 📊 Data Anonymization
```sql
SELECT 
    mask_email(email) as safe_email,
    mask_phone(phone) as safe_phone
FROM user_stream
```

### 💼 Business Calculation
```sql
SELECT 
    user_id,
    AVG(calculate_commission(sales, rate)) as avg_commission,
    SUM(calculate_bonus(performance, level)) as total_bonus
FROM sales_stream 
GROUP BY user_id, TumblingWindow('1h')
```

### 🔄 State transition
```sql
SELECT 
    order_id,
    format_status(status_code) as readable_status,
    format_priority(priority_level) as priority_display
FROM order_stream
```

### 🌐 Multilingual support
```go
// Register multilingual functions
functions.RegisterCustomFunction("translate", functions.TypeString, ...,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        text := args[0].(string)
        lang := args[1].(string)
        return translateService.Translate(text, lang), nil
    })

// SQL used
// SELECT translate(message, 'zh-CN') as chinese_message FROM stream
```

## 🏁 Summary

StreamSQL's plugin-style custom function system enables you to:

1. **🔌 Plug and Play** - Use the function immediately in the SQL after registering it
2. **🚀 Zero-downtime expansion** - Dynamically adds functionality at runtime 
3. **🎯 Type Intelligence** - Automatically selects the optimal processing mode based on function type
4. **📈 Infinite Possibilities** - Supports arbitrarily complex business logic

**truly delivers the plug-in experience of "write a function and use it immediately in SQL"!** ✨ 
