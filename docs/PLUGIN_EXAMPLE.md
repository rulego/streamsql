# StreamSQL 插件式自定义函数快速示例

## 🚀 5分钟上手插件式扩展

### 1️⃣ 注册自定义函数

```go
package main

import (
    "fmt"
    "github.com/rulego/streamsql"
    "github.com/rulego/streamsql/functions"
)

func main() {
    // 🔌 插件式注册 - 数据脱敏函数
    functions.RegisterCustomFunction(
        "mask_email",           // 函数名
        functions.TypeString,   // 函数类型
        "数据脱敏",             // 分类
        "邮箱地址脱敏",         // 描述
        1, 1,                  // 参数数量
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            email, _ := functions.ConvertToString(args[0])
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
    
    // 🔌 插件式注册 - 业务计算函数  
    functions.RegisterCustomFunction(
        "calculate_score",
        functions.TypeMath,
        "业务计算", 
        "计算用户评分",
        2, 2,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            base, _ := functions.ConvertToFloat64(args[0])
            bonus, _ := functions.ConvertToFloat64(args[1])
            return base + bonus*0.1, nil
        },
    )
    
    // 🔌 插件式注册 - 状态转换函数
    functions.RegisterCustomFunction(
        "format_status",
        functions.TypeConversion,
        "状态转换",
        "格式化状态显示", 
        1, 1,
        func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
            status, _ := functions.ConvertToString(args[0])
            switch status {
            case "1": return "✅ 活跃", nil
            case "0": return "❌ 非活跃", nil
            default: return "❓ 未知", nil
            }
        },
    )
}
```

### 2️⃣ 立即在SQL中使用

```go
func demonstrateUsage() {
    ssql := streamsql.New()
    defer ssql.Stop()
    
    // 🎯 直接在SQL中使用新注册的函数 - 无需修改任何核心代码！
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
    
    // 添加结果监听
    ssql.Stream().AddSink(func(result interface{}) {
        fmt.Printf("处理结果: %v\n", result)
    })
    
    // 添加测试数据
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
    
    // 等待结果
    time.Sleep(6 * time.Second)
}
```

### 3️⃣ 运行结果

```json
{
  "user_id": "U001",
  "safe_email": "jo***e@example.com",
  "status_display": "✅ 活跃", 
  "avg_score": 86.35
}
```

## 🔥 核心优势

### ✅ 完全插件式
- **无需修改SQL解析器** - 新函数自动识别
- **无需重启应用** - 运行时动态注册
- **无需额外配置** - 注册后立即可用

### ✅ 智能处理
- **字符串函数** → 直接处理模式（低延迟）
- **数学函数** → 窗口聚合模式（支持统计）
- **转换函数** → 直接处理模式（实时转换）

### ✅ 灵活管理
```go
// 运行时管理
fn, exists := functions.Get("mask_email")           // 查询函数
mathFuncs := functions.GetByType(functions.TypeMath) // 按类型查询
allFuncs := functions.ListAll()                     // 列出所有函数
success := functions.Unregister("old_function")     // 注销函数
```

## 🎯 实际应用场景

### 📊 数据脱敏
```sql
SELECT 
    mask_email(email) as safe_email,
    mask_phone(phone) as safe_phone
FROM user_stream
```

### 💼 业务计算
```sql
SELECT 
    user_id,
    AVG(calculate_commission(sales, rate)) as avg_commission,
    SUM(calculate_bonus(performance, level)) as total_bonus
FROM sales_stream 
GROUP BY user_id, TumblingWindow('1h')
```

### 🔄 状态转换
```sql
SELECT 
    order_id,
    format_status(status_code) as readable_status,
    format_priority(priority_level) as priority_display
FROM order_stream
```

### 🌐 多语言支持
```go
// 注册多语言函数
functions.RegisterCustomFunction("translate", functions.TypeString, ...,
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        text := args[0].(string)
        lang := args[1].(string)
        return translateService.Translate(text, lang), nil
    })

// SQL中使用
// SELECT translate(message, 'zh-CN') as chinese_message FROM stream
```

## 🏁 总结

StreamSQL 的插件式自定义函数系统让你能够：

1. **🔌 即插即用** - 注册函数后立即在SQL中使用
2. **🚀 零停机扩展** - 运行时动态增加功能 
3. **🎯 类型智能** - 根据函数类型自动选择最优处理模式
4. **📈 无限可能** - 支持任意复杂的业务逻辑

**真正实现了"写一个函数，SQL立即可用"的插件式体验！** ✨ 