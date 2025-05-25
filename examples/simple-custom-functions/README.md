# 简单自定义函数示例

## 简介

这个示例展示了如何使用StreamSQL的插件式自定义函数系统注册和使用简单的自定义函数。

## 功能演示

- ✅ 数学函数：平方计算、华氏度转摄氏度、圆面积计算
- ✅ 直接SQL查询模式和聚合查询模式
- ✅ 函数管理功能：查询、分类、统计

## 运行方式

```bash
cd examples/simple-custom-functions
go run main.go
```

## 代码亮点

### 1. 简单函数注册
```go
functions.RegisterCustomFunction(
    "square",               // 函数名
    functions.TypeMath,     // 函数类型
    "数学函数",             // 分类
    "计算平方",             // 描述
    1, 1,                  // 参数数量
    func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
        val, _ := functions.ConvertToFloat64(args[0])
        return val * val, nil
    },
)
```

### 2. SQL中直接使用
```sql
SELECT square(value) as squared_value FROM stream
```

### 3. 聚合查询
```sql
SELECT AVG(square(value)) as avg_squared FROM stream GROUP BY device, TumblingWindow('1s')
```

## 适用场景

- 🔰 初学者入门StreamSQL自定义函数
- 📚 学习插件式函数注册机制
- 🧪 快速验证函数功能 