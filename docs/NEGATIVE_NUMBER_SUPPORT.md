# StreamSQL 负数支持文档

## 概述

StreamSQL 现在全面支持负数在 CASE 表达式中的使用。本文档总结了负数支持的完善情况、支持范围和使用建议。

## ✅ 已支持的负数用法

### 1. 基本负数常量

```sql
-- CASE 表达式中的负数常量
CASE WHEN temperature > 0 THEN 1 ELSE -1 END

-- 负数小数
CASE WHEN temperature > 0 THEN 1.5 ELSE -2.5 END

-- 负零
CASE WHEN temperature = -0 THEN 1 ELSE 0 END
```

### 2. 比较运算符后的负数

```sql
-- 比较运算符后直接跟负数
CASE WHEN temperature < -10 THEN 'FREEZING' ELSE 'NORMAL' END
CASE WHEN temperature >= -5.5 THEN 'ABOVE' ELSE 'BELOW' END
CASE WHEN temperature > -20 THEN 'WARM' ELSE 'COLD' END
```

### 3. 简单 CASE 表达式中的负数

```sql
-- 简单 CASE 中使用负数作为匹配值
CASE temperature 
  WHEN -10 THEN 'FROZEN'
  WHEN -5 THEN 'COLD'
  WHEN 0 THEN 'ZERO'
  ELSE 'OTHER'
END
```

### 4. 算术表达式中的负数

```sql
-- 括号内的负数运算
CASE WHEN temperature + (-10) > 0 THEN 1 ELSE 0 END
CASE WHEN (temperature * -1) > 10 THEN 1 ELSE 0 END
```

## ⚠️ 部分支持或限制

### 1. 函数参数中的负数表达式

```sql
-- 当前不完全支持：函数参数中的负数变量
CASE WHEN ABS(-temperature) > 10 THEN 1 ELSE 0 END  -- ❌ 

-- 推荐替代方案：使用括号或先计算
CASE WHEN ABS(temperature * -1) > 10 THEN 1 ELSE 0 END  -- ✅
```

### 2. BETWEEN 语句中的负数范围

```sql
-- 当前不支持：BETWEEN 与负数组合
CASE WHEN temperature BETWEEN -20 AND -10 THEN 1 ELSE 0 END  -- ❌

-- 推荐替代方案：使用比较运算符
CASE WHEN temperature >= -20 AND temperature <= -10 THEN 1 ELSE 0 END  -- ✅
```

### 3. SQL 中的空格分隔负数

```sql
-- 避免在 SQL 中使用空格分隔的负数
SELECT CASE WHEN temperature < - 10 THEN 'COLD' END  -- ❌ 解析问题

-- 推荐写法：紧密连接或使用括号
SELECT CASE WHEN temperature < -10 THEN 'COLD' END   -- ✅
SELECT CASE WHEN temperature < (-10) THEN 'COLD' END -- ✅
```

## 🔧 技术实现

### 词法分析器增强

1. **智能负数识别**：
   - 识别比较运算符后的负数（`<`, `>`, `<=`, `>=`, `==`, `!=`）
   - 支持逻辑运算符后的负数（`AND`, `OR`）
   - 支持 CASE 关键字后的负数（`WHEN`, `THEN`, `ELSE`）

2. **连续运算符检查优化**：
   - 允许比较运算符后跟负数的合法组合
   - 智能区分负数与减号运算符

3. **空格处理**：
   - 正确处理空格分隔的负数标记
   - 改进 token 化过程以支持各种负数格式

### 表达式求值增强

1. **负数常量解析**：完全支持负整数和负小数
2. **类型转换**：正确处理负数的数值转换
3. **NULL 值处理**：负数与 NULL 值的正确交互

## 📊 测试覆盖

### 表达式级别测试

- ✅ 负数常量在 THEN/ELSE 中
- ✅ 负数常量在 WHEN 条件中  
- ✅ 负数小数支持
- ✅ 负数在算术表达式中
- ✅ 负数在简单 CASE 中
- ✅ 负零处理

### SQL 集成测试

- ✅ 完整 SQL 语句中的负数支持
- ✅ 非聚合查询中的负数表达式
- ✅ 聚合查询中的负数处理

## 🎯 使用建议

### 1. 推荐的负数写法

```sql
-- ✅ 推荐：紧密连接的负数
CASE WHEN temperature < -10 THEN 'FREEZING' END

-- ✅ 推荐：括号包围的负数（最安全）
CASE WHEN temperature < (-10) THEN 'FREEZING' END

-- ✅ 推荐：负数小数
CASE WHEN temperature < -10.5 THEN 'FREEZING' END
```

### 2. 避免的写法

```sql
-- ❌ 避免：空格分隔的负数
CASE WHEN temperature < - 10 THEN 'FREEZING' END

-- ❌ 避免：复杂的负数表达式在函数中
CASE WHEN ABS(-temperature) > 10 THEN 1 END
```

### 3. 最佳实践

1. **使用括号**：当不确定负数解析时，总是使用括号包围负数
2. **避免空格**：在负号和数字之间不要添加空格
3. **测试验证**：对包含负数的复杂表达式进行充分测试
4. **版本兼容**：确保使用的 StreamSQL 版本支持所需的负数功能

## 🚀 未来改进计划

1. **完全支持函数参数中的负数表达式**
2. **支持 BETWEEN 语句中的负数范围**
3. **改进 SQL 解析器对空格分隔负数的处理**
4. **扩展负数支持到更多数学和字符串函数**

## 示例代码

```go
package main

import (
    "fmt"
    "github.com/rulego/streamsql"
)

func main() {
    // 创建 StreamSQL 实例
    sql := streamsql.New()
    defer sql.Stop()

    // 包含负数的 SQL 查询
    query := `
        SELECT deviceId,
               temperature,
               CASE 
                 WHEN temperature < -10 THEN 'FREEZING'
                 WHEN temperature < 0 THEN 'COLD'
                 WHEN temperature = 0 THEN 'ZERO'
                 ELSE 'POSITIVE'
               END as temp_category,
               CASE 
                 WHEN temperature > 0 THEN temperature 
                 ELSE (-1.0)
               END as adjusted_temp
        FROM stream
    `

    // 执行查询
    err := sql.Execute(query)
    if err != nil {
        fmt.Printf("执行失败: %v\n", err)
        return
    }

    // 添加数据处理器
    sql.AddSink(func(result interface{}) {
        fmt.Printf("结果: %+v\n", result)
    })

    // 添加测试数据
    testData := []map[string]interface{}{
        {"deviceId": "sensor1", "temperature": -15.0},
        {"deviceId": "sensor2", "temperature": -5.0},
        {"deviceId": "sensor3", "temperature": 0.0},
        {"deviceId": "sensor4", "temperature": 10.0},
    }

    for _, data := range testData {
        sql.AddData(data)
    }
}
```

---

**更新日期**: 2025-06-17  
**版本**: StreamSQL v0.x  
**作者**: StreamSQL 开发团队 