# 嵌套字段访问功能

StreamSQL 支持对嵌套结构数据进行查询和聚合操作，可以使用点号（`.`）语法访问嵌套字段。

## 功能特性

- **点号语法访问**：支持 `field.subfield.property` 的访问方式
- **完整 SQL 支持**：SELECT、WHERE、GROUP BY、聚合函数中都可以使用嵌套字段
- **类型兼容**：支持 `map[string]interface{}` 和结构体类型的嵌套访问
- **向后兼容**：现有的平坦字段访问方式保持不变

## 支持的数据格式

```json
{
  "device": {
    "info": {
      "name": "sensor-001",
      "type": "temperature"
    },
    "location": "room-A"
  },
  "sensor": {
    "temperature": 25.5,
    "humidity": 60.2
  },
  "timestamp": "2023-01-01T10:00:00Z"
}
```

## 使用示例

### 1. 基本查询

```sql
-- 查询设备信息和传感器数据
SELECT device.info.name as device_name, 
       device.location,
       sensor.temperature 
FROM stream
```

### 2. 条件过滤

```sql
-- 筛选特定房间的数据
SELECT device.info.name, sensor.temperature 
FROM stream 
WHERE device.location = 'room-A'
```

### 3. 聚合查询

```sql
-- 按房间统计平均温度
SELECT device.location, 
       AVG(sensor.temperature) as avg_temp,
       COUNT(*) as sensor_count
FROM stream 
GROUP BY device.location, TumblingWindow('1s')
WITH (TIMESTAMP='timestamp', TIMEUNIT='ss')
```

### 4. 复杂嵌套访问

```sql
-- 访问深层嵌套字段
SELECT device.info.name,
       device.info.type,
       sensor.temperature,
       sensor.humidity
FROM stream
WHERE device.info.type = 'temperature'
```

## 实际应用示例

```go
package main

import (
    "fmt"
    "time"
    "github.com/rulego/streamsql"
)

func main() {
    // 创建 StreamSQL 实例
    ssql := streamsql.New()
    defer ssql.Stop()

    // 执行嵌套字段查询
    rsql := `SELECT device.info.name as device_name, 
                    device.location,
                    AVG(sensor.temperature) as avg_temp
             FROM stream 
             GROUP BY device.location, TumblingWindow('5s')
             WITH (TIMESTAMP='timestamp', TIMEUNIT='ss')`
    
    err := ssql.Execute(rsql)
    if err != nil {
        panic(err)
    }

    // 添加数据处理回调
    ssql.Stream().AddSink(func(result interface{}) {
        fmt.Printf("聚合结果: %+v\n", result)
    })

    // 添加嵌套结构数据
    testData := map[string]interface{}{
        "device": map[string]interface{}{
            "info": map[string]interface{}{
                "name": "sensor-001",
                "type": "temperature",
            },
            "location": "room-A",
        },
        "sensor": map[string]interface{}{
            "temperature": 25.5,
            "humidity":    60.2,
        },
        "timestamp": time.Now().Unix(),
    }

    // 推送数据到流
    ssql.Stream().AddData(testData)
}
```

## 输出结果示例

```json
[
  {
    "device.location": "room-A",
    "avg_temp": 23.9
  },
  {
    "device.location": "room-B", 
    "avg_temp": 28.1
  }
]
```

## 技术实现

嵌套字段访问功能通过以下核心模块实现：

1. **词法分析器**：修改标识符解析，支持点号作为标识符的一部分
2. **工具函数**：提供 `GetNestedField()` 和 `SetNestedField()` 函数
3. **表达式引擎**：在字段访问时检查是否为嵌套字段并使用相应的访问方法
4. **聚合器**：支持嵌套字段作为分组键和聚合目标
5. **流处理器**：在数据处理过程中支持嵌套字段访问

## 性能考虑

- 嵌套字段访问比平坦字段略慢，因为需要逐层解析
- 建议在高频查询中避免过深的嵌套层级
- 对于频繁访问的嵌套字段，可以考虑在数据预处理阶段展平结构

## 注意事项

1. 字段路径区分大小写
2. 如果嵌套路径中某个层级不存在，该字段值将为 `null`
3. 嵌套字段名在结果中会保持完整路径（如 `device.location`），除非使用 `AS` 别名
4. 支持任意深度的嵌套，但建议控制在合理范围内以保证性能 