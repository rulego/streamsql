# 嵌套字段访问功能

StreamSQL 支持对嵌套结构数据进行查询和聚合操作，提供了强大的字段访问语法，包括点号访问、数组索引、Map键访问等复杂操作。

## 功能特性

- **点号语法访问**：支持 `field.subfield.property` 的访问方式
- **数组索引访问**：支持 `array[0]`、`array[-1]` 等数组元素访问
- **Map键访问**：支持 `map['key']` 和 `map["key"]` 语法访问Map值
- **混合复杂访问**：组合使用点号、数组索引、Map键进行深层嵌套访问
- **负数索引**：支持负数索引从数组末尾开始访问元素
- **完整 SQL 支持**：SELECT、WHERE、GROUP BY、聚合函数中都可以使用嵌套字段
- **类型兼容**：支持 `map[string]interface{}` 和结构体类型的嵌套访问
- **向后兼容**：现有的平坦字段访问方式保持不变

## 支持的访问语法

### 1. 基本点号访问
```sql
SELECT device.info.name, 
       sensor.temperature 
FROM stream
```

### 2. 数组索引访问
```sql
-- 正数索引（从0开始）
SELECT data[0] as first_item,
       sensors[1].temperature as second_sensor_temp,
       matrix[2][1] as matrix_element
FROM stream

-- 负数索引（从末尾开始）
SELECT readings[-1] as latest_reading,
       history[-2] as second_last_event
FROM stream
```

### 3. Map键访问
```sql
-- 字符串键访问
SELECT config['host'] as server_host,
       settings["timeout"] as timeout_value,
       metadata['version'] as app_version
FROM stream
```

### 4. 混合复杂访问
```sql
-- 组合使用各种访问方式
SELECT users[0].profile['name'] as user_name,
       data.items[1][0] as nested_value,
       floors[0].rooms[2]['name'] as room_name,
       sensors[0].readings['temperature'] as temp
FROM stream
```

### 5. 在聚合中使用
```sql
-- 聚合函数中的复杂字段访问
SELECT location,
       AVG(sensors[0].temperature) as avg_first_sensor_temp,
       MAX(sensors[1].humidity) as max_second_sensor_humidity,
       COUNT(*) as device_count
FROM stream 
GROUP BY location, TumblingWindow('5s')
```

## 支持的数据格式

### 基本嵌套结构
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
  }
}
```

### 数组结构
```json
{
  "device": "工业传感器-001",
  "sensors": [
    {"temperature": 25.5, "humidity": 60.2},
    {"temperature": 26.8, "humidity": 58.7},
    {"temperature": 24.1, "humidity": 62.1}
  ],
  "data": ["status_ok", "battery_95%", "signal_strong"]
}
```

### Map键结构
```json
{
  "device_id": "gateway-001",
  "config": {
    "host": "192.168.1.100",
    "port": 8080,
    "protocol": "https"
  },
  "settings": {
    "enable_ssl": true,
    "timeout": 30
  }
}
```

### 复杂混合结构
```json
{
  "building": "智能大厦A座",
  "floors": [
    {
      "floor_number": 1,
      "rooms": [
        {"name": "大厅", "type": "public"},
        {"name": "会议室A", "type": "meeting"}
      ]
    },
    {
      "floor_number": 2,
      "sensors": [
        {
          "id": "sensor-201",
          "readings": {
            "temperature": 23.5,
            "humidity": 58.2
          }
        }
      ]
    }
  ]
}
```

## 使用示例

### 1. 基本嵌套字段查询

```sql
-- 查询设备信息和传感器数据
SELECT device.info.name as device_name, 
       device.location,
       sensor.temperature 
FROM stream
```

### 2. 数组索引访问

```sql
-- 访问数组中的特定元素
SELECT device,
       sensors[0].temperature as first_sensor_temp,
       sensors[1].humidity as second_sensor_humidity,
       data[2] as third_data_item
FROM stream
```

### 3. Map键访问

```sql
-- 使用字符串键访问Map数据
SELECT device_id,
       config['host'] as server_host,
       config["port"] as server_port,
       settings['enable_ssl'] as ssl_enabled
FROM stream
```

### 4. 负数索引访问

```sql
-- 使用负数索引访问数组末尾元素
SELECT device_name,
       readings[-1] as latest_reading,
       tags[-1] as last_tag
FROM stream
```

### 5. 混合复杂访问

```sql
-- 复杂的嵌套访问组合
SELECT building,
       floors[0].rooms[2]['name'] as first_floor_room3,
       floors[1].sensors[0].readings['temperature'] as second_floor_temp,
       metadata.building_info['architect'] as architect
FROM stream
```

### 6. 条件过滤

```sql
-- 在WHERE子句中使用复杂字段访问
SELECT device.info.name, sensor.temperature 
FROM stream 
WHERE device.location = 'room-A' 
  AND sensors[0].temperature > 25.0
  AND config['enable_monitoring'] = true
```

### 7. 聚合查询

```sql
-- 在聚合中使用复杂字段访问
SELECT device.location, 
       AVG(sensors[0].temperature) as avg_temp,
       MAX(sensors[1].humidity) as max_humidity,
       COUNT(*) as sensor_count
FROM stream 
GROUP BY device.location, TumblingWindow('1s')
WITH (TIMESTAMP='timestamp', TIMEUNIT='ss')
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

    // 执行复杂嵌套字段查询
    rsql := `SELECT device.info.name as device_name, 
                    device.location,
                    sensors[0].temperature as first_sensor_temp,
                    config['host'] as server_host,
                    readings[-1] as latest_reading,
                    AVG(sensors[1].humidity) as avg_humidity
             FROM stream 
             WHERE sensors[0].temperature > 20.0
               AND config['enable_monitoring'] = true
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

    // 添加复杂嵌套结构数据
    testData := map[string]interface{}{
        "device": map[string]interface{}{
            "info": map[string]interface{}{
                "name": "temperature-sensor-001",
                "type": "temperature",
            },
            "location": "智能温室-A区",
        },
        "sensors": []interface{}{
            map[string]interface{}{
                "temperature": 25.5,
                "humidity":    60.2,
            },
            map[string]interface{}{
                "temperature": 26.8,
                "humidity":    58.7,
            },
        },
        "config": map[string]interface{}{
            "host":               "192.168.1.100",
            "enable_monitoring":  true,
        },
        "readings": []interface{}{18.5, 19.2, 20.1, 23.5},
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
    "device.location": "智能温室-A区",
    "device_name": "temperature-sensor-001",
    "first_sensor_temp": 25.5,
    "server_host": "192.168.1.100",
    "latest_reading": 23.5,
    "avg_humidity": 59.45
  }
]
```

## 技术实现

复杂嵌套字段访问功能通过以下核心模块实现：

1. **字段路径解析器**：解析复杂的字段访问路径，支持点号、数组索引、Map键等语法
2. **访问器引擎**：根据解析结果进行实际的数据访问操作
3. **词法分析器扩展**：支持方括号、引号等特殊字符的词法分析
4. **表达式引擎**：在字段访问时检查是否为嵌套字段并使用相应的访问方法
5. **聚合器增强**：支持嵌套字段作为分组键和聚合目标
6. **流处理器优化**：在数据处理过程中高效支持复杂字段访问

### 支持的字段访问类型

- **field**：普通字段访问（`name`、`user.profile`）
- **array_index**：数组索引访问（`data[0]`、`items[-1]`）
- **map_key**：Map键访问（`config['host']`、`settings["timeout"]`）

### 解析示例

```
字段路径: users[0].profile['name']
解析结果:
  - {Type: "field", Name: "users"}
  - {Type: "array_index", Index: 0}
  - {Type: "field", Name: "profile"}
  - {Type: "map_key", Key: "name", KeyType: "string"}
```

## 性能考虑

- **复杂度影响**：复杂嵌套访问比简单字段访问略慢，因为需要逐层解析
- **负数索引成本**：负数索引需要计算数组长度，会有轻微性能影响
- **缓存机制**：字段路径解析结果会被缓存以提高重复访问性能
- **建议实践**：在高频查询中避免过深的嵌套层级（建议不超过5层）

## 注意事项

1. **索引范围**：数组索引从0开始，负数索引从-1开始（最后一个元素）
2. **键格式**：Map键访问支持单引号和双引号，键名区分大小写
3. **错误处理**：访问不存在的索引或键会返回null值，不会抛出异常
4. **字段名保持**：复杂字段路径在结果中会保持完整格式，建议使用AS别名
5. **类型安全**：所有访问操作都是类型安全的，支持自动类型推断
6. **兼容性**：完全向后兼容原有的简单点号访问方式

## 错误处理

- **解析错误**：无效的字段路径格式会在SQL解析阶段报错
- **访问错误**：运行时访问不存在的字段会返回null，不会中断查询
- **类型错误**：在非数组数据上使用数组索引会返回null
- **边界检查**：数组索引超出范围会安全返回null

## 最佳实践

1. **使用别名**：为复杂字段路径提供清晰的别名
2. **验证数据**：在生产环境中验证输入数据的结构
3. **合理嵌套**：避免过深的嵌套层级以保持性能
4. **错误预期**：预期并处理字段不存在的情况
5. **性能测试**：在大数据量场景下进行性能测试 