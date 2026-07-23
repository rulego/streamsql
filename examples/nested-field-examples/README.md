# StreamSQL Complete demonstration of nested field access functionality

This demo demonstrates StreamSQL's complete nested field access features, including basic dot point access, complex array indexing, Map key access, and hybrid operations.

## Feature Overview

### Basic Features
- **Point-Marked Syntax Access**: Use `field.subfield.property` to access nested structures
- **Conditional Filtering**: Use nested fields in WHERE clauses for data filtering
- **Aggregate Computation**: Perform GROUP BY and aggregation operations on nested fields

### Advanced Features
- **Array Index Access**: Use syntax such as `array[0]`, `array[-1]`, etc. to access array elements
- **Map key access to**: Supports `map['key']` and `map["key"]` syntax access to Map values
- **Hybrid complex access**: Combines dot numbers, array indexes, and Map keys for deep nested access
- **Negative Index**: Supports accessing elements from the end of the array using negative indexes
- **Complex Aggregation**: Use complex fields in aggregation functions to access paths

## Run the demo

```bash
cd examples/nested-field-examples
go run main.go
```

## Presentation Content

### Part One: Basic Nested Field Access

#### Example SQL
```sql
SELECT device.info.name as device_name, 
       device.location,
       sensor.temperature,
       sensor.humidity
FROM stream 
WHERE device.location = 'room-A' 
  AND sensor.temperature > 20
```

#### Test data structures
```json
{
  "device": {
    "info": {
      "name": "温度传感器-001",
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

### Part Two: Nested Field Aggregation

#### Example SQL
```sql
SELECT device.location, 
       AVG(sensor.temperature) as avg_temp,
       MAX(sensor.humidity) as max_humidity,
       COUNT(*) as sensor_count
FROM stream 
GROUP BY device.location, TumblingWindow('2s')
WITH (TIMESTAMP='timestamp', TIMEUNIT='ss')
```

### Part Three: Accessing Complex Nested Fields

#### Demo 1: Array Index Access

**SQL Query**
```sql
SELECT device, 
       sensors[0].temperature as first_sensor_temp,
       sensors[1].humidity as second_sensor_humidity,
       data[2] as third_data_item
FROM stream
```

**Data Structure**
```json
{
  "device": "工业传感器-001",
  "sensors": [
    {"temperature": 25.5, "humidity": 60.2},
    {"temperature": 26.8, "humidity": 58.7},
    {"temperature": 24.1, "humidity": 62.1}
  ],
  "data": ["status_ok", "battery_95%", "signal_strong", "location_A1"]
}
```

#### Demo 2: Map Key Access

**SQL Query**
```sql
SELECT device_id,
       config['host'] as server_host,
       config["port"] as server_port,
       settings['enable_ssl'] as ssl_enabled,
       metadata["version"] as app_version
FROM stream
```

**Data Structure**
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

#### Demo 3: Hybrid Complex Access

**SQL Query**
```sql
SELECT building,
       floors[0].rooms[2]['name'] as first_floor_room3_name,
       floors[1].sensors[0].readings['temperature'] as second_floor_first_sensor_temp,
       metadata.building_info['architect'] as building_architect,
       alerts[-1].message as latest_alert
FROM stream
```

**Data Structure**
```json
{
  "building": "智能大厦A座",
  "floors": [
    {
      "floor_number": 1,
      "rooms": [
        {"name": "大厅", "type": "public"},
        {"name": "接待室", "type": "office"},
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

#### Demo 4: Negative Index Access

**SQL Query**
```sql
SELECT device_name,
       readings[-1] as latest_reading,
       history[-2] as second_last_event,
       tags[-1] as last_tag
FROM stream
```

**Data Structure**
```json
{
  "device_name": "温度监测器-Alpha",
  "readings": [18.5, 19.2, 20.1, 21.3, 22.8, 23.5],
  "history": ["boot", "calibration", "running", "alert", "resolved"],
  "tags": ["indoor", "critical", "monitored"]
}
```

#### Demo 5: Array Index Aggregation Calculation

**SQL Query**
```sql
SELECT location,
       AVG(sensors[0].temperature) as avg_first_sensor_temp,
       MAX(sensors[1].humidity) as max_second_sensor_humidity,
       COUNT(*) as device_count
FROM stream 
GROUP BY location, TumblingWindow('2s')
WITH (TIMESTAMP='timestamp', TIMEUNIT='ss')
```

## Summary of supported access syntax

| Grammatical type | Example | Note |
|---------|------|------|
| Click access | `device.info.name` | Basic nested field access |
| Array positive index | `items[0]`, `data[2]` | Array index starting from 0 |
| Array negative index | `items[-1]`, `data[-2]` | Array index |, starting from the end
| Map Single quote key | `config['host']` | Use the single-quoted Map key to access |
| Map Double quotation key | `settings["timeout"]` | Access |using the Map key in double quotes
| Hybrid Access | `users[0].profile['name']` | Combine multiple access methods |
| Multidimensional array | `matrix[1][2]` | Two-dimensional or multidimensional array access |

## Example of expected output

### Basic Nested Field Access Results
```
📊 第一部分：基础嵌套字段访问
  📋 基础嵌套字段访问结果:
    记录 1:
      设备名称: 温度传感器-001
      设备位置: room-A
      温度: 25.5°C
      湿度: 60.2%
```

### Array index access result
```
📊 演示1: 数组索引访问
  📋 数组索引访问结果:
    记录 1:
      设备: 工业传感器-001
      第一个传感器温度: 25.5°C
      第二个传感器湿度: 58.7%
      第三个数据项: signal_strong
```

### Map key to access results
```
🗝️ 演示2: Map键访问
  🗝️ Map键访问结果:
    记录 1:
      设备ID: gateway-001
      服务器主机: 192.168.1.100
      服务器端口: 8080
      SSL启用: true
      应用版本: v2.1.3
```

### Aggregated calculation results
```
📈 演示5: 数组索引聚合计算
  📈 数组索引聚合计算结果:
    聚合结果 1:
      位置: 车间A
      第一个传感器平均温度: 24.83°C
      第二个传感器最大湿度: 68.2%
      设备数量: 4
```

## Technical Features

### Performance Optimization
- Caching field path parsing results
- Efficient nested access algorithms
- Minimizes memory allocation

### Error Handling
- Secure array boundary checks
- Elegant null value processing
- Type-safe access operations

### Compatibility
- Fully backward compatible with simple field access
- Supports all existing SQL features
- Seamless integration of aggregation and window functions

## Usage Recommendations

1. **Field Aliases**: Use AS Aliases for complex paths to improve readability
2. **Data Validation**: Verify the consistency of data structures in production environments
3. **Performance Considerations**: Avoid overly deep nested layers (recommended not to exceed 5 layers)
4. **Error Expectation**: Anticipate and handle cases where fields do not exist
5. **Test coverage**: Comprehensive testing of various data structures and boundary situations

## Notes

- The array index starts from 0, and the negative index starts at -1
- Map Key access is case-sensitive
- Returns null to non-existent fields without throwing exceptions
- All operations are thread-safe
- Supports real-time stream processing and batch processing modes 
