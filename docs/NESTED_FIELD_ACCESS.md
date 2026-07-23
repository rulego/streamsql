# Nested field access feature

StreamSQL Supports querying and aggregating nested structure data, providing powerful field access syntax including dot access, array indexing, Map key access, and other complex operations.

## Functional Features

- **Dotted Syntax Access**: Supports `field.subfield.property` access
- **Array Index Access**: Supports access to array elements such as `array[0]` and `array[-1]`
- **Map key access to**: Supports `map['key']` and `map["key"]` syntax access to Map values
- **Hybrid complex access**: Combines dot numbers, array indexes, and Map keys for deep nested access
- **Negative Index**: Supports accessing elements from the end of the array using negative indexes
- **Full SQL supports nested fields in**: SELECT, WHERE, GROUP BY, and aggregate functions
- **type compatibility**: Supports nested access to `map[string]interface{}` and struct types
- **Backward Compatibility**: The existing flat field access method remains unchanged

## Supported access syntax

### 1. Basic point number access
```sql
SELECT device.info.name, 
       sensor.temperature 
FROM stream
```

### 2. Array index access
```sql
-- Positive Index (starting from 0)
SELECT data[0] as first_item,
       sensors[1].temperature as second_sensor_temp,
       matrix[2][1] as matrix_element
FROM stream

-- Negative Index (starting from the end)
SELECT readings[-1] as latest_reading,
       history[-2] as second_last_event
FROM stream
```

### 3. Map Key access
```sql
-- String key access
SELECT config['host'] as server_host,
       settings["timeout"] as timeout_value,
       metadata['version'] as app_version
FROM stream
```

### 4. Hybrid complex access
```sql
-- Combine various access methods
SELECT users[0].profile['name'] as user_name,
       data.items[1][0] as nested_value,
       floors[0].rooms[2]['name'] as room_name,
       sensors[0].readings['temperature'] as temp
FROM stream
```

### 5. Used in aggregation
```sql
-- Complex field access in aggregate functions
SELECT location,
       AVG(sensors[0].temperature) as avg_first_sensor_temp,
       MAX(sensors[1].humidity) as max_second_sensor_humidity,
       COUNT(*) as device_count
FROM stream 
GROUP BY location, TumblingWindow('5s')
```

## Supported Data Formats

### Basic nested structure
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

### Array Structure
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

### Map Key structure
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

### Complex Hybrid Structures
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

## Usage Examples

### 1. Basic nested field queries

```sql
-- Query device information and sensor data
SELECT device.info.name as device_name, 
       device.location,
       sensor.temperature 
FROM stream
```

### 2. Array index access

```sql
-- Access specific elements in the array
SELECT device,
       sensors[0].temperature as first_sensor_temp,
       sensors[1].humidity as second_sensor_humidity,
       data[2] as third_data_item
FROM stream
```

### 3. Map Key access

```sql
-- Use string keys to access Map data
SELECT device_id,
       config['host'] as server_host,
       config["port"] as server_port,
       settings['enable_ssl'] as ssl_enabled
FROM stream
```

### 4. Negative index access

```sql
-- Use a negative index to access the last element of an array
SELECT device_name,
       readings[-1] as latest_reading,
       tags[-1] as last_tag
FROM stream
```

### 5. Hybrid complex access

```sql
-- Complex nested access combinations
SELECT building,
       floors[0].rooms[2]['name'] as first_floor_room3,
       floors[1].sensors[0].readings['temperature'] as second_floor_temp,
       metadata.building_info['architect'] as architect
FROM stream
```

### 6. Conditional filtering

```sql
-- Use complex fields to access WHERE clauses
SELECT device.info.name, sensor.temperature 
FROM stream 
WHERE device.location = 'room-A' 
  AND sensors[0].temperature > 25.0
  AND config['enable_monitoring'] = true
```

### 7. Aggregated queries

```sql
-- Use complex field access in aggregation
SELECT device.location, 
       AVG(sensors[0].temperature) as avg_temp,
       MAX(sensors[1].humidity) as max_humidity,
       COUNT(*) as sensor_count
FROM stream 
GROUP BY device.location, TumblingWindow('1s')
WITH (TIMESTAMP='timestamp', TIMEUNIT='ss')
```

## Practical Application Examples

```go
package main

import (
    "fmt"
    "time"
    "github.com/rulego/streamsql"
)

func main() {
    // Create StreamSQL instances
    ssql := streamsql.New()
    defer ssql.Stop()

    // Perform complex nested field queries
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

    // Add data processing callbacks
    ssql.Stream().AddSink(func(result interface{}) {
        fmt.Printf("聚合结果: %+v\n", result)
    })

    // Add complex nested structure data
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

    // Push data to the stream
    ssql.Stream().AddData(testData)
}
```

## Example output results

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

## Technical Implementation

Complex nested field access functionality is implemented through the following core modules:

1. **Field Path Parser**: Parses complex field access paths, supports point, array index, Map key, and other syntax
2. **Accessor Engine**: Performs actual data access operations based on parsing results
3. **Lexical analyzer extension**: Supports lexical analysis of special characters such as square brackets and quotation marks
4. **Expression Engine**: Checks whether the field is nested during field access and uses the corresponding access method
5. **Aggregator Enhancement**: Supports nested fields as group keys and aggregation targets
6. **Stream Processor Optimization**: Efficiently supports complex field access during data processing

### Supported types of field access

- **field**: Regular field access (`name`, `user.profile`)
- **array_index**: Array index access (`data[0]`, `items[-1]`)
- **map_key**: Map key access (`config['host']`, `settings["timeout"]`)

### Explanation Example

```
字段路径: users[0].profile['name']
解析结果:
  - {Type: "field", Name: "users"}
  - {Type: "array_index", Index: 0}
  - {Type: "field", Name: "profile"}
  - {Type: "map_key", Key: "name", KeyType: "string"}
```

## Performance considerations

- **Complexity Impact**: Complex nested access is slightly slower than simple field access because it requires layer-by-layer parsing
- **Cost of Negative Indexes**: Negative indexes require calculating array length, which may have a slight performance impact
- **Caching Mechanism**: Field path parsing results are cached to improve repeat access performance
- **Recommended Practice**: Avoid overly deep nested layers in high-frequency queries (recommended no more than 5 layers)

## Notes

1. **Index Range**: Array index starts from 0, negative index starts at -1 (last element)
2. **Key Format**: Map Key access supports single and double quotes, with case sensitivity
3. **Error Handling**: Accessing non-existent indexes or keys returns null values without throwing exceptions
4. **Keep field names**: Complex field paths will remain fully formatted in the results, so it is recommended to use AS aliases
5. **Type-safe**: All access operations are type-safe and support automatic type inference
6. **Compatibility**: Fully backward compatible with the original simple dot-point access method

## Error Handling

- **Parsing Error**: Invalid field path formats will cause errors during the SQL parsing phase
- **Access error**: Accessing fields that do not exist at runtime returns null and does not interrupt the query
- **Type Error**: Using an array index on non-array data returns null
- **Boundary Check**: If the array index goes out of range, it will safely return null

## Best Practices

1. **Use Aliases**: Provide clear aliases for complex field paths
2. **Validate Data**: Verify the structure of input data in a production environment
3. **Reasonable nesting**: Avoid overly deep nesting layers to maintain performance
4. **Error Expectation**: Anticipate and handle cases where fields do not exist
5. **Performance Testing**: Conduct performance tests in scenarios with large data volumes 
