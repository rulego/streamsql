# 嵌套字段访问演示

这个演示展示了 StreamSQL 的嵌套字段访问功能，包括基本查询和聚合操作。

## 功能特点

- **点号语法访问**：使用 `device.info.name` 的方式访问嵌套字段
- **完整 SQL 支持**：在 SELECT、WHERE、GROUP BY 中都支持嵌套字段
- **聚合计算**：支持对嵌套字段进行聚合计算

## 运行演示

```bash
cd examples/nested-field-access
go run main.go
```

## 演示内容

### 演示1：基本嵌套字段查询

展示如何使用点号语法查询嵌套结构中的字段：

```sql
SELECT device.info.name as device_name, 
       device.location,
       sensor.temperature,
       sensor.humidity
FROM stream
```

### 演示2：嵌套字段聚合查询

展示如何对嵌套字段进行分组聚合计算：

```sql
SELECT device.location,
       device.info.type,
       AVG(sensor.temperature) as avg_temp,
       AVG(sensor.humidity) as avg_humidity,
       COUNT(*) as sensor_count
FROM stream 
GROUP BY device.location, device.info.type, TumblingWindow('3s')
```

## 数据结构

演示使用的嵌套数据结构：

```json
{
  "device": {
    "info": {
      "name": "temperature-sensor-001",
      "type": "temperature",
      "model": "TempSense-Pro"
    },
    "location": "智能温室-A区",
    "status": "online"
  },
  "sensor": {
    "temperature": 24.5,
    "humidity": 62.3
  },
  "timestamp": 1672531200
}
```

## 预期输出

### 基本查询输出

```
📊 演示1: 基本嵌套字段查询
  📋 查询结果:
    记录 1:
      设备名称: temperature-sensor-001
      设备位置: 智能温室-A区
      温度: 24.5°C
      湿度: 62.3%

    记录 2:
      设备名称: humidity-sensor-002
      设备位置: 智能温室-B区
      温度: 26.8°C
      湿度: 58.7%
```

### 聚合查询输出

```
📈 演示2: 嵌套字段聚合查询
  📊 聚合结果:
    聚合组 1:
      设备位置: 智能温室-A区
      传感器类型: temperature
      平均温度: 27.45°C
      平均湿度: 62.31%
      传感器数量: 4
      窗口开始: 10:30:15
      窗口结束: 10:30:18
```

## 技术要点

1. **嵌套字段访问**：使用点号分隔符访问多层嵌套的字段
2. **别名支持**：可以为嵌套字段设置简洁的别名
3. **聚合分组**：支持以嵌套字段作为分组键
4. **窗口计算**：在时间窗口内对嵌套字段进行聚合计算

## 应用场景

- **IoT 设备管理**：处理设备的多层级信息结构
- **传感器数据分析**：分析来自不同位置和类型的传感器数据
- **智能监控**：对复杂设备状态进行实时聚合分析
- **数据仓库ETL**：处理和转换嵌套的JSON数据流 