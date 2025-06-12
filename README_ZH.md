# StreamSQL

[English](README.md)| 简体中文

**StreamSQL** 是一款轻量级的、基于 SQL 的物联网边缘流处理引擎。它能够高效地处理和分析无界数据流。

类似: [Apache Flink](https://flink.apache.org/) 和 [ekuiper](https://ekuiper.org/)

## 功能特性

- 轻量级
  - 纯内存操作
  - 无依赖
- SQL语法处理数据
- 数据分析
  - 内置多种窗口类型：滑动窗口、滚动窗口、计数窗口
  - 内置聚合函数：MAX, MIN, AVG, SUM, STDDEV,MEDIAN,PERCENTILE等
  - 支持分组聚合
  - 支持过滤条件
- 高可扩展性
  - 提供灵活的函数扩展
  - **完整的自定义函数系统**：支持数学、字符串、转换、聚合、分析等8种函数类型
  - **简单易用的函数注册**：一行代码即可注册自定义函数
  - **运行时动态扩展**：支持在运行时添加、移除和管理函数
  - 接入`RuleGo`生态，利用`RuleGo`组件方式扩展输出和输入源
- 与[RuleGo](https://gitee.com/rulego/rulego) 集成
  - 利用`RuleGo`丰富灵活的输入、输出、处理等组件，实现数据源接入以及和第三方系统联动

## 安装

```bash
go get github.com/rulego/streamsql
```

## 使用

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"math/rand"
	"github.com/rulego/streamsql"
)

func main() {
	// 1. 创建StreamSQL实例 - 这是流式SQL处理引擎的入口
	ssql := streamsql.New()
	
	// 2. 定义流式SQL查询语句
	// 核心概念解析：
	// - TumblingWindow('5s'): 滚动窗口，每5秒创建一个新窗口，窗口之间不重叠
	// - GROUP BY deviceId: 按设备ID分组，每个设备独立计算
	// - avg(temperature): 聚合函数，计算窗口内温度的平均值
	// - min(humidity): 聚合函数，计算窗口内湿度的最小值
	// - window_start()/window_end(): 窗口函数，获取当前窗口的开始和结束时间
	rsql := "SELECT deviceId,avg(temperature) as avg_temp,min(humidity) as min_humidity ," +
		"window_start() as start,window_end() as end FROM  stream  where deviceId!='device3' group by deviceId,TumblingWindow('5s')"
	
	// 3. 解析并执行SQL语句，创建流式分析任务
	// 这一步会：
	// - 解析SQL语句，构建执行计划
	// - 创建窗口管理器（每5秒触发一次计算）
	// - 设置数据过滤条件（排除device3）
	// - 配置聚合计算逻辑
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}
	
	var wg sync.WaitGroup
	wg.Add(1)
	// 设置30秒测试超时时间
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// 4. 数据生产者 - 模拟实时数据流输入
	// 在实际应用中，这可能是：
	// - IoT设备传感器数据
	// - 用户行为事件
	// - 系统监控指标
	// - 消息队列数据等
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// 每秒生成10条随机数据，模拟高频数据流
				// 数据特点：
				// - 只有device1和device2（device3被SQL过滤掉）
				// - 温度范围：20-30度
				// - 湿度范围：50-70%
				for i := 0; i < 10; i++ {
					randomData := map[string]interface{}{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(2)+1), // 随机生成device1或device2
						"temperature": 20.0 + rand.Float64()*10,                // 20-30度之间的随机温度
						"humidity":    50.0 + rand.Float64()*20,                // 50-70%之间的随机湿度
					}
					// 将数据推送到流处理引擎
					// 引擎会自动：
					// - 应用WHERE过滤条件
					// - 按deviceId分组
					// - 将数据分配到对应的时间窗口
					// - 更新聚合计算状态
					ssql.stream.AddData(randomData)
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	// 5. 结果处理管道 - 接收窗口计算结果
	resultChan := make(chan interface{})
	
	// 6. 注册结果回调函数
	// 当窗口触发时（每5秒），会调用这个回调函数
	// 传递聚合计算的结果
	ssql.stream.AddSink(func(result interface{}) {
		resultChan <- result
	})
	
	// 7. 结果消费者 - 处理计算结果
	// 在实际应用中，这里可能是：
	// - 发送告警通知
	// - 存储到数据库
	// - 推送到仪表板
	// - 触发下游业务逻辑
	resultCount := 0
	go func() {
		for result := range resultChan {
			// 每当5秒窗口结束时，会收到该窗口的聚合结果
			// 结果包含：
			// - deviceId: 设备ID
			// - avg_temp: 该设备在窗口内的平均温度
			// - min_humidity: 该设备在窗口内的最小湿度
			// - start/end: 窗口的时间范围
			fmt.Printf("窗口计算结果: [%s] %v\n", time.Now().Format("15:04:05.000"), result)
			resultCount++
		}
	}()
	
	// 8. 等待测试完成
	// 整个流程展示了StreamSQL的核心工作原理：
	// 数据输入 -> 过滤 -> 分组 -> 窗口聚合 -> 结果输出
	wg.Wait()
	fmt.Printf("\n测试完成，共收到 %d 个窗口结果\n", resultCount)
}
```

## 函数

StreamSQL 支持多种函数类型，包括数学、字符串、转换、聚合、分析、窗口等上百个函数。[文档](docs/FUNCTIONS_USAGE_GUIDE.md)

### 🎨 支持的函数类型

- **📊 数学函数** - sqrt, power, abs, 三角函数等
- **📝 字符串函数** - concat, upper, lower, trim等  
- **🔄 转换函数** - cast, hex2dec, encode/decode等
- **📈 聚合函数** - 自定义聚合逻辑
- **🔍 分析函数** - lag, latest, 变化检测等

## 概念

### 窗口

由于流数据是无限的，因此不可能将其作为一个整体来处理。窗口提供了一种机制，将无界的数据分割成一系列连续的有界数据来计算。StreamSQL 内置以下窗口类型：

- **滑动窗口（Sliding Window）**
  - **定义**：基于时间的窗口，窗口以固定的时间间隔向前滑动。例如，每 10 秒滑动一次。
  - **特点**：窗口的大小固定，但窗口的起始点会随着时间推移而不断更新。适合对连续时间段内的数据进行实时统计分析。
  - **应用场景**：在智能交通系统中，每 10 秒统计一次过去 1 分钟内的车辆流量。

- **滚动窗口（Tumbling Window）**
  - **定义**：基于时间的窗口，窗口之间没有重叠，完全独立。例如，每 1 分钟生成一个窗口。
  - **特点**：窗口的大小固定，且窗口之间互不重叠，适合对固定时间段内的数据进行整体分析。
  - **应用场景**：在智能农业监控系统中，每小时统计一次该小时内农田的温度和湿度。

- **计数窗口（Count Window）**
  - **定义**：基于数据条数的窗口，窗口大小由数据条数决定。例如，每 100 条数据生成一个窗口。
  - **特点**：窗口的大小与时间无关，而是根据数据量来划分，适合对数据量进行分段处理。
  - **应用场景**：在工业物联网中，每处理 100 条设备状态数据后进行一次聚合计算。

### 流（Stream）

- **定义**：流是数据的连续序列，数据以无界的方式产生，通常来自于传感器、日志系统、用户行为等。
- **特点**：流数据具有实时性、动态性和无限性，需要及时处理和分析。
- **应用场景**：物联网设备产生的实时数据流，如温度传感器数据、设备状态数据等。

### 时间语义

- **事件时间（Event Time）**
  - **定义**：数据实际发生的时间，通常由数据源生成的时间戳表示。

- **处理时间（Processing Time）**
  - **定义**：数据到达处理系统的时间。
- **窗口开始时间（Window Start Time）**
  - **定义**：基于事件时间，窗口的起始时间点。例如，对于一个基于事件时间的滑动窗口，窗口开始时间是窗口内最早事件的时间戳。
- **窗口结束时间（Window End Time）**
  - **定义**：基于事件时间，窗口的结束时间点。通常窗口结束时间是窗口开始时间加上窗口的持续时间。
  - 例如，一个滑动窗口的持续时间为 1 分钟，则窗口结束时间是窗口开始时间加上 1 分钟。

## 贡献指南

欢迎提交PR和Issue。请确保代码符合Go标准，并添加相应的测试用例。

## 许可证

Apache License 2.0