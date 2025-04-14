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
	"testing"
	"time"

	"math/rand"
	"github.com/rulego/streamsql"
)

func main() {
	ssql := streamsql.New()
	// 定义SQL语句。含义：每隔5秒按deviceId分组输出设备的温度平均值和湿度最小值。
	rsql := "SELECT deviceId,avg(temperature) as avg_temp,min(humidity) as min_humidity ," +
		"window_start() as start,window_end() as end FROM  stream  where deviceId!='device3' group by deviceId,TumblingWindow('5s')"
	// 根据SQL语句，创建流式分析任务。
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	// 设置30秒测试超时时间
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// 添加测试数据
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// 生成随机测试数据，每秒生成10条数据
				for i := 0; i < 10; i++ {
					randomData := map[string]interface{}{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(2)+1),
						"temperature": 20.0 + rand.Float64()*10, // 20-30度之间
						"humidity":    50.0 + rand.Float64()*20, // 50-70%湿度
					}
					// 将数据添加到流中
					ssql.stream.AddData(randomData)
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	resultChan := make(chan interface{})
	// 添加计算结果回调
	ssql.stream.AddSink(func(result interface{}) {
		resultChan <- result
	})
	// 记录收到的结果数量
	resultCount := 0
	go func() {
		for result := range resultChan {
			//每隔5秒打印一次结果
			fmt.Printf("打印结果: [%s] %v\n", time.Now().Format("15:04:05.000"), result)
			resultCount++
		}
	}()
    //测试结束
	wg.Wait()
}
```

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