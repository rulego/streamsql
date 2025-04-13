# StreamSQL

轻量级嵌入式流式SQL处理库，把无界的流式数据，通过窗口函数分割成一系列连续的有界数据，并对其进行聚合计算、转换、过滤等操作。

## 功能特性

- 支持多种窗口类型：滑动窗口、滚动窗口、计数窗口
- 支持聚合函数：MAX, MIN, AVG, SUM, STDDEV,MEDIAN,PERCENTILE等
- 支持分组聚合
- 支持过滤条件

## 安装

```bash
go get github.com/rulego/streamsql
```

## 使用示例

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
	//TumblingWindow 滚动窗口，2秒滚动一次
	rsql := "SELECT deviceId,max(temperature) as max_temp,min(humidity) as min_humidity ,window_start() as start,window_end() as end FROM  stream group by deviceId,TumblingWindow('2s')"
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}
	// 添加测试数据
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// 生成随机测试数据
				randomData := map[string]interface{}{
					"deviceId":    fmt.Sprintf("device%d", rand.Intn(2)+1),
					"temperature": 20.0 + rand.Float64()*10, // 20-30度之间
					"humidity":    50.0 + rand.Float64()*20, // 50-70%湿度
				}
				ssql.stream.AddData(randomData)
			}
		}
	}()

	// 添加结果回调
	resultChan := make(chan interface{})
	ssql.stream.AddSink(func(result interface{}) {
		resultChan <- result
	})
	//打印结果
	go func() {
		for result := range resultChan {
			fmt.Printf("打印结果: %v\n", result)
		}
	}()

	time.Sleep(30 * time.Second)
}
```

## 贡献指南

欢迎提交PR和Issue。请确保代码符合Go标准，并添加相应的测试用例。

## 许可证

Apache License 2.0