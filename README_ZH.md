# StreamSQL

[English](README.md)| 简体中文

**StreamSQL** 是一个轻量级、嵌入式的流式 SQL 处理库。它通过窗口函数将无界流数据切分为有界数据块，并支持聚合计算、数据转换和过滤等操作。

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
	// 定义SQL语句。TumblingWindow 滚动窗口，5秒滚动一次
	rsql := "SELECT deviceId,avg(temperature) as max_temp,min(humidity) as min_humidity ," +
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

## 贡献指南

欢迎提交PR和Issue。请确保代码符合Go标准，并添加相应的测试用例。

## 许可证

Apache License 2.0