/*
 * Copyright 2025 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Package streamsql 是一个轻量级的、基于 SQL 的物联网边缘流处理引擎。

StreamSQL 提供了高效的无界数据流处理和分析能力，支持多种窗口类型、聚合函数、
自定义函数，以及与 RuleGo 生态的无缝集成。

# 核心特性

• 轻量级设计 - 纯内存操作，无外部依赖
• SQL语法支持 - 使用熟悉的SQL语法处理流数据
• 多种窗口类型 - 滑动窗口、滚动窗口、计数窗口、会话窗口
• 丰富的聚合函数 - MAX, MIN, AVG, SUM, STDDEV, MEDIAN, PERCENTILE等
• 插件式自定义函数 - 运行时动态注册，支持8种函数类型
• RuleGo生态集成 - 利用RuleGo组件扩展输入输出源

# 入门示例

基本的流数据处理：

	package main

	import (
		"fmt"
		"math/rand"
		"time"
		"github.com/rulego/streamsql"
	)

	func main() {
		// 创建StreamSQL实例
		ssql := streamsql.New()

		// 定义SQL查询 - 每5秒按设备ID分组计算温度平均值
		sql := `SELECT deviceId,
			AVG(temperature) as avg_temp,
			MIN(humidity) as min_humidity,
			window_start() as start,
			window_end() as end
		FROM stream
		WHERE deviceId != 'device3'
		GROUP BY deviceId, TumblingWindow('5s')`

		// 执行SQL，创建流处理任务
		err := ssql.Execute(sql)
		if err != nil {
			panic(err)
		}

		// 添加结果处理回调
		ssql.AddSink(func(result interface{}) {
			fmt.Printf("聚合结果: %v\n", result)
		})

		// 模拟发送流数据
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					// 生成随机设备数据
					data := map[string]interface{}{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(3)+1),
						"temperature": 20.0 + rand.Float64()*10,
						"humidity":    50.0 + rand.Float64()*20,
					}
					ssql.Emit(data)
				}
			}
		}()

		// 运行30秒
		time.Sleep(30 * time.Second)
	}

# 窗口函数

StreamSQL 支持多种窗口类型：

	// 滚动窗口 - 每5秒一个独立窗口
	SELECT AVG(temperature) FROM stream GROUP BY TumblingWindow('5s')

	// 滑动窗口 - 窗口大小30秒，每10秒滑动一次
	SELECT MAX(temperature) FROM stream GROUP BY SlidingWindow('30s', '10s')

	// 计数窗口 - 每100条记录一个窗口
	SELECT COUNT(*) FROM stream GROUP BY CountingWindow(100)

	// 会话窗口 - 超时5分钟自动关闭会话
	SELECT user_id, COUNT(*) FROM stream GROUP BY user_id, SessionWindow('5m')

# 自定义函数

StreamSQL 支持插件式自定义函数，运行时动态注册：

	// 注册温度转换函数
	functions.RegisterCustomFunction(
		"fahrenheit_to_celsius",
		functions.TypeConversion,
		"温度转换",
		"华氏度转摄氏度",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			f, _ := functions.ConvertToFloat64(args[0])
			return (f - 32) * 5 / 9, nil
		},
	)

	// 立即在SQL中使用
	sql := `SELECT deviceId,
		AVG(fahrenheit_to_celsius(temperature)) as avg_celsius
	FROM stream GROUP BY deviceId, TumblingWindow('5s')`

支持的自定义函数类型：
• TypeMath - 数学计算函数
• TypeString - 字符串处理函数
• TypeConversion - 类型转换函数
• TypeDateTime - 时间日期函数
• TypeAggregation - 聚合函数
• TypeAnalytical - 分析函数
• TypeWindow - 窗口函数
• TypeCustom - 通用自定义函数

# 日志配置

StreamSQL 提供灵活的日志配置选项：

	// 设置日志级别
	ssql := streamsql.New(streamsql.WithLogLevel(logger.DEBUG))

	// 输出到文件
	logFile, _ := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	ssql := streamsql.New(streamsql.WithLogOutput(logFile, logger.INFO))

	// 禁用日志（生产环境）
	ssql := streamsql.New(streamsql.WithDiscardLog())

# 与RuleGo集成

StreamSQL提供了与RuleGo规则引擎的深度集成，通过两个专用组件实现流式数据处理：

• streamTransform (x/streamTransform) - 流转换器，处理非聚合SQL查询
• streamAggregator (x/streamAggregator) - 流聚合器，处理聚合SQL查询

基本集成示例：

	package main

	import (
		"github.com/rulego/rulego"
		"github.com/rulego/rulego/api/types"
		// 注册StreamSQL组件
		_ "github.com/rulego/rulego-components/external/streamsql"
	)

	func main() {
		// 规则链配置
		ruleChainJson := `{
			"ruleChain": {"id": "rule01"},
			"metadata": {
				"nodes": [{
					"id": "transform1",
					"type": "x/streamTransform",
					"configuration": {
						"sql": "SELECT deviceId, temperature * 1.8 + 32 as temp_f FROM stream WHERE temperature > 20"
					}
				}, {
					"id": "aggregator1",
					"type": "x/streamAggregator",
					"configuration": {
						"sql": "SELECT deviceId, AVG(temperature) as avg_temp FROM stream GROUP BY deviceId, TumblingWindow('5s')"
					}
				}],
				"connections": [{
					"fromId": "transform1",
					"toId": "aggregator1",
					"type": "Success"
				}]
			}
		}`

		// 创建规则引擎
		ruleEngine, _ := rulego.New("rule01", []byte(ruleChainJson))

		// 发送数据
		data := `{"deviceId":"sensor01","temperature":25.5}`
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), data)
		ruleEngine.OnMsg(msg)
	}
*/
package streamsql
