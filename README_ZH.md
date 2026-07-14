# StreamSQL
[![GoDoc](https://pkg.go.dev/badge/github.com/rulego/streamsql)](https://pkg.go.dev/github.com/rulego/streamsql)
[![Go Report](https://goreportcard.com/badge/github.com/rulego/streamsql)](https://goreportcard.com/report/github.com/rulego/streamsql)
[![CI](https://github.com/rulego/streamsql/actions/workflows/ci.yml/badge.svg)](https://github.com/rulego/streamsql/actions/workflows/ci.yml)
[![RELEASE](https://github.com/rulego/streamsql/actions/workflows/release.yml/badge.svg)](https://github.com/rulego/streamsql/actions/workflows/release.yml)
[![codecov](https://codecov.io/gh/rulego/streamsql/graph/badge.svg?token=1CK1O5J1BI)](https://codecov.io/gh/rulego/streamsql)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go?tab=readme-ov-file#stream-processing)

[English](README.md)| 简体中文

**StreamSQL** 是为物联网边缘设计的、可嵌入的 SQL 流处理引擎。它介于「时序数据库」和「Apache Flink」之间——既能像 Flink 一样做实时计算，又能像时序数据库一样轻量部署：在 128MB 的网关进程内，用一条 SQL 跑实时过滤、窗口聚合、CDC 式变化检测和复杂事件模式识别。

> **时序数据库的实时性 + Flink 的计算能力 + 极简的部署与集成体验。**

📖 **[官方文档](https://rulego.cc/pages/streamsql-overview/)** ｜ 🐙 [GitHub](https://github.com/rulego/streamsql) · [Gitee](https://gitee.com/rulego/streamsql) · [GitCode](https://gitcode.com/rulego/streamsql)

- 🪶 **轻量可嵌入**　纯内存、零外部依赖，作为基础库塞进 128MB 网关，秒级启动
- 🧩 **全功能 SQL**　滚动/滑动/计数/会话/全局窗口、事件时间 + Watermark、CASE、嵌套字段、HAVING
- 🔍 **分析函数**　`lag` / `had_changed` / `changed_col` / 累积统计，专攻 CDC 变化检测与上下文回溯
- 🧩 **复杂事件识别（CEP）**　`MATCH_RECOGNIZE`（SQL:2016，对齐 Flink）——**轻量边缘引擎中独有**
- 🚀 **边缘级性能**　x86 单核过滤 ~192 万 msg/s；128MB 可承载 10 万+ 设备分区状态
- 🔌 **RuleGo 生态**　借 RuleGo 组件接入 MQTT / HTTP / 消息队列 / 数据库等任意数据源

## 为什么用 StreamSQL

传统流处理只有两个极端：**时序数据库**存储强但实时计算弱；**Flink / Storm** 功能强但部署重、吃 GB 级内存，不适合边缘。StreamSQL 填补中间的空白——**专为边缘端设计**，在资源受限的环境里对海量数据做实时聚合与模式识别。

| 维度 | StreamSQL | Apache Flink | eKuiper | 时序数据库 |
|------|-----------|--------------|---------|------------|
| 部署复杂度 | 极简 | 复杂 | 简单 | 中等 |
| 资源占用 | 极低（~10MB） | 高（GB 级） | 极低（~10MB） | 中等 |
| 可嵌入 / 作基础库 | ✅ | ❌ | ⚠️ | ⚠️ |
| 完整 SQL | ✅ | ✅ | ✅ | 有限 |
| **复杂事件识别（CEP）** | ✅ | ✅ | ❌ | ❌ |
| 分析函数 / 变化检测 | ✅ | ✅ | ✅ | ❌ |
| 事件时间 + Watermark | ✅ | ✅ | ⚠️ | ❌ |
| 边缘部署 | ✅ | ❌ | ✅ | ⚠️ |
| 集群水平扩展 | 单机 | ✅ | 单机 | ✅ |

**适合**：IoT 网关 / 工业控制器 / 车载系统的边缘实时计算、设备监控与异常检测、流处理原型验证、为 RuleGo 规则链补 SQL 能力。
**不适合**：需要水平扩展的大规模集群、需要持久化状态或 ACID 事务的场景。

## 安装

```bash
go get github.com/rulego/streamsql
```

## 快速开始

每条数据立即处理并输出——实时转换与过滤，无需等待窗口：

```go
package main

import (
	"fmt"
	"github.com/rulego/streamsql"
)

func main() {
	ssql := streamsql.New()
	defer ssql.Stop()

	err := ssql.Execute(`SELECT deviceId,
	    temperature * 1.8 + 32 AS fahrenheit,
	    CASE WHEN temperature > 30 THEN 'hot' ELSE 'normal' END AS level
	    FROM stream WHERE temperature > 0`)
	if err != nil {
		panic(err)
	}

	ssql.AddSink(func(results []map[string]interface{}) {
		fmt.Printf("结果: %+v\n", results)
	})

	ssql.Emit(map[string]interface{}{"deviceId": "sensor01", "temperature": 32.5})
}
// => 结果: map[deviceId:sensor01 fahrenheit:90.5 level:hot]
```

## 核心能力

### 🧩 复杂事件识别（CEP）—— 轻量边缘引擎独有

识别**按特定顺序出现的事件序列**：连续越限防抖、先升后降、开停机工作流、乱序事件。SQL:2016 标准 `MATCH_RECOGNIZE`，对齐 Flink SQL，四道闸保证边缘内存有界。

```sql
-- 温度连续 3 次越限才算真报警（防抖，避免单点抖动误报）
SELECT * FROM stream
MATCH_RECOGNIZE (
    ORDER BY ts
    MEASURES MATCH_NUMBER() AS mn, LAST(A.temp) AS peak
    ONE ROW PER MATCH
    PATTERN (A{3}) WITHIN '1h'
    DEFINE A AS temp > 50
)
```

支持模式变量 + 量词（`? * + {n}`）、交替 `|`、`PERMUTE`、导航（`PREV`/`NEXT`/`FIRST`/`LAST`）、聚合、`SUBSET`、`FINAL`/`RUNNING`、`WITHIN` 主动过期。详见[模式识别文档](https://rulego.cc/pages/streamsql-cep/)。

### 🔍 分析函数 —— CDC 变化检测与累积

在无窗口的连续事件流上做跨事件状态计算，每条事件到达立刻求值，状态跨事件保留。

```sql
-- CDC 变化检测：只在温度变化时输出，并带上一次的温度值
SELECT deviceId, temperature, lag(temperature) AS prev
FROM stream
WHERE had_changed(true, temperature)

-- 分区 + 累积：每个设备各自的状态、开服至今累计
SELECT deviceId, acc_sum(score) OVER (PARTITION BY deviceId) AS total
FROM stream
```

`OVER (PARTITION BY ... WHEN ...)` 控制分区与更新条件。详见[分析函数文档](https://rulego.cc/pages/streamsql-analytic/)。

> **何时用什么**：相邻事件比较 → 分析函数；事件序列/顺序模式 → CEP；时间段统计 → 窗口聚合 + HAVING。

### 🪟 窗口聚合

将无界数据切成有界片段做统计，支持 5 种窗口：

```sql
-- 每 5 秒一个滚动窗口，按设备分组求均值
SELECT deviceId, AVG(temperature) AS avg_temp,
       window_start() AS start, window_end() AS end
FROM stream
GROUP BY deviceId, TumblingWindow('5s')
```

- **滚动窗口** `TumblingWindow('5s')`：固定大小，不重叠
- **滑动窗口** `SlidingWindow('30s','10s')`：固定大小，按步长滑动
- **计数窗口** `CountingWindow(100)`：按条数划分
- **会话窗口** `SessionWindow('5m')`：按数据活跃度动态开合
- **全局窗口** `GLOBAL WINDOW TRIGGER WHEN ...`：无时间边界，由聚合阈值谓词驱动，每分组 O(1) 运行态
- 内置聚合：`MAX` / `MIN` / `AVG` / `SUM` / `COUNT` / `STDDEV` / `MEDIAN` / `PERCENTILE` 等，支持 `GROUP BY`、`HAVING`

### ⏱ 事件时间与 Watermark

支持事件时间（数据自带时间戳）和处理时间（系统时钟）两种语义。事件时间用 Watermark 处理乱序与迟到数据：

```sql
SELECT deviceId, COUNT(*) AS cnt
FROM stream
GROUP BY deviceId, TumblingWindow('5m')
WITH (TIMESTAMP='eventTime', TIMEUNIT='ms',
      MAXOUTOFORDERNESS='5s',   -- 容忍 5 秒乱序
      ALLOWEDLATENESS='2s',     -- 触发后还能接受 2 秒迟到数据
      IDLETIMEOUT='5s')         -- 空闲 5 秒后按处理时间推进 watermark
```

### 🧩 嵌套字段

点号语法访问嵌套结构，数组下标访问数组元素：

```sql
SELECT device.info.name AS name, sensors[0].value AS v0
FROM stream WHERE device.info.type = 'temperature'
```

### 🔧 自定义函数

一行注册，立即在 SQL 中可用，支持 8 种函数类型（数学/字符串/转换/日期/聚合/分析/窗口/自定义），运行时可动态增删：

```go
functions.RegisterCustomFunction("f2c", functions.TypeConversion,
    "温度转换", "华氏转摄氏", 1, 1,
    func(ctx *functions.FunctionContext, args []any) (any, error) {
        f, _ := functions.ConvertToFloat64(args[0])
        return (f - 32) * 5 / 9, nil
    })
// SELECT f2c(temperature) AS celsius FROM stream
```

## 性能

**x86 单核 / 128MB / v1.0.3 实测**（`test/e2e/stress_test.go` 的 `BenchmarkGateway_*`）：

| 规则 | ns/op | allocs | msg/s |
|------|-------|--------|-------|
| 过滤 | 522 | 6 | **~192 万** |
| 转换 | 1359 | 12 | ~74 万 |
| 分析 + 分区 | 2095 | 18 | ~48 万 |

- 128MB 内存可承载 **10 万+ 设备**的分区状态——内存不是瓶颈，CPU 吞吐才是。
- 稳定性：无 goroutine 泄漏，堆不随负载/分区数线性增长。
- **一条规则吃满一个核**是边缘网关的最优用法；多核靠并行多个独立实例扩展（`GOGC` 调优可近线性）。

> ARM 网关数字为 x86 折算估算，上线前需在目标 SoC 实测。详见[网关容量与性能基准](docs/PERFORMANCE_GATEWAY_CAPACITY.md)。

## 概念

### 两种处理模式
- **非聚合模式**：不含聚合函数，每条数据立即处理输出，超低延迟——数据清洗、实时告警、数据富化。
- **聚合模式**：含聚合函数或 `GROUP BY`，数据进窗口，窗口触发时输出聚合结果。

### 窗口
流数据无界，无法整体处理。窗口把无界数据切成一系列有界片段：滚动、滑动、计数、会话、全局（见上）。

### 时间语义
- **事件时间**：数据实际产生的时间（如 `event_time` 字段）。基于时间戳划分窗口，配合 Watermark 正确处理乱序/迟到数据，结果准确但有延迟。
- **处理时间**：数据到达系统的当前时间（默认）。低延迟，但不处理乱序/迟到数据。

| 特性 | 事件时间 | 处理时间 |
|------|---------|---------|
| 时间来源 | 数据中的时间戳字段 | 系统当前时间 |
| 乱序/迟到 | 支持（Watermark） | 不支持 |
| 结果准确性 | 准确 | 可能不准 |
| 延迟 | 较高 | 低 |
| 配置 | `WITH (TIMESTAMP='field')` | 默认（不指定 WITH） |

深入概念（窗口、Watermark、迟到数据）见[核心概念文档](https://rulego.cc/pages/streamsql-concepts/)。

## 与 RuleGo 集成

StreamSQL 可作为[RuleGo](https://rulego.cc)规则链节点，借其 60+ 组件接入任意数据源与第三方系统，并叠加规则引擎能力：

- **streamTransform**（`x/streamTransform`）：非聚合 SQL，逐条流式转换
- **streamAggregator**（`x/streamAggregator`）：聚合 SQL，窗口聚合

```json
{
  "nodes": [{
    "id": "transform1", "type": "x/streamTransform",
    "configuration": { "sql": "SELECT deviceId, temperature*1.8+32 AS f FROM stream WHERE temperature>20" }
  }]
}
```

详见[RuleGo 集成文档](https://rulego.cc/pages/streamsql-rulego/)。

## 函数

60+ 内置函数：数学、字符串、转换、日期时间、聚合、分析、窗口等。[函数使用指南](docs/FUNCTIONS_USAGE_GUIDE.md)。

## 贡献与社区

欢迎提交 Issue 和 Pull Request。代码请符合 Go 标准，并附测试用例。
- 代码：[GitHub](https://github.com/rulego/streamsql) · [Gitee](https://gitee.com/rulego/streamsql)
- 文档：[rulego-doc](https://github.com/rulego/rulego-doc)，欢迎参与翻译与修订

## License

Apache License 2.0
