# StreamSQL
[![GoDoc](https://pkg.go.dev/badge/github.com/rulego/streamsql)](https://pkg.go.dev/github.com/rulego/streamsql)
[![Go Report](https://goreportcard.com/badge/github.com/rulego/streamsql)](https://goreportcard.com/report/github.com/rulego/streamsql)
[![CI](https://github.com/rulego/streamsql/actions/workflows/ci.yml/badge.svg)](https://github.com/rulego/streamsql/actions/workflows/ci.yml)
[![RELEASE](https://github.com/rulego/streamsql/actions/workflows/release.yml/badge.svg)](https://github.com/rulego/streamsql/actions/workflows/release.yml)
[![codecov](https://codecov.io/gh/rulego/streamsql/graph/badge.svg?token=1CK1O5J1BI)](https://codecov.io/gh/rulego/streamsql)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go?tab=readme-ov-file#stream-processing)

English | [Chinese Simplified](README_ZH.md)

**StreamSQL** is an embeddable, SQL-based stream processing engine built for the IoT edge. It sits between a **time-series database** and **Apache Flink**: Flink-grade real-time computation with TSDB-grade lightweight deployment — run real-time filtering, windowed aggregation, CDC-style change detection, and complex event pattern matching, in-process, inside a 128MB gateway.

> *The real-time power of a TSDB + the computation power of Flink + minimal deployment and integration overhead.*

📖 **[Documentation](https://rulego.cc/en/pages/streamsql-overview/)** | Similar to: [Apache Flink](https://flink.apache.org/)

- 🪶 **Lightweight & embeddable** — pure in-memory, zero external deps, fits a 128MB gateway as a library, starts in seconds
- 🧩 **Full SQL** — tumbling/sliding/counting/session/global windows, event time + watermark, CASE, nested fields, HAVING
- 🔍 **Analytic functions** — `lag` / `had_changed` / `changed_col` / cumulative stats, purpose-built for CDC change detection and context backtracking
- 🧩 **Complex Event Processing (CEP)** — `MATCH_RECOGNIZE` (SQL:2016, Flink-aligned) — **unique among lightweight edge engines**
- 🚀 **Edge-grade performance** — ~1.92M msg/s single-core filtering on x86; 128MB holds 100k+ device partitions
- 🔌 **RuleGo ecosystem** — tap RuleGo components for MQTT / HTTP / message queues / databases and any data source

## Why StreamSQL

Traditional stream processing forces two extremes: **time-series databases** store well but compute weakly in real time; **Flink / Storm** are powerful but heavy, consuming GBs of memory — unsuitable for the edge. StreamSQL fills the gap — **purpose-built for the edge**, doing real-time aggregation and pattern recognition on massive data under tight resource constraints.

| | StreamSQL | Apache Flink | eKuiper | Time-series DB |
|------|-----------|--------------|---------|----------------|
| Deployment | Minimal | Complex | Simple | Moderate |
| Footprint | Tiny (~10MB) | High (GBs) | Tiny (~10MB) | Moderate |
| Embeddable / as a library | ✅ | ❌ | ⚠️ | ⚠️ |
| Full SQL | ✅ | ✅ | ✅ | Limited |
| **Complex Event Processing (CEP)** | ✅ | ✅ | ❌ | ❌ |
| Analytic / change detection | ✅ | ✅ | ✅ | ❌ |
| Event time + watermark | ✅ | ✅ | ⚠️ | ❌ |
| Edge deployment | ✅ | ❌ | ✅ | ⚠️ |
| Horizontal cluster scaling | Single-node | ✅ | Single-node | ✅ |

**Good fit**: edge real-time compute on IoT gateways / industrial controllers / vehicle systems, device monitoring & anomaly detection, stream-processing prototyping, adding SQL muscle to RuleGo rule chains.
**Not a fit**: large-scale clusters needing horizontal scaling, apps needing persisted state or ACID transactions.

## Installation

```bash
go get github.com/rulego/streamsql
```

## Quick Start

Each record is processed and emitted immediately — real-time transformation and filtering, no window wait:

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
		fmt.Printf("Result: %+v\n", results)
	})

	ssql.Emit(map[string]interface{}{"deviceId": "sensor01", "temperature": 32.5})
}
// => Result: map[deviceId:sensor01 fahrenheit:90.5 level:hot]
```

## Core Capabilities

### 🧩 Complex Event Processing (CEP) — unique among lightweight edge engines

Recognize **event sequences that appear in a specific order**: consecutive threshold crossings (debounced), rise-then-drop, start→run→stop workflows, out-of-order events. Standard SQL:2016 `MATCH_RECOGNIZE`, Flink-SQL-aligned, with four guards bounding edge memory.

```sql
-- Fire only after temperature crosses 50 three times in a row (debounce single-point jitter)
SELECT * FROM stream
MATCH_RECOGNIZE (
    ORDER BY ts
    MEASURES MATCH_NUMBER() AS mn, LAST(A.temp) AS peak
    ONE ROW PER MATCH
    PATTERN (A{3}) WITHIN '1h'
    DEFINE A AS temp > 50
)
```

Supports pattern variables with quantifiers (`? * + {n}`), alternation `|`, `PERMUTE`, navigation (`PREV`/`NEXT`/`FIRST`/`LAST`), aggregates, `SUBSET`, `FINAL`/`RUNNING`, `WITHIN` active expiry. See the [CEP docs](https://rulego.cc/en/pages/streamsql-cep/).

### 🔍 Analytic functions — CDC change detection & accumulation

Stateful computation across events on a windowless continuous stream — evaluated immediately on each event, with state retained across events.

```sql
-- CDC change detection: emit only when temperature changes, with the previous value
SELECT deviceId, temperature, lag(temperature) AS prev
FROM stream
WHERE had_changed(true, temperature)

-- Partitioned + cumulative: per-device state, running total since start
SELECT deviceId, acc_sum(score) OVER (PARTITION BY deviceId) AS total
FROM stream
```

`OVER (PARTITION BY ... WHEN ...)` controls partitioning and update conditions. See the [analytic docs](https://rulego.cc/en/pages/streamsql-analytical-functions/).

> **Which to use**: compare adjacent events → analytic; ordered/sequence patterns → CEP; time-windowed stats → windowed aggregation + HAVING.

### 🪟 Windowed aggregation

Slice unbounded data into bounded segments for statistics, with 5 window types:

```sql
-- One tumbling window every 5 seconds, averaged per device
SELECT deviceId, AVG(temperature) AS avg_temp,
       window_start() AS start, window_end() AS end
FROM stream
GROUP BY deviceId, TumblingWindow('5s')
```

- **Tumbling** `TumblingWindow('5s')`: fixed size, no overlap
- **Sliding** `SlidingWindow('30s','10s')`: fixed size, slides by a step
- **Counting** `CountingWindow(100)`: by record count
- **Session** `SessionWindow('5m')`: dynamic, by data activity
- **Global** `GLOBAL WINDOW TRIGGER WHEN ...`: no time boundary, predicate-driven on the running aggregate, O(1) state per group
- Built-in aggregates: `MAX` / `MIN` / `AVG` / `SUM` / `COUNT` / `STDDEV` / `MEDIAN` / `PERCENTILE`, with `GROUP BY`, `HAVING`

### ⏱ Event time & watermark

Two time semantics: event time (timestamps embedded in data) and processing time (system clock). Event-time windows use a watermark to handle out-of-order and late data:

```sql
SELECT deviceId, COUNT(*) AS cnt
FROM stream
GROUP BY deviceId, TumblingWindow('5m')
WITH (TIMESTAMP='eventTime', TIMEUNIT='ms',
      MAXOUTOFORDERNESS='5s',   -- tolerate 5s of out-of-order
      ALLOWEDLATENESS='2s',     -- accept 2s of late data after trigger
      IDLETIMEOUT='5s')         -- advance watermark on processing time after 5s idle
```

### 🧩 Nested fields

Dot notation for nested structures, index access for arrays:

```sql
SELECT device.info.name AS name, sensors[0].value AS v0
FROM stream WHERE device.info.type = 'temperature'
```

### 🔧 Custom functions

Register in one line, use immediately in SQL. Eight function types (math/string/conversion/datetime/aggregate/analytic/window/custom), addable and removable at runtime:

```go
functions.RegisterCustomFunction("f2c", functions.TypeConversion,
    "Temperature", "Fahrenheit to Celsius", 1, 1,
    func(ctx *functions.FunctionContext, args []any) (any, error) {
        f, _ := functions.ConvertToFloat64(args[0])
        return (f - 32) * 5 / 9, nil
    })
// SELECT f2c(temperature) AS celsius FROM stream
```

## Performance

**x86 single-core / 128MB / v1.0.3, measured** (`BenchmarkGateway_*` in `test/e2e/stress_test.go`):

| Rule | ns/op | allocs | msg/s |
|------|-------|--------|-------|
| Filter | 522 | 6 | **~1.92M** |
| Transform | 1359 | 12 | ~740K |
| Analytic + partition | 2095 | 18 | ~480K |

- 128MB holds **100k+ devices** of partition state — memory is not the bottleneck, CPU throughput is.
- Stability: no goroutine leaks, heap does not grow with load or partition count.
- **One rule saturating one core** is the optimal edge-gateway usage; multi-core scales by running parallel independent instances (`GOGC` tuning gets near-linear).

> ARM gateway figures are estimates derived from x86; measure on your target SoC before production. See the [gateway capacity & performance benchmark](docs/PERFORMANCE_GATEWAY_CAPACITY.md).

## Concepts

### Two processing modes
- **Non-aggregation mode**: no aggregate functions — each record is processed and emitted immediately, ultra-low latency. Data cleaning, real-time alerting, enrichment.
- **Aggregation mode**: contains aggregate functions or `GROUP BY` — data goes into windows; aggregated results are emitted when windows trigger.

### Windows
Stream data is unbounded and cannot be processed whole. Windows slice it into bounded segments: tumbling, sliding, counting, session, global (above).

### Time semantics
- **Event time**: when the data was actually generated (e.g. an `event_time` field). Windows are partitioned by timestamp; the watermark handles out-of-order/late data correctly — accurate, but with some latency.
- **Processing time**: the system clock when data arrives (default). Low latency, but no handling of out-of-order/late data.

| Feature | Event time | Processing time |
|---------|-----------|-----------------|
| Source | Timestamp field in data | System clock |
| Out-of-order / late | Supported (watermark) | Not supported |
| Accuracy | Accurate | May be inaccurate |
| Latency | Higher | Low |
| Config | `WITH (TIMESTAMP='field')` | Default (no WITH) |

For deeper concepts (windows, watermark, late data) see the [core concepts docs](https://rulego.cc/en/pages/streamsql-concepts/).

## RuleGo integration

StreamSQL runs as [RuleGo](https://rulego.cc) rule-chain nodes, tapping its 60+ components for any data source or third-party system, plus the rule engine:

- **streamTransform** (`x/streamTransform`): non-aggregation SQL, row-by-row streaming transform
- **streamAggregator** (`x/streamAggregator`): aggregation SQL, windowed aggregation

```json
{
  "nodes": [{
    "id": "transform1", "type": "x/streamTransform",
    "configuration": { "sql": "SELECT deviceId, temperature*1.8+32 AS f FROM stream WHERE temperature>20" }
  }]
}
```

See the [RuleGo integration docs](https://rulego.cc/en/pages/streamsql-rulego/).

## Functions

60+ built-in functions: math, string, conversion, datetime, aggregate, analytic, window, and more. [Function guide](docs/FUNCTIONS_USAGE_GUIDE.md).

## Contributing & Community

Issues and pull requests are welcome. Code should follow Go standards and include tests.
- Code: [GitHub](https://github.com/rulego/streamsql) · [Gitee](https://gitee.com/rulego/streamsql)
- Docs: [rulego-doc](https://github.com/rulego/rulego-doc) — translations and revisions welcome

## License

Apache License 2.0
