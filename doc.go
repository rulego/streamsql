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
Package streamsql is a lightweight, SQL-based IoT edge stream processing engine.

StreamSQL provides efficient unbounded data stream processing and analysis capabilities,
supporting multiple window types, aggregate functions, custom functions, and seamless
integration with the RuleGo ecosystem.

# Core Features

• Lightweight design - Pure in-memory operations, no external dependencies
• SQL syntax support - Process stream data using familiar SQL syntax
• Multiple window types - Sliding, tumbling, counting, and session windows
• Rich aggregate functions - MAX, MIN, AVG, SUM, STDDEV, MEDIAN, PERCENTILE, etc.
• Plugin-based custom functions - Runtime dynamic registration, supports 8 function types
• RuleGo ecosystem integration - Extend input/output sources using RuleGo components

# Getting Started

Basic stream data processing:

	package main

	import (
		"fmt"
		"math/rand"
		"time"
		"github.com/rulego/streamsql"
	)

	func main() {
		// Create StreamSQL instance
		ssql := streamsql.New()

		// Define SQL query - Calculate temperature average by device ID every 5 seconds
		sql := `SELECT deviceId,
			AVG(temperature) as avg_temp,
			MIN(humidity) as min_humidity,
			window_start() as start,
			window_end() as end
		FROM stream
		WHERE deviceId != 'device3'
		GROUP BY deviceId, TumblingWindow('5s')`

		// Execute SQL, create stream processing task
		err := ssql.Execute(sql)
		if err != nil {
			panic(err)
		}

		// Add result processing callback
		ssql.AddSink(func(result []map[string]interface{}) {
			fmt.Printf("Aggregation result: %v\n", result)
		})

		// Simulate sending stream data
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					// Generate random device data
					data := map[string]interface{}{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(3)+1),
						"temperature": 20.0 + rand.Float64()*10,
						"humidity":    50.0 + rand.Float64()*20,
					}
					ssql.Emit(data)
				}
			}
		}()

		// Run for 30 seconds
		time.Sleep(30 * time.Second)
	}

# Window Functions

StreamSQL supports multiple window types:

	// Tumbling window - Independent window every 5 seconds
	SELECT AVG(temperature) FROM stream GROUP BY TumblingWindow('5s')

	// Sliding window - 30-second window size, slides every 10 seconds
	SELECT MAX(temperature) FROM stream GROUP BY SlidingWindow('30s', '10s')

	// Counting window - One window per 100 records
	SELECT COUNT(*) FROM stream GROUP BY CountingWindow(100)

	// Session window - Automatically closes session after 5-minute timeout
	SELECT user_id, COUNT(*) FROM stream GROUP BY user_id, SessionWindow('5m')

# Custom Functions

StreamSQL supports plugin-based custom functions with runtime dynamic registration:

	// Register temperature conversion function
	functions.RegisterCustomFunction(
		"fahrenheit_to_celsius",
		functions.TypeConversion,
		"Temperature conversion",
		"Fahrenheit to Celsius",
		1, 1,
		func(ctx *functions.FunctionContext, args []interface{}) (interface{}, error) {
			f, _ := functions.ConvertToFloat64(args[0])
			return (f - 32) * 5 / 9, nil
		},
	)

	// Use immediately in SQL
	sql := `SELECT deviceId,
		AVG(fahrenheit_to_celsius(temperature)) as avg_celsius
	FROM stream GROUP BY deviceId, TumblingWindow('5s')`

Supported custom function types:
• TypeMath - Mathematical calculation functions
• TypeString - String processing functions
• TypeConversion - Type conversion functions
• TypeDateTime - Date and time functions
• TypeAggregation - Aggregate functions
• TypeAnalytical - Analytical functions
• TypeWindow - Window functions
• TypeCustom - General custom functions

# Log Configuration

StreamSQL provides flexible log configuration options:

	// Set log level
	ssql := streamsql.New(streamsql.WithLogLevel(logger.DEBUG))

	// Output to file
	logFile, _ := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	ssql := streamsql.New(streamsql.WithLogOutput(logFile, logger.INFO))

	// Disable logging (production environment)
	ssql := streamsql.New(streamsql.WithDiscardLog())

# RuleGo Integration

StreamSQL provides deep integration with the RuleGo rule engine through two dedicated components for stream data processing:

• streamTransform (x/streamTransform) - Stream transformer, handles non-aggregation SQL queries
• streamAggregator (x/streamAggregator) - Stream aggregator, handles aggregation SQL queries

Basic integration example:

	package main

	import (
		"github.com/rulego/rulego"
		"github.com/rulego/rulego/api/types"
		// Register StreamSQL components
		_ "github.com/rulego/rulego-components/external/streamsql"
	)

	func main() {
		// Rule chain configuration
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

		// Create rule engine
		ruleEngine, _ := rulego.New("rule01", []byte(ruleChainJson))

		// Send data
		data := `{"deviceId":"sensor01","temperature":25.5}`
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), data)
		ruleEngine.OnMsg(msg)
	}
*/
package streamsql
