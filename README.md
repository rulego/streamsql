# StreamSQL
[![GoDoc](https://pkg.go.dev/badge/github.com/rulego/streamsql)](https://pkg.go.dev/github.com/rulego/streamsql)
[![Go Report](https://goreportcard.com/badge/github.com/rulego/streamsql)](https://goreportcard.com/report/github.com/rulego/streamsql)
[![CI](https://github.com/rulego/streamsql/actions/workflows/ci.yml/badge.svg)](https://github.com/rulego/streamsql/actions/workflows/ci.yml)
[![RELEASE](https://github.com/rulego/streamsql/actions/workflows/release.yml/badge.svg)](https://github.com/rulego/streamsql/actions/workflows/release.yml)
[![codecov](https://codecov.io/gh/rulego/streamsql/graph/badge.svg?token=1CK1O5J1BI)](https://codecov.io/gh/rulego/streamsql)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go?tab=readme-ov-file#stream-processing)

English| [简体中文](README_ZH.md)

**StreamSQL** is a lightweight, SQL-based stream processing engine for IoT edge, enabling efficient data processing and analysis on unbounded streams.

📖 **[Documentation](https://rulego.cc/en/pages/streamsql-overview/)** | Similar to: [Apache Flink](https://flink.apache.org/)

## Features

- Lightweight
    - Pure in-memory operations
    - No dependencies
- Data processing with SQL syntax
  - **Nested field access**: Support dot notation syntax (`device.info.name`) and array indexing (`sensors[0].value`) for accessing nested structured data
- Data analysis
    - Built-in multiple window types: sliding window, tumbling window, counting window
    - Built-in aggregate functions: MAX, MIN, AVG, SUM, STDDEV, MEDIAN, PERCENTILE, etc.
    - Support for group-by aggregation
    - Support for filtering conditions
- High extensibility
    - Flexible function extension provided
    - Integration with the **RuleGo** ecosystem to expand input and output sources using **RuleGo** components
- Integration with [RuleGo](https://gitee.com/rulego/rulego)
    - Utilize the rich and flexible input, output, and processing components of **RuleGo** to achieve data source access and integration with third-party systems

## Installation

```bash
go get github.com/rulego/streamsql
```

## Usage

StreamSQL supports two main processing modes for different business scenarios:

### Non-Aggregation Mode - Real-time Data Transformation and Filtering

Suitable for scenarios requiring **real-time response** and **low latency**, where each data record is processed and output immediately.

**Typical Use Cases:**
- **Data Cleaning**: Clean and standardize dirty data from IoT devices
- **Real-time Alerting**: Monitor key metrics and alert immediately when thresholds are exceeded
- **Data Enrichment**: Add calculated fields and business labels to raw data
- **Format Conversion**: Convert data to formats required by downstream systems
- **Data Routing**: Route data to different processing channels based on content

```go
package main

import (
	"fmt"
	"time"
	"github.com/rulego/streamsql"
)

func main() {
	// Create StreamSQL instance
	ssql := streamsql.New()
	defer ssql.Stop()

	// Non-aggregation SQL: Real-time data transformation and filtering
	// Feature: Each input data is processed immediately, no need to wait for windows
	rsql := `SELECT deviceId, 
	                UPPER(deviceType) as device_type,
	                temperature * 1.8 + 32 as temp_fahrenheit,
	                CASE WHEN temperature > 30 THEN 'hot'
	                     WHEN temperature < 15 THEN 'cold'
	                     ELSE 'normal' END as temp_category,
	                CONCAT(location, '-', deviceId) as full_identifier,
	                NOW() as processed_time
	         FROM stream 
	         WHERE temperature > 0 AND STARTSWITH(deviceId, 'sensor')`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	// Handle real-time transformation results
    ssql.AddSink(func(results []map[string]interface{}) {
        fmt.Printf("Real-time result: %+v\n", results)
    })

	// Simulate sensor data input
	sensorData := []map[string]interface{}{
		{
			"deviceId":     "sensor001",
			"deviceType":   "temperature", 
			"temperature":  25.0,
			"location":     "warehouse-A",
		},
		{
			"deviceId":     "sensor002",
			"deviceType":   "humidity",
			"temperature":  32.5,
			"location":     "warehouse-B", 
		},
		{
			"deviceId":     "pump001",  // Will be filtered out
			"deviceType":   "actuator",
			"temperature":  20.0,
			"location":     "factory",
		},
	}

	// Process data one by one, each will output results immediately
	for _, data := range sensorData {
		ssql.Emit(data)
        //changedData,err:=ssql.EmitSync(data) //Synchronize to obtain processing results
		time.Sleep(100 * time.Millisecond) // Simulate real-time data arrival
	}

	time.Sleep(500 * time.Millisecond) // Wait for processing completion
}
```

### Aggregation Mode - Windowed Statistical Analysis

Suitable for scenarios requiring **statistical analysis** and **batch processing**, collecting data over a period of time for aggregated computation.

**Typical Use Cases:**
- **Monitoring Dashboard**: Display real-time statistical charts of device operational status
- **Performance Analysis**: Analyze key metrics like QPS, latency, etc.
- **Anomaly Detection**: Detect data anomalies based on statistical models
- **Report Generation**: Generate various business reports periodically
- **Trend Analysis**: Analyze data trends and patterns

```go
package main

import (
	"context"
	"fmt"
	"time"

	"math/rand"
	"sync"
	"github.com/rulego/streamsql"
)

// StreamSQL Usage Example
// This example demonstrates the complete workflow of StreamSQL: from instance creation to data processing and result handling
func main() {
	// Step 1: Create StreamSQL Instance
	// StreamSQL is the core component of the stream SQL processing engine, managing the entire stream processing lifecycle
	ssql := streamsql.New()
    defer ssql.Stop()
	// Step 2: Define Stream SQL Query Statement
	// This SQL statement showcases StreamSQL's core capabilities:
	// - SELECT: Choose output fields and aggregation functions
	// - FROM stream: Specify the data source as stream data
	// - WHERE: Filter condition, excluding device3 data
	// - GROUP BY: Group by deviceId, combined with tumbling window for aggregation
	// - TumblingWindow('5s'): 5-second tumbling window, triggers computation every 5 seconds
	// - avg(), min(): Aggregation functions for calculating average and minimum values
	// - window_start(), window_end(): Window functions to get window start and end times
	rsql := "SELECT deviceId,avg(temperature) as avg_temp,min(humidity) as min_humidity ," +
		"window_start() as start,window_end() as end FROM  stream  where deviceId!='device3' group by deviceId,TumblingWindow('5s')"
	
	// Step 3: Execute SQL Statement and Start Stream Analysis Task
	// The Execute method parses SQL, builds execution plan, initializes window manager and aggregators
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}
	
	// Step 4: Setup Test Environment and Concurrency Control
	var wg sync.WaitGroup
	wg.Add(1)
	// Set 30-second test timeout to prevent infinite running
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// Step 5: Start Data Producer Goroutine
	// Simulate real-time data stream, continuously feeding data into StreamSQL
	go func() {
		defer wg.Done()
		// Create ticker to trigger data generation every second
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Generate 10 random test data points per second, simulating high-frequency data stream
				// This data density tests StreamSQL's real-time processing capability
				for i := 0; i < 10; i++ {
					// Construct device data containing deviceId, temperature, and humidity
					randomData := map[string]interface{}{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(2)+1), // Randomly select device1 or device2
						"temperature": 20.0 + rand.Float64()*10,                // Temperature range: 20-30 degrees
						"humidity":    50.0 + rand.Float64()*20,                // Humidity range: 50-70%
					}
					// Add data to stream, triggering StreamSQL's real-time processing
                    // Emit distributes data to corresponding windows and aggregators
                    ssql.Emit(randomData)
				}

			case <-ctx.Done():
				// Timeout or cancellation signal, stop data generation
				return
			}
		}
	}()

	// Step 6: Setup Result Processing Pipeline
	resultChan := make(chan interface{})
	// Add computation result callback function (Sink)
    // When window triggers computation, results are output through this callback
    ssql.AddSink(func(results []map[string]interface{}) {
        resultChan <- results
    })
	
	// Step 7: Start Result Consumer Goroutine
	// Count received results for effect verification
	resultCount := 0
	go func() {
		for result := range resultChan {
			// Print results when window computation is triggered (every 5 seconds)
			// This demonstrates StreamSQL's window-based aggregation results
			fmt.Printf("Window Result [%s]: %v\n", time.Now().Format("15:04:05.000"), result)
			resultCount++
		}
	}()
	
	// Step 8: Wait for Processing Completion
	// Wait for data producer goroutine to finish (30-second timeout or manual cancellation)
	wg.Wait()
	
	// Step 9: Display Final Statistics
	// Show total number of window results received during the test period
	fmt.Printf("\nTotal window results received: %d\n", resultCount)
	fmt.Println("StreamSQL processing completed successfully!")
}
```

### Nested Field Access

StreamSQL supports querying nested structured data using dot notation (`.`) syntax to access nested fields:

```go
// Nested field access example
package main

import (
	"fmt"
	"time"
	"github.com/rulego/streamsql"
)

func main() {
	ssql := streamsql.New()
	defer ssql.Stop()

	// SQL query using nested fields - supports dot notation syntax for accessing nested structures
	rsql := `SELECT device.info.name as device_name, 
	                device.location,
	                AVG(sensor.temperature) as avg_temp,
	                COUNT(*) as sensor_count,
	                window_start() as start,
	                window_end() as end
	         FROM stream 
	         WHERE device.info.type = 'temperature'
	         GROUP BY device.location, TumblingWindow('5s')`

	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}

	// Handle aggregation results
    ssql.AddSink(func(results []map[string]interface{}) {
        fmt.Printf("Aggregation result: %+v\n", results)
    })

	// Add nested structured data
	nestedData := map[string]interface{}{
		"device": map[string]interface{}{
			"info": map[string]interface{}{
				"name": "temperature-sensor-001",
				"type": "temperature",
			},
			"location": "smart-greenhouse-A",
		},
		"sensor": map[string]interface{}{
			"temperature": 25.5,
			"humidity":    60.2,
		},
		"timestamp": time.Now().Unix(),
	}

	ssql.Emit(nestedData)
}
```

## Functions

StreamSQL supports a variety of function types, including mathematical, string, conversion, aggregate, analytic, window, and more. [Documentation](docs/FUNCTIONS_USAGE_GUIDE.md)

## Concepts

### Processing Modes

StreamSQL supports two main processing modes:

#### Aggregation Mode (Windowed Processing)
Used when the SQL query contains aggregate functions (SUM, AVG, COUNT, etc.) or GROUP BY clauses. Data is collected in windows and aggregated results are output when windows are triggered.

#### Non-Aggregation Mode (Real-time Processing)  
Used for immediate data transformation and filtering without aggregation operations. Each input record is processed and output immediately, providing ultra-low latency for real-time scenarios like data cleaning, enrichment, and filtering.

### Windows

Since stream data is unbounded, it cannot be processed as a whole. Windows provide a mechanism to divide unbounded data into a series of bounded data segments for computation. StreamSQL includes the following types of windows:

- **Sliding Window**
  - **Definition**: A time-based window that slides forward at fixed time intervals. For example, it slides every 10 seconds.
  - **Characteristics**: The size of the window is fixed, but the starting point of the window is continuously updated over time. It is suitable for real-time statistical analysis of data within continuous time periods.
  - **Application Scenario**: In intelligent transportation systems, the vehicle traffic is counted every 10 seconds over the past 1 minute.

- **Tumbling Window**
  - **Definition**: A time-based window that does not overlap and is completely independent. For example, a window is generated every 1 minute.
  - **Characteristics**: The size of the window is fixed, and the windows do not overlap with each other. It is suitable for overall analysis of data within fixed time periods.
  - **Application Scenario**: In smart agriculture monitoring systems, the temperature and humidity of the farmland are counted every hour within that hour.

- **Count Window**
  - **Definition**: A window based on the number of data records, where the window size is determined by the number of data records. For example, a window is generated every 100 data records.
  - **Characteristics**: The size of the window is not related to time but is divided based on the volume of data. It is suitable for segmenting data based on the amount of data.
  - **Application Scenario**: In industrial IoT, an aggregation calculation is performed every time 100 device status data records are processed.

- **Session Window**
  - **Definition**: A dynamic window based on data activity. When the interval between data exceeds a specified timeout, the current session ends and triggers the window.
  - **Characteristics**: Window size changes dynamically, automatically dividing sessions based on data arrival intervals. When data arrives continuously, the session continues; when the data interval exceeds the timeout, the session ends and triggers the window.
  - **Application Scenario**: In user behavior analysis, maintain a session when users operate continuously, and close the session and count operations within that session when users stop operating for more than 5 minutes.

### Stream

- **Definition**: A continuous sequence of data that is generated in an unbounded manner, typically from sensors, log systems, user behaviors, etc.
- **Characteristics**: Stream data is real-time, dynamic, and unbounded, requiring timely processing and analysis.
- **Application Scenario**: Real-time data streams generated by IoT devices, such as temperature sensor data and device status data.

### Time Semantics

StreamSQL supports two time concepts that determine how windows are divided and triggered:

#### Event Time

- **Definition**: Event time refers to the actual time when data was generated, usually recorded in a field within the data itself (such as `event_time`, `timestamp`, `order_time`, etc.).
- **Characteristics**:
  - Windows are divided based on timestamp field values in the data
  - Even if data arrives late, it can be correctly counted into the corresponding window based on event time
  - Uses Watermark mechanism to handle out-of-order and late data
  - Results are accurate but may have delays (need to wait for late data)
- **Use Cases**:
  - Scenarios requiring precise temporal analysis
  - Scenarios where data may arrive out of order or delayed
  - Historical data replay and analysis
- **Configuration**: Use `WITH (TIMESTAMP='field_name', TIMEUNIT='ms')` to specify the event time field
- **Example**:
  ```sql
  SELECT deviceId, COUNT(*) as cnt
  FROM stream
  GROUP BY deviceId, TumblingWindow('1m')
  WITH (TIMESTAMP='eventTime', TIMEUNIT='ms')
  ```

#### Processing Time

- **Definition**: Processing time refers to the time when data arrives at the StreamSQL processing system, i.e., the current time when the system receives the data.
- **Characteristics**:
  - Windows are divided based on the time data arrives at the system (`time.Now()`)
  - Regardless of the time field value in the data, it is counted into the current window based on arrival time
  - Uses system clock (Timer) to trigger windows
  - Low latency but results may be inaccurate (cannot handle out-of-order and late data)
- **Use Cases**:
  - Real-time monitoring and alerting scenarios
  - Scenarios with high latency requirements and relatively low accuracy requirements
  - Scenarios where data arrives in order and delay is controllable
- **Configuration**: Default when `WITH (TIMESTAMP=...)` is not specified
- **Example**:
  ```sql
  SELECT deviceId, COUNT(*) as cnt
  FROM stream
  GROUP BY deviceId, TumblingWindow('1m')
  -- No WITH clause specified, defaults to processing time
  ```

#### Event Time vs Processing Time Comparison

| Feature | Event Time | Processing Time |
|---------|------------|-----------------|
| **Time Source** | Timestamp field in data | System current time |
| **Window Division** | Based on event timestamp | Based on data arrival time |
| **Late Data Handling** | Supported (Watermark mechanism) | Not supported |
| **Out-of-Order Handling** | Supported (Watermark mechanism) | Not supported |
| **Result Accuracy** | Accurate | May be inaccurate |
| **Processing Latency** | Higher (need to wait for late data) | Low (real-time trigger) |
| **Configuration** | `WITH (TIMESTAMP='field')` | Default (no WITH clause) |
| **Use Cases** | Precise temporal analysis, historical replay | Real-time monitoring, low latency requirements |

#### Window Time

- **Window Start Time**
  - **Event Time Windows**: The starting time point of the window, aligned to window boundaries based on event time (e.g., aligned to minute or hour boundaries).
  - **Processing Time Windows**: The starting time point of the window, based on the time data arrives at the system.
  - **Example**: For an event-time-based tumbling window `TumblingWindow('5m')`, the window start time aligns to multiples of 5 minutes (e.g., 10:00, 10:05, 10:10).

- **Window End Time**
  - **Event Time Windows**: The ending time point of the window, usually the window start time plus the window duration. Windows trigger when `watermark >= window_end`.
  - **Processing Time Windows**: The ending time point of the window, based on the time data arrives at the system plus the window duration. Windows trigger when the system clock reaches the end time.
  - **Example**: For a tumbling window with a duration of 1 minute, if the window start time is 10:00, then the window end time is 10:01.

#### Watermark Mechanism (Event Time Windows Only)

- **Definition**: Watermark indicates "events with timestamps less than this time should not arrive anymore", used to determine when windows can trigger.
- **Calculation Formula**: `Watermark = max(event_time) - MaxOutOfOrderness`
- **Window Trigger Condition**: Windows trigger when `watermark >= window_end`
- **Configuration Parameters**:
  - `MAXOUTOFORDERNESS`: Maximum allowed out-of-order time for tolerating data disorder (default: 0, no out-of-order allowed)
  - `ALLOWEDLATENESS`: Time window can accept late data after triggering (default: 0, no late data accepted)
  - `IDLETIMEOUT`: Timeout for advancing Watermark based on processing time when data source is idle (default: 0, disabled)
- **Example**:
  ```sql
  SELECT deviceId, COUNT(*) as cnt
  FROM stream
  GROUP BY deviceId, TumblingWindow('5m')
  WITH (
      TIMESTAMP='eventTime',
      TIMEUNIT='ms',
      MAXOUTOFORDERNESS='5s',  -- Tolerate 5 seconds of out-of-order
      ALLOWEDLATENESS='2s',     -- Accept 2 seconds of late data after window triggers
      IDLETIMEOUT='5s'          -- Advance watermark based on processing time after 5s of no data
  )
  ```
  
## Contribution Guidelines

Pull requests and issues are welcome. Please ensure that the code conforms to Go standards and include relevant test cases.

## License

Apache License 2.0