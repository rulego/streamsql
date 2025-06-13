# StreamSQL
[![GoDoc](https://pkg.go.dev/badge/github.com/rulego/streamsql)](https://pkg.go.dev/github.com/rulego/streamsql)
[![Go Report](https://goreportcard.com/badge/github.com/rulego/streamsql)](https://goreportcard.com/report/github.com/rulego/streamsql)
[![ci](https://github.com/rulego/streamsql/workflows/test/badge.svg)](https://github.com/rulego/streamsql/actions/workflows/ci.yml)
[![build](https://github.com/rulego/streamsql/workflows/release/badge.svg)](https://github.com/rulego/streamsql/actions/workflows/release.yml)

English| [简体中文](README_ZH.md)

**StreamSQL** is a lightweight, SQL-based stream processing engine for IoT edge, enabling efficient data processing and analysis on unbounded streams.

Similar to: [Apache Flink](https://flink.apache.org/) and [ekuiper](https://ekuiper.org/)

## Features

- Lightweight
    - Pure in-memory operations
    - No dependencies
- Data processing with SQL syntax
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
					// AddData distributes data to corresponding windows and aggregators
					ssql.stream.AddData(randomData)
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
	ssql.stream.AddSink(func(result interface{}) {
		resultChan <- result
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

## Functions

StreamSQL supports a variety of function types, including mathematical, string, conversion, aggregate, analytic, window, and more. [Documentation](docs/FUNCTIONS_USAGE_GUIDE.md)

## Concepts

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

### Stream

- **Definition**: A continuous sequence of data that is generated in an unbounded manner, typically from sensors, log systems, user behaviors, etc.
- **Characteristics**: Stream data is real-time, dynamic, and unbounded, requiring timely processing and analysis.
- **Application Scenario**: Real-time data streams generated by IoT devices, such as temperature sensor data and device status data.

### Time Semantics

- **Event Time**
  - **Definition**: The actual time when the data occurred, usually represented by a timestamp generated by the data source.

- **Processing Time**
  - **Definition**: The time when the data arrives at the processing system.

- **Window Start Time**
  - **Definition**: The starting time point of the window based on event time. For example, for a sliding window based on event time, the window start time is the timestamp of the earliest event within the window.

- **Window End Time**
  - **Definition**: The ending time point of the window based on event time. Typically, the window end time is the window start time plus the duration of the window. For example, if the duration of a sliding window is 1 minute, then the window end time is the window start time plus 1 minute.
  
## Contribution Guidelines

Pull requests and issues are welcome. Please ensure that the code conforms to Go standards and include relevant test cases.

## License

Apache License 2.0