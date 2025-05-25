# StreamSQL

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
	"testing"
	"time"

	"math/rand"
	"sync"
	"github.com/rulego/streamsql"
)

func main() {
	ssql := streamsql.New()
	// Define the SQL statement. Every 5 seconds, group by deviceId and output the average temperature and minimum humidity of the device.
	rsql := "SELECT deviceId,avg(temperature) as avg_temp,min(humidity) as min_humidity ," +
		"window_start() as start,window_end() as end FROM  stream  where deviceId!='device3' group by deviceId,TumblingWindow('5s')"
	// Create a stream processing task based on the SQL statement.
	err := ssql.Execute(rsql)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	// Set a 30-second test timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Add test data
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Generate random test data, generating 10 data points per second
				for i := 0; i < 10; i++ {
					randomData := map[string]interface{}{
						"deviceId":    fmt.Sprintf("device%d", rand.Intn(2)+1),
						"temperature": 20.0 + rand.Float64()*10, // Temperature between 20-30 degrees
						"humidity":    50.0 + rand.Float64()*20, // Humidity between 50-70%
					}
					// Add data to the stream
					ssql.stream.AddData(randomData)
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	resultChan := make(chan interface{})
	// Add a result callback
	ssql.stream.AddSink(func(result interface{}) {
		resultChan <- result
	})
	// Count the number of results received
	resultCount := 0
	go func() {
		for result := range resultChan {
			// Print results every 5 seconds
			fmt.Printf("Print result: [%s] %v\n", time.Now().Format("15:04:05.000"), result)
			resultCount++
		}
	}()
    // End of test
	wg.Wait()
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