# StreamSQL

English| [简体中文](README_ZH.md)

**StreamSQL** is a lightweight, embedded stream SQL processing library. It splits unbounded stream data into bounded data chunks using window functions and supports operations such as aggregation, data transformation, and filtering.

## Features

- Supports multiple window types: Sliding Window, Tumbling Window, Count Window
- Supports aggregate functions: MAX, MIN, AVG, SUM, STDDEV, MEDIAN, PERCENTILE, etc.
- Supports group-by aggregation
- Supports filtering conditions

## Installation

```bash
go get github.com/rulego/streamsql
```

## Usage Example

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
	// Define the SQL statement. TumblingWindow is a tumbling window that rolls every 5 seconds
	rsql := "SELECT deviceId,avg(temperature) as max_temp,min(humidity) as min_humidity ," +
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

## Contribution Guidelines

Pull requests and issues are welcome. Please ensure that the code conforms to Go standards and include relevant test cases.

## License

Apache License 2.0