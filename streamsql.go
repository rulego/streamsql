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

package streamsql

import (
	"fmt"

	"github.com/rulego/streamsql/rsql"
	"github.com/rulego/streamsql/stream"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/table"
)

// Streamsql is the main interface for the StreamSQL streaming engine.
// It encapsulates core functionality including SQL parsing, stream processing, and window management.
//
// Usage example:
//
//	ssql := streamsql.New()
//	err := ssql.Execute("SELECT AVG(temperature) FROM stream GROUP BY TumblingWindow('5s')")
//	ssql.Emit(map[string]interface{}{"temperature": 25.5})
type Streamsql struct {
	stream *stream.Stream

	// Performance configuration mode
	performanceMode string // "default", "high_performance", "low_latency", "zero_data_loss", "custom"
	customConfig    *types.PerformanceConfig

	// Save original SELECT field order to maintain field order for table output
	fieldOrder []string
}

// New creates a new StreamSQL instance.
// Supports configuration through optional Option parameters.
//
// Parameters:
//   - options: Variable configuration options for customizing StreamSQL behavior
//
// Returns:
//   - *Streamsql: Newly created StreamSQL instance
//
// Examples:
//
//	// Create default instance
//	ssql := streamsql.New()
//
//	// Create high performance instance
//	ssql := streamsql.New(streamsql.WithHighPerformance())
//
//	// Create zero data loss instance
//	ssql := streamsql.New(streamsql.WithZeroDataLoss())
func New(options ...Option) *Streamsql {
	s := &Streamsql{
		performanceMode: "default", // Default to standard performance configuration
	}

	// Apply all configuration options
	for _, option := range options {
		option(s)
	}

	return s
}

// Execute parses and executes SQL queries, creating corresponding stream processing pipelines.
// This is the core method of StreamSQL, responsible for converting SQL into actual stream processing logic.
//
// Supported SQL syntax:
//   - SELECT clause: Select fields and aggregate functions
//   - FROM clause: Specify data source (usually 'stream')
//   - WHERE clause: Data filtering conditions
//   - GROUP BY clause: Grouping fields and window functions
//   - HAVING clause: Aggregate result filtering
//   - LIMIT clause: Limit result count
//   - DISTINCT: Result deduplication
//
// Window functions:
//   - TumblingWindow('5s'): Tumbling window
//   - SlidingWindow('30s', '10s'): Sliding window
//   - CountingWindow(100): Counting window
//   - SessionWindow('5m'): Session window
//
// Parameters:
//   - sql: SQL query statement to execute
//
// Returns:
//   - error: Returns error if SQL parsing or execution fails
//
// Examples:
//
//	// Basic aggregation query
//	err := ssql.Execute("SELECT deviceId, AVG(temperature) FROM stream GROUP BY deviceId, TumblingWindow('5s')")
//
//	// Query with filtering conditions
//	err := ssql.Execute("SELECT * FROM stream WHERE temperature > 30")
//
//	// Complex window aggregation
//	err := ssql.Execute(`
//	    SELECT deviceId,
//	           AVG(temperature) as avg_temp,
//	           MAX(humidity) as max_humidity
//	    FROM stream
//	    WHERE deviceId != 'test'
//	    GROUP BY deviceId, SlidingWindow('1m', '30s')
//	    HAVING avg_temp > 25
//	    LIMIT 100
//	`)
func (s *Streamsql) Execute(sql string) error {
	// Parse SQL statement
	config, condition, err := rsql.Parse(sql)
	if err != nil {
		return fmt.Errorf("SQL parsing failed: %w", err)
	}

	// Get field order information from parsing result
	s.fieldOrder = config.FieldOrder

	// Create stream processor based on performance mode
	var streamInstance *stream.Stream

	switch s.performanceMode {
	case "high_performance":
		streamInstance, err = stream.NewStreamWithHighPerformance(*config)
	case "low_latency":
		streamInstance, err = stream.NewStreamWithLowLatency(*config)
	case "zero_data_loss":
		streamInstance, err = stream.NewStreamWithZeroDataLoss(*config)
	case "custom":
		if s.customConfig != nil {
			streamInstance, err = stream.NewStreamWithCustomPerformance(*config, *s.customConfig)
		} else {
			streamInstance, err = stream.NewStream(*config)
		}
	default: // "default"
		streamInstance, err = stream.NewStream(*config)
	}

	if err != nil {
		return fmt.Errorf("failed to create stream processor: %w", err)
	}

	s.stream = streamInstance

	// Register filter condition
	if err = s.stream.RegisterFilter(condition); err != nil {
		return fmt.Errorf("failed to register filter condition: %w", err)
	}

	// Start stream processing
	s.stream.Start()
	return nil
}

// Emit adds data to the stream processing pipeline.
// Accepts type-safe map[string]interface{} format data.
//
// Parameters:
//   - data: Data to add, must be map[string]interface{} type
//
// Examples:
//
//	// Add device data
//	ssql.Emit(map[string]interface{}{
//	    "deviceId": "sensor001",
//	    "temperature": 25.5,
//	    "humidity": 60.0,
//	    "timestamp": time.Now(),
//	})
//
//	// Add user behavior data
//	ssql.Emit(map[string]interface{}{
//	    "userId": "user123",
//	    "action": "click",
//	    "page": "/home",
//	})
func (s *Streamsql) Emit(data map[string]interface{}) {
	if s.stream != nil {
		s.stream.Emit(data)
	}
}

// EmitSync processes data synchronously, returning results immediately.
// Only applicable for non-aggregation queries, aggregation queries will return an error.
// Accepts type-safe map[string]interface{} format data.
//
// Parameters:
//   - data: Data to process, must be map[string]interface{} type
//
// Returns:
//   - map[string]interface{}: Processed result data, returns nil if filter conditions don't match
//   - error: Processing error
//
// Examples:
//
//	result, err := ssql.EmitSync(map[string]interface{}{
//	    "deviceId": "sensor001",
//	    "temperature": 25.5,
//	})
//	if err != nil {
//	    log.Printf("processing error: %v", err)
//	} else if result != nil {
//	    // Use processed result immediately (result is map[string]interface{} type)
//	    fmt.Printf("Processing result: %v\n", result)
//	}
func (s *Streamsql) EmitSync(data map[string]interface{}) (map[string]interface{}, error) {
	if s.stream == nil {
		return nil, fmt.Errorf("stream not initialized")
	}

	// Check if it's a non-aggregation query
	if s.stream.IsAggregationQuery() {
		return nil, fmt.Errorf("synchronous mode only supports non-aggregation queries, use Emit() method for aggregation queries")
	}

	return s.stream.ProcessSync(data)
}

// IsAggregationQuery checks if the current query is an aggregation query
func (s *Streamsql) IsAggregationQuery() bool {
	if s.stream == nil {
		return false
	}
	return s.stream.IsAggregationQuery()
}

// Stream returns the underlying stream processor instance.
// Provides access to lower-level stream processing functionality.
//
// Returns:
//   - *stream.Stream: Underlying stream processor instance, returns nil if SQL not executed
//
// Common use cases:
//   - Add result processing callbacks
//   - Get result channel
//   - Manually control stream processing lifecycle
//
// Examples:
//
//	// Add result processing callback
//	ssql.Stream().AddSink(func(results []map[string]interface{}) {
//	    fmt.Printf("Processing results: %v\n", results)
//	})
//
//	// Get result channel
//	resultChan := ssql.Stream().GetResultsChan()
//	go func() {
//	    for result := range resultChan {
//	        // Process result
//	    }
//	}()
func (s *Streamsql) Stream() *stream.Stream {
	return s.stream
}

// GetStats returns stream processing statistics
func (s *Streamsql) GetStats() map[string]int64 {
	if s.stream != nil {
		return s.stream.GetStats()
	}
	return make(map[string]int64)
}

// GetDetailedStats returns detailed performance statistics
func (s *Streamsql) GetDetailedStats() map[string]interface{} {
	if s.stream != nil {
		return s.stream.GetDetailedStats()
	}
	return make(map[string]interface{})
}

// Stop stops the stream processor and releases related resources.
// After calling this method, the stream processor will stop receiving and processing new data.
//
// Recommended to call this method for cleanup before application exit:
//
//	defer ssql.Stop()
//
// Note: StreamSQL instance cannot be restarted after stopping, create a new instance.
func (s *Streamsql) Stop() {
	if s.stream != nil {
		s.stream.Stop()
	}
}

// AddSink directly adds result processing callback functions.
// Convenience wrapper for Stream().AddSink() for cleaner API calls.
//
// Parameters:
//   - sink: Result processing function, receives []map[string]interface{} type result data
//
// Examples:
//
//	// Directly add result processing
//	ssql.AddSink(func(results []map[string]interface{}) {
//	    fmt.Printf("Processing results: %v\n", results)
//	})
//
//	// Add multiple processors
//	ssql.AddSink(func(results []map[string]interface{}) {
//	    // Save to database
//	    saveToDatabase(results)
//	})
//	ssql.AddSink(func(results []map[string]interface{}) {
//	    // Send to message queue
//	    sendToQueue(results)
//	})
func (s *Streamsql) AddSink(sink func([]map[string]interface{})) {
	if s.stream != nil {
		s.stream.AddSink(sink)
	}
}

// PrintTable prints results to console in table format, similar to database output.
// Displays column names first, then data rows.
//
// Supported data formats:
//   - []map[string]interface{}: Multiple rows
//   - map[string]interface{}: Single row
//   - Other types: Direct print
//
// Example:
//
//	// Print results in table format
//	ssql.PrintTable()
//
//	// Output format:
//	// +--------+----------+
//	// | device | max_temp |
//	// +--------+----------+
//	// | aa     | 30.0     |
//	// | bb     | 22.0     |
//	// +--------+----------+
func (s *Streamsql) PrintTable() {
	s.AddSink(func(results []map[string]interface{}) {
		s.printTableFormat(results)
	})
}

// printTableFormat formats and prints table data
// Parameters:
//   - results: Result data of type []map[string]interface{}
func (s *Streamsql) printTableFormat(results []map[string]interface{}) {
	table.FormatTableData(results, s.fieldOrder)
}

// ToChannel returns result channel for asynchronous result retrieval.
// Provides non-blocking access to stream processing results.
//
// Returns:
//   - <-chan interface{}: Read-only result channel, returns nil if SQL not executed
//
// Example:
//
//	// Get result channel
//	resultChan := ssql.ToChannel()
//	if resultChan != nil {
//	    go func() {
//	        for result := range resultChan {
//	            fmt.Printf("Async result: %v\n", result)
//	        }
//	    }()
//	}

// ToChannel converts query results to channel output
// Returns a read-only channel for receiving query results
//
// Notes:
//   - Consumer must continuously read from channel to prevent stream processing blocking
//   - Channel transmits batch result data
func (s *Streamsql) ToChannel() <-chan []map[string]interface{} {
	if s.stream != nil {
		return s.stream.GetResultsChan()
	}
	return nil
}
