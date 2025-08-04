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
Package window provides windowing functionality for StreamSQL stream processing.

This package implements various types of windows for aggregating streaming data
over time intervals or record counts. It supports tumbling, sliding, counting,
and session windows with efficient memory management and concurrent processing.

# Core Features

• Multiple Window Types - Tumbling, Sliding, Counting, and Session windows
• Time Management - Time-based window boundaries and event time processing
• Trigger Mechanisms - Triggering based on time, count, or custom conditions
• Memory Efficiency - Optimized data structures and memory management
• Concurrent Processing - Thread-safe operations
• Late Data Handling - Configurable policies for late-arriving data

# Window Types

Four distinct window types for different stream processing scenarios:

• Tumbling Windows - Non-overlapping, fixed-size time windows
• Sliding Windows - Overlapping time windows with configurable slide interval  
• Counting Windows - Count-based windows that trigger after N records
• Session Windows - Activity-based windows with configurable timeout

# Window Interface

All window types implement a unified Window interface:

	type Window interface {
		Add(row types.Row) error              // Add data to window
		Reset() error                         // Reset window state
		Start() error                         // Start window processing
		OutputChan() <-chan []types.Row       // Get output channel
		SetCallback(func([]types.Row))        // Set callback function
		Trigger() error                       // Manual trigger
	}

# Tumbling Windows

Non-overlapping time-based windows:

	// Create tumbling window
	config := types.WindowConfig{
		Type: "tumbling",
		Params: map[string]interface{}{
			"size": "5s",  // 5-second windows
		},
		TsProp: "timestamp",
	}
	window, err := NewTumblingWindow(config)
	
	// Window characteristics:
	// - Fixed size (e.g., 5 seconds)
	// - No overlap between windows
	// - Triggers at regular intervals
	// - Memory efficient
	// - Suitable for periodic aggregations
	
	// Example timeline:
	// Window 1: [00:00 - 00:05)
	// Window 2: [00:05 - 00:10)
	// Window 3: [00:10 - 00:15)

# Sliding Windows

Overlapping time-based windows with configurable slide interval:

	// Create sliding window
	config := types.WindowConfig{
		Type: "sliding",
		Params: map[string]interface{}{
			"size":  "30s", // 30-second window size
			"slide": "10s", // 10-second slide interval
		},
		TsProp: "timestamp",
	}
	window, err := NewSlidingWindow(config)
	
	// Window characteristics:
	// - Fixed size with configurable slide
	// - Overlapping windows
	// - More frequent updates
	// - Higher memory usage
	// - Suitable for smooth trend analysis
	
	// Example timeline (30s window, 10s slide):
	// Window 1: [00:00 - 00:30)
	// Window 2: [00:10 - 00:40)
	// Window 3: [00:20 - 00:50)

# Counting Windows

Count-based windows that trigger after a specified number of records:

	// Create counting window
	config := types.WindowConfig{
		Type: "counting",
		Params: map[string]interface{}{
			"count": 100, // Trigger every 100 records
		},
	}
	window, err := NewCountingWindow(config)
	
	// Window characteristics:
	// - Fixed record count
	// - Time-independent
	// - Predictable memory usage
	// - Suitable for batch processing
	// - Handles variable data rates
	
	// Example:
	// Window 1: Records 1-100
	// Window 2: Records 101-200
	// Window 3: Records 201-300

# Session Windows

Activity-based windows with configurable session timeout:

	// Create session window
	config := types.WindowConfig{
		Type: "session",
		Params: map[string]interface{}{
			"timeout": "5m", // 5-minute session timeout
		},
		GroupByKey: "user_id", // Group sessions by user
	}
	window, err := NewSessionWindow(config)
	
	// Window characteristics:
	// - Variable window size
	// - Activity-based triggers
	// - Per-group session tracking
	// - Automatic session expiration
	// - Suitable for user behavior analysis
	
	// Example (5-minute timeout):
	// User A: [10:00 - 10:15) - 15-minute session
	// User B: [10:05 - 10:08) - 3-minute session
	// User A: [10:20 - 10:25) - New 5-minute session

# Window Factory

Centralized window creation:

	func CreateWindow(config types.WindowConfig) (Window, error)

# Time Management

Time handling for window operations:

	func GetTimestamp(data interface{}, timeField string) (time.Time, error)

	type TimeSlot struct {
		Start    time.Time
		End      time.Time
		Duration time.Duration
	}

# Performance Features

• Memory Management - Efficient buffer management and garbage collection
• Concurrency - Thread-safe operations with minimal locking
• Time Efficiency - Optimized timestamp processing and timer management

# Usage Examples

Basic tumbling window:

	config := types.WindowConfig{
		Type: "tumbling",
		Params: map[string]interface{}{"size": "10s"},
		TsProp: "timestamp",
	}
	window, err := CreateWindow(config)
	window.SetCallback(func(results []types.Row) {
		fmt.Printf("Window results: %d records\n", len(results))
	})
	window.Start()

Sliding window:

	config := types.WindowConfig{
		Type: "sliding",
		Params: map[string]interface{}{
			"size":  "1m",
			"slide": "10s",
		},
		TsProp: "event_time",
	}
	window, err := NewSlidingWindow(config)

Session window:

	config := types.WindowConfig{
		Type: "session",
		Params: map[string]interface{}{"timeout": "30m"},
		GroupByKey: "user_id",
	}
	window, err := NewSessionWindow(config)

# Integration

Integrates with other StreamSQL components:

• Stream package - Stream processing and data flow
• RSQL package - SQL-based window definitions
• Functions package - Aggregation functions for window results
• Types package - Shared data types and configuration
*/
package window