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
		Stop()                                // Stop window operations and clean up resources
		OutputChan() <-chan []types.Row       // Get output channel
		SetCallback(func([]types.Row))        // Set callback function
		Trigger() error                       // Manual trigger
	}

# Tumbling Windows

Non-overlapping time-based windows:

	// Create tumbling window with processing time (default)
	config := types.WindowConfig{
		Type: "tumbling",
		Params: []interface{}{"5s"},  // 5-second windows
		TsProp: "timestamp",
		TimeCharacteristic: types.ProcessingTime, // Uses system clock
	}
	window, err := NewTumblingWindow(config)

	// Create tumbling window with event time
	config := types.WindowConfig{
		Type: "tumbling",
		Params: []interface{}{"5s"},  // 5-second windows
		TsProp: "timestamp",
		TimeCharacteristic: types.EventTime, // Uses event timestamps
		MaxOutOfOrderness: 2 * time.Second, // Allow 2 seconds of out-of-order data
		WatermarkInterval: 200 * time.Millisecond, // Update watermark every 200ms
		AllowedLateness: 1 * time.Second, // Allow 1 second of late data after window closes
	}
	window, err := NewTumblingWindow(config)

	// Window characteristics:
	// - Fixed size (e.g., 5 seconds)
	// - No overlap between windows
	// - Triggers at regular intervals (ProcessingTime) or based on watermark (EventTime)
	// - Memory efficient
	// - Suitable for periodic aggregations

	// ProcessingTime example timeline (based on data arrival):
	// Window 1: [00:00 - 00:05) - triggers when 5s elapsed from first data
	// Window 2: [00:05 - 00:10) - triggers when next 5s elapsed
	// Window 3: [00:10 - 00:15) - triggers when next 5s elapsed

	// EventTime example timeline (based on event timestamps):
	// Window 1: [00:00 - 00:05) - triggers when watermark >= 00:05
	// Window 2: [00:05 - 00:10) - triggers when watermark >= 00:10
	// Window 3: [00:10 - 00:15) - triggers when watermark >= 00:15

# Sliding Windows

Overlapping time-based windows with configurable slide interval:

	// Create sliding window with processing time (default)
	config := types.WindowConfig{
		Type: "sliding",
		Params: []interface{}{"30s", "10s"}, // 30-second window size, 10-second slide
		TsProp: "timestamp",
		TimeCharacteristic: types.ProcessingTime, // Uses system clock
	}
	window, err := NewSlidingWindow(config)

	// Create sliding window with event time
	config := types.WindowConfig{
		Type: "sliding",
		Params: []interface{}{"30s", "10s"}, // 30-second window size, 10-second slide
		TsProp: "timestamp",
		TimeCharacteristic: types.EventTime, // Uses event timestamps
		MaxOutOfOrderness: 2 * time.Second, // Allow 2 seconds of out-of-order data
		WatermarkInterval: 200 * time.Millisecond, // Update watermark every 200ms
	}
	window, err := NewSlidingWindow(config)

	// Window characteristics:
	// - Fixed size with configurable slide
	// - Overlapping windows
	// - More frequent updates
	// - Higher memory usage
	// - Suitable for smooth trend analysis

	// ProcessingTime example timeline (30s window, 10s slide, based on data arrival):
	// Window 1: [00:00 - 00:30) - triggers when 30s elapsed from first data
	// Window 2: [00:10 - 00:40) - triggers 10s after Window 1
	// Window 3: [00:20 - 00:50) - triggers 10s after Window 2

	// EventTime example timeline (30s window, 10s slide, based on event timestamps):
	// Window 1: [00:00 - 00:30) - triggers when watermark >= 00:30
	// Window 2: [00:10 - 00:40) - triggers when watermark >= 00:40
	// Window 3: [00:20 - 00:50) - triggers when watermark >= 00:50

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

# Time Characteristics

Windows support two time characteristics:

## ProcessingTime (Default)
- Uses system clock for window operations
- Windows trigger based on when data arrives
- Cannot handle out-of-order data
- Lower latency, but results may be inconsistent
- Suitable for real-time monitoring and low-latency requirements

## EventTime
- Uses event timestamps for window operations
- Windows trigger based on event time via watermark mechanism
- Can handle out-of-order and late-arriving data
- Consistent results, but may have higher latency
- Suitable for accurate time-based analysis and historical data processing

## Watermark Mechanism
For EventTime windows, watermark indicates that no events with timestamp less than watermark time are expected:
- Watermark = max(event_time) - max_out_of_orderness
- Windows trigger when watermark >= window_end_time
- Late data (before watermark) can be detected and handled specially

## Allowed Lateness
For EventTime windows, `allowedLateness` allows windows to accept late data after they have been triggered:
- When watermark >= window_end, window triggers and outputs result
- Window remains open until watermark >= window_end + allowedLateness
- Late data arriving within allowedLateness triggers delayed updates (window fires again)
- After allowedLateness expires, window closes and late data is ignored
- Default: 0 (no late data accepted after window closes)

Example:
- Window [00:00 - 00:05) triggers when watermark >= 00:05
- With allowedLateness = 2s, window stays open until watermark >= 00:07
- Late data with timestamp in [00:00 - 00:05) arriving before watermark >= 00:07 triggers delayed update
- After watermark >= 00:07, window closes and late data is ignored

## Idle Source Mechanism
For EventTime windows, `idleTimeout` enables watermark advancement based on processing time when the data source is idle:
- Normally: Watermark advances based on event time (Watermark = max(event_time) - maxOutOfOrderness)
- When idle: If no data arrives within idleTimeout, watermark advances based on processing time
- This ensures windows can close even when the data source stops sending data
- Prevents memory leaks from windows that never close
- Default: 0 (disabled, watermark only advances based on event time)

Example:
- Window [00:00 - 00:05) has data with max event time = 00:02
- Data source stops sending data at 00:03
- With idleTimeout = 5s, after 5 seconds of no data (at 00:08), watermark advances based on processing time
- Watermark = currentProcessingTime - maxOutOfOrderness = 00:08 - 1s = 00:07
- Window [00:00 - 00:05) can trigger (watermark >= 00:05) and close

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
