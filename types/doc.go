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
Package types provides core type definitions and data structures for StreamSQL.

This package defines fundamental data types, configuration structures, and interfaces
used throughout the StreamSQL stream processing pipeline. It ensures type safety
and provides a unified API for data manipulation across components.

# Core Features

• Data Types - Core data structures for stream processing
• Configuration Management - Centralized configuration structures
• Type Safety - Strong typing with validation
• Serialization Support - JSON serialization support
• Cross-Component Compatibility - Shared types across packages

# Configuration Structures

Core configuration types:

	type Config struct {
		WindowConfig     WindowConfig                        // Window settings
		GroupFields      []string                            // GROUP BY fields
		SelectFields     map[string]aggregator.AggregateType // SELECT aggregations
		FieldAlias       map[string]string                   // Field aliases
		SimpleFields     []string                            // Non-aggregated fields
		FieldExpressions map[string]FieldExpression          // Computed expressions
		Where            string                              // WHERE clause
		Having           string                              // HAVING clause
		NeedWindow       bool                                // Window requirement
		Distinct         bool                                // DISTINCT flag
		Limit            int                                 // LIMIT clause
		PerformanceConfig PerformanceConfig                  // Performance settings
	}

# Window Configuration

Unified configuration for all window types:

	type WindowConfig struct {
		Type       string                 // Window type
		Params     map[string]interface{} // Parameters
		TsProp     string                 // Timestamp property
		TimeUnit   time.Duration          // Time unit
		GroupByKey string                 // Grouping key
	}
	
	// Example configurations
	// Tumbling window
	windowConfig := WindowConfig{
		Type: "tumbling",
		Params: map[string]interface{}{
			"size": "5s",
		},
		TsProp: "timestamp",
	}
	
	// Sliding window
	windowConfig := WindowConfig{
		Type: "sliding",
		Params: map[string]interface{}{
			"size": "30s",
			"slide": "10s",
		},
		TsProp: "timestamp",
	}
	
	// Counting window
	windowConfig := WindowConfig{
		Type: "counting",
		Params: map[string]interface{}{
			"count": 100,
		},
	}
	
	// Session window
	windowConfig := WindowConfig{
		Type: "session",
		Params: map[string]interface{}{
			"timeout": "5m",
		},
		GroupByKey: "user_id",
	}

# Performance Configuration

Comprehensive performance tuning options:

	type PerformanceConfig struct {
		// Buffer management
		BufferSize       int           // Input buffer size
		BatchSize        int           // Processing batch size
		FlushInterval    time.Duration // Automatic flush interval
		HighWaterMark    float64       // Buffer high water mark (0.0-1.0)
		LowWaterMark     float64       // Buffer low water mark (0.0-1.0)
		
		// Worker pool configuration
		WorkerPoolSize   int           // Number of worker goroutines
		MaxWorkers       int           // Maximum worker limit
		WorkerIdleTime   time.Duration // Worker idle timeout
		
		// Overflow handling
		OverflowStrategy string        // "drop", "block", "spill", "compress"
		SpillDirectory   string        // Directory for spill files
		CompressionLevel int           // Compression level (1-9)
		
		// Memory management
		MaxMemoryUsage   int64         // Maximum memory usage in bytes
		GCInterval       time.Duration // Garbage collection interval
		MemoryThreshold  float64       // Memory usage threshold
		
		// Monitoring
		MetricsEnabled   bool          // Enable metrics collection
		MetricsInterval  time.Duration // Metrics collection interval
		HealthCheckPort  int           // Health check HTTP port
		
		// Persistence
		PersistenceEnabled bool        // Enable data persistence
		PersistenceType    string      // "memory", "file", "database"
		PersistencePath    string      // Persistence storage path
		RecoveryEnabled    bool        // Enable automatic recovery
	}

# Field Management

Advanced field handling and expression support:

	type FieldExpression struct {
		Field      string   // Field name
		Expression string   // Expression
		Fields     []string // Referenced fields
	}
	
	type Projection struct {
		SourceType ProjectionSourceType // Source type (field, expression, aggregate)
		Source     string               // Source identifier
		Alias      string               // Output alias
		DataType   string               // Expected data type
	}
	
	type ProjectionSourceType string
	
	const (
		ProjectionSourceField      ProjectionSourceType = "field"      // Direct field reference
		ProjectionSourceExpression ProjectionSourceType = "expression" // Computed expression
		ProjectionSourceAggregate  ProjectionSourceType = "aggregate"  // Aggregate function
		ProjectionSourceConstant   ProjectionSourceType = "constant"   // Constant value
	)

# Data Row Representation

Type-safe data row structures for stream processing:

	type Row struct {
		Data      map[string]interface{} // Row data
		Timestamp time.Time              // Row timestamp
		Metadata  map[string]interface{} // Additional metadata
		GroupKey  string                 // Grouping key for aggregation
		WindowID  string                 // Window identifier
	}
	
	// Row creation and manipulation
	func NewRow(data map[string]interface{}) *Row
	func (r *Row) GetValue(field string) interface{}
	func (r *Row) SetValue(field string, value interface{})
	func (r *Row) HasField(field string) bool
	func (r *Row) Clone() *Row

# Time Management

Time-based data structures for window processing:

	type TimeSlot struct {
		Start    time.Time // Slot start time
		End      time.Time // Slot end time
		Duration time.Duration // Slot duration
		ID       string    // Unique slot identifier
	}
	
	// Time slot operations
	func NewTimeSlot(start time.Time, duration time.Duration) *TimeSlot
	func (ts *TimeSlot) Contains(timestamp time.Time) bool
	func (ts *TimeSlot) Overlaps(other *TimeSlot) bool
	func (ts *TimeSlot) String() string

# Configuration Presets

Pre-defined configuration templates for common use cases:

	// High Performance Configuration
	func NewHighPerformanceConfig() *PerformanceConfig {
		return &PerformanceConfig{
			BufferSize:       50000,
			BatchSize:        1000,
			WorkerPoolSize:   8,
			FlushInterval:    100 * time.Millisecond,
			OverflowStrategy: "spill",
			MetricsEnabled:   true,
		}
	}
	
	// Low Latency Configuration
	func NewLowLatencyConfig() *PerformanceConfig {
		return &PerformanceConfig{
			BufferSize:       1000,
			BatchSize:        10,
			WorkerPoolSize:   2,
			FlushInterval:    10 * time.Millisecond,
			OverflowStrategy: "drop",
			MetricsEnabled:   false,
		}
	}
	
	// Zero Data Loss Configuration
	func NewZeroDataLossConfig() *PerformanceConfig {
		return &PerformanceConfig{
			BufferSize:         10000,
			BatchSize:          100,
			WorkerPoolSize:     4,
			FlushInterval:      time.Second,
			OverflowStrategy:   "block",
			PersistenceEnabled: true,
			RecoveryEnabled:    true,
			MetricsEnabled:     true,
		}
	}

# Usage Examples

Basic configuration:

	config := &Config{
		WindowConfig: WindowConfig{
			Type: "tumbling",
			Params: map[string]interface{}{"size": "5s"},
		},
		GroupFields: []string{"device_id"},
		SelectFields: map[string]aggregator.AggregateType{
			"temperature": aggregator.AggregateTypeAvg,
		},
		NeedWindow: true,
	}

Data row operations:

	row := NewRow(map[string]interface{}{
		"device_id":   "sensor001",
		"temperature": 25.5,
	})
	
	deviceID := row.GetValue("device_id").(string)
	row.SetValue("processed", true)

# Integration

Integrates with other StreamSQL components:

• Stream Package - Core data types for stream processing
• Window Package - WindowConfig for window configurations
• Aggregator Package - AggregateType definitions
• Condition Package - Data structures for clause evaluation
• Functions Package - Type definitions for functions
• RSQL Package - Config structures for query execution
*/
package types