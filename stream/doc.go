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
Package stream provides the core stream processing engine for StreamSQL.

This package implements the central stream processing pipeline that orchestrates data flow,
window management, aggregation, filtering, and result generation. It serves as the execution
engine that brings together all StreamSQL components into a cohesive streaming system.

# Core Features

• Real-time Stream Processing - High-throughput, low-latency data stream processing
• Window Management - Integration with all window types (tumbling, sliding, counting, session)
• Aggregation Engine - Efficient aggregation with incremental computation
• Filtering Pipeline - Multi-stage filtering with WHERE and HAVING clause support
• Performance Modes - Configurable performance profiles for different use cases
• Metrics and Monitoring - Comprehensive performance metrics and health monitoring
• Persistence Support - Optional data persistence for reliability and recovery
• Backpressure Handling - Intelligent backpressure management and overflow strategies

# Stream Architecture

The stream processing pipeline consists of several key components:

	type Stream struct {
		dataChan       chan map[string]interface{}  // Input data channel
		filter         condition.Condition          // WHERE clause filter
		Window         window.Window                // Window manager
		aggregator     aggregator.Aggregator        // Aggregation engine
		config         types.Config                 // Stream configuration
		sinks          []func([]map[string]interface{}) // Result processors
		resultChan     chan []map[string]interface{} // Result channel

		dataStrategy   DataProcessingStrategy       // Data processing strategy
	}

# Performance Modes

Configurable performance profiles for different scenarios:

	// High Performance Mode
	// - Optimized for maximum throughput
	// - Larger buffer sizes
	// - Batch processing optimization
	stream := NewStreamWithHighPerformance(config)

	// Low Latency Mode
	// - Optimized for minimal processing delay
	// - Smaller buffer sizes
	// - Immediate processing
	stream := NewStreamWithLowLatency(config)

	// Zero Data Loss Mode
	// - Guaranteed data persistence
	// - Synchronous processing
	// - Enhanced error recovery
	stream := NewStreamWithZeroDataLoss(config)

	// Custom Performance Mode
	// - User-defined performance parameters
	customConfig := &PerformanceConfig{
		BufferSize:     1000,
		BatchSize:      50,
		FlushInterval:  time.Second,
		WorkerPoolSize: 4,
	}
	stream := NewStreamWithCustomPerformance(config, *customConfig)

# Data Processing Pipeline

Multi-stage processing pipeline with optimized data flow:

 1. Data Ingestion
    ├── Input validation and type checking
    ├── Timestamp extraction and normalization
    └── Initial data transformation

 2. Filtering (WHERE clause)
    ├── Field-based filtering
    ├── Expression evaluation
    └── Early data rejection

 3. Window Processing
    ├── Window assignment
    ├── Data buffering
    └── Window trigger management

 4. Aggregation
    ├── Group-by processing
    ├── Aggregate function execution
    └── Incremental computation

 5. Post-Aggregation Filtering (HAVING clause)
    ├── Aggregate result filtering
    ├── Complex condition evaluation
    └── Final result validation

 6. Result Generation
    ├── Field projection
    ├── Alias application
    └── Output formatting

# Window Integration

Seamless integration with all window types:

	// Tumbling Windows - Non-overlapping time-based windows
	config.WindowConfig = WindowConfig{
		Type: "tumbling",
		Params: map[string]interface{}{
			"size": "5s",
		},
	}

	// Sliding Windows - Overlapping time-based windows
	config.WindowConfig = WindowConfig{
		Type: "sliding",
		Params: map[string]interface{}{
			"size": "30s",
			"slide": "10s",
		},
	}

	// Counting Windows - Count-based windows
	config.WindowConfig = WindowConfig{
		Type: "counting",
		Params: map[string]interface{}{
			"count": 100,
		},
	}

	// Session Windows - Activity-based windows
	config.WindowConfig = WindowConfig{
		Type: "session",
		Params: map[string]interface{}{
			"timeout": "5m",
			"groupBy": "user_id",
		},
	}

# Metrics and Monitoring

Comprehensive performance monitoring:

	type MetricsManager struct {
		processedCount    int64     // Total processed records
		filteredCount     int64     // Filtered out records
		aggregatedCount   int64     // Aggregated records
		errorCount        int64     // Processing errors
		processingTime    time.Duration // Average processing time
		throughput        float64   // Records per second
		memoryUsage       int64     // Memory consumption
		bufferUtilization float64   // Buffer usage percentage
	}

	// Get basic statistics
	stats := stream.GetStats()
	fmt.Printf("Processed: %d, Errors: %d\n", stats["processed"], stats["errors"])

	// Get detailed performance metrics
	detailed := stream.GetDetailedStats()
	fmt.Printf("Throughput: %.2f records/sec\n", detailed["throughput"])
	fmt.Printf("Memory Usage: %d bytes\n", detailed["memory_usage"])

# Backpressure Management

Intelligent handling of system overload:

	// Overflow strategies
	const (
		OverflowStrategyDrop     = "drop"     // Drop oldest data
		OverflowStrategyBlock    = "block"    // Block new data
		OverflowStrategySpill    = "spill"    // Spill to disk
		OverflowStrategyCompress = "compress" // Compress data
	)

	// Configure backpressure handling
	config.PerformanceConfig.OverflowStrategy = OverflowStrategySpill
	config.PerformanceConfig.BufferSize = 10000
	config.PerformanceConfig.HighWaterMark = 0.8

# Usage Examples

Basic stream processing:

	// Create stream with default configuration
	stream, err := NewStream(config)
	if err != nil {
		log.Fatal(err)
	}

	// Register result handler
	stream.AddSink(func(results []map[string]interface{}) {
		fmt.Printf("Results: %v\n", results)
	})

	// Start processing
	stream.Start()

	// Send data
	stream.Emit(map[string]interface{}{
		"device_id":   "sensor001",
		"temperature": 25.5,
		"timestamp":   time.Now(),
	})

High-performance stream processing:

	// Create high-performance stream
	stream, err := NewStreamWithHighPerformance(config)

	// Configure for maximum throughput
	stream.SetBufferSize(50000)
	stream.SetBatchSize(1000)
	stream.SetWorkerPoolSize(8)

	// Enable metrics monitoring
	stream.EnableMetrics(true)

	// Process data in batches
	for _, batch := range dataBatches {
		stream.EmitBatch(batch)
	}

Synchronous processing for non-aggregation queries:

	// Process single record synchronously
	result, err := stream.ProcessSync(data)
	if err != nil {
		log.Printf("Processing error: %v", err)
	} else if result != nil {
		fmt.Printf("Immediate result: %v\n", result)
	}

# Integration

Central integration point for all StreamSQL components:

• RSQL package - Configuration parsing and application
• Window package - Window lifecycle management
• Aggregator package - Aggregation execution
• Functions package - Function execution in expressions
• Condition package - Filter condition evaluation
• Types package - Data type handling and configuration
• Logger package - Comprehensive logging and debugging
*/
package stream
