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
Package aggregator provides data aggregation functionality for StreamSQL.

This package implements group-based aggregation operations for stream processing,
supporting various aggregation functions and expression evaluation. It provides
thread-safe aggregation with support for custom expressions and built-in functions.

# Core Features

• Group Aggregation - Group data by specified fields and apply aggregation functions
• Built-in Functions - Support for Sum, Count, Avg, Max, Min, and more
• Expression Support - Custom expression evaluation within aggregations
• Thread Safety - Concurrent aggregation operations with proper synchronization
• Type Flexibility - Automatic type conversion and validation
• Performance Optimized - Efficient memory usage and processing

# Aggregation Types

Supported aggregation functions (re-exported from functions package):

	// Mathematical aggregations
	Sum, Count, Avg, Max, Min
	StdDev, StdDevS, Var, VarS
	Median, Percentile

	// Collection aggregations
	Collect, LastValue, MergeAgg
	Deduplicate

	// Window aggregations
	WindowStart, WindowEnd

	// Analytical functions
	Lag, Latest, ChangedCol, HadChanged

	// Custom expressions
	Expression

# Core Interfaces

Main aggregation interfaces:

	type Aggregator interface {
		Add(data interface{}) error
		Put(key string, val interface{}) error
		GetResults() ([]map[string]interface{}, error)
		Reset()
		RegisterExpression(field, expression string, fields []string, evaluator func(data interface{}) (interface{}, error))
	}

	type AggregatorFunction interface {
		New() AggregatorFunction
		Add(value interface{})
		Result() interface{}
	}

# Aggregation Configuration

Field configuration for aggregations:

	type AggregationField struct {
		InputField    string        // Source field name
		AggregateType AggregateType // Aggregation function type
		OutputAlias   string        // Result field alias
	}

# Usage Examples

Basic group aggregation:

	// Define aggregation fields
	aggFields := []AggregationField{
		{InputField: "temperature", AggregateType: Avg, OutputAlias: "avg_temp"},
		{InputField: "humidity", AggregateType: Max, OutputAlias: "max_humidity"},
		{InputField: "device_id", AggregateType: Count, OutputAlias: "device_count"},
	}

	// Create group aggregator
	aggregator := NewGroupAggregator([]string{"location"}, aggFields)

	// Add data
	data := map[string]interface{}{
		"location": "room1",
		"temperature": 25.5,
		"humidity": 60,
		"device_id": "sensor001",
	}
	aggregator.Add(data)

	// Get results
	results, err := aggregator.GetResults()

Expression-based aggregation:

	// Register custom expression
	aggregator.RegisterExpression(
		"comfort_index",
		"temperature * 0.7 + humidity * 0.3",
		[]string{"temperature", "humidity"},
		func(data interface{}) (interface{}, error) {
			// Custom evaluation logic
			return evaluateComfortIndex(data)
		},
	)

Multiple group aggregation:

	// Group by multiple fields
	aggregator := NewGroupAggregator(
		[]string{"location", "device_type"},
		aggFields,
	)

	// Results will be grouped by both location and device_type
	results, err := aggregator.GetResults()

# Built-in Aggregators

Create built-in aggregation functions:

	// Create specific aggregator
	sumAgg := CreateBuiltinAggregator(Sum)
	avgAgg := CreateBuiltinAggregator(Avg)
	countAgg := CreateBuiltinAggregator(Count)

	// Use aggregator
	sumAgg.Add(10)
	sumAgg.Add(20)
	result := sumAgg.Result() // returns 30

# Custom Aggregators

Register custom aggregation functions:

	Register("custom_avg", func() AggregatorFunction {
		return &CustomAvgAggregator{}
	})

# Integration

Integrates with other StreamSQL components:

• Functions package - Built-in aggregation function implementations
• Stream package - Real-time data aggregation in streams
• Window package - Window-based aggregation operations
• Types package - Data type definitions and conversions
• RSQL package - SQL GROUP BY and aggregation parsing
*/
package aggregator
