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
Package functions provides a comprehensive function registry and execution framework for StreamSQL.

This package implements a unified function management system that supports built-in functions,
custom user-defined functions, and specialized aggregation and analytical functions.
It serves as the central hub for all function-related operations in SQL expressions and stream processing.

# Core Features

• Unified Function Registry - Centralized registration and management of all function types
• Plugin Architecture - Runtime registration of custom functions without code modification
• Type System - Comprehensive function categorization and type validation
• Aggregation Support - Specialized interfaces for incremental aggregation functions
• Analytical Functions - Advanced analytical functions with state management
• Performance Optimization - Efficient function dispatch and execution
• Automatic Adaptation - Seamless integration between function types and aggregator modules

# Function Types

The package supports eight distinct function categories:

	TypeMath        - Mathematical functions (SIN, COS, SQRT, ABS, etc.)
	TypeString      - String manipulation functions (UPPER, LOWER, SUBSTRING, etc.)
	TypeConversion  - Type conversion functions (CAST, CONVERT, TO_NUMBER, etc.)
	TypeDateTime    - Date and time functions (NOW, DATE_FORMAT, EXTRACT, etc.)
	TypeAggregation - Aggregate functions (SUM, AVG, COUNT, MAX, MIN, etc.)
	TypeAnalytical  - Analytical functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
	TypeWindow      - Window functions (TUMBLING_WINDOW, SLIDING_WINDOW, etc.)
	TypeCustom      - User-defined custom functions

# Built-in Functions

Extensive collection of built-in functions across all categories:

	// Mathematical functions
	ABS(x)          - Absolute value
	SQRT(x)         - Square root
	POWER(x, y)     - Power operation
	ROUND(x, d)     - Round to decimal places
	
	// String functions
	UPPER(str)      - Convert to uppercase
	LOWER(str)      - Convert to lowercase
	LENGTH(str)     - String length
	SUBSTRING(str, start, len) - Extract substring
	
	// Aggregation functions
	SUM(field)      - Sum of values
	AVG(field)      - Average of values
	COUNT(*)        - Count of records
	MAX(field)      - Maximum value
	MIN(field)      - Minimum value

# Custom Function Registration

Simple API for registering custom functions:

	// Register a simple custom function
	RegisterCustomFunction(
		"fahrenheit_to_celsius",
		TypeConversion,
		"Temperature conversion",
		"Convert Fahrenheit to Celsius",
		1, 1, // min and max arguments
		func(ctx *FunctionContext, args []interface{}) (interface{}, error) {
			f := args[0].(float64)
			return (f - 32) * 5 / 9, nil
		},
	)
	
	// Register an aggregation function
	type CustomSumFunction struct {
		*BaseFunction
		sum float64
	}
	
	func (f *CustomSumFunction) Add(value interface{}) {
		if v, ok := value.(float64); ok {
			f.sum += v
		}
	}
	
	func (f *CustomSumFunction) Result() interface{} {
		return f.sum
	}

# Function Interfaces

The package defines several interfaces for different function types:

	// Basic function interface
	type Function interface {
		GetName() string
		GetType() FunctionType
		Execute(ctx *FunctionContext, args []interface{}) (interface{}, error)
	}
	
	// Aggregation function interface
	type AggregatorFunction interface {
		Function
		New() AggregatorFunction
		Add(value interface{})
		Result() interface{}
		Reset()
		Clone() AggregatorFunction
	}
	
	// Analytical function interface
	type AnalyticalFunction interface {
		AggregatorFunction
	}

# Adapter System

Automatic adaptation between function types and aggregator modules:

	// AggregatorAdapter - Adapts functions to aggregator interface
	type AggregatorAdapter struct {
		function AggregatorFunction
	}
	
	// AnalyticalAdapter - Adapts analytical functions
	type AnalyticalAdapter struct {
		function AnalyticalFunction
	}

# Performance Features

• Function Caching - Efficient function lookup and caching
• Lazy Initialization - Functions are initialized only when needed
• Batch Processing - Optimized batch execution for aggregation functions
• Memory Management - Automatic cleanup and resource management
• Type Optimization - Specialized execution paths for common data types

# Usage Examples

Basic function usage in SQL:

	SELECT UPPER(device_name), ROUND(temperature, 2)
	FROM stream
	WHERE ABS(temperature - 25) > 5

Aggregation functions with windows:

	SELECT device_id,
	       AVG(temperature) as avg_temp,
	       STDDEV(temperature) as temp_variance
	FROM stream
	GROUP BY device_id, TumblingWindow('5s')

Custom function in expressions:

	SELECT device_id,
	       fahrenheit_to_celsius(temperature) as temp_celsius
	FROM stream
	WHERE fahrenheit_to_celsius(temperature) > 30

# Integration

Seamless integration with other StreamSQL components:

• Expr package - Function execution in expressions
• Aggregator package - Automatic function adaptation
• RSQL package - Function parsing and validation
• Stream package - Real-time function execution
• Types package - Function context and data type support
*/
package functions