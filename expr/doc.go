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
Package expr provides expression parsing and evaluation capabilities for StreamSQL.

This package implements a comprehensive expression engine that supports mathematical operations,
logical comparisons, function calls, field references, and complex CASE expressions.
It serves as the foundation for WHERE clauses, HAVING clauses, and computed fields in SQL queries.

# Core Features

• Mathematical Operations - Supports arithmetic operators (+, -, *, /, %, ^) with proper precedence
• Logical Operations - Boolean logic with AND, OR operators and comparison operators (=, !=, <, >, <=, >=, LIKE)
• Function Integration - Seamless integration with the functions package for built-in and custom functions
• Field References - Dynamic field access with dot notation support for nested data structures
• CASE Expressions - Full support for both simple and searched CASE expressions
• Type Safety - Automatic type conversion and validation during expression evaluation
• Fallback Support - Integration with expr-lang/expr library for complex expressions

# Expression Types

The package supports various expression node types:

	// Basic types
	TypeNumber      - Numeric constants (integers and floats)
	TypeString      - String literals with proper escaping
	TypeField       - Field references (e.g., "temperature", "device.id")
	TypeOperator    - Binary and unary operators
	TypeFunction    - Function calls with argument validation
	TypeParenthesis - Grouped expressions for precedence control
	TypeCase        - CASE expressions for conditional logic

# Usage Examples

Basic mathematical expression:

	expr, err := NewExpression("temperature * 1.8 + 32")
	if err != nil {
		log.Fatal(err)
	}
	result, err := expr.Evaluate(data)

Logical expression with field references:

	expr, err := NewExpression("temperature > 25 AND humidity < 60")
	result, err := expr.Evaluate(data)

Function call expression:

	expr, err := NewExpression("UPPER(device_name) LIKE 'SENSOR%'")
	result, err := expr.Evaluate(data)

CASE expression for conditional logic:

	expr, err := NewExpression(`
		CASE
			WHEN temperature > 30 THEN 'hot'
			WHEN temperature > 20 THEN 'warm'
			ELSE 'cold'
		END
	`)
	result, err := expr.Evaluate(data)

# Operator Precedence

The expression parser follows standard mathematical precedence rules:

 1. Parentheses (highest)
 2. Power (^)
 3. Multiplication, Division, Modulo (*, /, %)
 4. Addition, Subtraction (+, -)
 5. Comparison (>, <, >=, <=, LIKE, IS)
 6. Equality (=, ==, !=, <>)
 7. Logical AND
 8. Logical OR (lowest)

# Error Handling

The package provides comprehensive error handling with detailed error messages:

• Syntax validation during expression creation
• Type checking during evaluation
• Function argument validation
• Graceful fallback to expr-lang for unsupported expressions

# Performance Considerations

• Expressions are parsed once and can be evaluated multiple times
• Built-in operator optimization for common mathematical operations
• Lazy evaluation for logical operators (short-circuiting)
• Efficient field access caching for repeated evaluations
• Automatic fallback to optimized expr-lang library when needed

# Integration

This package integrates seamlessly with other StreamSQL components:

• Functions package - For built-in and custom function execution
• Types package - For data type definitions and conversions
• Stream package - For real-time expression evaluation in data streams
• RSQL package - For SQL parsing and expression extraction
*/
package expr
