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
Package condition provides condition evaluation functionality for StreamSQL.

This package implements condition evaluation using the expr-lang library,
supporting complex boolean expressions for filtering and conditional logic.
It provides custom functions for SQL-like operations including LIKE pattern
matching and NULL checking.

# Core Features

• Boolean Expression Evaluation - Evaluate complex boolean conditions
• LIKE Pattern Matching - SQL-style pattern matching with % and _ wildcards
• NULL Checking - Support for IS NULL and IS NOT NULL operations
• Custom Functions - Extended function library for SQL compatibility
• Type Safety - Automatic type conversion and validation
• Performance Optimized - Compiled expressions for fast evaluation

# Condition Interface

Unified interface for condition evaluation:

	type Condition interface {
		Evaluate(env interface{}) bool
	}

# Custom Functions

Built-in SQL-compatible functions:

	// LIKE pattern matching
	like_match(text, pattern) - SQL LIKE operation with % and _ wildcards

	// NULL checking
	is_null(value) - Check if value is NULL
	is_not_null(value) - Check if value is not NULL

# Usage Examples

Basic condition evaluation:

	condition, err := NewExprCondition("age >= 18 AND status == 'active'")
	if err != nil {
		log.Fatal(err)
	}

	data := map[string]interface{}{
		"age": 25,
		"status": "active",
	}

	result := condition.Evaluate(data) // returns true

LIKE pattern matching:

	condition, err := NewExprCondition("like_match(name, 'John%')")
	data := map[string]interface{}{"name": "John Smith"}
	result := condition.Evaluate(data) // returns true

NULL checking:

	condition, err := NewExprCondition("is_not_null(email)")
	data := map[string]interface{}{"email": "user@example.com"}
	result := condition.Evaluate(data) // returns true

Complex conditions:

	condition, err := NewExprCondition(`
		age >= 18 AND
		like_match(email, '%@company.com') AND
		is_not_null(department)
	`)

# Pattern Matching

LIKE pattern matching supports:

	% - Matches any sequence of characters (including empty)
	_ - Matches exactly one character

Examples:

	'John%' matches 'John', 'John Smith', 'Johnny'
	'J_hn' matches 'John' but not 'Johan'
	'%@gmail.com' matches any email ending with @gmail.com

# Integration

Integrates with other StreamSQL components:

• Stream package - Data filtering and conditional processing
• RSQL package - WHERE and HAVING clause evaluation
• Types package - Data type handling and conversion
• Expr package - Expression parsing and evaluation
*/
package condition
