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
Package rsql provides SQL parsing and analysis capabilities for StreamSQL.

This package implements a comprehensive SQL parser specifically designed for stream processing,
supporting standard SQL syntax with extensions for window functions and streaming operations.
It transforms SQL queries into executable stream processing configurations.

# Core Features

• Complete SQL Parser - Full support for SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
• Window Function Support - Native parsing of tumbling, sliding, counting, and session windows
• Expression Analysis - Deep parsing of complex expressions, functions, and field references
• Error Recovery - Advanced error detection and recovery with detailed error reporting
• Function Validation - Integration with function registry for syntax and semantic validation
• AST Generation - Abstract Syntax Tree generation for query optimization
• Stream-Specific Extensions - Custom syntax for streaming operations and window management

# Supported SQL Syntax

Standard SQL clauses with streaming extensions:

	// Basic SELECT statement
	SELECT field1, field2, AGG_FUNC(field3)
	FROM stream
	WHERE condition
	GROUP BY field1, WindowFunction('params')
	HAVING aggregate_condition
	ORDER BY field1 ASC, field2 DESC
	LIMIT 100
	
	// Window functions
	TumblingWindow('5s')           - Non-overlapping time windows
	SlidingWindow('30s', '10s')    - Overlapping time windows
	CountingWindow(100)            - Count-based windows
	SessionWindow('5m')            - Session-based windows

# Lexical Analysis

Advanced tokenization with comprehensive token types:

	// Token types
	TOKEN_SELECT, TOKEN_FROM, TOKEN_WHERE    - SQL keywords
	TOKEN_IDENTIFIER, TOKEN_STRING           - Identifiers and literals
	TOKEN_NUMBER, TOKEN_FLOAT               - Numeric literals
	TOKEN_OPERATOR, TOKEN_COMPARISON        - Operators
	TOKEN_FUNCTION, TOKEN_WINDOW            - Function calls
	TOKEN_LPAREN, TOKEN_RPAREN             - Parentheses
	TOKEN_COMMA, TOKEN_SEMICOLON           - Delimiters

# Parser Architecture

Recursive descent parser with error recovery:

	type Parser struct {
		lexer         *Lexer
		errorRecovery *ErrorRecovery
		currentToken  Token
		input         string
	}
	
	// Main parsing entry point
	func (p *Parser) Parse() (*SelectStatement, error)
	
	// Clause-specific parsers
	func (p *Parser) parseSelect(stmt *SelectStatement) error
	func (p *Parser) parseFrom(stmt *SelectStatement) error
	func (p *Parser) parseWhere(stmt *SelectStatement) error
	func (p *Parser) parseGroupBy(stmt *SelectStatement) error

# Error Handling

Comprehensive error detection and recovery:

	// Error types
	type ParseError struct {
		Message   string
		Position  int
		Line      int
		Column    int
		Context   string
		ErrorType ErrorType
	}
	
	// Error recovery strategies
	type ErrorRecovery struct {
		errors   []*ParseError
		parser   *Parser
		strategies []RecoveryStrategy
	}

# Function Validation

Integration with function registry for validation:

	// Function validator
	type FunctionValidator struct {
		functionRegistry map[string]FunctionInfo
	}
	
	// Validation methods
	func (fv *FunctionValidator) ValidateFunction(name string, args []Expression) error
	func (fv *FunctionValidator) ValidateAggregateFunction(name string, context AggregateContext) error
	func (fv *FunctionValidator) ValidateWindowFunction(name string, params []Parameter) error

# AST Structure

StreamSQL AST representation:

	type SelectStatement struct {
		Fields    []Field
		Distinct  bool
		SelectAll bool
		Source    string
		Condition string
		Window    WindowDefinition
		GroupBy   []string
		Limit     int
		Having    string
	}
	
	type Field struct {
		Expression string
		Alias      string
		AggType    string
	}
	
	type WindowDefinition struct {
		Type     string
		Params   []interface{}
		TsProp   string
		TimeUnit time.Duration
	}

# Usage Examples

Basic SQL parsing:

	parser := NewParser("SELECT AVG(temperature) FROM stream WHERE device_id = 'sensor1'")
	stmt, err := parser.Parse()
	if err != nil {
		log.Fatal(err)
	}
	
	// Convert to stream configuration
	config, condition, err := stmt.ToStreamConfig()

Window function parsing:

	sql := `SELECT device_id, AVG(temperature)
	        FROM stream
	        GROUP BY device_id, TumblingWindow('5s')`
	config, condition, err := Parse(sql)

Complex query with multiple clauses:

	sql := `SELECT device_id,
	               AVG(temperature) as avg_temp,
	               MAX(humidity) as max_humidity
	        FROM stream
	        WHERE device_id LIKE 'sensor%'
	        GROUP BY device_id, SlidingWindow('1m', '30s')
	        HAVING avg_temp > 25
	        ORDER BY avg_temp DESC
	        LIMIT 10`
	config, condition, err := Parse(sql)

# Configuration Generation

Transformation from AST to stream processing configuration:

	type Config struct {
		WindowConfig     WindowConfig
		GroupFields      []string
		SelectFields     map[string]AggregateType
		FieldAlias       map[string]string
		SimpleFields     []string
		FieldExpressions map[string]FieldExpression
		FieldOrder       []string
		Where            string
		Having           string
		NeedWindow       bool
		Distinct         bool
		Limit            int
	}



# Integration

Seamless integration with other StreamSQL components:

• Functions package - Function validation and registry integration
• Expr package - Expression parsing and evaluation
• Types package - Configuration and data type definitions
• Stream package - Configuration application and execution
• Window package - Window function parsing and configuration
*/
package rsql