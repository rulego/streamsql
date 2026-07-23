# Function validation function

## Overview

StreamSQL Now supports function validation during the parsing phase, detecting and reporting unknown function usage, providing better error handling and user experience.

## Functional Features

### 1. Function existence check

During SQL parsing, the system automatically validates function calls at the following locations:
- SELECT Functions in clauses
- WHERE Functions in clauses
- HAVING Functions in clauses

### 2. Supported function types

Validators can recognize the following types of functions:
- **Built-in mathematical functions**: `abs`, `sqrt`, `sin`, `cos`, `tan`, `floor`, `ceil`, `round`, `log`, `log10`, `exp`, `pow`, `mod`
- **Registered Custom Function**: Function registered via `functions.Register()`
- **expr-lang Function**: Functions bridged via expr-lang

### 3. Error types

A dedicated error type `ErrorTypeUnknownFunction` has been added to identify unknown function errors.

### 4. Smart suggestions

When an unknown function is detected, the system provides useful suggestions:
- Suggestions for correcting common spelling mistakes
- General guidance for function registration and usage

## Usage Examples

### Correct Use of Functions

```go
// Built-in functions
ssql := streamsql.New()
err := ssql.Execute("SELECT abs(temperature) FROM stream")
// err == nil

// Nested functions
err = ssql.Execute("SELECT sqrt(abs(temperature)) FROM stream")
// err == nil
```

### Unknown function error

```go
ssql := streamsql.New()
err := ssql.Execute("SELECT unknown_func(temperature) FROM stream")
// err != nil
// err.Error() contains "Unknown function 'unknown_func'"
```

### Custom function registration

```go
// Register custom functions
functions.Register("custom_func", func(args ...interface{}) (interface{}, error) {
    // Function implementation
    return args[0], nil
})

// Custom functions can now be used
ssql := streamsql.New()
err := ssql.Execute("SELECT custom_func(temperature) FROM stream")
// err == nil
```

## Error Handling

### Error message format

Unknown function errors contain the following information:
- Error type: `ErrorTypeUnknownFunction`
- Error message: Contains the specific unknown function name
- Position information: The position of the function in SQL
- Suggestion: Possible solutions

### Error recovery

Function validation errors are recoverable, and the parser continues to process SQL in other parts, collecting all possible errors.

## Implementation details

### Core Components

1. **FunctionValidator**: The main function validator
   - `ValidateExpression()`: Verify functions in the expression
   - `extractFunctionCalls()`: Extract function calls
   - `isBuiltinFunction()`: Check the built-in functions
   - `isKeyword()`: Filter SQL keywords

2. **Error Type Extension**:
   - `ErrorTypeUnknownFunction`: New error type
   - `CreateUnknownFunctionError()`: Creates an unknown function error
   - `generateFunctionSuggestions()`: Generate suggestions

3. **Parser Integration**:
   - Validate the SELECT field in the `parseSelect()`
   - Verify WHERE conditions in `parseWhere()`
   - Verify HAVING conditions in `parseHaving()`

### Regular Expression Pattern

Function call detection uses regular expressions `([a-zA-Z_][a-zA-Z0-9_]*)\s*\(` to match:
- Identifiers starting with letters or underscores
- Optional whitespace characters followed by the option
- Then the left parenthesis

### Keyword Filtering

The validator filters out SQL keywords to prevent `CASE(.)` or `WHEN(.)` from being mistakenly identified as function calls.

## Configuration options

Currently, function validation is enabled by default and requires no additional configuration. The following configuration options may be added in the future:
- Disable function validation
- Custom validation rules
- Expand the built-in function list

## Performance considerations

- Function verification is performed during the parsing phase and does not affect runtime performance
- Regular expression matching has been optimized for expression length
- Error collection using efficient data structures

## Test coverage

Features include comprehensive testing coverage:
- Unit Test: `function_validator_test.go`
- Integration Testing: `streamsql_validation_test.go`
- Error handling testing: Relevant use cases in `error_test.go`
