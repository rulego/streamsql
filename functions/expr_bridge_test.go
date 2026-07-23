package functions

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestExprFunctionGetsRowData is a regression test: the compiled evaluation path
// bakes StreamSQL functions into the program with closures that do not carry the
// per-row data. The expr() function reads the row to evaluate its dynamic
// sub-expression, so it must be routed through the env path (which builds
// data-capturing closures). Before the fix, expr() silently returned nil.
func TestExprFunctionGetsRowData(t *testing.T) {
	bridge := NewExprBridge()
	data := map[string]any{"temperature": 99.0, "humidity": 60.0}

	// Field reference inside expr() must resolve against the row.
	got, err := bridge.EvaluateExpression("expr('temperature')", data)
	assert.NoError(t, err)
	assert.Equal(t, 99.0, got)

	// Arithmetic inside expr() must resolve fields too.
	got, err = bridge.EvaluateExpression("expr('temperature * 2 + humidity')", data)
	assert.NoError(t, err)
	assert.Equal(t, 258.0, got)

	// Mixed: regular function result composed with expr() result is unaffected.
	got, err = bridge.EvaluateExpression("abs(-3) + expr('humidity')", data)
	assert.NoError(t, err)
	assert.Equal(t, 63.0, got)
}

func TestExprBridge(t *testing.T) {
	bridge := NewExprBridge()

	t.Run("StreamSQL Functions Available", func(t *testing.T) {
		// Test whether the StreamSQL function is usable
		data := map[string]any{
			"temperature": 25.5,
			"humidity":    60,
		}

		// Test mathematical functions
		result, err := bridge.EvaluateExpression("abs(-5)", data)
		assert.NoError(t, err)
		assert.Equal(t, float64(5), result)

		// Test string function
		result, err = bridge.EvaluateExpression("length(\"hello\")", data)
		assert.NoError(t, err)
		assert.Equal(t, int(5), result)
	})

	t.Run("Expr-Lang Functions Available", func(t *testing.T) {
		data := map[string]any{
			"numbers": []int{1, 2, 3, 4, 5},
			"text":    "Hello World",
		}

		// Test the expr-lang array function
		result, err := bridge.EvaluateExpression("len(numbers)", data)
		assert.NoError(t, err)
		assert.Equal(t, 5, result)

		// Test the expr-lang string function
		result, err = bridge.EvaluateExpression("trim(\"  hello  \")", data)
		assert.NoError(t, err)
		assert.Equal(t, "hello", result)
	})

	t.Run("Mixed Functions", func(t *testing.T) {
		data := map[string]any{
			"values": []float64{-3.5, 2.1, -1.8, 4.2},
		}

		// Use StreamSQL's abs function and expr-lang's filter function
		// Note: This test may need to be adjusted based on actual implementation
		env := bridge.CreateEnhancedExprEnvironment(data)

		// The verification environment contains all expected functions
		assert.Contains(t, env, "abs")
		assert.Contains(t, env, "length")
		assert.Contains(t, env, "values")
	})

	t.Run("Function Resolution", func(t *testing.T) {
		// Test function parsing priority
		_, exists, source := bridge.ResolveFunction("abs")
		assert.True(t, exists)
		assert.Equal(t, "streamsql", source) // StreamSQL is the priority

		_, exists, source = bridge.ResolveFunction("encode")
		assert.True(t, exists)
		assert.Equal(t, "streamsql", source) // Unique to StreamSQL

		_, exists, _ = bridge.ResolveFunction("nonexistent")
		assert.False(t, exists)
	})

	t.Run("Function Information", func(t *testing.T) {
		info := bridge.GetFunctionInfo()

		// Verify that information about the StreamSQL function is included
		streamSQLFuncs, ok := info["streamsql"].(map[string]any)
		assert.True(t, ok)
		assert.Contains(t, streamSQLFuncs, "abs")
		assert.Contains(t, streamSQLFuncs, "encode")

		// Verify that the information containing the expr-lang function is included
		exprLangFuncs, ok := info["expr-lang"].(map[string]any)
		assert.True(t, ok)
		assert.Contains(t, exprLangFuncs, "trim")
		assert.Contains(t, exprLangFuncs, "filter")
	})
}

func TestEvaluateWithBridge(t *testing.T) {
	data := map[string]any{
		"x": 3.5,
		"y": -2.1,
	}

	// Test simple expressions
	result, err := EvaluateWithBridge("abs(y)", data)
	assert.NoError(t, err)
	assert.Equal(t, 2.1, result)

	// Test the composite expression
	result, err = EvaluateWithBridge("x + abs(y)", data)
	assert.NoError(t, err)
	assert.Equal(t, 5.6, result)
}

func TestGetAllAvailableFunctions(t *testing.T) {
	info := GetAllAvailableFunctions()

	// Verify the returned information structure
	assert.Contains(t, info, "streamsql")
	assert.Contains(t, info, "expr-lang")

	// The number of verification functions is reasonable
	streamSQLFuncs := info["streamsql"].(map[string]any)
	t.Logf("StreamSQL functions count: %d", len(streamSQLFuncs))
	// for name := range streamSQLFuncs {
	// 	t.Logf("StreamSQL function: %s", name)
	// }
	assert.GreaterOrEqual(t, len(streamSQLFuncs), 1) // At the very least, there should be a function

	exprLangFuncs := info["expr-lang"].(map[string]any)
	t.Logf("Expr-lang functions count: %d", len(exprLangFuncs))
	assert.GreaterOrEqual(t, len(exprLangFuncs), 1) // At the very least, there should be a function
}

func TestFunctionConflictResolution(t *testing.T) {
	bridge := NewExprBridge()
	data := map[string]any{
		"value": -5.5,
	}

	// Testing the parsing of the collision function (the abs function exists in both systems)
	// The expr-lang version should be prioritized
	env := bridge.CreateEnhancedExprEnvironment(data)

	// Verify that the StreamSQL function can be accessed with an alias
	assert.Contains(t, env, "streamsql_abs")
	assert.Contains(t, env, "abs")

	// Both versions worked properly
	result, err := bridge.EvaluateExpression("abs(value)", data)
	assert.NoError(t, err)
	assert.Equal(t, 5.5, result)
}

func TestExprBridgeAdvancedFunctions(t *testing.T) {
	bridge := NewExprBridge()

	t.Run("String Concatenation Detection", func(t *testing.T) {
		data := map[string]any{
			"name": "John",
			"age":  25,
		}

		// Test string concatenation expression detection
		isConcat := bridge.isStringConcatenationExpression("name + ' is ' + age", data)
		assert.True(t, isConcat)

		isConcat = bridge.isStringConcatenationExpression("abs(-5)", data)
		assert.False(t, isConcat)
	})

	t.Run("Fallback to Custom Expression", func(t *testing.T) {
		data := map[string]any{
			"text": "hello",
		}

		// Test backward to custom expression processing
		result, err := bridge.fallbackToCustomExpr("text + ' world'", data)
		assert.NoError(t, err)
		assert.Equal(t, "hello world", result)
	})

	t.Run("String Concatenation Evaluation", func(t *testing.T) {
		data := map[string]any{
			"first":  "Hello",
			"second": "World",
		}

		// Test string connection evaluation
		result, err := bridge.evaluateStringConcatenation("first + ' ' + second", data)
		assert.NoError(t, err)
		assert.Equal(t, "Hello World", result)
	})

	t.Run("Simple Numeric Expression", func(t *testing.T) {
		data := map[string]any{
			"x": 10,
			"y": 5,
		}

		// Test simple numerical expressions
		result, err := bridge.evaluateSimpleNumericExpression("x + y", data)
		assert.NoError(t, err)
		assert.Equal(t, float64(15), result)
	})

	t.Run("Function Call Detection", func(t *testing.T) {
		// Test function call detection
		assert.True(t, bridge.isFunctionCall("abs(-5)"))
		assert.True(t, bridge.isFunctionCall("length('hello')"))
		assert.False(t, bridge.isFunctionCall("x + y"))
		assert.False(t, bridge.isFunctionCall("simple_variable"))
	})

	t.Run("Like Expression Preprocessing", func(t *testing.T) {
		// Test LIKE expression preprocessing
		processed, err := bridge.PreprocessLikeExpression("name LIKE '%john%'")
		assert.NoError(t, err)
		assert.Contains(t, processed, "contains")
	})

	t.Run("IsNull Expression Preprocessing", func(t *testing.T) {
		// Test the IS NULL expression preprocessing
		processed, err := bridge.PreprocessIsNullExpression("field IS NULL")
		assert.NoError(t, err)
		assert.Contains(t, processed, "== nil")
	})

	t.Run("Backtick Identifiers", func(t *testing.T) {
		// Test backquote identifier detection
		assert.True(t, bridge.ContainsBacktickIdentifiers("`field_name` = 1"))
		assert.False(t, bridge.ContainsBacktickIdentifiers("field_name = 1"))

		// Test backquote identifier preprocessing
		processed, err := bridge.PreprocessBacktickIdentifiers("`field_name` = 1")
		assert.NoError(t, err)
		assert.Contains(t, processed, "field_name")
	})

	t.Run("Like Pattern Matching", func(t *testing.T) {
		// Test LIKE pattern matching
		assert.True(t, bridge.matchesLikePattern("hello", "h%"))
		assert.True(t, bridge.matchesLikePattern("world", "%d"))
		assert.False(t, bridge.matchesLikePattern("hello", "x%"))

		// Test LIKE matching
		assert.True(t, bridge.matchesLikePattern("hello", "h%o"))
		assert.False(t, bridge.matchesLikePattern("hello", "x%"))
	})

	t.Run("Type Conversion", func(t *testing.T) {
		// Test type conversion
		result, err := bridge.toFloat64(10)
		assert.NoError(t, err)
		assert.Equal(t, float64(10), result)

		result, err = bridge.toFloat64("10.5")
		assert.NoError(t, err)
		assert.Equal(t, float64(10.5), result)

		_, err = bridge.toFloat64("invalid")
		assert.Error(t, err)
	})

	t.Run("Expr Lang Function Detection", func(t *testing.T) {
		// Test expr-lang function detection
		assert.True(t, bridge.IsExprLangFunction("trim"))
		assert.True(t, bridge.IsExprLangFunction("len"))
		assert.True(t, bridge.IsExprLangFunction("abs"))
		assert.False(t, bridge.IsExprLangFunction("nonexistent"))
	})

	t.Run("Like to Function Conversion", func(t *testing.T) {
		// Test LIKE is converted into a function call
		result := bridge.convertLikeToFunction("name", "%john%")
		assert.Contains(t, result, "contains")
		assert.Contains(t, result, "name")
		assert.Contains(t, result, "john")
	})
}

// TestConvertLikeUnderscoreRoute verifies that LIKE patterns containing _ or
// an interior % route to like_match, instead of the startsWith/endsWith/
// contains fast paths that treat those wildcards as literals (H13).
func TestConvertLikeUnderscoreRoute(t *testing.T) {
	bridge := NewExprBridge()

	underscoreCases := []string{"a_b%", "%a_b", "%a_b%", "a_b", "%a%b%"}
	for _, p := range underscoreCases {
		got := bridge.convertLikeToFunction("name", p)
		assert.Contains(t, got, "like_match", "pattern %q should route to like_match", p)
	}

	// Patterns without _ or interior % still use fast paths.
	assert.Contains(t, bridge.convertLikeToFunction("name", "abc%"), "startsWith")
	assert.Contains(t, bridge.convertLikeToFunction("name", "%abc"), "endsWith")
	assert.Contains(t, bridge.convertLikeToFunction("name", "%abc%"), "contains")
}

// TestExprBridgeComplexExpressions tests complex expression handling
func TestExprBridgeComplexExpressions(t *testing.T) {
	bridge := NewExprBridge()

	tests := []struct {
		name       string
		expression string
		data       map[string]any
		expected   any
		wantErr    bool
	}{
		{
			name:       "math_and_string",
			expression: "length('test')",
			data:       map[string]any{},
			expected:   4,
			wantErr:    false,
		},
		{
			name:       "nested_function_calls",
			expression: "abs(sqrt(16) - 5)",
			data:       map[string]any{},
			expected:   float64(1),
			wantErr:    false,
		},
		{
			name:       "array_operations",
			expression: "array_length([1, 2, 3, 4])",
			data:       map[string]any{},
			expected:   4,
			wantErr:    false,
		},
		{
			name:       "string_with_variables",
			expression: "upper(name)",
			data:       map[string]any{"name": "john"},
			expected:   "JOHN",
			wantErr:    false,
		},
		{
			name:       "conditional_expression",
			expression: "age > 18 ? 'adult' : 'minor'",
			data:       map[string]any{"age": 25},
			expected:   "adult",
			wantErr:    false,
		},
		{
			name:       "complex_math",
			expression: "power(2, 3) + mod(10, 3)",
			data:       map[string]any{},
			expected:   float64(9),
			wantErr:    false,
		},
		{
			name:       "array_contains_check",
			expression: "array_contains([1, 2, 3], 2)",
			data:       map[string]any{},
			expected:   true,
			wantErr:    false,
		},
		{
			name:       "string_concatenation",
			expression: "concat(first_name, ' ', last_name)",
			data:       map[string]any{"first_name": "John", "last_name": "Doe"},
			expected:   "John Doe",
			wantErr:    false,
		},
		{
			name:       "invalid_function",
			expression: "nonexistent_function(1)",
			data:       map[string]any{},
			expected:   nil,
			wantErr:    true,
		},
		{
			name:       "invalid_syntax",
			expression: "length(",
			data:       map[string]any{},
			expected:   nil,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := bridge.EvaluateExpression(tt.expression, tt.data)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
