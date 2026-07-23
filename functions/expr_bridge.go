package functions

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/rulego/streamsql/utils/cast"
)

// exprLangBuiltinNames are built-in function names in expr-lang/expr that do not conflict with StreamSQL functions.
// Serves as a single source of fact shared by ResolveFunction / IsExprLangFunction / GetFunctionInfo.
// Functions with the same name as StreamSQL, such as concat, are not included (StreamSQL registration takes priority).
var exprLangBuiltinNames = []string{
	"abs", "ceil", "floor", "round", "max", "min",
	"trim", "upper", "lower", "split", "replace", "indexOf", "hasPrefix", "hasSuffix",
	"all", "any", "filter", "map", "find", "count", "flatten",
	"now", "duration", "date",
	"int", "float", "string", "type",
	"toJSON", "fromJSON", "toBase64", "fromBase64",
	"len", "get",
}

// ExprBridge bridges StreamSQL function system with expr-lang/expr
type ExprBridge struct {
	streamSQLFunctions map[string]Function
	// programCache caches compiled expr-lang programs keyed by expression source
	// (prefixed by the env type), so repeated evaluation of the same expression
	// compiles once and runs many times instead of recompiling per row.
	programCache sync.Map
	// preprocessCache caches the deterministic backtick/LIKE/IS-NULL
	// preprocessing result per input expression, avoiding repeated
	// ToUpper/Contains/regex scans on every row.
	preprocessCache sync.Map
	exprEnv         map[string]any
	mutex           sync.RWMutex // Add read-write lock to protect concurrent access
}

// NewExprBridge creates new expression bridge
func NewExprBridge() *ExprBridge {
	return &ExprBridge{
		streamSQLFunctions: make(map[string]Function), // Initialize as empty, dynamically get
		exprEnv:            make(map[string]any),
	}
}

// RegisterStreamSQLFunctionsToExpr registers StreamSQL functions to expr environment
func (bridge *ExprBridge) RegisterStreamSQLFunctionsToExpr() []expr.Option {
	bridge.mutex.Lock()
	defer bridge.mutex.Unlock()

	options := make([]expr.Option, 0)

	// Dynamically get all currently registered functions
	allFunctions := ListAll()

	// Register all StreamSQL functions to expr environment
	for name, fn := range allFunctions {
		// To avoid closure issues, use immediately executed function
		function := fn

		wrappedFunc := func(function Function) func(params ...any) (any, error) {
			return func(params ...any) (any, error) {
				ctx := &FunctionContext{
					Data: bridge.exprEnv,
				}
				return function.Execute(ctx, params)
			}
		}(function)

		// Add function to expr environment
		bridge.exprEnv[name] = wrappedFunc
		bridge.exprEnv[strings.ToUpper(name)] = wrappedFunc

		// Register function type information for both lowercase and uppercase
		options = append(options, expr.Function(
			name,
			wrappedFunc,
		))
		options = append(options, expr.Function(
			strings.ToUpper(name),
			wrappedFunc,
		))
	}

	return options
}

// CreateEnhancedExprEnvironment creates enhanced expr execution environment
func (bridge *ExprBridge) CreateEnhancedExprEnvironment(data map[string]any) map[string]any {
	bridge.mutex.RLock()
	defer bridge.mutex.RUnlock()

	// Merge data and function environment
	env := make(map[string]any)

	// Add user data
	for k, v := range data {
		env[k] = v
	}

	// Dynamically get all currently registered functions
	allFunctions := ListAll()

	// Add all StreamSQL functions
	for name, fn := range allFunctions {
		// Ensure closure captures correct function instance
		function := fn

		wrappedFunc := func(function Function) func(params ...any) (any, error) {
			return func(params ...any) (any, error) {
				ctx := &FunctionContext{
					Data: data, // Use current data context
				}
				return function.Execute(ctx, params)
			}
		}(function)

		// Register lowercase version
		env[name] = wrappedFunc
		// Register uppercase version
		env[strings.ToUpper(name)] = wrappedFunc
	}

	// Add some convenient math function aliases to avoid conflicts with built-ins
	env["streamsql_abs"] = env["abs"]
	env["streamsql_sqrt"] = env["sqrt"]
	env["streamsql_min"] = env["min"]
	env["streamsql_max"] = env["max"]

	// Add custom LIKE matching function
	env["like_match"] = func(text, pattern string) bool {
		return bridge.matchesLikePattern(text, pattern)
	}

	return env
}

// preprocessCached applies the deterministic backtick / LIKE / IS NULL
// preprocessing, memoized per input expression. These transforms depend only
// on the expression text, so caching avoids repeated ToUpper/Contains/regex
// scans on every row. The data-dependent string-concat check is NOT cached and
// stays in EvaluateExpression.
func (bridge *ExprBridge) preprocessCached(expression string) string {
	if v, ok := bridge.preprocessCache.Load(expression); ok {
		return v.(string)
	}
	result := expression
	if bridge.ContainsBacktickIdentifiers(result) {
		if processed, err := bridge.PreprocessBacktickIdentifiers(result); err == nil {
			result = processed
		}
	}
	if bridge.ContainsLikeOperator(result) {
		if processed, err := bridge.PreprocessLikeExpression(result); err == nil {
			result = processed
		}
	}
	if bridge.ContainsIsNullOperator(result) {
		if processed, err := bridge.PreprocessIsNullExpression(result); err == nil {
			result = processed
		}
	}
	bridge.preprocessCache.Store(expression, result)
	return result
}

// CompileExpressionWithStreamSQLFunctions compiled expression, including the StreamSQL function
func (bridge *ExprBridge) CompileExpressionWithStreamSQLFunctions(expression string, dataType any) (*vm.Program, error) {
	// Cache compiled programs by expression source. A program is reusable while
	// the env type is unchanged, so the entry stores the reflect.Type it was
	// compiled against and recompiles only if a different type appears. Keying
	// purely on the expression string (already in hand) avoids a per-call
	// fmt.Sprintf allocation on the hot path; reflect.TypeOf is allocation-free.
	dt := reflect.TypeOf(dataType)
	if cached, ok := bridge.programCache.Load(expression); ok {
		if e, ok := cached.(*progCacheEntry); ok && e.typ == dt {
			return e.prog, nil
		}
	}

	options := []expr.Option{
		expr.Env(dataType),
	}

	// Add the StreamSQL function
	streamSQLOptions := bridge.RegisterStreamSQLFunctionsToExpr()
	options = append(options, streamSQLOptions...)

	// Add custom functions related to LIKE (only like_match; others are built-in operators)
	options = append(options,
		expr.Function("like_match", func(params ...any) (any, error) {
			if len(params) != 2 {
				return false, fmt.Errorf("like_match function requires 2 parameters")
			}
			text, ok1 := params[0].(string)
			pattern, ok2 := params[1].(string)
			if !ok1 || !ok2 {
				return false, fmt.Errorf("like_match function requires string parameters")
			}
			return bridge.matchesLikePattern(text, pattern), nil
		}),
	)

	// Enable some useful expr features
	options = append(options,
		expr.AllowUndefinedVariables(), // Allows undefined variables
		// Remove expr.AsBool() to allow returns of values of any type
	)

	program, err := expr.Compile(expression, options...)
	if err != nil {
		return nil, err
	}
	bridge.programCache.Store(expression, &progCacheEntry{typ: dt, prog: program})
	return program, nil
}

// progCacheEntry pairs a compiled program with the env type it was compiled
// against, so the program is reused only when the env type matches.
type progCacheEntry struct {
	typ  reflect.Type
	prog *vm.Program
}

// EvaluateExpression evaluates expressions and automatically selects the most suitable engine
func (bridge *ExprBridge) EvaluateExpression(expression string, data map[string]any) (any, error) {
	// Preprocessing (backquotes / LIKE / IS NULL): relies solely on expression text, cached by input expression.
	expression = bridge.preprocessCached(expression)

	// Check whether string concatenation mode is included
	if bridge.isStringConcatenationExpression(expression, data) {
		result, err := bridge.evaluateStringConcatenation(expression, data)
		if err == nil {
			return result, nil
		}
	}

	// expr() evaluates dynamic subexpressions for the current row during runtime. The compilation path bakes the StreamSQL function into the compilation path
	// program, the closure's ctx.Data only carries function wrappers and does not include row data, so expr() must be used
	// env path (whose closure captures the real data). The remaining expressions follow a fast compilation path.
	if !bridge.usesExprFunction(expression) {
		program, err := bridge.CompileExpressionWithStreamSQLFunctions(expression, data)
		if err == nil {
			// Functions are compiled into the program via expr.Function, so the
			// runtime env only needs the data. expr-lang evaluation is read-only
			// (no assignment), so the caller's data map is passed directly instead
			// of being copied per call. Expressions that still need env-level
			// functions (e.g. streamsql_* aliases) fall through to expr.Eval below.
			result, err := expr.Run(program, data)
			if err == nil {
				return result, nil
			}
		}
	}

	// env path: The correct path to expr(), and also the fallback when compilation fails.
	env := bridge.CreateEnhancedExprEnvironment(data)
	result, err := expr.Eval(expression, env)
	if err != nil {
		// Check whether it is a function call; if so, do not revert to numeric expression processing
		if bridge.isFunctionCall(expression) {
			return nil, fmt.Errorf("failed to evaluate function call '%s': %v", expression, err)
		}
		// If expr fails, it falls back to a custom expr system (only numerical calculations).
		return bridge.fallbackToCustomExpr(expression, data)
	}

	return result, nil
}

// exprCallPattern matches a call to the expr() function (case-insensitive),
// allowing optional whitespace before the opening parenthesis. The leading word
// boundary prevents matching identifiers like "myexpr(".
var exprCallPattern = regexp.MustCompile(`(?i)\bexpr\s*\(`)

// usesExprFunction reports whether the expression invokes expr(), the only
// StreamSQL function that reads the per-row data context. Such expressions must
// take the env path so the dynamic sub-expression is evaluated against the row.
func (bridge *ExprBridge) usesExprFunction(expression string) bool {
	return exprCallPattern.MatchString(expression)
}

// isStringConcatenationExpression checks whether it is a string concatenation expression
func (bridge *ExprBridge) isStringConcatenationExpression(expression string, data map[string]any) bool {
	// If the expression contains the + operator
	if !strings.Contains(expression, "+") {
		return false
	}

	// Analyze operands in the expression
	parts := strings.Split(expression, "+")
	for _, part := range parts {
		part = strings.TrimSpace(part)

		// If string literals are included (enclosed in quotes)
		if (strings.HasPrefix(part, "'") && strings.HasSuffix(part, "'")) ||
			(strings.HasPrefix(part, "\"") && strings.HasSuffix(part, "\"")) ||
			part == "_" {
			return true
		}

		// If it is a field reference, check whether the field value is a string
		if value, exists := data[part]; exists {
			if _, isString := value.(string); isString {
				return true
			}
		}
	}

	return false
}

// fallbackToCustomExpr falls back to the custom expression system
func (bridge *ExprBridge) fallbackToCustomExpr(expression string, data map[string]any) (any, error) {
	// Try handling string concatenation expressions
	result, err := bridge.evaluateStringConcatenation(expression, data)
	if err == nil {
		return result, nil
	}

	// If it's not string concatenation, try simple numeric expressions
	numResult, err := bridge.evaluateSimpleNumericExpression(expression, data)
	if err == nil {
		return numResult, nil
	}

	return nil, fmt.Errorf("unable to evaluate expression: %s, string concat error: %v, numeric error: %v", expression, err, err)
}

// evaluateStringConcatenation handles string concatenation expressions
func (bridge *ExprBridge) evaluateStringConcatenation(expression string, data map[string]any) (any, error) {
	// Check if it is a string concatenation expression (including + and string literals)
	if !strings.Contains(expression, "+") {
		return nil, fmt.Errorf("not a concatenation expression")
	}

	// A simple string concatenation parser
	// Supported formats: field1 + 'literal' + field2 or field1 + "_" + field2
	parts := strings.Split(expression, "+")
	var result strings.Builder

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Handling string literals (enclosed in single quotes)
		if strings.HasPrefix(part, "'") && strings.HasSuffix(part, "'") {
			literal := strings.Trim(part, "'")
			result.WriteString(literal)
		} else if strings.HasPrefix(part, "\"") && strings.HasSuffix(part, "\"") {
			literal := strings.Trim(part, "\"")
			result.WriteString(literal)
		} else if part == "_" {
			// Handle underlined literal quantities
			result.WriteString("_")
		} else {
			// Processing field references
			if value, exists := data[part]; exists {
				strValue := cast.ToString(value)
				result.WriteString(strValue)
			} else {
				return nil, fmt.Errorf("field %s not found in data", part)
			}
		}
	}

	return result.String(), nil
}

// evaluateSimpleNumericExpression handles simple numerical expressions
func (bridge *ExprBridge) evaluateSimpleNumericExpression(expression string, data map[string]any) (any, error) {
	expression = strings.TrimSpace(expression)

	// Handle simple field references
	if value, exists := data[expression]; exists {
		return value, nil
	}

	// Handle numerical literal quantities
	if num, err := strconv.ParseFloat(expression, 64); err == nil {
		return num, nil
	}

	// Handling simple mathematical operations (e.g., field * 2, field + 5)
	for _, op := range []string{"+", "-", "*", "/"} {
		if strings.Contains(expression, op) {
			parts := strings.Split(expression, op)
			if len(parts) == 2 {
				left := strings.TrimSpace(parts[0])
				right := strings.TrimSpace(parts[1])

				// Obtain the lvalue
				var leftVal float64
				if val, exists := data[left]; exists {
					if f, err := bridge.toFloat64(val); err == nil {
						leftVal = f
					} else {
						return nil, fmt.Errorf("cannot convert left operand to number: %v", val)
					}
				} else if f, err := strconv.ParseFloat(left, 64); err == nil {
					leftVal = f
				} else {
					continue // Try the next operator
				}

				// Get an rvalue
				var rightVal float64
				if val, exists := data[right]; exists {
					if f, err := bridge.toFloat64(val); err == nil {
						rightVal = f
					} else {
						return nil, fmt.Errorf("cannot convert right operand to number: %v", val)
					}
				} else if f, err := strconv.ParseFloat(right, 64); err == nil {
					rightVal = f
				} else {
					continue // Try the next operator
				}

				// Perform the calculation
				switch op {
				case "+":
					return leftVal + rightVal, nil
				case "-":
					return leftVal - rightVal, nil
				case "*":
					return leftVal * rightVal, nil
				case "/":
					if rightVal == 0 {
						return nil, fmt.Errorf("division by zero")
					}
					return leftVal / rightVal, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("unsupported expression: %s", expression)
}

// ContainsLikeOperator checks whether the expression contains the LIKE operator
func (bridge *ExprBridge) ContainsLikeOperator(expression string) bool {
	// A simple check to see if the LIKE keyword is included
	upperExpr := strings.ToUpper(expression)
	return strings.Contains(upperExpr, " LIKE ")
}

// ContainsIsNullOperator checks whether the expression contains the IS NULL or IS NOT NULL operator
func (bridge *ExprBridge) ContainsIsNullOperator(expression string) bool {
	upperExpr := strings.ToUpper(expression)
	return strings.Contains(upperExpr, " IS NULL") || strings.Contains(upperExpr, " IS NOT NULL")
}

// isFunctionCall checks whether the expression is a function call
func (bridge *ExprBridge) isFunctionCall(expression string) bool {
	// If it is a CASE expression, it is not a function call
	trimmed := strings.TrimSpace(expression)
	upperTrimmed := strings.ToUpper(trimmed)
	if strings.HasPrefix(upperTrimmed, "CASE ") || strings.HasPrefix(upperTrimmed, "CASE\t") || strings.HasPrefix(upperTrimmed, "CASE\n") {
		return false
	}

	// Check if it matches the simple function call pattern: function_name(args)
	// Function calls should start with an identifier, followed by parentheses
	if !strings.Contains(expression, "(") || !strings.Contains(expression, ")") {
		return false
	}

	// Check if it starts with an identifier (function name)
	for i, r := range trimmed {
		if i == 0 {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_') {
				return false
			}
		} else if r == '(' {
			// Finding the open parentheses means this may be a function call
			return true
		} else if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			// If you encounter a non-identifier character that is not in open parentheses, it means it is not a simple function call
			return false
		}
	}

	return false
}

// PreprocessLikeExpression: Preprocesses the LIKE expression, converting it into a function call understandable by expr-lang
func (bridge *ExprBridge) PreprocessLikeExpression(expression string) (string, error) {
	// Use regular expressions to match the LIKE pattern
	// Match: `field` LIKE 'pattern' or 'field' LIKE 'pattern' (allow null mode)
	// Supports backquote identifiers and standard identifiers
	likePattern := `((?:` + "`" + `[^` + "`" + `]+` + "`" + `|\w+)(?:\.(?:` + "`" + `[^` + "`" + `]+` + "`" + `|\w+))*)\s+LIKE\s+'([^']*)'`
	re, err := regexp.Compile(likePattern)
	if err != nil {
		return expression, err
	}

	// Replace all LIKE expressions
	result := re.ReplaceAllStringFunc(expression, func(match string) string {
		submatches := re.FindStringSubmatch(match)
		if len(submatches) != 3 {
			return match // Keep it as is
		}

		field := submatches[1]
		pattern := submatches[2]

		// Handle backquote identifiers and remove backquotes
		if len(field) >= 2 && field[0] == '`' && field[len(field)-1] == '`' {
			field = field[1 : len(field)-1] // Remove the quotation marks
		}

		// Convert the LIKE pattern into the corresponding function call
		return bridge.convertLikeToFunction(field, pattern)
	})

	return result, nil
}

// PreprocessIsNullExpression preprocesses IS NULL and IS NOT NULL expressions, converting them into expressions understandable by expr-lang
func (bridge *ExprBridge) PreprocessIsNullExpression(expression string) (string, error) {
	// Matching complex expressions in IS NOT NULL pattern (such as function calls)
	complexNotNullPattern := `([A-Za-z_][A-Za-z0-9_]*\s*\([^)]*\))\s+IS\s+NOT\s+NULL`
	reComplexNotNull, err := regexp.Compile(complexNotNullPattern)
	if err != nil {
		return expression, err
	}

	// First, handle the complex expression IS NOT NULL
	result := reComplexNotNull.ReplaceAllString(expression, "is_not_null($1)")

	// Matches the IS NULL pattern for complex expressions
	complexNullPattern := `([A-Za-z_][A-Za-z0-9_]*\s*\([^)]*\))\s+IS\s+NULL`
	reComplexNull, err := regexp.Compile(complexNullPattern)
	if err != nil {
		return result, err
	}

	// Handles complex expressions with IS NULL
	result = reComplexNull.ReplaceAllString(result, "is_null($1)")

	// Matching simple fields in IS NOT NULL pattern (must be processed after complex expressions)
	isNotNullPattern := `(\w+(?:\.\w+)*)\s+IS\s+NOT\s+NULL`
	reNotNull, err := regexp.Compile(isNotNullPattern)
	if err != nil {
		return result, err
	}

	// Replace the simple field IS NOT NULL
	result = reNotNull.ReplaceAllString(result, "$1 != nil")

	// Matches the IS NULL pattern for simple fields
	isNullPattern := `(\w+(?:\.\w+)*)\s+IS\s+NULL`
	reNull, err := regexp.Compile(isNullPattern)
	if err != nil {
		return result, err
	}

	// Then replace the simple field IS NULL
	result = reNull.ReplaceAllString(result, "$1 == nil")

	return result, nil
}

// ContainsBacktickIdentifiers Check if the expression contains backtick identifiers
func (bridge *ExprBridge) ContainsBacktickIdentifiers(expression string) bool {
	return strings.Contains(expression, "`")
}

// PreprocessBacktickIdentifiers Preprocesses backquote identifiers and removes backquotes
func (bridge *ExprBridge) PreprocessBacktickIdentifiers(expression string) (string, error) {
	// Use regular expressions to match backquote identifiers
	// Match: `identifier` or `nested.field`
	backtickPattern := "`([^`]+)`"
	re, err := regexp.Compile(backtickPattern)
	if err != nil {
		return expression, err
	}

	// Replace all backquote identifiers and remove backquotes
	result := re.ReplaceAllString(expression, "$1")
	return result, nil
}

// convertLikeToFunction converts LIKE mode to the expr-lang operator
func (bridge *ExprBridge) convertLikeToFunction(field, pattern string) string {
	// Handle the null mode
	if pattern == "" {
		return fmt.Sprintf("%s == ''", field)
	}

	// Patterns containing _ or internal % must use the full matcher, startsWith/endsWith/contains
	// Fastpaths treat these wildcards as literals.
	core := strings.Trim(pattern, "%")
	if core != "" && (strings.Contains(core, "_") || strings.Contains(core, "%")) {
		return fmt.Sprintf("like_match(%s, '%s')", field, pattern)
	}

	// Analyze the types of patterns
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") && len(pattern) > 1 {
		// %pattern% -> contains operator (but not a standalone %)
		inner := strings.Trim(pattern, "%")
		if inner == "" {
			// %% means matches any string
			return "true"
		}
		return fmt.Sprintf("%s contains '%s'", field, inner)
	} else if strings.HasPrefix(pattern, "%") && len(pattern) > 1 {
		// %pattern -> endsWith operator
		suffix := strings.TrimPrefix(pattern, "%")
		return fmt.Sprintf("%s endsWith '%s'", field, suffix)
	} else if strings.HasSuffix(pattern, "%") && len(pattern) > 1 {
		// pattern% -> startsWith operator
		prefix := strings.TrimSuffix(pattern, "%")
		return fmt.Sprintf("%s startsWith '%s'", field, prefix)
	} else if pattern == "%" {
		// A single % matches any string
		return "true"
	} else if strings.Contains(pattern, "%") || strings.Contains(pattern, "_") {
		// Complex patterns (such as prefix%suffix) or those containing single-character wildcards use custom like_match functions
		return fmt.Sprintf("like_match(%s, '%s')", field, pattern)
	} else {
		// Precise matching
		return fmt.Sprintf("%s == '%s'", field, pattern)
	}
}

// matchesLikePattern to achieve LIKE pattern matching
// Supports % (matches any character sequence) and _ (matches a single character).
// Using the classic double-pointer backtracking algorithm, the worst case is O(n*m), and the adversarial mode does not exponentially inflate.
func (bridge *ExprBridge) matchesLikePattern(text, pattern string) bool {
	ti, pi := 0, 0
	starIdx, matchIdx := -1, 0
	for ti < len(text) {
		if pi < len(pattern) && (pattern[pi] == '_' || pattern[pi] == text[ti]) {
			ti++
			pi++
		} else if pi < len(pattern) && pattern[pi] == '%' {
			starIdx = pi
			matchIdx = ti
			pi++
		} else if starIdx != -1 {
			pi = starIdx + 1
			matchIdx++
			ti = matchIdx
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}
	return pi == len(pattern)
}

// toFloat64 converts the value to float64
func (bridge *ExprBridge) toFloat64(val any) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

// GetFunctionInfo retrieves function information to unify the functions of the two systems
func (bridge *ExprBridge) GetFunctionInfo() map[string]any {
	bridge.mutex.RLock()
	defer bridge.mutex.RUnlock()

	info := make(map[string]any)

	// StreamSQL Function Information - Dynamically retrieves all currently registered functions
	streamSQLFuncs := make(map[string]any)
	allFunctions := ListAll() // Dynamically retrieves all registered functions
	for name, fn := range allFunctions {
		streamSQLFuncs[name] = map[string]any{
			"name":        fn.GetName(),
			"type":        fn.GetType(),
			"category":    fn.GetCategory(),
			"description": fn.GetDescription(),
			"source":      "StreamSQL",
		}
	}
	info["streamsql"] = streamSQLFuncs

	// expr-lang/expr built-in functions (main ones listed)
	exprBuiltins := map[string]any{
		// Mathematical functions
		"abs":   map[string]any{"category": "math", "description": "absolute value", "source": "expr-lang"},
		"ceil":  map[string]any{"category": "math", "description": "ceiling", "source": "expr-lang"},
		"floor": map[string]any{"category": "math", "description": "floor", "source": "expr-lang"},
		"round": map[string]any{"category": "math", "description": "round", "source": "expr-lang"},
		"max":   map[string]any{"category": "math", "description": "maximum", "source": "expr-lang"},
		"min":   map[string]any{"category": "math", "description": "minimum", "source": "expr-lang"},

		// String function
		"trim":      map[string]any{"category": "string", "description": "trim whitespace", "source": "expr-lang"},
		"upper":     map[string]any{"category": "string", "description": "to uppercase", "source": "expr-lang"},
		"lower":     map[string]any{"category": "string", "description": "to lowercase", "source": "expr-lang"},
		"split":     map[string]any{"category": "string", "description": "split string", "source": "expr-lang"},
		"replace":   map[string]any{"category": "string", "description": "replace substring", "source": "expr-lang"},
		"indexOf":   map[string]any{"category": "string", "description": "find index", "source": "expr-lang"},
		"hasPrefix": map[string]any{"category": "string", "description": "check prefix", "source": "expr-lang"},
		"hasSuffix": map[string]any{"category": "string", "description": "check suffix", "source": "expr-lang"},

		// Array/set functions
		"all":     map[string]any{"category": "array", "description": "all elements satisfy", "source": "expr-lang"},
		"any":     map[string]any{"category": "array", "description": "any element satisfies", "source": "expr-lang"},
		"filter":  map[string]any{"category": "array", "description": "filter elements", "source": "expr-lang"},
		"map":     map[string]any{"category": "array", "description": "transform elements", "source": "expr-lang"},
		"find":    map[string]any{"category": "array", "description": "find element", "source": "expr-lang"},
		"count":   map[string]any{"category": "array", "description": "count elements", "source": "expr-lang"},
		"flatten": map[string]any{"category": "array", "description": "flatten array", "source": "expr-lang"},

		// Time function
		"now":      map[string]any{"category": "datetime", "description": "current time", "source": "expr-lang"},
		"duration": map[string]any{"category": "datetime", "description": "parse duration", "source": "expr-lang"},
		"date":     map[string]any{"category": "datetime", "description": "parse date", "source": "expr-lang"},

		// Type conversion
		"int":    map[string]any{"category": "conversion", "description": "to integer", "source": "expr-lang"},
		"float":  map[string]any{"category": "conversion", "description": "to float", "source": "expr-lang"},
		"string": map[string]any{"category": "conversion", "description": "to string", "source": "expr-lang"},
		"type":   map[string]any{"category": "conversion", "description": "get type", "source": "expr-lang"},

		// JSON processing
		"toJSON":   map[string]any{"category": "json", "description": "to JSON", "source": "expr-lang"},
		"fromJSON": map[string]any{"category": "json", "description": "from JSON", "source": "expr-lang"},

		// Base64 encoding
		"toBase64":   map[string]any{"category": "encoding", "description": "to Base64", "source": "expr-lang"},
		"fromBase64": map[string]any{"category": "encoding", "description": "from Base64", "source": "expr-lang"},
	}
	info["expr-lang"] = exprBuiltins

	return info
}

// ResolveFunction parsing function calls, preferably using the StreamSQL function
func (bridge *ExprBridge) ResolveFunction(name string) (any, bool, string) {
	bridge.mutex.RLock()
	defer bridge.mutex.RUnlock()

	// Perform case-insensitive searches
	lowerName := strings.ToLower(name)

	// First, check the StreamSQL function (higher priority) - Dynamic Acquisition
	allFunctions := ListAll()
	if fn, exists := allFunctions[lowerName]; exists {
		return fn, true, "streamsql"
	}

	// Then check whether it is a built-in expr-lang function
	for _, b := range exprLangBuiltinNames {
		if strings.ToLower(b) == lowerName {
			return nil, true, "expr-lang"
		}
	}

	return nil, false, ""
}

// IsExprLangFunction checks whether the function name is an expr-lang built-in function
func (bridge *ExprBridge) IsExprLangFunction(name string) bool {
	for _, b := range exprLangBuiltinNames {
		if b == name {
			return true
		}
	}
	return false
}

// Global Bridge Example
var globalBridge *ExprBridge
var globalBridgeMutex sync.RWMutex

// GetExprBridge retrieves the global bridge instance
func GetExprBridge() *ExprBridge {
	// First, use a lock read to check if initialization has been completed
	globalBridgeMutex.RLock()
	if globalBridge != nil {
		defer globalBridgeMutex.RUnlock()
		return globalBridge
	}
	globalBridgeMutex.RUnlock()

	// Initialization is performed using write locks
	globalBridgeMutex.Lock()
	defer globalBridgeMutex.Unlock()

	// Dual-check mode prevents concurrent initialization
	if globalBridge == nil {
		globalBridge = NewExprBridge()
	}
	return globalBridge
}

// Convenience function: Directly evaluates expressions
func EvaluateWithBridge(expression string, data map[string]any) (any, error) {
	return GetExprBridge().EvaluateExpression(expression, data)
}

// Convenient functions: Get information on all available functions
func GetAllAvailableFunctions() map[string]any {
	return GetExprBridge().GetFunctionInfo()
}
