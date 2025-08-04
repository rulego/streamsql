package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/fieldpath"
)

// Expression types
const (
	TypeNumber      = "number"      // Number constant
	TypeField       = "field"       // Field reference
	TypeOperator    = "operator"    // Operator
	TypeFunction    = "function"    // Function call
	TypeParenthesis = "parenthesis" // Parenthesis
	TypeCase        = "case"        // CASE expression
	TypeString      = "string"      // String constant
)

// Operator precedence
var operatorPrecedence = map[string]int{
	"OR":  1,
	"AND": 2,
	"==":  3, "=": 3, "!=": 3, "<>": 3,
	">": 4, "<": 4, ">=": 4, "<=": 4, "LIKE": 4, "IS": 4,
	"+": 5, "-": 5,
	"*": 6, "/": 6, "%": 6,
	"^": 7, // Power operation
}

// WhenClause represents a WHEN clause in CASE expression
type WhenClause struct {
	Condition *ExprNode // WHEN condition
	Result    *ExprNode // THEN result
}

// ExprNode represents an expression node
type ExprNode struct {
	Type  string
	Value string
	Left  *ExprNode
	Right *ExprNode
	Args  []*ExprNode // Arguments for function calls

	// Fields specific to CASE expressions
	CaseExpr    *ExprNode    // Expression after CASE (simple CASE)
	WhenClauses []WhenClause // List of WHEN clauses
	ElseExpr    *ExprNode    // ELSE expression
}

// Expression represents a computable expression
type Expression struct {
	Root               *ExprNode
	useExprLang        bool   // Whether to use expr-lang/expr
	exprLangExpression string // expr-lang expression string
}

// NewExpression creates a new expression
func NewExpression(exprStr string) (*Expression, error) {
	// Perform basic syntax validation
	if err := validateBasicSyntax(exprStr); err != nil {
		return nil, err
	}

	// First try using custom parser
	tokens, err := tokenize(exprStr)
	if err != nil {
		// If custom parsing fails, mark to use expr-lang
		return &Expression{
			Root:               nil,
			useExprLang:        true,
			exprLangExpression: exprStr,
		}, nil
	}

	root, err := parseExpression(tokens)
	if err != nil {
		// If custom parsing fails, mark to use expr-lang
		return &Expression{
			Root:               nil,
			useExprLang:        true,
			exprLangExpression: exprStr,
		}, nil
	}

	return &Expression{
		Root:        root,
		useExprLang: false,
	}, nil
}

// validateBasicSyntax performs basic syntax validation
func validateBasicSyntax(exprStr string) error {
	// Check empty expression
	trimmed := strings.TrimSpace(exprStr)
	if trimmed == "" {
		return fmt.Errorf("empty expression")
	}

	// 检查不匹配的括号
	parenthesesCount := 0
	for _, ch := range trimmed {
		if ch == '(' {
			parenthesesCount++
		} else if ch == ')' {
			parenthesesCount--
			if parenthesesCount < 0 {
				return fmt.Errorf("mismatched parentheses")
			}
		}
	}
	if parenthesesCount != 0 {
		return fmt.Errorf("mismatched parentheses")
	}

	// 检查无效字符
	for i, ch := range trimmed {
		// 允许的字符：字母、数字、运算符、括号、点、下划线、空格、引号
		if !isValidChar(ch) {
			return fmt.Errorf("invalid character '%c' at position %d", ch, i)
		}
	}

	// 检查连续运算符
	if err := checkConsecutiveOperators(trimmed); err != nil {
		return err
	}

	return nil
}

// checkConsecutiveOperators checks for consecutive operators
func checkConsecutiveOperators(expr string) error {
	// Simplified consecutive operator check: look for obvious double operator patterns
	// But allow comparison operators followed by negative numbers
	operators := []string{"+", "-", "*", "/", "%", "^", "==", "!=", ">=", "<=", ">", "<"}
	comparisonOps := []string{"==", "!=", ">=", "<=", ">", "<"}

	for i := 0; i < len(expr)-1; i++ {
		// 跳过空白字符
		if expr[i] == ' ' || expr[i] == '\t' {
			continue
		}

		// 检查当前位置是否是运算符
		isCurrentOp := false
		currentOpLen := 0
		currentOp := ""
		for _, op := range operators {
			if i+len(op) <= len(expr) && expr[i:i+len(op)] == op {
				isCurrentOp = true
				currentOpLen = len(op)
				currentOp = op
				break
			}
		}

		if isCurrentOp {
			// 查找下一个非空白字符
			nextPos := i + currentOpLen
			for nextPos < len(expr) && (expr[nextPos] == ' ' || expr[nextPos] == '\t') {
				nextPos++
			}

			// 检查下一个字符是否也是运算符
			if nextPos < len(expr) {
				// 特殊处理：如果当前是比较运算符，下一个是负号，且负号后跟数字，则允许
				isCurrentComparison := false
				for _, compOp := range comparisonOps {
					if currentOp == compOp {
						isCurrentComparison = true
						break
					}
				}

				// 检查是否是负数的情况
				if isCurrentComparison && nextPos < len(expr) && expr[nextPos] == '-' {
					// 检查负号后是否跟数字
					digitPos := nextPos + 1
					for digitPos < len(expr) && (expr[digitPos] == ' ' || expr[digitPos] == '\t') {
						digitPos++
					}
					if digitPos < len(expr) && expr[digitPos] >= '0' && expr[digitPos] <= '9' {
						// 这是比较运算符后跟负数，允许通过
						i = nextPos // 跳过到负号位置
						continue
					}
				}

				// 检查其他连续运算符
				for _, op := range operators {
					if nextPos+len(op) <= len(expr) && expr[nextPos:nextPos+len(op)] == op {
						// 如果不是允许的负数情况，则报错
						return fmt.Errorf("consecutive operators found: '%s' followed by '%s'",
							currentOp, op)
					}
				}
			}

			// 跳过当前运算符
			i += currentOpLen - 1
		}
	}

	return nil
}

// isValidChar checks if a character is valid
func isValidChar(ch rune) bool {
	// Letters and digits
	if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
		return true
	}
	// Special characters
	switch ch {
	case ' ', '\t', '\n', '\r': // Whitespace characters
		return true
	case '+', '-', '*', '/', '%', '^': // Arithmetic operators
		return true
	case '(', ')', ',': // Parentheses and comma
		return true
	case '>', '<', '=', '!': // Comparison operators
		return true
	case '\'', '"': // Quotes
		return true
	case '.', '_': // Dot and underscore
		return true
	case '$': // Dollar sign (for JSON paths etc.)
		return true
	default:
		return false
	}
}

// Evaluate calculates the value of the expression
func (e *Expression) Evaluate(data map[string]interface{}) (float64, error) {
	if e.useExprLang {
		return e.evaluateWithExprLang(data)
	}
	return evaluateNode(e.Root, data)
}

// evaluateWithExprLang evaluates expression using expr-lang/expr
func (e *Expression) evaluateWithExprLang(data map[string]interface{}) (float64, error) {
	// Use bridge to evaluate expression
	bridge := functions.GetExprBridge()
	result, err := bridge.EvaluateExpression(e.exprLangExpression, data)
	if err != nil {
		return 0, err
	}

	// Try to convert result to float64
	switch v := result.(type) {
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
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("cannot convert string result '%s' to float64", v)
	default:
		return 0, fmt.Errorf("expression result type %T is not convertible to float64", result)
	}
}

// GetFields gets all fields referenced in the expression
func (e *Expression) GetFields() []string {
	if e.useExprLang {
		// For expr-lang expressions, need to parse field references
		// Simplified handling here, should use AST analysis in practice
		return extractFieldsFromExprLang(e.exprLangExpression)
	}

	fields := make(map[string]bool)
	collectFields(e.Root, fields)

	result := make([]string, 0, len(fields))
	for field := range fields {
		result = append(result, field)
	}
	return result
}

// extractFieldsFromExprLang extracts field references from expr-lang expression (simplified version)
func extractFieldsFromExprLang(expression string) []string {
	// This is a simplified implementation, should use AST parsing in practice
	// Temporarily use regex or simple string parsing
	fields := make(map[string]bool)

	// Simple field extraction: find identifier patterns, support dot-separated nested fields
	tokens := strings.FieldsFunc(expression, func(c rune) bool {
		return !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '_' && c != '.'
	})

	for _, token := range tokens {
		if isValidFieldIdentifier(token) && !isNumber(token) && !isFunctionOrKeyword(token) {
			fields[token] = true
		}
	}

	result := make([]string, 0, len(fields))
	for field := range fields {
		result = append(result, field)
	}
	return result
}

// isValidFieldIdentifier checks if it's a valid field identifier (supports dot-separated nested fields)
func isValidFieldIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	// Split dot-separated fields
	parts := strings.Split(s, ".")
	for _, part := range parts {
		if !isIdentifier(part) {
			return false
		}
	}

	return true
}

// isFunctionOrKeyword checks if it's a function name or keyword
func isFunctionOrKeyword(token string) bool {
	// Check if it's a known function or keyword
	keywords := []string{
		"and", "or", "not", "true", "false", "nil", "null", "is",
		"if", "else", "then", "in", "contains", "matches",
		// CASE expression keywords
		"case", "when", "then", "else", "end",
	}

	for _, keyword := range keywords {
		if strings.ToLower(token) == keyword {
			return true
		}
	}

	// Check if it's a registered function
	bridge := functions.GetExprBridge()
	_, exists, _ := bridge.ResolveFunction(token)
	return exists
}

// collectFields collects all fields in the expression
func collectFields(node *ExprNode, fields map[string]bool) {
	if node == nil {
		return
	}

	if node.Type == TypeField {
		fields[node.Value] = true
	}

	// Handle field collection for CASE expressions
	if node.Type == TypeCase {
		// Collect fields from CASE expression itself
		if node.CaseExpr != nil {
			collectFields(node.CaseExpr, fields)
		}

		// Collect fields from all WHEN clauses
		for _, whenClause := range node.WhenClauses {
			collectFields(whenClause.Condition, fields)
			collectFields(whenClause.Result, fields)
		}

		// Collect fields from ELSE expression
		if node.ElseExpr != nil {
			collectFields(node.ElseExpr, fields)
		}

		return
	}

	collectFields(node.Left, fields)
	collectFields(node.Right, fields)

	for _, arg := range node.Args {
		collectFields(arg, fields)
	}
}

// evaluateNode calculates the value of a node
func evaluateNode(node *ExprNode, data map[string]interface{}) (float64, error) {
	if node == nil {
		return 0, fmt.Errorf("null expression node")
	}

	switch node.Type {
	case TypeNumber:
		return strconv.ParseFloat(node.Value, 64)

	case TypeString:
		// Handle string type, remove quotes and try to convert to number
		// If conversion fails, return error (since this function returns float64)
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1] // Remove quotes
		}

		// Try to convert to number
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f, nil
		}

		// For string comparison, we need to return a hash value or error
		// Simplified handling here, convert string to its length (as temporary solution)
		return float64(len(value)), nil

	case TypeField:
		// Handle backtick identifiers, remove backticks
		fieldName := node.Value
		if len(fieldName) >= 2 && fieldName[0] == '`' && fieldName[len(fieldName)-1] == '`' {
			fieldName = fieldName[1 : len(fieldName)-1] // Remove backticks
		}

		// Support nested field access
		if fieldpath.IsNestedField(fieldName) {
			if val, found := fieldpath.GetNestedField(data, fieldName); found {
				// Try to convert to float64
				if floatVal, err := convertToFloat(val); err == nil {
					return floatVal, nil
				}
				// If cannot convert to number, return error
				return 0, fmt.Errorf("field '%s' value cannot be converted to number: %v", fieldName, val)
			}
		} else {
			// Original simple field access
			if val, found := data[fieldName]; found {
				// Try to convert to float64
				if floatVal, err := convertToFloat(val); err == nil {
					return floatVal, nil
				}
				// If cannot convert to number, return error
				return 0, fmt.Errorf("field '%s' value cannot be converted to number: %v", fieldName, val)
			}
		}
		return 0, fmt.Errorf("field '%s' not found", fieldName)

	case TypeOperator:
		// Calculate values of left and right sub-expressions
		left, err := evaluateNode(node.Left, data)
		if err != nil {
			return 0, err
		}

		right, err := evaluateNode(node.Right, data)
		if err != nil {
			return 0, err
		}

		// Perform operation
		switch node.Value {
		case "+":
			return left + right, nil
		case "-":
			return left - right, nil
		case "*":
			return left * right, nil
		case "/":
			if right == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			return left / right, nil
		case "%":
			if right == 0 {
				return 0, fmt.Errorf("modulo by zero")
			}
			return math.Mod(left, right), nil
		case "^":
			return math.Pow(left, right), nil
		default:
			return 0, fmt.Errorf("unknown operator: %s", node.Value)
		}

	case TypeFunction:
		// First check if it's a function in the new function registration system
		fn, exists := functions.Get(node.Value)
		if exists {
			// Calculate all arguments but keep original types
			args := make([]interface{}, len(node.Args))
			for i, arg := range node.Args {
				// Use evaluateNodeValue to get original type values
				val, err := evaluateNodeValue(arg, data)
				if err != nil {
					return 0, err
				}
				args[i] = val
			}

			// Create function execution context
			ctx := &functions.FunctionContext{
				Data: data,
			}

			// Execute function
			result, err := fn.Execute(ctx, args)
			if err != nil {
				return 0, err
			}

			// Convert result to float64
			switch r := result.(type) {
			case float64:
				return r, nil
			case float32:
				return float64(r), nil
			case int:
				return float64(r), nil
			case int32:
				return float64(r), nil
			case int64:
				return float64(r), nil
			case string:
				// For string results, try to convert to number, if failed return string length
				if f, err := strconv.ParseFloat(r, 64); err == nil {
					return f, nil
				}
				return float64(len(r)), nil
			case bool:
				// Boolean conversion: true=1, false=0
				if r {
					return 1.0, nil
				}
				return 0.0, nil
			default:
				return 0, fmt.Errorf("function %s returned unsupported type for numeric conversion: %T", node.Value, result)
			}
		}

		// Fall back to built-in function handling (maintain backward compatibility)
		return evaluateBuiltinFunction(node, data)

	case TypeCase:
		// Handle CASE expression
		return evaluateCaseExpression(node, data)
	}

	return 0, fmt.Errorf("unknown node type: %s", node.Type)
}

// evaluateBuiltinFunction handles built-in functions (backward compatibility)
func evaluateBuiltinFunction(node *ExprNode, data map[string]interface{}) (float64, error) {
	switch strings.ToLower(node.Value) {
	case "abs":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("abs function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Abs(arg), nil

	case "sqrt":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("sqrt function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		if arg < 0 {
			return 0, fmt.Errorf("sqrt of negative number")
		}
		return math.Sqrt(arg), nil

	case "sin":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("sin function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Sin(arg), nil

	case "cos":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("cos function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Cos(arg), nil

	case "tan":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("tan function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Tan(arg), nil

	case "floor":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("floor function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Floor(arg), nil

	case "ceil":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("ceil function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Ceil(arg), nil

	case "round":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("round function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Round(arg), nil

	default:
		return 0, fmt.Errorf("unknown function: %s", node.Value)
	}
}

// evaluateCaseExpression evaluates CASE expression
func evaluateCaseExpression(node *ExprNode, data map[string]interface{}) (float64, error) {
	if node.Type != TypeCase {
		return 0, fmt.Errorf("node is not a CASE expression")
	}

	// Handle simple CASE expression (CASE expr WHEN value1 THEN result1 ...)
	if node.CaseExpr != nil {
		// Calculate the value of expression after CASE
		caseValue, err := evaluateNodeValue(node.CaseExpr, data)
		if err != nil {
			return 0, err
		}

		// Iterate through WHEN clauses to find matching values
		for _, whenClause := range node.WhenClauses {
			conditionValue, err := evaluateNodeValue(whenClause.Condition, data)
			if err != nil {
				return 0, err
			}

			// Compare if values are equal
			isEqual, err := compareValues(caseValue, conditionValue, "==")
			if err != nil {
				return 0, err
			}

			if isEqual {
				return evaluateNode(whenClause.Result, data)
			}
		}
	} else {
		// Handle search CASE expression (CASE WHEN condition1 THEN result1 ...)
		for _, whenClause := range node.WhenClauses {
			// Evaluate WHEN condition, need special handling for boolean expressions
			conditionResult, err := evaluateBooleanCondition(whenClause.Condition, data)
			if err != nil {
				return 0, err
			}

			// If condition is true, return corresponding result
			if conditionResult {
				return evaluateNode(whenClause.Result, data)
			}
		}
	}

	// If no WHEN clause matches, execute ELSE clause
	if node.ElseExpr != nil {
		return evaluateNode(node.ElseExpr, data)
	}

	// If no ELSE clause, SQL standard returns NULL, here return 0
	return 0, nil
}

// evaluateBooleanCondition evaluates boolean condition expression
func evaluateBooleanCondition(node *ExprNode, data map[string]interface{}) (bool, error) {
	if node == nil {
		return false, fmt.Errorf("null condition expression")
	}

	// Handle logical operators (implement short-circuit evaluation)
	if node.Type == TypeOperator && (node.Value == "AND" || node.Value == "OR") {
		leftBool, err := evaluateBooleanCondition(node.Left, data)
		if err != nil {
			return false, err
		}

		// Short-circuit evaluation: for AND, if left is false, return false immediately
		if node.Value == "AND" && !leftBool {
			return false, nil
		}

		// Short-circuit evaluation: for OR, if left is true, return true immediately
		if node.Value == "OR" && leftBool {
			return true, nil
		}

		// Only evaluate right expression when needed
		rightBool, err := evaluateBooleanCondition(node.Right, data)
		if err != nil {
			return false, err
		}

		switch node.Value {
		case "AND":
			return leftBool && rightBool, nil
		case "OR":
			return leftBool || rightBool, nil
		}
	}

	// Handle IS NULL and IS NOT NULL special cases
	if node.Type == TypeOperator && node.Value == "IS" {
		return evaluateIsCondition(node, data)
	}

	// Handle comparison operators
	if node.Type == TypeOperator {
		leftValue, err := evaluateNodeValue(node.Left, data)
		if err != nil {
			return false, err
		}

		rightValue, err := evaluateNodeValue(node.Right, data)
		if err != nil {
			return false, err
		}

		return compareValues(leftValue, rightValue, node.Value)
	}

	// For other expressions, calculate numeric value and convert to boolean
	result, err := evaluateNode(node, data)
	if err != nil {
		return false, err
	}

	// Non-zero values are true, zero values are false
	return result != 0, nil
}

// evaluateIsCondition handles IS NULL and IS NOT NULL conditions
func evaluateIsCondition(node *ExprNode, data map[string]interface{}) (bool, error) {
	if node == nil || node.Left == nil || node.Right == nil {
		return false, fmt.Errorf("invalid IS condition")
	}

	// Get left side value
	leftValue, err := evaluateNodeValue(node.Left, data)
	if err != nil {
		// If field doesn't exist, consider it as null
		leftValue = nil
	}

	// Check if right side is NULL or NOT NULL
	if node.Right.Type == TypeField && strings.ToUpper(node.Right.Value) == "NULL" {
		// IS NULL
		return leftValue == nil, nil
	}

	// Check if it's IS NOT NULL
	if node.Right.Type == TypeOperator && node.Right.Value == "NOT" &&
		node.Right.Right != nil && node.Right.Right.Type == TypeField &&
		strings.ToUpper(node.Right.Right.Value) == "NULL" {
		// IS NOT NULL
		return leftValue != nil, nil
	}

	// Other IS comparisons (like IS TRUE, IS FALSE etc., not supported yet)
	rightValue, err := evaluateNodeValue(node.Right, data)
	if err != nil {
		return false, err
	}

	return compareValues(leftValue, rightValue, "==")
}

// evaluateNodeValue calculates node value, returns interface{} to support different types
func evaluateNodeValue(node *ExprNode, data map[string]interface{}) (interface{}, error) {
	if node == nil {
		return nil, fmt.Errorf("null expression node")
	}

	switch node.Type {
	case TypeNumber:
		return strconv.ParseFloat(node.Value, 64)

	case TypeString:
		// Remove quotes
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1]
		}
		return value, nil

	case TypeField:
		// Handle backtick identifiers, remove backticks
		fieldName := node.Value
		if len(fieldName) >= 2 && fieldName[0] == '`' && fieldName[len(fieldName)-1] == '`' {
			fieldName = fieldName[1 : len(fieldName)-1] // Remove backticks
		}

		// Support nested field access
		if fieldpath.IsNestedField(fieldName) {
			if val, found := fieldpath.GetNestedField(data, fieldName); found {
				return val, nil
			}
		} else {
			// Original simple field access
			if val, found := data[fieldName]; found {
				return val, nil
			}
		}
		return nil, fmt.Errorf("field '%s' not found", fieldName)

	default:
		// For other types, fall back to numeric calculation
		return evaluateNode(node, data)
	}
}

// compareValues compares two values
func compareValues(left, right interface{}, operator string) (bool, error) {
	// Try string comparison
	leftStr, leftIsStr := left.(string)
	rightStr, rightIsStr := right.(string)

	if leftIsStr && rightIsStr {
		switch operator {
		case "==", "=":
			return leftStr == rightStr, nil
		case "!=", "<>":
			return leftStr != rightStr, nil
		case ">":
			return leftStr > rightStr, nil
		case ">=":
			return leftStr >= rightStr, nil
		case "<":
			return leftStr < rightStr, nil
		case "<=":
			return leftStr <= rightStr, nil
		case "LIKE":
			return matchesLikePattern(leftStr, rightStr), nil
		default:
			return false, fmt.Errorf("unsupported string comparison operator: %s", operator)
		}
	}

	// Convert to numeric values for comparison
	leftNum, err1 := convertToFloat(left)
	rightNum, err2 := convertToFloat(right)

	if err1 != nil || err2 != nil {
		return false, fmt.Errorf("cannot compare values: %v and %v", left, right)
	}

	switch operator {
	case ">":
		return leftNum > rightNum, nil
	case ">=":
		return leftNum >= rightNum, nil
	case "<":
		return leftNum < rightNum, nil
	case "<=":
		return leftNum <= rightNum, nil
	case "==", "=":
		return math.Abs(leftNum-rightNum) < 1e-9, nil
	case "!=", "<>":
		return math.Abs(leftNum-rightNum) >= 1e-9, nil
	default:
		return false, fmt.Errorf("unsupported comparison operator: %s", operator)
	}
}

// matchesLikePattern implements LIKE pattern matching
// Supports % (matches any character sequence) and _ (matches single character)
func matchesLikePattern(text, pattern string) bool {
	return likeMatch(text, pattern, 0, 0)
}

// likeMatch recursively implements LIKE matching algorithm
func likeMatch(text, pattern string, textIndex, patternIndex int) bool {
	// If pattern matching is complete
	if patternIndex >= len(pattern) {
		return textIndex >= len(text) // Text should also be completely matched
	}

	// If text has ended but pattern still has non-% characters, no match
	if textIndex >= len(text) {
		// Check if remaining pattern consists only of %
		for i := patternIndex; i < len(pattern); i++ {
			if pattern[i] != '%' {
				return false
			}
		}
		return true
	}

	switch pattern[patternIndex] {
	case '%':
		// % can match 0 or more characters
		// Try matching 0 characters (skip %)
		if likeMatch(text, pattern, textIndex, patternIndex+1) {
			return true
		}
		// Try matching 1 or more characters
		for i := textIndex; i < len(text); i++ {
			if likeMatch(text, pattern, i+1, patternIndex+1) {
				return true
			}
		}
		return false

	case '_':
		// _ matches any single character
		return likeMatch(text, pattern, textIndex+1, patternIndex+1)

	default:
		// Regular characters must match exactly
		if text[textIndex] == pattern[patternIndex] {
			return likeMatch(text, pattern, textIndex+1, patternIndex+1)
		}
		return false
	}
}

// convertToFloat converts value to float64
func convertToFloat(val interface{}) (float64, error) {
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

// tokenize converts expression string to token list
func tokenize(expr string) ([]string, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty expression")
	}

	tokens := make([]string, 0)
	i := 0

	for i < len(expr) {
		ch := expr[i]

		// Skip whitespace characters
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			i++
			continue
		}

		// Handle numbers
		if isDigit(ch) || (ch == '.' && i+1 < len(expr) && isDigit(expr[i+1])) {
			start := i
			hasDot := ch == '.'

			i++
			for i < len(expr) && (isDigit(expr[i]) || (expr[i] == '.' && !hasDot)) {
				if expr[i] == '.' {
					hasDot = true
				}
				i++
			}

			tokens = append(tokens, expr[start:i])
			continue
		}

		// Handle operators and parentheses
		if ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '%' || ch == '^' ||
			ch == '(' || ch == ')' || ch == ',' {

			// Special handling for minus sign: if it's minus and preceded by operator, parenthesis or start position, it might be negative number
			if ch == '-' {
				// Check if it could be the start of a negative number
				canBeNegativeNumber := i == 0 || // Expression start
					len(tokens) == 0 // When tokens is empty, it could also be negative number start

				// Only check previous token when tokens is not empty
				if len(tokens) > 0 {
					prevToken := tokens[len(tokens)-1]
					canBeNegativeNumber = canBeNegativeNumber ||
						prevToken == "(" || // After left parenthesis
						prevToken == "," || // After comma (function parameter)
						isOperator(prevToken) || // After operator
						isComparisonOperator(prevToken) || // After comparison operator
						strings.ToUpper(prevToken) == "THEN" || // After THEN
						strings.ToUpper(prevToken) == "ELSE" || // After ELSE
						strings.ToUpper(prevToken) == "WHEN" || // After WHEN
						strings.ToUpper(prevToken) == "AND" || // After AND
						strings.ToUpper(prevToken) == "OR" // After OR
				}

				if canBeNegativeNumber && i+1 < len(expr) && isDigit(expr[i+1]) {
					// This is a negative number, parse the entire number
					start := i
					i++ // Skip minus sign

					// Parse numeric part
					for i < len(expr) && (isDigit(expr[i]) || expr[i] == '.') {
						i++
					}

					tokens = append(tokens, expr[start:i])
					continue
				}
			}

			tokens = append(tokens, string(ch))
			i++
			continue
		}

		// Handle comparison operators
		if ch == '>' || ch == '<' || ch == '=' || ch == '!' {
			start := i
			i++

			// Handle two-character operators
			if i < len(expr) {
				switch ch {
				case '>':
					if expr[i] == '=' {
						i++
						tokens = append(tokens, ">=")
						continue
					}
				case '<':
					if expr[i] == '=' {
						i++
						tokens = append(tokens, "<=")
						continue
					} else if expr[i] == '>' {
						i++
						tokens = append(tokens, "<>")
						continue
					}
				case '=':
					if expr[i] == '=' {
						i++
						tokens = append(tokens, "==")
						continue
					}
				case '!':
					if expr[i] == '=' {
						i++
						tokens = append(tokens, "!=")
						continue
					}
				}
			}

			// Single character operator
			tokens = append(tokens, expr[start:i])
			continue
		}

		// Handle string literals (single and double quotes)
		if ch == '\'' || ch == '"' {
			quote := ch
			start := i
			i++ // Skip opening quote

			// Find closing quote
			for i < len(expr) && expr[i] != quote {
				if expr[i] == '\\' && i+1 < len(expr) {
					i += 2 // Skip escape character
				} else {
					i++
				}
			}

			if i >= len(expr) {
				return nil, fmt.Errorf("unterminated string literal starting at position %d", start)
			}

			i++ // Skip closing quote
			tokens = append(tokens, expr[start:i])
			continue
		}

		// Handle backtick identifiers
		if ch == '`' {
			start := i
			i++ // Skip opening backtick

			// Find closing backtick
			for i < len(expr) && expr[i] != '`' {
				i++
			}

			if i >= len(expr) {
				return nil, fmt.Errorf("unterminated quoted identifier starting at position %d", start)
			}

			i++ // Skip closing backtick
			tokens = append(tokens, expr[start:i])
			continue
		}

		// Handle identifiers (field names or function names)
		if isLetter(ch) {
			start := i
			i++
			for i < len(expr) && (isLetter(expr[i]) || isDigit(expr[i]) || expr[i] == '_') {
				i++
			}

			tokens = append(tokens, expr[start:i])
			continue
		}

		// Unknown character
		return nil, fmt.Errorf("unexpected character: %c at position %d", ch, i)
	}

	return tokens, nil
}

// parseExpression parses expression
func parseExpression(tokens []string) (*ExprNode, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty token list")
	}

	// Use Shunting-yard algorithm to handle operator precedence
	output := make([]*ExprNode, 0)
	operators := make([]string, 0)

	i := 0
	for i < len(tokens) {
		token := tokens[i]

		// Handle numbers
		if isNumber(token) {
			output = append(output, &ExprNode{
				Type:  TypeNumber,
				Value: token,
			})
			i++
			continue
		}

		// Handle string literals
		if isStringLiteral(token) {
			output = append(output, &ExprNode{
				Type:  TypeString,
				Value: token,
			})
			i++
			continue
		}

		// Handle field names or function calls
		if isIdentifier(token) {
			// Check if it's a logical operator keyword
			upperToken := strings.ToUpper(token)
			if upperToken == "AND" || upperToken == "OR" || upperToken == "NOT" || upperToken == "LIKE" {
				// Handle logical operators
				for len(operators) > 0 && operators[len(operators)-1] != "(" &&
					operatorPrecedence[operators[len(operators)-1]] >= operatorPrecedence[upperToken] {
					op := operators[len(operators)-1]
					operators = operators[:len(operators)-1]

					if len(output) < 2 {
						return nil, fmt.Errorf("not enough operands for operator: %s", op)
					}

					right := output[len(output)-1]
					left := output[len(output)-2]
					output = output[:len(output)-2]

					output = append(output, &ExprNode{
						Type:  TypeOperator,
						Value: op,
						Left:  left,
						Right: right,
					})
				}

				operators = append(operators, upperToken)
				i++
				continue
			}

			// Special handling for IS operator, need to check subsequent NOT NULL combination
			if upperToken == "IS" {
				// Handle pending operators
				for len(operators) > 0 && operators[len(operators)-1] != "(" &&
					operatorPrecedence[operators[len(operators)-1]] >= operatorPrecedence["IS"] {
					op := operators[len(operators)-1]
					operators = operators[:len(operators)-1]

					if len(output) < 2 {
						return nil, fmt.Errorf("not enough operands for operator: %s", op)
					}

					right := output[len(output)-1]
					left := output[len(output)-2]
					output = output[:len(output)-2]

					output = append(output, &ExprNode{
						Type:  TypeOperator,
						Value: op,
						Left:  left,
						Right: right,
					})
				}

				// Check if it's IS NOT NULL pattern
				if i+2 < len(tokens) &&
					strings.ToUpper(tokens[i+1]) == "NOT" &&
					strings.ToUpper(tokens[i+2]) == "NULL" {
					// This is IS NOT NULL, create special right-side node structure
					notNullNode := &ExprNode{
						Type:  TypeOperator,
						Value: "NOT",
						Right: &ExprNode{
							Type:  TypeField,
							Value: "NULL",
						},
					}

					operators = append(operators, "IS")
					output = append(output, notNullNode)
					i += 3 // Skip three tokens: IS NOT NULL
					continue
				} else if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "NULL" {
					// This is IS NULL, create NULL node
					nullNode := &ExprNode{
						Type:  TypeField,
						Value: "NULL",
					}

					operators = append(operators, "IS")
					output = append(output, nullNode)
					i += 2 // Skip two tokens: IS NULL
					continue
				} else {
					// Regular IS operator
					operators = append(operators, "IS")
					i++
					continue
				}
			}

			// Check if it's CASE expression
			if strings.ToUpper(token) == "CASE" {
				caseNode, newIndex, err := parseCaseExpression(tokens, i)
				if err != nil {
					return nil, err
				}
				output = append(output, caseNode)
				i = newIndex
				continue
			}

			// Check if next token is left parenthesis, if so it's a function call
			if i+1 < len(tokens) && tokens[i+1] == "(" {
				funcName := token
				i += 2 // Skip function name and left parenthesis

				// Parse function arguments
				args, newIndex, err := parseFunctionArgs(tokens, i)
				if err != nil {
					return nil, err
				}

				output = append(output, &ExprNode{
					Type:  TypeFunction,
					Value: funcName,
					Args:  args,
				})

				i = newIndex
				continue
			}

			// Regular field
			output = append(output, &ExprNode{
				Type:  TypeField,
				Value: token,
			})
			i++
			continue
		}

		// Handle left parenthesis
		if token == "(" {
			operators = append(operators, token)
			i++
			continue
		}

		// Handle right parenthesis
		if token == ")" {
			for len(operators) > 0 && operators[len(operators)-1] != "(" {
				op := operators[len(operators)-1]
				operators = operators[:len(operators)-1]

				if len(output) < 2 {
					return nil, fmt.Errorf("not enough operands for operator: %s", op)
				}

				right := output[len(output)-1]
				left := output[len(output)-2]
				output = output[:len(output)-2]

				output = append(output, &ExprNode{
					Type:  TypeOperator,
					Value: op,
					Left:  left,
					Right: right,
				})
			}

			if len(operators) == 0 || operators[len(operators)-1] != "(" {
				return nil, fmt.Errorf("mismatched parentheses")
			}

			operators = operators[:len(operators)-1] // Pop left parenthesis
			i++
			continue
		}

		// Handle operators
		if isOperator(token) {
			for len(operators) > 0 && operators[len(operators)-1] != "(" &&
				operatorPrecedence[operators[len(operators)-1]] >= operatorPrecedence[token] {
				op := operators[len(operators)-1]
				operators = operators[:len(operators)-1]

				if len(output) < 2 {
					return nil, fmt.Errorf("not enough operands for operator: %s", op)
				}

				right := output[len(output)-1]
				left := output[len(output)-2]
				output = output[:len(output)-2]

				output = append(output, &ExprNode{
					Type:  TypeOperator,
					Value: op,
					Left:  left,
					Right: right,
				})
			}

			operators = append(operators, token)
			i++
			continue
		}

		// Handle comma (processed in function argument list)
		if token == "," {
			i++
			continue
		}

		return nil, fmt.Errorf("unexpected token: %s", token)
	}

	// Handle remaining operators
	for len(operators) > 0 {
		op := operators[len(operators)-1]
		operators = operators[:len(operators)-1]

		if op == "(" {
			return nil, fmt.Errorf("mismatched parentheses")
		}

		if len(output) < 2 {
			return nil, fmt.Errorf("not enough operands for operator: %s", op)
		}

		right := output[len(output)-1]
		left := output[len(output)-2]
		output = output[:len(output)-2]

		output = append(output, &ExprNode{
			Type:  TypeOperator,
			Value: op,
			Left:  left,
			Right: right,
		})
	}

	if len(output) != 1 {
		return nil, fmt.Errorf("invalid expression")
	}

	return output[0], nil
}

// parseFunctionArgs parses function arguments
func parseFunctionArgs(tokens []string, startIndex int) ([]*ExprNode, int, error) {
	args := make([]*ExprNode, 0)
	i := startIndex

	// Handle empty argument list
	if i < len(tokens) && tokens[i] == ")" {
		return args, i + 1, nil
	}

	for i < len(tokens) {
		// Parse argument expression
		argTokens := make([]string, 0)
		parenthesesCount := 0

		for i < len(tokens) {
			token := tokens[i]

			if token == "(" {
				parenthesesCount++
			} else if token == ")" {
				parenthesesCount--
				if parenthesesCount < 0 {
					break
				}
			} else if token == "," && parenthesesCount == 0 {
				break
			}

			argTokens = append(argTokens, token)
			i++
		}

		if len(argTokens) > 0 {
			arg, err := parseExpression(argTokens)
			if err != nil {
				return nil, 0, err
			}
			args = append(args, arg)
		}

		if i >= len(tokens) {
			return nil, 0, fmt.Errorf("unexpected end of tokens in function arguments")
		}

		if tokens[i] == ")" {
			return args, i + 1, nil
		}

		if tokens[i] == "," {
			i++
			continue
		}

		return nil, 0, fmt.Errorf("unexpected token in function arguments: %s", tokens[i])
	}

	return nil, 0, fmt.Errorf("unexpected end of tokens in function arguments")
}

// parseCaseExpression parses CASE expression
func parseCaseExpression(tokens []string, startIndex int) (*ExprNode, int, error) {
	if startIndex >= len(tokens) || strings.ToUpper(tokens[startIndex]) != "CASE" {
		return nil, startIndex, fmt.Errorf("expected CASE keyword")
	}

	caseNode := &ExprNode{
		Type:        TypeCase,
		WhenClauses: make([]WhenClause, 0),
	}

	i := startIndex + 1 // 跳过CASE关键字

	// 检查是否是简单CASE表达式（CASE expr WHEN value1 THEN result1 ...）
	// 或搜索CASE表达式（CASE WHEN condition1 THEN result1 ...）
	if i < len(tokens) && strings.ToUpper(tokens[i]) != "WHEN" {
		// 这是简单CASE表达式，需要解析CASE后面的表达式
		caseExprTokens := make([]string, 0)

		// 收集CASE表达式直到遇到WHEN
		for i < len(tokens) && strings.ToUpper(tokens[i]) != "WHEN" {
			caseExprTokens = append(caseExprTokens, tokens[i])
			i++
		}

		if len(caseExprTokens) == 0 {
			return nil, i, fmt.Errorf("expected expression after CASE")
		}

		// 对于简单的情况，直接处理单个token
		if len(caseExprTokens) == 1 {
			token := caseExprTokens[0]
			if isNumber(token) {
				caseNode.CaseExpr = &ExprNode{Type: TypeNumber, Value: token}
			} else if isStringLiteral(token) {
				caseNode.CaseExpr = &ExprNode{Type: TypeString, Value: token}
			} else if isIdentifier(token) {
				caseNode.CaseExpr = &ExprNode{Type: TypeField, Value: token}
			} else {
				return nil, i, fmt.Errorf("invalid CASE expression token: %s", token)
			}
		} else {
			// 对于复杂表达式，调用parseExpression
			caseExpr, err := parseExpression(caseExprTokens)
			if err != nil {
				return nil, i, fmt.Errorf("failed to parse CASE expression: %w", err)
			}
			caseNode.CaseExpr = caseExpr
		}
	}

	// 解析WHEN子句
	for i < len(tokens) && strings.ToUpper(tokens[i]) == "WHEN" {
		i++ // 跳过WHEN关键字

		// 收集WHEN条件直到遇到THEN
		conditionTokens := make([]string, 0)
		for i < len(tokens) && strings.ToUpper(tokens[i]) != "THEN" {
			conditionTokens = append(conditionTokens, tokens[i])
			i++
		}

		if len(conditionTokens) == 0 {
			return nil, i, fmt.Errorf("expected condition after WHEN")
		}

		if i >= len(tokens) || strings.ToUpper(tokens[i]) != "THEN" {
			return nil, i, fmt.Errorf("expected THEN after WHEN condition")
		}

		i++ // 跳过THEN关键字

		// 收集THEN结果直到遇到WHEN、ELSE或END
		resultTokens := make([]string, 0)
		for i < len(tokens) {
			upper := strings.ToUpper(tokens[i])
			if upper == "WHEN" || upper == "ELSE" || upper == "END" {
				break
			}
			resultTokens = append(resultTokens, tokens[i])
			i++
		}

		if len(resultTokens) == 0 {
			return nil, i, fmt.Errorf("expected result after THEN")
		}

		// 解析条件和结果表达式
		conditionExpr, err := parseExpression(conditionTokens)
		if err != nil {
			return nil, i, fmt.Errorf("failed to parse WHEN condition: %w", err)
		}

		resultExpr, err := parseExpression(resultTokens)
		if err != nil {
			return nil, i, fmt.Errorf("failed to parse THEN result: %w", err)
		}

		// 添加WHEN子句
		caseNode.WhenClauses = append(caseNode.WhenClauses, WhenClause{
			Condition: conditionExpr,
			Result:    resultExpr,
		})
	}

	// 检查是否有ELSE子句
	if i < len(tokens) && strings.ToUpper(tokens[i]) == "ELSE" {
		i++ // 跳过ELSE关键字

		// 收集ELSE结果直到遇到END
		elseTokens := make([]string, 0)
		for i < len(tokens) && strings.ToUpper(tokens[i]) != "END" {
			elseTokens = append(elseTokens, tokens[i])
			i++
		}

		if len(elseTokens) == 0 {
			return nil, i, fmt.Errorf("expected result after ELSE")
		}

		// 解析ELSE表达式
		elseExpr, err := parseExpression(elseTokens)
		if err != nil {
			return nil, i, fmt.Errorf("failed to parse ELSE result: %w", err)
		}
		caseNode.ElseExpr = elseExpr
	}

	// 检查END关键字
	if i >= len(tokens) || strings.ToUpper(tokens[i]) != "END" {
		return nil, i, fmt.Errorf("expected END to close CASE expression")
	}

	i++ // 跳过END关键字

	// 验证至少有一个WHEN子句
	if len(caseNode.WhenClauses) == 0 {
		return nil, i, fmt.Errorf("CASE expression must have at least one WHEN clause")
	}

	return caseNode, i, nil
}

// 辅助函数
func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isNumber(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	if !isLetter(s[0]) && s[0] != '_' {
		return false
	}

	for i := 1; i < len(s); i++ {
		if !isLetter(s[i]) && !isDigit(s[i]) && s[i] != '_' {
			return false
		}
	}

	return true
}

func isOperator(s string) bool {
	switch s {
	case "+", "-", "*", "/", "%", "^":
		return true
	case ">", "<", ">=", "<=", "==", "=", "!=", "<>":
		return true
	case "AND", "OR", "NOT":
		return true
	case "LIKE", "IS":
		return true
	default:
		return false
	}
}

// isComparisonOperator 检查是否是比较运算符
func isComparisonOperator(s string) bool {
	switch s {
	case ">", "<", ">=", "<=", "==", "=", "!=", "<>":
		return true
	default:
		return false
	}
}

func isStringLiteral(expr string) bool {
	return len(expr) > 1 && (expr[0] == '\'' || expr[0] == '"') && expr[len(expr)-1] == expr[0]
}

// evaluateNodeWithNull 计算节点值，支持NULL值返回
// 返回 (result, isNull, error)
func evaluateNodeWithNull(node *ExprNode, data map[string]interface{}) (float64, bool, error) {
	if node == nil {
		return 0, true, nil // NULL
	}

	switch node.Type {
	case TypeNumber:
		val, err := strconv.ParseFloat(node.Value, 64)
		return val, false, err

	case TypeString:
		// 字符串长度作为数值，特殊处理NULL字符串
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1]
		}
		// 检查是否是NULL字符串
		if strings.ToUpper(value) == "NULL" {
			return 0, true, nil
		}
		return float64(len(value)), false, nil

	case TypeField:
		// 支持嵌套字段访问
		var fieldVal interface{}
		var found bool

		if fieldpath.IsNestedField(node.Value) {
			fieldVal, found = fieldpath.GetNestedField(data, node.Value)
		} else {
			fieldVal, found = data[node.Value]
		}

		if !found || fieldVal == nil {
			return 0, true, nil // NULL
		}

		// 尝试转换为数值
		if val, err := convertToFloat(fieldVal); err == nil {
			return val, false, nil
		}
		return 0, true, fmt.Errorf("cannot convert field '%s' to number", node.Value)

	case TypeOperator:
		return evaluateOperatorWithNull(node, data)

	case TypeFunction:
		// 函数调用保持原有逻辑，但处理NULL结果
		result, err := evaluateBuiltinFunction(node, data)
		return result, false, err

	case TypeCase:
		return evaluateCaseExpressionWithNull(node, data)

	default:
		return 0, true, fmt.Errorf("unsupported node type: %s", node.Type)
	}
}

// evaluateOperatorWithNull 计算运算符表达式，支持NULL值
func evaluateOperatorWithNull(node *ExprNode, data map[string]interface{}) (float64, bool, error) {
	leftVal, leftNull, err := evaluateNodeWithNull(node.Left, data)
	if err != nil {
		return 0, false, err
	}

	rightVal, rightNull, err := evaluateNodeWithNull(node.Right, data)
	if err != nil {
		return 0, false, err
	}

	// 算术运算：如果任一操作数为NULL，结果为NULL
	if leftNull || rightNull {
		switch node.Value {
		case "+", "-", "*", "/", "%", "^":
			return 0, true, nil
		}
	}

	// 比较运算：NULL值的比较有特殊规则
	switch node.Value {
	case "==", "=":
		if leftNull && rightNull {
			return 1, false, nil // NULL = NULL 为 true
		}
		if leftNull || rightNull {
			return 0, false, nil // NULL = value 为 false
		}
		if leftVal == rightVal {
			return 1, false, nil
		}
		return 0, false, nil

	case "!=", "<>":
		if leftNull && rightNull {
			return 0, false, nil // NULL != NULL 为 false
		}
		if leftNull || rightNull {
			return 0, false, nil // NULL != value 为 false
		}
		if leftVal != rightVal {
			return 1, false, nil
		}
		return 0, false, nil

	case ">", "<", ">=", "<=":
		if leftNull || rightNull {
			return 0, false, nil // NULL与任何值的比较都为false
		}
	}

	// 对于非NULL值，执行正常的算术和比较运算
	switch node.Value {
	case "+":
		return leftVal + rightVal, false, nil
	case "-":
		return leftVal - rightVal, false, nil
	case "*":
		return leftVal * rightVal, false, nil
	case "/":
		if rightVal == 0 {
			return 0, true, nil // 除零返回NULL
		}
		return leftVal / rightVal, false, nil
	case "%":
		if rightVal == 0 {
			return 0, true, nil
		}
		return math.Mod(leftVal, rightVal), false, nil
	case "^":
		return math.Pow(leftVal, rightVal), false, nil
	case ">":
		if leftVal > rightVal {
			return 1, false, nil
		}
		return 0, false, nil
	case "<":
		if leftVal < rightVal {
			return 1, false, nil
		}
		return 0, false, nil
	case ">=":
		if leftVal >= rightVal {
			return 1, false, nil
		}
		return 0, false, nil
	case "<=":
		if leftVal <= rightVal {
			return 1, false, nil
		}
		return 0, false, nil
	default:
		return 0, false, fmt.Errorf("unsupported operator: %s", node.Value)
	}
}

// evaluateCaseExpressionWithNull 计算CASE表达式，支持NULL值
func evaluateCaseExpressionWithNull(node *ExprNode, data map[string]interface{}) (float64, bool, error) {
	if node.Type != TypeCase {
		return 0, false, fmt.Errorf("node is not a CASE expression")
	}

	// 处理简单CASE表达式 (CASE expr WHEN value1 THEN result1 ...)
	if node.CaseExpr != nil {
		// 计算CASE后面的表达式值
		caseValue, caseNull, err := evaluateNodeValueWithNull(node.CaseExpr, data)
		if err != nil {
			return 0, false, err
		}

		// 遍历WHEN子句，查找匹配的值
		for _, whenClause := range node.WhenClauses {
			conditionValue, condNull, err := evaluateNodeValueWithNull(whenClause.Condition, data)
			if err != nil {
				return 0, false, err
			}

			// 比较值是否相等（考虑NULL值）
			var isEqual bool
			if caseNull && condNull {
				isEqual = true // NULL = NULL
			} else if caseNull || condNull {
				isEqual = false // NULL != value
			} else {
				isEqual, err = compareValuesForEquality(caseValue, conditionValue)
				if err != nil {
					return 0, false, err
				}
			}

			if isEqual {
				return evaluateNodeWithNull(whenClause.Result, data)
			}
		}
	} else {
		// 处理搜索CASE表达式 (CASE WHEN condition1 THEN result1 ...)
		for _, whenClause := range node.WhenClauses {
			// 评估WHEN条件
			conditionResult, err := evaluateBooleanConditionWithNull(whenClause.Condition, data)
			if err != nil {
				return 0, false, err
			}

			// 如果条件为真，返回对应的结果
			if conditionResult {
				return evaluateNodeWithNull(whenClause.Result, data)
			}
		}
	}

	// 如果没有匹配的WHEN子句，执行ELSE子句
	if node.ElseExpr != nil {
		return evaluateNodeWithNull(node.ElseExpr, data)
	}

	// 如果没有ELSE子句，SQL标准是返回NULL
	return 0, true, nil
}

// evaluateCaseExpressionValueWithNull 计算CASE表达式并返回实际值（支持字符串），支持NULL值
func evaluateCaseExpressionValueWithNull(node *ExprNode, data map[string]interface{}) (interface{}, bool, error) {
	if node.Type != TypeCase {
		return nil, false, fmt.Errorf("node is not a CASE expression")
	}

	// 处理简单CASE表达式 (CASE expr WHEN value1 THEN result1 ...)
	if node.CaseExpr != nil {
		// 计算CASE后面的表达式值
		caseValue, caseNull, err := evaluateNodeValueWithNull(node.CaseExpr, data)
		if err != nil {
			return nil, false, err
		}

		// 遍历WHEN子句，查找匹配的值
		for _, whenClause := range node.WhenClauses {
			conditionValue, condNull, err := evaluateNodeValueWithNull(whenClause.Condition, data)
			if err != nil {
				return nil, false, err
			}

			// 比较值是否相等（考虑NULL值）
			var isEqual bool
			if caseNull && condNull {
				isEqual = true // NULL = NULL
			} else if caseNull || condNull {
				isEqual = false // NULL != value
			} else {
				isEqual, err = compareValuesForEquality(caseValue, conditionValue)
				if err != nil {
					return nil, false, err
				}
			}

			if isEqual {
				return evaluateNodeValueWithNull(whenClause.Result, data)
			}
		}
	} else {
		// 处理搜索CASE表达式 (CASE WHEN condition1 THEN result1 ...)
		for _, whenClause := range node.WhenClauses {
			// 评估WHEN条件
			conditionResult, err := evaluateBooleanConditionWithNull(whenClause.Condition, data)
			if err != nil {
				return nil, false, err
			}

			// 如果条件为真，返回对应的结果
			if conditionResult {
				return evaluateNodeValueWithNull(whenClause.Result, data)
			}
		}
	}

	// 如果没有匹配的WHEN子句，执行ELSE子句
	if node.ElseExpr != nil {
		return evaluateNodeValueWithNull(node.ElseExpr, data)
	}

	// 如果没有ELSE子句，SQL标准是返回NULL
	return nil, true, nil
}

// evaluateNodeValueWithNull 计算节点值，返回interface{}以支持不同类型，包含NULL检查
func evaluateNodeValueWithNull(node *ExprNode, data map[string]interface{}) (interface{}, bool, error) {
	if node == nil {
		return nil, true, nil
	}

	switch node.Type {
	case TypeNumber:
		val, err := strconv.ParseFloat(node.Value, 64)
		return val, false, err

	case TypeString:
		// 去掉引号
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1]
		}
		// 检查是否是NULL字符串
		if strings.ToUpper(value) == "NULL" {
			return nil, true, nil
		}
		return value, false, nil

	case TypeField:
		// 处理反引号标识符，去除反引号
		fieldName := node.Value
		if len(fieldName) >= 2 && fieldName[0] == '`' && fieldName[len(fieldName)-1] == '`' {
			fieldName = fieldName[1 : len(fieldName)-1] // 去掉反引号
		}

		// 支持嵌套字段访问
		if fieldpath.IsNestedField(fieldName) {
			if val, found := fieldpath.GetNestedField(data, fieldName); found {
				return val, val == nil, nil
			}
		} else {
			// 原有的简单字段访问
			if val, found := data[fieldName]; found {
				return val, val == nil, nil
			}
		}
		return nil, true, nil // 字段不存在视为NULL

	case TypeCase:
		// 处理CASE表达式，返回实际值
		return evaluateCaseExpressionValueWithNull(node, data)

	default:
		// 对于其他类型，回退到数值计算
		result, isNull, err := evaluateNodeWithNull(node, data)
		return result, isNull, err
	}
}

// evaluateBooleanConditionWithNull 计算布尔条件表达式，支持NULL值
func evaluateBooleanConditionWithNull(node *ExprNode, data map[string]interface{}) (bool, error) {
	if node == nil {
		return false, fmt.Errorf("null condition expression")
	}

	// 处理逻辑运算符（实现短路求值）
	if node.Type == TypeOperator && (node.Value == "AND" || node.Value == "OR") {
		leftBool, err := evaluateBooleanConditionWithNull(node.Left, data)
		if err != nil {
			return false, err
		}

		// 短路求值：对于AND，如果左边为false，立即返回false
		if node.Value == "AND" && !leftBool {
			return false, nil
		}

		// 短路求值：对于OR，如果左边为true，立即返回true
		if node.Value == "OR" && leftBool {
			return true, nil
		}

		// 只有在需要时才评估右边的表达式
		rightBool, err := evaluateBooleanConditionWithNull(node.Right, data)
		if err != nil {
			return false, err
		}

		switch node.Value {
		case "AND":
			return leftBool && rightBool, nil
		case "OR":
			return leftBool || rightBool, nil
		}
	}

	// 处理IS NULL和IS NOT NULL特殊情况
	if node.Type == TypeOperator && node.Value == "IS" {
		return evaluateIsConditionWithNull(node, data)
	}

	// 处理比较运算符
	if node.Type == TypeOperator {
		leftValue, leftNull, err := evaluateNodeValueWithNull(node.Left, data)
		if err != nil {
			return false, err
		}

		rightValue, rightNull, err := evaluateNodeValueWithNull(node.Right, data)
		if err != nil {
			return false, err
		}

		return compareValuesWithNull(leftValue, leftNull, rightValue, rightNull, node.Value)
	}

	// 对于其他表达式，计算其数值并转换为布尔值
	result, isNull, err := evaluateNodeWithNull(node, data)
	if err != nil {
		return false, err
	}

	// NULL值在布尔上下文中为false，非零值为真，零值为假
	return !isNull && result != 0, nil
}

// evaluateIsConditionWithNull 处理IS NULL和IS NOT NULL条件，支持NULL值
func evaluateIsConditionWithNull(node *ExprNode, data map[string]interface{}) (bool, error) {
	if node == nil || node.Left == nil || node.Right == nil {
		return false, fmt.Errorf("invalid IS condition")
	}

	// 获取左侧值
	leftValue, leftNull, err := evaluateNodeValueWithNull(node.Left, data)
	if err != nil {
		// 如果字段不存在，认为是null
		leftValue = nil
		leftNull = true
	}

	// 检查右侧是否是NULL或NOT NULL
	if node.Right.Type == TypeField && strings.ToUpper(node.Right.Value) == "NULL" {
		// IS NULL
		return leftNull || leftValue == nil, nil
	}

	// 检查是否是IS NOT NULL
	if node.Right.Type == TypeOperator && node.Right.Value == "NOT" &&
		node.Right.Right != nil && node.Right.Right.Type == TypeField &&
		strings.ToUpper(node.Right.Right.Value) == "NULL" {
		// IS NOT NULL
		return !leftNull && leftValue != nil, nil
	}

	// 其他IS比较
	rightValue, rightNull, err := evaluateNodeValueWithNull(node.Right, data)
	if err != nil {
		return false, err
	}

	return compareValuesWithNullForEquality(leftValue, leftNull, rightValue, rightNull)
}

// compareValuesForEquality 比较两个值是否相等
func compareValuesForEquality(left, right interface{}) (bool, error) {
	// 尝试字符串比较
	leftStr, leftIsStr := left.(string)
	rightStr, rightIsStr := right.(string)

	if leftIsStr && rightIsStr {
		return leftStr == rightStr, nil
	}

	// 尝试数值比较
	leftFloat, leftErr := convertToFloat(left)
	rightFloat, rightErr := convertToFloat(right)

	if leftErr == nil && rightErr == nil {
		return leftFloat == rightFloat, nil
	}

	// 如果都不能转换，直接比较
	return left == right, nil
}

// compareValuesWithNull 比较两个值（支持NULL）
func compareValuesWithNull(left interface{}, leftNull bool, right interface{}, rightNull bool, operator string) (bool, error) {
	// NULL值的比较有特殊规则
	switch operator {
	case "==", "=":
		if leftNull && rightNull {
			return true, nil // NULL = NULL 为 true
		}
		if leftNull || rightNull {
			return false, nil // NULL = value 为 false
		}

	case "!=", "<>":
		if leftNull && rightNull {
			return false, nil // NULL != NULL 为 false
		}
		if leftNull || rightNull {
			return false, nil // NULL != value 为 false
		}

	case ">", "<", ">=", "<=":
		if leftNull || rightNull {
			return false, nil // NULL与任何值的比较都为false
		}
	}

	// 对于非NULL值，执行正确的比较逻辑
	switch operator {
	case "==", "=":
		return compareValuesForEquality(left, right)
	case "!=", "<>":
		equal, err := compareValuesForEquality(left, right)
		return !equal, err
	case ">", "<", ">=", "<=":
		// 进行数值比较
		leftFloat, leftErr := convertToFloat(left)
		rightFloat, rightErr := convertToFloat(right)

		if leftErr != nil || rightErr != nil {
			// 如果不能转换为数值，尝试字符串比较
			leftStr := fmt.Sprintf("%v", left)
			rightStr := fmt.Sprintf("%v", right)

			switch operator {
			case ">":
				return leftStr > rightStr, nil
			case "<":
				return leftStr < rightStr, nil
			case ">=":
				return leftStr >= rightStr, nil
			case "<=":
				return leftStr <= rightStr, nil
			}
		}

		// 数值比较
		switch operator {
		case ">":
			return leftFloat > rightFloat, nil
		case "<":
			return leftFloat < rightFloat, nil
		case ">=":
			return leftFloat >= rightFloat, nil
		case "<=":
			return leftFloat <= rightFloat, nil
		}
	}

	return false, fmt.Errorf("unsupported operator: %s", operator)
}

// compareValuesWithNullForEquality 比较两个值是否相等（支持NULL）
func compareValuesWithNullForEquality(left interface{}, leftNull bool, right interface{}, rightNull bool) (bool, error) {
	if leftNull && rightNull {
		return true, nil // NULL = NULL 为 true
	}
	if leftNull || rightNull {
		return false, nil // NULL = value 为 false
	}
	return compareValuesForEquality(left, right)
}

// EvaluateWithNull 提供公开接口，用于聚合函数调用
func (e *Expression) EvaluateWithNull(data map[string]interface{}) (float64, bool, error) {
	if e.useExprLang {
		// expr-lang不支持NULL，回退到原有逻辑
		result, err := e.evaluateWithExprLang(data)
		return result, false, err
	}
	return evaluateNodeWithNull(e.Root, data)
}

// EvaluateValueWithNull 评估表达式并返回任意类型的值，支持NULL
func (e *Expression) EvaluateValueWithNull(data map[string]interface{}) (interface{}, bool, error) {
	if e.useExprLang {
		// expr-lang不支持NULL，回退到原有逻辑
		result, err := e.evaluateWithExprLang(data)
		return result, false, err
	}
	return evaluateNodeValueWithNull(e.Root, data)
}
