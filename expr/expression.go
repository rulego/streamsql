package expr

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rulego/streamsql/functions"
)

// Expression types - expression type constants
const (
	TypeNumber      = "number"      // Number constant
	TypeField       = "field"       // Field reference
	TypeOperator    = "operator"    // Operator
	TypeFunction    = "function"    // Function call
	TypeParenthesis = "parenthesis" // Parenthesis
	TypeCase        = "case"        // CASE expression
	TypeString      = "string"      // String constant
)

// WhenClause represents a WHEN clause in a CASE expression
type WhenClause struct {
	Condition *ExprNode // WHEN condition
	Result    *ExprNode // THEN result
}

// CaseExpression represents the structure of a CASE expression
type CaseExpression struct {
	Value       *ExprNode    // Expression after CASE (simple CASE)
	WhenClauses []WhenClause // List of WHEN clauses
	ElseResult  *ExprNode    // ELSE expression
}

// ExprNode represents an expression node
type ExprNode struct {
	Type  string      // Node type
	Value string      // Node value
	Left  *ExprNode   // Left child node
	Right *ExprNode   // Right child node
	Args  []*ExprNode // Function argument list

	// Fields specific to CASE expressions
	CaseExpr *CaseExpression // CASE expression structure
}

// Expression represents a computable expression
type Expression struct {
	Root               *ExprNode // Expression root node
	useExprLang        bool      // Whether to use expr-lang/expr
	exprLangExpression string    // expr-lang expression string
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
	// Check for empty expression
	trimmed := strings.TrimSpace(exprStr)
	if trimmed == "" {
		return fmt.Errorf("empty expression")
	}

	// Check for mismatched parentheses
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

	// Check for consecutive operators
	operators := []string{"+", "-", "*", "/", "%", "^", "=", "!=", "<>", ">", "<", ">=", "<="}
	for _, op1 := range operators {
		for _, op2 := range operators {
			if strings.Contains(trimmed, " "+op1+" "+op2+" ") {
				return fmt.Errorf("consecutive operators")
			}
		}
	}

	// Check if expression starts or ends with operator
	for _, op := range operators {
		if strings.HasPrefix(trimmed, op+" ") {
			return fmt.Errorf("expression cannot start with operator")
		}
		if strings.HasSuffix(trimmed, " "+op) {
			return fmt.Errorf("expression cannot end with operator")
		}
	}

	// Check for invalid characters
	for i, ch := range trimmed {
		// Allowed characters: letters, numbers, operators, parentheses, dots, underscores, spaces, quotes
		if !isValidChar(ch) {
			return fmt.Errorf("invalid character '%c' at position %d", ch, i)
		}
	}

	return nil
}

// isValidChar checks if a character is valid
func isValidChar(ch rune) bool {
	// Letters and numbers
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
	case '$': // Dollar sign (for JSON paths, etc.)
		return true
	case '`': // Backtick (for identifiers)
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
		// Remove backticks (if present)
		fieldName := node.Value
		if len(fieldName) >= 2 && fieldName[0] == '`' && fieldName[len(fieldName)-1] == '`' {
			fieldName = fieldName[1 : len(fieldName)-1]
		}
		fields[fieldName] = true
	}

	// Recursively collect fields from child nodes
	collectFields(node.Left, fields)
	collectFields(node.Right, fields)

	// Collect fields from function arguments
	for _, arg := range node.Args {
		collectFields(arg, fields)
	}

	// Collect fields from CASE expression
	if node.CaseExpr != nil {
		collectFields(node.CaseExpr.Value, fields)
		collectFields(node.CaseExpr.ElseResult, fields)
		for _, whenClause := range node.CaseExpr.WhenClauses {
			collectFields(whenClause.Condition, fields)
			collectFields(whenClause.Result, fields)
		}
	}
}

// EvaluateBool calculates the boolean value of the expression
func (e *Expression) EvaluateBool(data map[string]interface{}) (bool, error) {
	if e.useExprLang {
		// For expr-lang expressions, calculate numeric value first then convert to boolean
		result, err := e.evaluateWithExprLang(data)
		if err != nil {
			return false, err
		}
		return result != 0, nil
	}
	return evaluateBoolNode(e.Root, data)
}

// EvaluateWithNull provides public interface for aggregate function calls, supports NULL value handling
func (e *Expression) EvaluateWithNull(data map[string]interface{}) (float64, bool, error) {
	if e.useExprLang {
		// expr-lang doesn't support NULL, fallback to original logic
		result, err := e.evaluateWithExprLang(data)
		return result, false, err
	}
	return evaluateNodeWithNull(e.Root, data)
}

// EvaluateValueWithNull evaluates expression and returns value of any type, supports NULL
func (e *Expression) EvaluateValueWithNull(data map[string]interface{}) (interface{}, bool, error) {
	if e.useExprLang {
		// expr-lang doesn't support NULL, fallback to original logic
		result, err := e.evaluateWithExprLang(data)
		return result, false, err
	}
	return evaluateNodeValueWithNull(e.Root, data)
}
