package expr

import (
	"fmt"
	"strings"

	"github.com/rulego/streamsql/functions"
)

// validateExpression validates the validity of an expression
func validateExpression(node *ExprNode) error {
	if node == nil {
		return fmt.Errorf("expression node is nil")
	}

	switch node.Type {
	case TypeNumber:
		return validateNumberNode(node)
	case TypeString:
		return validateStringNode(node)
	case TypeField:
		return validateFieldNode(node)
	case TypeOperator:
		return validateOperatorNode(node)
	case TypeFunction:
		return validateFunctionNode(node)
	case TypeCase:
		return validateCaseNode(node)
	case TypeParenthesis:
		return validateParenthesisNode(node)
	default:
		return fmt.Errorf("unknown node type: %s", node.Type)
	}
}

// validateNumberNode validates a number node
func validateNumberNode(node *ExprNode) error {
	if node.Value == "" {
		return fmt.Errorf("number node has empty value")
	}

	// Check if it's a valid number format
	if !isNumber(node.Value) {
		return fmt.Errorf("invalid number format: %s", node.Value)
	}

	return nil
}

// validateStringNode validates a string node
func validateStringNode(node *ExprNode) error {
	if node.Value == "" {
		return fmt.Errorf("string node has empty value")
	}

	// Check if string is properly quoted
	if !isStringLiteral(node.Value) {
		return fmt.Errorf("invalid string literal format: %s", node.Value)
	}

	return nil
}

// validateFieldNode validates a field node
func validateFieldNode(node *ExprNode) error {
	if node.Value == "" {
		return fmt.Errorf("field node has empty value")
	}

	// Check if field name is a valid identifier
	fieldName := node.Value

	if !isValidFieldName(fieldName) {
		return fmt.Errorf("invalid field name: %s", node.Value)
	}

	return nil
}

// validateOperatorNode validates an operator node
func validateOperatorNode(node *ExprNode) error {
	if node.Value == "" {
		return fmt.Errorf("operator node has empty value")
	}

	// Check if it's a valid operator
	if !isOperator(node.Value) && !isComparisonOperator(node.Value) {
		return fmt.Errorf("invalid operator: %s", node.Value)
	}

	// Check if it's a unary operator
	if isUnaryOperator(node.Value) {
		// Unary operators only need left operand
		if node.Left == nil {
			return fmt.Errorf("unary operator '%s' missing operand", node.Value)
		}
		// Unary operators should not have right operand
		if node.Right != nil {
			return fmt.Errorf("unary operator '%s' should not have right operand", node.Value)
		}
		// Validate left operand
		return validateExpression(node.Left)
	}

	// Binary operators need both left and right operands
	if node.Left == nil {
		return fmt.Errorf("operator %s missing left operand", node.Value)
	}

	if node.Right == nil {
		return fmt.Errorf("operator %s missing right operand", node.Value)
	}

	// Recursively validate operands
	if err := validateExpression(node.Left); err != nil {
		return fmt.Errorf("invalid left operand for operator %s: %v", node.Value, err)
	}

	if err := validateExpression(node.Right); err != nil {
		return fmt.Errorf("invalid right operand for operator %s: %v", node.Value, err)
	}

	return nil
}

// validateFunctionNode validates a function node
// Use unified function registration system for validation
func validateFunctionNode(node *ExprNode) error {
	if node.Value == "" {
		return fmt.Errorf("function node has empty value")
	}

	// Check if function exists in registration system (using lowercase name)
	fn, exists := functions.Get(strings.ToLower(node.Value))
	if !exists {
		return fmt.Errorf("unknown function: %s", node.Value)
	}

	// Use function's own validation logic for basic validation
	// Create temporary argument array for validating argument count
	tempArgs := make([]interface{}, len(node.Args))
	if err := fn.Validate(tempArgs); err != nil {
		return fmt.Errorf("function %s validation failed: %v", node.Value, err)
	}

	// Validate argument expressions
	return validateFunctionArgs(node)
}

// validateFunctionArgs validates function arguments
func validateFunctionArgs(node *ExprNode) error {
	for i, arg := range node.Args {
		if err := validateExpression(arg); err != nil {
			return fmt.Errorf("invalid argument %d for function %s: %v", i+1, node.Value, err)
		}
	}
	return nil
}

// validateParenthesisNode validates a parenthesis node
func validateParenthesisNode(node *ExprNode) error {
	// Parenthesis node should have a Left child containing the inner expression
	if node.Left == nil {
		return fmt.Errorf("parenthesis node missing inner expression")
	}

	// Validate the inner expression
	return validateExpression(node.Left)
}

// validateCaseNode validates a CASE expression node
func validateCaseNode(node *ExprNode) error {
	if node.CaseExpr == nil {
		return fmt.Errorf("CASE expression is missing")
	}

	caseExpr := node.CaseExpr

	// Validate the value part of CASE expression (if it's a simple CASE)
	if caseExpr.Value != nil {
		if err := validateExpression(caseExpr.Value); err != nil {
			return fmt.Errorf("invalid CASE value expression: %v", err)
		}
	}

	// Validate WHEN clauses
	if len(caseExpr.WhenClauses) == 0 {
		return fmt.Errorf("CASE expression must have at least one WHEN clause")
	}

	for i, whenClause := range caseExpr.WhenClauses {
		if err := validateExpression(whenClause.Condition); err != nil {
			return fmt.Errorf("invalid WHEN condition %d: %v", i+1, err)
		}
		if err := validateExpression(whenClause.Result); err != nil {
			return fmt.Errorf("invalid THEN result %d: %v", i+1, err)
		}
	}

	// Validate ELSE clause (if exists)
	if caseExpr.ElseResult != nil {
		if err := validateExpression(caseExpr.ElseResult); err != nil {
			return fmt.Errorf("invalid ELSE expression: %v", err)
		}
	}

	return nil
}

// isValidFieldName checks if it's a valid field name
// isValidFieldName validates if field name is valid
// Supports normal identifiers and backtick-enclosed field names
func isValidFieldName(name string) bool {
	if name == "" {
		return false
	}

	// If it's a backtick-enclosed field name, check if backticks are properly closed
	if len(name) >= 2 && name[0] == '`' && name[len(name)-1] == '`' {
		// Content inside backticks can contain any character (except backticks themselves)
		inner := name[1 : len(name)-1]
		if inner == "" {
			return false // Empty backticks not allowed
		}
		// Check if there are backticks inside
		for _, r := range inner {
			if r == '`' {
				return false
			}
		}
		return true
	}

	// Normal field name: can only contain letters, numbers, underscores (dots not allowed)
	for i, r := range name {
		// For non-ASCII characters, return false directly
		if r > 127 {
			return false
		}
		ch := byte(r)
		if i == 0 {
			// First character must be letter or underscore
			if !isLetter(ch) && r != '_' {
				return false
			}
		} else {
			// Subsequent characters can be letters, numbers, underscores
			if !isLetter(ch) && !isDigit(ch) && r != '_' {
				return false
			}
		}
	}

	return true
}

// validateTokens validates the validity of token list
func validateTokens(tokens []string) error {
	if len(tokens) == 0 {
		return fmt.Errorf("empty token list")
	}

	// Check parentheses matching
	if err := validateParentheses(tokens); err != nil {
		return err
	}

	// Check order of operators and operands
	if err := validateTokenOrder(tokens); err != nil {
		return err
	}

	return nil
}

// validateParentheses validates parentheses matching
func validateParentheses(tokens []string) error {
	stack := 0
	for i, token := range tokens {
		if token == "(" {
			stack++
		} else if token == ")" {
			stack--
			if stack < 0 {
				return fmt.Errorf("unmatched closing parenthesis at position %d", i)
			}
		}
	}

	if stack > 0 {
		return fmt.Errorf("unmatched opening parenthesis")
	}

	return nil
}

// validateTokenOrder validates token order
func validateTokenOrder(tokens []string) error {
	if len(tokens) == 0 {
		return nil
	}

	// Check cannot start with operator (except unary operators)
	firstToken := tokens[0]
	if isOperator(firstToken) && !isUnaryOperator(firstToken) {
		return fmt.Errorf("expression cannot start with operator: %s", firstToken)
	}

	// Check cannot end with operator
	lastToken := tokens[len(tokens)-1]
	if isOperator(lastToken) {
		return fmt.Errorf("expression cannot end with operator: %s", lastToken)
	}

	// Check consecutive operators and consecutive operands
	for i := 0; i < len(tokens)-1; i++ {
		current := tokens[i]
		next := tokens[i+1]

		// Check consecutive operators
		if isOperator(current) && isOperator(next) {
			// Allowed combination: operator followed by unary operator
			if !isUnaryOperator(next) {
				return fmt.Errorf("consecutive operators not allowed: %s %s at position %d", current, next, i)
			}
		}

		// Check consecutive operands (two non-operator, non-parenthesis tokens adjacent)
		// Special handling for CASE expression keywords
		if !isOperator(current) && !isOperator(next) &&
			current != "(" && current != ")" && next != "(" && next != ")" &&
			current != "," && next != "," &&
			!isCaseKeyword(current) && !isCaseKeyword(next) {
			return fmt.Errorf("consecutive operands not allowed: %s %s at position %d", current, next, i)
		}
	}

	return nil
}

// isCaseKeyword checks if it's a CASE expression keyword
func isCaseKeyword(token string) bool {
	switch strings.ToUpper(token) {
	case "CASE", "WHEN", "THEN", "ELSE", "END":
		return true
	default:
		return false
	}
}

// validateSyntax validates expression syntax
func validateSyntax(expr string) error {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return fmt.Errorf("empty expression")
	}

	// Check basic syntax errors
	if strings.Contains(trimmed, "()") {
		return fmt.Errorf("empty parentheses not allowed")
	}

	// Check mismatched parentheses
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

	// Check consecutive operators
	operators := []string{"+", "-", "*", "/", "%", "^", "=", "!=", "<>", ">", "<", ">=", "<="}
	for _, op1 := range operators {
		for _, op2 := range operators {
			// Check consecutive operators (separated by spaces)
			if strings.Contains(trimmed, " "+op1+" "+op2+" ") {
				return fmt.Errorf("consecutive operators")
			}
			// Check directly adjacent operators (except allowed combinations)
			if op1 != op2 && strings.Contains(trimmed, op1+op2) {
				// Allow certain combinations like ">=, <=, !=, <>"
				allowed := []string{">=", "<=", "!=", "<>"}
				combination := op1 + op2
				isAllowed := false
				for _, allowedOp := range allowed {
					if combination == allowedOp {
						isAllowed = true
						break
					}
				}
				if !isAllowed {
					return fmt.Errorf("consecutive operators")
				}
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

	return nil
}

// ValidateExpression public interface: validates expression string
func ValidateExpression(expr string) error {
	// First validate syntax
	if err := validateSyntax(expr); err != nil {
		return err
	}

	// Tokenize
	tokens, err := tokenize(expr)
	if err != nil {
		return fmt.Errorf("tokenization error: %v", err)
	}

	// Validate tokens
	if err := validateTokens(tokens); err != nil {
		return fmt.Errorf("token validation error: %v", err)
	}

	// Parse to AST
	node, err := parseExpression(tokens)
	if err != nil {
		return fmt.Errorf("parsing error: %v", err)
	}

	// Validate AST
	if err := validateExpression(node); err != nil {
		return fmt.Errorf("expression validation error: %v", err)
	}

	return nil
}
