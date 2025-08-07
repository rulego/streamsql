package expr

import (
	"fmt"
	"strings"
)

// ParseExpression parses expression token list to AST
func ParseExpression(tokens []string) (*ExprNode, error) {
	return parseExpression(tokens)
}

// parseExpression parses expression token list to AST
func parseExpression(tokens []string) (*ExprNode, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty token list")
	}

	// Handle CASE expression
	if len(tokens) > 0 && strings.ToUpper(tokens[0]) == "CASE" {
		node, _, err := parseCaseExpression(tokens)
		return node, err
	}

	node, _, err := parseOrExpression(tokens)
	return node, err
}

// parseOrExpression parses OR expression
func parseOrExpression(tokens []string) (*ExprNode, []string, error) {
	left, remaining, err := parseAndExpression(tokens)
	if err != nil {
		return nil, nil, err
	}

	for len(remaining) > 0 && strings.ToUpper(remaining[0]) == "OR" {
		right, newRemaining, err := parseAndExpression(remaining[1:])
		if err != nil {
			return nil, nil, err
		}

		left = &ExprNode{
			Type:  TypeOperator,
			Value: "OR",
			Left:  left,
			Right: right,
		}
		remaining = newRemaining
	}

	return left, remaining, nil
}

// parseAndExpression parses AND expression
func parseAndExpression(tokens []string) (*ExprNode, []string, error) {
	left, remaining, err := parseComparisonExpression(tokens)
	if err != nil {
		return nil, nil, err
	}

	for len(remaining) > 0 && strings.ToUpper(remaining[0]) == "AND" {
		right, newRemaining, err := parseComparisonExpression(remaining[1:])
		if err != nil {
			return nil, nil, err
		}

		left = &ExprNode{
			Type:  TypeOperator,
			Value: "AND",
			Left:  left,
			Right: right,
		}
		remaining = newRemaining
	}

	return left, remaining, nil
}

// parseComparisonExpression parses comparison expression
func parseComparisonExpression(tokens []string) (*ExprNode, []string, error) {
	left, remaining, err := parseArithmeticExpression(tokens)
	if err != nil {
		return nil, nil, err
	}

	// Check IS NOT operator (two tokens)
	if len(remaining) >= 2 && strings.ToUpper(remaining[0]) == "IS" && strings.ToUpper(remaining[1]) == "NOT" {
		op := "IS NOT"
		right, newRemaining, err := parseArithmeticExpression(remaining[2:])
		if err != nil {
			return nil, nil, err
		}

		return &ExprNode{
			Type:  TypeOperator,
			Value: op,
			Left:  left,
			Right: right,
		}, newRemaining, nil
	}

	// Check single token comparison operators
	if len(remaining) > 0 && isComparisonOperator(remaining[0]) {
		op := remaining[0]
		right, newRemaining, err := parseArithmeticExpression(remaining[1:])
		if err != nil {
			return nil, nil, err
		}

		return &ExprNode{
			Type:  TypeOperator,
			Value: op,
			Left:  left,
			Right: right,
		}, newRemaining, nil
	}

	return left, remaining, nil
}

// parseArithmeticExpression parses arithmetic expression
func parseArithmeticExpression(tokens []string) (*ExprNode, []string, error) {
	left, remaining, err := parseTermExpression(tokens)
	if err != nil {
		return nil, nil, err
	}

	for len(remaining) > 0 && (remaining[0] == "+" || remaining[0] == "-") {
		op := remaining[0]
		right, newRemaining, err := parseTermExpression(remaining[1:])
		if err != nil {
			return nil, nil, err
		}

		left = &ExprNode{
			Type:  TypeOperator,
			Value: op,
			Left:  left,
			Right: right,
		}
		remaining = newRemaining
	}

	return left, remaining, nil
}

// parseTermExpression parses term expression (multiply, divide, modulo)
func parseTermExpression(tokens []string) (*ExprNode, []string, error) {
	left, remaining, err := parsePowerExpression(tokens)
	if err != nil {
		return nil, nil, err
	}

	for len(remaining) > 0 && (remaining[0] == "*" || remaining[0] == "/" || remaining[0] == "%") {
		op := remaining[0]
		right, newRemaining, err := parsePowerExpression(remaining[1:])
		if err != nil {
			return nil, nil, err
		}

		left = &ExprNode{
			Type:  TypeOperator,
			Value: op,
			Left:  left,
			Right: right,
		}
		remaining = newRemaining
	}

	return left, remaining, nil
}

// parsePowerExpression parses power expression
func parsePowerExpression(tokens []string) (*ExprNode, []string, error) {
	left, remaining, err := parseUnaryExpression(tokens)
	if err != nil {
		return nil, nil, err
	}

	if len(remaining) > 0 && remaining[0] == "^" {
		right, newRemaining, err := parsePowerExpression(remaining[1:]) // Right associative
		if err != nil {
			return nil, nil, err
		}

		return &ExprNode{
			Type:  TypeOperator,
			Value: "^",
			Left:  left,
			Right: right,
		}, newRemaining, nil
	}

	return left, remaining, nil
}

// parseUnaryExpression parses unary expression
func parseUnaryExpression(tokens []string) (*ExprNode, []string, error) {
	if len(tokens) == 0 {
		return nil, nil, fmt.Errorf("unexpected end of expression")
	}

	// Handle unary minus
	if tokens[0] == "-" {
		operand, remaining, err := parseUnaryExpression(tokens[1:])
		if err != nil {
			return nil, nil, err
		}

		return &ExprNode{
			Type:  TypeOperator,
			Value: "-",
			Left: &ExprNode{
				Type:  TypeNumber,
				Value: "0",
			},
			Right: operand,
		}, remaining, nil
	}

	return parsePrimaryExpression(tokens)
}

// parsePrimaryExpression parses primary expression
func parsePrimaryExpression(tokens []string) (*ExprNode, []string, error) {
	if len(tokens) == 0 {
		return nil, nil, fmt.Errorf("unexpected end of expression")
	}

	token := tokens[0]

	// Handle parentheses
	if token == "(" {
		expr, remaining, err := parseOrExpression(tokens[1:])
		if err != nil {
			return nil, nil, err
		}

		if len(remaining) == 0 || remaining[0] != ")" {
			return nil, nil, fmt.Errorf("missing closing parenthesis")
		}

		// Create parenthesis node
		return &ExprNode{
			Type: TypeParenthesis,
			Left: expr,
		}, remaining[1:], nil
	}

	// Handle numbers
	if isNumber(token) {
		return &ExprNode{
			Type:  TypeNumber,
			Value: token,
		}, tokens[1:], nil
	}

	// Handle string literals
	if isStringLiteral(token) {
		return &ExprNode{
			Type:  TypeString,
			Value: token,
		}, tokens[1:], nil
	}

	// Handle function calls
	if len(tokens) > 1 && tokens[1] == "(" {
		return parseFunctionCall(tokens)
	}

	// Check for invalid function calls (identifier followed by non-parenthesis token)
	// But exclude keywords in CASE expressions
	if isIdentifier(token) && len(tokens) > 1 && tokens[1] != "(" && !isOperator(tokens[1]) && tokens[1] != ")" && tokens[1] != "," {
		// Allow keywords in CASE expressions
		nextToken := strings.ToUpper(tokens[1])
		if nextToken != "WHEN" && nextToken != "THEN" && nextToken != "ELSE" && nextToken != "END" {
			return nil, nil, fmt.Errorf("invalid function call")
		}
	}

	// Handle field references
	if isIdentifier(token) || (len(token) >= 2 && token[0] == '`' && token[len(token)-1] == '`') {
		return &ExprNode{
			Type:  TypeField,
			Value: token,
		}, tokens[1:], nil
	}

	return nil, nil, fmt.Errorf("unexpected token: %s", token)
}

// parseFunctionCall parses function call
func parseFunctionCall(tokens []string) (*ExprNode, []string, error) {
	if len(tokens) < 2 || tokens[1] != "(" {
		return nil, nil, fmt.Errorf("invalid function call")
	}

	funcName := tokens[0]
	remaining := tokens[2:] // Skip function name and opening parenthesis

	var args []*ExprNode

	// Handle empty parameter list
	if len(remaining) > 0 && remaining[0] == ")" {
		return &ExprNode{
			Type:  TypeFunction,
			Value: funcName,
			Args:  args,
		}, remaining[1:], nil
	}

	// Parse arguments
	for {
		arg, newRemaining, err := parseOrExpression(remaining)
		if err != nil {
			return nil, nil, err
		}

		args = append(args, arg)
		remaining = newRemaining

		if len(remaining) == 0 {
			return nil, nil, fmt.Errorf("missing closing parenthesis in function call")
		}

		if remaining[0] == ")" {
			break
		}

		if remaining[0] != "," {
			return nil, nil, fmt.Errorf("expected ',' or ')' in function call")
		}

		remaining = remaining[1:] // Skip comma
	}

	return &ExprNode{
		Type:  TypeFunction,
		Value: funcName,
		Args:  args,
	}, remaining[1:], nil // Skip closing parenthesis
}
