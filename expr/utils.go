package expr

import (
	"fmt"
	"github.com/rulego/streamsql/utils/cast"
	"strings"
)

// unquoteString removes quotes from both ends of string
func unquoteString(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// unquoteBacktick removes backticks from both ends of string
func unquoteBacktick(s string) string {
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
}

// getNodeType gets node type
func getNodeType(node *ExprNode) string {
	if node == nil {
		return "nil"
	}
	return node.Type
}

// getNodeValue gets node value
func getNodeValue(node *ExprNode) string {
	if node == nil {
		return ""
	}
	return node.Value
}

// setNodeValue sets node value
func setNodeValue(node *ExprNode, value string) {
	if node != nil {
		node.Value = value
	}
}

// isArithmeticOperator checks if it's an arithmetic operator
func isArithmeticOperator(op string) bool {
	switch op {
	case "+", "-", "*", "/", "%", "^":
		return true
	default:
		return false
	}
}

// isLogicalOperator checks if it's a logical operator
func isLogicalOperator(op string) bool {
	switch strings.ToUpper(op) {
	case "AND", "OR", "NOT":
		return true
	default:
		return false
	}
}

// isUnaryOperator checks if it's a unary operator
func isUnaryOperator(op string) bool {
	switch strings.ToUpper(op) {
	case "NOT":
		return true
	default:
		return false
	}
}

// isKeyword checks if it's a keyword
func isKeyword(word string) bool {
	switch strings.ToUpper(word) {
	case "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "LIKE", "IS", "NULL", "TRUE", "FALSE":
		return true
	default:
		return false
	}
}

// normalizeIdentifier normalizes identifier (convert to lowercase)
func normalizeIdentifier(identifier string) string {
	return strings.ToLower(identifier)
}

// convertToFloat converts any type to float64
func convertToFloat(value interface{}) (float64, error) {
	return cast.ToFloat64E(value)
}

// convertToFloatSafe safely converts any type to float64, returns conversion result and success status
func convertToFloatSafe(value interface{}) (float64, bool) {
	result, err := convertToFloat(value)
	return result, err == nil
}

// convertToBool converts any type to boolean
func convertToBool(value interface{}) bool {
	return cast.ToBool(value)
}

// getOperatorPrecedence gets operator precedence
func getOperatorPrecedence(op string) int {
	switch op {
	case "OR":
		return 1
	case "AND":
		return 2
	case "NOT":
		return 3
	case "=", "==", "!=", "<>", ">", "<", ">=", "<=", "LIKE", "NOT LIKE", "IS", "IS NOT":
		return 4
	case "+", "-":
		return 5
	case "*", "/", "%":
		return 6
	case "^":
		return 7
	default:
		return 0
	}
}

// isRightAssociative checks if operator is right associative
func isRightAssociative(op string) bool {
	// Only power operator is right associative
	return op == "^"
}

// parseFunction parses function call (test helper function)
func parseFunction(tokens []string, pos int) (*ExprNode, int, error) {
	if pos >= len(tokens) {
		return nil, pos, fmt.Errorf("unexpected end of tokens")
	}

	// Call existing parseFunctionCall function
	node, remaining, err := parseFunctionCall(tokens[pos:])
	if err != nil {
		return nil, pos, err
	}

	// Calculate new position
	newPos := len(tokens) - len(remaining)
	return node, newPos, nil
}

// formatError formats error message
func formatError(message string, args ...interface{}) error {
	if len(args) == 0 {
		return fmt.Errorf("%s", message)
	}
	return fmt.Errorf(message, args...)
}

// copyNode deep copies expression node
func copyNode(node *ExprNode) *ExprNode {
	if node == nil {
		return nil
	}

	newNode := &ExprNode{
		Type:  node.Type,
		Value: node.Value,
		Left:  copyNode(node.Left),
		Right: copyNode(node.Right),
	}

	// Copy function arguments
	if len(node.Args) > 0 {
		newNode.Args = make([]*ExprNode, len(node.Args))
		for i, arg := range node.Args {
			newNode.Args[i] = copyNode(arg)
		}
	}

	// Copy CASE expression
	if node.CaseExpr != nil {
		newNode.CaseExpr = &CaseExpression{
			Value:      copyNode(node.CaseExpr.Value),
			ElseResult: copyNode(node.CaseExpr.ElseResult),
		}

		// Copy WHEN clauses
		if len(node.CaseExpr.WhenClauses) > 0 {
			newNode.CaseExpr.WhenClauses = make([]WhenClause, len(node.CaseExpr.WhenClauses))
			for i, whenClause := range node.CaseExpr.WhenClauses {
				newNode.CaseExpr.WhenClauses[i] = WhenClause{
					Condition: copyNode(whenClause.Condition),
					Result:    copyNode(whenClause.Result),
				}
			}
		}
	}

	return newNode
}

// nodeToString converts expression node to string representation
func nodeToString(node *ExprNode) string {
	if node == nil {
		return "<nil>"
	}

	switch node.Type {
	case TypeNumber, TypeString, TypeField:
		return node.Value
	case TypeOperator:
		left := nodeToString(node.Left)
		right := nodeToString(node.Right)
		return fmt.Sprintf("(%s %s %s)", left, node.Value, right)
	case TypeFunction:
		args := make([]string, len(node.Args))
		for i, arg := range node.Args {
			args[i] = nodeToString(arg)
		}
		return fmt.Sprintf("%s(%s)", node.Value, strings.Join(args, ", "))
	case TypeCase:
		return caseExprToString(node.CaseExpr)
	default:
		return fmt.Sprintf("<%s:%s>", node.Type, node.Value)
	}
}

// caseExprToString converts CASE expression to string representation
func caseExprToString(caseExpr *CaseExpression) string {
	if caseExpr == nil {
		return "<nil case>"
	}

	var result strings.Builder
	result.WriteString("CASE")

	if caseExpr.Value != nil {
		result.WriteString(" ")
		result.WriteString(nodeToString(caseExpr.Value))
	}

	for _, whenClause := range caseExpr.WhenClauses {
		result.WriteString(" WHEN ")
		result.WriteString(nodeToString(whenClause.Condition))
		result.WriteString(" THEN ")
		result.WriteString(nodeToString(whenClause.Result))
	}

	if caseExpr.ElseResult != nil {
		result.WriteString(" ELSE ")
		result.WriteString(nodeToString(caseExpr.ElseResult))
	}

	result.WriteString(" END")
	return result.String()
}
