package expr

import (
	"fmt"
	"strings"
)

// parseCaseExpression parses CASE expression
func parseCaseExpression(tokens []string) (*ExprNode, []string, error) {
	if len(tokens) == 0 || strings.ToUpper(tokens[0]) != "CASE" {
		return nil, nil, fmt.Errorf("expected CASE keyword")
	}

	remaining := tokens[1:]
	caseExpr := &CaseExpression{}

	// Check if it's a simple CASE expression (CASE expr WHEN value THEN result)
	if len(remaining) > 0 && strings.ToUpper(remaining[0]) != "WHEN" {
		// Simple CASE expression
		value, newRemaining, err := parseOrExpression(remaining)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing CASE expression: %v", err)
		}
		caseExpr.Value = value
		remaining = newRemaining
	}

	// Parse WHEN clauses
	for len(remaining) > 0 && strings.ToUpper(remaining[0]) == "WHEN" {
		remaining = remaining[1:] // Skip WHEN

		// Parse WHEN condition
		condition, newRemaining, err := parseOrExpression(remaining)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing WHEN condition: %v", err)
		}
		remaining = newRemaining

		// Check THEN keyword
		if len(remaining) == 0 || strings.ToUpper(remaining[0]) != "THEN" {
			return nil, nil, fmt.Errorf("expected THEN after WHEN condition")
		}
		remaining = remaining[1:] // Skip THEN

		// Parse THEN result
		result, newRemaining, err := parseOrExpression(remaining)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing THEN result: %v", err)
		}
		remaining = newRemaining

		// Add WHEN clause
		caseExpr.WhenClauses = append(caseExpr.WhenClauses, WhenClause{
			Condition: condition,
			Result:    result,
		})
	}

	// Parse optional ELSE clause
	if len(remaining) > 0 && strings.ToUpper(remaining[0]) == "ELSE" {
		remaining = remaining[1:] // Skip ELSE

		elseExpr, newRemaining, err := parseOrExpression(remaining)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing ELSE expression: %v", err)
		}
		caseExpr.ElseResult = elseExpr
		remaining = newRemaining
	}

	// Check END keyword
	if len(remaining) == 0 || strings.ToUpper(remaining[0]) != "END" {
		return nil, nil, fmt.Errorf("expected END to close CASE expression")
	}

	// Create ExprNode containing CaseExpression
	caseNode := &ExprNode{
		Type:     TypeCase,
		CaseExpr: caseExpr,
	}

	return caseNode, remaining[1:], nil
}

// evaluateCaseExpression evaluates the value of CASE expression
func evaluateCaseExpression(node *ExprNode, data map[string]interface{}) (float64, error) {
	if node.Type != TypeCase {
		return 0, fmt.Errorf("not a CASE expression")
	}

	if node.CaseExpr == nil {
		return 0, fmt.Errorf("invalid CASE expression")
	}

	// Simple CASE expression: CASE expr WHEN value THEN result
	if node.CaseExpr.Value != nil {
		return evaluateSimpleCaseExpression(node, data)
	}

	// Search CASE expression: CASE WHEN condition THEN result
	return evaluateSearchCaseExpression(node, data)
}

// evaluateSimpleCaseExpression evaluates simple CASE expression
func evaluateSimpleCaseExpression(node *ExprNode, data map[string]interface{}) (float64, error) {
	caseExpr := node.CaseExpr
	if caseExpr == nil {
		return 0, fmt.Errorf("invalid CASE expression")
	}

	// Evaluate CASE expression value
	caseValue, err := evaluateNodeValue(caseExpr.Value, data)
	if err != nil {
		return 0, err
	}

	// Iterate through WHEN clauses
	for _, whenClause := range caseExpr.WhenClauses {
		// Evaluate WHEN value
		whenValue, err := evaluateNodeValue(whenClause.Condition, data)
		if err != nil {
			return 0, err
		}

		// Compare values
		if compareValuesForEquality(caseValue, whenValue) {
			// Evaluate and return THEN result
			return evaluateNode(whenClause.Result, data)
		}
	}

	// If no matching WHEN clause, evaluate ELSE expression
	if caseExpr.ElseResult != nil {
		return evaluateNode(caseExpr.ElseResult, data)
	}

	// If no ELSE clause, return NULL (return 0 here)
	return 0, nil
}

// evaluateSearchCaseExpression evaluates search CASE expression
func evaluateSearchCaseExpression(node *ExprNode, data map[string]interface{}) (float64, error) {
	caseExpr := node.CaseExpr
	if caseExpr == nil {
		return 0, fmt.Errorf("invalid CASE expression")
	}

	// Iterate through WHEN clauses
	for _, whenClause := range caseExpr.WhenClauses {
		// Evaluate WHEN condition - use boolean evaluation to handle logical operators
		conditionResult, err := evaluateBoolNode(whenClause.Condition, data)
		if err != nil {
			return 0, err
		}

		// If condition is true, return THEN result
		if conditionResult {
			return evaluateNode(whenClause.Result, data)
		}
	}

	// If no matching WHEN clause, evaluate ELSE expression
	if caseExpr.ElseResult != nil {
		return evaluateNode(caseExpr.ElseResult, data)
	}

	// If no ELSE clause, return NULL (return 0 here)
	return 0, nil
}

// evaluateCaseExpressionWithNull evaluates CASE expression with NULL value support
func evaluateCaseExpressionWithNull(node *ExprNode, data map[string]interface{}) (interface{}, bool, error) {
	if node.Type != TypeCase {
		return nil, false, fmt.Errorf("not a CASE expression")
	}

	caseExpr := node.CaseExpr
	if caseExpr == nil {
		return nil, false, fmt.Errorf("invalid CASE expression")
	}

	// Simple CASE expression: CASE expr WHEN value THEN result
	if caseExpr.Value != nil {
		return evaluateCaseExpressionValueWithNull(node, data)
	}

	// Search CASE expression: CASE WHEN condition THEN result
	for _, whenClause := range caseExpr.WhenClauses {
		// Evaluate WHEN condition - use boolean evaluation to handle logical operators
		conditionResult, err := evaluateBoolNode(whenClause.Condition, data)
		if err != nil {
			return nil, false, err
		}

		// If condition is true, return THEN result
		if conditionResult {
			return evaluateNodeValueWithNull(whenClause.Result, data)
		}
	}

	// If no matching WHEN clause, evaluate ELSE expression
	if caseExpr.ElseResult != nil {
		return evaluateNodeValueWithNull(caseExpr.ElseResult, data)
	}

	// If no ELSE clause, return NULL
	return nil, true, nil
}

// evaluateCaseExpressionValueWithNull evaluates simple CASE expression (with NULL support)
func evaluateCaseExpressionValueWithNull(node *ExprNode, data map[string]interface{}) (interface{}, bool, error) {
	caseExpr := node.CaseExpr
	if caseExpr == nil {
		return nil, false, fmt.Errorf("invalid CASE expression")
	}

	// Evaluate CASE expression value
	caseValue, caseIsNull, err := evaluateNodeValueWithNull(caseExpr.Value, data)
	if err != nil {
		return nil, false, err
	}

	// Iterate through WHEN clauses
	for _, whenClause := range caseExpr.WhenClauses {
		// Evaluate WHEN value
		whenValue, whenIsNull, err := evaluateNodeValueWithNull(whenClause.Condition, data)
		if err != nil {
			return nil, false, err
		}

		// Compare values (with NULL comparison support)
		if compareValuesWithNullForEquality(caseValue, caseIsNull, whenValue, whenIsNull) {
			// Evaluate and return THEN result
			return evaluateNodeValueWithNull(whenClause.Result, data)
		}
	}

	// If no matching WHEN clause, evaluate ELSE expression
	if caseExpr.ElseResult != nil {
		return evaluateNodeValueWithNull(caseExpr.ElseResult, data)
	}

	// If no ELSE clause, return NULL
	return nil, true, nil
}
