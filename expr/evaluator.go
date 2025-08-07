package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/fieldpath"
)

// evaluateNode evaluates the value of a node
func evaluateNode(node *ExprNode, data map[string]interface{}) (float64, error) {
	if node == nil {
		return 0, fmt.Errorf("null expression node")
	}

	switch node.Type {
	case TypeNumber:
		return strconv.ParseFloat(node.Value, 64)

	case TypeString:
		// Handle string type, remove quotes and try to convert to number
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1] // Remove quotes
		}

		// Try to convert to number
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f, nil
		}

		// For string comparison, return string length (temporary solution)
		return float64(len(value)), nil

	case TypeField:
		return evaluateFieldNode(node, data)

	case TypeOperator:
		return evaluateOperatorNode(node, data)

	case TypeFunction:
		return evaluateFunctionNode(node, data)

	case TypeCase:
		// Handle CASE expression
		return evaluateCaseExpression(node, data)

	case TypeParenthesis:
		// Handle parenthesis expression, directly evaluate inner expression
		return evaluateNode(node.Left, data)
	}

	return 0, fmt.Errorf("unknown node type: %s", node.Type)
}

// evaluateFieldNode evaluates the value of a field node
func evaluateFieldNode(node *ExprNode, data map[string]interface{}) (float64, error) {
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
				// Check if it's NaN
				if math.IsNaN(floatVal) {
					return 0, fmt.Errorf("field '%s' contains NaN value", fieldName)
				}
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
				// Check if it's NaN
				if math.IsNaN(floatVal) {
					return 0, fmt.Errorf("field '%s' contains NaN value", fieldName)
				}
				return floatVal, nil
			}
			// If cannot convert to number, return error
			return 0, fmt.Errorf("field '%s' value cannot be converted to number: %v", fieldName, val)
		}
	}
	return 0, fmt.Errorf("field '%s' not found", fieldName)
}

// evaluateOperatorNode evaluates the value of an operator node
func evaluateOperatorNode(node *ExprNode, data map[string]interface{}) (float64, error) {
	// Check if it's a comparison operator
	if isComparisonOperator(node.Value) {
		// For comparison operators, use evaluateNodeValue to get original type
		leftValue, err := evaluateNodeValue(node.Left, data)
		if err != nil {
			return 0, err
		}

		rightValue, err := evaluateNodeValue(node.Right, data)
		if err != nil {
			return 0, err
		}

		// Execute comparison and convert boolean to number
		result, err := compareValues(leftValue, rightValue, node.Value)
		if err != nil {
			return 0, err
		}
		if result {
			return 1.0, nil
		}
		return 0.0, nil
	}

	// For arithmetic operators, calculate numeric values
	left, err := evaluateNode(node.Left, data)
	if err != nil {
		return 0, err
	}

	right, err := evaluateNode(node.Right, data)
	if err != nil {
		return 0, err
	}

	// Check if operands are NaN
	if math.IsNaN(left) {
		return 0, fmt.Errorf("left operand is NaN")
	}
	if math.IsNaN(right) {
		return 0, fmt.Errorf("right operand is NaN")
	}

	// Execute operation
	var result float64
	switch node.Value {
	case "+":
		result = left + right
	case "-":
		result = left - right
	case "*":
		result = left * right
	case "/":
		if right == 0 {
			return 0, fmt.Errorf("division by zero")
		}
		result = left / right
	case "%":
		if right == 0 {
			return 0, fmt.Errorf("modulo by zero")
		}
		result = math.Mod(left, right)
	case "^":
		result = math.Pow(left, right)
	default:
		return 0, fmt.Errorf("unknown operator: %s", node.Value)
	}

	// Check if result is NaN
	if math.IsNaN(result) {
		return 0, fmt.Errorf("operation result is NaN")
	}

	return result, nil
}

// evaluateFunctionNode evaluates the value of a function node
// Uses unified function registration system to handle all function calls
func evaluateFunctionNode(node *ExprNode, data map[string]interface{}) (float64, error) {
	// Check if function exists in the new function registration system
	fn, exists := functions.Get(node.Value)
	if !exists {
		return 0, fmt.Errorf("unknown function: %s", node.Value)
	}

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

	// Validate arguments
	if err := fn.Validate(args); err != nil {
		return 0, err
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
		// For string results, try to convert to number, return string length if failed
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

// evaluateNodeValue evaluates the original value of a node (preserving type)
func evaluateNodeValue(node *ExprNode, data map[string]interface{}) (interface{}, error) {
	if node == nil {
		return nil, fmt.Errorf("null expression node")
	}

	switch node.Type {
	case TypeNumber:
		return strconv.ParseFloat(node.Value, 64)

	case TypeString:
		// Handle string type, remove quotes
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1] // Remove quotes
		}
		return value, nil

	case TypeField:
		return evaluateFieldValue(node, data)

	case TypeOperator:
		return evaluateOperatorValue(node, data)

	case TypeFunction:
		return evaluateFunctionValue(node, data)

	case TypeCase:
		// Handle CASE expression
		return evaluateCaseExpression(node, data)

	case TypeParenthesis:
		// Handle parenthesis expression, directly evaluate inner expression
		return evaluateNodeValue(node.Left, data)
	}

	return nil, fmt.Errorf("unknown node type: %s", node.Type)
}

// evaluateFieldValue evaluates the original value of a field
func evaluateFieldValue(node *ExprNode, data map[string]interface{}) (interface{}, error) {
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
}

// evaluateOperatorValue evaluates the original value of an operator
func evaluateOperatorValue(node *ExprNode, data map[string]interface{}) (interface{}, error) {
	// Special handling for IS and IS NOT operators
	operator := strings.ToUpper(node.Value)
	if operator == "IS" || operator == "IS NOT" {
		return evaluateIsOperator(node, data)
	}

	// Check if it's a logical operator
	if isLogicalOperator(node.Value) {
		// For logical operators, use boolean evaluation
		result, err := evaluateBoolOperator(node, data)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// Check if it's a comparison operator
	if isComparisonOperator(node.Value) {
		leftValue, err := evaluateNodeValue(node.Left, data)
		if err != nil {
			return nil, err
		}

		rightValue, err := evaluateNodeValue(node.Right, data)
		if err != nil {
			return nil, err
		}

		// Execute comparison
		return compareValues(leftValue, rightValue, node.Value)
	}

	// For arithmetic operators, use NULL-supporting evaluation
	left, leftIsNull, err := evaluateNodeValueWithNull(node.Left, data)
	if err != nil {
		return nil, err
	}

	right, rightIsNull, err := evaluateNodeValueWithNull(node.Right, data)
	if err != nil {
		return nil, err
	}

	// If any operand is NULL, result is NULL
	if leftIsNull || rightIsNull {
		return nil, nil
	}

	// Try to convert operands to numbers
	leftFloat, leftOk := convertToFloatSafe(left)
	rightFloat, rightOk := convertToFloatSafe(right)

	if !leftOk {
		return nil, fmt.Errorf("left operand cannot be converted to number: %v", left)
	}
	if !rightOk {
		return nil, fmt.Errorf("right operand cannot be converted to number: %v", right)
	}

	// Execute arithmetic operation
	var result float64
	switch node.Value {
	case "+":
		result = leftFloat + rightFloat
	case "-":
		result = leftFloat - rightFloat
	case "*":
		result = leftFloat * rightFloat
	case "/":
		if rightFloat == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		result = leftFloat / rightFloat
	case "%":
		if rightFloat == 0 {
			return nil, fmt.Errorf("modulo by zero")
		}
		result = math.Mod(leftFloat, rightFloat)
	case "^":
		result = math.Pow(leftFloat, rightFloat)
	default:
		return nil, fmt.Errorf("unknown arithmetic operator: %s", node.Value)
	}

	// Check if result is NaN
	if math.IsNaN(result) {
		return nil, fmt.Errorf("operation result is NaN")
	}

	return result, nil
}

// evaluateFunctionValue evaluates the original value of a function
// Uses unified function registration system to handle all function calls
func evaluateFunctionValue(node *ExprNode, data map[string]interface{}) (interface{}, error) {
	// Check if function exists in the new function registration system
	fn, exists := functions.Get(node.Value)
	if !exists {
		return nil, fmt.Errorf("unknown function: %s", node.Value)
	}

	// Calculate all arguments but keep original types
	args := make([]interface{}, len(node.Args))
	for i, arg := range node.Args {
		val, err := evaluateNodeValue(arg, data)
		if err != nil {
			return nil, err
		}
		args[i] = val
	}

	// Validate arguments
	if err := fn.Validate(args); err != nil {
		return nil, err
	}

	// Create function execution context
	ctx := &functions.FunctionContext{
		Data: data,
	}

	// Execute function
	return fn.Execute(ctx, args)
}

// compareValues compares two values
func compareValues(left, right interface{}, operator string) (bool, error) {
	// Handle NULL values
	if left == nil || right == nil {
		switch strings.ToUpper(operator) {
		case "IS":
			return left == right, nil
		case "IS NOT":
			return left != right, nil
		default:
			return false, nil // NULL compared with any value returns false
		}
	}

	// Try numeric comparison
	leftFloat, leftIsFloat := convertToFloatSafe(left)
	rightFloat, rightIsFloat := convertToFloatSafe(right)

	if leftIsFloat && rightIsFloat {
		return compareFloats(leftFloat, rightFloat, operator)
	}

	// Check for incompatible type comparison (one is number, one is not)
	if (leftIsFloat && !rightIsFloat) || (!leftIsFloat && rightIsFloat) {
		// For equality comparison, allow type conversion
		operatorUpper := strings.ToUpper(operator)
		if operatorUpper == "==" || operatorUpper == "=" || operatorUpper == "!=" || operatorUpper == "<>" {
			// String comparison
			leftStr := fmt.Sprintf("%v", left)
			rightStr := fmt.Sprintf("%v", right)
			return compareStrings(leftStr, rightStr, operator)
		}
		// For size comparison, return error
		return false, fmt.Errorf("cannot compare incompatible types: %T and %T", left, right)
	}

	// String comparison
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)

	return compareStrings(leftStr, rightStr, operator)
}

// compareFloats compares two floating point numbers
func compareFloats(left, right float64, operator string) (bool, error) {
	switch strings.ToUpper(operator) {
	case "==", "=":
		return left == right, nil
	case "!=", "<>":
		return left != right, nil
	case ">":
		return left > right, nil
	case "<":
		return left < right, nil
	case ">=":
		return left >= right, nil
	case "<=":
		return left <= right, nil
	default:
		return false, fmt.Errorf("unsupported numeric comparison operator: %s", operator)
	}
}

// compareStrings compares two strings
func compareStrings(left, right, operator string) (bool, error) {
	switch strings.ToUpper(operator) {
	case "==", "=":
		return left == right, nil
	case "!=", "<>":
		return left != right, nil
	case ">":
		return left > right, nil
	case "<":
		return left < right, nil
	case ">=":
		return left >= right, nil
	case "<=":
		return left <= right, nil
	case "LIKE":
		return matchLikePattern(left, right), nil
	default:
		return false, fmt.Errorf("unsupported string comparison operator: %s", operator)
	}
}

// matchLikePattern implements LIKE pattern matching
func matchLikePattern(text, pattern string) bool {
	// Simplified LIKE implementation, supports % and _ wildcards
	// % matches any character sequence, _ matches single character
	return matchPattern(text, pattern, 0, 0)
}

// matchPattern recursively matches pattern
func matchPattern(text, pattern string, textIdx, patternIdx int) bool {
	if patternIdx == len(pattern) {
		return textIdx == len(text)
	}

	if pattern[patternIdx] == '%' {
		// % matches any character sequence
		for i := textIdx; i <= len(text); i++ {
			if matchPattern(text, pattern, i, patternIdx+1) {
				return true
			}
		}
		return false
	}

	if textIdx == len(text) {
		return false
	}

	if pattern[patternIdx] == '_' || pattern[patternIdx] == text[textIdx] {
		// _ matches single character or exact character match
		return matchPattern(text, pattern, textIdx+1, patternIdx+1)
	}

	return false
}

// compareValuesForEquality compares two values for equality (for simple CASE expressions)
func compareValuesForEquality(left, right interface{}) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}

	// Try numeric comparison
	leftFloat, leftIsFloat := convertToFloatSafe(left)
	rightFloat, rightIsFloat := convertToFloatSafe(right)

	if leftIsFloat && rightIsFloat {
		return leftFloat == rightFloat
	}

	// String comparison
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)
	return leftStr == rightStr
}

// evaluateNodeWithNull evaluates node value with NULL value handling
func evaluateNodeWithNull(node *ExprNode, data map[string]interface{}) (float64, bool, error) {
	if node == nil {
		return 0, true, nil // NULL node
	}

	switch node.Type {
	case TypeNumber:
		val, err := strconv.ParseFloat(node.Value, 64)
		return val, false, err

	case TypeField:
		// Handle backtick identifiers
		fieldName := node.Value
		if len(fieldName) >= 2 && fieldName[0] == '`' && fieldName[len(fieldName)-1] == '`' {
			fieldName = fieldName[1 : len(fieldName)-1]
		}

		// Support nested field access
		var val interface{}
		var found bool
		if fieldpath.IsNestedField(fieldName) {
			val, found = fieldpath.GetNestedField(data, fieldName)
		} else {
			val, found = data[fieldName]
		}

		if !found {
			return 0, true, nil // Field not found is treated as NULL
		}

		if val == nil {
			return 0, true, nil // NULL value
		}

		// Try to convert to numeric value
		if floatVal, ok := convertToFloatSafe(val); ok {
			return floatVal, false, nil
		}

		return 0, false, fmt.Errorf("field '%s' is not a number", fieldName)

	case TypeOperator:
		// For comparison operators, return boolean converted to numeric
		if isComparisonOperator(node.Value) {
			leftValue, leftIsNull, err := evaluateNodeValueWithNull(node.Left, data)
			if err != nil {
				return 0, false, err
			}

			rightValue, rightIsNull, err := evaluateNodeValueWithNull(node.Right, data)
			if err != nil {
				return 0, false, err
			}

			// Handle NULL comparison
			if leftIsNull || rightIsNull {
				switch strings.ToUpper(node.Value) {
				case "IS":
					if leftIsNull && rightIsNull {
						return 1, false, nil
					}
					return 0, false, nil
				case "IS NOT":
					if leftIsNull && rightIsNull {
						return 0, false, nil
					}
					return 1, false, nil
				default:
					return 0, true, nil // NULL compared with any value returns NULL
				}
			}

			// Execute comparison
			result, err := compareValues(leftValue, rightValue, node.Value)
			if err != nil {
				return 0, false, err
			}
			if result {
				return 1, false, nil
			}
			return 0, false, nil
		}

		// Arithmetic operators
		leftVal, leftIsNull, err := evaluateNodeWithNull(node.Left, data)
		if err != nil {
			return 0, false, err
		}
		if leftIsNull {
			return 0, true, nil
		}

		rightVal, rightIsNull, err := evaluateNodeWithNull(node.Right, data)
		if err != nil {
			return 0, false, err
		}
		if rightIsNull {
			return 0, true, nil
		}

		switch node.Value {
		case "+":
			return leftVal + rightVal, false, nil
		case "-":
			return leftVal - rightVal, false, nil
		case "*":
			return leftVal * rightVal, false, nil
		case "/":
			if rightVal == 0 {
				return 0, false, fmt.Errorf("division by zero")
			}
			return leftVal / rightVal, false, nil
		case "%":
			if rightVal == 0 {
				return 0, false, fmt.Errorf("modulo by zero")
			}
			return math.Mod(leftVal, rightVal), false, nil
		default:
			return 0, false, fmt.Errorf("unknown operator: %s", node.Value)
		}

	case TypeFunction:
		// Function call, if any argument is NULL, result is usually NULL
		val, err := evaluateNode(node, data)
		return val, false, err

	case TypeCase:
		// CASE expression
		result, isNull, err := evaluateCaseExpressionWithNull(node, data)
		if err != nil {
			return 0, false, err
		}
		if isNull {
			return 0, true, nil
		}
		if floatVal, ok := convertToFloatSafe(result); ok {
			return floatVal, false, nil
		}
		return 0, false, fmt.Errorf("CASE expression result is not a number")

	case TypeParenthesis:
		// Handle parenthesis expression, directly evaluate inner expression
		return evaluateNodeWithNull(node.Left, data)

	default:
		return 0, false, fmt.Errorf("unknown node type: %s", node.Type)
	}
}

// evaluateNodeValueWithNull evaluates the original value of a node with NULL value handling
func evaluateNodeValueWithNull(node *ExprNode, data map[string]interface{}) (interface{}, bool, error) {
	if node == nil {
		return nil, true, nil
	}

	switch node.Type {
	case TypeNumber:
		val, err := strconv.ParseFloat(node.Value, 64)
		return val, false, err

	case TypeString:
		// Handle string type, remove quotes
		value := node.Value
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1] // Remove quotes
		}
		return value, false, nil

	case TypeField:
		// Handle backtick identifiers
		fieldName := node.Value
		if len(fieldName) >= 2 && fieldName[0] == '`' && fieldName[len(fieldName)-1] == '`' {
			fieldName = fieldName[1 : len(fieldName)-1]
		}

		// Support nested field access
		var val interface{}
		var found bool
		if fieldpath.IsNestedField(fieldName) {
			val, found = fieldpath.GetNestedField(data, fieldName)
		} else {
			val, found = data[fieldName]
		}

		if !found {
			return nil, true, nil // Field not found is treated as NULL
		}

		return val, val == nil, nil

	case TypeOperator:
		val, err := evaluateOperatorValue(node, data)
		if err != nil {
			return nil, false, err
		}
		return val, false, nil

	case TypeFunction:
		val, err := evaluateFunctionValue(node, data)
		return val, val == nil, err

	case TypeCase:
		return evaluateCaseExpressionWithNull(node, data)

	case TypeParenthesis:
		// Handle parenthesis expression, directly evaluate inner expression
		return evaluateNodeValueWithNull(node.Left, data)

	default:
		return nil, false, fmt.Errorf("unknown node type: %s", node.Type)
	}
}

// compareValuesWithNullForEquality compares two values for equality (supports NULL comparison)
func compareValuesWithNullForEquality(left interface{}, leftIsNull bool, right interface{}, rightIsNull bool) bool {
	if leftIsNull && rightIsNull {
		return true
	}
	if leftIsNull || rightIsNull {
		return false
	}
	return compareValuesForEquality(left, right)
}

// evaluateBoolNode evaluates the boolean value of a node
func evaluateBoolNode(node *ExprNode, data map[string]interface{}) (bool, error) {
	if node == nil {
		return false, fmt.Errorf("null expression node")
	}

	switch node.Type {
	case TypeOperator:
		return evaluateBoolOperator(node, data)
	case TypeFunction:
		return evaluateBoolFunction(node, data)
	case TypeParenthesis:
		// Parenthesis node, recursively evaluate inner expression
		if node.Left != nil {
			return evaluateBoolNode(node.Left, data)
		}
		return false, fmt.Errorf("empty parenthesis expression")
	case TypeField:
		// Convert field value to boolean
		value, err := evaluateFieldValue(node, data)
		if err != nil {
			// If field doesn't exist, treat as NULL, convert to false
			return false, nil
		}
		return convertToBool(value), nil
	case TypeNumber:
		// Convert number to boolean (non-zero is true)
		value, err := strconv.ParseFloat(node.Value, 64)
		if err != nil {
			return false, err
		}
		return value != 0, nil
	case TypeString:
		// Convert string to boolean (non-empty is true)
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1] // Remove quotes
		}
		return value != "", nil
	default:
		return false, fmt.Errorf("unsupported node type for boolean evaluation: %s", node.Type)
	}
}

// evaluateBoolOperator evaluates boolean operators
func evaluateBoolOperator(node *ExprNode, data map[string]interface{}) (bool, error) {
	operator := strings.ToUpper(node.Value)

	switch operator {
	case "AND", "&&":
		left, err := evaluateBoolNode(node.Left, data)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil // Short-circuit evaluation
		}
		return evaluateBoolNode(node.Right, data)

	case "OR", "||":
		left, err := evaluateBoolNode(node.Left, data)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil // Short-circuit evaluation
		}
		return evaluateBoolNode(node.Right, data)

	case "NOT", "!":
		// NOT operator may use Left or Right node
		var operand *ExprNode
		if node.Left != nil {
			operand = node.Left
		} else if node.Right != nil {
			operand = node.Right
		} else {
			return false, fmt.Errorf("NOT operator requires an operand")
		}
		result, err := evaluateBoolNode(operand, data)
		if err != nil {
			return false, err
		}
		return !result, nil

	case "IS", "IS NOT":
		// IS and IS NOT operators (including IS NULL and IS NOT NULL)
		result, err := evaluateIsOperator(node, data)
		if err != nil {
			return false, err
		}
		return convertToBool(result), nil

	case "==", "=", "!=", "<>", ">", "<", ">=", "<=", "LIKE":
		// Comparison operators
		leftValue, err := evaluateNodeValue(node.Left, data)
		if err != nil {
			return false, err
		}
		rightValue, err := evaluateNodeValue(node.Right, data)
		if err != nil {
			return false, err
		}
		return compareValues(leftValue, rightValue, operator)

	default:
		return false, fmt.Errorf("unsupported boolean operator: %s", operator)
	}
}

// evaluateBoolFunction evaluates boolean functions
func evaluateBoolFunction(node *ExprNode, data map[string]interface{}) (bool, error) {
	// Call function and convert result to boolean
	result, err := evaluateFunctionValue(node, data)
	if err != nil {
		return false, err
	}
	return convertToBool(result), nil
}

// evaluateIsOperator handles IS and IS NOT operators (mainly IS NULL and IS NOT NULL)
func evaluateIsOperator(node *ExprNode, data map[string]interface{}) (interface{}, error) {
	if node.Right == nil {
		return nil, fmt.Errorf("IS operator requires a right operand")
	}

	operator := strings.ToUpper(node.Value)

	// Check if right side is NULL
	if node.Right.Type == TypeField && strings.ToUpper(node.Right.Value) == "NULL" {
		// Get left value using NULL-supporting method
		_, leftIsNull, err := evaluateNodeValueWithNull(node.Left, data)
		if err != nil {
			// If field doesn't exist, consider it NULL
			leftIsNull = true
		}

		if operator == "IS" {
			// IS NULL comparison
			return leftIsNull, nil
		} else if operator == "IS NOT" {
			// IS NOT NULL comparison
			return !leftIsNull, nil
		}
	}

	// Other IS comparisons
	leftValue, err := evaluateNodeValue(node.Left, data)
	if err != nil {
		return nil, err
	}

	rightValue, err := evaluateNodeValue(node.Right, data)
	if err != nil {
		return nil, err
	}

	if operator == "IS" {
		return compareValuesForEquality(leftValue, rightValue), nil
	} else if operator == "IS NOT" {
		return !compareValuesForEquality(leftValue, rightValue), nil
	}

	return nil, fmt.Errorf("unsupported IS operator: %s", operator)
}
