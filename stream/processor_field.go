package stream

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/logger"
	"github.com/rulego/streamsql/utils/fieldpath"
)

// fieldProcessInfo field processing information for caching pre-compiled field processing logic
type fieldProcessInfo struct {
	fieldName       string // Original field name
	outputName      string // Output field name
	isFunctionCall  bool   // Whether it's a function call
	hasNestedField  bool   // Whether it contains nested fields
	isSelectAll     bool   // Whether it's SELECT *
	isStringLiteral bool   // Whether it's a string literal
	stringValue     string // Pre-processed string literal value (quotes removed)
	alias           string // Field alias for quick access
}

// expressionProcessInfo expression processing information for caching pre-compiled expression processing logic
type expressionProcessInfo struct {
	originalExpr            string           // Original expression
	processedExpr           string           // Pre-processed expression
	isFunctionCall          bool             // Whether it's a function call
	hasNestedFields         bool             // Whether it contains nested fields
	compiledExpr            *expr.Expression // Pre-compiled expression object
	needsBacktickPreprocess bool             // Whether backtick preprocessing is needed
}

// compileFieldProcessInfo pre-compiles field processing information to avoid runtime re-parsing
func (s *Stream) compileFieldProcessInfo() {
	s.compiledFieldInfo = make(map[string]*fieldProcessInfo)
	s.compiledExprInfo = make(map[string]*expressionProcessInfo)

	// Compile SimpleFields information
	for _, fieldSpec := range s.config.SimpleFields {
		info := s.compileSimpleFieldInfo(fieldSpec)
		s.compiledFieldInfo[fieldSpec] = info
	}

	// Pre-compile expression field information
	s.compileExpressionInfo()
}

// compileSimpleFieldInfo compiles simple field information
func (s *Stream) compileSimpleFieldInfo(fieldSpec string) *fieldProcessInfo {
	info := &fieldProcessInfo{}

	if fieldSpec == "*" {
		info.isSelectAll = true
		info.fieldName = ""
		info.outputName = "*"
		info.alias = "*"
		return info
	}

	// Parse alias
	parts := strings.Split(fieldSpec, ":")
	info.fieldName = parts[0]
	// Remove backticks from field name
	if len(info.fieldName) >= 2 && info.fieldName[0] == '`' && info.fieldName[len(info.fieldName)-1] == '`' {
		info.fieldName = info.fieldName[1 : len(info.fieldName)-1]
	}
	info.outputName = info.fieldName
	if len(parts) > 1 {
		info.outputName = parts[1]
		// Remove backticks from output name
		if len(info.outputName) >= 2 && info.outputName[0] == '`' && info.outputName[len(info.outputName)-1] == '`' {
			info.outputName = info.outputName[1 : len(info.outputName)-1]
		}
	}

	// Pre-determine field characteristics
	info.isFunctionCall = strings.Contains(info.fieldName, "(") && strings.Contains(info.fieldName, ")")
	info.hasNestedField = !info.isFunctionCall && fieldpath.IsNestedField(info.fieldName)

	// Check if it's a string literal and preprocess value
	info.isStringLiteral = (len(info.fieldName) >= 2 &&
		((info.fieldName[0] == '\'' && info.fieldName[len(info.fieldName)-1] == '\'') ||
			(info.fieldName[0] == '"' && info.fieldName[len(info.fieldName)-1] == '"')))

	// Preprocess string literal value, remove quotes
	if info.isStringLiteral && len(info.fieldName) >= 2 {
		info.stringValue = info.fieldName[1 : len(info.fieldName)-1]
	}

	// Set alias for quick access
	info.alias = info.outputName

	return info
}

// compileExpressionInfo pre-compiles expression processing information
func (s *Stream) compileExpressionInfo() {
	bridge := functions.GetExprBridge()

	for fieldName, fieldExpr := range s.config.FieldExpressions {
		exprInfo := &expressionProcessInfo{
			originalExpr: fieldExpr.Expression,
		}

		// Preprocess expression
		processedExpr := fieldExpr.Expression
		if bridge.ContainsIsNullOperator(processedExpr) {
			if processed, err := bridge.PreprocessIsNullExpression(processedExpr); err == nil {
				processedExpr = processed
			}
		}
		if bridge.ContainsLikeOperator(processedExpr) {
			if processed, err := bridge.PreprocessLikeExpression(processedExpr); err == nil {
				processedExpr = processed
			}
		}
		exprInfo.processedExpr = processedExpr

		// Pre-judge expression characteristics
		exprInfo.isFunctionCall = strings.Contains(fieldExpr.Expression, "(") && strings.Contains(fieldExpr.Expression, ")")
		exprInfo.hasNestedFields = !exprInfo.isFunctionCall && strings.Contains(fieldExpr.Expression, ".")
		exprInfo.needsBacktickPreprocess = bridge.ContainsBacktickIdentifiers(fieldExpr.Expression)

		// Pre-compile expression object (only for non-function call expressions)
		if !exprInfo.isFunctionCall {
			exprToCompile := fieldExpr.Expression
			if exprInfo.needsBacktickPreprocess {
				if processed, err := bridge.PreprocessBacktickIdentifiers(exprToCompile); err == nil {
					exprToCompile = processed
				}
			}
			if compiledExpr, err := expr.NewExpression(exprToCompile); err == nil {
				exprInfo.compiledExpr = compiledExpr
			}
		}

		s.compiledExprInfo[fieldName] = exprInfo
	}
}

// processExpressionField processes expression field
func (s *Stream) processExpressionField(fieldName string, dataMap map[string]interface{}, result map[string]interface{}) {
	exprInfo := s.compiledExprInfo[fieldName]
	if exprInfo == nil {
		// Fallback to original logic
		s.processExpressionFieldFallback(fieldName, dataMap, result)
		return
	}

	var evalResult interface{}
	bridge := functions.GetExprBridge()

	if exprInfo.isFunctionCall {
		// For function calls, use bridge processor
		exprResult, err := bridge.EvaluateExpression(exprInfo.processedExpr, dataMap)
		if err != nil {
			logger.Error("Function call evaluation failed for field %s: %v", fieldName, err)
			result[fieldName] = nil
			return
		}
		evalResult = exprResult
	} else if exprInfo.hasNestedFields {
		// Use pre-compiled expression object
		if exprInfo.compiledExpr != nil {
			// Use EvaluateValueWithNull to get actual value (including strings)
			exprResult, isNull, err := exprInfo.compiledExpr.EvaluateValueWithNull(dataMap)
			if err != nil {
				logger.Error("Expression evaluation failed for field %s: %v", fieldName, err)
				result[fieldName] = nil
				return
			}
			if isNull {
				evalResult = nil
			} else {
				evalResult = exprResult
			}
		} else {
			// Fallback to dynamic compilation
			s.processExpressionFieldFallback(fieldName, dataMap, result)
			return
		}
	} else {
		// Try using bridge processor for other expressions
		exprResult, err := bridge.EvaluateExpression(exprInfo.processedExpr, dataMap)
		if err != nil {
			// If bridge fails, use pre-compiled expression object
			if exprInfo.compiledExpr != nil {
				// Use EvaluateValueWithNull to get actual value (including strings)
				exprResult, isNull, evalErr := exprInfo.compiledExpr.EvaluateValueWithNull(dataMap)
				if evalErr != nil {
					logger.Error("Expression evaluation failed for field %s: %v", fieldName, evalErr)
					result[fieldName] = nil
					return
				}
				if isNull {
					evalResult = nil
				} else {
					evalResult = exprResult
				}
			} else {
				// Fallback to dynamic compilation
				s.processExpressionFieldFallback(fieldName, dataMap, result)
				return
			}
		} else {
			evalResult = exprResult
		}
	}

	result[fieldName] = evalResult
}

// processExpressionFieldFallback fallback logic for expression field processing
func (s *Stream) processExpressionFieldFallback(fieldName string, dataMap map[string]interface{}, result map[string]interface{}) {
	fieldExpr, exists := s.config.FieldExpressions[fieldName]
	if !exists {
		result[fieldName] = nil
		return
	}

	// Use bridge to calculate expression, supports IS NULL and other syntax
	bridge := functions.GetExprBridge()

	// Preprocess IS NULL and LIKE syntax in expression
	processedExpr := fieldExpr.Expression
	if bridge.ContainsIsNullOperator(processedExpr) {
		if processed, err := bridge.PreprocessIsNullExpression(processedExpr); err == nil {
			processedExpr = processed
		}
	}
	if bridge.ContainsLikeOperator(processedExpr) {
		if processed, err := bridge.PreprocessLikeExpression(processedExpr); err == nil {
			processedExpr = processed
		}
	}

	// Check if expression is a function call (contains parentheses)
	isFunctionCall := strings.Contains(fieldExpr.Expression, "(") && strings.Contains(fieldExpr.Expression, ")")

	// Check if expression contains nested fields (but exclude dots in function calls)
	hasNestedFields := false
	if !isFunctionCall && strings.Contains(fieldExpr.Expression, ".") {
		hasNestedFields = true
	}

	var evalResult interface{}

	if isFunctionCall {
		// For function calls, prioritize bridge processor
		exprResult, err := bridge.EvaluateExpression(processedExpr, dataMap)
		if err != nil {
			logger.Error("Function call evaluation failed for field %s: %v", fieldName, err)
			result[fieldName] = nil
			return
		}
		evalResult = exprResult
	} else if hasNestedFields {
		// Detected nested fields (non-function call), use custom expression engine
		exprToUse := fieldExpr.Expression
		if bridge.ContainsBacktickIdentifiers(exprToUse) {
			if processed, err := bridge.PreprocessBacktickIdentifiers(exprToUse); err == nil {
				exprToUse = processed
			}
		}
		expression, parseErr := expr.NewExpression(exprToUse)
		if parseErr != nil {
			logger.Error("Expression parse failed for field %s: %v", fieldName, parseErr)
			result[fieldName] = nil
			return
		}

		// Use EvaluateValueWithNull to get actual value (including strings)
		exprResult, isNull, err := expression.EvaluateValueWithNull(dataMap)
		if err != nil {
			logger.Error("Expression evaluation failed for field %s: %v", fieldName, err)
			result[fieldName] = nil
			return
		}
		if isNull {
			evalResult = nil
		} else {
			evalResult = exprResult
		}
	} else {
		// Try using bridge processor for other expressions
		exprResult, err := bridge.EvaluateExpression(processedExpr, dataMap)
		if err != nil {
			// If bridge fails, fallback to original expression engine
			exprToUse := fieldExpr.Expression
			if bridge.ContainsBacktickIdentifiers(exprToUse) {
				if processed, err := bridge.PreprocessBacktickIdentifiers(exprToUse); err == nil {
					exprToUse = processed
				}
			}
			expression, parseErr := expr.NewExpression(exprToUse)
			if parseErr != nil {
				logger.Error("Expression parse failed for field %s: %v", fieldName, parseErr)
				result[fieldName] = nil
				return
			}

			// Use EvaluateValueWithNull to get actual value (including strings)
			exprResult, isNull, evalErr := expression.EvaluateValueWithNull(dataMap)
			if evalErr != nil {
				logger.Error("Expression evaluation failed for field %s: %v", fieldName, evalErr)
				result[fieldName] = nil
				return
			}
			if isNull {
				evalResult = nil
			} else {
				evalResult = exprResult
			}
		} else {
			evalResult = exprResult
		}
	}

	result[fieldName] = evalResult
}

// processSimpleField processes simple field
func (s *Stream) processSimpleField(fieldSpec string, dataMap map[string]interface{}, data interface{}, result map[string]interface{}) {
	info := s.compiledFieldInfo[fieldSpec]
	if info == nil {
		// If no pre-compiled info, fallback to original logic (safety guarantee)
		s.processSingleFieldFallback(fieldSpec, dataMap, data, result)
		return
	}

	if info.isSelectAll {
		// SELECT *: batch copy all fields, skip expression fields
		for k, v := range dataMap {
			if _, isExpression := s.config.FieldExpressions[k]; !isExpression {
				result[k] = v
			}
		}
		return
	}

	// Skip fields already processed by expression fields
	if _, isExpression := s.config.FieldExpressions[info.outputName]; isExpression {
		return
	}

	if info.isStringLiteral {
		// String literal processing: use pre-compiled string value
		result[info.alias] = info.stringValue
	} else if info.isFunctionCall {
		// Execute function call
		if funcResult, err := s.executeFunction(info.fieldName, dataMap); err == nil {
			result[info.outputName] = funcResult
		} else {
			logger.Error("Function execution error %s: %v", info.fieldName, err)
			result[info.outputName] = nil
		}
	} else {
		// Ordinary field processing
		var value interface{}
		var exists bool

		if info.hasNestedField {
			value, exists = fieldpath.GetNestedField(data, info.fieldName)
		} else {
			value, exists = dataMap[info.fieldName]
		}

		if exists {
			result[info.outputName] = value
		} else {
			result[info.outputName] = nil
		}
	}
}

// processSingleFieldFallback fallback processing for single field (when pre-compiled info is missing)
func (s *Stream) processSingleFieldFallback(fieldSpec string, dataMap map[string]interface{}, data interface{}, result map[string]interface{}) {
	// Handle special case of SELECT *
	if fieldSpec == "*" {
		// SELECT *: return all fields, but skip fields already processed by expression fields
		for k, v := range dataMap {
			// If field already processed by expression field, skip, maintain expression calculation result
			if _, isExpression := s.config.FieldExpressions[k]; !isExpression {
				result[k] = v
			}
		}
		return
	}

	// Handle alias
	parts := strings.Split(fieldSpec, ":")
	fieldName := parts[0]
	outputName := fieldName
	if len(parts) > 1 {
		outputName = parts[1]
	}

	// Skip fields already processed by expression fields
	if _, isExpression := s.config.FieldExpressions[outputName]; isExpression {
		return
	}

	// Check if it's a function call
	if strings.Contains(fieldName, "(") && strings.Contains(fieldName, ")") {
		// Execute function call
		if funcResult, err := s.executeFunction(fieldName, dataMap); err == nil {
			result[outputName] = funcResult
		} else {
			logger.Error("Function execution error %s: %v", fieldName, err)
			result[outputName] = nil
		}
	} else {
		// Ordinary field - supports nested fields
		var value interface{}
		var exists bool

		if fieldpath.IsNestedField(fieldName) {
			value, exists = fieldpath.GetNestedField(data, fieldName)
		} else {
			value, exists = dataMap[fieldName]
		}

		if exists {
			result[outputName] = value
		} else {
			result[outputName] = nil
		}
	}
}

// executeFunction executes function call
func (s *Stream) executeFunction(funcExpr string, data map[string]interface{}) (interface{}, error) {
	// Check if it's a custom function
	funcName := extractFunctionName(funcExpr)
	if funcName != "" {
		// Use function system directly
		fn, exists := functions.Get(funcName)
		if exists {
			// Parse parameters
			args, err := s.parseFunctionArgs(funcExpr, data)
			if err != nil {
				return nil, err
			}

			// Create function context
			ctx := &functions.FunctionContext{Data: data}

			// Execute function
			return fn.Execute(ctx, args)
		}
	}

	// For complex nested function calls, use ExprBridge directly
	// This avoids the float64 type limitation of Expression.Evaluate
	bridge := functions.GetExprBridge()
	result, err := bridge.EvaluateExpression(funcExpr, data)
	if err != nil {
		return nil, fmt.Errorf("evaluate function expression failed: %w", err)
	}

	return result, nil
}

// extractFunctionName extracts function name from expression
func extractFunctionName(expr string) string {
	parenIndex := strings.Index(expr, "(")
	if parenIndex == -1 {
		return ""
	}
	funcName := strings.TrimSpace(expr[:parenIndex])
	if strings.ContainsAny(funcName, " +-*/=<>!&|") {
		return ""
	}
	return funcName
}

// parseFunctionArgs parses function arguments, supports nested function calls
func (s *Stream) parseFunctionArgs(funcExpr string, data map[string]interface{}) ([]interface{}, error) {
	// Extract parameters within parentheses
	start := strings.Index(funcExpr, "(")
	end := strings.LastIndex(funcExpr, ")")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("invalid function expression: %s", funcExpr)
	}

	argsStr := strings.TrimSpace(funcExpr[start+1 : end])
	if argsStr == "" {
		return []interface{}{}, nil
	}

	// Smart split arguments, handle nested functions and quotes
	argParts, err := s.smartSplitArgs(argsStr)
	if err != nil {
		return nil, err
	}

	args := make([]interface{}, len(argParts))

	for i, arg := range argParts {
		arg = strings.TrimSpace(arg)

		// If parameter is string constant (enclosed in quotes)
		if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") {
			args[i] = strings.Trim(arg, "'")
		} else if strings.HasPrefix(arg, "\"") && strings.HasSuffix(arg, "\"") {
			args[i] = strings.Trim(arg, "\"")
		} else if strings.Contains(arg, "(") {
			// If parameter contains function call, execute recursively
			result, err := s.executeFunction(arg, data)
			if err != nil {
				return nil, fmt.Errorf("failed to execute nested function '%s': %v", arg, err)
			}
			args[i] = result
		} else if value, exists := data[arg]; exists {
			// If it's a data field
			args[i] = value
		} else {
			// Try to parse as number
			if val, err := strconv.ParseFloat(arg, 64); err == nil {
				args[i] = val
			} else {
				args[i] = arg
			}
		}
	}

	return args, nil
}

// smartSplitArgs intelligently splits arguments, considering bracket nesting and quotes
func (s *Stream) smartSplitArgs(argsStr string) ([]string, error) {
	var args []string
	var current strings.Builder
	parenDepth := 0
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(argsStr); i++ {
		ch := argsStr[i]

		switch ch {
		case '\'':
			if !inQuotes {
				inQuotes = true
				quoteChar = ch
			} else if quoteChar == ch {
				inQuotes = false
				quoteChar = 0
			}
			current.WriteByte(ch)
		case '"':
			if !inQuotes {
				inQuotes = true
				quoteChar = ch
			} else if quoteChar == ch {
				inQuotes = false
				quoteChar = 0
			}
			current.WriteByte(ch)
		case '(':
			if !inQuotes {
				parenDepth++
			}
			current.WriteByte(ch)
		case ')':
			if !inQuotes {
				parenDepth--
			}
			current.WriteByte(ch)
		case ',':
			if !inQuotes && parenDepth == 0 {
				// Found parameter separator
				args = append(args, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		default:
			current.WriteByte(ch)
		}
	}

	// Add the last parameter
	if current.Len() > 0 {
		args = append(args, strings.TrimSpace(current.String()))
	}

	return args, nil
}
