package rsql

import (
	"fmt"
	"strings"
	"time"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/window"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/logger"
)

type SelectStatement struct {
	Fields    []Field
	Distinct  bool
	SelectAll bool // Flag to indicate if this is a SELECT * query
	Source    string
	Condition string
	Window    WindowDefinition
	GroupBy   []string
	Limit     int
	Having    string
}

type Field struct {
	Expression string
	Alias      string
	AggType    string
}

type WindowDefinition struct {
	Type     string
	Params   []interface{}
	TsProp   string
	TimeUnit time.Duration
}

// ToStreamConfig converts AST to Stream configuration
func (s *SelectStatement) ToStreamConfig() (*types.Config, string, error) {
	if s.Source == "" {
		return nil, "", fmt.Errorf("missing FROM clause")
	}

	// Parse window configuration
	windowType := window.TypeTumbling
	if strings.ToUpper(s.Window.Type) == "TUMBLINGWINDOW" {
		windowType = window.TypeTumbling
	} else if strings.ToUpper(s.Window.Type) == "SLIDINGWINDOW" {
		windowType = window.TypeSliding
	} else if strings.ToUpper(s.Window.Type) == "COUNTINGWINDOW" {
		windowType = window.TypeCounting
	} else if strings.ToUpper(s.Window.Type) == "SESSIONWINDOW" {
		windowType = window.TypeSession
	}

	params, err := parseWindowParamsWithType(s.Window.Params, windowType)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse window parameters: %w", err)
	}

	// Check if window processing is needed
	needWindow := s.Window.Type != ""
	var simpleFields []string

	// Check if there are aggregation functions
	hasAggregation := false
	for _, field := range s.Fields {
		if isAggregationFunction(field.Expression) {
			hasAggregation = true
			break
		}
	}

	// If no window is specified but has aggregation functions, use tumbling window by default
	if !needWindow && hasAggregation {
		needWindow = true
		windowType = window.TypeTumbling
		params = map[string]interface{}{
			"size": 10 * time.Second, // Default 10-second window
		}
	}

	// Handle special configuration for SessionWindow
	var groupByKey string
	if windowType == window.TypeSession && len(s.GroupBy) > 0 {
		// For session window, use the first GROUP BY field as session key
		groupByKey = s.GroupBy[0]
	}

	// If no aggregation functions, collect simple fields
	if !hasAggregation {
		// If SELECT * query, set special marker
		if s.SelectAll {
			simpleFields = append(simpleFields, "*")
		} else {
			for _, field := range s.Fields {
				fieldName := field.Expression
				if field.Alias != "" {
					// If has alias, use alias as field name
					simpleFields = append(simpleFields, fieldName+":"+field.Alias)
				} else {
					// For fields without alias, check if it's a string literal
					_, n, _, _ := ParseAggregateTypeWithExpression(fieldName)
					if n != "" {
						// If string literal, use parsed field name (remove quotes)
						simpleFields = append(simpleFields, n)
					} else {
						// Otherwise use original expression
						simpleFields = append(simpleFields, fieldName)
					}
				}
			}
		}
		logger.Debug("Collected simple fields: %v", simpleFields)
	}

	// Build field mapping and expression information
	aggs, fields, expressions := buildSelectFieldsWithExpressions(s.Fields)

	// Extract field order information
	fieldOrder := extractFieldOrder(s.Fields)

	// Build Stream configuration
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:       windowType,
			Params:     params,
			TsProp:     s.Window.TsProp,
			TimeUnit:   s.Window.TimeUnit,
			GroupByKey: groupByKey,
		},
		GroupFields:      extractGroupFields(s),
		SelectFields:     aggs,
		FieldAlias:       fields,
		Distinct:         s.Distinct,
		Limit:            s.Limit,
		NeedWindow:       needWindow,
		SimpleFields:     simpleFields,
		Having:           s.Having,
		FieldExpressions: expressions,
		FieldOrder:       fieldOrder,
	}

	return &config, s.Condition, nil
}

// Check if expression is an aggregation function
func isAggregationFunction(expr string) bool {
	// Extract function name
	funcName := extractFunctionName(expr)
	if funcName == "" {
		return false
	}

	// Check if it's a registered function
	if fn, exists := functions.Get(funcName); exists {
		// Determine if aggregation processing is needed based on function type
		switch fn.GetType() {
		case functions.TypeAggregation:
			// Aggregation function needs aggregation processing
			return true
		case functions.TypeAnalytical:
			// Analytical function also needs aggregation processing (state management)
			return true
		case functions.TypeWindow:
			// Window function needs aggregation processing
			return true

		default:
			// Other types of functions (string, conversion, etc.) don't need aggregation processing
			return false
		}
	}

	// For unregistered functions, check if it's expr-lang built-in function
	// These functions are handled through ExprBridge, don't need aggregation mode
	bridge := functions.GetExprBridge()
	if bridge.IsExprLangFunction(funcName) {
		return false
	}

	// If not registered function and not expr-lang function, but contains parentheses, conservatively assume it might be aggregation function
	if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
		return true
	}
	return false
}

// extractFieldOrder extracts original order of fields from Fields slice
// Returns field names list in order of appearance in SELECT statement
func extractFieldOrder(fields []Field) []string {
	var fieldOrder []string

	for _, field := range fields {
		// If has alias, use alias as field name
		if field.Alias != "" {
			fieldOrder = append(fieldOrder, field.Alias)
		} else {
			// Without alias, try to parse expression to get field name
			_, fieldName, _, _ := ParseAggregateTypeWithExpression(field.Expression)
			if fieldName != "" {
				// If parsed field name (like string literal), use parsed name
				fieldOrder = append(fieldOrder, fieldName)
			} else {
				// Otherwise use original expression as field name
				fieldOrder = append(fieldOrder, field.Expression)
			}
		}
	}

	return fieldOrder
}
func extractGroupFields(s *SelectStatement) []string {
	var fields []string
	for _, f := range s.GroupBy {
		if !strings.Contains(f, "(") { // Exclude aggregation functions
			fields = append(fields, f)
		}
	}
	return fields
}

func buildSelectFields(fields []Field) (aggMap map[string]aggregator.AggregateType, fieldMap map[string]string) {
	selectFields := make(map[string]aggregator.AggregateType)
	fieldMap = make(map[string]string)

	for _, f := range fields {
		if alias := f.Alias; alias != "" {
			t, n, _, _ := ParseAggregateTypeWithExpression(f.Expression)
			if t != "" {
				// Use alias as key for aggregator, not field name
				selectFields[alias] = t

				// Field mapping: output field name(alias) -> input field name (consistent with buildSelectFieldsWithExpressions)
				if n != "" {
					fieldMap[alias] = n
				} else {
					// If no field name extracted, use alias itself
					fieldMap[alias] = alias
				}
			}
		} else {
			// Without alias, use expression itself as field name
			t, n, _, _ := ParseAggregateTypeWithExpression(f.Expression)
			if t != "" && n != "" {
				selectFields[n] = t
				fieldMap[n] = n
			}
		}
	}
	return selectFields, fieldMap
}

// Parse aggregation function and return expression information
func ParseAggregateTypeWithExpression(exprStr string) (aggType aggregator.AggregateType, name string, expression string, allFields []string) {
	// Special handling for CASE expressions
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(exprStr)), "CASE") {
		// CASE expressions are handled as special expressions
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", "", exprStr, allFields
	}

	// Check if it's nested functions
	if hasNestedFunctions(exprStr) {
		// Nested function case, extract all functions
		funcs := extractAllFunctions(exprStr)

		// Find aggregation functions
		var aggregationFunc string
		for _, funcName := range funcs {
			if fn, exists := functions.Get(funcName); exists {
				switch fn.GetType() {
				case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
					aggregationFunc = funcName
					break
				}
			}
		}

		if aggregationFunc != "" {
			// Nested expression with aggregation function, handle entire expression as expression
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
			return aggregator.AggregateType(aggregationFunc), "", exprStr, allFields
		} else {
			// Nested expression without aggregation function, handle as regular expression
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
			return "expression", "", exprStr, allFields
		}
	}

	// Original logic for single function
	// Extract function name
	funcName := extractFunctionName(exprStr)
	if funcName == "" {
		// Check if it's a string literal
		trimmed := strings.TrimSpace(exprStr)
		if (strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'")) ||
			(strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"")) {
			// String literal: use content without quotes as field name
			fieldName := trimmed[1 : len(trimmed)-1]
			return "expression", fieldName, exprStr, nil
		}

		// If not a function call but contains operators or keywords, it might be an expression
		if strings.ContainsAny(exprStr, "+-*/<>=!&|") ||
			strings.Contains(strings.ToUpper(exprStr), "AND") ||
			strings.Contains(strings.ToUpper(exprStr), "OR") {
			// Handle as expression
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
			return "expression", "", exprStr, allFields
		}
		return "", "", "", nil
	}

	// Check if it's a registered function
	fn, exists := functions.Get(funcName)
	if !exists {
		return "", "", "", nil
	}

	// Extract function parameters and expression information
	name, expression, allFields = extractAggFieldWithExpression(exprStr, funcName)

	// Determine aggregation type based on function type
	switch fn.GetType() {
	case functions.TypeAggregation:
		// Aggregation function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields

	case functions.TypeAnalytical:
		// Analytical function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields

	case functions.TypeWindow:
		// Window function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields

	case functions.TypeString, functions.TypeConversion, functions.TypeCustom, functions.TypeMath:
		// String, conversion, custom, math functions: handle as expressions in aggregation queries
		// Use "expression" as special aggregation type, indicating this is an expression calculation
		// For these functions, should save complete function call as expression, not just parameter part
		fullExpression := exprStr
		if parsedExpr, err := expr.NewExpression(fullExpression); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", name, fullExpression, allFields

	default:
		// Other types of functions don't use aggregation
		// These functions will be handled directly in non-window mode
		return "", "", "", nil
	}
}

// extractFunctionName extracts function name from expression
func extractFunctionName(expr string) string {
	// Find first left parenthesis
	parenIndex := strings.Index(expr, "(")
	if parenIndex == -1 {
		return ""
	}

	// Extract function name part
	funcName := strings.TrimSpace(expr[:parenIndex])

	// If function name contains other operators or spaces, it's not a simple function call
	if strings.ContainsAny(funcName, " +-*/=<>!&|") {
		return ""
	}

	return funcName
}

// Extract all function names from expression
func extractAllFunctions(expr string) []string {
	var funcNames []string

	// Simple function name matching
	i := 0
	for i < len(expr) {
		// Find function name pattern
		start := i
		for i < len(expr) && (expr[i] >= 'a' && expr[i] <= 'z' || expr[i] >= 'A' && expr[i] <= 'Z' || expr[i] == '_') {
			i++
		}

		if i < len(expr) && expr[i] == '(' && i > start {
			// Found possible function name
			funcName := expr[start:i]
			if _, exists := functions.Get(funcName); exists {
				funcNames = append(funcNames, funcName)
			}
		}

		if i < len(expr) {
			i++
		}
	}

	return funcNames
}

// Check if expression contains nested functions
func hasNestedFunctions(expr string) bool {
	funcs := extractAllFunctions(expr)
	return len(funcs) > 1
}

// Extract aggregation function fields and parse expression information
func extractAggFieldWithExpression(exprStr string, funcName string) (fieldName string, expression string, allFields []string) {
	start := strings.Index(strings.ToLower(exprStr), strings.ToLower(funcName)+"(")
	if start < 0 {
		return "", "", nil
	}
	start += len(funcName) + 1

	end := strings.LastIndex(exprStr, ")")
	if end <= start {
		return "", "", nil
	}

	// Extract expression within parentheses
	fieldExpr := strings.TrimSpace(exprStr[start:end])

	// Special handling for count(*) case
	if strings.ToLower(funcName) == "count" && fieldExpr == "*" {
		return "*", "", nil
	}

	// Check if it's a simple field name (only letters, numbers, underscores)
	isSimpleField := true
	for _, char := range fieldExpr {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			isSimpleField = false
			break
		}
	}

	// If simple field, return field name directly, don't create expression
	if isSimpleField {
		return fieldExpr, "", nil
	}

	// For complex expressions, including multi-parameter function calls
	expression = fieldExpr

	// For string functions like CONCAT, save complete expression directly
	if strings.ToLower(funcName) == "concat" {
		// Intelligently parse CONCAT function parameters to extract field names
		var fields []string
		params := parseSmartParameters(fieldExpr)
		for _, param := range params {
			param = strings.TrimSpace(param)
			// If parameter is not string constant (not surrounded by quotes), consider it as field name
			if !((strings.HasPrefix(param, "'") && strings.HasSuffix(param, "'")) ||
				(strings.HasPrefix(param, "\"") && strings.HasSuffix(param, "\""))) {
				if isIdentifier(param) {
					fields = append(fields, param)
				}
			}
		}
		if len(fields) > 0 {
			// For CONCAT function, save complete function call as expression
			return fields[0], funcName + "(" + fieldExpr + ")", fields
		}
		// If no field found, return empty field name but keep expression
		return "", funcName + "(" + fieldExpr + ")", nil
	}

	// Use expression engine to parse
	parsedExpr, err := expr.NewExpression(fieldExpr)
	if err != nil {
		// If expression parsing fails, try manual parameter parsing
		// This is mainly used to handle multi-parameter functions like distance(x1, y1, x2, y2)
		if strings.Contains(fieldExpr, ",") {
			// Split parameters
			params := strings.Split(fieldExpr, ",")
			var fields []string
			for _, param := range params {
				param = strings.TrimSpace(param)
				if isIdentifier(param) {
					fields = append(fields, param)
				}
			}
			if len(fields) > 0 {
				// For multi-parameter functions, use all parameter fields, main field name is first parameter
				return fields[0], expression, fields
			}
		}

		// If still fails to parse, try simple extraction method
		fieldName = extractSimpleField(fieldExpr)
		return fieldName, expression, []string{fieldName}
	}

	// Get all fields referenced in expression
	allFields = parsedExpr.GetFields()

	// If only one field, return directly
	if len(allFields) == 1 {
		return allFields[0], expression, allFields
	}

	// If multiple fields, use first field name as main field
	if len(allFields) > 0 {
		// Record complete expression and all fields
		return allFields[0], expression, allFields
	}

	// If no fields (pure constant expression), return entire expression as field name
	return fieldExpr, expression, nil
}

// parseSmartParameters intelligently parses function parameters, correctly handles commas within quotes
func parseSmartParameters(paramsStr string) []string {
	var params []string
	var current strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(paramsStr); i++ {
		ch := paramsStr[i]

		if !inQuotes {
			if ch == '\'' || ch == '"' {
				inQuotes = true
				quoteChar = ch
				current.WriteByte(ch)
			} else if ch == ',' {
				// Parameter separator
				params = append(params, current.String())
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		} else {
			if ch == quoteChar {
				inQuotes = false
				quoteChar = 0
			}
			current.WriteByte(ch)
		}
	}

	// Add the last parameter
	if current.Len() > 0 {
		params = append(params, current.String())
	}

	return params
}

// isIdentifier checks if string is a valid identifier
func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}
	// First character must be letter or underscore
	if !((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') || s[0] == '_') {
		return false
	}
	// Remaining characters must be letters, numbers, or underscores
	for i := 1; i < len(s); i++ {
		char := s[i]
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	return true
}

// extractSimpleField for backward compatibility
func extractSimpleField(fieldExpr string) string {
	// If contains operators, extract first operand as field name
	for _, op := range []string{"/", "*", "+", "-"} {
		if opIndex := strings.Index(fieldExpr, op); opIndex > 0 {
			return strings.TrimSpace(fieldExpr[:opIndex])
		}
	}
	return fieldExpr
}

func parseWindowParams(params []interface{}) (map[string]interface{}, error) {
	return parseWindowParamsWithType(params, "")
}

func parseWindowParamsWithType(params []interface{}, windowType string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	var key string
	for index, v := range params {
		if windowType == window.TypeSession {
			// First parameter for SessionWindow is timeout
			if index == 0 {
				key = "timeout"
			} else {
				key = fmt.Sprintf("param%d", index)
			}
		} else {
			// Parameters for other window types
			if index == 0 {
				key = "size"
			} else if index == 1 {
				key = "slide"
			} else {
				key = "offset"
			}
		}
		if s, ok := v.(string); ok {
			dur, err := time.ParseDuration(s)
			if err != nil {
				return nil, fmt.Errorf("invalid %s duration: %w", s, err)
			}
			result[key] = dur
		} else {
			return nil, fmt.Errorf("%s parameter must be string format (like '5s')", s)
		}
	}

	return result, nil
}

func parseAggregateExpression(expr string) string {
	if strings.Contains(expr, functions.AvgStr+"(") {
		return functions.AvgStr
	}
	if strings.Contains(expr, functions.SumStr+"(") {
		return functions.SumStr
	}
	if strings.Contains(expr, functions.MaxStr+"(") {
		return functions.MaxStr
	}
	if strings.Contains(expr, functions.MinStr+"(") {
		return functions.MinStr
	}
	return ""
}

// Parse field information including expressions
func buildSelectFieldsWithExpressions(fields []Field) (
	aggMap map[string]aggregator.AggregateType,
	fieldMap map[string]string,
	expressions map[string]types.FieldExpression) {

	selectFields := make(map[string]aggregator.AggregateType)
	fieldMap = make(map[string]string)
	expressions = make(map[string]types.FieldExpression)

	for _, f := range fields {
		if alias := f.Alias; alias != "" {
			t, n, expression, allFields := ParseAggregateTypeWithExpression(f.Expression)
			if t != "" {
				// Use alias as key so each aggregation function has unique key
				selectFields[alias] = t

				// Field mapping: output field name -> input field name (prepare correct mapping for aggregator)
				if n != "" {
					fieldMap[alias] = n
				} else {
					// If no field name extracted, use alias itself
					fieldMap[alias] = alias
				}

				// If expression exists, save expression information
				if expression != "" {
					expressions[alias] = types.FieldExpression{
						Field:      n,
						Expression: expression,
						Fields:     allFields,
					}
				}
			}
		} else {
			// Without alias, use expression itself as field name
			t, n, expression, allFields := ParseAggregateTypeWithExpression(f.Expression)
			if t != "" && n != "" {
				// For string literals, use parsed field name (remove quotes) as key
				selectFields[n] = t
				fieldMap[n] = n

				// If expression exists, save expression information
				if expression != "" {
					expressions[n] = types.FieldExpression{
						Field:      n,
						Expression: expression,
						Fields:     allFields,
					}
				}
			}
		}
	}
	return selectFields, fieldMap, expressions
}
