package rsql

import (
	"fmt"
	"regexp"
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
			_, n, _, _, err := ParseAggregateTypeWithExpression(fieldName)
			if err != nil {
				return nil, "", err
			}
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
	aggs, fields, expressions, postAggExpressions, err := buildSelectFieldsWithExpressions(s.Fields)
	if err != nil {
		return nil, "", err
	}

	// Extract field order information
	fieldOrder, err := extractFieldOrder(s.Fields)
	if err != nil {
		return nil, "", err
	}

	// Build Stream configuration
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:       windowType,
			Params:     params,
			TsProp:     s.Window.TsProp,
			TimeUnit:   s.Window.TimeUnit,
			GroupByKey: groupByKey,
		},
		GroupFields:        extractGroupFields(s),
		SelectFields:       aggs,
		FieldAlias:         fields,
		Distinct:           s.Distinct,
		Limit:              s.Limit,
		NeedWindow:         needWindow,
		SimpleFields:       simpleFields,
		Having:             s.Having,
		FieldExpressions:   expressions,
		PostAggExpressions: postAggExpressions,
		FieldOrder:         fieldOrder,
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
func extractFieldOrder(fields []Field) ([]string, error) {
	var fieldOrder []string

	for _, field := range fields {
		// If has alias, use alias as field name
		if field.Alias != "" {
			fieldOrder = append(fieldOrder, field.Alias)
		} else {
			// Without alias, try to parse expression to get field name
			_, fieldName, _, _, err := ParseAggregateTypeWithExpression(field.Expression)
			if err != nil {
				return nil, err
			}
			if fieldName != "" {
				// If parsed field name (like string literal), use parsed name
				fieldOrder = append(fieldOrder, fieldName)
			} else {
				// Otherwise use original expression as field name
				fieldOrder = append(fieldOrder, field.Expression)
			}
		}
	}

	return fieldOrder, nil
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

func buildSelectFields(fields []Field) (aggMap map[string]aggregator.AggregateType, fieldMap map[string]string, err error) {
	selectFields := make(map[string]aggregator.AggregateType)
	fieldMap = make(map[string]string)

	for _, f := range fields {
		if alias := f.Alias; alias != "" {
			t, n, _, _, parseErr := ParseAggregateTypeWithExpression(f.Expression)
		if parseErr != nil {
			return nil, nil, parseErr
		}
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
			t, n, _, _, parseErr := ParseAggregateTypeWithExpression(f.Expression)
			if parseErr != nil {
				return nil, nil, parseErr
			}
			if t != "" && n != "" {
				selectFields[n] = t
				fieldMap[n] = n
			}
		}
	}
	return selectFields, fieldMap, nil
}

// detectNestedAggregation 检测表达式中是否存在聚合函数嵌套聚合函数的情况
// 如果发现嵌套聚合函数，返回错误信息
func detectNestedAggregation(expr string) error {
	return detectNestedAggregationRecursive(expr, false)
}

// detectNestedAggregationRecursive 递归检测嵌套聚合函数
// inAggregation 表示当前是否在聚合函数内部
func detectNestedAggregationRecursive(expr string, inAggregation bool) error {
	// 使用正则表达式匹配函数调用模式
	pattern := regexp.MustCompile(`(?i)([a-z_]+)\s*\(`)
	matches := pattern.FindAllStringSubmatchIndex(expr, -1)
	
	for _, match := range matches {
		funcStart := match[0]
		funcName := strings.ToLower(expr[match[2]:match[3]])
		
		// 检查函数是否为聚合函数
		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				// 如果当前已经在聚合函数内部，且又发现了聚合函数，则报错
				if inAggregation {
					return fmt.Errorf("aggregate function calls cannot be nested")
				}
				
				// 找到该函数的参数部分
				funcEnd := findMatchingParenInternal(expr, funcStart+len(funcName))
				if funcEnd > funcStart {
					// 提取函数参数
					paramStart := funcStart + len(funcName) + 1
					params := expr[paramStart:funcEnd]
					
					// 在聚合函数参数内部递归检查
					if err := detectNestedAggregationRecursive(params, true); err != nil {
						return err
					}
				}
			}
		}
	}
	
	return nil
}

// Parse aggregation function and return expression information
func ParseAggregateTypeWithExpression(exprStr string) (aggType aggregator.AggregateType, name string, expression string, allFields []string, err error) {
	// 首先检测是否存在嵌套聚合函数
	if err := detectNestedAggregation(exprStr); err != nil {
		// 如果发现嵌套聚合，返回错误
		return "", "", "", nil, err
	}

	// Special handling for CASE expressions
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(exprStr)), "CASE") {
		// CASE expressions are handled as special expressions
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", "", exprStr, allFields, nil
	}

	// Check if it's an expression containing operators with functions
	if containsOperatorsOutsideFunctions(exprStr) && containsFunctions(exprStr) {
		// This is a complex expression with functions and operators
		// Extract all fields referenced in the expression
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		// Return as expression type for post-aggregation evaluation
		return "expression", "", exprStr, allFields, nil
	}

	// Original logic for single function (moved up to prioritize outer function detection)
	// Extract function name
	funcName := extractFunctionName(exprStr)

	// Check if it's nested functions without operators
	hasNested := hasNestedFunctions(exprStr)
	if hasNested && funcName != "" {
		// For nested functions, check if the outer function is an aggregation function
		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				// Outer function is aggregation - handle as aggregation with expression parameter
				name, expression, allFields := extractAggFieldWithExpression(exprStr, funcName)

				return aggregator.AggregateType(funcName), name, expression, allFields, nil
			}
		}
		// Multiple functions but no operators and outer function is not aggregation - treat as expression
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", "", exprStr, allFields, nil
	}
	if funcName == "" {
		// Special handling for SELECT * case
		if strings.TrimSpace(exprStr) == "*" {
			return "", "", "", nil, nil // Don't treat * as expression
		}

		// Check if it's a string literal
		trimmed := strings.TrimSpace(exprStr)
		if (strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'")) ||
			(strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"")) {
			// String literal: use content without quotes as field name
			fieldName := trimmed[1 : len(trimmed)-1]
			return "expression", fieldName, exprStr, nil, nil
		}

		// If not a function call but contains operators or keywords, it might be an expression
		if strings.ContainsAny(exprStr, "+-*/<>=!&|") ||
			strings.Contains(strings.ToUpper(exprStr), "AND") ||
			strings.Contains(strings.ToUpper(exprStr), "OR") {
			// Handle as expression
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
			return "expression", "", exprStr, allFields, nil
		}
		return "", "", "", nil, nil
	}

	// Check if it's a registered function
	fn, exists := functions.Get(funcName)
	if !exists {
		return "", "", "", nil, nil
	}

	// Extract function parameters and expression information
	name, expression, allFields = extractAggFieldWithExpression(exprStr, funcName)

	// Determine aggregation type based on function type
	switch fn.GetType() {
	case functions.TypeAggregation:
		// Aggregation function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields, nil

	case functions.TypeAnalytical:
		// Analytical function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields, nil

	case functions.TypeWindow:
		// Window function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields, nil

	case functions.TypeString, functions.TypeConversion, functions.TypeCustom, functions.TypeMath:
		// String, conversion, custom, math functions: handle as expressions in aggregation queries
		// Use "expression" as special aggregation type, indicating this is an expression calculation
		// For these functions, should save complete function call as expression, not just parameter part
		fullExpression := exprStr
		if parsedExpr, err := expr.NewExpression(fullExpression); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", name, fullExpression, allFields, nil

	default:
		// Other types of functions don't use aggregation
		// These functions will be handled directly in non-window mode
		return "", "", "", nil, nil
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

// containsOperators checks if expression contains arithmetic or comparison operators
func containsOperators(expr string) bool {
	return strings.ContainsAny(expr, "+-*/<>=!&|")
}

// containsFunctions checks if expression contains function calls
func containsFunctions(expr string) bool {
	funcs := extractAllFunctions(expr)
	return len(funcs) > 0
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

	// Check if it's a registered function and get its type
	if fn, exists := functions.Get(funcName); exists {
		// For string functions that need special parameter parsing
		if fn.GetType() == functions.TypeString {
			// Intelligently parse function parameters to extract field names
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
				// For string functions, save complete function call as expression
				// Return all extracted fields as allFields
				return fields[0], strings.ToLower(funcName) + "(" + fieldExpr + ")", fields
			}
			// If no field found, return empty field name but keep expression
			return "", strings.ToLower(funcName) + "(" + fieldExpr + ")", nil
		}
	}

	// Check if it's a multi-parameter function call (contains comma)
	if strings.Contains(fieldExpr, ",") {
		// For multi-parameter functions, extract the first parameter as the field name
		params := strings.Split(fieldExpr, ",")
		if len(params) > 0 {
			firstParam := strings.TrimSpace(params[0])
			// Return first parameter as field name, and full expression for parameter processing
			return firstParam, fieldExpr, nil
		}
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

// Parse field information including expressions with post-aggregation support
func buildSelectFieldsWithExpressions(fields []Field) (
	aggMap map[string]aggregator.AggregateType,
	fieldMap map[string]string,
	expressions map[string]types.FieldExpression,
	postAggExpressions []types.PostAggregationExpression,
	err error) {

	selectFields := make(map[string]aggregator.AggregateType)
	fieldMap = make(map[string]string)
	expressions = make(map[string]types.FieldExpression)
	postAggExpressions = make([]types.PostAggregationExpression, 0)

	for _, f := range fields {
		alias := f.Alias
		if alias == "" {
			// For string literals without alias, use the content without quotes as alias
			trimmed := strings.TrimSpace(f.Expression)
			if (strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'")) ||
				(strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"")) {
				alias = trimmed[1 : len(trimmed)-1] // Remove quotes
			} else {
				alias = f.Expression
			}
		}

		// Check if this is a complex aggregation expression
		if isComplexAggregationExpression(f.Expression) {
			// Parse complex aggregation expression
			aggFields, exprTemplate, err := parseComplexAggregationExpression(f.Expression)
			if err == nil && len(aggFields) > 0 {
				// Add individual aggregation functions
				for _, aggField := range aggFields {
					selectFields[aggField.Placeholder] = aggField.AggType
					fieldMap[aggField.Placeholder] = aggField.InputField
				}

				// Add post-aggregation expression
				postAggExpressions = append(postAggExpressions, types.PostAggregationExpression{
					OutputField:        alias,
					OriginalExpr:       f.Expression,
					ExpressionTemplate: exprTemplate,
					RequiredFields:     aggFields,
				})

				// Mark the main field as post-aggregation
				selectFields[alias] = "post_aggregation"
				fieldMap[alias] = alias
				continue
			}
		}

		// Handle as regular expression
		t, n, expression, allFields, parseErr := ParseAggregateTypeWithExpression(f.Expression)
		if parseErr != nil {
			// 如果检测到嵌套聚合函数，返回错误
			return nil, nil, nil, nil, parseErr
		}
		if t != "" {
			// Check if this is a multi-parameter function that needs special handling
			isMultiParamFunction := false
			if expression != "" && strings.Contains(expression, ",") {
				// Check if the function needs multi-parameter handling
				funcName := extractFunctionName(f.Expression)
				if fn, exists := functions.Get(funcName); exists {
					minArgs := fn.GetMinArgs()
					maxArgs := fn.GetMaxArgs()
					// Function needs multi-parameter handling if it has multiple parameters
					isMultiParamFunction = minArgs > 1 || (maxArgs > minArgs && minArgs >= 1)
				}
			}

			// For multi-parameter functions, treat as post-aggregation expression
			if isMultiParamFunction {
				// Parse as single aggregation function with parameters
				aggFields := []types.AggregationFieldInfo{{
					FuncName:    extractFunctionName(f.Expression),
					InputField:  n,
					Placeholder: "__" + extractFunctionName(f.Expression) + "_" + alias + "__",
					AggType:     aggregator.AggregateType(extractFunctionName(f.Expression)),
					FullCall:    f.Expression,
				}}

				// Add the aggregation function
				selectFields[aggFields[0].Placeholder] = aggFields[0].AggType
				fieldMap[aggFields[0].Placeholder] = aggFields[0].InputField

				// Add post-aggregation expression (which just returns the placeholder value)
				postAggExpressions = append(postAggExpressions, types.PostAggregationExpression{
					OutputField:        alias,
					OriginalExpr:       f.Expression,
					ExpressionTemplate: aggFields[0].Placeholder,
					RequiredFields:     aggFields,
				})

				// Mark the main field as post-aggregation
				selectFields[alias] = "post_aggregation"
				fieldMap[alias] = alias
				continue
			}

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
	}
	return selectFields, fieldMap, expressions, postAggExpressions, nil
}

// isComplexAggregationExpression checks if an expression contains multiple aggregation functions or operators with aggregation functions
func isComplexAggregationExpression(expr string) bool {
	// Check if expression contains aggregation functions
	funcs := extractAllFunctions(expr)
	aggCount := 0
	nonAggCount := 0

	for _, funcName := range funcs {
		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				aggCount++
			default:
				nonAggCount++
			}
		} else {
			nonAggCount++
		}
	}

	// Determine the outermost function name (if any)
	outerFuncName := ""
	if m := regexp.MustCompile(`(?i)^\s*([a-z_][a-z0-9_]*)\s*\(`).FindStringSubmatch(expr); len(m) == 2 {
		outerFuncName = strings.ToLower(m[1])
	}
	outerIsAggregation := false
	if outerFuncName != "" {
		if fn, ok := functions.Get(outerFuncName); ok {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				outerIsAggregation = true
			}
		}
	}

	// Special case: single aggregation function with nested expression (only when OUTER is aggregation)
	isSingleAggWithNestedFunc := false
	if aggCount == 1 && outerIsAggregation {
		start := strings.Index(expr, "(")
		end := strings.LastIndex(expr, ")")
		if start != -1 && end != -1 && end > start {
			innerExpr := strings.TrimSpace(expr[start+1 : end])
			if !containsOperators(innerExpr) {
				isSingleAggWithNestedFunc = true
			}
		}
	}

	result := (aggCount > 1) ||
		(aggCount > 0 && containsOperatorsOutsideFunctions(expr) && !isSingleAggWithNestedFunc) ||
		(aggCount > 0 && nonAggCount > 0 && !isSingleAggWithNestedFunc)

	return result
}

// containsOperatorsOutsideFunctions checks if expression contains operators outside function calls
func containsOperatorsOutsideFunctions(expr string) bool {
	// Remove function calls first, then check for operators
	// Simple approach: if it's just a single function call, it shouldn't be treated as complex
	trimmed := strings.TrimSpace(expr)

	// If it starts with a function name and ends with ), it's likely a simple function call
	if match := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\s*\([^)]*\)$`).FindString(trimmed); match == trimmed {
		return false
	}

	// Check for operators
	return containsOperators(expr)
}

// parseComplexAggregationExpression parses expressions containing multiple aggregation functions
func parseComplexAggregationExpression(expr string) ([]types.AggregationFieldInfo, string, error) {
	return parseComplexAggExpressionInternal(expr)
}

// parseComplexAggExpressionInternal implements the actual parsing logic
func parseComplexAggExpressionInternal(expr string) ([]types.AggregationFieldInfo, string, error) {
	// 首先检测嵌套聚合
	if err := detectNestedAggregation(expr); err != nil {
		return nil, "", err
	}
	
	// 使用改进的递归解析方法
	aggFields, exprTemplate := parseNestedFunctionsInternal(expr, make([]types.AggregationFieldInfo, 0))
	return aggFields, exprTemplate, nil
}

// parseNestedFunctionsInternal 递归解析嵌套函数调用
func parseNestedFunctionsInternal(expr string, aggFields []types.AggregationFieldInfo) ([]types.AggregationFieldInfo, string) {
	// 匹配函数调用，支持大小写不敏感
	pattern := regexp.MustCompile(`(?i)([a-z_]+)\s*\(`)

	// 找到所有函数调用的起始位置
	matches := pattern.FindAllStringSubmatchIndex(expr, -1)
	if len(matches) == 0 {
		return aggFields, expr
	}

	// 从右到左处理，避免索引偏移问题
	for i := len(matches) - 1; i >= 0; i-- {
		match := matches[i]
		funcStart := match[0]
		funcName := strings.ToLower(expr[match[2]:match[3]])

		// 找到匹配的右括号
		parenStart := match[3]
		parenEnd := findMatchingParenInternal(expr, parenStart)
		if parenEnd == -1 {
			continue
		}

		fullFuncCall := expr[funcStart : parenEnd+1]
		funcParam := expr[parenStart+1 : parenEnd]

		// 检查是否是聚合函数
		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				// 生成唯一占位符
				callHash := 0
				for _, c := range fullFuncCall {
					callHash = callHash*31 + int(c)
				}
				if callHash < 0 {
					callHash = -callHash
				}
				placeholder := fmt.Sprintf("__%s_%d__", funcName, callHash)

				// 解析函数参数
				inputField := strings.TrimSpace(funcParam)
				// 对于聚合函数，如果参数包含嵌套函数调用，保留完整参数
				// 只有在参数是简单的逗号分隔列表时才进行分割
				if strings.Contains(funcParam, ",") && !containsNestedFunctions(funcParam) {
					params := strings.Split(funcParam, ",")
					if len(params) > 0 {
						inputField = strings.TrimSpace(params[0])
					}
				}

				// 添加到聚合字段列表
				fieldInfo := types.AggregationFieldInfo{
					FuncName:    funcName,
					InputField:  inputField,
					Placeholder: placeholder,
					AggType:     aggregator.AggregateType(funcName),
					FullCall:    fullFuncCall,
				}
				aggFields = append(aggFields, fieldInfo)

				// 替换表达式中的聚合函数调用
				expr = expr[:funcStart] + placeholder + expr[parenEnd+1:]
			}
		}
	}

	return aggFields, expr
}

// containsNestedFunctions 检查参数字符串是否包含嵌套函数调用
func containsNestedFunctions(param string) bool {
	// 简单检查：如果包含函数名模式后跟括号，则认为是嵌套函数
	pattern := regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*\s*\(`)
	return pattern.MatchString(param)
}

// findMatchingParenInternal 找到匹配的右括号
func findMatchingParenInternal(s string, start int) int {
	if start >= len(s) || s[start] != '(' {
		return -1
	}

	count := 1
	for i := start + 1; i < len(s); i++ {
		switch s[i] {
		case '(':
			count++
		case ')':
			count--
			if count == 0 {
				return i
			}
		}
	}
	return -1 // 未找到匹配的右括号
}
