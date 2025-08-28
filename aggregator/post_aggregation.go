package aggregator

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/rulego/streamsql/functions"
)

// Configuration constants for post-aggregation processing
const (
	// PlaceholderPrefix defines the prefix for aggregation field placeholders
	PlaceholderPrefix = "__"
	// PlaceholderSuffix defines the suffix for aggregation field placeholders
	PlaceholderSuffix = "__"
	// HashMultiplier is used for generating unique hash values for function calls
	HashMultiplier = 31
	// MaxFunctionNameLength defines the maximum allowed length for function names
	MaxFunctionNameLength = 100
	// MaxExpressionDepth defines the maximum nesting depth for expression parsing
	MaxExpressionDepth = 50
)

var (
	// funcCallRegex is a compiled regex for function calls, cached for performance
	funcCallRegex = regexp.MustCompile(`(?i)([a-z_]+)\s*\(`)
	// placeholderRegex is a compiled regex for placeholder detection
	placeholderRegex = regexp.MustCompile(`^` + regexp.QuoteMeta(PlaceholderPrefix) + `.*` + regexp.QuoteMeta(PlaceholderSuffix) + `$`)
)

// PostAggregationExpression represents an expression that needs to be evaluated after aggregation
type PostAggregationExpression struct {
	OutputField       string                    // 输出字段名
	Expression        string                    // 表达式模板，如 "__first_value_0__ - __last_value_1__"
	RequiredAggFields []string                  // 依赖的聚合字段，如 ["__first_value_0__", "__last_value_1__"]
	OriginalExpr      string                    // 原始表达式，用于调试
	processor         *PostAggregationProcessor // 处理器引用
}

// Evaluate 评估后聚合表达式
func (pae *PostAggregationExpression) Evaluate(data map[string]interface{}) (interface{}, error) {
	if pae == nil {
		return nil, fmt.Errorf("post-aggregation expression is nil")
	}
	if pae.processor == nil {
		return nil, fmt.Errorf("post-aggregation processor not initialized")
	}
	if strings.TrimSpace(pae.Expression) == "" {
		return nil, fmt.Errorf("expression cannot be empty")
	}
	if data == nil {
		return nil, fmt.Errorf("evaluation data cannot be nil")
	}
	return pae.processor.evaluateExpression(pae.Expression, data)
}

// PostAggregationProcessor handles expressions that contain aggregation functions
type PostAggregationProcessor struct {
	expressions []PostAggregationExpression
	mu          sync.RWMutex
	exprBridge  *functions.ExprBridge
	fieldsCache map[string][]string
}

// NewPostAggregationProcessor creates a new post-aggregation processor
func NewPostAggregationProcessor() *PostAggregationProcessor {
	return &PostAggregationProcessor{
		expressions: make([]PostAggregationExpression, 0),
		exprBridge:  functions.GetExprBridge(),
		fieldsCache: make(map[string][]string),
	}
}

// AddExpression adds a post-aggregation expression
func (p *PostAggregationProcessor) AddExpression(outputField, originalExpr string, aggFields []string, exprTemplate string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	expr := PostAggregationExpression{
		OutputField:       outputField,
		Expression:        exprTemplate,
		RequiredAggFields: aggFields,
		OriginalExpr:      originalExpr,
		processor:         p,
	}
	p.expressions = append(p.expressions, expr)
	p.fieldsCache[outputField] = aggFields
}

// ProcessResults processes aggregation results and evaluates post-aggregation expressions
func (p *PostAggregationProcessor) ProcessResults(results []map[string]interface{}) ([]map[string]interface{}, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.expressions) == 0 {
		return results, nil
	}

	// Pre-allocate cleanup fields map to avoid repeated allocations
	fieldsToCleanup := make(map[string]bool, len(p.expressions)*2)

	// Collect all placeholder fields that need cleanup
	for j := range p.expressions {
		expr := &p.expressions[j]
		p.markPlaceholderFields(expr.RequiredAggFields, fieldsToCleanup)
	}

	// Process each result row
	for i := range results {
		result := results[i]

		for j := range p.expressions {
			expr := &p.expressions[j]
			// Fast path: check required fields presence
			allPresent := p.checkRequiredFields(result, expr.RequiredAggFields)

			if !allPresent {
				result[expr.OutputField] = nil
				continue
			}

			// Evaluate expression
			exprResult, err := p.evaluateExpressionFast(expr.Expression, result)
			if err != nil {
				result[expr.OutputField] = nil
			} else {
				result[expr.OutputField] = exprResult
			}
		}

		// Batch cleanup of placeholder fields
		for field := range fieldsToCleanup {
			delete(result, field)
		}
	}

	return results, nil
}

// checkRequiredFields checks if all required fields are present in the result
func (p *PostAggregationProcessor) checkRequiredFields(result map[string]interface{}, requiredFields []string) bool {
	for _, field := range requiredFields {
		if _, exists := result[field]; !exists {
			return false
		}
	}
	return true
}

// markPlaceholderFields marks placeholder fields for cleanup
func (p *PostAggregationProcessor) markPlaceholderFields(requiredFields []string, fieldsToCleanup map[string]bool) {
	for _, field := range requiredFields {
		if placeholderRegex.MatchString(field) {
			fieldsToCleanup[field] = true
		}
	}
}

// evaluateExpressionFast evaluates an expression using cached bridge
func (p *PostAggregationProcessor) evaluateExpressionFast(expression string, data map[string]interface{}) (interface{}, error) {
	result, err := p.exprBridge.EvaluateExpression(expression, data)
	if err != nil {
		return nil, err
	}
	return p.unwrapNestedSlices(result), nil
}

// evaluateExpression evaluates an expression using aggregated values
func (p *PostAggregationProcessor) evaluateExpression(expression string, data map[string]interface{}) (interface{}, error) {
	return p.evaluateExpressionFast(expression, data)
}

// unwrapNestedSlices recursively unwraps nested empty slices to get the actual value
func (p *PostAggregationProcessor) unwrapNestedSlices(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	// Check if it's a slice
	if slice, ok := value.([]interface{}); ok {
		// If it's an empty slice or contains only nil, return nil
		if len(slice) == 0 {
			return nil
		}
		// If it contains only one element, recursively unwrap it
		if len(slice) == 1 {
			return p.unwrapNestedSlices(slice[0])
		}
		// If it contains multiple elements, return as is
		return slice
	}

	// For non-slice values, return as is
	return value
}

// ParseComplexAggregationExpression parses expressions containing multiple aggregation functions
// Returns the list of required aggregation fields and the expression template
// 该函数将包含聚合函数的复杂表达式分解为：
// 1. 后聚合表达式模板（聚合函数被占位符替换）
// 2. 需要预先计算的聚合字段信息列表
// 3. 错误信息（如果解析失败）
//
// 示例：
//
//	输入: "SUM(price) + AVG(quantity) * 2"
//	输出: 表达式模板 "__SUM_123__ + __AVG_456__ * 2"
//	      聚合字段 [{FieldName: "__SUM_123__", FunctionName: "SUM", Arguments: ["price"]}, ...]
func ParseComplexAggregationExpression(expr string) (aggFields []AggregationFieldInfo, exprTemplate string, err error) {
	exprTemplate = expr

	// 使用递归方法解析嵌套函数调用
	aggFields, exprTemplate = parseNestedFunctions(expr, make([]AggregationFieldInfo, 0))

	return aggFields, exprTemplate, nil
}

// parseNestedFunctions 递归解析嵌套函数调用
func parseNestedFunctions(expr string, aggFields []AggregationFieldInfo) ([]AggregationFieldInfo, string) {
	return parseNestedFunctionsWithDepth(expr, aggFields, 0)
}

// findFunctionCalls 查找表达式中的所有函数调用
func findFunctionCalls(expr string) [][]int {
	return funcCallRegex.FindAllStringSubmatchIndex(expr, -1)
}

// generatePlaceholder 为函数调用生成唯一占位符
func generatePlaceholder(funcName, fullFuncCall string) string {
	callHash := uint32(0)
	for i := 0; i < len(fullFuncCall); i++ {
		callHash = callHash*HashMultiplier + uint32(fullFuncCall[i])
	}
	return PlaceholderPrefix + funcName + "_" + strconv.FormatUint(uint64(callHash), 10) + PlaceholderSuffix
}

// parseNestedFunctionsWithDepth 递归解析嵌套函数调用，支持深度控制
func parseNestedFunctionsWithDepth(expr string, aggFields []AggregationFieldInfo, depth int) ([]AggregationFieldInfo, string) {
	if depth > MaxExpressionDepth {
		return aggFields, expr
	}

	isTopLevelSingleAggregation := (depth == 0 && isTopLevelAggregationFunction(expr))
	matches := findFunctionCalls(expr)
	if len(matches) == 0 {
		return aggFields, expr
	}

	// 从右到左处理，避免索引偏移问题
	for i := len(matches) - 1; i >= 0; i-- {
		match := matches[i]
		funcStart := match[0]
		funcName := strings.ToLower(expr[match[2]:match[3]])

		parenStart := match[3]
		parenEnd := findMatchingParen(expr, parenStart)
		if parenEnd == -1 {
			continue
		}

		fullFuncCall := expr[funcStart : parenEnd+1]
		funcParam := expr[parenStart+1 : parenEnd]

		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				if isTopLevelSingleAggregation && i == 0 {
					innerAggFields, processedParam := parseNestedFunctionsWithDepth(funcParam, aggFields, depth+1)
					aggFields = innerAggFields
					expr = expr[:parenStart+1] + processedParam + expr[parenEnd:]
					continue
				}

				placeholder := generatePlaceholder(funcName, fullFuncCall)
				inputField := funcParam

				if strings.Contains(funcParam, ",") && fn.GetMinArgs() > 1 {
					if commaIdx := strings.Index(funcParam, ","); commaIdx > 0 {
						inputField = strings.TrimSpace(funcParam[:commaIdx])
					}
				}

				aggFields = append(aggFields, AggregationFieldInfo{
					FuncName:    funcName,
					InputField:  inputField,
					Placeholder: placeholder,
					AggType:     AggregateType(funcName),
					FullCall:    fullFuncCall,
				})

				expr = expr[:funcStart] + placeholder + expr[parenEnd+1:]
			default:
				innerAggFields, processedParam := parseNestedFunctionsWithDepth(funcParam, aggFields, depth+1)
				aggFields = innerAggFields
				expr = expr[:parenStart+1] + processedParam + expr[parenEnd:]
			}
		}
	}

	return aggFields, expr
}

// isTopLevelAggregationFunction 检查表达式是否是顶层的单一聚合函数调用
func isTopLevelAggregationFunction(expr string) bool {
	// 提取最外层的函数名
	funcName := extractOutermostFunctionName(expr)
	if funcName == "" {
		return false
	}

	// 检查是否是聚合函数
	if fn, exists := functions.Get(funcName); exists {
		switch fn.GetType() {
		case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
			return true
		}
	}
	return false
}

// extractOutermostFunctionName 提取最外层的函数名
func extractOutermostFunctionName(expr string) string {
	expr = strings.TrimSpace(expr)

	// 查找第一个左括号
	parenIndex := strings.Index(expr, "(")
	if parenIndex == -1 {
		return ""
	}

	// 提取函数名
	funcName := strings.TrimSpace(expr[:parenIndex])

	// 检查函数名是否有效（只包含字母、数字、下划线）
	for _, char := range funcName {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return ""
		}
	}

	return funcName
}

// findMatchingParen 找到匹配的右括号
func findMatchingParen(s string, start int) int {
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

// AggregationFieldInfo holds information about an aggregation function in an expression
type AggregationFieldInfo struct {
	FuncName    string        // 函数名，如 "first_value"
	InputField  string        // 输入字段，如 "displayNum"
	Placeholder string        // 占位符，如 "__first_value_0__"
	AggType     AggregateType // 聚合类型
	FullCall    string        // 完整函数调用，如 "NTH_VALUE(value, 2)"
}

// Enhanced GroupAggregator with post-aggregation support
type EnhancedGroupAggregator struct {
	*GroupAggregator
	postProcessor *PostAggregationProcessor
}

// NewEnhancedGroupAggregator creates a new enhanced group aggregator with post-aggregation support
func NewEnhancedGroupAggregator(groupFields []string, aggregationFields []AggregationField) *EnhancedGroupAggregator {

	baseAggregator := NewGroupAggregator(groupFields, aggregationFields)
	return &EnhancedGroupAggregator{
		GroupAggregator: baseAggregator,
		postProcessor:   NewPostAggregationProcessor(),
	}
}

// AddPostAggregationExpression adds an expression that needs post-aggregation processing
func (ega *EnhancedGroupAggregator) AddPostAggregationExpression(outputField, originalExpr string, requiredFields []AggregationFieldInfo) error {
	// Validate input parameters
	if strings.TrimSpace(originalExpr) == "" {
		return fmt.Errorf("expression cannot be empty")
	}

	// Check for malformed expressions (basic validation)
	if strings.Count(originalExpr, "(") != strings.Count(originalExpr, ")") {
		return fmt.Errorf("malformed expression: mismatched parentheses")
	}

	// Validate required fields contain valid function names
	for _, field := range requiredFields {
		if field.FuncName != "" {
			if _, exists := functions.Get(field.FuncName); !exists {
				return fmt.Errorf("invalid function name: %s", field.FuncName)
			}
		}
	}

	// Add individual aggregation fields to the base aggregator (only if not already exists)
	for _, field := range requiredFields {

		// For parameterized functions, always recreate the aggregator with correct parameters
		// even if it already exists (it was created with default parameters)
		// A function is considered parameterized if it needs multiple parameters to configure its behavior
		isParameterized := false
		if fn, exists := functions.Get(field.FuncName); exists {
			minArgs := fn.GetMinArgs()
			maxArgs := fn.GetMaxArgs()
			// Function is parameterized if:
			// 1. It requires more than 1 parameter (minArgs > 1), OR
			// 2. It has optional parameters that can configure its behavior (maxArgs > minArgs && minArgs >= 1)
			isParameterized = minArgs > 1 || (maxArgs > minArgs && minArgs >= 1)
		}

		// Check if field already exists in aggregationFields to avoid duplicates
		fieldExistsInAggFields := false
		for _, existingField := range ega.GroupAggregator.aggregationFields {
			if existingField.OutputAlias == field.Placeholder {
				fieldExistsInAggFields = true
				break
			}
		}

		// Check if input field is an expression (contains function calls)
		isInputExpression := strings.Contains(field.InputField, "(") && strings.Contains(field.InputField, ")")

		// If input expression itself contains aggregation calls, skip creating an aggregator for this field
		// Use dynamic function registry instead of hardcoded list
		containsAggCall := func(s string) bool {
			lower := strings.ToLower(s)
			// Extract potential function names from the expression
			for i := 0; i < len(lower); i++ {
				if lower[i] >= 'a' && lower[i] <= 'z' {
					// Find the end of the function name
					j := i
					for j < len(lower) && (lower[j] >= 'a' && lower[j] <= 'z' || lower[j] == '_') {
						j++
					}
					// Check if it's followed by '(' and is an aggregator function
					if j < len(lower) && lower[j] == '(' {
						funcName := lower[i:j]
						if functions.IsAggregatorFunction(funcName) {
							return true
						}
					}
					i = j
				} else {
					i++
				}
			}
			return false
		}

		// Check if expression is already registered
		hasExpressionRegistered := false
		if ega.GroupAggregator.expressions != nil {
			_, hasExpressionRegistered = ega.GroupAggregator.expressions[field.Placeholder]
		}

		// For parameterized functions, always recreate the aggregator with correct parameters
		// For non-parameterized functions, only add if field doesn't exist
		// For expression fields, always ensure expression is registered
		shouldProcess := (!fieldExistsInAggFields && !isParameterized) || isParameterized || (isInputExpression && !hasExpressionRegistered)
		if isInputExpression && containsAggCall(field.InputField) {
			shouldProcess = false
		}

		if shouldProcess {
			// Debug: log field creation (can be removed in production)
			// fmt.Printf("Creating aggregator for field: %s (%s) -> %s\n", field.FuncName, field.InputField, field.Placeholder)

			// Create aggregation field
			aggField := AggregationField{
				InputField:    field.InputField,
				AggregateType: field.AggType,
				OutputAlias:   field.Placeholder,
			}

			// Add to aggregation fields (only if not duplicate)
			if !fieldExistsInAggFields {
				ega.GroupAggregator.aggregationFields = append(ega.GroupAggregator.aggregationFields, aggField)
			}

			// If input field is an expression, register expression evaluator (only if it does not depend on aggregation)
			if isInputExpression && !containsAggCall(field.InputField) {

				bridge := functions.GetExprBridge()
				ega.GroupAggregator.RegisterExpression(
					field.Placeholder,
					field.InputField,
					[]string{}, // Will be populated by expression parsing
					func(data interface{}) (interface{}, error) {
						if dataMap, ok := data.(map[string]interface{}); ok {
							result, err := bridge.EvaluateExpression(field.InputField, dataMap)

							return result, err
						}
						return nil, fmt.Errorf("unsupported data type: %T", data)
					},
				)
			}

			// Create aggregator instance
			// For parameterized functions, create with parameters only when multiple top-level args are present
			if isParameterized && hasMultipleTopLevelArgs(field.FullCall) {
				aggregator := ega.createParameterizedAggregator(field)
				if aggregator != nil {
					ega.GroupAggregator.aggregators[field.Placeholder] = aggregator
				} else {
					// Fallback to simple aggregator
					ega.GroupAggregator.aggregators[field.Placeholder] = CreateBuiltinAggregator(field.AggType)
				}
			} else {
				ega.GroupAggregator.aggregators[field.Placeholder] = CreateBuiltinAggregator(field.AggType)
			}
		}
	}

	// Extract required field names
	var requiredFieldNames []string
	for _, field := range requiredFields {
		requiredFieldNames = append(requiredFieldNames, field.Placeholder)
	}

	// Build expression template by replacing each full function call with its placeholder
	// This preserves any outer non-aggregation functions (e.g., CEIL(__avg__)) and ensures
	// placeholders exactly match the ones created earlier for requiredFields.
	exprTemplate := originalExpr
	for _, field := range requiredFields {
		exprTemplate = strings.ReplaceAll(exprTemplate, field.FullCall, field.Placeholder)
	}

	// Detect aggregators whose input expressions themselves contain aggregation calls
	// Use dynamic function registry instead of hardcoded list
	containsAggCall := func(s string) bool {
		lower := strings.ToLower(s)
		// Extract potential function names from the expression
		for i := 0; i < len(lower); i++ {
			if lower[i] >= 'a' && lower[i] <= 'z' {
				// Find the end of the function name
				j := i
				for j < len(lower) && (lower[j] >= 'a' && lower[j] <= 'z' || lower[j] == '_') {
					j++
				}
				// Check if it's followed by '(' and is an aggregator function
				if j < len(lower) && lower[j] == '(' {
					funcName := lower[i:j]
					if functions.IsAggregatorFunction(funcName) {
						return true
					}
				}
				i = j
			} else {
				i++
			}
		}
		return false
	}

	// Adjust template and required fields: drop outer aggregators that wrap other aggregations
	adjustedTemplate := exprTemplate
	var adjustedRequired []AggregationFieldInfo
	for _, field := range requiredFields {
		if containsAggCall(field.InputField) {
			// Transform the input by replacing inner full calls with placeholders
			transformed := field.InputField
			for _, inner := range requiredFields {
				if inner.FullCall != field.FullCall {
					transformed = strings.ReplaceAll(transformed, inner.FullCall, inner.Placeholder)
				}
			}
			// Replace the placeholder of this outer aggregator back to the transformed expression
			adjustedTemplate = strings.ReplaceAll(adjustedTemplate, field.Placeholder, transformed)
			// Do NOT keep this outer aggregator in required list (it will not be created)
			continue
		}
		adjustedRequired = append(adjustedRequired, field)
	}
	requiredFields = adjustedRequired

	// Add to post-processor
	ega.postProcessor.AddExpression(outputField, originalExpr, requiredFieldNames, adjustedTemplate)

	return nil
}

// GetResults returns results with post-aggregation expressions evaluated
func (ega *EnhancedGroupAggregator) GetResults() ([]map[string]interface{}, error) {
	// Get base aggregation results
	results, err := ega.GroupAggregator.GetResults()
	if err != nil {
		return nil, err
	}

	// Process post-aggregation expressions
	return ega.postProcessor.ProcessResults(results)
}

// createParameterizedAggregator creates aggregator with parameters for complex functions
// 使用新的接口方法替代硬编码实现
func (ega *EnhancedGroupAggregator) createParameterizedAggregator(field AggregationFieldInfo) AggregatorFunction {
	// Parse function call to extract parameters
	args, err := ega.parseFunctionCall(field.FullCall)
	if err != nil {
		return nil
	}

	// Use the new interface method to create parameterized aggregator
	aggFunc, err := functions.CreateParameterizedAggregator(field.FuncName, args)
	if err != nil {
		return nil
	}

	// Wrap with WindowFunctionWrapper for compatibility
	return &WindowFunctionWrapper{aggFunc: aggFunc}
}

// hasMultipleTopLevelArgs returns true if the function call has more than one top-level argument
func hasMultipleTopLevelArgs(funcCall string) bool {
	// Check if this is a function call with parentheses (starts with identifier followed by parentheses)
	start := strings.Index(funcCall, "(")
	end := strings.LastIndex(funcCall, ")")

	var params string
	var isDirectArgList bool

	// Only treat as function call if it starts with an identifier and has matching parentheses
	if start > 0 && end != -1 && end > start && end == len(funcCall)-1 {
		// Check if everything before the first '(' is a valid identifier (function name)
		funcName := strings.TrimSpace(funcCall[:start])
		if isValidIdentifier(funcName) {
			// Function call format: func(args) - extract only the arguments inside parentheses
			params = funcCall[start+1 : end]
			isDirectArgList = false
		} else {
			// Direct argument list format: arg1, arg2
			params = strings.TrimSpace(funcCall)
			isDirectArgList = true
		}
	} else {
		// Direct argument list format: arg1, arg2
		params = strings.TrimSpace(funcCall)
		if params == "" {
			return false
		}
		isDirectArgList = true
	}

	params = strings.TrimSpace(params)
	if params == "" {
		return false
	}

	// For direct argument lists, special case: if the entire params is wrapped in parentheses
	// and has no top-level commas, it's a single complex argument
	if isDirectArgList && strings.HasPrefix(params, "(") && strings.HasSuffix(params, ")") {
		// Check if this is a complete parenthesized expression
		level := 0
		for i, ch := range params {
			if ch == '(' {
				level++
			} else if ch == ')' {
				level--
				if level == 0 && i == len(params)-1 {
					// This is a single complete parenthesized expression
					return false
				}
			}
		}
	}

	level := 0
	count := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(params); i++ {
		ch := params[i]

		// Handle string literals
		if !inString && (ch == '\'' || ch == '"') {
			inString = true
			stringChar = ch
			continue
		}
		if inString && ch == stringChar {
			inString = false
			stringChar = 0
			continue
		}

		// Skip processing if inside string
		if inString {
			continue
		}

		switch ch {
		case '(':
			level++
		case ')':
			if level > 0 {
				level--
			}
		case ',':
			if level == 0 {
				count++
			}
		}
	}

	// If we found any commas at top level, we have multiple arguments
	return count > 0
}

// isValidIdentifier checks if a string is a valid identifier (function name)
func isValidIdentifier(s string) bool {
	if len(s) == 0 || len(s) > MaxFunctionNameLength {
		return false
	}

	// First character must be letter or underscore
	if !isValidIdentifierStart(s[0]) {
		return false
	}

	// Remaining characters must be letters, digits, or underscores
	for i := 1; i < len(s); i++ {
		if !isValidIdentifierChar(s[i]) {
			return false
		}
	}

	return true
}

// isValidIdentifierStart checks if a character can be used as the start of an identifier
func isValidIdentifierStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

// isValidIdentifierChar checks if a character can be used in an identifier
func isValidIdentifierChar(c byte) bool {
	return isValidIdentifierStart(c) || (c >= '0' && c <= '9')
}


// parseFunctionCall parses a function call string and returns the arguments
func (ega *EnhancedGroupAggregator) parseFunctionCall(funcCall string) ([]interface{}, error) {
	// Find the parentheses
	start := strings.Index(funcCall, "(")
	end := strings.LastIndex(funcCall, ")")
	if start == -1 || end == -1 {
		return nil, fmt.Errorf("invalid function call format: %s", funcCall)
	}

	// Extract parameters string
	paramsStr := strings.TrimSpace(funcCall[start+1 : end])
	if paramsStr == "" {
		return []interface{}{}, nil
	}

	// Split parameters by comma
	paramStrs := strings.Split(paramsStr, ",")
	args := make([]interface{}, len(paramStrs))

	for i, paramStr := range paramStrs {
		paramStr = strings.TrimSpace(paramStr)

		// Try to parse as number first
		if val, err := strconv.Atoi(paramStr); err == nil {
			args[i] = val
		} else if val, err := strconv.ParseFloat(paramStr, 64); err == nil {
			args[i] = val
		} else {
			// Treat as string (remove quotes if present)
			if (strings.HasPrefix(paramStr, "'") && strings.HasSuffix(paramStr, "'")) ||
				(strings.HasPrefix(paramStr, "\"") && strings.HasSuffix(paramStr, "\"")) {
				args[i] = paramStr[1 : len(paramStr)-1]
			} else {
				args[i] = paramStr
			}
		}
	}

	return args, nil
}

// WindowFunctionWrapper wraps window functions to make them compatible with LegacyAggregatorFunction
type WindowFunctionWrapper struct {
	aggFunc functions.AggregatorFunction
}

func (w *WindowFunctionWrapper) New() AggregatorFunction {
	return &WindowFunctionWrapper{aggFunc: w.aggFunc.New()}
}

func (w *WindowFunctionWrapper) Add(value interface{}) {
	w.aggFunc.Add(value)
}

func (w *WindowFunctionWrapper) Result() interface{} {
	return w.aggFunc.Result()
}

func (w *WindowFunctionWrapper) Reset() {
	w.aggFunc.Reset()
}

func (w *WindowFunctionWrapper) Clone() AggregatorFunction {
	return &WindowFunctionWrapper{aggFunc: w.aggFunc.Clone()}
}

// Interface compliance check
var _ Aggregator = (*EnhancedGroupAggregator)(nil)
