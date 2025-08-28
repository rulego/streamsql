package aggregator

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/rulego/streamsql/functions"
)

// PostAggregationExpression represents an expression that needs to be evaluated after aggregation
type PostAggregationExpression struct {
	OutputField       string   // 输出字段名
	Expression        string   // 表达式模板，如 "__first_value_0__ - __last_value_1__"
	RequiredAggFields []string // 依赖的聚合字段，如 ["__first_value_0__", "__last_value_1__"]
	OriginalExpr      string   // 原始表达式，用于调试
}

// PostAggregationProcessor handles expressions that contain aggregation functions
type PostAggregationProcessor struct {
	expressions []PostAggregationExpression
	mu          sync.RWMutex
}

// NewPostAggregationProcessor creates a new post-aggregation processor
func NewPostAggregationProcessor() *PostAggregationProcessor {
	return &PostAggregationProcessor{
		expressions: make([]PostAggregationExpression, 0),
	}
}

// AddExpression adds a post-aggregation expression
func (p *PostAggregationProcessor) AddExpression(outputField, originalExpr string, aggFields []string, exprTemplate string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.expressions = append(p.expressions, PostAggregationExpression{
		OutputField:       outputField,
		Expression:        exprTemplate,
		RequiredAggFields: aggFields,
		OriginalExpr:      originalExpr,
	})
}

// ProcessResults processes aggregation results and evaluates post-aggregation expressions
func (p *PostAggregationProcessor) ProcessResults(results []map[string]interface{}) ([]map[string]interface{}, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.expressions) == 0 {
		return results, nil
	}

	// Process each result row
	for i, result := range results {
		// Collect all fields used by expressions for cleanup
		fieldsToCleanup := make(map[string]bool)

		for _, expr := range p.expressions {
			// Check if all required aggregation fields are present
			allPresent := true
			var missingFields []string
			for _, field := range expr.RequiredAggFields {
				if _, exists := result[field]; !exists {
					allPresent = false
					missingFields = append(missingFields, field)
				}
			}

			if !allPresent {
				// Log missing fields for debugging (can be removed in production)
				// fmt.Printf("Missing fields for expression %s -> %s: %v\n", expr.OriginalExpr, expr.OutputField, missingFields)
				// Set to nil if not all required fields are present
				result[expr.OutputField] = nil
				continue
			}

			// Evaluate the expression using the aggregated values
			exprResult, err := p.evaluateExpression(expr.Expression, result)
			if err != nil {
				result[expr.OutputField] = nil
			} else {
				result[expr.OutputField] = exprResult
			}

			// Mark fields for cleanup (only if expression was successful)
			if err == nil {
				for _, field := range expr.RequiredAggFields {
					if strings.HasPrefix(field, "__") && strings.HasSuffix(field, "__") {
						fieldsToCleanup[field] = true
					}
				}
			}
		}

		// Clean up intermediate aggregation fields after all expressions are processed
		for field := range fieldsToCleanup {
			delete(result, field)
		}

		results[i] = result
	}

	return results, nil
}

// evaluateExpression evaluates an expression using aggregated values
func (p *PostAggregationProcessor) evaluateExpression(expression string, data map[string]interface{}) (interface{}, error) {

	// Use the function bridge to evaluate the expression
	bridge := functions.GetExprBridge()
	result, err := bridge.EvaluateExpression(expression, data)
	if err != nil {
		return nil, err
	}

	// Unwrap nested slices that might be returned by expr library
	// This handles cases where expr returns []interface{}([]interface{}(nil)) instead of nil
	result = p.unwrapNestedSlices(result)

	return result, nil
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

// parseNestedFunctionsWithDepth 递归解析嵌套函数调用，支持深度控制
func parseNestedFunctionsWithDepth(expr string, aggFields []AggregationFieldInfo, depth int) ([]AggregationFieldInfo, string) {
	// 对于复杂聚合表达式，我们需要特殊处理：
	// - 最外层的聚合函数应该保留在表达式模板中（用于后聚合）
	// - 内层的聚合函数应该被替换为占位符（用于预聚合）

	// 首先检查是否是最外层的单一聚合函数调用
	isTopLevelSingleAggregation := (depth == 0 && isTopLevelAggregationFunction(expr))

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
		parenStart := match[3] // '(' 的位置
		parenEnd := findMatchingParen(expr, parenStart)
		if parenEnd == -1 {
			continue // 无效的函数调用，跳过
		}

		fullFuncCall := expr[funcStart : parenEnd+1]
		funcParam := expr[parenStart+1 : parenEnd]

		// 检查是否是聚合函数
		if fn, exists := functions.Get(funcName); exists {
			// 只处理真正的聚合、分析和窗口函数
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				// 如果是最外层的单一聚合函数，跳过替换，但仍需要递归处理其参数
				// 注意：由于我们是从右到左处理，i == 0 才是最外层的函数
				if isTopLevelSingleAggregation && i == 0 {

					// 递归处理函数参数
					innerAggFields, processedParam := parseNestedFunctionsWithDepth(funcParam, aggFields, depth+1)
					aggFields = innerAggFields

					// 重构表达式，保持外层函数但使用处理过的参数
					expr = expr[:parenStart+1] + processedParam + expr[parenEnd:]

					continue
				}
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
				inputField := funcParam

				// 对于包含逗号的参数，需要判断是否为多参数函数
				// 使用函数接口来判断而不是硬编码函数名
				if strings.Contains(funcParam, ",") {
					// 检查函数的最大参数数量来判断是否需要多参数处理
					needsMultiParamHandling := false
					if fn, exists := functions.Get(funcName); exists {
						minArgs := fn.GetMinArgs()

						// 对于聚合函数，主要看最小参数数量
						// 如果最小参数数量大于1，则肯定需要多参数处理
						// 如果最小参数数量为1，则通常是单参数函数，即使参数中包含逗号也应视为单个表达式
						if minArgs > 1 {
							needsMultiParamHandling = true
						}
						// 特殊情况：某些分析函数虽然minArgs为1，但确实需要多参数处理
						// 这些函数通常有特定的参数模式，可以通过函数名或其他特征识别
						// 但大多数聚合函数（max, min, sum, avg等）都是单参数的

					}

					if needsMultiParamHandling {
						// 对于真正的多参数函数，使用第一个参数作为输入字段
						params := strings.Split(funcParam, ",")
						if len(params) > 0 {
							inputField = strings.TrimSpace(params[0])
						}
					}
					// 否则保持完整的参数表达式（对于单参数函数，即使参数中包含逗号）
				}

				// 添加到聚合字段列表
				fieldInfo := AggregationFieldInfo{
					FuncName:    funcName,
					InputField:  inputField,
					Placeholder: placeholder,
					AggType:     AggregateType(funcName),
					FullCall:    fullFuncCall,
				}
				aggFields = append(aggFields, fieldInfo)

				// 替换表达式中的聚合函数调用
				expr = expr[:funcStart] + placeholder + expr[parenEnd+1:]
			default:
				// 对于非聚合函数（如数学函数round），递归处理其参数
				// 但保持函数本身不变
				innerAggFields, processedParam := parseNestedFunctionsWithDepth(funcParam, aggFields, depth+1)
				aggFields = innerAggFields

				// 重构表达式，保持函数但使用处理过的参数
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
	start := strings.Index(funcCall, "(")
	end := strings.LastIndex(funcCall, ")")
	if start == -1 || end == -1 || end <= start+1 {
		return false
	}
	params := funcCall[start+1 : end]
	level := 0
	count := 1
	for i := 0; i < len(params); i++ {
		switch params[i] {
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
	return count > 1
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
