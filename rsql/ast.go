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
	SelectAll bool // 新增：标识是否是SELECT *查询
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

// ToStreamConfig 将AST转换为Stream配置
func (s *SelectStatement) ToStreamConfig() (*types.Config, string, error) {
	if s.Source == "" {
		return nil, "", fmt.Errorf("missing FROM clause")
	}

	// 解析窗口配置
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
		return nil, "", fmt.Errorf("解析窗口参数失败: %w", err)
	}

	// 检查是否需要窗口处理
	needWindow := s.Window.Type != ""
	var simpleFields []string

	// 检查是否有聚合函数
	hasAggregation := false
	for _, field := range s.Fields {
		if isAggregationFunction(field.Expression) {
			hasAggregation = true
			break
		}
	}

	// 如果没有指定窗口但有聚合函数，默认使用滚动窗口
	if !needWindow && hasAggregation {
		needWindow = true
		windowType = window.TypeTumbling
		params = map[string]interface{}{
			"size": 10 * time.Second, // 默认10秒窗口
		}
	}

	// 处理 SessionWindow 的特殊配置
	var groupByKey string
	if windowType == window.TypeSession && len(s.GroupBy) > 0 {
		// 对于会话窗口，使用第一个 GROUP BY 字段作为会话键
		groupByKey = s.GroupBy[0]
	}

	// 如果没有聚合函数，收集简单字段
	if !hasAggregation {
		// 如果是SELECT *查询，设置特殊标记
		if s.SelectAll {
			simpleFields = append(simpleFields, "*")
		} else {
			for _, field := range s.Fields {
				fieldName := field.Expression
				if field.Alias != "" {
					// 如果有别名，用别名作为字段名
					simpleFields = append(simpleFields, fieldName+":"+field.Alias)
				} else {
					// 对于没有别名的字段，检查是否为字符串字面量
					_, n, _, _ := ParseAggregateTypeWithExpression(fieldName)
					if n != "" {
						// 如果是字符串字面量，使用解析出的字段名（去掉引号）
						simpleFields = append(simpleFields, n)
					} else {
						// 否则使用原始表达式
						simpleFields = append(simpleFields, fieldName)
					}
				}
			}
		}
		logger.Debug("收集简单字段: %v", simpleFields)
	}

	// 构建字段映射和表达式信息
	aggs, fields, expressions := buildSelectFieldsWithExpressions(s.Fields)

	// 提取字段顺序信息
	fieldOrder := extractFieldOrder(s.Fields)

	// 构建Stream配置
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

// 判断表达式是否是聚合函数
func isAggregationFunction(expr string) bool {
	// 提取函数名
	funcName := extractFunctionName(expr)
	if funcName == "" {
		return false
	}

	// 检查是否是注册的函数
	if fn, exists := functions.Get(funcName); exists {
		// 根据函数类型判断是否需要聚合处理
		switch fn.GetType() {
		case functions.TypeAggregation:
			// 聚合函数需要聚合处理
			return true
		case functions.TypeAnalytical:
			// 分析函数也需要聚合处理（状态管理）
			return true
		case functions.TypeWindow:
			// 窗口函数需要聚合处理
			return true

		default:
			// 其他类型的函数（字符串、转换等）不需要聚合处理
			return false
		}
	}

	// 对于未注册的函数，检查是否是expr-lang内置函数
	// 这些函数通过ExprBridge处理，不需要聚合模式
	bridge := functions.GetExprBridge()
	if bridge.IsExprLangFunction(funcName) {
		return false
	}

	// 如果不是注册的函数也不是expr-lang函数，但包含括号，保守起见认为可能是聚合函数
	if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
		return true
	}
	return false
}

// extractFieldOrder 从Fields切片中提取字段的原始顺序
// 返回按SELECT语句中出现顺序排列的字段名列表
func extractFieldOrder(fields []Field) []string {
	var fieldOrder []string
	
	for _, field := range fields {
		// 如果有别名，使用别名作为字段名
		if field.Alias != "" {
			fieldOrder = append(fieldOrder, field.Alias)
		} else {
			// 没有别名时，尝试解析表达式获取字段名
			_, fieldName, _, _ := ParseAggregateTypeWithExpression(field.Expression)
			if fieldName != "" {
				// 如果解析出字段名（如字符串字面量），使用解析出的名称
				fieldOrder = append(fieldOrder, fieldName)
			} else {
				// 否则使用原始表达式作为字段名
				fieldOrder = append(fieldOrder, field.Expression)
			}
		}
	}
	
	return fieldOrder
}
func extractGroupFields(s *SelectStatement) []string {
	var fields []string
	for _, f := range s.GroupBy {
		if !strings.Contains(f, "(") { // 排除聚合函数
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
				// 使用别名作为聚合器的key，而不是字段名
				selectFields[alias] = t

				// 字段映射：输出字段名(别名) -> 输入字段名（保持与buildSelectFieldsWithExpressions一致）
				if n != "" {
					fieldMap[alias] = n
				} else {
					// 如果没有提取到字段名，使用别名本身
					fieldMap[alias] = alias
				}
			}
		} else {
			// 没有别名的情况，使用表达式本身作为字段名
			t, n, _, _ := ParseAggregateTypeWithExpression(f.Expression)
			if t != "" && n != "" {
				selectFields[n] = t
				fieldMap[n] = n
			}
		}
	}
	return selectFields, fieldMap
}

// 解析聚合函数，并返回表达式信息
func ParseAggregateTypeWithExpression(exprStr string) (aggType aggregator.AggregateType, name string, expression string, allFields []string) {
	// 特殊处理 CASE 表达式
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(exprStr)), "CASE") {
		// CASE 表达式作为特殊的表达式处理
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", "", exprStr, allFields
	}

	// 检查是否是嵌套函数
	if hasNestedFunctions(exprStr) {
		// 嵌套函数情况，提取所有函数
		funcs := extractAllFunctions(exprStr)

		// 查找聚合函数
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
			// 有聚合函数的嵌套表达式，整个表达式作为expression处理
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
			return aggregator.AggregateType(aggregationFunc), "", exprStr, allFields
		} else {
			// 没有聚合函数的嵌套表达式，作为普通表达式处理
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
			return "expression", "", exprStr, allFields
		}
	}

	// 单一函数的原有逻辑
	// 提取函数名
	funcName := extractFunctionName(exprStr)
	if funcName == "" {
		// 检查是否是字符串字面量
		trimmed := strings.TrimSpace(exprStr)
		if (strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'")) ||
			(strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"")) {
			// 字符串字面量：使用去掉引号的内容作为字段名
			fieldName := trimmed[1 : len(trimmed)-1]
			return "expression", fieldName, exprStr, nil
		}
		
		// 如果不是函数调用，但包含运算符或关键字，可能是表达式
		if strings.ContainsAny(exprStr, "+-*/<>=!&|") ||
			strings.Contains(strings.ToUpper(exprStr), "AND") ||
			strings.Contains(strings.ToUpper(exprStr), "OR") {
			// 作为表达式处理
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
			return "expression", "", exprStr, allFields
		}
		return "", "", "", nil
	}

	// 检查是否是注册的函数
	fn, exists := functions.Get(funcName)
	if !exists {
		return "", "", "", nil
	}

	// 提取函数参数和表达式信息
	name, expression, allFields = extractAggFieldWithExpression(exprStr, funcName)

	// 根据函数类型决定聚合类型
	switch fn.GetType() {
	case functions.TypeAggregation:
		// 聚合函数：使用函数名作为聚合类型
		return aggregator.AggregateType(funcName), name, expression, allFields

	case functions.TypeAnalytical:
		// 分析函数：使用函数名作为聚合类型
		return aggregator.AggregateType(funcName), name, expression, allFields

	case functions.TypeWindow:
		// 窗口函数：使用函数名作为聚合类型
		return aggregator.AggregateType(funcName), name, expression, allFields

	case functions.TypeString, functions.TypeConversion, functions.TypeCustom, functions.TypeMath:
		// 字符串函数、转换函数、自定义函数、数学函数：在聚合查询中作为表达式处理
		// 使用 "expression" 作为特殊的聚合类型，表示这是一个表达式计算
		// 对于这些函数，应该保存完整的函数调用作为表达式，而不是只保存参数部分
		fullExpression := exprStr
		if parsedExpr, err := expr.NewExpression(fullExpression); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", name, fullExpression, allFields

	default:
		// 其他类型的函数不使用聚合
		// 这些函数将在非窗口模式下直接处理
		return "", "", "", nil
	}
}

// extractFunctionName 从表达式中提取函数名
func extractFunctionName(expr string) string {
	// 查找第一个左括号
	parenIndex := strings.Index(expr, "(")
	if parenIndex == -1 {
		return ""
	}

	// 提取函数名部分
	funcName := strings.TrimSpace(expr[:parenIndex])

	// 如果函数名包含其他运算符或空格，说明不是简单的函数调用
	if strings.ContainsAny(funcName, " +-*/=<>!&|") {
		return ""
	}

	return funcName
}

// 提取表达式中的所有函数名
func extractAllFunctions(expr string) []string {
	var funcNames []string

	// 简单的函数名匹配
	i := 0
	for i < len(expr) {
		// 查找函数名模式
		start := i
		for i < len(expr) && (expr[i] >= 'a' && expr[i] <= 'z' || expr[i] >= 'A' && expr[i] <= 'Z' || expr[i] == '_') {
			i++
		}

		if i < len(expr) && expr[i] == '(' && i > start {
			// 找到可能的函数名
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

// 检查表达式是否包含嵌套函数
func hasNestedFunctions(expr string) bool {
	funcs := extractAllFunctions(expr)
	return len(funcs) > 1
}

// 提取聚合函数字段，并解析表达式信息
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

	// 提取括号内的表达式
	fieldExpr := strings.TrimSpace(exprStr[start:end])

	// 特殊处理count(*)的情况
	if strings.ToLower(funcName) == "count" && fieldExpr == "*" {
		return "*", "", nil
	}

	// 检查是否是简单字段名（只包含字母、数字、下划线）
	isSimpleField := true
	for _, char := range fieldExpr {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			isSimpleField = false
			break
		}
	}

	// 如果是简单字段，直接返回字段名，不创建表达式
	if isSimpleField {
		return fieldExpr, "", nil
	}

	// 对于复杂表达式，包括多参数函数调用
	expression = fieldExpr

	// 对于CONCAT等字符串函数，直接保存完整表达式
	if strings.ToLower(funcName) == "concat" {
		// 智能解析CONCAT函数的参数来提取字段名
		var fields []string
		params := parseSmartParameters(fieldExpr)
		for _, param := range params {
			param = strings.TrimSpace(param)
			// 如果参数不是字符串常量（不被引号包围），则认为是字段名
			if !((strings.HasPrefix(param, "'") && strings.HasSuffix(param, "'")) ||
				(strings.HasPrefix(param, "\"") && strings.HasSuffix(param, "\""))) {
				if isIdentifier(param) {
					fields = append(fields, param)
				}
			}
		}
		if len(fields) > 0 {
			// 对于CONCAT函数，保存完整的函数调用作为表达式
			return fields[0], funcName + "(" + fieldExpr + ")", fields
		}
		// 如果没有找到字段，返回空字段名但保留表达式
		return "", funcName + "(" + fieldExpr + ")", nil
	}

	// 使用表达式引擎解析
	parsedExpr, err := expr.NewExpression(fieldExpr)
	if err != nil {
		// 如果表达式解析失败，尝试手动解析参数
		// 这主要用于处理多参数函数如distance(x1, y1, x2, y2)
		if strings.Contains(fieldExpr, ",") {
			// 分割参数
			params := strings.Split(fieldExpr, ",")
			var fields []string
			for _, param := range params {
				param = strings.TrimSpace(param)
				if isIdentifier(param) {
					fields = append(fields, param)
				}
			}
			if len(fields) > 0 {
				// 对于多参数函数，使用所有参数字段，主字段名为第一个参数
				return fields[0], expression, fields
			}
		}

		// 如果还是解析失败，尝试使用简单方法提取
		fieldName = extractSimpleField(fieldExpr)
		return fieldName, expression, []string{fieldName}
	}

	// 获取表达式中引用的所有字段
	allFields = parsedExpr.GetFields()

	// 如果只有一个字段，直接返回
	if len(allFields) == 1 {
		return allFields[0], expression, allFields
	}

	// 如果有多个字段，使用第一个字段名作为主字段
	if len(allFields) > 0 {
		// 记录完整表达式和所有字段
		logger.Debug("复杂表达式 '%s' 包含多个字段: %v", fieldExpr, allFields)
		return allFields[0], expression, allFields
	}

	// 如果没有字段（纯常量表达式），返回整个表达式作为字段名
	return fieldExpr, expression, nil
}

// parseSmartParameters 智能解析函数参数，正确处理引号内的逗号
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
				// 参数分隔符
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

	// 添加最后一个参数
	if current.Len() > 0 {
		params = append(params, current.String())
	}

	return params
}

// isIdentifier 检查字符串是否是有效的标识符
func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}
	// 第一个字符必须是字母或下划线
	if !((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') || s[0] == '_') {
		return false
	}
	// 其余字符必须是字母、数字或下划线
	for i := 1; i < len(s); i++ {
		char := s[i]
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	return true
}

// 提取简单字段（向后兼容）
func extractSimpleField(fieldExpr string) string {
	// 如果包含运算符，提取第一个操作数作为字段名
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
			// SessionWindow 的第一个参数是 timeout
			if index == 0 {
				key = "timeout"
			} else {
				key = fmt.Sprintf("param%d", index)
			}
		} else {
			// 其他窗口类型的参数
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
			return nil, fmt.Errorf("%s参数必须为字符串格式(如'5s')", s)
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

// 解析包括表达式在内的字段信息
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
				// 使用别名作为键，这样每个聚合函数都有唯一的键
				selectFields[alias] = t

				// 字段映射：输出字段名 -> 输入字段名（直接为聚合器准备正确的映射）
				if n != "" {
					fieldMap[alias] = n
				} else {
					// 如果没有提取到字段名，使用别名本身
					fieldMap[alias] = alias
				}

				// 如果存在表达式，保存表达式信息
				if expression != "" {
					expressions[alias] = types.FieldExpression{
						Field:      n,
						Expression: expression,
						Fields:     allFields,
					}
				}
			}
		} else {
			// 没有别名的情况，使用表达式本身作为字段名
			t, n, expression, allFields := ParseAggregateTypeWithExpression(f.Expression)
			if t != "" && n != "" {
				// 对于字符串字面量，使用解析出的字段名（去掉引号）作为键
				selectFields[n] = t
				fieldMap[n] = n

				// 如果存在表达式，保存表达式信息
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
