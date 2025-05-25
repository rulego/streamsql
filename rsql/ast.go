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

	params, err := parseWindowParams(s.Window.Params)
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
		for _, field := range s.Fields {
			fieldName := field.Expression
			if field.Alias != "" {
				// 如果有别名，用别名作为字段名
				simpleFields = append(simpleFields, fieldName+":"+field.Alias)
			} else {
				simpleFields = append(simpleFields, fieldName)
			}
		}
		logger.Debug("收集简单字段: %v", simpleFields)
	}

	// 构建字段映射和表达式信息
	aggs, fields, expressions := buildSelectFieldsWithExpressions(s.Fields)

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
		case functions.TypeMath:
			// 数学函数在聚合上下文中需要聚合处理
			return true
		default:
			// 其他类型的函数（字符串、转换等）不需要聚合处理
			return false
		}
	}

	// 如果不是注册的函数，但包含括号，保守起见认为可能是函数
	if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
		return true
	}

	return false
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
	fieldExpressions := make(map[string]types.FieldExpression)

	for _, f := range fields {
		if alias := f.Alias; alias != "" {
			t, n, expression, allFields := parseAggregateTypeWithExpression(f.Expression)
			if n != "" {
				selectFields[n] = t
				fieldMap[n] = alias

				// 如果存在表达式，保存表达式信息
				if expression != "" {
					fieldExpressions[n] = types.FieldExpression{
						Field:      n,
						Expression: expression,
						Fields:     allFields,
					}
				}
			} else if t != "" {
				// 只有在聚合类型非空时才添加
				selectFields[alias] = t
			}
			// 如果聚合类型和字段名都为空，不做处理，避免空聚合器类型
		}
	}
	return selectFields, fieldMap
}

// 解析聚合函数，并返回表达式信息
func parseAggregateTypeWithExpression(exprStr string) (aggType aggregator.AggregateType, name string, expression string, allFields []string) {
	// 提取函数名
	funcName := extractFunctionName(exprStr)
	if funcName == "" {
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

	case functions.TypeMath:
		// 数学函数：在聚合上下文中使用avg作为聚合类型
		if expression == "" {
			expression = exprStr
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
		}
		return "avg", name, expression, allFields

	case functions.TypeString, functions.TypeConversion, functions.TypeCustom:
		// 字符串函数、转换函数、自定义函数：在聚合查询中作为表达式处理
		// 使用 "expression" 作为特殊的聚合类型，表示这是一个表达式计算
		if expression == "" {
			expression = exprStr
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
		}
		return "expression", name, expression, allFields

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

// isIdentifier 检查字符串是否是有效的标识符
func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	if !((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') || s[0] == '_') {
		return false
	}

	for i := 1; i < len(s); i++ {
		if !((s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') ||
			(s[i] >= '0' && s[i] <= '9') || s[i] == '_') {
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
	result := make(map[string]interface{})
	var key string
	for index, v := range params {
		if index == 0 {
			key = "size"
		} else if index == 1 {
			key = "slide"
		} else {
			key = "offset"
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
			t, n, expression, allFields := parseAggregateTypeWithExpression(f.Expression)
			if t != "" {
				// 使用别名作为键，这样每个聚合函数都有唯一的键
				selectFields[alias] = t

				// 字段映射：别名 -> 输入字段名
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
			t, n, expression, allFields := parseAggregateTypeWithExpression(f.Expression)
			if t != "" && n != "" {
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
