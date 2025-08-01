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

// fieldProcessInfo 字段处理信息，用于缓存预编译的字段处理逻辑
type fieldProcessInfo struct {
	fieldName       string // 原始字段名
	outputName      string // 输出字段名
	isFunctionCall  bool   // 是否为函数调用
	hasNestedField  bool   // 是否包含嵌套字段
	isSelectAll     bool   // 是否为SELECT *
	isStringLiteral bool   // 是否为字符串字面量
	stringValue     string // 预处理的字符串字面量值（去除引号）
	alias           string // 字段别名，用于快速访问
}

// expressionProcessInfo 表达式处理信息，用于缓存预编译的表达式处理逻辑
type expressionProcessInfo struct {
	originalExpr            string           // 原始表达式
	processedExpr           string           // 预处理后的表达式
	isFunctionCall          bool             // 是否为函数调用
	hasNestedFields         bool             // 是否包含嵌套字段
	compiledExpr            *expr.Expression // 预编译的表达式对象
	needsBacktickPreprocess bool             // 是否需要反引号预处理
}

// compileFieldProcessInfo 预编译字段处理信息，避免运行时重复解析
func (s *Stream) compileFieldProcessInfo() {
	s.compiledFieldInfo = make(map[string]*fieldProcessInfo)
	s.compiledExprInfo = make(map[string]*expressionProcessInfo)

	// 编译SimpleFields信息
	for _, fieldSpec := range s.config.SimpleFields {
		info := s.compileSimpleFieldInfo(fieldSpec)
		s.compiledFieldInfo[fieldSpec] = info
	}

	// 预编译表达式字段信息
	s.compileExpressionInfo()
}

// compileSimpleFieldInfo 编译简单字段信息
func (s *Stream) compileSimpleFieldInfo(fieldSpec string) *fieldProcessInfo {
	info := &fieldProcessInfo{}

	if fieldSpec == "*" {
		info.isSelectAll = true
		info.fieldName = "*"
		info.outputName = "*"
		return info
	}

	// 解析别名
	parts := strings.Split(fieldSpec, ":")
	info.fieldName = parts[0]
	// 去除字段名中的反引号
	if len(info.fieldName) >= 2 && info.fieldName[0] == '`' && info.fieldName[len(info.fieldName)-1] == '`' {
		info.fieldName = info.fieldName[1 : len(info.fieldName)-1]
	}
	info.outputName = info.fieldName
	if len(parts) > 1 {
		info.outputName = parts[1]
		// 去除输出名中的反引号
		if len(info.outputName) >= 2 && info.outputName[0] == '`' && info.outputName[len(info.outputName)-1] == '`' {
			info.outputName = info.outputName[1 : len(info.outputName)-1]
		}
	}

	// 预判断字段特征
	info.isFunctionCall = strings.Contains(info.fieldName, "(") && strings.Contains(info.fieldName, ")")
	info.hasNestedField = !info.isFunctionCall && fieldpath.IsNestedField(info.fieldName)

	// 检查是否为字符串字面量并预处理值
	info.isStringLiteral = (len(info.fieldName) >= 2 &&
		((info.fieldName[0] == '\'' && info.fieldName[len(info.fieldName)-1] == '\'') ||
			(info.fieldName[0] == '"' && info.fieldName[len(info.fieldName)-1] == '"')))

	// 预处理字符串字面量值，去除引号
	if info.isStringLiteral && len(info.fieldName) >= 2 {
		info.stringValue = info.fieldName[1 : len(info.fieldName)-1]
	}

	// 设置别名用于快速访问
	info.alias = info.outputName

	return info
}

// compileExpressionInfo 预编译表达式处理信息
func (s *Stream) compileExpressionInfo() {
	bridge := functions.GetExprBridge()

	for fieldName, fieldExpr := range s.config.FieldExpressions {
		exprInfo := &expressionProcessInfo{
			originalExpr: fieldExpr.Expression,
		}

		// 预处理表达式
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

		// 预判断表达式特征
		exprInfo.isFunctionCall = strings.Contains(fieldExpr.Expression, "(") && strings.Contains(fieldExpr.Expression, ")")
		exprInfo.hasNestedFields = !exprInfo.isFunctionCall && strings.Contains(fieldExpr.Expression, ".")
		exprInfo.needsBacktickPreprocess = bridge.ContainsBacktickIdentifiers(fieldExpr.Expression)

		// 预编译表达式对象（仅对非函数调用的表达式）
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

// processExpressionField 处理表达式字段
func (s *Stream) processExpressionField(fieldName string, dataMap map[string]interface{}, result map[string]interface{}) {
	exprInfo := s.compiledExprInfo[fieldName]
	if exprInfo == nil {
		// 回退到原逻辑（安全性保证）
		s.processExpressionFieldFallback(fieldName, dataMap, result)
		return
	}

	var evalResult interface{}
	bridge := functions.GetExprBridge()

	if exprInfo.isFunctionCall {
		// 对于函数调用，使用桥接器处理
		exprResult, err := bridge.EvaluateExpression(exprInfo.processedExpr, dataMap)
		if err != nil {
			logger.Error("Function call evaluation failed for field %s: %v", fieldName, err)
			result[fieldName] = nil
			return
		}
		evalResult = exprResult
	} else if exprInfo.hasNestedFields {
		// 使用预编译的表达式对象
		if exprInfo.compiledExpr != nil {
			numResult, err := exprInfo.compiledExpr.Evaluate(dataMap)
			if err != nil {
				logger.Error("Expression evaluation failed for field %s: %v", fieldName, err)
				result[fieldName] = nil
				return
			}
			evalResult = numResult
		} else {
			// 回退到动态编译
			s.processExpressionFieldFallback(fieldName, dataMap, result)
			return
		}
	} else {
		// 尝试使用桥接器处理其他表达式
		exprResult, err := bridge.EvaluateExpression(exprInfo.processedExpr, dataMap)
		if err != nil {
			// 如果桥接器失败，使用预编译的表达式对象
			if exprInfo.compiledExpr != nil {
				numResult, evalErr := exprInfo.compiledExpr.Evaluate(dataMap)
				if evalErr != nil {
					logger.Error("Expression evaluation failed for field %s: %v", fieldName, evalErr)
					result[fieldName] = nil
					return
				}
				evalResult = numResult
			} else {
				// 回退到动态编译
				s.processExpressionFieldFallback(fieldName, dataMap, result)
				return
			}
		} else {
			evalResult = exprResult
		}
	}

	result[fieldName] = evalResult
}

// processExpressionFieldFallback 表达式字段处理的回退逻辑
func (s *Stream) processExpressionFieldFallback(fieldName string, dataMap map[string]interface{}, result map[string]interface{}) {
	fieldExpr, exists := s.config.FieldExpressions[fieldName]
	if !exists {
		result[fieldName] = nil
		return
	}

	// 使用桥接器计算表达式，支持IS NULL等语法
	bridge := functions.GetExprBridge()

	// 预处理表达式中的IS NULL和LIKE语法
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

	// 检查表达式是否是函数调用（包含括号）
	isFunctionCall := strings.Contains(fieldExpr.Expression, "(") && strings.Contains(fieldExpr.Expression, ")")

	// 检查表达式是否包含嵌套字段（但排除函数调用中的点号）
	hasNestedFields := false
	if !isFunctionCall && strings.Contains(fieldExpr.Expression, ".") {
		hasNestedFields = true
	}

	var evalResult interface{}

	if isFunctionCall {
		// 对于函数调用，优先使用桥接器处理
		exprResult, err := bridge.EvaluateExpression(processedExpr, dataMap)
		if err != nil {
			logger.Error("Function call evaluation failed for field %s: %v", fieldName, err)
			result[fieldName] = nil
			return
		}
		evalResult = exprResult
	} else if hasNestedFields {
		// 检测到嵌套字段（非函数调用），使用自定义表达式引擎
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

		numResult, err := expression.Evaluate(dataMap)
		if err != nil {
			logger.Error("Expression evaluation failed for field %s: %v", fieldName, err)
			result[fieldName] = nil
			return
		}
		evalResult = numResult
	} else {
		// 尝试使用桥接器处理其他表达式
		exprResult, err := bridge.EvaluateExpression(processedExpr, dataMap)
		if err != nil {
			// 如果桥接器失败，回退到原来的表达式引擎
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

			numResult, evalErr := expression.Evaluate(dataMap)
			if evalErr != nil {
				logger.Error("Expression evaluation failed for field %s: %v", fieldName, evalErr)
				result[fieldName] = nil
				return
			}
			evalResult = numResult
		} else {
			evalResult = exprResult
		}
	}

	result[fieldName] = evalResult
}

// processSimpleField 处理简单字段
func (s *Stream) processSimpleField(fieldSpec string, dataMap map[string]interface{}, data interface{}, result map[string]interface{}) {
	info := s.compiledFieldInfo[fieldSpec]
	if info == nil {
		// 如果没有预编译信息，回退到原逻辑（安全性保证）
		s.processSingleFieldFallback(fieldSpec, dataMap, data, result)
		return
	}

	if info.isSelectAll {
		// SELECT *：批量复制所有字段，跳过表达式字段
		for k, v := range dataMap {
			if _, isExpression := s.config.FieldExpressions[k]; !isExpression {
				result[k] = v
			}
		}
		return
	}

	// 跳过已经通过表达式字段处理的字段
	if _, isExpression := s.config.FieldExpressions[info.outputName]; isExpression {
		return
	}

	if info.isStringLiteral {
		// 字符串字面量处理：使用预编译的字符串值
		result[info.alias] = info.stringValue
	} else if info.isFunctionCall {
		// 执行函数调用
		if funcResult, err := s.executeFunction(info.fieldName, dataMap); err == nil {
			result[info.outputName] = funcResult
		} else {
			logger.Error("Function execution error %s: %v", info.fieldName, err)
			result[info.outputName] = nil
		}
	} else {
		// 普通字段处理
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

// processSingleFieldFallback 回退处理单个字段（当预编译信息缺失时）
func (s *Stream) processSingleFieldFallback(fieldSpec string, dataMap map[string]interface{}, data interface{}, result map[string]interface{}) {
	// 处理SELECT *的特殊情况
	if fieldSpec == "*" {
		// SELECT *：返回所有字段，但跳过已经通过表达式字段处理的字段
		for k, v := range dataMap {
			// 如果该字段已经通过表达式字段处理，则跳过，保持表达式计算结果
			if _, isExpression := s.config.FieldExpressions[k]; !isExpression {
				result[k] = v
			}
		}
		return
	}

	// 处理别名
	parts := strings.Split(fieldSpec, ":")
	fieldName := parts[0]
	outputName := fieldName
	if len(parts) > 1 {
		outputName = parts[1]
	}

	// 跳过已经通过表达式字段处理的字段
	if _, isExpression := s.config.FieldExpressions[outputName]; isExpression {
		return
	}

	// 检查是否是函数调用
	if strings.Contains(fieldName, "(") && strings.Contains(fieldName, ")") {
		// 执行函数调用
		if funcResult, err := s.executeFunction(fieldName, dataMap); err == nil {
			result[outputName] = funcResult
		} else {
			logger.Error("Function execution error %s: %v", fieldName, err)
			result[outputName] = nil
		}
	} else {
		// 普通字段 - 支持嵌套字段
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

// executeFunction 执行函数调用
func (s *Stream) executeFunction(funcExpr string, data map[string]interface{}) (interface{}, error) {
	// 检查是否是自定义函数
	funcName := extractFunctionName(funcExpr)
	if funcName != "" {
		// 直接使用函数系统
		fn, exists := functions.Get(funcName)
		if exists {
			// 解析参数
			args, err := s.parseFunctionArgs(funcExpr, data)
			if err != nil {
				return nil, err
			}

			// 创建函数上下文
			ctx := &functions.FunctionContext{Data: data}

			// 执行函数
			return fn.Execute(ctx, args)
		}
	}

	// 对于复杂的嵌套函数调用，直接使用ExprBridge
	// 这样可以避免Expression.Evaluate的float64类型限制
	bridge := functions.GetExprBridge()
	result, err := bridge.EvaluateExpression(funcExpr, data)
	if err != nil {
		return nil, fmt.Errorf("evaluate function expression failed: %w", err)
	}

	return result, nil
}

// extractFunctionName 从表达式中提取函数名
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

// parseFunctionArgs 解析函数参数，支持嵌套函数调用
func (s *Stream) parseFunctionArgs(funcExpr string, data map[string]interface{}) ([]interface{}, error) {
	// 提取括号内的参数
	start := strings.Index(funcExpr, "(")
	end := strings.LastIndex(funcExpr, ")")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("invalid function expression: %s", funcExpr)
	}

	argsStr := strings.TrimSpace(funcExpr[start+1 : end])
	if argsStr == "" {
		return []interface{}{}, nil
	}

	// 智能分割参数，处理嵌套函数和引号
	argParts, err := s.smartSplitArgs(argsStr)
	if err != nil {
		return nil, err
	}

	args := make([]interface{}, len(argParts))

	for i, arg := range argParts {
		arg = strings.TrimSpace(arg)

		// 如果参数是字符串常量（用引号包围）
		if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") {
			args[i] = strings.Trim(arg, "'")
		} else if strings.HasPrefix(arg, "\"") && strings.HasSuffix(arg, "\"") {
			args[i] = strings.Trim(arg, "\"")
		} else if strings.Contains(arg, "(") {
			// 如果参数包含函数调用，递归执行
			result, err := s.executeFunction(arg, data)
			if err != nil {
				return nil, fmt.Errorf("failed to execute nested function '%s': %v", arg, err)
			}
			args[i] = result
		} else if value, exists := data[arg]; exists {
			// 如果是数据字段
			args[i] = value
		} else {
			// 尝试解析为数字
			if val, err := strconv.ParseFloat(arg, 64); err == nil {
				args[i] = val
			} else {
				args[i] = arg
			}
		}
	}

	return args, nil
}

// smartSplitArgs 智能分割参数，考虑括号嵌套和引号
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
				// 找到参数分隔符
				args = append(args, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		default:
			current.WriteByte(ch)
		}
	}

	// 添加最后一个参数
	if current.Len() > 0 {
		args = append(args, strings.TrimSpace(current.String()))
	}

	return args, nil
}