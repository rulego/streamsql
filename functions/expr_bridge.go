package functions

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/rulego/streamsql/utils/cast"
)

// ExprBridge 桥接 StreamSQL 函数系统与 expr-lang/expr
type ExprBridge struct {
	streamSQLFunctions map[string]Function
	exprProgram        *vm.Program
	exprEnv            map[string]interface{}
	mutex              sync.RWMutex // 添加读写锁保护并发访问
}

// NewExprBridge 创建新的表达式桥接器
func NewExprBridge() *ExprBridge {
	return &ExprBridge{
		streamSQLFunctions: make(map[string]Function), // 初始化为空，动态获取
		exprEnv:            make(map[string]interface{}),
	}
}

// RegisterStreamSQLFunctionsToExpr 将StreamSQL函数注册到expr环境中
func (bridge *ExprBridge) RegisterStreamSQLFunctionsToExpr() []expr.Option {
	bridge.mutex.Lock()
	defer bridge.mutex.Unlock()

	options := make([]expr.Option, 0)

	// 动态获取所有当前注册的函数
	allFunctions := ListAll()

	// 将所有StreamSQL函数注册到expr环境
	for name, fn := range allFunctions {
		// 为了避免闭包问题，使用立即执行函数
		function := fn

		wrappedFunc := func(function Function) func(params ...interface{}) (interface{}, error) {
			return func(params ...interface{}) (interface{}, error) {
				ctx := &FunctionContext{
					Data: bridge.exprEnv,
				}
				return function.Execute(ctx, params)
			}
		}(function)

		// 添加函数到expr环境
		bridge.exprEnv[name] = wrappedFunc

		// 注册函数类型信息
		options = append(options, expr.Function(
			name,
			wrappedFunc,
		))
	}

	return options
}

// CreateEnhancedExprEnvironment 创建增强的expr执行环境
func (bridge *ExprBridge) CreateEnhancedExprEnvironment(data map[string]interface{}) map[string]interface{} {
	bridge.mutex.RLock()
	defer bridge.mutex.RUnlock()

	// 合并数据和函数环境
	env := make(map[string]interface{})

	// 添加用户数据
	for k, v := range data {
		env[k] = v
	}

	// 动态获取所有当前注册的函数
	allFunctions := ListAll()

	// 添加所有StreamSQL函数
	for name, fn := range allFunctions {
		// 确保闭包捕获正确的函数实例
		function := fn

		wrappedFunc := func(function Function) func(params ...interface{}) (interface{}, error) {
			return func(params ...interface{}) (interface{}, error) {
				ctx := &FunctionContext{
					Data: data, // 使用当前数据上下文
				}
				return function.Execute(ctx, params)
			}
		}(function)

		// 注册小写版本
		env[name] = wrappedFunc
		// 注册大写版本
		env[strings.ToUpper(name)] = wrappedFunc
	}

	// 添加一些便捷的数学函数别名，避免与内置冲突
	env["streamsql_abs"] = env["abs"]
	env["streamsql_sqrt"] = env["sqrt"]
	env["streamsql_min"] = env["min"]
	env["streamsql_max"] = env["max"]

	// 添加自定义的LIKE匹配函数
	env["like_match"] = func(text, pattern string) bool {
		return bridge.matchesLikePattern(text, pattern)
	}

	return env
}

// CompileExpressionWithStreamSQLFunctions 编译表达式，包含StreamSQL函数
func (bridge *ExprBridge) CompileExpressionWithStreamSQLFunctions(expression string, dataType interface{}) (*vm.Program, error) {
	options := []expr.Option{
		expr.Env(dataType),
	}

	// 添加StreamSQL函数
	streamSQLOptions := bridge.RegisterStreamSQLFunctionsToExpr()
	options = append(options, streamSQLOptions...)

	// 添加LIKE相关的自定义函数（只需要like_match，其他是内置操作符）
	options = append(options,
		expr.Function("like_match", func(params ...any) (any, error) {
			if len(params) != 2 {
				return false, fmt.Errorf("like_match function requires 2 parameters")
			}
			text, ok1 := params[0].(string)
			pattern, ok2 := params[1].(string)
			if !ok1 || !ok2 {
				return false, fmt.Errorf("like_match function requires string parameters")
			}
			return bridge.matchesLikePattern(text, pattern), nil
		}),
	)

	// 启用一些有用的expr功能
	options = append(options,
		expr.AllowUndefinedVariables(), // 允许未定义变量
		expr.AsBool(),                  // 期望布尔结果（可根据需要调整）
	)

	return expr.Compile(expression, options...)
}

// EvaluateExpression 评估表达式，自动选择最合适的引擎
func (bridge *ExprBridge) EvaluateExpression(expression string, data map[string]interface{}) (interface{}, error) {
	// 首先检查是否包含LIKE操作符，如果有则进行预处理
	if bridge.ContainsLikeOperator(expression) {
		processedExpr, err := bridge.PreprocessLikeExpression(expression)
		if err == nil {
			expression = processedExpr
		}
	}

	// 检查是否包含IS NULL或IS NOT NULL操作符，如果有则进行预处理
	if bridge.ContainsIsNullOperator(expression) {
		processedExpr, err := bridge.PreprocessIsNullExpression(expression)
		if err == nil {
			expression = processedExpr
		}
	}

	// 检查是否包含字符串拼接模式
	if bridge.isStringConcatenationExpression(expression, data) {
		result, err := bridge.evaluateStringConcatenation(expression, data)
		if err == nil {
			return result, nil
		}
	}

	// 尝试使用编译后的程序执行（包含StreamSQL函数）
	program, err := bridge.CompileExpressionWithStreamSQLFunctions(expression, data)
	if err == nil {
		// 创建增强环境
		env := bridge.CreateEnhancedExprEnvironment(data)
		result, err := expr.Run(program, env)
		if err == nil {
			return result, nil
		}
	}

	// 如果编译失败，尝试直接使用expr.Eval
	env := bridge.CreateEnhancedExprEnvironment(data)
	result, err := expr.Eval(expression, env)
	if err != nil {
		// 检查是否是函数调用，如果是则不要回退到数值表达式处理
		if bridge.isFunctionCall(expression) {
			return nil, fmt.Errorf("failed to evaluate function call '%s': %v", expression, err)
		}
		// 如果expr失败，回退到自定义expr系统（仅限数值计算）
		return bridge.fallbackToCustomExpr(expression, data)
	}

	return result, nil
}

// isStringConcatenationExpression 检查是否是字符串拼接表达式
func (bridge *ExprBridge) isStringConcatenationExpression(expression string, data map[string]interface{}) bool {
	// 如果表达式包含 + 操作符
	if !strings.Contains(expression, "+") {
		return false
	}

	// 分析表达式中的操作数
	parts := strings.Split(expression, "+")
	for _, part := range parts {
		part = strings.TrimSpace(part)

		// 如果包含字符串字面量（用引号包围）
		if (strings.HasPrefix(part, "'") && strings.HasSuffix(part, "'")) ||
			(strings.HasPrefix(part, "\"") && strings.HasSuffix(part, "\"")) ||
			part == "_" {
			return true
		}

		// 如果是字段引用，检查字段值是否为字符串
		if value, exists := data[part]; exists {
			if _, isString := value.(string); isString {
				return true
			}
		}
	}

	return false
}

// fallbackToCustomExpr 回退到自定义表达式系统
func (bridge *ExprBridge) fallbackToCustomExpr(expression string, data map[string]interface{}) (interface{}, error) {
	// 尝试处理字符串拼接表达式
	result, err := bridge.evaluateStringConcatenation(expression, data)
	if err == nil {
		return result, nil
	}

	// 如果不是字符串拼接，尝试简单的数值表达式
	numResult, err := bridge.evaluateSimpleNumericExpression(expression, data)
	if err == nil {
		return numResult, nil
	}

	return nil, fmt.Errorf("unable to evaluate expression: %s, string concat error: %v, numeric error: %v", expression, err, err)
}

// evaluateStringConcatenation 处理字符串拼接表达式
func (bridge *ExprBridge) evaluateStringConcatenation(expression string, data map[string]interface{}) (interface{}, error) {
	// 检查是否是字符串拼接表达式 (包含 + 和字符串字面量)
	if !strings.Contains(expression, "+") {
		return nil, fmt.Errorf("not a concatenation expression")
	}

	// 简单的字符串拼接解析器
	// 支持格式: field1 + 'literal' + field2 或 field1 + "_" + field2
	parts := strings.Split(expression, "+")
	var result strings.Builder

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// 处理字符串字面量 (用单引号包围)
		if strings.HasPrefix(part, "'") && strings.HasSuffix(part, "'") {
			literal := strings.Trim(part, "'")
			result.WriteString(literal)
		} else if strings.HasPrefix(part, "\"") && strings.HasSuffix(part, "\"") {
			literal := strings.Trim(part, "\"")
			result.WriteString(literal)
		} else if part == "_" {
			// 处理下划线字面量
			result.WriteString("_")
		} else {
			// 处理字段引用
			if value, exists := data[part]; exists {
				strValue := cast.ToString(value)
				result.WriteString(strValue)
			} else {
				return nil, fmt.Errorf("field %s not found in data", part)
			}
		}
	}

	return result.String(), nil
}

// evaluateSimpleNumericExpression 处理简单的数值表达式
func (bridge *ExprBridge) evaluateSimpleNumericExpression(expression string, data map[string]interface{}) (interface{}, error) {
	expression = strings.TrimSpace(expression)

	// 处理简单的字段引用
	if value, exists := data[expression]; exists {
		return value, nil
	}

	// 处理数字字面量
	if num, err := strconv.ParseFloat(expression, 64); err == nil {
		return num, nil
	}

	// 处理简单的数学运算 (例如: field * 2, field + 5)
	for _, op := range []string{"+", "-", "*", "/"} {
		if strings.Contains(expression, op) {
			parts := strings.Split(expression, op)
			if len(parts) == 2 {
				left := strings.TrimSpace(parts[0])
				right := strings.TrimSpace(parts[1])

				// 获取左值
				var leftVal float64
				if val, exists := data[left]; exists {
					if f, err := bridge.toFloat64(val); err == nil {
						leftVal = f
					} else {
						return nil, fmt.Errorf("cannot convert left operand to number: %v", val)
					}
				} else if f, err := strconv.ParseFloat(left, 64); err == nil {
					leftVal = f
				} else {
					continue // 尝试下一个操作符
				}

				// 获取右值
				var rightVal float64
				if val, exists := data[right]; exists {
					if f, err := bridge.toFloat64(val); err == nil {
						rightVal = f
					} else {
						return nil, fmt.Errorf("cannot convert right operand to number: %v", val)
					}
				} else if f, err := strconv.ParseFloat(right, 64); err == nil {
					rightVal = f
				} else {
					continue // 尝试下一个操作符
				}

				// 执行运算
				switch op {
				case "+":
					return leftVal + rightVal, nil
				case "-":
					return leftVal - rightVal, nil
				case "*":
					return leftVal * rightVal, nil
				case "/":
					if rightVal == 0 {
						return nil, fmt.Errorf("division by zero")
					}
					return leftVal / rightVal, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("unsupported expression: %s", expression)
}

// ContainsLikeOperator 检查表达式是否包含LIKE操作符
func (bridge *ExprBridge) ContainsLikeOperator(expression string) bool {
	// 简单检查是否包含LIKE关键字
	upperExpr := strings.ToUpper(expression)
	return strings.Contains(upperExpr, " LIKE ")
}

// ContainsIsNullOperator 检查表达式是否包含IS NULL或IS NOT NULL操作符
func (bridge *ExprBridge) ContainsIsNullOperator(expression string) bool {
	upperExpr := strings.ToUpper(expression)
	return strings.Contains(upperExpr, " IS NULL") || strings.Contains(upperExpr, " IS NOT NULL")
}

// isFunctionCall 检查表达式是否是函数调用
func (bridge *ExprBridge) isFunctionCall(expression string) bool {
	// 如果是CASE表达式，则不是函数调用
	trimmed := strings.TrimSpace(expression)
	upperTrimmed := strings.ToUpper(trimmed)
	if strings.HasPrefix(upperTrimmed, "CASE ") || strings.HasPrefix(upperTrimmed, "CASE\t") || strings.HasPrefix(upperTrimmed, "CASE\n") {
		return false
	}

	// 检查是否符合简单函数调用模式: function_name(args)
	// 函数调用应该以标识符开始，后跟括号
	if !strings.Contains(expression, "(") || !strings.Contains(expression, ")") {
		return false
	}

	// 检查是否以标识符开始（函数名）
	for i, r := range trimmed {
		if i == 0 {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_') {
				return false
			}
		} else if r == '(' {
			// 找到了开括号，说明这可能是函数调用
			return true
		} else if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			// 遇到了非标识符字符且不是开括号，说明不是简单函数调用
			return false
		}
	}

	return false
}

// PreprocessLikeExpression 预处理LIKE表达式，转换为expr-lang可理解的函数调用
func (bridge *ExprBridge) PreprocessLikeExpression(expression string) (string, error) {
	// 使用正则表达式匹配LIKE模式
	// 匹配: field LIKE 'pattern' (允许空模式)
	likePattern := `(\w+(?:\.\w+)*)\s+LIKE\s+'([^']*)'`
	re, err := regexp.Compile(likePattern)
	if err != nil {
		return expression, err
	}

	// 替换所有LIKE表达式
	result := re.ReplaceAllStringFunc(expression, func(match string) string {
		submatches := re.FindStringSubmatch(match)
		if len(submatches) != 3 {
			return match // 保持原样
		}

		field := submatches[1]
		pattern := submatches[2]

		// 将LIKE模式转换为相应的函数调用
		return bridge.convertLikeToFunction(field, pattern)
	})

	return result, nil
}

// PreprocessIsNullExpression 预处理IS NULL和IS NOT NULL表达式，转换为expr-lang可理解的表达式
func (bridge *ExprBridge) PreprocessIsNullExpression(expression string) (string, error) {
	// 匹配复杂表达式的 IS NOT NULL 模式 (如函数调用)
	complexNotNullPattern := `([A-Za-z_][A-Za-z0-9_]*\s*\([^)]*\))\s+IS\s+NOT\s+NULL`
	reComplexNotNull, err := regexp.Compile(complexNotNullPattern)
	if err != nil {
		return expression, err
	}

	// 先处理复杂表达式的IS NOT NULL
	result := reComplexNotNull.ReplaceAllString(expression, "is_not_null($1)")

	// 匹配复杂表达式的 IS NULL 模式
	complexNullPattern := `([A-Za-z_][A-Za-z0-9_]*\s*\([^)]*\))\s+IS\s+NULL`
	reComplexNull, err := regexp.Compile(complexNullPattern)
	if err != nil {
		return result, err
	}

	// 处理复杂表达式的IS NULL
	result = reComplexNull.ReplaceAllString(result, "is_null($1)")

	// 匹配简单字段的 IS NOT NULL 模式 (必须在复杂表达式之后处理)
	isNotNullPattern := `(\w+(?:\.\w+)*)\s+IS\s+NOT\s+NULL`
	reNotNull, err := regexp.Compile(isNotNullPattern)
	if err != nil {
		return result, err
	}

	// 替换简单字段的IS NOT NULL
	result = reNotNull.ReplaceAllString(result, "$1 != nil")

	// 匹配简单字段的 IS NULL 模式
	isNullPattern := `(\w+(?:\.\w+)*)\s+IS\s+NULL`
	reNull, err := regexp.Compile(isNullPattern)
	if err != nil {
		return result, err
	}

	// 再替换简单字段的IS NULL
	result = reNull.ReplaceAllString(result, "$1 == nil")

	return result, nil
}

// convertLikeToFunction 将LIKE模式转换为expr-lang操作符
func (bridge *ExprBridge) convertLikeToFunction(field, pattern string) string {
	// 处理空模式
	if pattern == "" {
		return fmt.Sprintf("%s == ''", field)
	}

	// 分析模式类型
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") && len(pattern) > 1 {
		// %pattern% -> contains操作符（但不是单独的%）
		inner := strings.Trim(pattern, "%")
		if inner == "" {
			// %% 表示匹配任何字符串
			return "true"
		}
		return fmt.Sprintf("%s contains '%s'", field, inner)
	} else if strings.HasPrefix(pattern, "%") && len(pattern) > 1 {
		// %pattern -> endsWith操作符
		suffix := strings.TrimPrefix(pattern, "%")
		return fmt.Sprintf("%s endsWith '%s'", field, suffix)
	} else if strings.HasSuffix(pattern, "%") && len(pattern) > 1 {
		// pattern% -> startsWith操作符
		prefix := strings.TrimSuffix(pattern, "%")
		return fmt.Sprintf("%s startsWith '%s'", field, prefix)
	} else if pattern == "%" {
		// 单独的%匹配任何字符串
		return "true"
	} else if strings.Contains(pattern, "%") || strings.Contains(pattern, "_") {
		// 复杂模式（如prefix%suffix）或包含单字符通配符，使用自定义的like_match函数
		return fmt.Sprintf("like_match(%s, '%s')", field, pattern)
	} else {
		// 精确匹配
		return fmt.Sprintf("%s == '%s'", field, pattern)
	}
}

// matchesLikePattern 实现LIKE模式匹配
// 支持%（匹配任意字符序列）和_（匹配单个字符）
func (bridge *ExprBridge) matchesLikePattern(text, pattern string) bool {
	return bridge.likeMatch(text, pattern, 0, 0)
}

// likeMatch 递归实现LIKE匹配算法
func (bridge *ExprBridge) likeMatch(text, pattern string, textIndex, patternIndex int) bool {
	// 如果模式已经匹配完成
	if patternIndex >= len(pattern) {
		return textIndex >= len(text) // 文本也应该匹配完成
	}

	// 如果文本已经结束，但模式还有非%字符，则不匹配
	if textIndex >= len(text) {
		// 检查剩余的模式是否都是%
		for i := patternIndex; i < len(pattern); i++ {
			if pattern[i] != '%' {
				return false
			}
		}
		return true
	}

	// 处理当前模式字符
	patternChar := pattern[patternIndex]

	if patternChar == '%' {
		// %可以匹配0个或多个字符
		// 尝试匹配0个字符（跳过%）
		if bridge.likeMatch(text, pattern, textIndex, patternIndex+1) {
			return true
		}
		// 尝试匹配1个或多个字符
		for i := textIndex; i < len(text); i++ {
			if bridge.likeMatch(text, pattern, i+1, patternIndex+1) {
				return true
			}
		}
		return false
	} else if patternChar == '_' {
		// _匹配恰好一个字符
		return bridge.likeMatch(text, pattern, textIndex+1, patternIndex+1)
	} else {
		// 普通字符必须精确匹配
		if text[textIndex] == patternChar {
			return bridge.likeMatch(text, pattern, textIndex+1, patternIndex+1)
		}
		return false
	}
}

// toFloat64 将值转换为float64
func (bridge *ExprBridge) toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

// GetFunctionInfo 获取函数信息，统一两个系统的函数
func (bridge *ExprBridge) GetFunctionInfo() map[string]interface{} {
	bridge.mutex.RLock()
	defer bridge.mutex.RUnlock()

	info := make(map[string]interface{})

	// StreamSQL函数信息 - 动态获取所有当前注册的函数
	streamSQLFuncs := make(map[string]interface{})
	allFunctions := ListAll() // 动态获取所有注册的函数
	for name, fn := range allFunctions {
		streamSQLFuncs[name] = map[string]interface{}{
			"name":        fn.GetName(),
			"type":        fn.GetType(),
			"category":    fn.GetCategory(),
			"description": fn.GetDescription(),
			"source":      "StreamSQL",
		}
	}
	info["streamsql"] = streamSQLFuncs

	// expr-lang/expr内置函数（列出主要的）
	exprBuiltins := map[string]interface{}{
		// 数学函数
		"abs":   map[string]interface{}{"category": "math", "description": "absolute value", "source": "expr-lang"},
		"ceil":  map[string]interface{}{"category": "math", "description": "ceiling", "source": "expr-lang"},
		"floor": map[string]interface{}{"category": "math", "description": "floor", "source": "expr-lang"},
		"round": map[string]interface{}{"category": "math", "description": "round", "source": "expr-lang"},
		"max":   map[string]interface{}{"category": "math", "description": "maximum", "source": "expr-lang"},
		"min":   map[string]interface{}{"category": "math", "description": "minimum", "source": "expr-lang"},

		// 字符串函数
		"trim":      map[string]interface{}{"category": "string", "description": "trim whitespace", "source": "expr-lang"},
		"upper":     map[string]interface{}{"category": "string", "description": "to uppercase", "source": "expr-lang"},
		"lower":     map[string]interface{}{"category": "string", "description": "to lowercase", "source": "expr-lang"},
		"split":     map[string]interface{}{"category": "string", "description": "split string", "source": "expr-lang"},
		"replace":   map[string]interface{}{"category": "string", "description": "replace substring", "source": "expr-lang"},
		"indexOf":   map[string]interface{}{"category": "string", "description": "find index", "source": "expr-lang"},
		"hasPrefix": map[string]interface{}{"category": "string", "description": "check prefix", "source": "expr-lang"},
		"hasSuffix": map[string]interface{}{"category": "string", "description": "check suffix", "source": "expr-lang"},

		// 数组/集合函数
		"all":     map[string]interface{}{"category": "array", "description": "all elements satisfy", "source": "expr-lang"},
		"any":     map[string]interface{}{"category": "array", "description": "any element satisfies", "source": "expr-lang"},
		"filter":  map[string]interface{}{"category": "array", "description": "filter elements", "source": "expr-lang"},
		"map":     map[string]interface{}{"category": "array", "description": "transform elements", "source": "expr-lang"},
		"find":    map[string]interface{}{"category": "array", "description": "find element", "source": "expr-lang"},
		"count":   map[string]interface{}{"category": "array", "description": "count elements", "source": "expr-lang"},
		"concat":  map[string]interface{}{"category": "array", "description": "concatenate arrays", "source": "expr-lang"},
		"flatten": map[string]interface{}{"category": "array", "description": "flatten array", "source": "expr-lang"},

		// 时间函数
		"now":      map[string]interface{}{"category": "datetime", "description": "current time", "source": "expr-lang"},
		"duration": map[string]interface{}{"category": "datetime", "description": "parse duration", "source": "expr-lang"},
		"date":     map[string]interface{}{"category": "datetime", "description": "parse date", "source": "expr-lang"},

		// 类型转换
		"int":    map[string]interface{}{"category": "conversion", "description": "to integer", "source": "expr-lang"},
		"float":  map[string]interface{}{"category": "conversion", "description": "to float", "source": "expr-lang"},
		"string": map[string]interface{}{"category": "conversion", "description": "to string", "source": "expr-lang"},
		"type":   map[string]interface{}{"category": "conversion", "description": "get type", "source": "expr-lang"},

		// JSON处理
		"toJSON":   map[string]interface{}{"category": "json", "description": "to JSON", "source": "expr-lang"},
		"fromJSON": map[string]interface{}{"category": "json", "description": "from JSON", "source": "expr-lang"},

		// Base64编码
		"toBase64":   map[string]interface{}{"category": "encoding", "description": "to Base64", "source": "expr-lang"},
		"fromBase64": map[string]interface{}{"category": "encoding", "description": "from Base64", "source": "expr-lang"},
	}
	info["expr-lang"] = exprBuiltins

	return info
}

// ResolveFunction 解析函数调用，优先使用StreamSQL函数
func (bridge *ExprBridge) ResolveFunction(name string) (interface{}, bool, string) {
	bridge.mutex.RLock()
	defer bridge.mutex.RUnlock()

	// 进行大小写不敏感的查找
	lowerName := strings.ToLower(name)

	// 首先检查StreamSQL函数（优先级更高） - 动态获取
	allFunctions := ListAll()
	if fn, exists := allFunctions[lowerName]; exists {
		return fn, true, "streamsql"
	}

	// 然后检查是否是expr-lang内置函数
	exprBuiltins := []string{
		"abs", "ceil", "floor", "round", "max", "min", // math
		"trim", "upper", "lower", "split", "replace", "indexOf", "hasPrefix", "hasSuffix", // string
		"all", "any", "filter", "map", "find", "count", "flatten", // array (移除concat)
		"now", "duration", "date", // time
		"int", "float", "string", "type", // conversion
		"toJSON", "fromJSON", "toBase64", "fromBase64", // encoding
		"len", "get", // misc
	}

	for _, builtin := range exprBuiltins {
		if strings.ToLower(builtin) == lowerName {
			return nil, true, "expr-lang" // expr-lang会自动处理
		}
	}

	return nil, false, ""
}

// IsExprLangFunction 检查函数名是否是expr-lang内置函数
func (bridge *ExprBridge) IsExprLangFunction(name string) bool {
	// expr-lang内置函数列表（移除concat避免冲突）
	exprBuiltins := []string{
		"abs", "ceil", "floor", "round", "max", "min", // math
		"trim", "upper", "lower", "split", "replace", "indexOf", "hasPrefix", "hasSuffix", // string
		"all", "any", "filter", "map", "find", "count", "flatten", // array (移除concat)
		"now", "duration", "date", // time
		"int", "float", "string", "type", // conversion
		"toJSON", "fromJSON", "toBase64", "fromBase64", // encoding
		"len", "get", // misc
	}

	for _, builtin := range exprBuiltins {
		if builtin == name {
			return true
		}
	}
	return false
}

// 全局桥接器实例
var globalBridge *ExprBridge
var globalBridgeMutex sync.RWMutex

// GetExprBridge 获取全局桥接器实例
func GetExprBridge() *ExprBridge {
	// 首先使用读锁检查是否已初始化
	globalBridgeMutex.RLock()
	if globalBridge != nil {
		defer globalBridgeMutex.RUnlock()
		return globalBridge
	}
	globalBridgeMutex.RUnlock()

	// 使用写锁进行初始化
	globalBridgeMutex.Lock()
	defer globalBridgeMutex.Unlock()

	// 双重检查模式，防止并发初始化
	if globalBridge == nil {
		globalBridge = NewExprBridge()
	}
	return globalBridge
}

// 便捷函数：直接评估表达式
func EvaluateWithBridge(expression string, data map[string]interface{}) (interface{}, error) {
	return GetExprBridge().EvaluateExpression(expression, data)
}

// 便捷函数：获取所有可用函数信息
func GetAllAvailableFunctions() map[string]interface{} {
	return GetExprBridge().GetFunctionInfo()
}
