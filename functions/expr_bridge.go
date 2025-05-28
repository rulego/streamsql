package functions

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/rulego/streamsql/utils/cast"
)

// ExprBridge 桥接 StreamSQL 函数系统与 expr-lang/expr
type ExprBridge struct {
	streamSQLFunctions map[string]Function
	exprProgram        *vm.Program
	exprEnv            map[string]interface{}
}

// NewExprBridge 创建新的表达式桥接器
func NewExprBridge() *ExprBridge {
	return &ExprBridge{
		streamSQLFunctions: ListAll(),
		exprEnv:            make(map[string]interface{}),
	}
}

// RegisterStreamSQLFunctionsToExpr 将StreamSQL函数注册到expr环境中
func (bridge *ExprBridge) RegisterStreamSQLFunctionsToExpr() []expr.Option {
	options := make([]expr.Option, 0)

	// 将所有StreamSQL函数注册到expr环境
	for name, fn := range bridge.streamSQLFunctions {
		// 为了避免闭包问题，创建局部变量
		funcName := name
		function := fn

		// 将StreamSQL函数包装为expr兼容的函数
		wrappedFunc := func(params ...interface{}) (interface{}, error) {
			ctx := &FunctionContext{
				Data: bridge.exprEnv,
			}
			return function.Execute(ctx, params)
		}

		// 添加函数到expr环境
		bridge.exprEnv[funcName] = wrappedFunc

		// 注册函数类型信息
		options = append(options, expr.Function(
			funcName,
			wrappedFunc,
		))
	}

	return options
}

// CreateEnhancedExprEnvironment 创建增强的expr执行环境
func (bridge *ExprBridge) CreateEnhancedExprEnvironment(data map[string]interface{}) map[string]interface{} {
	// 合并数据和函数环境
	env := make(map[string]interface{})

	// 添加用户数据
	for k, v := range data {
		env[k] = v
	}

	// 添加所有StreamSQL函数
	for name, fn := range bridge.streamSQLFunctions {
		funcName := name
		function := fn

		env[funcName] = func(params ...interface{}) (interface{}, error) {
			ctx := &FunctionContext{
				Data: data, // 使用当前数据上下文
			}
			return function.Execute(ctx, params)
		}
	}

	// 添加一些便捷的数学函数别名，避免与内置冲突
	env["streamsql_abs"] = env["abs"]
	env["streamsql_sqrt"] = env["sqrt"]
	env["streamsql_min"] = env["min"]
	env["streamsql_max"] = env["max"]

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

	// 启用一些有用的expr功能
	options = append(options,
		expr.AllowUndefinedVariables(), // 允许未定义变量
		expr.AsBool(),                  // 期望布尔结果（可根据需要调整）
	)

	return expr.Compile(expression, options...)
}

// EvaluateExpression 评估表达式，自动选择最合适的引擎
func (bridge *ExprBridge) EvaluateExpression(expression string, data map[string]interface{}) (interface{}, error) {
	// 首先检查是否是CONCAT函数调用
	if strings.HasPrefix(strings.ToUpper(expression), "CONCAT(") {
		return bridge.evaluateConcatFunction(expression, data)
	}

	// 首先检查是否包含字符串拼接模式
	if bridge.isStringConcatenationExpression(expression, data) {
		result, err := bridge.evaluateStringConcatenation(expression, data)
		if err == nil {
			return result, nil
		}
	}

	// 创建增强环境
	env := bridge.CreateEnhancedExprEnvironment(data)

	// 尝试使用expr-lang/expr评估
	result, err := expr.Eval(expression, env)
	if err != nil {
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

// evaluateConcatFunction 处理CONCAT函数调用
func (bridge *ExprBridge) evaluateConcatFunction(expression string, data map[string]interface{}) (interface{}, error) {
	// 提取CONCAT函数的参数
	start := strings.Index(expression, "(")
	end := strings.LastIndex(expression, ")")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("invalid CONCAT function syntax: %s", expression)
	}

	// 获取参数字符串
	paramsStr := strings.TrimSpace(expression[start+1 : end])
	if paramsStr == "" {
		return "", nil // 空参数返回空字符串
	}

	// 解析参数
	params := bridge.parseParameters(paramsStr)
	var result strings.Builder

	for _, param := range params {
		param = strings.TrimSpace(param)

		// 处理字符串字面量
		if (strings.HasPrefix(param, "'") && strings.HasSuffix(param, "'")) ||
			(strings.HasPrefix(param, "\"") && strings.HasSuffix(param, "\"")) {
			// 去掉引号
			literal := param[1 : len(param)-1]
			result.WriteString(literal)
		} else {
			// 处理字段引用
			if value, exists := data[param]; exists {
				strValue := cast.ToString(value)
				result.WriteString(strValue)
			} else {
				return nil, fmt.Errorf("field %s not found in data", param)
			}
		}
	}

	return result.String(), nil
}

// parseParameters 解析函数参数，正确处理引号内的逗号
func (bridge *ExprBridge) parseParameters(paramsStr string) []string {
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
	info := make(map[string]interface{})

	// StreamSQL函数信息
	streamSQLFuncs := make(map[string]interface{})
	for name, fn := range bridge.streamSQLFunctions {
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

// ResolveFunction 解析函数调用，优先使用expr-lang/expr的函数
func (bridge *ExprBridge) ResolveFunction(name string) (interface{}, bool, string) {
	// 检查是否是expr-lang内置函数
	exprBuiltins := []string{
		"abs", "ceil", "floor", "round", "max", "min", // math
		"trim", "upper", "lower", "split", "replace", "indexOf", "hasPrefix", "hasSuffix", // string
		"all", "any", "filter", "map", "find", "count", "concat", "flatten", // array
		"now", "duration", "date", // time
		"int", "float", "string", "type", // conversion
		"toJSON", "fromJSON", "toBase64", "fromBase64", // encoding
		"len", "get", // misc
	}

	for _, builtin := range exprBuiltins {
		if builtin == name {
			return nil, true, "expr-lang" // expr-lang会自动处理
		}
	}

	// 检查StreamSQL函数
	if fn, exists := bridge.streamSQLFunctions[name]; exists {
		return fn, true, "streamsql"
	}

	return nil, false, ""
}

// 全局桥接器实例
var globalBridge *ExprBridge

// GetExprBridge 获取全局桥接器实例
func GetExprBridge() *ExprBridge {
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
