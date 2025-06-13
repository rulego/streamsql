package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/rulego/streamsql/functions"
)

// 表达式类型
const (
	TypeNumber      = "number"      // 数字常量
	TypeField       = "field"       // 字段引用
	TypeOperator    = "operator"    // 运算符
	TypeFunction    = "function"    // 函数调用
	TypeParenthesis = "parenthesis" // 括号
	TypeCase        = "case"        // CASE表达式
	TypeString      = "string"      // 字符串常量
)

// 操作符优先级
var operatorPrecedence = map[string]int{
	"OR":  1,
	"AND": 2,
	"==":  3, "=": 3, "!=": 3, "<>": 3,
	">": 4, "<": 4, ">=": 4, "<=": 4,
	"+": 5, "-": 5,
	"*": 6, "/": 6, "%": 6,
	"^": 7, // 幂运算
}

// CASE表达式的WHEN子句
type WhenClause struct {
	Condition *ExprNode // WHEN条件
	Result    *ExprNode // THEN结果
}

// 表达式节点
type ExprNode struct {
	Type  string
	Value string
	Left  *ExprNode
	Right *ExprNode
	Args  []*ExprNode // 用于函数调用的参数

	// CASE表达式专用字段
	CaseExpr    *ExprNode    // CASE后面的表达式（简单CASE）
	WhenClauses []WhenClause // WHEN子句列表
	ElseExpr    *ExprNode    // ELSE表达式
}

// Expression 表示一个可计算的表达式
type Expression struct {
	Root               *ExprNode
	useExprLang        bool   // 是否使用expr-lang/expr
	exprLangExpression string // expr-lang表达式字符串
}

// NewExpression 创建一个新的表达式
func NewExpression(exprStr string) (*Expression, error) {
	// 首先尝试使用自定义解析器
	tokens, err := tokenize(exprStr)
	if err != nil {
		// 如果自定义解析失败，标记为使用expr-lang
		return &Expression{
			Root:               nil,
			useExprLang:        true,
			exprLangExpression: exprStr,
		}, nil
	}

	root, err := parseExpression(tokens)
	if err != nil {
		// 如果自定义解析失败，标记为使用expr-lang
		return &Expression{
			Root:               nil,
			useExprLang:        true,
			exprLangExpression: exprStr,
		}, nil
	}

	return &Expression{
		Root:        root,
		useExprLang: false,
	}, nil
}

// Evaluate 计算表达式的值
func (e *Expression) Evaluate(data map[string]interface{}) (float64, error) {
	if e.useExprLang {
		return e.evaluateWithExprLang(data)
	}
	return evaluateNode(e.Root, data)
}

// evaluateWithExprLang 使用expr-lang/expr评估表达式
func (e *Expression) evaluateWithExprLang(data map[string]interface{}) (float64, error) {
	// 使用桥接器评估表达式
	bridge := functions.GetExprBridge()
	result, err := bridge.EvaluateExpression(e.exprLangExpression, data)
	if err != nil {
		return 0, err
	}

	// 尝试转换结果为float64
	switch v := result.(type) {
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
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("cannot convert string result '%s' to float64", v)
	default:
		return 0, fmt.Errorf("expression result type %T is not convertible to float64", result)
	}
}

// GetFields 获取表达式中引用的所有字段
func (e *Expression) GetFields() []string {
	if e.useExprLang {
		// 对于expr-lang表达式，需要解析字段引用
		// 这里简化处理，实际应该使用AST分析
		return extractFieldsFromExprLang(e.exprLangExpression)
	}

	fields := make(map[string]bool)
	collectFields(e.Root, fields)

	result := make([]string, 0, len(fields))
	for field := range fields {
		result = append(result, field)
	}
	return result
}

// extractFieldsFromExprLang 从expr-lang表达式中提取字段引用（简化版本）
func extractFieldsFromExprLang(expression string) []string {
	// 这是一个简化的实现，实际应该使用AST解析
	// 暂时使用正则表达式或简单的字符串解析
	fields := make(map[string]bool)

	// 简单的字段提取：查找标识符模式
	tokens := strings.FieldsFunc(expression, func(c rune) bool {
		return !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '_'
	})

	for _, token := range tokens {
		if isIdentifier(token) && !isNumber(token) && !isFunctionOrKeyword(token) {
			fields[token] = true
		}
	}

	result := make([]string, 0, len(fields))
	for field := range fields {
		result = append(result, field)
	}
	return result
}

// isFunctionOrKeyword 检查是否是函数名或关键字
func isFunctionOrKeyword(token string) bool {
	// 检查是否是已知函数或关键字
	keywords := []string{
		"and", "or", "not", "true", "false", "nil", "null",
		"if", "else", "then", "in", "contains", "matches",
		// CASE表达式关键字
		"case", "when", "then", "else", "end",
	}

	for _, keyword := range keywords {
		if strings.ToLower(token) == keyword {
			return true
		}
	}

	// 检查是否是注册的函数
	bridge := functions.GetExprBridge()
	_, exists, _ := bridge.ResolveFunction(token)
	return exists
}

// collectFields 收集表达式中所有字段
func collectFields(node *ExprNode, fields map[string]bool) {
	if node == nil {
		return
	}

	if node.Type == TypeField {
		fields[node.Value] = true
	}

	// 处理CASE表达式的字段收集
	if node.Type == TypeCase {
		// 收集CASE表达式本身的字段
		if node.CaseExpr != nil {
			collectFields(node.CaseExpr, fields)
		}

		// 收集所有WHEN子句中的字段
		for _, whenClause := range node.WhenClauses {
			collectFields(whenClause.Condition, fields)
			collectFields(whenClause.Result, fields)
		}

		// 收集ELSE表达式中的字段
		if node.ElseExpr != nil {
			collectFields(node.ElseExpr, fields)
		}

		return
	}

	collectFields(node.Left, fields)
	collectFields(node.Right, fields)

	for _, arg := range node.Args {
		collectFields(arg, fields)
	}
}

// evaluateNode 计算节点的值
func evaluateNode(node *ExprNode, data map[string]interface{}) (float64, error) {
	if node == nil {
		return 0, fmt.Errorf("null expression node")
	}

	switch node.Type {
	case TypeNumber:
		return strconv.ParseFloat(node.Value, 64)

	case TypeString:
		// 处理字符串类型，去掉引号并尝试转换为数字
		// 如果无法转换，返回错误（因为这个函数返回float64）
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1] // 去掉引号
		}

		// 尝试转换为数字
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f, nil
		}

		// 对于字符串比较，我们需要返回一个哈希值或者错误
		// 这里简化处理，将字符串转换为其长度（作为临时解决方案）
		return float64(len(value)), nil

	case TypeField:
		// 从数据中获取字段值
		val, ok := data[node.Value]
		if !ok {
			return 0, fmt.Errorf("field %s not found in data", node.Value)
		}

		// 尝试转换为 float64
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
		default:
			// 尝试字符串转换
			if strVal, ok := val.(string); ok {
				if f, err := strconv.ParseFloat(strVal, 64); err == nil {
					return f, nil
				}
			}
			return 0, fmt.Errorf("cannot convert field %s value to number", node.Value)
		}

	case TypeOperator:
		// 计算左右子表达式的值
		left, err := evaluateNode(node.Left, data)
		if err != nil {
			return 0, err
		}

		right, err := evaluateNode(node.Right, data)
		if err != nil {
			return 0, err
		}

		// 执行运算
		switch node.Value {
		case "+":
			return left + right, nil
		case "-":
			return left - right, nil
		case "*":
			return left * right, nil
		case "/":
			if right == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			return left / right, nil
		case "%":
			if right == 0 {
				return 0, fmt.Errorf("modulo by zero")
			}
			return math.Mod(left, right), nil
		case "^":
			return math.Pow(left, right), nil
		default:
			return 0, fmt.Errorf("unknown operator: %s", node.Value)
		}

	case TypeFunction:
		// 首先检查是否是新的函数注册系统中的函数
		fn, exists := functions.Get(node.Value)
		if exists {
			// 计算所有参数
			args := make([]interface{}, len(node.Args))
			for i, arg := range node.Args {
				val, err := evaluateNode(arg, data)
				if err != nil {
					return 0, err
				}
				args[i] = val
			}

			// 创建函数执行上下文
			ctx := &functions.FunctionContext{
				Data: data,
			}

			// 执行函数
			result, err := fn.Execute(ctx, args)
			if err != nil {
				return 0, err
			}

			// 转换结果为 float64
			switch r := result.(type) {
			case float64:
				return r, nil
			case float32:
				return float64(r), nil
			case int:
				return float64(r), nil
			case int32:
				return float64(r), nil
			case int64:
				return float64(r), nil
			default:
				return 0, fmt.Errorf("function %s returned non-numeric value", node.Value)
			}
		}

		// 回退到内置函数处理（保持向后兼容）
		return evaluateBuiltinFunction(node, data)

	case TypeCase:
		// 处理CASE表达式
		return evaluateCaseExpression(node, data)
	}

	return 0, fmt.Errorf("unknown node type: %s", node.Type)
}

// evaluateBuiltinFunction 处理内置函数（向后兼容）
func evaluateBuiltinFunction(node *ExprNode, data map[string]interface{}) (float64, error) {
	switch strings.ToLower(node.Value) {
	case "abs":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("abs function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Abs(arg), nil

	case "sqrt":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("sqrt function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		if arg < 0 {
			return 0, fmt.Errorf("sqrt of negative number")
		}
		return math.Sqrt(arg), nil

	case "sin":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("sin function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Sin(arg), nil

	case "cos":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("cos function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Cos(arg), nil

	case "tan":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("tan function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Tan(arg), nil

	case "floor":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("floor function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Floor(arg), nil

	case "ceil":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("ceil function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Ceil(arg), nil

	case "round":
		if len(node.Args) != 1 {
			return 0, fmt.Errorf("round function requires exactly 1 argument")
		}
		arg, err := evaluateNode(node.Args[0], data)
		if err != nil {
			return 0, err
		}
		return math.Round(arg), nil

	default:
		return 0, fmt.Errorf("unknown function: %s", node.Value)
	}
}

// evaluateCaseExpression 计算CASE表达式
func evaluateCaseExpression(node *ExprNode, data map[string]interface{}) (float64, error) {
	if node.Type != TypeCase {
		return 0, fmt.Errorf("node is not a CASE expression")
	}

	// 处理简单CASE表达式 (CASE expr WHEN value1 THEN result1 ...)
	if node.CaseExpr != nil {
		// 计算CASE后面的表达式值
		caseValue, err := evaluateNodeValue(node.CaseExpr, data)
		if err != nil {
			return 0, err
		}

		// 遍历WHEN子句，查找匹配的值
		for _, whenClause := range node.WhenClauses {
			conditionValue, err := evaluateNodeValue(whenClause.Condition, data)
			if err != nil {
				return 0, err
			}

			// 比较值是否相等
			isEqual, err := compareValues(caseValue, conditionValue, "==")
			if err != nil {
				return 0, err
			}

			if isEqual {
				return evaluateNode(whenClause.Result, data)
			}
		}
	} else {
		// 处理搜索CASE表达式 (CASE WHEN condition1 THEN result1 ...)
		for _, whenClause := range node.WhenClauses {
			// 评估WHEN条件，这里需要特殊处理布尔表达式
			conditionResult, err := evaluateBooleanCondition(whenClause.Condition, data)
			if err != nil {
				return 0, err
			}

			// 如果条件为真，返回对应的结果
			if conditionResult {
				return evaluateNode(whenClause.Result, data)
			}
		}
	}

	// 如果没有匹配的WHEN子句，执行ELSE子句
	if node.ElseExpr != nil {
		return evaluateNode(node.ElseExpr, data)
	}

	// 如果没有ELSE子句，SQL标准是返回NULL，这里返回0
	return 0, nil
}

// evaluateBooleanCondition 计算布尔条件表达式
func evaluateBooleanCondition(node *ExprNode, data map[string]interface{}) (bool, error) {
	if node == nil {
		return false, fmt.Errorf("null condition expression")
	}

	// 处理逻辑运算符
	if node.Type == TypeOperator && (node.Value == "AND" || node.Value == "OR") {
		leftBool, err := evaluateBooleanCondition(node.Left, data)
		if err != nil {
			return false, err
		}

		rightBool, err := evaluateBooleanCondition(node.Right, data)
		if err != nil {
			return false, err
		}

		switch node.Value {
		case "AND":
			return leftBool && rightBool, nil
		case "OR":
			return leftBool || rightBool, nil
		}
	}

	// 处理比较运算符
	if node.Type == TypeOperator {
		leftValue, err := evaluateNodeValue(node.Left, data)
		if err != nil {
			return false, err
		}

		rightValue, err := evaluateNodeValue(node.Right, data)
		if err != nil {
			return false, err
		}

		return compareValues(leftValue, rightValue, node.Value)
	}

	// 对于其他表达式，计算其数值并转换为布尔值
	result, err := evaluateNode(node, data)
	if err != nil {
		return false, err
	}

	// 非零值为真，零值为假
	return result != 0, nil
}

// evaluateNodeValue 计算节点值，返回interface{}以支持不同类型
func evaluateNodeValue(node *ExprNode, data map[string]interface{}) (interface{}, error) {
	if node == nil {
		return nil, fmt.Errorf("null expression node")
	}

	switch node.Type {
	case TypeNumber:
		return strconv.ParseFloat(node.Value, 64)

	case TypeString:
		// 去掉引号
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1]
		}
		return value, nil

	case TypeField:
		val, ok := data[node.Value]
		if !ok {
			return nil, fmt.Errorf("field %s not found in data", node.Value)
		}
		return val, nil

	default:
		// 对于其他类型，回退到数值计算
		return evaluateNode(node, data)
	}
}

// compareValues 比较两个值
func compareValues(left, right interface{}, operator string) (bool, error) {
	// 尝试字符串比较
	leftStr, leftIsStr := left.(string)
	rightStr, rightIsStr := right.(string)

	if leftIsStr && rightIsStr {
		switch operator {
		case "==", "=":
			return leftStr == rightStr, nil
		case "!=", "<>":
			return leftStr != rightStr, nil
		case ">":
			return leftStr > rightStr, nil
		case ">=":
			return leftStr >= rightStr, nil
		case "<":
			return leftStr < rightStr, nil
		case "<=":
			return leftStr <= rightStr, nil
		default:
			return false, fmt.Errorf("unsupported string comparison operator: %s", operator)
		}
	}

	// 转换为数值进行比较
	leftNum, err1 := convertToFloat(left)
	rightNum, err2 := convertToFloat(right)

	if err1 != nil || err2 != nil {
		return false, fmt.Errorf("cannot compare values: %v and %v", left, right)
	}

	switch operator {
	case ">":
		return leftNum > rightNum, nil
	case ">=":
		return leftNum >= rightNum, nil
	case "<":
		return leftNum < rightNum, nil
	case "<=":
		return leftNum <= rightNum, nil
	case "==", "=":
		return math.Abs(leftNum-rightNum) < 1e-9, nil
	case "!=", "<>":
		return math.Abs(leftNum-rightNum) >= 1e-9, nil
	default:
		return false, fmt.Errorf("unsupported comparison operator: %s", operator)
	}
}

// convertToFloat 将值转换为float64
func convertToFloat(val interface{}) (float64, error) {
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

// tokenize 将表达式字符串转换为token列表
func tokenize(expr string) ([]string, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty expression")
	}

	tokens := make([]string, 0)
	i := 0

	for i < len(expr) {
		ch := expr[i]

		// 跳过空白字符
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			i++
			continue
		}

		// 处理数字
		if isDigit(ch) || (ch == '.' && i+1 < len(expr) && isDigit(expr[i+1])) {
			start := i
			hasDot := ch == '.'

			i++
			for i < len(expr) && (isDigit(expr[i]) || (expr[i] == '.' && !hasDot)) {
				if expr[i] == '.' {
					hasDot = true
				}
				i++
			}

			tokens = append(tokens, expr[start:i])
			continue
		}

		// 处理运算符和括号
		if ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '%' || ch == '^' ||
			ch == '(' || ch == ')' || ch == ',' {

			// 特殊处理负号：如果是负号且前面是运算符、括号或开始位置，则可能是负数
			if ch == '-' {
				// 检查是否可能是负数的开始
				prevTokenIndex := len(tokens) - 1
				canBeNegativeNumber := i == 0 || // 表达式开始
					tokens[prevTokenIndex] == "(" || // 左括号后
					tokens[prevTokenIndex] == "," || // 逗号后（函数参数）
					isOperator(tokens[prevTokenIndex]) || // 运算符后
					strings.ToUpper(tokens[prevTokenIndex]) == "THEN" || // THEN后
					strings.ToUpper(tokens[prevTokenIndex]) == "ELSE" // ELSE后

				if canBeNegativeNumber && i+1 < len(expr) && isDigit(expr[i+1]) {
					// 这是一个负数，解析整个数字
					start := i
					i++ // 跳过负号

					// 解析数字部分
					for i < len(expr) && (isDigit(expr[i]) || expr[i] == '.') {
						i++
					}

					tokens = append(tokens, expr[start:i])
					continue
				}
			}

			tokens = append(tokens, string(ch))
			i++
			continue
		}

		// 处理比较运算符
		if ch == '>' || ch == '<' || ch == '=' || ch == '!' {
			start := i
			i++

			// 处理双字符运算符
			if i < len(expr) {
				switch ch {
				case '>':
					if expr[i] == '=' {
						i++
						tokens = append(tokens, ">=")
						continue
					}
				case '<':
					if expr[i] == '=' {
						i++
						tokens = append(tokens, "<=")
						continue
					} else if expr[i] == '>' {
						i++
						tokens = append(tokens, "<>")
						continue
					}
				case '=':
					if expr[i] == '=' {
						i++
						tokens = append(tokens, "==")
						continue
					}
				case '!':
					if expr[i] == '=' {
						i++
						tokens = append(tokens, "!=")
						continue
					}
				}
			}

			// 单字符运算符
			tokens = append(tokens, expr[start:i])
			continue
		}

		// 处理字符串字面量（单引号和双引号）
		if ch == '\'' || ch == '"' {
			quote := ch
			start := i
			i++ // 跳过开始引号

			// 寻找结束引号
			for i < len(expr) && expr[i] != quote {
				if expr[i] == '\\' && i+1 < len(expr) {
					i += 2 // 跳过转义字符
				} else {
					i++
				}
			}

			if i >= len(expr) {
				return nil, fmt.Errorf("unterminated string literal starting at position %d", start)
			}

			i++ // 跳过结束引号
			tokens = append(tokens, expr[start:i])
			continue
		}

		// 处理标识符（字段名或函数名）
		if isLetter(ch) {
			start := i
			i++
			for i < len(expr) && (isLetter(expr[i]) || isDigit(expr[i]) || expr[i] == '_') {
				i++
			}

			tokens = append(tokens, expr[start:i])
			continue
		}

		// 未知字符
		return nil, fmt.Errorf("unexpected character: %c at position %d", ch, i)
	}

	return tokens, nil
}

// parseExpression 解析表达式
func parseExpression(tokens []string) (*ExprNode, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty token list")
	}

	// 使用Shunting-yard算法处理运算符优先级
	output := make([]*ExprNode, 0)
	operators := make([]string, 0)

	i := 0
	for i < len(tokens) {
		token := tokens[i]

		// 处理数字
		if isNumber(token) {
			output = append(output, &ExprNode{
				Type:  TypeNumber,
				Value: token,
			})
			i++
			continue
		}

		// 处理字符串字面量
		if isStringLiteral(token) {
			output = append(output, &ExprNode{
				Type:  TypeString,
				Value: token,
			})
			i++
			continue
		}

		// 处理字段名或函数调用
		if isIdentifier(token) {
			// 检查是否是逻辑运算符关键字
			upperToken := strings.ToUpper(token)
			if upperToken == "AND" || upperToken == "OR" || upperToken == "NOT" {
				// 处理逻辑运算符
				for len(operators) > 0 && operators[len(operators)-1] != "(" &&
					operatorPrecedence[operators[len(operators)-1]] >= operatorPrecedence[upperToken] {
					op := operators[len(operators)-1]
					operators = operators[:len(operators)-1]

					if len(output) < 2 {
						return nil, fmt.Errorf("not enough operands for operator: %s", op)
					}

					right := output[len(output)-1]
					left := output[len(output)-2]
					output = output[:len(output)-2]

					output = append(output, &ExprNode{
						Type:  TypeOperator,
						Value: op,
						Left:  left,
						Right: right,
					})
				}

				operators = append(operators, upperToken)
				i++
				continue
			}

			// 检查是否是CASE表达式
			if strings.ToUpper(token) == "CASE" {
				caseNode, newIndex, err := parseCaseExpression(tokens, i)
				if err != nil {
					return nil, err
				}
				output = append(output, caseNode)
				i = newIndex
				continue
			}

			// 检查下一个token是否是左括号，如果是则为函数调用
			if i+1 < len(tokens) && tokens[i+1] == "(" {
				funcName := token
				i += 2 // 跳过函数名和左括号

				// 解析函数参数
				args, newIndex, err := parseFunctionArgs(tokens, i)
				if err != nil {
					return nil, err
				}

				output = append(output, &ExprNode{
					Type:  TypeFunction,
					Value: funcName,
					Args:  args,
				})

				i = newIndex
				continue
			}

			// 普通字段
			output = append(output, &ExprNode{
				Type:  TypeField,
				Value: token,
			})
			i++
			continue
		}

		// 处理左括号
		if token == "(" {
			operators = append(operators, token)
			i++
			continue
		}

		// 处理右括号
		if token == ")" {
			for len(operators) > 0 && operators[len(operators)-1] != "(" {
				op := operators[len(operators)-1]
				operators = operators[:len(operators)-1]

				if len(output) < 2 {
					return nil, fmt.Errorf("not enough operands for operator: %s", op)
				}

				right := output[len(output)-1]
				left := output[len(output)-2]
				output = output[:len(output)-2]

				output = append(output, &ExprNode{
					Type:  TypeOperator,
					Value: op,
					Left:  left,
					Right: right,
				})
			}

			if len(operators) == 0 || operators[len(operators)-1] != "(" {
				return nil, fmt.Errorf("mismatched parentheses")
			}

			operators = operators[:len(operators)-1] // 弹出左括号
			i++
			continue
		}

		// 处理运算符
		if isOperator(token) {
			for len(operators) > 0 && operators[len(operators)-1] != "(" &&
				operatorPrecedence[operators[len(operators)-1]] >= operatorPrecedence[token] {
				op := operators[len(operators)-1]
				operators = operators[:len(operators)-1]

				if len(output) < 2 {
					return nil, fmt.Errorf("not enough operands for operator: %s", op)
				}

				right := output[len(output)-1]
				left := output[len(output)-2]
				output = output[:len(output)-2]

				output = append(output, &ExprNode{
					Type:  TypeOperator,
					Value: op,
					Left:  left,
					Right: right,
				})
			}

			operators = append(operators, token)
			i++
			continue
		}

		// 处理逗号（在函数参数列表中处理）
		if token == "," {
			i++
			continue
		}

		return nil, fmt.Errorf("unexpected token: %s", token)
	}

	// 处理剩余的运算符
	for len(operators) > 0 {
		op := operators[len(operators)-1]
		operators = operators[:len(operators)-1]

		if op == "(" {
			return nil, fmt.Errorf("mismatched parentheses")
		}

		if len(output) < 2 {
			return nil, fmt.Errorf("not enough operands for operator: %s", op)
		}

		right := output[len(output)-1]
		left := output[len(output)-2]
		output = output[:len(output)-2]

		output = append(output, &ExprNode{
			Type:  TypeOperator,
			Value: op,
			Left:  left,
			Right: right,
		})
	}

	if len(output) != 1 {
		return nil, fmt.Errorf("invalid expression")
	}

	return output[0], nil
}

// parseFunctionArgs 解析函数参数
func parseFunctionArgs(tokens []string, startIndex int) ([]*ExprNode, int, error) {
	args := make([]*ExprNode, 0)
	i := startIndex

	// 处理空参数列表
	if i < len(tokens) && tokens[i] == ")" {
		return args, i + 1, nil
	}

	for i < len(tokens) {
		// 解析参数表达式
		argTokens := make([]string, 0)
		parenthesesCount := 0

		for i < len(tokens) {
			token := tokens[i]

			if token == "(" {
				parenthesesCount++
			} else if token == ")" {
				parenthesesCount--
				if parenthesesCount < 0 {
					break
				}
			} else if token == "," && parenthesesCount == 0 {
				break
			}

			argTokens = append(argTokens, token)
			i++
		}

		if len(argTokens) > 0 {
			arg, err := parseExpression(argTokens)
			if err != nil {
				return nil, 0, err
			}
			args = append(args, arg)
		}

		if i >= len(tokens) {
			return nil, 0, fmt.Errorf("unexpected end of tokens in function arguments")
		}

		if tokens[i] == ")" {
			return args, i + 1, nil
		}

		if tokens[i] == "," {
			i++
			continue
		}

		return nil, 0, fmt.Errorf("unexpected token in function arguments: %s", tokens[i])
	}

	return nil, 0, fmt.Errorf("unexpected end of tokens in function arguments")
}

// parseCaseExpression 解析CASE表达式
func parseCaseExpression(tokens []string, startIndex int) (*ExprNode, int, error) {
	if startIndex >= len(tokens) || strings.ToUpper(tokens[startIndex]) != "CASE" {
		return nil, startIndex, fmt.Errorf("expected CASE keyword")
	}

	caseNode := &ExprNode{
		Type:        TypeCase,
		WhenClauses: make([]WhenClause, 0),
	}

	i := startIndex + 1 // 跳过CASE关键字

	// 检查是否是简单CASE表达式（CASE expr WHEN value1 THEN result1 ...）
	// 或搜索CASE表达式（CASE WHEN condition1 THEN result1 ...）
	if i < len(tokens) && strings.ToUpper(tokens[i]) != "WHEN" {
		// 这是简单CASE表达式，需要解析CASE后面的表达式
		caseExprTokens := make([]string, 0)

		// 收集CASE表达式直到遇到WHEN
		for i < len(tokens) && strings.ToUpper(tokens[i]) != "WHEN" {
			caseExprTokens = append(caseExprTokens, tokens[i])
			i++
		}

		if len(caseExprTokens) == 0 {
			return nil, i, fmt.Errorf("expected expression after CASE")
		}

		// 对于简单的情况，直接处理单个token
		if len(caseExprTokens) == 1 {
			token := caseExprTokens[0]
			if isNumber(token) {
				caseNode.CaseExpr = &ExprNode{Type: TypeNumber, Value: token}
			} else if isStringLiteral(token) {
				caseNode.CaseExpr = &ExprNode{Type: TypeString, Value: token}
			} else if isIdentifier(token) {
				caseNode.CaseExpr = &ExprNode{Type: TypeField, Value: token}
			} else {
				return nil, i, fmt.Errorf("invalid CASE expression token: %s", token)
			}
		} else {
			// 对于复杂表达式，调用parseExpression
			caseExpr, err := parseExpression(caseExprTokens)
			if err != nil {
				return nil, i, fmt.Errorf("failed to parse CASE expression: %w", err)
			}
			caseNode.CaseExpr = caseExpr
		}
	}

	// 解析WHEN子句
	for i < len(tokens) && strings.ToUpper(tokens[i]) == "WHEN" {
		i++ // 跳过WHEN关键字

		// 收集WHEN条件直到遇到THEN
		conditionTokens := make([]string, 0)
		for i < len(tokens) && strings.ToUpper(tokens[i]) != "THEN" {
			conditionTokens = append(conditionTokens, tokens[i])
			i++
		}

		if len(conditionTokens) == 0 {
			return nil, i, fmt.Errorf("expected condition after WHEN")
		}

		if i >= len(tokens) || strings.ToUpper(tokens[i]) != "THEN" {
			return nil, i, fmt.Errorf("expected THEN after WHEN condition")
		}

		i++ // 跳过THEN关键字

		// 收集THEN结果直到遇到WHEN、ELSE或END
		resultTokens := make([]string, 0)
		for i < len(tokens) {
			upper := strings.ToUpper(tokens[i])
			if upper == "WHEN" || upper == "ELSE" || upper == "END" {
				break
			}
			resultTokens = append(resultTokens, tokens[i])
			i++
		}

		if len(resultTokens) == 0 {
			return nil, i, fmt.Errorf("expected result after THEN")
		}

		// 解析条件和结果表达式
		conditionExpr, err := parseExpression(conditionTokens)
		if err != nil {
			return nil, i, fmt.Errorf("failed to parse WHEN condition: %w", err)
		}

		resultExpr, err := parseExpression(resultTokens)
		if err != nil {
			return nil, i, fmt.Errorf("failed to parse THEN result: %w", err)
		}

		// 添加WHEN子句
		caseNode.WhenClauses = append(caseNode.WhenClauses, WhenClause{
			Condition: conditionExpr,
			Result:    resultExpr,
		})
	}

	// 检查是否有ELSE子句
	if i < len(tokens) && strings.ToUpper(tokens[i]) == "ELSE" {
		i++ // 跳过ELSE关键字

		// 收集ELSE结果直到遇到END
		elseTokens := make([]string, 0)
		for i < len(tokens) && strings.ToUpper(tokens[i]) != "END" {
			elseTokens = append(elseTokens, tokens[i])
			i++
		}

		if len(elseTokens) == 0 {
			return nil, i, fmt.Errorf("expected result after ELSE")
		}

		// 解析ELSE表达式
		elseExpr, err := parseExpression(elseTokens)
		if err != nil {
			return nil, i, fmt.Errorf("failed to parse ELSE result: %w", err)
		}
		caseNode.ElseExpr = elseExpr
	}

	// 检查END关键字
	if i >= len(tokens) || strings.ToUpper(tokens[i]) != "END" {
		return nil, i, fmt.Errorf("expected END to close CASE expression")
	}

	i++ // 跳过END关键字

	// 验证至少有一个WHEN子句
	if len(caseNode.WhenClauses) == 0 {
		return nil, i, fmt.Errorf("CASE expression must have at least one WHEN clause")
	}

	return caseNode, i, nil
}

// 辅助函数
func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isNumber(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	if !isLetter(s[0]) && s[0] != '_' {
		return false
	}

	for i := 1; i < len(s); i++ {
		if !isLetter(s[i]) && !isDigit(s[i]) && s[i] != '_' {
			return false
		}
	}

	return true
}

func isOperator(s string) bool {
	switch s {
	case "+", "-", "*", "/", "%", "^":
		return true
	case ">", "<", ">=", "<=", "==", "=", "!=", "<>":
		return true
	case "AND", "OR", "NOT":
		return true
	default:
		return false
	}
}

func isStringLiteral(expr string) bool {
	return len(expr) > 1 && (expr[0] == '\'' || expr[0] == '"') && expr[len(expr)-1] == expr[0]
}
