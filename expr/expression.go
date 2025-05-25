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
)

// 操作符优先级
var operatorPrecedence = map[string]int{
	"+": 1,
	"-": 1,
	"*": 2,
	"/": 2,
	"%": 2,
	"^": 3, // 幂运算
}

// 表达式节点
type ExprNode struct {
	Type  string
	Value string
	Left  *ExprNode
	Right *ExprNode
	Args  []*ExprNode // 用于函数调用的参数
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

		// 处理运算符和括号
		if ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '%' || ch == '^' ||
			ch == '(' || ch == ')' || ch == ',' {
			tokens = append(tokens, string(ch))
			i++
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

		// 处理字段名或函数调用
		if isIdentifier(token) {
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
	_, ok := operatorPrecedence[s]
	return ok
}
