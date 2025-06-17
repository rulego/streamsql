package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/utils/fieldpath"
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
	">": 4, "<": 4, ">=": 4, "<=": 4, "LIKE": 4, "IS": 4,
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
	// 进行基本的语法验证
	if err := validateBasicSyntax(exprStr); err != nil {
		return nil, err
	}

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

// validateBasicSyntax 进行基本的语法验证
func validateBasicSyntax(exprStr string) error {
	// 检查空表达式
	trimmed := strings.TrimSpace(exprStr)
	if trimmed == "" {
		return fmt.Errorf("empty expression")
	}

	// 检查不匹配的括号
	parenthesesCount := 0
	for _, ch := range trimmed {
		if ch == '(' {
			parenthesesCount++
		} else if ch == ')' {
			parenthesesCount--
			if parenthesesCount < 0 {
				return fmt.Errorf("mismatched parentheses")
			}
		}
	}
	if parenthesesCount != 0 {
		return fmt.Errorf("mismatched parentheses")
	}

	// 检查无效字符
	for i, ch := range trimmed {
		// 允许的字符：字母、数字、运算符、括号、点、下划线、空格、引号
		if !isValidChar(ch) {
			return fmt.Errorf("invalid character '%c' at position %d", ch, i)
		}
	}

	// 检查连续运算符
	if err := checkConsecutiveOperators(trimmed); err != nil {
		return err
	}

	return nil
}

// checkConsecutiveOperators 检查连续运算符
func checkConsecutiveOperators(expr string) error {
	// 简化的连续运算符检查：查找明显的双运算符模式
	// 但要允许比较运算符后跟负数的情况
	operators := []string{"+", "-", "*", "/", "%", "^", "==", "!=", ">=", "<=", ">", "<"}
	comparisonOps := []string{"==", "!=", ">=", "<=", ">", "<"}

	for i := 0; i < len(expr)-1; i++ {
		// 跳过空白字符
		if expr[i] == ' ' || expr[i] == '\t' {
			continue
		}

		// 检查当前位置是否是运算符
		isCurrentOp := false
		currentOpLen := 0
		currentOp := ""
		for _, op := range operators {
			if i+len(op) <= len(expr) && expr[i:i+len(op)] == op {
				isCurrentOp = true
				currentOpLen = len(op)
				currentOp = op
				break
			}
		}

		if isCurrentOp {
			// 查找下一个非空白字符
			nextPos := i + currentOpLen
			for nextPos < len(expr) && (expr[nextPos] == ' ' || expr[nextPos] == '\t') {
				nextPos++
			}

			// 检查下一个字符是否也是运算符
			if nextPos < len(expr) {
				// 特殊处理：如果当前是比较运算符，下一个是负号，且负号后跟数字，则允许
				isCurrentComparison := false
				for _, compOp := range comparisonOps {
					if currentOp == compOp {
						isCurrentComparison = true
						break
					}
				}

				// 检查是否是负数的情况
				if isCurrentComparison && nextPos < len(expr) && expr[nextPos] == '-' {
					// 检查负号后是否跟数字
					digitPos := nextPos + 1
					for digitPos < len(expr) && (expr[digitPos] == ' ' || expr[digitPos] == '\t') {
						digitPos++
					}
					if digitPos < len(expr) && expr[digitPos] >= '0' && expr[digitPos] <= '9' {
						// 这是比较运算符后跟负数，允许通过
						i = nextPos // 跳过到负号位置
						continue
					}
				}

				// 检查其他连续运算符
				for _, op := range operators {
					if nextPos+len(op) <= len(expr) && expr[nextPos:nextPos+len(op)] == op {
						// 如果不是允许的负数情况，则报错
						return fmt.Errorf("consecutive operators found: '%s' followed by '%s'",
							currentOp, op)
					}
				}
			}

			// 跳过当前运算符
			i += currentOpLen - 1
		}
	}

	return nil
}

// isValidChar 检查字符是否有效
func isValidChar(ch rune) bool {
	// 字母和数字
	if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
		return true
	}
	// 特殊字符
	switch ch {
	case ' ', '\t', '\n', '\r': // 空白字符
		return true
	case '+', '-', '*', '/', '%', '^': // 算术运算符
		return true
	case '(', ')', ',': // 括号和逗号
		return true
	case '>', '<', '=', '!': // 比较运算符
		return true
	case '\'', '"': // 引号
		return true
	case '.', '_': // 点和下划线
		return true
	case '$': // 美元符号（用于JSON路径等）
		return true
	default:
		return false
	}
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

	// 简单的字段提取：查找标识符模式，支持点号分隔的嵌套字段
	tokens := strings.FieldsFunc(expression, func(c rune) bool {
		return !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '_' && c != '.'
	})

	for _, token := range tokens {
		if isValidFieldIdentifier(token) && !isNumber(token) && !isFunctionOrKeyword(token) {
			fields[token] = true
		}
	}

	result := make([]string, 0, len(fields))
	for field := range fields {
		result = append(result, field)
	}
	return result
}

// isValidFieldIdentifier 检查是否是有效的字段标识符（支持点号分隔的嵌套字段）
func isValidFieldIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	// 分割点号分隔的字段
	parts := strings.Split(s, ".")
	for _, part := range parts {
		if !isIdentifier(part) {
			return false
		}
	}

	return true
}

// isFunctionOrKeyword 检查是否是函数名或关键字
func isFunctionOrKeyword(token string) bool {
	// 检查是否是已知函数或关键字
	keywords := []string{
		"and", "or", "not", "true", "false", "nil", "null", "is",
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
		// 支持嵌套字段访问
		if fieldpath.IsNestedField(node.Value) {
			if val, found := fieldpath.GetNestedField(data, node.Value); found {
				// 尝试转换为float64
				if floatVal, err := convertToFloat(val); err == nil {
					return floatVal, nil
				}
				// 如果不能转换为数字，返回错误
				return 0, fmt.Errorf("field '%s' value cannot be converted to number: %v", node.Value, val)
			}
		} else {
			// 原有的简单字段访问
			if val, found := data[node.Value]; found {
				// 尝试转换为float64
				if floatVal, err := convertToFloat(val); err == nil {
					return floatVal, nil
				}
				// 如果不能转换为数字，返回错误
				return 0, fmt.Errorf("field '%s' value cannot be converted to number: %v", node.Value, val)
			}
		}
		return 0, fmt.Errorf("field '%s' not found", node.Value)

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
			// 计算所有参数，但保持原始类型
			args := make([]interface{}, len(node.Args))
			for i, arg := range node.Args {
				// 使用evaluateNodeValue获取原始类型的值
				val, err := evaluateNodeValue(arg, data)
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
			case string:
				// 对于字符串结果，尝试转换为数字，如果失败则返回字符串长度
				if f, err := strconv.ParseFloat(r, 64); err == nil {
					return f, nil
				}
				return float64(len(r)), nil
			case bool:
				// 布尔值转换：true=1, false=0
				if r {
					return 1.0, nil
				}
				return 0.0, nil
			default:
				return 0, fmt.Errorf("function %s returned unsupported type for numeric conversion: %T", node.Value, result)
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

	// 处理逻辑运算符（实现短路求值）
	if node.Type == TypeOperator && (node.Value == "AND" || node.Value == "OR") {
		leftBool, err := evaluateBooleanCondition(node.Left, data)
		if err != nil {
			return false, err
		}

		// 短路求值：对于AND，如果左边为false，立即返回false
		if node.Value == "AND" && !leftBool {
			return false, nil
		}

		// 短路求值：对于OR，如果左边为true，立即返回true
		if node.Value == "OR" && leftBool {
			return true, nil
		}

		// 只有在需要时才评估右边的表达式
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

	// 处理IS NULL和IS NOT NULL特殊情况
	if node.Type == TypeOperator && node.Value == "IS" {
		return evaluateIsCondition(node, data)
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

// evaluateIsCondition 处理IS NULL和IS NOT NULL条件
func evaluateIsCondition(node *ExprNode, data map[string]interface{}) (bool, error) {
	if node == nil || node.Left == nil || node.Right == nil {
		return false, fmt.Errorf("invalid IS condition")
	}

	// 获取左侧值
	leftValue, err := evaluateNodeValue(node.Left, data)
	if err != nil {
		// 如果字段不存在，认为是null
		leftValue = nil
	}

	// 检查右侧是否是NULL或NOT NULL
	if node.Right.Type == TypeField && strings.ToUpper(node.Right.Value) == "NULL" {
		// IS NULL
		return leftValue == nil, nil
	}

	// 检查是否是IS NOT NULL
	if node.Right.Type == TypeOperator && node.Right.Value == "NOT" &&
		node.Right.Right != nil && node.Right.Right.Type == TypeField &&
		strings.ToUpper(node.Right.Right.Value) == "NULL" {
		// IS NOT NULL
		return leftValue != nil, nil
	}

	// 其他IS比较（如IS TRUE, IS FALSE等，暂不支持）
	rightValue, err := evaluateNodeValue(node.Right, data)
	if err != nil {
		return false, err
	}

	return compareValues(leftValue, rightValue, "==")
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
		// 支持嵌套字段访问
		if fieldpath.IsNestedField(node.Value) {
			if val, found := fieldpath.GetNestedField(data, node.Value); found {
				return val, nil
			}
		} else {
			// 原有的简单字段访问
			if val, found := data[node.Value]; found {
				return val, nil
			}
		}
		return nil, fmt.Errorf("field '%s' not found", node.Value)

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
		case "LIKE":
			return matchesLikePattern(leftStr, rightStr), nil
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

// matchesLikePattern 实现LIKE模式匹配
// 支持%（匹配任意字符序列）和_（匹配单个字符）
func matchesLikePattern(text, pattern string) bool {
	return likeMatch(text, pattern, 0, 0)
}

// likeMatch 递归实现LIKE匹配算法
func likeMatch(text, pattern string, textIndex, patternIndex int) bool {
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

	switch pattern[patternIndex] {
	case '%':
		// %可以匹配0个或多个字符
		// 尝试匹配0个字符（跳过%）
		if likeMatch(text, pattern, textIndex, patternIndex+1) {
			return true
		}
		// 尝试匹配1个或多个字符
		for i := textIndex; i < len(text); i++ {
			if likeMatch(text, pattern, i+1, patternIndex+1) {
				return true
			}
		}
		return false

	case '_':
		// _匹配任意单个字符
		return likeMatch(text, pattern, textIndex+1, patternIndex+1)

	default:
		// 普通字符必须精确匹配
		if text[textIndex] == pattern[patternIndex] {
			return likeMatch(text, pattern, textIndex+1, patternIndex+1)
		}
		return false
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
				canBeNegativeNumber := i == 0 || // 表达式开始
					len(tokens) == 0 // tokens为空时也可能是负数开始

				// 只有当tokens不为空时才检查前一个token
				if len(tokens) > 0 {
					prevToken := tokens[len(tokens)-1]
					canBeNegativeNumber = canBeNegativeNumber ||
						prevToken == "(" || // 左括号后
						prevToken == "," || // 逗号后（函数参数）
						isOperator(prevToken) || // 运算符后
						isComparisonOperator(prevToken) || // 比较运算符后
						strings.ToUpper(prevToken) == "THEN" || // THEN后
						strings.ToUpper(prevToken) == "ELSE" || // ELSE后
						strings.ToUpper(prevToken) == "WHEN" || // WHEN后
						strings.ToUpper(prevToken) == "AND" || // AND后
						strings.ToUpper(prevToken) == "OR" // OR后
				}

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
			if upperToken == "AND" || upperToken == "OR" || upperToken == "NOT" || upperToken == "LIKE" {
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

			// 特殊处理IS运算符，需要检查后续的NOT NULL组合
			if upperToken == "IS" {
				// 处理待处理的运算符
				for len(operators) > 0 && operators[len(operators)-1] != "(" &&
					operatorPrecedence[operators[len(operators)-1]] >= operatorPrecedence["IS"] {
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

				// 检查是否是IS NOT NULL模式
				if i+2 < len(tokens) &&
					strings.ToUpper(tokens[i+1]) == "NOT" &&
					strings.ToUpper(tokens[i+2]) == "NULL" {
					// 这是IS NOT NULL，创建特殊的右侧节点结构
					notNullNode := &ExprNode{
						Type:  TypeOperator,
						Value: "NOT",
						Right: &ExprNode{
							Type:  TypeField,
							Value: "NULL",
						},
					}

					operators = append(operators, "IS")
					output = append(output, notNullNode)
					i += 3 // 跳过IS NOT NULL三个token
					continue
				} else if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "NULL" {
					// 这是IS NULL，创建NULL节点
					nullNode := &ExprNode{
						Type:  TypeField,
						Value: "NULL",
					}

					operators = append(operators, "IS")
					output = append(output, nullNode)
					i += 2 // 跳过IS NULL两个token
					continue
				} else {
					// 普通的IS运算符
					operators = append(operators, "IS")
					i++
					continue
				}
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
	case "LIKE", "IS":
		return true
	default:
		return false
	}
}

// isComparisonOperator 检查是否是比较运算符
func isComparisonOperator(s string) bool {
	switch s {
	case ">", "<", ">=", "<=", "==", "=", "!=", "<>":
		return true
	default:
		return false
	}
}

func isStringLiteral(expr string) bool {
	return len(expr) > 1 && (expr[0] == '\'' || expr[0] == '"') && expr[len(expr)-1] == expr[0]
}

// evaluateNodeWithNull 计算节点值，支持NULL值返回
// 返回 (result, isNull, error)
func evaluateNodeWithNull(node *ExprNode, data map[string]interface{}) (float64, bool, error) {
	if node == nil {
		return 0, true, nil // NULL
	}

	switch node.Type {
	case TypeNumber:
		val, err := strconv.ParseFloat(node.Value, 64)
		return val, false, err

	case TypeString:
		// 字符串长度作为数值，特殊处理NULL字符串
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1]
		}
		// 检查是否是NULL字符串
		if strings.ToUpper(value) == "NULL" {
			return 0, true, nil
		}
		return float64(len(value)), false, nil

	case TypeField:
		// 支持嵌套字段访问
		var fieldVal interface{}
		var found bool

		if fieldpath.IsNestedField(node.Value) {
			fieldVal, found = fieldpath.GetNestedField(data, node.Value)
		} else {
			fieldVal, found = data[node.Value]
		}

		if !found || fieldVal == nil {
			return 0, true, nil // NULL
		}

		// 尝试转换为数值
		if val, err := convertToFloat(fieldVal); err == nil {
			return val, false, nil
		}
		return 0, true, fmt.Errorf("cannot convert field '%s' to number", node.Value)

	case TypeOperator:
		return evaluateOperatorWithNull(node, data)

	case TypeFunction:
		// 函数调用保持原有逻辑，但处理NULL结果
		result, err := evaluateBuiltinFunction(node, data)
		return result, false, err

	case TypeCase:
		return evaluateCaseExpressionWithNull(node, data)

	default:
		return 0, true, fmt.Errorf("unsupported node type: %s", node.Type)
	}
}

// evaluateOperatorWithNull 计算运算符表达式，支持NULL值
func evaluateOperatorWithNull(node *ExprNode, data map[string]interface{}) (float64, bool, error) {
	leftVal, leftNull, err := evaluateNodeWithNull(node.Left, data)
	if err != nil {
		return 0, false, err
	}

	rightVal, rightNull, err := evaluateNodeWithNull(node.Right, data)
	if err != nil {
		return 0, false, err
	}

	// 算术运算：如果任一操作数为NULL，结果为NULL
	if leftNull || rightNull {
		switch node.Value {
		case "+", "-", "*", "/", "%", "^":
			return 0, true, nil
		}
	}

	// 比较运算：NULL值的比较有特殊规则
	switch node.Value {
	case "==", "=":
		if leftNull && rightNull {
			return 1, false, nil // NULL = NULL 为 true
		}
		if leftNull || rightNull {
			return 0, false, nil // NULL = value 为 false
		}
		if leftVal == rightVal {
			return 1, false, nil
		}
		return 0, false, nil

	case "!=", "<>":
		if leftNull && rightNull {
			return 0, false, nil // NULL != NULL 为 false
		}
		if leftNull || rightNull {
			return 0, false, nil // NULL != value 为 false
		}
		if leftVal != rightVal {
			return 1, false, nil
		}
		return 0, false, nil

	case ">", "<", ">=", "<=":
		if leftNull || rightNull {
			return 0, false, nil // NULL与任何值的比较都为false
		}
	}

	// 对于非NULL值，执行正常的算术和比较运算
	switch node.Value {
	case "+":
		return leftVal + rightVal, false, nil
	case "-":
		return leftVal - rightVal, false, nil
	case "*":
		return leftVal * rightVal, false, nil
	case "/":
		if rightVal == 0 {
			return 0, true, nil // 除零返回NULL
		}
		return leftVal / rightVal, false, nil
	case "%":
		if rightVal == 0 {
			return 0, true, nil
		}
		return math.Mod(leftVal, rightVal), false, nil
	case "^":
		return math.Pow(leftVal, rightVal), false, nil
	case ">":
		if leftVal > rightVal {
			return 1, false, nil
		}
		return 0, false, nil
	case "<":
		if leftVal < rightVal {
			return 1, false, nil
		}
		return 0, false, nil
	case ">=":
		if leftVal >= rightVal {
			return 1, false, nil
		}
		return 0, false, nil
	case "<=":
		if leftVal <= rightVal {
			return 1, false, nil
		}
		return 0, false, nil
	default:
		return 0, false, fmt.Errorf("unsupported operator: %s", node.Value)
	}
}

// evaluateCaseExpressionWithNull 计算CASE表达式，支持NULL值
func evaluateCaseExpressionWithNull(node *ExprNode, data map[string]interface{}) (float64, bool, error) {
	if node.Type != TypeCase {
		return 0, false, fmt.Errorf("node is not a CASE expression")
	}

	// 处理简单CASE表达式 (CASE expr WHEN value1 THEN result1 ...)
	if node.CaseExpr != nil {
		// 计算CASE后面的表达式值
		caseValue, caseNull, err := evaluateNodeValueWithNull(node.CaseExpr, data)
		if err != nil {
			return 0, false, err
		}

		// 遍历WHEN子句，查找匹配的值
		for _, whenClause := range node.WhenClauses {
			conditionValue, condNull, err := evaluateNodeValueWithNull(whenClause.Condition, data)
			if err != nil {
				return 0, false, err
			}

			// 比较值是否相等（考虑NULL值）
			var isEqual bool
			if caseNull && condNull {
				isEqual = true // NULL = NULL
			} else if caseNull || condNull {
				isEqual = false // NULL != value
			} else {
				isEqual, err = compareValuesForEquality(caseValue, conditionValue)
				if err != nil {
					return 0, false, err
				}
			}

			if isEqual {
				return evaluateNodeWithNull(whenClause.Result, data)
			}
		}
	} else {
		// 处理搜索CASE表达式 (CASE WHEN condition1 THEN result1 ...)
		for _, whenClause := range node.WhenClauses {
			// 评估WHEN条件
			conditionResult, err := evaluateBooleanConditionWithNull(whenClause.Condition, data)
			if err != nil {
				return 0, false, err
			}

			// 如果条件为真，返回对应的结果
			if conditionResult {
				return evaluateNodeWithNull(whenClause.Result, data)
			}
		}
	}

	// 如果没有匹配的WHEN子句，执行ELSE子句
	if node.ElseExpr != nil {
		return evaluateNodeWithNull(node.ElseExpr, data)
	}

	// 如果没有ELSE子句，SQL标准是返回NULL
	return 0, true, nil
}

// evaluateNodeValueWithNull 计算节点值，返回interface{}以支持不同类型，包含NULL检查
func evaluateNodeValueWithNull(node *ExprNode, data map[string]interface{}) (interface{}, bool, error) {
	if node == nil {
		return nil, true, nil
	}

	switch node.Type {
	case TypeNumber:
		val, err := strconv.ParseFloat(node.Value, 64)
		return val, false, err

	case TypeString:
		// 去掉引号
		value := node.Value
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') {
			value = value[1 : len(value)-1]
		}
		// 检查是否是NULL字符串
		if strings.ToUpper(value) == "NULL" {
			return nil, true, nil
		}
		return value, false, nil

	case TypeField:
		// 支持嵌套字段访问
		if fieldpath.IsNestedField(node.Value) {
			if val, found := fieldpath.GetNestedField(data, node.Value); found {
				return val, val == nil, nil
			}
		} else {
			// 原有的简单字段访问
			if val, found := data[node.Value]; found {
				return val, val == nil, nil
			}
		}
		return nil, true, nil // 字段不存在视为NULL

	default:
		// 对于其他类型，回退到数值计算
		result, isNull, err := evaluateNodeWithNull(node, data)
		return result, isNull, err
	}
}

// evaluateBooleanConditionWithNull 计算布尔条件表达式，支持NULL值
func evaluateBooleanConditionWithNull(node *ExprNode, data map[string]interface{}) (bool, error) {
	if node == nil {
		return false, fmt.Errorf("null condition expression")
	}

	// 处理逻辑运算符（实现短路求值）
	if node.Type == TypeOperator && (node.Value == "AND" || node.Value == "OR") {
		leftBool, err := evaluateBooleanConditionWithNull(node.Left, data)
		if err != nil {
			return false, err
		}

		// 短路求值：对于AND，如果左边为false，立即返回false
		if node.Value == "AND" && !leftBool {
			return false, nil
		}

		// 短路求值：对于OR，如果左边为true，立即返回true
		if node.Value == "OR" && leftBool {
			return true, nil
		}

		// 只有在需要时才评估右边的表达式
		rightBool, err := evaluateBooleanConditionWithNull(node.Right, data)
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

	// 处理IS NULL和IS NOT NULL特殊情况
	if node.Type == TypeOperator && node.Value == "IS" {
		return evaluateIsConditionWithNull(node, data)
	}

	// 处理比较运算符
	if node.Type == TypeOperator {
		leftValue, leftNull, err := evaluateNodeValueWithNull(node.Left, data)
		if err != nil {
			return false, err
		}

		rightValue, rightNull, err := evaluateNodeValueWithNull(node.Right, data)
		if err != nil {
			return false, err
		}

		return compareValuesWithNull(leftValue, leftNull, rightValue, rightNull, node.Value)
	}

	// 对于其他表达式，计算其数值并转换为布尔值
	result, isNull, err := evaluateNodeWithNull(node, data)
	if err != nil {
		return false, err
	}

	// NULL值在布尔上下文中为false，非零值为真，零值为假
	return !isNull && result != 0, nil
}

// evaluateIsConditionWithNull 处理IS NULL和IS NOT NULL条件，支持NULL值
func evaluateIsConditionWithNull(node *ExprNode, data map[string]interface{}) (bool, error) {
	if node == nil || node.Left == nil || node.Right == nil {
		return false, fmt.Errorf("invalid IS condition")
	}

	// 获取左侧值
	leftValue, leftNull, err := evaluateNodeValueWithNull(node.Left, data)
	if err != nil {
		// 如果字段不存在，认为是null
		leftValue = nil
		leftNull = true
	}

	// 检查右侧是否是NULL或NOT NULL
	if node.Right.Type == TypeField && strings.ToUpper(node.Right.Value) == "NULL" {
		// IS NULL
		return leftNull || leftValue == nil, nil
	}

	// 检查是否是IS NOT NULL
	if node.Right.Type == TypeOperator && node.Right.Value == "NOT" &&
		node.Right.Right != nil && node.Right.Right.Type == TypeField &&
		strings.ToUpper(node.Right.Right.Value) == "NULL" {
		// IS NOT NULL
		return !leftNull && leftValue != nil, nil
	}

	// 其他IS比较
	rightValue, rightNull, err := evaluateNodeValueWithNull(node.Right, data)
	if err != nil {
		return false, err
	}

	return compareValuesWithNullForEquality(leftValue, leftNull, rightValue, rightNull)
}

// compareValuesForEquality 比较两个值是否相等
func compareValuesForEquality(left, right interface{}) (bool, error) {
	// 尝试字符串比较
	leftStr, leftIsStr := left.(string)
	rightStr, rightIsStr := right.(string)

	if leftIsStr && rightIsStr {
		return leftStr == rightStr, nil
	}

	// 尝试数值比较
	leftFloat, leftErr := convertToFloat(left)
	rightFloat, rightErr := convertToFloat(right)

	if leftErr == nil && rightErr == nil {
		return leftFloat == rightFloat, nil
	}

	// 如果都不能转换，直接比较
	return left == right, nil
}

// compareValuesWithNull 比较两个值（支持NULL）
func compareValuesWithNull(left interface{}, leftNull bool, right interface{}, rightNull bool, operator string) (bool, error) {
	// NULL值的比较有特殊规则
	switch operator {
	case "==", "=":
		if leftNull && rightNull {
			return true, nil // NULL = NULL 为 true
		}
		if leftNull || rightNull {
			return false, nil // NULL = value 为 false
		}

	case "!=", "<>":
		if leftNull && rightNull {
			return false, nil // NULL != NULL 为 false
		}
		if leftNull || rightNull {
			return false, nil // NULL != value 为 false
		}

	case ">", "<", ">=", "<=":
		if leftNull || rightNull {
			return false, nil // NULL与任何值的比较都为false
		}
	}

	// 对于非NULL值，执行正确的比较逻辑
	switch operator {
	case "==", "=":
		return compareValuesForEquality(left, right)
	case "!=", "<>":
		equal, err := compareValuesForEquality(left, right)
		return !equal, err
	case ">", "<", ">=", "<=":
		// 进行数值比较
		leftFloat, leftErr := convertToFloat(left)
		rightFloat, rightErr := convertToFloat(right)

		if leftErr != nil || rightErr != nil {
			// 如果不能转换为数值，尝试字符串比较
			leftStr := fmt.Sprintf("%v", left)
			rightStr := fmt.Sprintf("%v", right)

			switch operator {
			case ">":
				return leftStr > rightStr, nil
			case "<":
				return leftStr < rightStr, nil
			case ">=":
				return leftStr >= rightStr, nil
			case "<=":
				return leftStr <= rightStr, nil
			}
		}

		// 数值比较
		switch operator {
		case ">":
			return leftFloat > rightFloat, nil
		case "<":
			return leftFloat < rightFloat, nil
		case ">=":
			return leftFloat >= rightFloat, nil
		case "<=":
			return leftFloat <= rightFloat, nil
		}
	}

	return false, fmt.Errorf("unsupported operator: %s", operator)
}

// compareValuesWithNullForEquality 比较两个值是否相等（支持NULL）
func compareValuesWithNullForEquality(left interface{}, leftNull bool, right interface{}, rightNull bool) (bool, error) {
	if leftNull && rightNull {
		return true, nil // NULL = NULL 为 true
	}
	if leftNull || rightNull {
		return false, nil // NULL = value 为 false
	}
	return compareValuesForEquality(left, right)
}

// EvaluateWithNull 提供公开接口，用于聚合函数调用
func (e *Expression) EvaluateWithNull(data map[string]interface{}) (float64, bool, error) {
	if e.useExprLang {
		// expr-lang不支持NULL，回退到原有逻辑
		result, err := e.evaluateWithExprLang(data)
		return result, false, err
	}
	return evaluateNodeWithNull(e.Root, data)
}
