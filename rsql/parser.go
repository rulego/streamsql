package rsql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
)

// 解析器配置常量
const (
	// MaxRecursionDepth 定义 expectTokenWithDepth 方法的最大递归深度
	// 用于防止无限递归
	MaxRecursionDepth = 30

	// MaxSelectFields 定义 SELECT 子句中允许的最大字段数量
	MaxSelectFields = 300
)

// tokenTypeNames 定义 token 类型到名称的映射表
var tokenTypeNames = map[TokenType]string{
	TokenSELECT:      "SELECT",
	TokenFROM:        "FROM",
	TokenWHERE:       "WHERE",
	TokenGROUP:       "GROUP",
	TokenBY:          "BY",
	TokenComma:       ",",
	TokenLParen:      "(",
	TokenRParen:      ")",
	TokenIdent:       "identifier",
	TokenQuotedIdent: "quoted identifier",
	TokenNumber:      "number",
	TokenString:      "string",
	TokenAND:         "AND",
	TokenOR:          "OR",
	TokenNOT:         "NOT",
	TokenAS:          "AS",
	TokenDISTINCT:    "DISTINCT",
	TokenLIMIT:       "LIMIT",
	TokenHAVING:      "HAVING",
	TokenWITH:        "WITH",
	TokenEOF:         "EOF",
}

type Parser struct {
	lexer         *Lexer
	errorRecovery *ErrorRecovery
	currentToken  Token
	input         string
}

func NewParser(input string) *Parser {
	lexer := NewLexer(input)
	p := &Parser{
		lexer: lexer,
		input: input,
	}
	p.errorRecovery = NewErrorRecovery(p)
	lexer.SetErrorRecovery(p.errorRecovery)
	return p
}

// GetErrors 获取解析过程中的所有错误
func (p *Parser) GetErrors() []*ParseError {
	return p.errorRecovery.GetErrors()
}

// HasErrors 检查是否有错误
func (p *Parser) HasErrors() bool {
	return p.errorRecovery.HasErrors()
}

// expectToken 期望特定类型的token
func (p *Parser) expectToken(expected TokenType, context string) (Token, error) {
	return p.expectTokenWithDepth(expected, context, 0)
}

// expectTokenWithDepth 期望特定类型的token，带递归深度限制
// 使用可配置的最大递归深度防止无限递归，提供更好的错误处理和恢复机制
func (p *Parser) expectTokenWithDepth(expected TokenType, context string, depth int) (Token, error) {
	// 防止无限递归，使用可配置的最大递归深度
	if depth > MaxRecursionDepth {
		tok := p.lexer.NextToken()
		err := p.createTokenError(tok, expected, context, "maximum recursion depth exceeded")
		return tok, err
	}

	tok := p.lexer.NextToken()
	if tok.Type != expected {
		err := p.createTokenError(tok, expected, context, "")
		p.errorRecovery.AddError(err)

		// 尝试错误恢复，但限制递归深度
		if p.shouldAttemptRecovery(err, depth) {
			return p.expectTokenWithDepth(expected, context, depth+1)
		}

		return tok, err
	}
	return tok, nil
}

// getTokenTypeName 获取token类型名称
// 使用映射表提高性能和可维护性
func (p *Parser) getTokenTypeName(tokenType TokenType) string {
	if name, exists := tokenTypeNames[tokenType]; exists {
		return name
	}
	return "unknown"
}

// createTokenError 创建标准化的 token 错误
// 提供统一的错误创建逻辑，便于维护和扩展
func (p *Parser) createTokenError(tok Token, expected TokenType, context, additionalInfo string) *ParseError {
	err := CreateUnexpectedTokenError(
		tok.Value,
		[]string{p.getTokenTypeName(expected)},
		tok.Pos,
	)
	err.Context = context
	if additionalInfo != "" {
		err.Message = fmt.Sprintf("%s (%s)", err.Message, additionalInfo)
	}
	return err
}

// shouldAttemptRecovery 判断是否应该尝试错误恢复
// 基于错误类型和递归深度做出智能决策
func (p *Parser) shouldAttemptRecovery(err *ParseError, depth int) bool {
	// 如果已经接近最大递归深度，不再尝试恢复
	if depth >= MaxRecursionDepth-1 {
		return false
	}

	// 检查错误是否可恢复，并且错误恢复机制允许恢复
	return err.IsRecoverable() && p.errorRecovery.RecoverFromError(ErrorTypeUnexpectedToken)
}

func (p *Parser) Parse() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	// 解析SELECT子句 - 对于特定的关键错误直接返回
	if err := p.parseSelect(stmt); err != nil {
		// 检查是否是关键的语法错误，这些错误应该停止进一步解析
		if strings.Contains(err.Error(), "Expected SELECT") {
			// SELECT关键字错误是致命的，直接返回
			return nil, p.createDetailedError(err)
		}

		// 检查是否是特定的关键错误模式，这些错误不应该被恢复
		// 只有当查询看起来像 "SELECT FROM table WHERE" 这样的模式时才直接返回错误
		if strings.Contains(err.Error(), "no fields specified") {
			// 检查是否有FROM关键字紧跟在SELECT后面
			nextTok := p.lexer.lookupIdent(p.lexer.readPreviousIdentifier())
			if nextTok.Type == TokenFROM {
				// 进一步检查：如果后面还有其他内容（如WHERE、GROUP等），则允许错误恢复
				// 只有当查询是简单的 "SELECT FROM table WHERE" 模式时才直接返回错误
				if !strings.Contains(p.input, "WHERE") || !strings.Contains(p.input, "GROUP") {
					return nil, p.createDetailedError(err)
				}
			}
		}

		if parseErr, ok := err.(*ParseError); ok {
			p.errorRecovery.AddError(parseErr)
		}
		// 对于其他错误，继续尝试解析其他部分
	}

	// 解析FROM子句
	if err := p.parseFrom(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// 解析WHERE子句
	if err := p.parseWhere(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// 解析GROUP BY子句
	if err := p.parseGroupBy(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// 解析 HAVING 子句
	if err := p.parseHaving(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	if err := p.parseWith(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// 解析LIMIT子句
	if err := p.parseLimit(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// 如果有错误但可以恢复，返回部分解析结果和错误信息
	if p.errorRecovery.HasErrors() {
		return stmt, p.createCombinedError()
	}

	return stmt, nil
}

// isKeyword 检查给定的字符串是否是SQL关键字
// 使用预定义的关键字映射表进行快速查找
// 参数: word - 要检查的字符串
// 返回: 如果是关键字返回 true，否则返回 false
func isKeyword(word string) bool {
	keywords := map[string]bool{
		"SELECT": true, "FROM": true, "WHERE": true, "GROUP": true, "BY": true,
		"ORDER": true, "HAVING": true, "LIMIT": true, "WITH": true, "AS": true,
		"CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
		"AND": true, "OR": true, "NOT": true, "IN": true, "IS": true, "NULL": true,
		"DISTINCT": true, "COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
		"INNER": true, "LEFT": true, "RIGHT": true, "FULL": true, "OUTER": true, "JOIN": true,
		"ON": true, "UNION": true, "ALL": true, "EXCEPT": true, "INTERSECT": true,
		"EXISTS": true, "BETWEEN": true, "LIKE": true, "ASC": true, "DESC": true,
	}
	return keywords[word]
}

// createDetailedError 创建详细的错误信息
// 为 ParseError 类型的错误添加上下文信息，便于调试和错误定位
// 参数: err - 原始错误
// 返回: 包含详细上下文信息的错误
func (p *Parser) createDetailedError(err error) error {
	if parseErr, ok := err.(*ParseError); ok {
		parseErr.Context = FormatErrorContext(p.input, parseErr.Position, 20)
		return parseErr
	}
	return err
}

// createCombinedError 创建组合错误信息
// 将多个解析错误合并为一个统一的错误消息，便于用户理解所有问题
// 返回: 包含所有错误信息的组合错误
func (p *Parser) createCombinedError() error {
	errors := p.errorRecovery.GetErrors()
	if len(errors) == 1 {
		return p.createDetailedError(errors[0])
	}

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Found %d parsing errors:\n", len(errors)))
	for i, err := range errors {
		builder.WriteString(fmt.Sprintf("%d. %s\n", i+1, err.Error()))
	}
	return fmt.Errorf("%s", builder.String())
}

// parseSelect 解析 SELECT 子句，包括字段列表、DISTINCT 关键字和别名
// 支持 SELECT * 语法，并提供字段数量限制防止无限循环
// 参数: stmt - 要填充的 SelectStatement 结构体
// 返回: 解析过程中遇到的错误，如果成功则返回 nil
func (p *Parser) parseSelect(stmt *SelectStatement) error {
	// Validate if first token is SELECT
	firstToken := p.lexer.NextToken()
	if firstToken.Type != TokenSELECT {
		// 直接返回语法错误
		return CreateSyntaxError(
			fmt.Sprintf("Expected SELECT, got %s", firstToken.Value),
			firstToken.Pos,
			firstToken.Value,
			[]string{"SELECT"},
		)
	}
	currentToken := p.lexer.NextToken()

	if currentToken.Type == TokenDISTINCT {
		stmt.Distinct = true
		currentToken = p.lexer.NextToken() // 消费 DISTINCT，移动到下一个 token
	}

	// 检查是否是SELECT *查询
	if currentToken.Type == TokenIdent && currentToken.Value == "*" {
		stmt.SelectAll = true
		// 添加一个特殊的字段标记SELECT *
		stmt.Fields = append(stmt.Fields, Field{Expression: "*"})

		// 消费*token并检查下一个token
		currentToken = p.lexer.NextToken()

		// 如果下一个token是FROM或EOF，则完成SELECT *解析
		if currentToken.Type == TokenFROM || currentToken.Type == TokenEOF {
			return nil
		}

		// 如果不是FROM/EOF，继续正常的字段解析流程
	}

	// 设置最大字段数量限制，防止无限循环
	fieldCount := 0

	for {
		fieldCount++
		// Safety check: prevent infinite loops
		if fieldCount > MaxSelectFields {
			return errors.New("select field list parsing exceeded maximum fields, possible syntax error")
		}

		var expr strings.Builder
		parenthesesLevel := 0 // 跟踪括号嵌套层级

		// 设置最大表达式长度，防止无限循环
		maxExprParts := 100
		exprPartCount := 0

		for {
			exprPartCount++
			// 安全检查：防止无限循环
			if exprPartCount > maxExprParts {
				return errors.New("select field expression parsing exceeded maximum length, possible syntax error")
			}

			// 跟踪括号层级
			if currentToken.Type == TokenLParen {
				parenthesesLevel++
			} else if currentToken.Type == TokenRParen {
				parenthesesLevel--
			}

			// 只有在括号层级为0时，逗号才被视为字段分隔符
			if parenthesesLevel == 0 && (currentToken.Type == TokenFROM || currentToken.Type == TokenComma || currentToken.Type == TokenAS || currentToken.Type == TokenEOF) {
				break
			}

			// 如果不是第一个token，添加空格分隔符
			// 但要注意特殊情况：某些token之间不应该加空格
			if expr.Len() > 0 {
				shouldAddSpace := true

				// 获取前一个token的信息
				exprStr := expr.String()
				lastChar := exprStr[len(exprStr)-1:]

				// 以下情况不添加空格：
				// 1. 函数名和左括号之间
				// 2. 标识符和数字之间（如 x1, y1）
				// 3. 数字和标识符之间
				// 4. 左括号之后
				// 5. 右括号之前
				if currentToken.Type == TokenLParen && lastChar != " " && lastChar != "(" {
					// 函数名和左括号之间不加空格
					shouldAddSpace = false
				} else if lastChar == "(" || currentToken.Type == TokenRParen {
					// 左括号之后或右括号之前不加空格
					shouldAddSpace = false
				} else if len(exprStr) > 0 && currentToken.Type == TokenNumber {
					// 检查前一个字符是否是字母（标识符的一部分），且前面没有空格
					// 这主要处理 x1, y1 这类标识符，但排除 THEN 1, ELSE 0 这类情况
					if ((lastChar[0] >= 'a' && lastChar[0] <= 'z') || (lastChar[0] >= 'A' && lastChar[0] <= 'Z') || lastChar[0] == '_') &&
						!strings.HasSuffix(exprStr, " ") {
						// 进一步检查：如果前面是SQL关键字，则应该加空格
						words := strings.Fields(exprStr)
						if len(words) > 0 {
							lastWord := strings.ToUpper(words[len(words)-1])
							// 如果是关键字，应该加空格
							if isKeyword(lastWord) {
								shouldAddSpace = true
							} else {
								shouldAddSpace = false
							}
						} else {
							shouldAddSpace = false
						}
					}
				} else if len(exprStr) > 0 && (currentToken.Type == TokenIdent || currentToken.Type == TokenQuotedIdent) {
					// 检查前一个字符是否是数字，且前面没有空格
					if (lastChar[0] >= '0' && lastChar[0] <= '9') && !strings.HasSuffix(exprStr, " ") {
						shouldAddSpace = false
					}
				}

				if shouldAddSpace {
					expr.WriteString(" ")
				}
			}
			expr.WriteString(currentToken.Value)
			currentToken = p.lexer.NextToken()
		}

		field := Field{Expression: strings.TrimSpace(expr.String())}

		// 处理别名
		if currentToken.Type == TokenAS {
			field.Alias = p.lexer.NextToken().Value
			currentToken = p.lexer.NextToken()
		}

		// 如果表达式为空，跳过这个字段
		if field.Expression != "" {
			// 验证表达式中的函数
			validator := NewFunctionValidator(p.errorRecovery)
			pos, _, _ := p.lexer.GetPosition()
			validator.ValidateExpression(field.Expression, pos-len(field.Expression))

			stmt.Fields = append(stmt.Fields, field)
		}

		if currentToken.Type == TokenFROM || currentToken.Type == TokenEOF {
			break
		}

		if currentToken.Type != TokenComma {
			// 如果不是逗号，那么应该是语法错误
			return fmt.Errorf("unexpected token %v, expected comma or FROM", currentToken.Value)
		}

		currentToken = p.lexer.NextToken()
	}

	// 确保至少有一个字段
	if len(stmt.Fields) == 0 {
		return errors.New("no fields specified in SELECT clause")
	}

	return nil
}

func (p *Parser) parseWhere(stmt *SelectStatement) error {
	var conditions []string
	current := p.lexer.NextToken() // 获取下一个token
	if current.Type != TokenWHERE {
		// 如果不是WHERE，回退token位置
		return nil
	}

	// Set max iterations limit to prevent infinite loops
	maxIterations := 100
	iterations := 0

	for {
		iterations++
		// 安全检查：防止无限循环
		if iterations > maxIterations {
			return errors.New("WHERE clause parsing exceeded maximum iterations, possible syntax error")
		}

		tok := p.lexer.NextToken()
		if tok.Type == TokenGROUP || tok.Type == TokenEOF || tok.Type == TokenSliding ||
			tok.Type == TokenTumbling || tok.Type == TokenCounting || tok.Type == TokenSession ||
			tok.Type == TokenHAVING || tok.Type == TokenLIMIT {
			break
		}
		switch tok.Type {
		case TokenIdent, TokenNumber, TokenQuotedIdent:
			conditions = append(conditions, tok.Value)
		case TokenString:
			conditions = append(conditions, tok.Value)
		case TokenEQ:
			if tok.Value == "=" {
				conditions = append(conditions, "==")
			} else {
				conditions = append(conditions, tok.Value)
			}
		case TokenAND:
			conditions = append(conditions, "&&")
		case TokenOR:
			conditions = append(conditions, "||")
		case TokenLIKE:
			conditions = append(conditions, "LIKE")
		case TokenIS:
			conditions = append(conditions, "IS")
		case TokenNULL:
			conditions = append(conditions, "NULL")
		case TokenNOT:
			conditions = append(conditions, "NOT")
		default:
			// Handle string value quotes
			if len(conditions) > 0 && conditions[len(conditions)-1] == "'" {
				conditions[len(conditions)-1] = conditions[len(conditions)-1] + tok.Value
			} else {
				conditions = append(conditions, tok.Value)
			}
		}
	}

	// Validate functions in WHERE condition
	whereCondition := strings.Join(conditions, " ")
	if whereCondition != "" {
		validator := NewFunctionValidator(p.errorRecovery)
		pos, _, _ := p.lexer.GetPosition()
		validator.ValidateExpression(whereCondition, pos-len(whereCondition))
	}

	stmt.Condition = whereCondition
	return nil
}

func (p *Parser) parseWindowFunction(stmt *SelectStatement, winType string) error {
	nextTok := p.lexer.NextToken() // 读取下一个 token，应该是 '('
	if nextTok.Type != TokenLParen {
		return fmt.Errorf("expected '(' after window function %s, got %s (type: %v)", winType, nextTok.Value, nextTok.Type)
	}

	var params []interface{}
	maxIterations := 100
	iterations := 0

	// Parse parameters until we find the closing parenthesis
	for {
		iterations++
		if iterations > maxIterations {
			return fmt.Errorf("window function parameter parsing exceeded maximum iterations")
		}

		// Read the next token first
		valTok := p.lexer.NextToken()

		// If we hit the closing parenthesis or EOF, break
		if valTok.Type == TokenRParen || valTok.Type == TokenEOF {
			break
		}

		// Skip commas
		if valTok.Type == TokenComma {
			continue
		}

		// Handle quoted values
		if strings.HasPrefix(valTok.Value, "'") && strings.HasSuffix(valTok.Value, "'") {
			valTok.Value = strings.Trim(valTok.Value, "'")
		}

		// Add the parameter value
		params = append(params, convertValue(valTok.Value))
	}

	stmt.Window.Params = params
	stmt.Window.Type = winType
	return nil
}

func convertValue(s string) interface{} {
	if s == "true" {
		return true
	}
	if s == "false" {
		return false
	}
	if i, err := strconv.Atoi(s); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	// 处理引号包裹的字符串
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return strings.Trim(s, "'")
	}
	return s
}

func (p *Parser) parseFrom(stmt *SelectStatement) error {
	tok := p.lexer.NextToken()
	if tok.Type != TokenIdent {
		err := CreateUnexpectedTokenError(
			tok.Value,
			[]string{"table_name", "stream_name"},
			tok.Pos,
		)
		err.Message = "Expected source identifier after FROM"
		err.Context = "FROM clause requires a table or stream name"
		err.Suggestions = []string{
			"Ensure FROM is followed by a valid table or stream name",
			"Check if the table name is spelled correctly",
		}
		p.errorRecovery.AddError(err)
		return err
	}
	stmt.Source = tok.Value
	return nil
}

func (p *Parser) parseGroupBy(stmt *SelectStatement) error {
	tok := p.lexer.lookupIdent(p.lexer.readPreviousIdentifier())
	hasWindowFunction := false
	if tok.Type == TokenTumbling || tok.Type == TokenSliding || tok.Type == TokenCounting || tok.Type == TokenSession {
		hasWindowFunction = true
		if err := p.parseWindowFunction(stmt, tok.Value); err != nil {
			return err
		}
	}

	hasGroupBy := false
	if tok.Type == TokenGROUP {
		hasGroupBy = true
		p.lexer.NextToken() // 跳过BY
	}

	// 如果没有GROUP BY子句且没有窗口函数，直接返回
	if !hasGroupBy && !hasWindowFunction {
		return nil
	}

	// 设置最大次数限制，防止无限循环
	maxIterations := 100
	iterations := 0

	var limitToken *Token // 保存LIMIT token以便后续处理

	for {
		iterations++
		// 安全检查：防止无限循环
		if iterations > maxIterations {
			return errors.New("group by clause parsing exceeded maximum iterations, possible syntax error")
		}

		tok := p.lexer.NextToken()
		if tok.Type == TokenWITH || tok.Type == TokenOrder || tok.Type == TokenEOF ||
			tok.Type == TokenHAVING || tok.Type == TokenLIMIT {
			// 如果是LIMIT token，保存它以便parseLimit处理
			if tok.Type == TokenLIMIT {
				limitToken = &tok
			}
			break
		}
		if tok.Type == TokenComma {
			continue
		}
		if tok.Type == TokenTumbling || tok.Type == TokenSliding || tok.Type == TokenCounting || tok.Type == TokenSession {
			if err := p.parseWindowFunction(stmt, tok.Value); err != nil {
				return err
			}
			// After parsing window function, skip adding it to GroupBy and continue
			continue
		}

		// Skip right parenthesis tokens (they should be consumed by parseWindowFunction)
		if tok.Type == TokenRParen {
			continue
		}

		// 只有在有GROUP BY时才添加到GroupBy中
		if hasGroupBy {
			stmt.GroupBy = append(stmt.GroupBy, tok.Value)
		}
	}

	// 如果遇到了LIMIT token，直接在这里处理
	if limitToken != nil {
		return p.handleLimitToken(stmt, *limitToken)
	}
	return nil
}

func (p *Parser) parseWith(stmt *SelectStatement) error {
	// 查看当前 token，如果不是 WITH，则返回
	tok := p.lexer.lookupIdent(p.lexer.readPreviousIdentifier())
	if tok.Type != TokenWITH {
		return nil // 没有 WITH 子句，不是错误
	}

	p.lexer.NextToken() // 跳过(

	// 设置最大次数限制，防止无限循环
	maxIterations := 100
	iterations := 0

	for p.lexer.peekChar() != ')' {
		iterations++
		// 安全检查：防止无限循环
		if iterations > maxIterations {
			return errors.New("WITH clause parsing exceeded maximum iterations, possible syntax error")
		}

		valTok := p.lexer.NextToken()
		if valTok.Type == TokenRParen || valTok.Type == TokenEOF {
			break
		}
		if valTok.Type == TokenComma {
			continue
		}

		if valTok.Type == TokenTimestamp {
			next := p.lexer.NextToken()
			if next.Type == TokenEQ {
				next = p.lexer.NextToken()
				if strings.HasPrefix(next.Value, "'") && strings.HasSuffix(next.Value, "'") {
					next.Value = strings.Trim(next.Value, "'")
				}
				// Check if Window is initialized; if not, create new WindowDefinition
				if stmt.Window.Type == "" {
					stmt.Window = WindowDefinition{
						TsProp: next.Value,
					}
				} else {
					stmt.Window.TsProp = next.Value
				}
			}
		}
		if valTok.Type == TokenTimeUnit {
			timeUnit := time.Millisecond // Default to milliseconds
			next := p.lexer.NextToken()
			if next.Type == TokenEQ {
				next = p.lexer.NextToken()
				if strings.HasPrefix(next.Value, "'") && strings.HasSuffix(next.Value, "'") {
					next.Value = strings.Trim(next.Value, "'")
				}
				switch next.Value {
				case "dd":
					timeUnit = 24 * time.Hour
				case "hh":
					timeUnit = time.Hour
				case "mi":
					timeUnit = time.Minute
				case "ss":
					timeUnit = time.Second
				case "ms":
					timeUnit = time.Millisecond
				case "ns":
					timeUnit = time.Nanosecond
				default:
					// If unknown unit, keep default (milliseconds)
				}
				// Check if Window is initialized; if not, create new WindowDefinition
				if stmt.Window.Type == "" {
					stmt.Window = WindowDefinition{
						TimeUnit: timeUnit,
					}
				} else {
					stmt.Window.TimeUnit = timeUnit
				}
			}
		}
		if valTok.Type == TokenMaxOutOfOrderness {
			next := p.lexer.NextToken()
			if next.Type == TokenEQ {
				next = p.lexer.NextToken()
				durationStr := next.Value
				if strings.HasPrefix(durationStr, "'") && strings.HasSuffix(durationStr, "'") {
					durationStr = strings.Trim(durationStr, "'")
				}
				// Parse duration string like '5s', '2m', '1h', etc.
				if duration, err := cast.ToDurationE(durationStr); err == nil {
					// Check if Window is initialized; if not, create new WindowDefinition
					if stmt.Window.Type == "" {
						stmt.Window = WindowDefinition{
							MaxOutOfOrderness: duration,
						}
					} else {
						stmt.Window.MaxOutOfOrderness = duration
					}
				}
				// If parsing fails, silently ignore (keep default 0)
			}
		}
		if valTok.Type == TokenAllowedLateness {
			next := p.lexer.NextToken()
			if next.Type == TokenEQ {
				next = p.lexer.NextToken()
				durationStr := next.Value
				if strings.HasPrefix(durationStr, "'") && strings.HasSuffix(durationStr, "'") {
					durationStr = strings.Trim(durationStr, "'")
				}
				// Parse duration string like '5s', '2m', '1h', etc.
				if duration, err := cast.ToDurationE(durationStr); err == nil {
					// Check if Window is initialized; if not, create new WindowDefinition
					if stmt.Window.Type == "" {
						stmt.Window = WindowDefinition{
							AllowedLateness: duration,
						}
					} else {
						stmt.Window.AllowedLateness = duration
					}
				}
				// If parsing fails, silently ignore (keep default 0)
			}
		}
		if valTok.Type == TokenIdleTimeout {
			next := p.lexer.NextToken()
			if next.Type == TokenEQ {
				next = p.lexer.NextToken()
				durationStr := next.Value
				if strings.HasPrefix(durationStr, "'") && strings.HasSuffix(durationStr, "'") {
					durationStr = strings.Trim(durationStr, "'")
				}
				// Parse duration string like '5s', '2m', '1h', etc.
				if duration, err := cast.ToDurationE(durationStr); err == nil {
					// Check if Window is initialized; if not, create new WindowDefinition
					if stmt.Window.Type == "" {
						stmt.Window = WindowDefinition{
							IdleTimeout: duration,
						}
					} else {
						stmt.Window.IdleTimeout = duration
					}
				}
				// If parsing fails, silently ignore (keep default 0)
			}
		}
	}

	return nil
}

// handleLimitToken 处理在parseGroupBy中遇到的LIMIT token
func (p *Parser) handleLimitToken(stmt *SelectStatement, limitToken Token) error {
	// 获取下一个token，应该是一个数字
	tok := p.lexer.NextToken()
	if tok.Type == TokenNumber {
		// 将数字字符串转换为整数
		limit, err := strconv.Atoi(tok.Value)
		if err != nil {
			parseErr := CreateSyntaxError(
				"LIMIT value must be a valid integer",
				tok.Pos,
				tok.Value,
				[]string{"positive_integer"},
			)
			parseErr.Context = "LIMIT clause"
			parseErr.Suggestions = []string{
				"Use a positive integer, e.g., LIMIT 10",
				"Ensure the number format is correct",
			}
			p.errorRecovery.AddError(parseErr)
			return parseErr
		}
		if limit < 0 {
			parseErr := CreateSyntaxError(
				"LIMIT value must be positive",
				tok.Pos,
				tok.Value,
				[]string{"positive_integer"},
			)
			parseErr.Suggestions = []string{"Use a positive integer, e.g., LIMIT 10"}
			p.errorRecovery.AddError(parseErr)
			return parseErr
		}
		stmt.Limit = limit
	} else if tok.Type == TokenMinus {
		// 处理负数情况："-5"
		nextTok := p.lexer.NextToken()
		if nextTok.Type == TokenNumber {
			parseErr := CreateSyntaxError(
				"LIMIT value must be positive",
				nextTok.Pos,
				"-"+nextTok.Value,
				[]string{"positive_integer"},
			)
			parseErr.Context = "LIMIT clause"
			parseErr.Suggestions = []string{"Use a positive integer, e.g., LIMIT 10"}
			p.errorRecovery.AddError(parseErr)
			return parseErr
		} else {
			parseErr := CreateMissingTokenError("number", tok.Pos)
			parseErr.Message = "LIMIT must be followed by an integer"
			parseErr.Context = "LIMIT clause"
			parseErr.Suggestions = []string{
				"Add a number after LIMIT, e.g., LIMIT 10",
				"Ensure LIMIT syntax is correct",
			}
			p.errorRecovery.AddError(parseErr)
			return parseErr
		}
	} else {
		// 处理非数字情况：如 "abc"
		parseErr := CreateMissingTokenError("number", tok.Pos)
		parseErr.Message = "LIMIT must be followed by an integer"
		parseErr.Context = "LIMIT clause"
		parseErr.Suggestions = []string{
			"Add a number after LIMIT, e.g., LIMIT 10",
			"Ensure LIMIT syntax is correct",
		}
		p.errorRecovery.AddError(parseErr)
		return parseErr
	}
	return nil
}

// parseLimit 解析LIMIT子句
func (p *Parser) parseLimit(stmt *SelectStatement) error {
	// 如果LIMIT已经被设置（可能在parseGroupBy中已处理），则跳过
	if stmt.Limit > 0 {
		return nil
	}

	// 直接解析输入字符串中的LIMIT子句
	input := strings.ToUpper(p.input)
	limitIndex := strings.LastIndex(input, "LIMIT")
	if limitIndex == -1 {
		return nil
	}

	// 找到LIMIT后面的内容
	afterLimit := strings.TrimSpace(p.input[limitIndex+5:]) // 跳过"LIMIT"
	if afterLimit == "" {
		parseErr := CreateMissingTokenError("number", limitIndex+5)
		parseErr.Message = "LIMIT must be followed by an integer"
		parseErr.Context = "LIMIT clause"
		parseErr.Suggestions = []string{
			"Add a number after LIMIT, e.g., LIMIT 10",
			"Ensure LIMIT syntax is correct",
		}
		p.errorRecovery.AddError(parseErr)
		return parseErr
	}

	// 分割出第一个单词（应该是数字）
	parts := strings.Fields(afterLimit)
	if len(parts) == 0 {
		parseErr := CreateMissingTokenError("number", limitIndex+5)
		parseErr.Message = "LIMIT must be followed by an integer"
		parseErr.Context = "LIMIT clause"
		parseErr.Suggestions = []string{
			"Add a number after LIMIT, e.g., LIMIT 10",
			"Ensure LIMIT syntax is correct",
		}
		p.errorRecovery.AddError(parseErr)
		return parseErr
	}

	limitValue := parts[0]

	// 处理负数情况
	if strings.HasPrefix(limitValue, "-") {
		parseErr := CreateMissingTokenError("number", limitIndex+6)
		parseErr.Message = "LIMIT must be followed by an integer"
		parseErr.Context = "LIMIT clause"
		parseErr.Suggestions = []string{"Use a positive integer, e.g., LIMIT 10"}
		p.errorRecovery.AddError(parseErr)
		return parseErr
	}

	// 尝试转换为整数
	limit, err := strconv.Atoi(limitValue)
	if err != nil {
		parseErr := CreateMissingTokenError("number", limitIndex+6)
		parseErr.Message = "LIMIT must be followed by an integer"
		parseErr.Context = "LIMIT clause"
		parseErr.Suggestions = []string{
			"Add a number after LIMIT, e.g., LIMIT 10",
			"Ensure LIMIT syntax is correct",
		}
		p.errorRecovery.AddError(parseErr)
		return parseErr
	}

	if limit < 0 {
		parseErr := CreateMissingTokenError("number", limitIndex+6)
		parseErr.Message = "LIMIT must be followed by an integer"
		parseErr.Context = "LIMIT clause"
		parseErr.Suggestions = []string{"Use a positive integer, e.g., LIMIT 10"}
		p.errorRecovery.AddError(parseErr)
		return parseErr
	}

	stmt.Limit = limit
	return nil
}

// parseHaving 解析HAVING子句
func (p *Parser) parseHaving(stmt *SelectStatement) error {
	// 查看当前token
	tok := p.lexer.lookupIdent(p.lexer.readPreviousIdentifier())
	if tok.Type != TokenHAVING {
		return nil // 没有 HAVING 子句，不是错误
	}

	// 设置最大次数限制，防止无限循环
	maxIterations := 100
	iterations := 0

	var conditions []string
	for {
		iterations++
		// 安全检查：防止无限循环
		if iterations > maxIterations {
			return errors.New("HAVING clause parsing exceeded maximum iterations, possible syntax error")
		}

		tok := p.lexer.NextToken()
		if tok.Type == TokenLIMIT || tok.Type == TokenEOF || tok.Type == TokenWITH {
			break
		}

		switch tok.Type {
		case TokenIdent, TokenNumber:
			conditions = append(conditions, tok.Value)
		case TokenString:
			conditions = append(conditions, tok.Value)
		case TokenEQ:
			if tok.Value == "=" {
				conditions = append(conditions, "==")
			} else {
				conditions = append(conditions, tok.Value)
			}
		case TokenAND:
			conditions = append(conditions, "&&")
		case TokenOR:
			conditions = append(conditions, "||")
		case TokenLIKE:
			conditions = append(conditions, "LIKE")
		case TokenIS:
			conditions = append(conditions, "IS")
		case TokenNULL:
			conditions = append(conditions, "NULL")
		case TokenNOT:
			conditions = append(conditions, "NOT")
		default:
			// Handle string value quotes
			if len(conditions) > 0 && conditions[len(conditions)-1] == "'" {
				conditions[len(conditions)-1] = conditions[len(conditions)-1] + tok.Value
			} else {
				conditions = append(conditions, tok.Value)
			}
		}
	}

	// Validate functions in HAVING condition
	havingCondition := strings.Join(conditions, " ")
	if havingCondition != "" {
		validator := NewFunctionValidator(p.errorRecovery)
		pos, _, _ := p.lexer.GetPosition()
		validator.ValidateExpression(havingCondition, pos-len(havingCondition))
	}

	stmt.Having = havingCondition
	return nil
}

// Parse 是包级别的Parse函数，用于解析SQL字符串并返回配置和条件
func Parse(sql string) (*types.Config, string, error) {
	parser := NewParser(sql)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, "", err
	}

	config, condition, err := stmt.ToStreamConfig()
	if err != nil {
		return nil, "", err
	}

	return config, condition, nil
}
