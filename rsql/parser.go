package rsql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rulego/streamsql/types"
)

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
	tok := p.lexer.NextToken()
	if tok.Type != expected {
		err := CreateUnexpectedTokenError(
			tok.Value,
			[]string{p.getTokenTypeName(expected)},
			tok.Pos,
		)
		err.Context = context
		p.errorRecovery.AddError(err)

		// 尝试错误恢复
		if err.IsRecoverable() && p.errorRecovery.RecoverFromError(ErrorTypeUnexpectedToken) {
			return p.expectToken(expected, context)
		}

		return tok, err
	}
	return tok, nil
}

// getTokenTypeName 获取token类型名称
func (p *Parser) getTokenTypeName(tokenType TokenType) string {
	switch tokenType {
	case TokenSELECT:
		return "SELECT"
	case TokenFROM:
		return "FROM"
	case TokenWHERE:
		return "WHERE"
	case TokenGROUP:
		return "GROUP"
	case TokenBY:
		return "BY"
	case TokenComma:
		return ","
	case TokenLParen:
		return "("
	case TokenRParen:
		return ")"
	case TokenIdent:
		return "identifier"
	case TokenQuotedIdent:
		return "quoted identifier"
	case TokenNumber:
		return "number"
	case TokenString:
		return "string"
	default:
		return "unknown"
	}
}

func (p *Parser) Parse() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	// 解析SELECT子句
	if err := p.parseSelect(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
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
func (p *Parser) createDetailedError(err error) error {
	if parseErr, ok := err.(*ParseError); ok {
		parseErr.Context = FormatErrorContext(p.input, parseErr.Position, 20)
		return parseErr
	}
	return err
}

// createCombinedError 创建组合错误信息
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
	return fmt.Errorf(builder.String())
}

func (p *Parser) parseSelect(stmt *SelectStatement) error {
	// Validate if first token is SELECT
	firstToken := p.lexer.NextToken()
	if firstToken.Type != TokenSELECT {
		// If not SELECT, check for typos
		if firstToken.Type == TokenIdent {
			// The error here has been handled by lexer's checkForTypos
			// Continue parsing, assuming user meant SELECT
		} else {
			return CreateSyntaxError(
				fmt.Sprintf("Expected SELECT, got %s", firstToken.Value),
				firstToken.Pos,
				firstToken.Value,
				[]string{"SELECT"},
			)
		}
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
	maxFields := 100
	fieldCount := 0

	for {
		fieldCount++
		// Safety check: prevent infinite loops
		if fieldCount > maxFields {
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
	p.lexer.NextToken() // 跳过(
	var params []interface{}

	// 设置最大次数限制，防止无限循环
	maxIterations := 100
	iterations := 0

	for p.lexer.peekChar() != ')' {
		iterations++
		// 安全检查：防止无限循环
		if iterations > maxIterations {
			return errors.New("window function parameter parsing exceeded maximum iterations, possible syntax error")
		}

		valTok := p.lexer.NextToken()
		if valTok.Type == TokenRParen || valTok.Type == TokenEOF {
			break
		}
		if valTok.Type == TokenComma {
			continue
		}
		//valTok := p.lexer.NextToken()
		// Handle quoted values
		if strings.HasPrefix(valTok.Value, "'") && strings.HasSuffix(valTok.Value, "'") {
			valTok.Value = strings.Trim(valTok.Value, "'")
		}
		params = append(params, convertValue(valTok.Value))
	}

	if &stmt.Window != nil {
		stmt.Window.Params = params
		stmt.Window.Type = winType
	} else {
		stmt.Window = WindowDefinition{
			Type:   winType,
			Params: params,
		}
	}
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
		_ = p.parseWindowFunction(stmt, tok.Value)
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
			_ = p.parseWindowFunction(stmt, tok.Value)
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
			timeUnit := time.Minute
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
				default:

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
