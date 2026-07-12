package rsql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rulego/streamsql/logger"
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
	TokenDot:         ".",
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

	// 解析JOIN子句（流-表 JOIN，v0.5）
	if err := p.parseJoin(stmt); err != nil {
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

	// 解析 ORDER BY 子句
	if err := p.parseOrderBy(stmt); err != nil {
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

	// 检查是否是SELECT *查询（词法器把 * 归为 TokenAsterisk，非 TokenIdent）
	if currentToken.Type == TokenAsterisk {
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
			if parenthesesLevel == 0 && (currentToken.Type == TokenFROM || currentToken.Type == TokenComma || currentToken.Type == TokenAS || currentToken.Type == TokenEOF || currentToken.Type == TokenOVER) {
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
				// 6. 数组索引相关：[ 前，[ 后，] 前
				// 7. 点号前后
				if (currentToken.Type == TokenLParen || currentToken.Type == TokenLBracket) && lastChar != " " && lastChar != "(" && lastChar != "[" {
					// 函数名/数组名和左括号/左中括号之间不加空格
					shouldAddSpace = false
				} else if lastChar == "(" || lastChar == "[" || currentToken.Type == TokenRParen || currentToken.Type == TokenRBracket {
					// 左括号/左中括号之后或右括号/右中括号之前不加空格
					shouldAddSpace = false
				} else if currentToken.Type == TokenDot || lastChar == "." {
					// 点号前后不加空格
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

		// 解析可选的 OVER 子句（分析函数。OVER 在断点条件中被识别，
		// 此处 currentToken == TokenOVER；parseOverClause 消费 OVER(...)，返回后 )
		// 已读出，再 NextToken 取后续 token（AS/FROM/Comma/EOF）。
		if currentToken.Type == TokenOVER {
			over, err := p.parseOverClause()
			if err != nil {
				return err
			}
			field.OverSpec = over
			currentToken = p.lexer.NextToken()
		}

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
			tok.Type == TokenGlobal ||
			tok.Type == TokenHAVING || tok.Type == TokenLIMIT || tok.Type == TokenWITH ||
			tok.Type == TokenOrder {
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

	// Validate functions in WHERE condition. 分析函数调用（含 OVER）先替换为占位符，
	// 避免 OVER 被误判为未知函数；stmt.Condition 保留原文，由 ToStreamConfig 提取。
	whereCondition := strings.Join(conditions, " ")
	if whereCondition != "" {
		validated, _, _ := extractWhereAnalyticCalls(whereCondition)
		validator := NewFunctionValidator(p.errorRecovery)
		pos, _, _ := p.lexer.GetPosition()
		validator.ValidateExpression(validated, pos-len(whereCondition))
	}

	stmt.Condition = whereCondition
	return nil
}

func (p *Parser) parseWindowFunction(stmt *SelectStatement, winType string) error {
	nextTok := p.lexer.NextToken() // 读取下一个 token，应该是 '('
	if nextTok.Type != TokenLParen {
		return fmt.Errorf("expected '(' after window function %s, got %s (type: %v)", winType, nextTok.Value, nextTok.Type)
	}

	var params []any
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

// parseGlobalWindow parses "GLOBAL WINDOW [TRIGGER WHEN <predicate>]".
// Unlike other windows, the global window takes no parentheses/params; its
// output is driven by the TRIGGER WHEN predicate. The predicate is collected
// as a raw string and evaluated at runtime against the group's running
// aggregate values.
//
// Convention (same as parseWindowFunction): the GLOBAL keyword has already been
// consumed by the caller (the parseGroupBy initial peek path consumes it via
// parseWhere's leading NextToken; the loop path consumes it via its own
// NextToken). This function starts by consuming WINDOW.
func (p *Parser) parseGlobalWindow(stmt *SelectStatement) error {
	// Expect WINDOW (GLOBAL already consumed by the caller).
	wTok := p.lexer.NextToken()
	if wTok.Type != TokenWindow {
		return fmt.Errorf("expected WINDOW after GLOBAL, got %q", wTok.Value)
	}
	stmt.Window.Type = "GLOBALWINDOW"

	// Optional TRIGGER WHEN <predicate>. Absence means NeverTrigger (validated
	// later in ToStreamConfig as a parse error, since it would never output).
	snap := p.lexer.save()
	next := p.lexer.NextToken()
	if next.Type != TokenTrigger {
		// Not a TRIGGER clause; put the token back for the next parser.
		p.lexer.restore(snap)
		return nil
	}
	whenTok := p.lexer.NextToken()
	if whenTok.Type != TokenWHEN {
		return fmt.Errorf("expected WHEN after TRIGGER, got %q", whenTok.Value)
	}

	// Collect predicate tokens until a clause boundary. The boundary token is
	// restored (not consumed) so the enclosing parseGroupBy loop and the
	// downstream clause parsers (parseWith/parseHaving/...) can see it — same
	// convention as parseWindowFunction leaving the token after ")" in place.
	var parts []string
	maxIter := 100
	iter := 0
	for {
		iter++
		if iter > maxIter {
			return errors.New("TRIGGER WHEN predicate parsing exceeded maximum iterations")
		}
		snap := p.lexer.save()
		t := p.lexer.NextToken()
		if t.Type == TokenWITH || t.Type == TokenOrder || t.Type == TokenEOF ||
			t.Type == TokenHAVING || t.Type == TokenLIMIT {
			p.lexer.restore(snap)
			break
		}
		switch t.Type {
		case TokenEQ:
			parts = append(parts, "==")
		case TokenAND:
			parts = append(parts, "&&")
		case TokenOR:
			parts = append(parts, "||")
		default:
			parts = append(parts, t.Value)
		}
	}
	stmt.Window.TriggerCondition = strings.Join(parts, " ")
	return nil
}

// parseOverClause 解析分析函数的 OVER 子句：OVER ([PARTITION BY ...] [WHEN ...])。
// 仅支持 PARTITION BY 和 WHEN，ORDER BY / ROWS / BETWEEN 一律报错。
// 约定：调用时 currentToken == TokenOVER（已读出），lexer 待读为 '('；返回时
// OVER(...) 已全部消费，'(' 内的 ')' 是最后读出的 token，调用者需 NextToken 取后续。
func (p *Parser) parseOverClause() (*types.OverSpec, error) {
	lp := p.lexer.NextToken()
	if lp.Type != TokenLParen {
		return nil, fmt.Errorf("expected '(' after OVER, got %q", lp.Value)
	}
	spec := &types.OverSpec{}
	for {
		t := p.lexer.NextToken()
		switch t.Type {
		case TokenRParen:
			return spec, nil
		case TokenPARTITION:
			if err := p.parseOverPartitionBy(spec); err != nil {
				return nil, err
			}
		case TokenWHEN:
			pred, err := p.parseOverWhen()
			if err != nil {
				return nil, err
			}
			spec.When = pred
		default:
			return nil, fmt.Errorf("OVER clause only supports PARTITION BY and WHEN (ORDER BY/ROWS not supported), got %q", t.Value)
		}
	}
}

// parseOverPartitionBy 解析 PARTITION BY <field>[, <field>...]。PARTITION 已读出。
func (p *Parser) parseOverPartitionBy(spec *types.OverSpec) error {
	by := p.lexer.NextToken()
	if by.Type != TokenBY {
		return fmt.Errorf("expected BY after PARTITION, got %q", by.Value)
	}
	for {
		id := p.lexer.NextToken()
		if id.Type != TokenIdent && id.Type != TokenQuotedIdent {
			return fmt.Errorf("expected partition field after PARTITION BY, got %q", id.Value)
		}
		// 去掉反引号
		name := id.Value
		if len(name) >= 2 && name[0] == '`' && name[len(name)-1] == '`' {
			name = name[1 : len(name)-1]
		}
		spec.PartitionBy = append(spec.PartitionBy, name)
		snap := p.lexer.save()
		sep := p.lexer.NextToken()
		if sep.Type == TokenComma {
			continue
		}
		p.lexer.restore(snap) // 回退（WHEN 或 ')'），交给上层循环
		return nil
	}
}

// parseOverWhen 解析 WHEN <predicate>，收集到 ) 或 PARTITION 为止。WHEN 已读出。
// 跟踪括号深度：WHEN 谓词里的函数调用（如 had_changed(true, status)）的括号要计入，
// 仅在深度归零时 ')' 才是 OVER 子句结束。
func (p *Parser) parseOverWhen() (string, error) {
	var parts []string
	depth := 0
	for i := 0; i < 100; i++ {
		snap := p.lexer.save()
		t := p.lexer.NextToken()
		if depth == 0 && (t.Type == TokenRParen || t.Type == TokenPARTITION) {
			p.lexer.restore(snap)
			return strings.Join(parts, " "), nil
		}
		switch t.Type {
		case TokenLParen:
			depth++
		case TokenRParen:
			depth--
		}
		switch t.Type {
		case TokenEQ:
			parts = append(parts, "==")
		case TokenAND:
			parts = append(parts, "&&")
		case TokenOR:
			parts = append(parts, "||")
		default:
			parts = append(parts, t.Value)
		}
	}
	return "", errors.New("OVER WHEN predicate too long")
}

func convertValue(s string) any {
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

	// Optional alias: "FROM stream AS s" or "FROM stream s".
	// A following JOIN/WHERE/GROUP/... keyword is not an alias.
	snap := p.lexer.save()
	next := p.lexer.NextToken()
	switch {
	case next.Type == TokenAS:
		aliasTok := p.lexer.NextToken()
		if aliasTok.Type == TokenIdent {
			stmt.SourceAlias = aliasTok.Value
		}
	case next.Type == TokenIdent && !isClauseBoundaryIdent(next.Value):
		stmt.SourceAlias = next.Value
	default:
		// Not an alias; put it back for the next clause parser.
		p.lexer.restore(snap)
	}
	return nil
}

// isClauseBoundaryIdent reports whether an identifier-looking token value is a
// keyword that starts a later clause (JOIN/WHERE/...) rather than a stream
// alias. JOIN/ON/INNER/LEFT/RIGHT/FULL/CROSS are not lexer keywords, so they
// arrive as TokenIdent and must be excluded from alias consumption here.
func isClauseBoundaryIdent(value string) bool {
	switch strings.ToUpper(value) {
	case "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "ON",
		"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "WITH":
		return true
	}
	return false
}

// parseJoin parses zero or more "[INNER|LEFT] JOIN table [AS] alias ON ...".
// It runs on the main lexer right after parseFrom, fully consuming each JOIN
// clause and leaving the lexer positioned at the next clause keyword (WHERE/
// GROUP/...). The lexer's save/restore is used to look ahead without committing.
func (p *Parser) parseJoin(stmt *SelectStatement) error {
	for {
		snap := p.lexer.save()
		tok := p.lexer.NextToken()
		joinType := "INNER"

		switch strings.ToUpper(tok.Value) {
		case "INNER":
			if j := p.lexer.NextToken(); strings.ToUpper(j.Value) != "JOIN" {
				return fmt.Errorf("expected JOIN after INNER, got %q", j.Value)
			}
		case "LEFT":
			// optional OUTER
			outerSnap := p.lexer.save()
			if o := p.lexer.NextToken(); strings.ToUpper(o.Value) != "OUTER" {
				p.lexer.restore(outerSnap)
			}
			if j := p.lexer.NextToken(); strings.ToUpper(j.Value) != "JOIN" {
				return fmt.Errorf("expected JOIN after LEFT, got %q", j.Value)
			}
			joinType = "LEFT"
		case "JOIN":
			// bare JOIN == INNER
		default:
			// Not a JOIN clause; restore and let the next clause parser handle it.
			p.lexer.restore(snap)
			return nil
		}

		// Table name.
		tableTok := p.lexer.NextToken()
		if tableTok.Type != TokenIdent {
			return fmt.Errorf("expected table name after JOIN, got %q", tableTok.Value)
		}
		jc := types.JoinConfig{Table: tableTok.Value, JoinType: joinType}

		// Optional alias: "AS m" or bare "m".
		aliasSnap := p.lexer.save()
		aliasTok := p.lexer.NextToken()
		switch {
		case aliasTok.Type == TokenAS:
			a := p.lexer.NextToken()
			if a.Type != TokenIdent {
				return fmt.Errorf("expected alias after AS, got %q", a.Value)
			}
			jc.Alias = a.Value
		case aliasTok.Type == TokenIdent && !isClauseBoundaryIdent(aliasTok.Value):
			jc.Alias = aliasTok.Value
		default:
			p.lexer.restore(aliasSnap)
		}
		if jc.Alias == "" {
			jc.Alias = jc.Table
		}

		// ON <field> = <field> [AND <field> = <field>]...
		onTok := p.lexer.NextToken()
		if strings.ToUpper(onTok.Value) != "ON" {
			return fmt.Errorf("expected ON after JOIN table, got %q", onTok.Value)
		}
		for {
			left, err := p.readJoinedFieldName()
			if err != nil {
				return err
			}
			eq := p.lexer.NextToken()
			if eq.Type != TokenEQ {
				return fmt.Errorf("expected = in JOIN ON, got %q", eq.Value)
			}
			right, err := p.readJoinedFieldName()
			if err != nil {
				return err
			}
			jc.OnPairs = append(jc.OnPairs, types.JoinOnPair{
				StreamField: stripAliasPrefix(left, stmt.SourceAlias, jc.Alias),
				TableField:  stripAliasPrefix(right, stmt.SourceAlias, jc.Alias),
			})

			// Continue on AND, otherwise stop and put the boundary token back.
			andSnap := p.lexer.save()
			andTok := p.lexer.NextToken()
			if andTok.Type != TokenAND {
				p.lexer.restore(andSnap)
				break
			}
		}

		stmt.JoinConfigs = append(stmt.JoinConfigs, jc)
	}
}

// readJoinedFieldName reads a dotted field path from the lexer (e.g. "s.deviceId"
// or "deviceId" or "m.profile.id"), used in JOIN ON clauses.
func (p *Parser) readJoinedFieldName() (string, error) {
	tok := p.lexer.NextToken()
	if tok.Type != TokenIdent && tok.Type != TokenQuotedIdent {
		return "", fmt.Errorf("expected field name in ON clause, got %q", tok.Value)
	}
	name := tok.Value
	if len(name) >= 2 && name[0] == '`' && name[len(name)-1] == '`' {
		name = name[1 : len(name)-1]
	}
	for {
		dotSnap := p.lexer.save()
		dot := p.lexer.NextToken()
		if dot.Type != TokenDot {
			p.lexer.restore(dotSnap)
			break
		}
		part := p.lexer.NextToken()
		if part.Type != TokenIdent && part.Type != TokenQuotedIdent {
			return "", fmt.Errorf("expected field name after '.', got %q", part.Value)
		}
		pv := part.Value
		if len(pv) >= 2 && pv[0] == '`' && pv[len(pv)-1] == '`' {
			pv = pv[1 : len(pv)-1]
		}
		name += "." + pv
	}
	return name, nil
}

// stripAliasPrefix removes a leading "alias." qualifier so the stored field path
// resolves directly against the stream row or matched table row. "s.deviceId"
// (stream alias) -> "deviceId"; "m.location" (table alias) -> "location".
// Which side a pair belongs to is determined by which alias it carries.
func stripAliasPrefix(field, streamAlias, tableAlias string) string {
	parts := strings.SplitN(field, ".", 2)
	if len(parts) == 2 {
		if parts[0] == streamAlias || parts[0] == tableAlias {
			return parts[1]
		}
	}
	return field
}

func (p *Parser) parseGroupBy(stmt *SelectStatement) error {
	tok := p.lexer.lookupIdent(p.lexer.readPreviousIdentifier())
	hasWindowFunction := false
	if tok.Type == TokenGlobal {
		hasWindowFunction = true
		if err := p.parseGlobalWindow(stmt); err != nil {
			return err
		}
	} else if tok.Type == TokenTumbling || tok.Type == TokenSliding || tok.Type == TokenCounting || tok.Type == TokenSession {
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

	// 累积分组项：跟踪括号深度，把函数表达式（如 upper(device)）作为整体一项，
	// 顶层逗号分隔。collapseSpacesOutsideQuotes 归一化（parser 读出的多 token 带空格）。
	var currentItem strings.Builder
	parenLevel := 0
	flushItem := func() {
		if hasGroupBy && currentItem.Len() > 0 {
			stmt.GroupBy = append(stmt.GroupBy, collapseSpacesOutsideQuotes(currentItem.String()))
		}
		currentItem.Reset()
	}

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
			flushItem()
			break
		}
		if tok.Type == TokenComma {
			flushItem()
			continue
		}
		// 顶层（括号外）的窗口/全局窗/OVER 子句：先收尾当前项再处理。
		if parenLevel == 0 {
			if tok.Type == TokenGlobal {
				flushItem()
				if err := p.parseGlobalWindow(stmt); err != nil {
					return err
				}
				continue
			}
			if tok.Type == TokenTumbling || tok.Type == TokenSliding || tok.Type == TokenCounting || tok.Type == TokenSession {
				flushItem()
				if err := p.parseWindowFunction(stmt, tok.Value); err != nil {
					return err
				}
				// After parsing window function, skip adding it to GroupBy and continue
				continue
			}
			if tok.Type == TokenOVER {
				// GROUP BY window 的 OVER(...) 子句（仅 WHEN 输入门控）。校验在 ToStreamConfig
				// 做（parseGroupBy 的返回错误会被 errorRecovery 当作可恢复错误吞掉）。
				flushItem()
				over, err := p.parseOverClause()
				if err != nil {
					return err
				}
				if over != nil && stmt.Window.Over == nil {
					stmt.Window.Over = over
				}
				continue
			}
			// Skip top-level right parenthesis tokens (left by parseWindowFunction)
			if tok.Type == TokenRParen {
				continue
			}
		}
		// 跟踪括号深度（函数调用参数），把 token 累积进当前分组项。
		if tok.Type == TokenLParen {
			parenLevel++
		} else if tok.Type == TokenRParen {
			parenLevel--
		}
		if currentItem.Len() > 0 {
			currentItem.WriteByte(' ')
		}
		currentItem.WriteString(tok.Value)
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
		// Unknown WITH parameters (plain identifiers rather than a recognized
		// option keyword) are tolerated but surfaced, so typos don't silently
		// drop configuration. The following = and value tokens are consumed by
		// later loop iterations (none of the known-option branches match).
		if valTok.Type == TokenIdent {
			logger.Warn("WITH: ignoring unknown option %q (known: TIMESTAMP, TIMEUNIT, MAXOUTOFORDERNESS, ALLOWEDLATENESS, IDLETIMEOUT, STATETTL)", valTok.Value)
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
		if valTok.Type == TokenStateTTL {
			next := p.lexer.NextToken()
			if next.Type == TokenEQ {
				next = p.lexer.NextToken()
				durationStr := next.Value
				if strings.HasPrefix(durationStr, "'") && strings.HasSuffix(durationStr, "'") {
					durationStr = strings.Trim(durationStr, "'")
				}
				if duration, err := cast.ToDurationE(durationStr); err == nil {
					if stmt.Window.Type == "" {
						stmt.Window = WindowDefinition{
							CountStateTTL: duration,
						}
					} else {
						stmt.Window.CountStateTTL = duration
					}
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

	// 在 token 流中查找真正的 LIMIT 关键字，避免误匹配标识符/字符串字面量中的子串
	limitLexer := NewLexer(p.input)
	limitLexer.SetErrorRecovery(NewErrorRecovery(nil))
	limitIndex := -1
	for {
		tok := limitLexer.NextToken()
		if tok.Type == TokenEOF {
			break
		}
		if tok.Type == TokenLIMIT {
			limitIndex = tok.Pos
			break
		}
	}
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

// parseOrderBy 解析 ORDER BY 子句。用独立 lexer 扫描真正的 TokenOrder，
// 避免误匹配标识符/字符串字面量中的 "ORDER" 子串（与 parseLimit 同样的稳健做法）。
// v0.5：每个排序键为结果列名（标识符，可含点路径），后接可选 ASC/DESC，逗号分隔。
func (p *Parser) parseOrderBy(stmt *SelectStatement) error {
	// 用独立 lexer 定位真正的 ORDER 关键字位置
	orderLexer := NewLexer(p.input)
	orderLexer.SetErrorRecovery(NewErrorRecovery(nil))
	orderPos := -1
	for {
		tok := orderLexer.NextToken()
		if tok.Type == TokenEOF {
			break
		}
		if tok.Type == TokenOrder {
			orderPos = tok.Pos
			break
		}
	}
	if orderPos == -1 {
		return nil // 无 ORDER BY 子句
	}

	// 从 ORDER 之后重新 lex，解析 BY 及字段列表
	fieldLexer := NewLexer(p.input[orderPos+len("ORDER"):])
	fieldLexer.SetErrorRecovery(NewErrorRecovery(nil))

	if tok := fieldLexer.NextToken(); tok.Type != TokenBY {
		// ORDER 后不是 BY，不当作 ORDER BY（例如列名含 ORDER 子串已被上面的 token 扫描排除）
		return nil
	}

	var fields []types.OrderByField
	for {
		var exprBuilder strings.Builder
		dir := types.SortAsc
		done := false    // reached end of ORDER BY (EOF/LIMIT)
		advance := false // a comma was consumed; another key follows

		// Collect the field expression tokens.
		for {
			tok := fieldLexer.NextToken()
			if tok.Type == TokenEOF || tok.Type == TokenLIMIT {
				done = true
				break
			}
			if tok.Type == TokenComma {
				advance = true
				break
			}
			// ASC/DESC 作为方向关键字（它们没有独立 token，按标识符值识别）
			if tok.Type == TokenIdent {
				upper := strings.ToUpper(tok.Value)
				if upper == "ASC" || upper == "DESC" {
					if upper == "DESC" {
						dir = types.SortDesc
					}
					// 方向已消费，其后应为逗号或子句结束
					sep := fieldLexer.NextToken()
					if sep.Type == TokenComma {
						advance = true
					} else {
						done = true
					}
					break
				}
			}
			// 追加 token 值（不加分隔符，使 a.b / backtick 字段能正确重建）
			exprBuilder.WriteString(tok.Value)
		}

		if exprStr := strings.TrimSpace(exprBuilder.String()); exprStr != "" {
			fields = append(fields, types.OrderByField{Expression: exprStr, Direction: dir})
		}
		if done || !advance {
			break
		}
	}

	stmt.OrderBy = fields
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

	// Reject malformed GROUP BY. 用原始 stmt.GroupBy（extractGroupFields 过滤前），
	// 否则 isAggregationFunction 的"含括号保守判聚合"兜底会把拼错的窗口函数
	// （如 InvalidWindow('5s')）当聚合丢掉，使 config.GroupFields 为空、校验落空。
	// 合法分组项：裸列名，或顶层为已注册标量函数的表达式（如 upper(device)）。
	// 引号 artifact 或未注册函数 → 视为拼错的窗口函数泄漏，拒绝。
	for _, g := range stmt.GroupBy {
		if strings.ContainsAny(g, "'\"") {
			return nil, "", fmt.Errorf("invalid GROUP BY field %q: unknown window function or unsupported expression", g)
		}
		if strings.Contains(g, "(") && !groupKeyIsScalarFunctionExpr(g) {
			return nil, "", fmt.Errorf("invalid GROUP BY field %q: unknown window function or unsupported expression", g)
		}
	}

	return config, condition, nil
}
