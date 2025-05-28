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
	lexer *Lexer
}

func NewParser(input string) *Parser {
	return &Parser{
		lexer: NewLexer(input),
	}
}

func (p *Parser) Parse() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	// 解析SELECT子句
	if err := p.parseSelect(stmt); err != nil {
		return nil, err
	}

	// 解析FROM子句
	if err := p.parseFrom(stmt); err != nil {
		return nil, err
	}

	// 解析WHERE子句
	if err := p.parseWhere(stmt); err != nil {
		return nil, err
	}

	// 解析GROUP BY子句
	if err := p.parseGroupBy(stmt); err != nil {
		return nil, err
	}

	// 解析 HAVING 子句
	if err := p.parseHaving(stmt); err != nil {
		return nil, err
	}

	if err := p.parseWith(stmt); err != nil {
		return nil, err
	}

	// 解析LIMIT子句
	if err := p.parseLimit(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (p *Parser) parseSelect(stmt *SelectStatement) error {
	p.lexer.NextToken() // 跳过SELECT
	currentToken := p.lexer.NextToken()

	if currentToken.Type == TokenDISTINCT {
		stmt.Distinct = true
		currentToken = p.lexer.NextToken() // 消费 DISTINCT，移动到下一个 token
	}

	// 设置最大字段数量限制，防止无限循环
	maxFields := 100
	fieldCount := 0

	for {
		fieldCount++
		// 安全检查：防止无限循环
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
	current := p.lexer.NextToken() // 跳过WHERE
	if current.Type != TokenWHERE {
		return nil
	}

	// 设置最大次数限制，防止无限循环
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
		default:
			// 处理字符串值的引号
			if len(conditions) > 0 && conditions[len(conditions)-1] == "'" {
				conditions[len(conditions)-1] = conditions[len(conditions)-1] + tok.Value
			} else {
				conditions = append(conditions, tok.Value)
			}
		}
	}
	stmt.Condition = strings.Join(conditions, " ")
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
		// 处理引号包裹的值
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
		return errors.New("expected source identifier after FROM")
	}
	stmt.Source = tok.Value
	return nil
}

func (p *Parser) parseGroupBy(stmt *SelectStatement) error {
	tok := p.lexer.lookupIdent(p.lexer.readPreviousIdentifier())
	if tok.Type == TokenTumbling || tok.Type == TokenSliding || tok.Type == TokenCounting || tok.Type == TokenSession {
		p.parseWindowFunction(stmt, tok.Value)
	}
	if tok.Type == TokenGROUP {
		p.lexer.NextToken() // 跳过BY
	}

	// 设置最大次数限制，防止无限循环
	maxIterations := 100
	iterations := 0

	for {
		iterations++
		// 安全检查：防止无限循环
		if iterations > maxIterations {
			return errors.New("group by clause parsing exceeded maximum iterations, possible syntax error")
		}

		tok := p.lexer.NextToken()
		if tok.Type == TokenWITH || tok.Type == TokenOrder || tok.Type == TokenEOF ||
			tok.Type == TokenHAVING || tok.Type == TokenLIMIT {
			break
		}
		if tok.Type == TokenComma {
			continue
		}
		if tok.Type == TokenTumbling || tok.Type == TokenSliding || tok.Type == TokenCounting || tok.Type == TokenSession {
			p.parseWindowFunction(stmt, tok.Value)
			continue
		}

		stmt.GroupBy = append(stmt.GroupBy, tok.Value)
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
				// 检查Window是否已初始化，如果未初始化则创建新的WindowDefinition
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
				// 检查Window是否已初始化，如果未初始化则创建新的WindowDefinition
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

// parseLimit 解析LIMIT子句
func (p *Parser) parseLimit(stmt *SelectStatement) error {
	// 查看当前token
	if p.lexer.lookupIdent(p.lexer.readPreviousIdentifier()).Type == TokenLIMIT {
		// 获取下一个token，应该是一个数字
		tok := p.lexer.NextToken()
		if tok.Type == TokenNumber {
			// 将数字字符串转换为整数
			limit, err := strconv.Atoi(tok.Value)
			if err != nil {
				return errors.New("LIMIT值必须是一个整数")
			}
			stmt.Limit = limit
		} else {
			return errors.New("LIMIT后必须跟一个整数")
		}
	}
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
		default:
			// 处理字符串值的引号
			if len(conditions) > 0 && conditions[len(conditions)-1] == "'" {
				conditions[len(conditions)-1] = conditions[len(conditions)-1] + tok.Value
			} else {
				conditions = append(conditions, tok.Value)
			}
		}
	}

	stmt.Having = strings.Join(conditions, " ")
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
