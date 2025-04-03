package rsql

import (
	"errors"
	"strconv"
	"strings"
	"time"
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

	if err := p.parseWith(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}
func (p *Parser) parseSelect(stmt *SelectStatement) error {
	p.lexer.NextToken() // 跳过SELECT
	currentToken := p.lexer.NextToken()
	for {
		var expr strings.Builder
		for {
			if currentToken.Type == TokenFROM || currentToken.Type == TokenComma || currentToken.Type == TokenAS {
				break
			}
			expr.WriteString(currentToken.Value)
			currentToken = p.lexer.NextToken()
		}

		field := Field{Expression: strings.TrimSpace(expr.String())}

		// 处理别名
		if currentToken.Type == TokenAS {
			field.Alias = p.lexer.NextToken().Value
		}
		stmt.Fields = append(stmt.Fields, field)
		currentToken = p.lexer.NextToken()
		if currentToken.Type == TokenFROM {
			break
		}
	}
	return nil
}

func (p *Parser) parseWhere(stmt *SelectStatement) error {
	var conditions []string
	current := p.lexer.NextToken() // 跳过WHERE
	if current.Type != TokenWHERE {
		return nil
	}
	for {
		tok := p.lexer.NextToken()
		if tok.Type == TokenGROUP || tok.Type == TokenEOF {
			break
		}
		switch tok.Type {
		case TokenIdent, TokenNumber:
			conditions = append(conditions, tok.Value)
		case TokenString:
			conditions = append(conditions, "'"+tok.Value+"'")
		case TokenEQ:
			conditions = append(conditions, "==")
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

	for p.lexer.peekChar() != ')' {
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
	//p.lexer.NextToken() // 跳过GROUP
	p.lexer.NextToken() // 跳过BY

	for {
		tok := p.lexer.NextToken()
		if tok.Type == TokenEOF {
			break
		}
		if tok.Type == TokenComma {
			continue
		}
		if tok.Type == TokenTumbling || tok.Type == TokenSliding || tok.Type == TokenCounting || tok.Type == TokenSession {
			return p.parseWindowFunction(stmt, tok.Value)
		}

		stmt.GroupBy = append(stmt.GroupBy, tok.Value)

		//if p.lexer.NextToken().Type != TokenComma {
		//	break
		//}
	}
	return nil
}

func (p *Parser) parseWith(stmt *SelectStatement) error {
	p.lexer.NextToken() // 跳过(
	for p.lexer.peekChar() != ')' {
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
