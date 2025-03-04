package rsql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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

	return stmt, nil
}

func (p *Parser) parseSelect(stmt *SelectStatement) error {
	p.lexer.NextToken() // 跳过SELECT

	for {
		tok := p.lexer.NextToken()
		if tok.Type == TokenFROM {
			break
		}

		field := Field{Expression: tok.Value}
		if p.lexer.peekChar() == ' ' {
			if aliasTok := p.lexer.NextToken(); aliasTok.Type == TokenAS {
				field.Alias = p.lexer.NextToken().Value
			}
		}
		stmt.Fields = append(stmt.Fields, field)

		if p.lexer.NextToken().Type != TokenComma {
			break
		}
	}
	return nil
}

func (p *Parser) parseFrom(stmt *SelectStatement) error {
	tok := p.lexer.NextToken()
	if tok.Type != TokenIdent {
		return errors.New("expected source identifier after FROM")
	}
	stmt.Source = tok.Value
	return nil
}

func (p *Parser) parseWhere(stmt *SelectStatement) error {
	var conditions []string
	p.lexer.NextToken() // 跳过WHERE

	for {
		tok := p.lexer.NextToken()
		switch tok.Type {
		case TokenIdent, TokenNumber, TokenString:
			conditions = append(conditions, tok.Value)
		case TokenEQ:
			conditions = append(conditions, "==")
		case TokenAND:
			conditions = append(conditions, "&&")
		case TokenOR:
			conditions = append(conditions, "||")
		default:
			stmt.Condition = strings.Join(conditions, " ")
			return nil
		}
	}
}

func (p *Parser) parseGroupBy(stmt *SelectStatement) error {
	p.lexer.NextToken() // 跳过GROUP
	p.lexer.NextToken() // 跳过BY

	for {
		tok := p.lexer.NextToken()
		if tok.Type == TokenTumbling || tok.Type == TokenSliding {
			return p.parseWindowFunction(stmt, tok.Value)
		}

		stmt.GroupBy = append(stmt.GroupBy, tok.Value)

		if p.lexer.NextToken().Type != TokenComma {
			break
		}
	}
	return nil
}

func (p *Parser) parseWindowFunction(stmt *SelectStatement, winType string) error {
	p.lexer.NextToken() // 跳过函数名
	params := make(map[string]interface{})

	for p.lexer.peekChar() != ')' {
		keyTok := p.lexer.NextToken()
		if keyTok.Type != TokenIdent {
			return fmt.Errorf("expected parameter key, got %v", keyTok)
		}

		valTok := p.lexer.NextToken()
		params[keyTok.Value] = convertValue(valTok.Value)
	}

	stmt.Window = WindowDefinition{
		Type:   winType,
		Params: params,
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
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return strings.Trim(s, "'")
	}
	return s
}
