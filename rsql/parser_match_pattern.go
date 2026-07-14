package rsql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rulego/streamsql/types"
)

// parseMRPatternBody 解析 PATTERN ( <正则> )。约定：调用时 lexer 在 '(' 之前。
func (p *Parser) parseMRPatternBody() (*types.PatternNode, error) {
	if t := p.lexer.NextToken(); t.Type != TokenLParen {
		return nil, fmt.Errorf("expected '(' after PATTERN, got %q", t.Value)
	}
	node, err := p.parseMRAlternation()
	if err != nil {
		return nil, err
	}
	if t := p.lexer.NextToken(); t.Type != TokenRParen {
		return nil, fmt.Errorf("expected ')' to close PATTERN, got %q", t.Value)
	}
	return node, nil
}

// parseMRAlternation: sequence ('|' sequence)*
func (p *Parser) parseMRAlternation() (*types.PatternNode, error) {
	first, err := p.parseMRSequence()
	if err != nil {
		return nil, err
	}
	children := []*types.PatternNode{first}
	for p.peekToken().Type == TokenPipe {
		p.lexer.NextToken() // consume '|'
		c, err := p.parseMRSequence()
		if err != nil {
			return nil, err
		}
		children = append(children, c)
	}
	if len(children) == 1 {
		return children[0], nil
	}
	return &types.PatternNode{Kind: types.PatternAlternation, Children: children}, nil
}

// parseMRSequence: quantified+ (并列即序列)
func (p *Parser) parseMRSequence() (*types.PatternNode, error) {
	var atoms []*types.PatternNode
	for isMRAtomStart(p.peekToken()) {
		a, err := p.parseMRQuantified()
		if err != nil {
			return nil, err
		}
		atoms = append(atoms, a)
	}
	if len(atoms) == 0 {
		return nil, fmt.Errorf("empty pattern sequence near %q", p.peekToken().Value)
	}
	if len(atoms) == 1 {
		return atoms[0], nil
	}
	return &types.PatternNode{Kind: types.PatternSequence, Children: atoms}, nil
}

// parseMRQuantified: atom + 可选量词
func (p *Parser) parseMRQuantified() (*types.PatternNode, error) {
	atom, err := p.parseMRAtom()
	if err != nil {
		return nil, err
	}
	q, ok, err := p.tryMRQuantifier()
	if err != nil {
		return nil, err
	}
	if !ok {
		return atom, nil
	}
	return &types.PatternNode{Kind: types.PatternRepetition, Children: []*types.PatternNode{atom}, Quant: q}, nil
}

// parseMRAtom: 模式变量 | ( 交替 ) | PERMUTE(...) | {- 排除 -}
// 模式变量可为任意标识符，含被词法器归为关键字的词（如 End/When），故按"值以字母开头"识别。
func (p *Parser) parseMRAtom() (*types.PatternNode, error) {
	t := p.peekToken()
	switch t.Type {
	case TokenLParen:
		p.lexer.NextToken() // consume '('
		inner, err := p.parseMRAlternation()
		if err != nil {
			return nil, err
		}
		if t := p.lexer.NextToken(); t.Type != TokenRParen {
			return nil, fmt.Errorf("expected ')' to close pattern group, got %q", t.Value)
		}
		return &types.PatternNode{Kind: types.PatternGroup, Children: []*types.PatternNode{inner}}, nil
	case TokenLBrace:
		// {- ... -} 排除（absence）：解析为 Exclusion 节点，编译期拒绝
		p.lexer.NextToken() // consume '{'
		if d := p.lexer.NextToken(); d.Type != TokenMinus {
			return nil, fmt.Errorf("expected '-' after '{' in exclusion pattern, got %q", d.Value)
		}
		inner, err := p.parseMRAlternation()
		if err != nil {
			return nil, err
		}
		if d := p.lexer.NextToken(); d.Type != TokenMinus {
			return nil, fmt.Errorf("expected '-}' to close exclusion pattern, got %q", d.Value)
		}
		if c := p.lexer.NextToken(); c.Type != TokenRBrace {
			return nil, fmt.Errorf("expected '-}' to close exclusion pattern, got %q", c.Value)
		}
		return &types.PatternNode{Kind: types.PatternExclusion, Children: []*types.PatternNode{inner}}, nil
	}
	if isMRIdentLike(t) {
		p.lexer.NextToken() // consume 变量名
		if t.Type == TokenIdent && strings.EqualFold(t.Value, "PERMUTE") {
			return p.parseMRPermute()
		}
		return &types.PatternNode{Kind: types.PatternLiteral, Symbol: stripBackticks(t.Value)}, nil
	}
	return nil, fmt.Errorf("unexpected %q in PATTERN", t.Value)
}

// isMRIdentLike 报告 token 是否像一个标识符（值以字母/下划线开头），用于把 End/When
// 等被词法器归为关键字的词也接受为模式变量。
func isMRIdentLike(t Token) bool {
	v := t.Value
	return v != "" && (isLetter(v[0]) || v[0] == '_')
}

// parseMRPermute: PERMUTE ( alt , alt , ... )
func (p *Parser) parseMRPermute() (*types.PatternNode, error) {
	if t := p.lexer.NextToken(); t.Type != TokenLParen {
		return nil, fmt.Errorf("expected '(' after PERMUTE, got %q", t.Value)
	}
	var children []*types.PatternNode
	for {
		c, err := p.parseMRAlternation()
		if err != nil {
			return nil, err
		}
		children = append(children, c)
		if p.peekToken().Type != TokenComma {
			break
		}
		p.lexer.NextToken() // consume ','
	}
	if t := p.lexer.NextToken(); t.Type != TokenRParen {
		return nil, fmt.Errorf("expected ')' to close PERMUTE, got %q", t.Value)
	}
	return &types.PatternNode{Kind: types.PatternPermute, Children: children}, nil
}

// isMRAtomStart 报告 token 能否开启一个模式原子。
func isMRAtomStart(t Token) bool {
	if t.Type == TokenLParen || t.Type == TokenLBrace {
		return true
	}
	return isMRIdentLike(t)
}

// tryMRQuantifier 尝试读取后缀量词；ok=false 表示无量词。
func (p *Parser) tryMRQuantifier() (*types.Quantifier, bool, error) {
	t := p.peekToken()
	var q types.Quantifier
	switch t.Type {
	case TokenQuestion:
		p.lexer.NextToken()
		q = types.Quantifier{Min: 0, Max: 1}
	case TokenAsterisk:
		p.lexer.NextToken()
		q = types.Quantifier{Min: 0, Max: -1}
	case TokenPlus:
		p.lexer.NextToken()
		q = types.Quantifier{Min: 1, Max: -1}
	case TokenLBrace:
		p.lexer.NextToken() // consume '{'
		parsed, err := p.parseMRBounded()
		if err != nil {
			return nil, false, err
		}
		q = parsed
	default:
		return nil, false, nil
	}
	q.Greedy = !p.consumeReluctant()
	return &q, true, nil
}

// parseMRBounded 解析 {n} / {n,} / {n,m}（'{' 已消费）。
func (p *Parser) parseMRBounded() (types.Quantifier, error) {
	nTok := p.lexer.NextToken()
	if nTok.Type != TokenNumber {
		return types.Quantifier{}, fmt.Errorf("expected number in quantifier, got %q", nTok.Value)
	}
	n, err := strconv.Atoi(nTok.Value)
	if err != nil {
		return types.Quantifier{}, fmt.Errorf("invalid quantifier bound %q: %w", nTok.Value, err)
	}
	q := types.Quantifier{Min: n, Max: n}
	t := p.lexer.NextToken()
	switch t.Type {
	case TokenRBrace:
		return q, nil
	case TokenComma:
		t2 := p.lexer.NextToken()
		if t2.Type == TokenRBrace {
			q.Max = -1
			return q, nil
		}
		if t2.Type != TokenNumber {
			return types.Quantifier{}, fmt.Errorf("expected number or '}' after ',' in quantifier, got %q", t2.Value)
		}
		m, err := strconv.Atoi(t2.Value)
		if err != nil {
			return types.Quantifier{}, fmt.Errorf("invalid quantifier max %q: %w", t2.Value, err)
		}
		if t3 := p.lexer.NextToken(); t3.Type != TokenRBrace {
			return types.Quantifier{}, fmt.Errorf("expected '}' to close quantifier, got %q", t3.Value)
		}
		q.Max = m
		return q, nil
	}
	return types.Quantifier{}, fmt.Errorf("expected ',' or '}' in quantifier, got %q", t.Value)
}

// consumeReluctant 若量词后紧跟 '?' 则消费它，返回 true（懒惰量词）。
func (p *Parser) consumeReluctant() bool {
	if p.peekToken().Type == TokenQuestion {
		p.lexer.NextToken()
		return true
	}
	return false
}
