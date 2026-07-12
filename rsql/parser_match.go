package rsql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rulego/streamsql/types"
)

// isMRClauseKeyword 报告 ident 是否是 MATCH_RECOGNIZE 子句起始关键字（用于表达式收集终止）。
func isMRClauseKeyword(v string) bool {
	switch strings.ToUpper(v) {
	case "PARTITION", "ORDER", "MEASURES", "ONE", "ALL", "AFTER",
		"PATTERN", "DEFINE", "SUBSET", "WITHIN":
		return true
	}
	return false
}

// peekToken 不消费地预读一个 token（基于 lexer save/restore）。
func (p *Parser) peekToken() Token {
	snap := p.lexer.save()
	t := p.lexer.NextToken()
	p.lexer.restore(snap)
	return t
}

// parseMatchRecognize 解析 FROM 之后的可选 MATCH_RECOGNIZE(...) 子句。
// 识别完整语法；未实现子句给明确错误。约定：进入时 lexer 在 MATCH 之前；非 MATCH 则不动。
// parseMatchRecognize 解析 FROM 之后的可选 MATCH_RECOGNIZE(...) 子句。
// 识别完整语法；未实现子句给明确错误。约定：进入时 lexer 在 MATCH_RECOGNIZE 之前；非该关键字则不动。
// 注：词法器把 MATCH_RECOGNIZE 读成单个标识符 token（下划线连接）。
func (p *Parser) parseMatchRecognize(stmt *SelectStatement) error {
	snap := p.lexer.save()
	t := p.lexer.NextToken()
	if !strings.EqualFold(t.Value, "MATCH_RECOGNIZE") {
		p.lexer.restore(snap)
		return nil
	}
	lp := p.lexer.NextToken()
	if lp.Type != TokenLParen {
		return fmt.Errorf("expected '(' after MATCH_RECOGNIZE, got %q", lp.Value)
	}

	spec := &types.MatchRecognizeSpec{
		RowsPerMatch: types.RowsPerMatchOne,
		Skip:         types.SkipPastLastRow,
	}
	for {
		t := p.lexer.NextToken()
		if t.Type == TokenRParen {
			break // MATCH_RECOGNIZE 结束
		}
		kw := strings.ToUpper(t.Value)
		switch kw {
		case "PARTITION":
			if err := p.expectKeyword("BY"); err != nil {
				return err
			}
			fields, err := p.readIdentList()
			if err != nil {
				return err
			}
			spec.PartitionBy = fields
		case "ORDER":
			if err := p.expectKeyword("BY"); err != nil {
				return err
			}
			obs, err := p.readMROrderBy()
			if err != nil {
				return err
			}
			spec.OrderBy = obs
		case "MEASURES":
			ms, err := p.readMRMeasures()
			if err != nil {
				return err
			}
			spec.Measures = ms
		case "ONE":
			// SQL 标准：ONE ROW PER MATCH
			if err := p.expectRowPerMatch("ROW"); err != nil {
				return err
			}
			spec.RowsPerMatch = types.RowsPerMatchOne
		case "ALL":
			// SQL 标准：ALL ROWS PER MATCH（复数）
			if err := p.expectRowPerMatch("ROWS"); err != nil {
				return err
			}
			spec.RowsPerMatch = types.RowsPerMatchAll
		case "AFTER":
			if err := p.readMRAfterMatchSkip(spec); err != nil {
				return err
			}
		case "PATTERN":
			node, err := p.parseMRPatternBody()
			if err != nil {
				return err
			}
			spec.Pattern = node
		case "SUBSET":
			ss, err := p.readMRSubsets()
			if err != nil {
				return err
			}
			spec.Subsets = append(spec.Subsets, ss...)
		case "WITHIN":
			d, err := p.parseMRDuration()
			if err != nil {
				return err
			}
			spec.Within = d
		case "DEFINE":
			ds, err := p.readMRDefines()
			if err != nil {
				return err
			}
			spec.Defines = ds
		default:
			return fmt.Errorf("unexpected %q in MATCH_RECOGNIZE", t.Value)
		}
	}

	// Pattern/OrderBy 必需性在 ToStreamConfig 校验（其错误不被解析恢复吞掉）。
	stmt.MatchRecognize = spec
	return nil
}

// expectKeyword 消费一个值匹配（大小写无关）的 ident token。
func (p *Parser) expectKeyword(want string) error {
	t := p.lexer.NextToken()
	if !strings.EqualFold(t.Value, want) {
		return fmt.Errorf("expected %s, got %q", want, t.Value)
	}
	return nil
}

// readIdentList 读取逗号分隔的标识符列表（去反引号），到非逗号终止符止（不消费终止符）。
func (p *Parser) readIdentList() ([]string, error) {
	var fields []string
	for {
		t := p.lexer.NextToken()
		if !isMRSymbolToken(t) {
			return nil, fmt.Errorf("expected identifier, got %q", t.Value)
		}
		fields = append(fields, stripBackticks(t.Value))
		snap := p.lexer.save()
		sep := p.lexer.NextToken()
		if sep.Type != TokenComma {
			p.lexer.restore(snap)
			return fields, nil
		}
	}
}

func stripBackticks(s string) string {
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
}

// readMROrderBy 读取 ORDER BY 字段[ ASC|DESC], ... 到非逗号终止符止。
func (p *Parser) readMROrderBy() ([]types.OrderByField, error) {
	var obs []types.OrderByField
	for {
		t := p.lexer.NextToken()
		if !isMRSymbolToken(t) {
			return nil, fmt.Errorf("expected ORDER BY field, got %q", t.Value)
		}
		f := types.OrderByField{Expression: stripBackticks(t.Value), Direction: types.SortAsc}
		// 可选 ASC/DESC
		snap := p.lexer.save()
		dir := p.lexer.NextToken()
		if strings.EqualFold(dir.Value, "DESC") {
			f.Direction = types.SortDesc
		} else if strings.EqualFold(dir.Value, "ASC") {
			// 默认
		} else {
			p.lexer.restore(snap)
		}
		obs = append(obs, f)
		snap2 := p.lexer.save()
		sep := p.lexer.NextToken()
		if sep.Type != TokenComma {
			p.lexer.restore(snap2)
			return obs, nil
		}
	}
}

// expectRowPerMatch 消费 "<rowKeyword> PER MATCH"（rowKeyword 为 ROW 或 ROWS）。
func (p *Parser) expectRowPerMatch(rowKeyword string) error {
	for _, want := range []string{rowKeyword, "PER", "MATCH"} {
		if err := p.expectKeyword(want); err != nil {
			return err
		}
	}
	return nil
}

// readMRMeasures 读取 MEASURES <expr> AS <alias>, ... 到非逗号终止符止。
func (p *Parser) readMRMeasures() ([]types.Measure, error) {
	var ms []types.Measure
	for {
		expr, err := p.readMRUntilAS()
		if err != nil {
			return nil, err
		}
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}
		aliasTok := p.lexer.NextToken()
		if !isMRSymbolToken(aliasTok) {
			return nil, fmt.Errorf("expected MEASURES alias after AS, got %q", aliasTok.Value)
		}
		ms = append(ms, types.Measure{Expr: expr, Alias: stripBackticks(aliasTok.Value)})
		snap := p.lexer.save()
		sep := p.lexer.NextToken()
		if sep.Type != TokenComma {
			p.lexer.restore(snap)
			return ms, nil
		}
	}
}

// readMRDefines 读取 DEFINE <sym> AS <cond>, ... 到非逗号终止符止。
func (p *Parser) readMRDefines() ([]types.MatchDefine, error) {
	var ds []types.MatchDefine
	for {
		symTok := p.lexer.NextToken()
		if !isMRSymbolToken(symTok) {
			return nil, fmt.Errorf("expected DEFINE symbol, got %q", symTok.Value)
		}
		if err := p.expectKeyword("AS"); err != nil {
			return nil, err
		}
		cond, err := p.readMRExpr()
		if err != nil {
			return nil, err
		}
		ds = append(ds, types.MatchDefine{Symbol: stripBackticks(symTok.Value), Cond: cond})
		snap := p.lexer.save()
		sep := p.lexer.NextToken()
		if sep.Type != TokenComma {
			p.lexer.restore(snap)
			return ds, nil
		}
	}
}

// readMRSubsets 读取 SUBSET <name> = (sym, ...), ... 到非逗号终止符止（P2：解析存储）。
func (p *Parser) readMRSubsets() ([]types.MatchSubset, error) {
	var ss []types.MatchSubset
	for {
		nameTok := p.lexer.NextToken()
		if nameTok.Type != TokenIdent {
			return nil, fmt.Errorf("expected SUBSET name, got %q", nameTok.Value)
		}
		if eq := p.lexer.NextToken(); eq.Type != TokenEQ {
			return nil, fmt.Errorf("expected '=' after SUBSET %s, got %q", nameTok.Value, eq.Value)
		}
		if t := p.lexer.NextToken(); t.Type != TokenLParen {
			return nil, fmt.Errorf("expected '(' after SUBSET %s=, got %q", nameTok.Value, t.Value)
		}
		syms, err := p.readIdentList()
		if err != nil {
			return nil, err
		}
		if t := p.lexer.NextToken(); t.Type != TokenRParen {
			return nil, fmt.Errorf("expected ')' to close SUBSET, got %q", t.Value)
		}
		ss = append(ss, types.MatchSubset{Name: nameTok.Value, Symbols: syms})
		snap := p.lexer.save()
		sep := p.lexer.NextToken()
		if sep.Type != TokenComma {
			p.lexer.restore(snap)
			return ss, nil
		}
	}
}

// readMRAfterMatchSkip 解析 AFTER MATCH SKIP (...)。
func (p *Parser) readMRAfterMatchSkip(spec *types.MatchRecognizeSpec) error {
	if err := p.expectKeyword("MATCH"); err != nil {
		return err
	}
	if err := p.expectKeyword("SKIP"); err != nil {
		return err
	}
	t := p.lexer.NextToken()
	switch strings.ToUpper(t.Value) {
	case "PAST":
		if err := p.expectKeyword("LAST"); err != nil {
			return err
		}
		if err := p.expectKeyword("ROW"); err != nil {
			return err
		}
		spec.Skip = types.SkipPastLastRow
	case "TO":
		n := p.lexer.NextToken()
		switch strings.ToUpper(n.Value) {
		case "NEXT":
			if err := p.expectKeyword("ROW"); err != nil {
				return err
			}
			spec.Skip = types.SkipToNextRow
		case "FIRST":
			sym, err := p.readSymbol()
			if err != nil {
				return err
			}
			spec.Skip = types.SkipToFirst
			spec.SkipSymbol = sym
		case "LAST":
			sym, err := p.readSymbol()
			if err != nil {
				return err
			}
			spec.Skip = types.SkipToLast
			spec.SkipSymbol = sym
		default:
			// TO <symbol>（无 FIRST/LAST）
			spec.Skip = types.SkipToVariable
			spec.SkipSymbol = n.Value
		}
	default:
		return fmt.Errorf("unexpected %q after AFTER MATCH SKIP", t.Value)
	}
	return nil
}

func (p *Parser) readSymbol() (string, error) {
	t := p.lexer.NextToken()
	if !isMRSymbolToken(t) {
		return "", fmt.Errorf("expected pattern variable, got %q", t.Value)
	}
	return stripBackticks(t.Value), nil
}

// isMRSymbolToken 报告 token 是否可作为模式变量符号（标识符/反引号标识符/被归为关键字的标识符词）。
func isMRSymbolToken(t Token) bool {
	return t.Type == TokenQuotedIdent || isMRIdentLike(t)
}

// parseMRDuration 解析 WITHIN 时长：'5s' / 5 SECONDS / 100 MS 等。
func (p *Parser) parseMRDuration() (time.Duration, error) {
	t := p.lexer.NextToken()
	if t.Type == TokenString {
		s := strings.Trim(t.Value, "'\"")
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("invalid WITHIN duration %q: %w", s, err)
		}
		return d, nil
	}
	if t.Type != TokenNumber {
		return 0, fmt.Errorf("expected WITHIN duration, got %q", t.Value)
	}
	n, err := strconv.ParseFloat(t.Value, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid WITHIN number %q: %w", t.Value, err)
	}
	unitTok := p.lexer.NextToken()
	d, ok := durationUnit(unitTok.Value, n)
	if !ok {
		return 0, fmt.Errorf("unknown WITHIN unit %q", unitTok.Value)
	}
	return d, nil
}

func durationUnit(unit string, n float64) (time.Duration, bool) {
	switch strings.ToUpper(unit) {
	case "NS", "NANO", "NANOS", "NANOSECOND", "NANOSECONDS":
		return time.Duration(n), true
	case "US", "MICRO", "MICROS", "MICROSECOND", "MICROSECONDS":
		return time.Duration(n * float64(time.Microsecond)), true
	case "MS", "MILLI", "MILLIS", "MILLISECOND", "MILLISECONDS":
		return time.Duration(n * float64(time.Millisecond)), true
	case "S", "SEC", "SECS", "SECOND", "SECONDS":
		return time.Duration(n * float64(time.Second)), true
	case "M", "MIN", "MINS", "MINUTE", "MINUTES":
		return time.Duration(n * float64(time.Minute)), true
	case "H", "HR", "HRS", "HOUR", "HOURS":
		return time.Duration(n * float64(time.Hour)), true
	}
	return 0, false
}

// readMRUntilAS 收集表达式到顶层 AS（不消费 AS）。
func (p *Parser) readMRUntilAS() (string, error) {
	var parts []string
	depth := 0
	for i := 0; i < 1000; i++ {
		snap := p.lexer.save()
		t := p.lexer.NextToken()
		// AS 被词法器归为 TokenAS（关键字），非 TokenIdent；顶层 AS 终止表达式收集。
		if depth == 0 && t.Type == TokenAS {
			p.lexer.restore(snap)
			return strings.Join(parts, " "), nil
		}
		switch t.Type {
		case TokenLParen, TokenLBrace:
			depth++
		case TokenRParen, TokenRBrace:
			depth--
		}
		parts = append(parts, t.Value)
	}
	return "", errors.New("MEASURES expression too long (missing AS)")
}

// readMRExpr 收集表达式到顶层 ',' 或 ')' 或子句关键字（不消费终止符）。
func (p *Parser) readMRExpr() (string, error) {
	var parts []string
	depth := 0
	for i := 0; i < 1000; i++ {
		snap := p.lexer.save()
		t := p.lexer.NextToken()
		if depth == 0 {
			if t.Type == TokenRParen || t.Type == TokenComma {
				p.lexer.restore(snap)
				return strings.Join(parts, " "), nil
			}
			if t.Type == TokenIdent && isMRClauseKeyword(t.Value) {
				p.lexer.restore(snap)
				return strings.Join(parts, " "), nil
			}
		}
		switch t.Type {
		case TokenLParen, TokenLBrace:
			depth++
		case TokenRParen, TokenRBrace:
			depth--
		}
		parts = append(parts, t.Value)
	}
	return "", errors.New("expression too long in MATCH_RECOGNIZE")
}
