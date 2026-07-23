package rsql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rulego/streamsql/types"
)

// isMRClauseKeyword reports whether ident is the starting key for MATCH_RECOGNIZE clauses (used to terminate expression collection).
func isMRClauseKeyword(v string) bool {
	switch strings.ToUpper(v) {
	case "PARTITION", "ORDER", "MEASURES", "ONE", "ALL", "AFTER",
		"PATTERN", "DEFINE", "SUBSET", "WITHIN":
		return true
	}
	return false
}

// peekToken prefetchs a token without consumption (based on lexer save/restore).
func (p *Parser) peekToken() Token {
	snap := p.lexer.save()
	t := p.lexer.NextToken()
	p.lexer.restore(snap)
	return t
}

// parseMatchRecognize parses the optional MATCH_RECOGNIZE(...) clause after FROM.
// Recognize complete grammar; Unrealized clauses give clear errors. Agreement: Lexer enters before MATCH; If not MATCH, it won't move.
// parseMatchRecognize parses the optional MATCH_RECOGNIZE(...) clause after FROM.
// Recognize complete grammar; Unrealized clauses give clear errors. Agreement: Lexer enters before MATCH_RECOGNIZE; Do not move any keywords that are not the same keywords.
// Note: The lexicon reads MATCH_RECOGNIZE as a single identifier token (underline conjunction).
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
			break // MATCH_RECOGNIZE End
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
			// SQL standard: ONE ROW PER MATCH
			if err := p.expectRowPerMatch("ROW"); err != nil {
				return err
			}
			spec.RowsPerMatch = types.RowsPerMatchOne
		case "ALL":
			// SQL standard: ALL ROWS PER MATCH (PLURAL)
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

	// Pattern/OrderBy is required in ToStreamConfig validation (its errors are not swallowed by parsing recovery).
	stmt.MatchRecognize = spec
	return nil
}

// expectKeyword consumes an ident token that matches a value (case-insensitive).
func (p *Parser) expectKeyword(want string) error {
	t := p.lexer.NextToken()
	if !strings.EqualFold(t.Value, want) {
		return fmt.Errorf("expected %s, got %q", want, t.Value)
	}
	return nil
}

// readIdentList reads the comma-separated identifier list (without backquotes), and stops at the non-comma terminator (no terminator is consumed).
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

// readMROrderBy reads the ORDER BY field[ASC| DESC],... Ends with a non-comma termination.
func (p *Parser) readMROrderBy() ([]types.OrderByField, error) {
	var obs []types.OrderByField
	for {
		t := p.lexer.NextToken()
		if !isMRSymbolToken(t) {
			return nil, fmt.Errorf("expected ORDER BY field, got %q", t.Value)
		}
		f := types.OrderByField{Expression: stripBackticks(t.Value), Direction: types.SortAsc}
		// Optional ASC/DESC
		snap := p.lexer.save()
		dir := p.lexer.NextToken()
		if strings.EqualFold(dir.Value, "DESC") {
			f.Direction = types.SortDesc
		} else if strings.EqualFold(dir.Value, "ASC") {
			// Default
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

// expectRowPerMatch Consumption "<rowKeyword> PER MATCH" (rowKeyword is ROW or ROWS).
func (p *Parser) expectRowPerMatch(rowKeyword string) error {
	for _, want := range []string{rowKeyword, "PER", "MATCH"} {
		if err := p.expectKeyword(want); err != nil {
			return err
		}
	}
	return nil
}

// readMRMeasures Read MEASURES <expr> AS<alias>,... Ends with a non-comma termination.
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

// readMRDefines Read DEFINE <sym> AS<cond>,... Ends with a non-comma termination.
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

// readMRSubsets reads SUBSET <name> = (sym,...),... Ends with a non-comma termination.
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
		ss = append(ss, types.MatchSubset{Name: stripBackticks(nameTok.Value), Symbols: syms})
		snap := p.lexer.save()
		sep := p.lexer.NextToken()
		if sep.Type != TokenComma {
			p.lexer.restore(snap)
			return ss, nil
		}
	}
}

// readMRAfterMatchSkip parses AFTER MATCH SKIP (...).
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
			// TO <symbol>(No FIRST/LAST)
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

// isMRSymbolToken reports whether tokens can be used as pattern variable symbols (identifiers/backquotes/identifiers classified as keywords).
func isMRSymbolToken(t Token) bool {
	return t.Type == TokenQuotedIdent || isMRIdentLike(t)
}

// parseMRDuration WITHIN duration: '5s' / 5 SECONDS / 100 MS, etc.
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

// readMRUntilAS collects expressions to the top-level AS (no AS is consumed).
func (p *Parser) readMRUntilAS() (string, error) {
	var parts []string
	depth := 0
	for i := 0; i < 1000; i++ {
		snap := p.lexer.save()
		t := p.lexer.NextToken()
		// AS is classified as TokenAS (keyword) by the lexical editor, not as TokenIdent; Top-level AS termination expression collection.
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

// readMRExpr collects expressions to the top-level ',' or ')' or clause keywords (without consuming terminators).
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
