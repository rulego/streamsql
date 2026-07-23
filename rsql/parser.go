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

// The parser constants are constant
const (
	// MaxRecursionDepth defines the maximum recursive depth of the expectTokenWithDepth method
	// Used to prevent infinite recursion
	MaxRecursionDepth = 30

	// MaxSelectFields defines the maximum number of fields allowed in the SELECT clause
	MaxSelectFields = 300
)

// tokenTypeNames defines the mapping table from token type to name
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
	TokenQuestion:    "?",
	TokenPipe:        "|",
	TokenLBrace:      "{",
	TokenRBrace:      "}",
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

// GetErrors retrieves all errors during the parsing process
func (p *Parser) GetErrors() []*ParseError {
	return p.errorRecovery.GetErrors()
}

// HasErrors checks for errors
func (p *Parser) HasErrors() bool {
	return p.errorRecovery.HasErrors()
}

// expectToken: Expects a specific type of token
func (p *Parser) expectToken(expected TokenType, context string) (Token, error) {
	return p.expectTokenWithDepth(expected, context, 0)
}

// expectTokenWithDepth: Expects a specific type of token, with a recursive depth limit
// Uses the maximum configurable recursion depth to prevent infinite recursion, providing better error handling and recovery mechanisms
func (p *Parser) expectTokenWithDepth(expected TokenType, context string, depth int) (Token, error) {
	// Prevents infinite recursion by using the maximum configurable recursion depth
	if depth > MaxRecursionDepth {
		tok := p.lexer.NextToken()
		err := p.createTokenError(tok, expected, context, "maximum recursion depth exceeded")
		return tok, err
	}

	tok := p.lexer.NextToken()
	if tok.Type != expected {
		err := p.createTokenError(tok, expected, context, "")
		p.errorRecovery.AddError(err)

		// Attempts and errors are restored, but recursion depth is limited
		if p.shouldAttemptRecovery(err, depth) {
			return p.expectTokenWithDepth(expected, context, depth+1)
		}

		return tok, err
	}
	return tok, nil
}

// getTokenTypeName gets the token type name
// Use mapping tables to improve performance and maintainability
func (p *Parser) getTokenTypeName(tokenType TokenType) string {
	if name, exists := tokenTypeNames[tokenType]; exists {
		return name
	}
	return "unknown"
}

// createTokenError Creates a standardized token error
// Provides a unified error creation logic for easy maintenance and scalability
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

// shouldAttemptRecovery determines whether a false recovery attempt should be made
// Make intelligent decisions based on error types and recursive depth
func (p *Parser) shouldAttemptRecovery(err *ParseError, depth int) bool {
	// If the maximum recursive depth is approached, no attempts to recover are made
	if depth >= MaxRecursionDepth-1 {
		return false
	}

	// Check whether errors can be recovered, and the error recovery mechanism allows for recovery
	return err.IsRecoverable() && p.errorRecovery.RecoverFromError(ErrorTypeUnexpectedToken)
}

func (p *Parser) Parse() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	// Parse the SELECT clause - returns directly for specific critical errors
	if err := p.parseSelect(stmt); err != nil {
		// Check for critical grammatical errors that should be stopped for further parsing
		if strings.Contains(err.Error(), "Expected SELECT") {
			// The SELECT keyword error is fatal and returns directly
			return nil, p.createDetailedError(err)
		}

		// Check whether there are specific critical error patterns that should not be restored
		// Only when the query appears in a pattern like "SELECT FROM table WHERE" does it return an error directly
		if strings.Contains(err.Error(), "no fields specified") {
			// Check if the FROM keyword is immediately followed by SELECT
			nextTok := p.lexer.lookupIdent(p.lexer.readPreviousIdentifier())
			if nextTok.Type == TokenFROM {
				// Further check: If there are other content after it (such as WHERE, GROUP, etc.), error recovery is allowed
				// Only when the query is simply in the "SELECT FROM table WHERE" mode will an error be returned directly
				if !strings.Contains(p.input, "WHERE") || !strings.Contains(p.input, "GROUP") {
					return nil, p.createDetailedError(err)
				}
			}
		}

		if parseErr, ok := err.(*ParseError); ok {
			p.errorRecovery.AddError(parseErr)
		}
		// For other errors, continue to try to parse other parts
	}

	// Parsing the FROM clause
	if err := p.parseFrom(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// Parsing the JOIN clause (stream-table JOIN, v0.5)
	if err := p.parseJoin(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// Parsing MATCH_RECOGNIZE Clause (CEP, after FROM, before WHERE)
	if err := p.parseMatchRecognize(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// Parse the WHERE clause
	if err := p.parseWhere(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// Parse the GROUP BY clause
	if err := p.parseGroupBy(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// Parse the HAVING clause
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

	// Parse the ORDER BY clause
	if err := p.parseOrderBy(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// Parse the LIMIT clause
	if err := p.parseLimit(stmt); err != nil {
		if !p.errorRecovery.RecoverFromError(ErrorTypeSyntax) {
			return nil, p.createDetailedError(err)
		}
	}

	// If there is an error but can be recovered, partial parsing results and error messages are returned
	if p.errorRecovery.HasErrors() {
		return stmt, p.createCombinedError()
	}

	return stmt, nil
}

// isKeyword checks whether the given string is an SQL keyword
// Use predefined keyword mapping tables for quick lookups
// Parameter: word - the string to check
// Return: Returns true if key, otherwise returns false
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

// createDetailedError Creates detailed error information
// Adds contextual information for ParseError-type errors to facilitate debugging and error localization
// Parameter: err - Original error
// Return: Error containing detailed contextual information
func (p *Parser) createDetailedError(err error) error {
	if parseErr, ok := err.(*ParseError); ok {
		parseErr.Context = FormatErrorContext(p.input, parseErr.Position, 20)
		return parseErr
	}
	return err
}

// createCombinedError creates a combined error message
// Merging multiple parsing errors into a unified error message makes it easier for users to understand all issues
// Return: Combination error containing all error messages
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

// parseSelect parses the SELECT clause, including the field list, DISTINCT keywords, and aliases
// Supports SELECT * syntax and provides field limit to prevent infinite loops
// Parameter: stmt - The SelectStatement structure to be filled
// Return: Error encountered during parsing; if successful, returns nil
func (p *Parser) parseSelect(stmt *SelectStatement) error {
	// Validate if first token is SELECT
	firstToken := p.lexer.NextToken()
	if firstToken.Type != TokenSELECT {
		// Directly returns grammar errors
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
		currentToken = p.lexer.NextToken() // Spend DISTINCT to move to the next token
	}

	// Check if it is a SELECT * query (the lexist classifies * as TokenAsterisk, not TokenIdent)
	if currentToken.Type == TokenAsterisk {
		stmt.SelectAll = true
		// Add a special field tag SELECT *
		stmt.Fields = append(stmt.Fields, Field{Expression: "*"})

		// Spend *token and check the next token
		currentToken = p.lexer.NextToken()

		// If the next token is FROM or EOF, SELECT * parse is completed
		if currentToken.Type == TokenFROM || currentToken.Type == TokenEOF {
			return nil
		}

		// If it is not FROM/EOF, continue with the normal field parsing process
	}

	// Set a maximum field limit to prevent endless loops
	fieldCount := 0

	for {
		fieldCount++
		// Safety check: prevent infinite loops
		if fieldCount > MaxSelectFields {
			return errors.New("select field list parsing exceeded maximum fields, possible syntax error")
		}

		var expr strings.Builder
		parenthesesLevel := 0 // Track parentheses nested hierarchies

		// Set the maximum expression length to prevent infinite loops
		maxExprParts := 100
		exprPartCount := 0

		for {
			exprPartCount++
			// Safety check: prevents endless loops
			if exprPartCount > maxExprParts {
				return errors.New("select field expression parsing exceeded maximum length, possible syntax error")
			}

			// Track the parenthesis level
			if currentToken.Type == TokenLParen {
				parenthesesLevel++
			} else if currentToken.Type == TokenRParen {
				parenthesesLevel--
			}

			// Commas are only considered field separators when the parenthesis level is 0
			if parenthesesLevel == 0 && (currentToken.Type == TokenFROM || currentToken.Type == TokenComma || currentToken.Type == TokenAS || currentToken.Type == TokenEOF || currentToken.Type == TokenOVER) {
				break
			}

			// If not the first token, add a space separator
			// But note special cases: certain tokens should not be spaced between tokens
			if expr.Len() > 0 {
				shouldAddSpace := true

				// Retrieve information about the previous token
				exprStr := expr.String()
				lastChar := exprStr[len(exprStr)-1:]

				// No spaces are added in the following cases:
				// 1. Between the function name and the left parenthesis
				// 2. Between identifiers and numbers (e.g., x1, y1)
				// 3. Between numbers and identifiers
				// 4. After the left parentheses
				// 5. Before the right parentheses
				// 6. Array index correlation: [ before, [ after ]
				// 7. Before and after the dot mark
				if (currentToken.Type == TokenLParen || currentToken.Type == TokenLBracket) && lastChar != " " && lastChar != "(" && lastChar != "[" {
					// No spaces are added between function names/array names and left parentheses/left square brackets
					shouldAddSpace = false
				} else if lastChar == "(" || lastChar == "[" || currentToken.Type == TokenRParen || currentToken.Type == TokenRBracket {
					// No spaces are added after left or right square brackets
					shouldAddSpace = false
				} else if currentToken.Type == TokenDot || lastChar == "." {
					// No spaces are added before or after the dot sign
					shouldAddSpace = false
				} else if len(exprStr) > 0 && currentToken.Type == TokenNumber {
					// Check whether the previous character is a letter (part of the identifier) and that there are no spaces before it
					// This mainly handles identifiers like x1 and y1, but excludes cases like THEN 1 and ELSE 0
					if ((lastChar[0] >= 'a' && lastChar[0] <= 'z') || (lastChar[0] >= 'A' && lastChar[0] <= 'Z') || lastChar[0] == '_') &&
						!strings.HasSuffix(exprStr, " ") {
						// Further check: If the preceding word is an SQL keyword, you should add a space
						words := strings.Fields(exprStr)
						if len(words) > 0 {
							lastWord := strings.ToUpper(words[len(words)-1])
							// If it's a keyword, you should add a space
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
					// Check whether the previous character is a number and that there are no spaces in front
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

		// Parse the optional OVER clause (parsing function). OVER is recognized under breakpoint conditions,
		// Here: currentToken == TokenOVER; parseOverClause consumes OVER(...), returns)
		// Already read, NextToken retrieves subsequent tokens (AS/FROM/Comma/EOF).
		if currentToken.Type == TokenOVER {
			over, err := p.parseOverClause()
			if err != nil {
				return err
			}
			field.OverSpec = over
			currentToken = p.lexer.NextToken()
		}

		// Handling aliases
		if currentToken.Type == TokenAS {
			field.Alias = p.lexer.NextToken().Value
			currentToken = p.lexer.NextToken()
		}

		// If the expression is empty, skip this field
		if field.Expression != "" {
			// Verify the function in the expression
			validator := NewFunctionValidator(p.errorRecovery)
			pos, _, _ := p.lexer.GetPosition()
			validator.ValidateExpression(field.Expression, pos-len(field.Expression))

			stmt.Fields = append(stmt.Fields, field)
		}

		if currentToken.Type == TokenFROM || currentToken.Type == TokenEOF {
			break
		}

		if currentToken.Type != TokenComma {
			// If it's not a comma, then it should be a grammatical error
			return fmt.Errorf("unexpected token %v, expected comma or FROM", currentToken.Value)
		}

		currentToken = p.lexer.NextToken()
	}

	// Make sure to have at least one field
	if len(stmt.Fields) == 0 {
		return errors.New("no fields specified in SELECT clause")
	}

	return nil
}

func (p *Parser) parseWhere(stmt *SelectStatement) error {
	var conditions []string
	current := p.lexer.NextToken() // Obtain the next token
	if current.Type != TokenWHERE {
		// If not WHERE (not HERE), revert the token position
		return nil
	}

	// Set max iterations limit to prevent infinite loops
	maxIterations := 100
	iterations := 0

	for {
		iterations++
		// Safety check: prevents endless loops
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

	// Validate functions in WHERE condition. Parser function calls (including OVER) are first replaced with placeholders,
	// Avoid OVER being mistakenly identified as an unknown function; stmt.Condition retains the original text and is extracted by ToStreamConfig.
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
	nextTok := p.lexer.NextToken() // Read the next token, which should be '('
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

// parseOverClause parsing the OVER clause of the analysis function: OVER ([PARTITION BY...] [WHEN...]).
// Only PARTITION BY and WHEN are supported; ORDER BY / ROWS / BETWEEN will all show errors.
// Convention: When called, currentToken == TokenOVER (read), lexer is to be read as '('; On the way back
// OVER(...) has been fully consumed, and the ')' in '(' is the last token read; the caller needs NextToken to retrieve the following.
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

// parseOverPartitionBy parses PARTITION BY <field>[, <field>...]. PARTITION has been read.
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
		// Remove the quotation marks
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
		p.lexer.restore(snap) // Fallback (WHEN or ')'), and pass it to the upper loop
		return nil
	}
}

// parseOverWhen parses WHEN <predicate>until) or PARTITION. WHEN has been read.
// Tracking parenthesis depth: WHEN function calls in the predicate (such as had_changed(true, status)), parentheses must be counted,
// Only when the depth is zeroed ')' does the OVER clause end.
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
	// Handle strings wrapped in quotes
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
		"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "WITH",
		"MATCH_RECOGNIZE": // The clause starting point (the lexical tool reads MATCH_RECOGNIZE as a single identifier) cannot be consumed as a source alias
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
		p.lexer.NextToken() // Skip BY
	}

	// If there is no GROUP BY clause and no window function, it returns directly
	if !hasGroupBy && !hasWindowFunction {
		return nil
	}

	// Set a maximum limit to prevent infinite loops
	maxIterations := 100
	iterations := 0

	var limitToken *Token // Save LIMIT tokens for future processing

	// Cumulative subgroup terms: track the depth of parentheses and treat function expressions (such as upper(device)) as the overall term,
	// The top layer is separated by commas. collapseSpacesOutsideQuotes normalization (multiple tokens read by parser with spaces).
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
		// Safety check: prevents endless loops
		if iterations > maxIterations {
			return errors.New("group by clause parsing exceeded maximum iterations, possible syntax error")
		}

		tok := p.lexer.NextToken()
		if tok.Type == TokenWITH || tok.Type == TokenOrder || tok.Type == TokenEOF ||
			tok.Type == TokenHAVING || tok.Type == TokenLIMIT {
			// If it is a LIMIT token, save it for parseLimit to handle
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
		// Top-level (outside parentheses) window/global window/OVER clause: wrap the current item before processing.
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
				// GROUP BY window's OVER(...) clause (only WHEN input gating). Verification is done in ToStreamConfig
				// (parseGroupBy return errors will be swallowed as recoverable errors by errorRecovery).
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
		// Track parenthesis depth (function call parameters) to accumulate tokens into the current group.
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

	// If you encounter a LIMIT token, handle it directly here
	if limitToken != nil {
		return p.handleLimitToken(stmt, *limitToken)
	}
	return nil
}

func (p *Parser) parseWith(stmt *SelectStatement) error {
	// Check the current token; if it is not WITH, it returns
	tok := p.lexer.lookupIdent(p.lexer.readPreviousIdentifier())
	if tok.Type != TokenWITH {
		return nil // No WITH clause, not an error
	}

	p.lexer.NextToken() // Skip (

	// Set a maximum limit to prevent infinite loops
	maxIterations := 100
	iterations := 0

	for p.lexer.peekChar() != ')' {
		iterations++
		// Safety check: prevents endless loops
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

// handleLimitToken handles LIMIT tokens encountered in parseGroupBy
func (p *Parser) handleLimitToken(stmt *SelectStatement, limitToken Token) error {
	// The next token should be a number
	tok := p.lexer.NextToken()
	if tok.Type == TokenNumber {
		// Convert numeric strings to integers
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
		// Handling negative numbers: "-5"
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
		// Handling non-numeric cases: such as "abc"
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

// parseLimit parses the LIMIT clause
func (p *Parser) parseLimit(stmt *SelectStatement) error {
	// If LIMIT has already been set (possibly processed in parseGroupBy), skip it
	if stmt.Limit > 0 {
		return nil
	}

	// Find the true LIMIT keyword in the token stream to avoid mismatching substrings in identifiers/string literals
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

	// Find the content after LIMIT
	afterLimit := strings.TrimSpace(p.input[limitIndex+5:]) // Skip "LIMIT"
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

	// Break down the first word (it should be a number)
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

	// Handle negative numbers
	if strings.HasPrefix(limitValue, "-") {
		parseErr := CreateMissingTokenError("number", limitIndex+6)
		parseErr.Message = "LIMIT must be followed by an integer"
		parseErr.Context = "LIMIT clause"
		parseErr.Suggestions = []string{"Use a positive integer, e.g., LIMIT 10"}
		p.errorRecovery.AddError(parseErr)
		return parseErr
	}

	// Try converting to an integer
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

// parseOrderBy parses the ORDER BY clause. Scan the true TokenOrder with an independent lexer,
// Avoid mismatching the "ORDER" substring in identifiers/string literals (the same robust approach as parseLimit).
// v0.5: Each sort key is the result column name (identifier, can include dot paths), followed by optional ASC/DESC, separated by commas.
func (p *Parser) parseOrderBy(stmt *SelectStatement) error {
	// Use independent lexer to locate the true ORDER keyword
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
		return nil // No ORDER BY clause
	}

	// Re-lex from ORDER to parse BY and the field list
	fieldLexer := NewLexer(p.input[orderPos+len("ORDER"):])
	fieldLexer.SetErrorRecovery(NewErrorRecovery(nil))

	if tok := fieldLexer.NextToken(); tok.Type != TokenBY {
		// If the ORDER is not BY, it should not be treated as an ORDER BY (for example, column names containing ORDER substrings have been scanned and excluded by the token above).
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
			// ASC/DESC as direction keywords (they do not have independent tokens and are identified by identifier values)
			if tok.Type == TokenIdent {
				upper := strings.ToUpper(tok.Value)
				if upper == "ASC" || upper == "DESC" {
					if upper == "DESC" {
						dir = types.SortDesc
					}
					// The direction has been consumed, followed by a comma or clause
					sep := fieldLexer.NextToken()
					if sep.Type == TokenComma {
						advance = true
					} else {
						done = true
					}
					break
				}
			}
			// Add token value (no separator, so the a.b / backtick fields can be correctly reconstructed)
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

// parseHaving parses the HAVING clause
func (p *Parser) parseHaving(stmt *SelectStatement) error {
	// View the current token
	tok := p.lexer.lookupIdent(p.lexer.readPreviousIdentifier())
	if tok.Type != TokenHAVING {
		return nil // No HAVING clause, not an error
	}

	// Set a maximum limit to prevent infinite loops
	maxIterations := 100
	iterations := 0

	var conditions []string
	for {
		iterations++
		// Safety check: prevents endless loops
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

// Parse is a packet-level Parse function used to parse SQL strings and return configurations and conditions
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

	// Reject malformed GROUP BY. Using the original stmt.GroupBy(extractGroupFields before filtering),
	// Otherwise, the "Conservative Aggregation with Parentheses" in isAggregationFunction will catch the misspelled window function
	// (e.g., InvalidWindow('5s')) When the aggregation is discarded, config.GroupFields is null, validation is missing.
	// Valid grouped items: bare column names, or expressions with registered scalar functions at the top (e.g., upper(device)).
	// Quotation marks like artifact or unregistered function → are considered misspelled window function leaks, rejected.
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
