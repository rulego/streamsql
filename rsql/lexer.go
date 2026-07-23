package rsql

import (
	"fmt"
	"strings"
)

type TokenType int

const (
	TokenEOF TokenType = iota
	TokenIdent
	TokenNumber
	TokenString
	TokenQuotedIdent // Backquote identifier
	TokenComma
	TokenLParen
	TokenRParen
	TokenPlus
	TokenMinus
	TokenAsterisk
	TokenSlash
	TokenEQ
	TokenNE
	TokenGT
	TokenLT
	TokenGE
	TokenLE
	TokenAND
	TokenOR
	TokenSELECT
	TokenFROM
	TokenWHERE
	TokenGROUP
	TokenBY
	TokenAS
	TokenTumbling
	TokenSliding
	TokenCounting
	TokenSession
	TokenGlobal
	TokenWindow
	TokenTrigger
	TokenWITH
	TokenTimestamp
	TokenTimeUnit
	TokenMaxOutOfOrderness
	TokenAllowedLateness
	TokenIdleTimeout
	TokenStateTTL
	TokenOrder
	TokenDISTINCT
	TokenLIMIT
	TokenHAVING
	TokenLIKE
	TokenIS
	TokenNULL
	TokenNOT
	// CASE expression-related tokens
	TokenCASE
	TokenWHEN
	TokenTHEN
	TokenELSE
	TokenEND
	// Array indexes related tokens
	TokenLBracket
	TokenRBracket
	// Analyze the token related to the OVER clause of the function
	TokenOVER
	TokenPARTITION
	// Point token
	TokenDot
	// MATCH_RECOGNIZE PATTERN regular syntax punctuation (PATTERN internal consumption only)
	TokenQuestion // ?
	TokenPipe     // |
	TokenLBrace   // {
	TokenRBrace   // }
)

type Token struct {
	Type   TokenType
	Value  string
	Pos    int
	Line   int
	Column int
}

type Lexer struct {
	input         string
	pos           int
	readPos       int
	ch            byte
	line          int
	column        int
	errorRecovery *ErrorRecovery
}

func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 0,
	}
	l.readChar()
	return l
}

// SetErrorRecovery sets the error recovery instance
func (l *Lexer) SetErrorRecovery(er *ErrorRecovery) {
	l.errorRecovery = er
}

// GetPosition retrieves the current location information
func (l *Lexer) GetPosition() (int, int, int) {
	return l.pos, l.line, l.column
}

// lexerSnapshot captures the mutable lexer state so a token read can be
// speculatively undone. All fields are package-private, so this stays in-package.
type lexerSnapshot struct {
	pos, readPos, line, column int
	ch                         byte
}

// save returns a snapshot of the current lexer position.
func (l *Lexer) save() lexerSnapshot {
	return lexerSnapshot{pos: l.pos, readPos: l.readPos, line: l.line, column: l.column, ch: l.ch}
}

// restore rewinds the lexer to a previously saved snapshot.
func (l *Lexer) restore(s lexerSnapshot) {
	l.pos = s.pos
	l.readPos = s.readPos
	l.line = s.line
	l.column = s.column
	l.ch = s.ch
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	// Record the starting position of the token
	tokenPos := l.pos
	tokenLine := l.line
	tokenColumn := l.column

	switch l.ch {
	case 0:
		return Token{Type: TokenEOF, Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case ',':
		l.readChar()
		return Token{Type: TokenComma, Value: ",", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '(':
		l.readChar()
		return Token{Type: TokenLParen, Value: "(", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case ')':
		l.readChar()
		return Token{Type: TokenRParen, Value: ")", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '[':
		l.readChar()
		return Token{Type: TokenLBracket, Value: "[", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case ']':
		l.readChar()
		return Token{Type: TokenRBracket, Value: "]", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '.':
		l.readChar()
		return Token{Type: TokenDot, Value: ".", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '?':
		l.readChar()
		return Token{Type: TokenQuestion, Value: "?", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '|':
		l.readChar()
		return Token{Type: TokenPipe, Value: "|", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '{':
		l.readChar()
		return Token{Type: TokenLBrace, Value: "{", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '}':
		l.readChar()
		return Token{Type: TokenRBrace, Value: "}", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '+':
		l.readChar()
		return Token{Type: TokenPlus, Value: "+", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '-':
		// Check if it's negative
		if l.peekChar() != 0 && isDigit(l.peekChar()) {
			// This is a negative number, read the entire number
			l.readChar() // Skip the minus sign
			number := "-" + l.readNumber()
			// Verify the digital format
			if !l.isValidNumber(number) && l.errorRecovery != nil {
				err := CreateLexicalError(fmt.Sprintf("Invalid number format: %s", number), tokenPos, 0)
				err.Type = ErrorTypeInvalidNumber
				l.errorRecovery.AddError(err)
			}
			return Token{Type: TokenNumber, Value: number, Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
		}
		// This is a minus operator
		l.readChar()
		return Token{Type: TokenMinus, Value: "-", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '*':
		l.readChar()
		return Token{Type: TokenAsterisk, Value: "*", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '/':
		l.readChar()
		return Token{Type: TokenSlash, Value: "/", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '=':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenEQ, Value: "==", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
		}
		l.readChar()
		return Token{Type: TokenEQ, Value: "=", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenGE, Value: ">=", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
		}
		l.readChar()
		return Token{Type: TokenGT, Value: ">", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenLE, Value: "<=", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
		}
		l.readChar()
		return Token{Type: TokenLT, Value: "<", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenNE, Value: "!=", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
		}
		// Handle invalid '!' characters
		if l.errorRecovery != nil {
			err := CreateLexicalErrorWithPosition("Invalid character '!', did you mean '!='?", tokenPos, tokenLine, tokenColumn, l.ch)
			l.errorRecovery.AddError(err)
		}
		l.readChar()
		return l.NextToken() // Skip invalid characters and continue parsing
	case '\'':
		return l.readStringToken(tokenPos, tokenLine, tokenColumn)
	case '"':
		return l.readStringToken(tokenPos, tokenLine, tokenColumn)
	case '`':
		return l.readQuotedIdentToken(tokenPos, tokenLine, tokenColumn)
	}

	if isLetter(l.ch) {
		ident := l.readIdentifier()
		token := l.lookupIdent(ident)
		token.Pos = tokenPos
		token.Line = tokenLine
		token.Column = tokenColumn
		return token
	}

	if isDigit(l.ch) {
		number := l.readNumber()
		// Verify the digital format
		if !l.isValidNumber(number) && l.errorRecovery != nil {
			err := CreateLexicalError(fmt.Sprintf("Invalid number format: %s", number), tokenPos, 0)
			err.Type = ErrorTypeInvalidNumber
			l.errorRecovery.AddError(err)
		}
		return Token{Type: TokenNumber, Value: number, Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	}

	// Handling unrecognizable characters
	if l.ch != 0 {
		if l.errorRecovery != nil {
			err := CreateLexicalErrorWithPosition(fmt.Sprintf("Unexpected character '%c'", l.ch), tokenPos, tokenLine, tokenColumn, l.ch)
			l.errorRecovery.AddError(err)
		}
		l.readChar()
		return l.NextToken() // Skip invalid characters and continue parsing
	}

	return Token{Type: TokenEOF, Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}

	// Update location information
	if l.ch == '\n' {
		l.line++
		l.column = 0
	} else {
		l.column++
	}

	l.pos = l.readPos
	l.readPos++
}

func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

func (l *Lexer) readIdentifier() string {
	position := l.pos
	// Only handle basic identifiers and dots (for nested field access)
	// Array indexing (brackets) should be handled by the parser, not the lexer
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '.' {
		l.readChar()
	}
	return l.input[position:l.pos]
}

func (l *Lexer) readPreviousIdentifier() string {
	// Save your current location
	endPos := l.pos

	// Move backward until finding a non-letter character or reaching the input start
	startPos := endPos - 1
	for startPos >= 0 && isLetter(l.input[startPos]) {
		startPos--
	}

	// Adjust to the position of the first letter character
	startPos++

	// If a valid identifier is found, return it
	if startPos < endPos {
		return l.input[startPos:endPos]
	}

	return ""
}

func (l *Lexer) readNumber() string {
	pos := l.pos
	for isDigit(l.ch) || l.ch == '.' {
		l.readChar()
	}
	return l.input[pos:l.pos]
}

func (l *Lexer) readString() string {
	quoteChar := l.ch // Record the quote type (single or double quote)
	startPos := l.pos // Record the start position (including the quote)
	l.readChar()      // Skip the opening quotation marks

	for l.ch != quoteChar && l.ch != 0 {
		l.readChar()
	}

	if l.ch == quoteChar {
		l.readChar() // Skip the closing quotes
	}

	// Return the complete string including quotes
	return l.input[startPos:l.pos]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) lookupIdent(ident string) Token {
	upperIdent := strings.ToUpper(ident)
	switch upperIdent {
	case "SELECT":
		return Token{Type: TokenSELECT, Value: ident}
	case "FROM":
		return Token{Type: TokenFROM, Value: ident}
	case "WHERE":
		return Token{Type: TokenWHERE, Value: ident}
	case "GROUP":
		return Token{Type: TokenGROUP, Value: ident}
	case "BY":
		return Token{Type: TokenBY, Value: ident}
	case "AS":
		return Token{Type: TokenAS, Value: ident}
	case "OR":
		return Token{Type: TokenOR, Value: ident}
	case "AND":
		return Token{Type: TokenAND, Value: ident}
	case "TUMBLINGWINDOW":
		return Token{Type: TokenTumbling, Value: ident}
	case "SLIDINGWINDOW":
		return Token{Type: TokenSliding, Value: ident}
	case "COUNTINGWINDOW":
		return Token{Type: TokenCounting, Value: ident}
	case "SESSIONWINDOW":
		return Token{Type: TokenSession, Value: ident}
	case "GLOBAL":
		return Token{Type: TokenGlobal, Value: ident}
	case "WINDOW":
		return Token{Type: TokenWindow, Value: ident}
	case "TRIGGER":
		return Token{Type: TokenTrigger, Value: ident}
	case "WITH":
		return Token{Type: TokenWITH, Value: ident}
	case "TIMESTAMP":
		return Token{Type: TokenTimestamp, Value: ident}
	case "TIMEUNIT":
		return Token{Type: TokenTimeUnit, Value: ident}
	case "MAXOUTOFORDERNESS":
		return Token{Type: TokenMaxOutOfOrderness, Value: ident}
	case "ALLOWEDLATENESS":
		return Token{Type: TokenAllowedLateness, Value: ident}
	case "IDLETIMEOUT":
		return Token{Type: TokenIdleTimeout, Value: ident}
	case "STATETTL":
		return Token{Type: TokenStateTTL, Value: ident}
	case "ORDER":
		return Token{Type: TokenOrder, Value: ident}
	case "DISTINCT":
		return Token{Type: TokenDISTINCT, Value: ident}
	case "LIMIT":
		return Token{Type: TokenLIMIT, Value: ident}
	case "HAVING":
		return Token{Type: TokenHAVING, Value: ident}
	case "LIKE":
		return Token{Type: TokenLIKE, Value: ident}
	case "IS":
		return Token{Type: TokenIS, Value: ident}
	case "NULL":
		return Token{Type: TokenNULL, Value: ident}
	case "NOT":
		return Token{Type: TokenNOT, Value: ident}
	// CASE expression-related keywords
	case "CASE":
		return Token{Type: TokenCASE, Value: ident}
	case "WHEN":
		return Token{Type: TokenWHEN, Value: ident}
	case "THEN":
		return Token{Type: TokenTHEN, Value: ident}
	case "ELSE":
		return Token{Type: TokenELSE, Value: ident}
	case "END":
		return Token{Type: TokenEND, Value: ident}
	case "OVER":
		return Token{Type: TokenOVER, Value: ident}
	case "PARTITION":
		return Token{Type: TokenPARTITION, Value: ident}
	default:
		// Check for common typos
		if l.errorRecovery != nil {
			l.checkForTypos(ident, upperIdent)
		}
		return Token{Type: TokenIdent, Value: ident}
	}
}

// checkForTypos checks for common spelling errors
func (l *Lexer) checkForTypos(original, upper string) {
	suggestions := make([]string, 0)

	switch upper {
	case "SELCT", "SELECCT", "SELET":
		suggestions = append(suggestions, "SELECT")
	case "FORM", "FRON", "FRMO":
		suggestions = append(suggestions, "FROM")
	case "WHER", "WHRE", "WEHRE":
		suggestions = append(suggestions, "WHERE")
	case "GROPU", "GRUP", "GRPUP":
		suggestions = append(suggestions, "GROUP")
	case "ODER", "ORDR", "OREDR":
		suggestions = append(suggestions, "ORDER")
	case "DSITINCT", "DISTINC", "DISTINT":
		suggestions = append(suggestions, "DISTINCT")
	}

	if len(suggestions) > 0 {
		err := &ParseError{
			Type:        ErrorTypeUnknownKeyword,
			Message:     fmt.Sprintf("Unknown keyword '%s'", original),
			Position:    l.pos,
			Line:        l.line,
			Column:      l.column,
			Token:       original,
			Suggestions: suggestions,
			Recoverable: true,
		}
		l.errorRecovery.AddError(err)
	}
}

// readStringToken reads the string token and handles the error
func (l *Lexer) readStringToken(pos, line, column int) Token {
	quoteChar := l.ch
	startPos := l.pos
	l.readChar() // Skip the opening quotation marks

	for l.ch != quoteChar && l.ch != 0 {
		l.readChar()
	}

	if l.ch == 0 {
		// Unterminated string
		if l.errorRecovery != nil {
			err := &ParseError{
				Type:        ErrorTypeUnterminatedString,
				Message:     "Unterminated string literal",
				Position:    startPos,
				Line:        line,
				Column:      column,
				Token:       string(quoteChar),
				Suggestions: []string{fmt.Sprintf("Add closing quote '%c'", quoteChar)},
				Recoverable: true,
			}
			l.errorRecovery.AddError(err)
		}
		value := l.input[startPos:l.pos]
		return Token{Type: TokenString, Value: value, Pos: pos, Line: line, Column: column}
	}

	if l.ch == quoteChar {
		l.readChar() // Skip the closing quotes
	}

	value := l.input[startPos:l.pos]
	return Token{Type: TokenString, Value: value, Pos: pos, Line: line, Column: column}
}

// readQuotedIdentToken reads the backquoted identifier token and handles the error
func (l *Lexer) readQuotedIdentToken(pos, line, column int) Token {
	startPos := l.pos
	l.readChar() // Skip the opening backquotes

	for l.ch != '`' && l.ch != 0 {
		l.readChar()
	}

	if l.ch == 0 {
		// Unterminated backtick identifier
		if l.errorRecovery != nil {
			err := &ParseError{
				Type:        ErrorTypeUnterminatedString,
				Message:     "Unterminated quoted identifier",
				Position:    startPos,
				Line:        line,
				Column:      column,
				Token:       "`",
				Suggestions: []string{"Add closing backtick '`'"},
				Recoverable: true,
			}
			l.errorRecovery.AddError(err)
		}
		value := l.input[startPos:l.pos]
		return Token{Type: TokenQuotedIdent, Value: value, Pos: pos, Line: line, Column: column}
	}

	if l.ch == '`' {
		l.readChar() // Skip the closing quotation marks
	}

	value := l.input[startPos:l.pos]
	return Token{Type: TokenQuotedIdent, Value: value, Pos: pos, Line: line, Column: column}
}

// isValidNumber verifies the numeric format
func (l *Lexer) isValidNumber(number string) bool {
	if number == "" {
		return false
	}

	// Handle negative numbers
	startIndex := 0
	if number[0] == '-' {
		if len(number) == 1 {
			return false // Only the minus sign
		}
		startIndex = 1
	}

	dotCount := 0
	for i := startIndex; i < len(number); i++ {
		ch := number[i]
		if ch == '.' {
			dotCount++
			if dotCount > 1 {
				return false // Multiple decimal points
			}
		} else if !isDigit(ch) {
			return false // Non-numeric characters
		}
	}

	// Check whether it starts or ends with a decimal point
	if number[startIndex] == '.' || number[len(number)-1] == '.' {
		return false
	}

	return true
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}
