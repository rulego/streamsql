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
	TokenWITH
	TokenTimestamp
	TokenTimeUnit
	TokenOrder
	TokenDISTINCT
	TokenLIMIT
	TokenHAVING
)

type Token struct {
	Type  TokenType
	Value string
	Pos   int
	Line  int
	Column int
}

type Lexer struct {
	input   string
	pos     int
	readPos int
	ch      byte
	line    int
	column  int
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

// SetErrorRecovery 设置错误恢复实例
func (l *Lexer) SetErrorRecovery(er *ErrorRecovery) {
	l.errorRecovery = er
}

// GetPosition 获取当前位置信息
func (l *Lexer) GetPosition() (int, int, int) {
	return l.pos, l.line, l.column
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	// 记录token开始位置
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
	case '+':
		l.readChar()
		return Token{Type: TokenPlus, Value: "+", Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	case '-':
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
		// 处理无效的 '!' 字符
		if l.errorRecovery != nil {
			err := CreateLexicalErrorWithPosition("Invalid character '!', did you mean '!='?", tokenPos, tokenLine, tokenColumn, l.ch)
			l.errorRecovery.AddError(err)
		}
		l.readChar()
		return l.NextToken() // 跳过无效字符，继续解析
	case '\'':
		return l.readStringToken(tokenPos, tokenLine, tokenColumn)
	case '"':
		return l.readStringToken(tokenPos, tokenLine, tokenColumn)
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
		// 验证数字格式
		if !l.isValidNumber(number) && l.errorRecovery != nil {
			err := CreateLexicalError(fmt.Sprintf("Invalid number format: %s", number), tokenPos, 0)
			err.Type = ErrorTypeInvalidNumber
			l.errorRecovery.AddError(err)
		}
		return Token{Type: TokenNumber, Value: number, Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
	}

	// 处理无法识别的字符
	if l.ch != 0 {
		if l.errorRecovery != nil {
			err := CreateLexicalErrorWithPosition(fmt.Sprintf("Unexpected character '%c'", l.ch), tokenPos, tokenLine, tokenColumn, l.ch)
			l.errorRecovery.AddError(err)
		}
		l.readChar()
		return l.NextToken() // 跳过无效字符，继续解析
	}

	return Token{Type: TokenEOF, Pos: tokenPos, Line: tokenLine, Column: tokenColumn}
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	
	// 更新位置信息
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
	pos := l.pos
	for isLetter(l.ch) {
		l.readChar()
	}
	return l.input[pos:l.pos]
}

func (l *Lexer) readPreviousIdentifier() string {
	// 保存当前位置
	endPos := l.pos

	// 向前移动直到找到非字母字符或到达输入开始
	startPos := endPos - 1
	for startPos >= 0 && isLetter(l.input[startPos]) {
		startPos--
	}

	// 调整到第一个字母字符的位置
	startPos++

	// 如果找到有效的标识符，返回它
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
	quoteChar := l.ch // 记录引号类型（单引号或双引号）
	startPos := l.pos // 记录开始位置（包含引号）
	l.readChar()      // 跳过开头引号

	for l.ch != quoteChar && l.ch != 0 {
		l.readChar()
	}

	if l.ch == quoteChar {
		l.readChar() // 跳过结尾引号
	}

	// 返回包含引号的完整字符串
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
	case "WITH":
		return Token{Type: TokenWITH, Value: ident}
	case "TIMESTAMP":
		return Token{Type: TokenTimestamp, Value: ident}
	case "TIMEUNIT":
		return Token{Type: TokenTimeUnit, Value: ident}
	case "ORDER":
		return Token{Type: TokenOrder, Value: ident}
	case "DISTINCT":
		return Token{Type: TokenDISTINCT, Value: ident}
	case "LIMIT":
		return Token{Type: TokenLIMIT, Value: ident}
	case "HAVING":
		return Token{Type: TokenHAVING, Value: ident}
	default:
		// 检查是否是常见的拼写错误
		if l.errorRecovery != nil {
			l.checkForTypos(ident, upperIdent)
		}
		return Token{Type: TokenIdent, Value: ident}
	}
}

// checkForTypos 检查常见的拼写错误
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

// readStringToken 读取字符串token并处理错误
func (l *Lexer) readStringToken(pos, line, column int) Token {
	quoteChar := l.ch
	startPos := l.pos
	l.readChar() // 跳过开头引号

	for l.ch != quoteChar && l.ch != 0 {
		l.readChar()
	}

	if l.ch == 0 {
		// 未闭合的字符串
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
		l.readChar() // 跳过结尾引号
	}

	value := l.input[startPos:l.pos]
	return Token{Type: TokenString, Value: value, Pos: pos, Line: line, Column: column}
}

// isValidNumber 验证数字格式
func (l *Lexer) isValidNumber(number string) bool {
	if number == "" {
		return false
	}
	
	dotCount := 0
	for _, ch := range number {
		if ch == '.' {
			dotCount++
			if dotCount > 1 {
				return false // 多个小数点
			}
		} else if !isDigit(byte(ch)) {
			return false // 非数字字符
		}
	}
	
	// 检查是否以小数点开头或结尾
	if number[0] == '.' || number[len(number)-1] == '.' {
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
