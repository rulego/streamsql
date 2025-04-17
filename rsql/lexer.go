package rsql

import "strings"

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
)

type Token struct {
	Type  TokenType
	Value string
	Pos   int
}

type Lexer struct {
	input   string
	pos     int
	readPos int
	ch      byte
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	switch l.ch {
	case 0:
		return Token{Type: TokenEOF}
	case ',':
		l.readChar()
		return Token{Type: TokenComma, Value: ","}
	case '(':
		l.readChar()
		return Token{Type: TokenLParen, Value: "("}
	case ')':
		l.readChar()
		return Token{Type: TokenRParen, Value: ")"}
	case '+':
		l.readChar()
		return Token{Type: TokenPlus, Value: "+"}
	case '-':
		l.readChar()
		return Token{Type: TokenMinus, Value: "-"}
	case '*':
		l.readChar()
		return Token{Type: TokenAsterisk, Value: "*"}
	case '/':
		l.readChar()
		return Token{Type: TokenSlash, Value: "/"}
	case '=':
		l.readChar()
		return Token{Type: TokenEQ, Value: "="}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenGE, Value: ">="}
		}
		l.readChar()
		return Token{Type: TokenGT, Value: ">"}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenLE, Value: "<="}
		}
		l.readChar()
		return Token{Type: TokenLT, Value: "<"}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			return Token{Type: TokenNE, Value: "!="}
		}
	}

	if isLetter(l.ch) {
		ident := l.readIdentifier()
		return l.lookupIdent(ident)
	}

	if isDigit(l.ch) {
		return Token{Type: TokenNumber, Value: l.readNumber()}
	}

	if l.ch == '\'' {
		return Token{Type: TokenString, Value: l.readString()}
	}

	l.readChar()
	return Token{Type: TokenEOF}
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
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
	l.readChar() // 跳过开头单引号
	pos := l.pos

	for l.ch != '\'' && l.ch != 0 {
		l.readChar()
	}

	str := l.input[pos:l.pos]
	l.readChar() // 跳过结尾单引号
	return str
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) lookupIdent(ident string) Token {
	switch strings.ToUpper(ident) {
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
	default:
		return Token{Type: TokenIdent, Value: ident}
	}
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}
