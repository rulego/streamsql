/*
 * Copyright 2024 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rsql

// Lexer 是词法分析器的接口，它定义了从输入中读取 Token 的方法
type Lexer interface {
	// NextToken 用于返回输入中的下一个 Token
	NextToken() Token
}

// NewLexer 是一个工厂函数，用于根据输入的字符串创建一个 Lexer 的实例
func NewLexer(input string) Lexer {
	// 创建一个 lexer 的实例，并读取第一个字符
	l := &lexer{input: input}
	l.readChar()
	// 返回这个实例
	return l
}

// lexer 是 Lexer 接口的一个实现，它用一个字符串作为输入
type lexer struct {
	// input 表示输入的字符串
	input string
	// position 表示当前读取的位置
	position int
	// readPosition 表示下一个要读取的位置
	readPosition int
	// ch 表示当前读取的字符
	ch byte
}

// newToken 用于根据给定的类型和字符，创建一个新的 Token 实例
func newToken(tokenType TokenType, ch byte) Token {
	// 返回一个 Token 结构体，设置其类型和字面值
	return Token{Type: tokenType, Literal: string(ch)}
}

// skipWhitespace 用于跳过空白字符
func (l *lexer) skipWhitespace() {
	// while 循环，判断当前字符是否为空白字符
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		// 如果是，就调用 readChar 方法，读取下一个字符
		l.readChar()
	}
}

// readChar 用于读取输入中的下一个字符，并更新位置信息
func (l *lexer) readChar() {
	// 如果已经到达输入的末尾，就将 ch 设置为 0
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		// 否则，就将 ch 设置为下一个字符
		l.ch = l.input[l.readPosition]
	}
	// 将当前位置设置为下一个位置，将下一个位置增加 1
	l.position = l.readPosition
	l.readPosition++
}

// peekChar 用于返回输入中的下一个字符，但不移动位置信息
func (l *lexer) peekChar() byte {
	// 如果已经到达输入的末尾，就返回 0
	if l.readPosition >= len(l.input) {
		return 0
	} else {
		// 否则，就返回下一个字符，但不改变位置信息
		return l.input[l.readPosition]
	}
}

// readComment 用于读取注释的内容，直到遇到换行符或文件结束
func (l *lexer) readComment() string {
	// 记录当前位置
	position := l.position + 1
	// while 循环，判断当前字符是否是换行符或文件结束
	for l.ch != '\n' && l.ch != 0 {
		// 如果不是，就读取下一个字符
		l.readChar()
	}
	// 返回注释的内容，不包括两个斜杠和换行符
	return l.input[position:l.position]
}

// readNumber 用于读取数字字面量的内容，直到遇到非数字的字符
func (l *lexer) readNumber() string {
	// 记录当前位置
	position := l.position
	// while 循环，判断当前字符是否是数字
	for l.isDigit(l.ch) {
		// 如果是，就读取下一个字符
		l.readChar()
	}
	// 返回数字的内容
	return l.input[position:l.position]
}

// isLetter 用于判断一个字符是否是字母，包括下划线
func (l *lexer) isLetter(ch byte) bool {
	// 如果是大写或小写字母，或者是下划线，就返回 true
	return 'A' <= ch && ch <= 'Z' || 'a' <= ch && ch <= 'z' || ch == '_'
}

// isDigit 用于判断一个字符是否是数字
func (l *lexer) isDigit(ch byte) bool {
	// 如果是 0 到 9 之间的数字，就返回 true
	return '0' <= ch && ch <= '9'
}

// readIdentifier 用于读取标识符的内容，直到遇到非字母或非数字的字符
func (l *lexer) readIdentifier() string {
	// 记录当前位置
	position := l.position
	// while 循环，判断当前字符是否是字母或数字
	for l.isLetter(l.ch) || l.isDigit(l.ch) {
		// 如果是，就读取下一个字符
		l.readChar()
	}
	// 返回标识符的内容
	return l.input[position:l.position]
}

// readString 用于读取字符串字面量的内容，直到遇到另一个单引号或文件结束
func (l *lexer) readString() string {
	// 记录当前位置
	position := l.position
	// while 循环，判断当前字符是否是单引号或文件结束
	for {
		// 读取下一个字符
		l.readChar()
		// 如果是单引号或文件结束，就跳出循环
		if l.ch == '\'' || l.ch == 0 {
			break
		}
	}
	// 读取下一个字符
	l.readChar()
	// 返回字符串的内容，包括两个单引号
	return l.input[position:l.position]
}

// NextToken 实现了 Lexer 接口的 NextToken 方法，用于返回输入中的下一个 Token
func (l *lexer) NextToken() Token {
	var tok Token

	// 跳过空白字符
	l.skipWhitespace()

	// 根据当前字符，判断 Token 的类型
	switch l.ch {
	case '=':
		// 如果是等号，就判断是赋值符号还是等于号
		if l.peekChar() == '=' {
			// 如果下一个字符也是等号，就是等于号
			ch := l.ch
			// 读取下一个字符
			l.readChar()
			// 设置 Token 的类型和字面值
			tok = Token{Type: EQ, Literal: string(ch) + string(l.ch)}
		} else {
			// 否则，就是赋值符号
			tok = newToken(ASSIGN, l.ch)
		}
	case '+':
		// 如果是加号，就设置 Token 的类型和字面值
		tok = newToken(PLUS, l.ch)
	case '-':
		// 如果是减号，就设置 Token 的类型和字面值
		tok = newToken(MINUS, l.ch)
	case '*':
		// 如果是星号，就设置 Token 的类型和字面值
		tok = newToken(ASTERISK, l.ch)
	case '/':
		// 如果是斜杠，就判断是除号还是注释
		if l.peekChar() == '/' {
			// 如果下一个字符也是斜杠，就是注释
			// 读取注释的内容，直到遇到换行符或文件结束
			literal := l.readComment()
			// 设置 Token 的类型和字面值
			tok = Token{Type: COMMENT, Literal: literal}
		} else {
			// 否则，就是除号
			tok = newToken(SLASH, l.ch)
		}
	case ',':
		// 如果是逗号，就设置 Token 的类型和字面值
		tok = newToken(COMMA, l.ch)
	case ';':
		// 如果是分号，就设置 Token 的类型和字面值
		tok = newToken(SEMICOLON, l.ch)
	case '(':
		// 如果是左括号，就设置 Token 的类型和字面值
		tok = newToken(LPAREN, l.ch)
	case ')':
		// 如果是右括号，就设置 Token 的类型和字面值
		tok = newToken(RPAREN, l.ch)
	case '{':
		// 如果是左大括号，就设置 Token 的类型和字面值
		tok = newToken(LBRACE, l.ch)
	case '}':
		// 如果是右大括号，就设置 Token 的类型和字面值
		tok = newToken(RBRACE, l.ch)
	case '<':
		// 如果是小于号，就判断是小于号、小于等于号还是不等于号
		if l.peekChar() == '=' {
			// 如果下一个字符是等号，就是小于等于号
			ch := l.ch
			// 读取下一个字符
			l.readChar()
			// 设置 Token 的类型和字面值
			tok = Token{Type: LE, Literal: string(ch) + string(l.ch)}
		} else if l.peekChar() == '>' {
			// 如果下一个字符是大于号，就是不等于号
			ch := l.ch
			// 读取下一个字符
			l.readChar()
			// 设置 Token 的类型和字面值
			tok = Token{Type: NOT_EQ, Literal: string(ch) + string(l.ch)}
		} else {
			// 否则，就是小于号
			tok = newToken(LT, l.ch)
		}
	case '>':
		// 如果是大于号，就判断是大于号还是大于等于号
		if l.peekChar() == '=' {
			// 如果下一个字符是等号，就是大于等于号
			ch := l.ch
			// 读取下一个字符
			l.readChar()
			// 设置 Token 的类型和字面值
			tok = Token{Type: GE, Literal: string(ch) + string(l.ch)}
		} else {
			// 否则，就是大于号
			tok = newToken(GT, l.ch)
		}
	case 0:
		// 如果是文件结束，就设置 Token 的类型和字面值
		tok.Literal = ""
		tok.Type = EOF
	default:
		// 如果是其他字符，就判断是标识符、数字字面量还是字符串字面量
		if l.isLetter(l.ch) {
			// 如果是字母，就是标识符
			// 读取标识符的内容
			tok.Literal = l.readIdentifier()
			// 根据标识符的内容，判断是关键字还是普通的标识符
			tok.Type = LookupIdent(tok.Literal)
			// 返回 Token，不需要再读取下一个字符
			return tok
		} else if l.isDigit(l.ch) {
			// 如果是数字，就是数字字面量
			// 读取数字的内容
			tok.Literal = l.readNumber()
			// 设置 Token 的类型为 NUMBER
			tok.Type = NUMBER
			// 返回 Token，不需要再读取下一个字符
			return tok
		} else if l.ch == '\'' {
			// 如果是单引号，就是字符串字面量
			// 读取字符串的内容，包括两个单引号
			tok.Literal = l.readString()
			// 设置 Token 的类型为 STRING
			tok.Type = STRING
			// 返回 Token，不需要再读取下一个字符
			return tok
		} else {
			// 否则，就是非法字符
			tok = newToken(ILLEGAL, l.ch)
		}
	}

	// 读取下一个字符
	l.readChar()
	// 返回 Token
	return tok
}
