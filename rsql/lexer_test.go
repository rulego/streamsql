package rsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewLexer 测试词法分析器的创建
func TestNewLexer(t *testing.T) {
	input := "SELECT * FROM table"
	lexer := NewLexer(input)

	if lexer == nil {
		t.Fatal("Expected lexer to be created, got nil")
	}

	if lexer.input != input {
		t.Errorf("Expected input %s, got %s", input, lexer.input)
	}

	if lexer.line != 1 {
		t.Errorf("Expected line to be 1, got %d", lexer.line)
	}

	if lexer.column != 1 {
		t.Errorf("Expected column to be 1, got %d", lexer.column)
	}
}

// TestLexerBasicTokens 测试基本token的识别
func TestLexerBasicTokens(t *testing.T) {
	tests := []struct {
		input    string
		expected []TokenType
	}{
		{"SELECT", []TokenType{TokenSELECT, TokenEOF}},
		{"FROM", []TokenType{TokenFROM, TokenEOF}},
		{"WHERE", []TokenType{TokenWHERE, TokenEOF}},
		{"GROUP BY", []TokenType{TokenGROUP, TokenBY, TokenEOF}},
		{"ORDER", []TokenType{TokenOrder, TokenEOF}},
		{"DISTINCT", []TokenType{TokenDISTINCT, TokenEOF}},
		{"LIMIT", []TokenType{TokenLIMIT, TokenEOF}},
		{"HAVING", []TokenType{TokenHAVING, TokenEOF}},
		{"AS", []TokenType{TokenAS, TokenEOF}},
		{"AND", []TokenType{TokenAND, TokenEOF}},
		{"OR", []TokenType{TokenOR, TokenEOF}},
		{"LIKE", []TokenType{TokenLIKE, TokenEOF}},
		{"IS", []TokenType{TokenIS, TokenEOF}},
		{"NULL", []TokenType{TokenNULL, TokenEOF}},
		{"NOT", []TokenType{TokenNOT, TokenEOF}},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			lexer := NewLexer(test.input)
			for i, expectedType := range test.expected {
				token := lexer.NextToken()
				if token.Type != expectedType {
					t.Errorf("Token %d: expected %v, got %v", i, expectedType, token.Type)
				}
			}
		})
	}
}

// TestQuotedIdentifiers 测试反引号标识符的词法分析
func TestQuotedIdentifiers(t *testing.T) {
	t.Run("基本反引号标识符", func(t *testing.T) {
		lexer := NewLexer("`deviceId`")
		token := lexer.NextToken()
		assert.Equal(t, TokenQuotedIdent, token.Type)
		assert.Equal(t, "`deviceId`", token.Value)
	})

	t.Run("包含特殊字符的反引号标识符", func(t *testing.T) {
		lexer := NewLexer("`device-id`")
		token := lexer.NextToken()
		assert.Equal(t, TokenQuotedIdent, token.Type)
		assert.Equal(t, "`device-id`", token.Value)
	})

	t.Run("包含空格的反引号标识符", func(t *testing.T) {
		lexer := NewLexer("`device id`")
		token := lexer.NextToken()
		assert.Equal(t, TokenQuotedIdent, token.Type)
		assert.Equal(t, "`device id`", token.Value)
	})

	t.Run("未闭合的反引号标识符", func(t *testing.T) {
		lexer := NewLexer("`deviceId")
		errorRecovery := NewErrorRecovery(nil)
		lexer.SetErrorRecovery(errorRecovery)
		token := lexer.NextToken()
		assert.Equal(t, TokenQuotedIdent, token.Type)
		assert.True(t, errorRecovery.HasErrors())
		errors := errorRecovery.GetErrors()
		assert.Equal(t, 1, len(errors))
		assert.Equal(t, ErrorTypeUnterminatedString, errors[0].Type)
	})
}

// TestStringLiterals 测试字符串常量的词法分析
func TestStringLiterals(t *testing.T) {
	t.Run("单引号字符串", func(t *testing.T) {
		lexer := NewLexer("'hello world'")
		token := lexer.NextToken()
		assert.Equal(t, TokenString, token.Type)
		assert.Equal(t, "'hello world'", token.Value)
	})

	t.Run("双引号字符串", func(t *testing.T) {
		lexer := NewLexer(`"hello world"`)
		token := lexer.NextToken()
		assert.Equal(t, TokenString, token.Type)
		assert.Equal(t, `"hello world"`, token.Value)
	})

	t.Run("未闭合的字符串", func(t *testing.T) {
		lexer := NewLexer("'hello world")
		errorRecovery := NewErrorRecovery(nil)
		lexer.SetErrorRecovery(errorRecovery)
		token := lexer.NextToken()
		assert.Equal(t, TokenString, token.Type)
		assert.True(t, errorRecovery.HasErrors())
		errors := errorRecovery.GetErrors()
		assert.Equal(t, 1, len(errors))
		assert.Equal(t, ErrorTypeUnterminatedString, errors[0].Type)
	})
}

// TestLexerErrorHandling 测试词法分析器错误处理
func TestLexerErrorHandling(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"InvalidCharacter", "SELECT * FROM table WHERE id # 5"},
		{"UnterminatedString", "SELECT * FROM table WHERE name = 'test"},
		{"UnterminatedQuotedIdent", "SELECT `field FROM table"},
		{"InvalidNumber", "SELECT * FROM table WHERE value = 123.456.789"},
		{"InvalidOperator", "SELECT * FROM table WHERE a !! b"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			errorRecovery := NewErrorRecovery(nil)
			lexer.SetErrorRecovery(errorRecovery)

			// 读取所有token直到EOF
			for {
				token := lexer.NextToken()
				if token.Type == TokenEOF {
					break
				}
			}

			// 应该有错误
			if !errorRecovery.HasErrors() {
				t.Errorf("Expected errors for input: %s", test.input)
			}
		})
	}

	// 测试词法分析器的位置获取
	lexer := NewLexer("SELECT * FROM table")
	pos, line, column := lexer.GetPosition()
	if pos < 0 || line < 1 || column < 0 {
		t.Errorf("Invalid position: pos=%d, line=%d, column=%d", pos, line, column)
	}

	// 测试词法分析器的位置跟踪
	lexer = NewLexer("SELECT\n  *\nFROM\n  table")

	// SELECT
	token := lexer.NextToken()
	if token.Line != 1 || token.Column != 1 {
		t.Errorf("Expected token at line 1, column 1, got line %d, column %d", token.Line, token.Column)
	}

	// *
	token = lexer.NextToken()
	if token.Line != 2 || token.Column != 3 {
		t.Errorf("Expected token at line 2, column 3, got line %d, column %d", token.Line, token.Column)
	}

	// FROM
	token = lexer.NextToken()
	if token.Line != 3 || token.Column != 1 {
		t.Errorf("Expected token at line 3, column 1, got line %d, column %d", token.Line, token.Column)
	}

	// table
	token = lexer.NextToken()
	if token.Line != 4 || token.Column != 3 {
		t.Errorf("Expected token at line 4, column 3, got line %d, column %d", token.Line, token.Column)
	}
}

// TestLexerOperators 测试操作符的词法分析
func TestLexerOperators(t *testing.T) {
	tests := []struct {
		input    string
		expected []TokenType
	}{
		{"=", []TokenType{TokenEQ, TokenEOF}},
		{"!=", []TokenType{TokenNE, TokenEOF}},
		{"<>", []TokenType{TokenLT, TokenGT, TokenEOF}},
		{"<", []TokenType{TokenLT, TokenEOF}},
		{"<=", []TokenType{TokenLE, TokenEOF}},
		{">", []TokenType{TokenGT, TokenEOF}},
		{">=", []TokenType{TokenGE, TokenEOF}},
		{"+", []TokenType{TokenPlus, TokenEOF}},
		{"-", []TokenType{TokenMinus, TokenEOF}},
		{"*", []TokenType{TokenAsterisk, TokenEOF}},
		{"/", []TokenType{TokenSlash, TokenEOF}},
		{"(", []TokenType{TokenLParen, TokenEOF}},
		{")", []TokenType{TokenRParen, TokenEOF}},
		{",", []TokenType{TokenComma, TokenEOF}},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			lexer := NewLexer(test.input)
			for i, expectedType := range test.expected {
				token := lexer.NextToken()
				if token.Type != expectedType {
					t.Errorf("Token %d: expected %v, got %v", i, expectedType, token.Type)
				}
			}
		})
	}
}

// TestLexerNumbers 测试数字的词法分析
func TestLexerNumbers(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"123", "123"},
		{"123.456", "123.456"},
		{"0", "0"},
		{"0.0", "0.0"},
		{"1000000", "1000000"},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			lexer := NewLexer(test.input)
			token := lexer.NextToken()
			if token.Type != TokenNumber {
				t.Errorf("Expected TokenNumber, got %v", token.Type)
			}
			if token.Value != test.expected {
				t.Errorf("Expected value %s, got %s", test.expected, token.Value)
			}
		})
	}
}

// TestLexerIdentifiers 测试标识符的词法分析
func TestLexerIdentifiers(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"table", "table"},
		{"field_name", "field_name"},
		{"table123", "table123"},
		{"_private", "_private"},
		{"CamelCase", "CamelCase"},
		{"deviceId", "deviceId"},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			lexer := NewLexer(test.input)
			token := lexer.NextToken()
			if token.Type != TokenIdent {
				t.Errorf("Expected TokenIdent, got %v", token.Type)
			}
			if token.Value != test.expected {
				t.Errorf("Expected value %s, got %s", test.expected, token.Value)
			}
		})
	}
}

// TestTokenTypes 测试Token类型
func TestTokenTypes(t *testing.T) {
	// 测试关键字token
	keywordTests := []struct {
		input    string
		expected TokenType
	}{
		{"SELECT", TokenSELECT},
		{"FROM", TokenFROM},
		{"WHERE", TokenWHERE},
		{"GROUP", TokenGROUP},
		{"BY", TokenBY},
		{"HAVING", TokenHAVING},
		{"ORDER", TokenOrder},
		{"LIMIT", TokenLIMIT},
		{"AND", TokenAND},
		{"OR", TokenOR},
		{"NOT", TokenNOT},
		{"AS", TokenAS},
		{"DISTINCT", TokenDISTINCT},
	}

	for _, test := range keywordTests {
		t.Run(test.input, func(t *testing.T) {
			lexer := NewLexer(test.input)
			token := lexer.NextToken()
			if token.Type != test.expected {
				t.Errorf("Expected token type %v for %s, got %v", test.expected, test.input, token.Type)
			}
			if token.Value != test.input {
				t.Errorf("Expected token value %s, got %s", test.input, token.Value)
			}
		})
	}
}

// TestLexerWhitespace 测试空白字符处理
func TestLexerWhitespace(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
	}{
		{
			name:     "Spaces",
			input:    "SELECT   *   FROM   table",
			expected: []TokenType{TokenSELECT, TokenAsterisk, TokenFROM, TokenIdent, TokenEOF},
		},
		{
			name:     "Tabs",
			input:    "SELECT\t*\tFROM\ttable",
			expected: []TokenType{TokenSELECT, TokenAsterisk, TokenFROM, TokenIdent, TokenEOF},
		},
		{
			name:     "Newlines",
			input:    "SELECT\n*\nFROM\ntable",
			expected: []TokenType{TokenSELECT, TokenAsterisk, TokenFROM, TokenIdent, TokenEOF},
		},
		{
			name:     "Mixed whitespace",
			input:    "SELECT \t\n * \t\n FROM \t\n table",
			expected: []TokenType{TokenSELECT, TokenAsterisk, TokenFROM, TokenIdent, TokenEOF},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			for i, expectedType := range test.expected {
				token := lexer.NextToken()
				if token.Type != expectedType {
					t.Errorf("Token %d: expected %v, got %v", i, expectedType, token.Type)
				}
			}
		})
	}
}

// TestLexerComplexTokens 测试复杂token组合
func TestLexerComplexTokens(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []struct {
			type_ TokenType
			value string
		}
	}{
		{
			name:  "Function call",
			input: "COUNT(*)",
			expected: []struct {
				type_ TokenType
				value string
			}{
				{TokenIdent, "COUNT"},
				{TokenLParen, "("},
				{TokenAsterisk, "*"},
				{TokenRParen, ")"},
				{TokenEOF, ""},
			},
		},
		{
			name:  "Comparison",
			input: "age >= 18",
			expected: []struct {
				type_ TokenType
				value string
			}{
				{TokenIdent, "age"},
				{TokenGE, ">="},
				{TokenNumber, "18"},
				{TokenEOF, ""},
			},
		},
		{
			name:  "String with quotes",
			input: "name = 'John Doe'",
			expected: []struct {
				type_ TokenType
				value string
			}{
				{TokenIdent, "name"},
				{TokenEQ, "="},
				{TokenString, "'John Doe'"},
				{TokenEOF, ""},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			for i, expected := range test.expected {
				token := lexer.NextToken()
				if token.Type != expected.type_ {
					t.Errorf("Token %d: expected type %v, got %v", i, expected.type_, token.Type)
				}
				if expected.value != "" && token.Value != expected.value {
					t.Errorf("Token %d: expected value %s, got %s", i, expected.value, token.Value)
				}
			}
		})
	}
}
