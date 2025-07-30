package rsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

	t.Run("包含特殊字符的字符串", func(t *testing.T) {
		lexer := NewLexer("'test-value_123'")
		token := lexer.NextToken()
		assert.Equal(t, TokenString, token.Type)
		assert.Equal(t, "'test-value_123'", token.Value)
	})

	t.Run("空字符串", func(t *testing.T) {
		lexer := NewLexer("''")
		token := lexer.NextToken()
		assert.Equal(t, TokenString, token.Type)
		assert.Equal(t, "''", token.Value)
	})
}

// TestComplexSQL 测试复杂SQL语句的词法分析
func TestComplexSQL(t *testing.T) {
	t.Run("包含反引号标识符和字符串常量的SQL", func(t *testing.T) {
		sql := "SELECT `deviceId`, deviceType, 'aa' as test FROM stream WHERE `deviceId` LIKE 'sensor%'"
		lexer := NewLexer(sql)

		// 验证token序列
		expectedTokens := []struct {
			Type  TokenType
			Value string
		}{
			{TokenSELECT, "SELECT"},
			{TokenQuotedIdent, "`deviceId`"},
			{TokenComma, ","},
			{TokenIdent, "deviceType"},
			{TokenComma, ","},
			{TokenString, "'aa'"},
			{TokenAS, "as"},
			{TokenIdent, "test"},
			{TokenFROM, "FROM"},
			{TokenIdent, "stream"},
			{TokenWHERE, "WHERE"},
			{TokenQuotedIdent, "`deviceId`"},
			{TokenLIKE, "LIKE"},
			{TokenString, "'sensor%'"},
			{TokenEOF, ""},
		}

		for i, expected := range expectedTokens {
			token := lexer.NextToken()
			assert.Equal(t, expected.Type, token.Type, "Token %d type mismatch", i)
			if expected.Value != "" {
				assert.Equal(t, expected.Value, token.Value, "Token %d value mismatch", i)
			}
		}
	})

	t.Run("双引号字符串常量", func(t *testing.T) {
		sql := `SELECT deviceId, "test value" as name FROM stream`
		lexer := NewLexer(sql)

		// 跳过前面的token直到字符串
		lexer.NextToken()          // SELECT
		lexer.NextToken()          // deviceId
		lexer.NextToken()          // ,
		token := lexer.NextToken() // "test value"

		assert.Equal(t, TokenString, token.Type)
		assert.Equal(t, `"test value"`, token.Value)
	})
}
