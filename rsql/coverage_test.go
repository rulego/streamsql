package rsql

import (
	"strings"
	"testing"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/window"
)

// TestParseSmartParameters tests the intelligent parameter parsing function
func TestParseSmartParameters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "简单参数",
			input:    "param1,param2,param3",
			expected: []string{"param1", "param2", "param3"},
		},
		{
			name:     "带引号的参数",
			input:    "'hello world',param2,\"quoted string\"",
			expected: []string{"'hello world'", "param2", "\"quoted string\""},
		},
		{
			name:     "空字符串",
			input:    "",
			expected: []string{},
		},
		{
			name:     "单个参数",
			input:    "single_param",
			expected: []string{"single_param"},
		},
		{
			name:     "带逗号的引号内容",
			input:    "'hello,world',param2",
			expected: []string{"'hello,world'", "param2"},
		},
		{
			name:     "混合引号类型",
			input:    "'single',\"double\",mixed",
			expected: []string{"'single'", "\"double\"", "mixed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseSmartParameters(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("parseSmartParameters() length = %v, want %v", len(result), len(tt.expected))
				return
			}
			for i, expected := range tt.expected {
				if result[i] != expected {
					t.Errorf("parseSmartParameters()[%d] = %v, want %v", i, result[i], expected)
				}
			}
		})
	}
}

// TestExpectTokenSuccess Tests the success status of the expectToken function
func TestExpectTokenSuccess(t *testing.T) {
	lexer := NewLexer("SELECT")
	parser := &Parser{lexer: lexer, errorRecovery: NewErrorRecovery(&Parser{})}

	token, err := parser.expectToken(TokenSELECT, "test context")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if token.Type != TokenSELECT {
		t.Errorf("Expected TokenSELECT, got %v", token.Type)
	}
}

// TestExpectTokenFailure tests the expectToken function failure
func TestExpectTokenFailure(t *testing.T) {
	lexer := NewLexer("FROM")
	parser := &Parser{lexer: lexer}
	parser.errorRecovery = NewErrorRecovery(parser)

	token, err := parser.expectToken(TokenSELECT, "test context")
	if err == nil {
		t.Error("Expected error, got none")
	}
	if token.Type == TokenSELECT {
		t.Error("Should not return expected token type on error")
	}
}

// TestExpectTokenWithRecovery tests the error recovery status of the expectToken function
func TestExpectTokenWithRecovery(t *testing.T) {
	lexer := NewLexer("FROM SELECT")
	parser := &Parser{lexer: lexer}
	parser.errorRecovery = NewErrorRecovery(parser)

	// The first call should fail
	_, err := parser.expectToken(TokenSELECT, "test context")
	if err == nil {
		t.Error("Expected error on first call")
	}
}

// TestParseWithMultipleErrors tests when the Parse function handles multiple errors
func TestParseWithMultipleErrors(t *testing.T) {
	// Create a query with multiple syntax errors
	parser := NewParser("SELECT FROM WHERE GROUP")
	stmt, err := parser.Parse()
	if err == nil {
		t.Error("Expected error for malformed query")
	}
	if stmt == nil {
		t.Error("Expected partial statement even with errors")
	}
}

// TestIsIdentifier is a test identifier verification function
func TestIsIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"有效标识符", "valid_identifier", true},
		{"带数字", "valid123", true},
		{"下划线开头", "_valid", true},
		{"大写字母", "VALID", true},
		{"混合大小写", "ValidIdentifier", true},
		{"空字符串", "", false},
		{"数字开头", "123invalid", false},
		{"特殊字符", "invalid@", false},
		{"空格", "invalid name", false},
		{"单个字母", "a", true},
		{"单个下划线", "_", true},
		{"包含连字符", "invalid-name", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("isIdentifier(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestExtractSimpleField tests the simple field extraction function
func TestExtractSimpleField(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"简单字段", "field_name", "field_name"},
		{"加法运算", "field1 + field2", "field1"},
		{"减法运算", "field1 - field2", "field1"},
		{"乘法运算", "field1 * field2", "field1"},
		{"除法运算", "field1 / field2", "field1"},
		{"带空格", " field1 + field2 ", "field1"},
		{"无运算符", "simple_field", "simple_field"},
		{"复杂表达式", "field1 + field2 * field3", "field1 + field2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractSimpleField(tt.input)
			if result != tt.expected {
				t.Errorf("extractSimpleField(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestParseWindowParams is a parameter parsing function for the test window
func TestParseWindowParams(t *testing.T) {
	tests := []struct {
		name        string
		params      []any
		windowType  string
		expectError bool
	}{
		{
			name:        "会话窗口参数",
			params:      []any{"10s", "5s"},
			windowType:  "SESSIONWINDOW",
			expectError: false,
		},
		{
			name:        "滚动窗口参数",
			params:      []any{"30s"},
			windowType:  "TUMBLINGWINDOW",
			expectError: false,
		},
		{
			name:        "滑动窗口参数",
			params:      []any{"60s", "30s"},
			windowType:  "SLIDINGWINDOW",
			expectError: false,
		},
		{
			name:        "计数窗口参数",
			params:      []any{100},
			windowType:  "COUNTINGWINDOW",
			expectError: false,
		},
		{
			name:        "无效持续时间",
			params:      []any{"invalid"},
			windowType:  "TUMBLINGWINDOW",
			expectError: true,
		},
		{
			name:        "非字符串参数",
			params:      []any{123},
			windowType:  "TUMBLINGWINDOW",
			expectError: false, // The integer parameter is considered a second, which is valid
		},
		{
			name:        "空参数",
			params:      []any{},
			windowType:  "TUMBLINGWINDOW",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result []any
			var err error

			// Convert window type to internal format
			windowType := ""
			switch tt.windowType {
			case "SESSIONWINDOW":
				windowType = window.TypeSession
			case "TUMBLINGWINDOW":
				windowType = window.TypeTumbling
			case "SLIDINGWINDOW":
				windowType = window.TypeSliding
			case "COUNTINGWINDOW":
				windowType = window.TypeCounting
			}

			result, err = validateWindowParams(tt.params, windowType)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if result == nil {
					t.Error("Expected result but got nil")
				}
			}
		})
	}
}

// TestParseAggregateExpression tests the parsing function of aggregate expressions
func TestParseAggregateExpression(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"AVG函数", "avg(temperature)", "avg"},
		{"SUM函数", "sum(price)", "sum"},
		{"MAX函数", "max(value)", "max"},
		{"MIN函数", "min(count)", "min"},
		{"嵌套函数", "avg(sum(temperature))", "avg"},
		{"无聚合函数", "temperature", ""},
		{"空字符串", "", ""},
		{"部分匹配", "average(temperature)", ""},
		{"带空格", " avg ( temperature ) ", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseAggregateExpression(tt.input)
			if result != tt.expected {
				t.Errorf("parseAggregateExpression(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestExpectToken tests the ExpectToken function
func TestExpectToken(t *testing.T) {
	// Test for normal conditions
	parser := NewParser("SELECT field FROM table")
	tok, err := parser.expectToken(TokenSELECT, "SELECT clause")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if tok.Type != TokenSELECT {
		t.Errorf("Expected SELECT token, got %v", tok.Type)
	}

	// Directly test the getTokenTypeName function
	result := parser.getTokenTypeName(TokenSELECT)
	if result != "SELECT" {
		t.Errorf("Expected 'SELECT', got %v", result)
	}

	// Test other branches of getTokenTypeName
	result2 := parser.getTokenTypeName(TokenType(999))
	if result2 != "unknown" {
		t.Errorf("Expected 'unknown', got %v", result2)
	}
}

// TestGetTokenTypeName Tests the function to get the token typename
func TestGetTokenTypeName(t *testing.T) {
	parser := NewParser("")
	tests := []struct {
		tokenType TokenType
		expected  string
	}{
		{TokenSELECT, "SELECT"},
		{TokenFROM, "FROM"},
		{TokenWHERE, "WHERE"},
		{TokenGROUP, "GROUP"},
		{TokenBY, "BY"},
		{TokenComma, ","},
		{TokenLParen, "("},
		{TokenRParen, ")"},
		{TokenIdent, "identifier"},
		{TokenQuotedIdent, "quoted identifier"},
		{TokenNumber, "number"},
		{TokenString, "string"},
		{TokenType(999), "unknown"}, // Unknown type
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := parser.getTokenTypeName(tt.tokenType)
			if result != tt.expected {
				t.Errorf("getTokenTypeName(%v) = %v, want %v", tt.tokenType, result, tt.expected)
			}
		})
	}
}

// TestSkipToNextDelimiter tests and jumps to the next delimiter function
func TestSkipToNextDelimiter(t *testing.T) {
	// Test the normal jump
	parser := NewParser("field1, field2 FROM table")
	er := NewErrorRecovery(parser)
	er.parser = parser

	// Skip to the comma
	success := er.skipToNextDelimiter()
	if !success {
		t.Error("Expected successful skip to delimiter")
	}

	// The test reached EOF
	parser2 := NewParser("field1 field2")
	er2 := NewErrorRecovery(parser2)
	er2.parser = parser2

	success = er2.skipToNextDelimiter()
	if success {
		t.Error("Expected failure when reaching EOF")
	}
}

// TestCreateSemanticError tests the semantic error function
func TestCreateSemanticError(t *testing.T) {
	err := CreateSemanticError("Invalid field reference", 10)
	if err == nil {
		t.Error("Expected semantic error to be created")
	}
	if err.Type != ErrorTypeSemantics {
		t.Errorf("Expected ErrorTypeSemantics, got %v", err.Type)
	}
	if err.Message != "Invalid field reference" {
		t.Errorf("Expected message 'Invalid field reference', got %v", err.Message)
	}
	if err.Position != 10 {
		t.Errorf("Expected position 10, got %v", err.Position)
	}
}

// TestFormatErrorContext Tests the formatting error context function
func TestFormatErrorContext(t *testing.T) {
	input := "SELECT field FROM table WHERE condition"
	position := 10
	contextLength := 5

	result := FormatErrorContext(input, position, contextLength)
	if result == "" {
		t.Error("Expected non-empty error context")
	}

	// Test boundary conditions
	result2 := FormatErrorContext("", 0, 0)
	if result2 != "" {
		t.Error("Expected empty result for empty input")
	}

	result3 := FormatErrorContext("short", 100, 10)
	if result3 != "" {
		t.Error("Expected empty result for out-of-bounds position")
	}
}

// TestConvertValue Converts the test value conversion function
func TestConvertValue(t *testing.T) {
	tests := []struct {
		input    string
		expected any
	}{
		{"123", 123},
		{"3.14", 3.14},
		{"true", true},
		{"false", false},
		{"hello", "hello"},
		{"", ""},
		{"null", "null"},
		{"NULL", "NULL"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := convertValue(tt.input)
			if result != tt.expected {
				t.Errorf("convertValue(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestHandleLimitToken tests the processing LIMIT token function
func TestHandleLimitToken(t *testing.T) {
	// Test for normal conditions
	parser := NewParser("10")
	stmt := &SelectStatement{}
	limitToken := Token{Type: TokenLIMIT, Value: "LIMIT"}

	err := parser.handleLimitToken(stmt, limitToken)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if stmt.Limit != 10 {
		t.Errorf("Expected limit 10, got %v", stmt.Limit)
	}

	// Test invalid LIMIT value
	parser2 := NewParser("invalid")
	stmt2 := &SelectStatement{}
	err = parser2.handleLimitToken(stmt2, limitToken)
	if err == nil {
		t.Error("Expected error for invalid limit value")
	}

	// Test the negative LIMIT value
	parser3 := NewParser("-5")
	stmt3 := &SelectStatement{}
	err = parser3.handleLimitToken(stmt3, limitToken)
	if err == nil {
		t.Error("Expected error for negative limit value")
	}

	// Test minus signs followed by non-numbers
	parser4 := NewParser("- abc")
	stmt4 := &SelectStatement{}
	err = parser4.handleLimitToken(stmt4, limitToken)
	if err == nil {
		t.Error("Expected error for minus followed by non-number")
	}

	// Test the minus sign followed by the number (negative number case)
	parser5 := NewParser("- 10")
	stmt5 := &SelectStatement{}
	err = parser5.handleLimitToken(stmt5, limitToken)
	if err == nil {
		t.Error("Expected error for negative number")
	}
}

// TestReadString tests the string function to be read
func TestReadString(t *testing.T) {
	// Test the normal string
	lexer := NewLexer("'hello world'")
	lexer.readChar() // Skip the quotation marks at the beginning

	result := lexer.readString()
	if result != "hello world'" {
		t.Errorf("Expected 'hello world', got %v", result)
	}

	// Test the unclosed string
	lexer2 := NewLexer("'unclosed string")
	lexer2.readChar()

	result2 := lexer2.readString()
	if result2 != "unclosed string" {
		t.Errorf("Expected partial string, got %v", result2)
	}
}

// TestParseLimitNotSubstring verifies parseLimit does not treat "limit"
// appearing inside an identifier or string literal as the LIMIT keyword (H7).
func TestParseLimitNotSubstring(t *testing.T) {
	cases := []string{
		"SELECT * FROM sublimits",
		"SELECT limited FROM t",
		"SELECT name FROM t WHERE tag='LIMIT'",
	}
	for _, sql := range cases {
		parser := NewParser(sql)
		stmt := &SelectStatement{}
		if err := parser.parseLimit(stmt); err != nil {
			t.Errorf("parseLimit(%q) returned unexpected error: %v", sql, err)
		}
		if stmt.Limit != 0 {
			t.Errorf("parseLimit(%q) set Limit=%d, want 0", sql, stmt.Limit)
		}
	}
}

// TestParseLimit tests and parses the LIMIT clause function
func TestParseLimit(t *testing.T) {
	// Test normal LIMIT
	parser := NewParser("SELECT * FROM table LIMIT 10")
	stmt := &SelectStatement{}
	err := parser.parseLimit(stmt)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if stmt.Limit != 10 {
		t.Errorf("Expected limit 10, got %v", stmt.Limit)
	}

	// The test lacks a LIMIT clause
	parser2 := NewParser("SELECT * FROM table")
	stmt2 := &SelectStatement{}
	err = parser2.parseLimit(stmt2)
	if err != nil {
		t.Errorf("Expected no error for missing LIMIT, got %v", err)
	}
	if stmt2.Limit != 0 {
		t.Errorf("Expected limit 0, got %v", stmt2.Limit)
	}

	// After testing LIMIT, there are no numbers
	parser3 := NewParser("SELECT * FROM table LIMIT")
	stmt3 := &SelectStatement{}
	err = parser3.parseLimit(stmt3)
	if err == nil {
		t.Error("Expected error for LIMIT without number")
	}

	// Test the invalid value after the LIMIT
	parser4 := NewParser("SELECT * FROM table LIMIT abc")
	stmt4 := &SelectStatement{}
	err = parser4.parseLimit(stmt4)
	if err == nil {
		t.Error("Expected error for invalid LIMIT value")
	}

	// Test LIMIT for negative numbers
	parser5 := NewParser("SELECT * FROM table LIMIT -5")
	stmt5 := &SelectStatement{}
	err = parser5.parseLimit(stmt5)
	if err == nil {
		t.Error("Expected error for negative LIMIT")
	}

	// Test when a LIMIT has been set
	parser6 := NewParser("SELECT * FROM table LIMIT 20")
	stmt6 := &SelectStatement{Limit: 15}
	err = parser6.parseLimit(stmt6)
	if err != nil {
		t.Errorf("Expected no error when limit already set, got %v", err)
	}
	if stmt6.Limit != 15 {
		t.Errorf("Expected limit to remain 15, got %v", stmt6.Limit)
	}
}

// TestParseHaving tests and parses the HAVING HAVING clause function
func TestParseHaving(t *testing.T) {
	// Test the normal HAVING clause
	parser := NewParser("SELECT COUNT(*) FROM table GROUP BY id HAVING COUNT(*) > 5")
	stmt := &SelectStatement{}
	err := parser.parseHaving(stmt)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// The test lacks a HAVING clause
	parser2 := NewParser("SELECT * FROM table")
	stmt2 := &SelectStatement{}
	err = parser2.parseHaving(stmt2)
	if err != nil {
		t.Errorf("Expected no error for missing HAVING, got %v", err)
	}

	// Test various conditions in the HAVING clause
	parser3 := NewParser("HAVING field = 'value' AND count > 10 OR status LIKE 'active%'")
	stmt3 := &SelectStatement{}
	err = parser3.parseHaving(stmt3)
	if err != nil {
		t.Errorf("Expected no error for complex HAVING, got %v", err)
	}

	// After testing HAVING, I encountered LIMIT
	parser4 := NewParser("HAVING count > 5 LIMIT 10")
	stmt4 := &SelectStatement{}
	err = parser4.parseHaving(stmt4)
	if err != nil {
		t.Errorf("Expected no error when HAVING followed by LIMIT, got %v", err)
	}

	// After testing HAVING, I encountered WITH
	parser5 := NewParser("HAVING count > 5 WITH TUMBLING")
	stmt5 := &SelectStatement{}
	err = parser5.parseHaving(stmt5)
	if err != nil {
		t.Errorf("Expected no error when HAVING followed by WITH, got %v", err)
	}
}

// TestParseWithErrorRecovery: Error recovery for the parse function
func TestParseWithErrorRecovery(t *testing.T) {
	// Testing basic error scenarios
	parser := NewParser("SELECT * FROM table WHERE id = 1")
	stmt, err := parser.Parse()
	if err != nil {
		t.Errorf("Unexpected error for valid syntax: %v", err)
	}
	if stmt == nil {
		t.Error("Expected statement for valid syntax")
	}

	// Continue parsing after error recovery during testing
	parser2 := NewParser("SELECT * FROM table GROUP BY field")
	stmt2, err := parser2.Parse()
	// This should be effective grammar
	if err != nil {
		t.Errorf("Unexpected error for valid GROUP BY: %v", err)
	}
	if stmt2 == nil {
		t.Error("Expected statement for valid syntax")
	}

	// Testing completely ineffective grammar
	parser3 := NewParser("COMPLETELY INVALID SYNTAX")
	_, err = parser3.Parse()
	if err == nil {
		t.Error("Expected error for completely invalid syntax")
	}
}

// TestGenerateFunctionSuggestions Test-generated function Suggestions function
func TestGenerateFunctionSuggestions(t *testing.T) {
	tests := []struct {
		functionName string
		expectEmpty  bool
	}{
		{"AVG", false},
		{"SUM", false},
		{"MAX", false},
		{"MIN", false},
		{"COUNT", false},
		{"unknown_func", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.functionName, func(t *testing.T) {
			suggestions := generateFunctionSuggestions(tt.functionName)
			if tt.expectEmpty && len(suggestions) > 0 {
				t.Errorf("Expected empty suggestions for %q, got %v", tt.functionName, suggestions)
			}
			if !tt.expectEmpty && len(suggestions) == 0 {
				t.Errorf("Expected suggestions for %q, got empty", tt.functionName)
			}
		})
	}
}

// TestBuildSelectFieldsWithExpressions tests the selection field constructor with expressions
func TestBuildSelectFieldsWithExpressions(t *testing.T) {
	tests := []struct {
		name      string
		fields    []Field
		checkFunc func(*testing.T, map[string]aggregator.AggregateType, map[string]string, map[string]types.FieldExpression)
	}{
		{
			name: "带别名的聚合函数",
			fields: []Field{
				{Expression: "AVG(temperature)", Alias: "avg_temp"},
				{Expression: "SUM(price)", Alias: "total_price"},
			},
			checkFunc: func(t *testing.T, aggMap map[string]aggregator.AggregateType, fieldMap map[string]string, expressions map[string]types.FieldExpression) {
				if len(aggMap) != 2 {
					t.Errorf("Expected 2 aggregate functions, got %d", len(aggMap))
				}
				if len(fieldMap) != 2 {
					t.Errorf("Expected 2 field mappings, got %d", len(fieldMap))
				}
				if len(expressions) != 0 {
					t.Errorf("Expected 0 expressions, got %d", len(expressions))
				}
			},
		},
		{
			name: "无别名的聚合函数",
			fields: []Field{
				{Expression: "AVG(temperature)", Alias: ""},
				{Expression: "COUNT(*)", Alias: ""},
			},
			checkFunc: func(t *testing.T, aggMap map[string]aggregator.AggregateType, fieldMap map[string]string, expressions map[string]types.FieldExpression) {
				if len(aggMap) == 0 {
					t.Error("Expected aggregate functions")
				}
			},
		},
		{
			name: "非聚合字段",
			fields: []Field{
				{Expression: "temperature", Alias: "temp"},
				{Expression: "humidity", Alias: "hum"},
			},
			checkFunc: func(t *testing.T, aggMap map[string]aggregator.AggregateType, fieldMap map[string]string, expressions map[string]types.FieldExpression) {
				if len(aggMap) != 0 {
					t.Errorf("Expected no aggregate functions, got %d", len(aggMap))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggMap, fieldMap, expressions, _, err := buildSelectFieldsWithExpressions(tt.fields)
			if err != nil {
				t.Errorf("buildSelectFieldsWithExpressions() error = %v", err)
				return
			}
			tt.checkFunc(t, aggMap, fieldMap, expressions)
		})
	}
}

// TestParserLexerErrorHandling Error handling in the test lexer analyzer
func TestParserLexerErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		errorCount  int
	}{
		{
			name:        "无效字符",
			input:       "SELECT @ FROM table",
			expectError: true,
			errorCount:  1,
		},
		{
			name:        "未终止字符串",
			input:       "SELECT 'unterminated FROM table",
			expectError: true,
			errorCount:  1,
		},
		{
			name:        "无效数字格式",
			input:       "SELECT 123abc FROM table",
			expectError: false, // Interpreted as the number 123 and the identifier ABC
		},
		{
			name:        "反引号不匹配",
			input:       "SELECT `column FROM table",
			expectError: true,
			errorCount:  1,
		},
		{
			name:        "多个语法错误",
			input:       "SELECT @ 'unterminated `column FROM table",
			expectError: true,
			errorCount:  2,
		},
		{
			name:        "空输入",
			input:       "",
			expectError: false,
			errorCount:  0,
		},
		{
			name:        "只有空格",
			input:       "   \t\n   ",
			expectError: false,
			errorCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			er := NewErrorRecovery(nil)
			lexer.SetErrorRecovery(er)

			// Read all tokens
			for {
				token := lexer.NextToken()
				if token.Type == TokenEOF {
					break
				}
			}

			if er.HasErrors() != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, er.HasErrors())
			}

			if tt.expectError && len(er.GetErrors()) != tt.errorCount {
				t.Errorf("expected %d errors, got %d", tt.errorCount, len(er.GetErrors()))
			}
		})
	}
}

// TestParserErrorRecoveryCases test parser error recovery
// Note: The current parser error recovery mechanism may differ from what is expected
func TestParserErrorRecoveryCases(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
		shouldParse bool
	}{
		{
			name:        "语法错误恢复",
			query:       "SELECT FROM table WHERE", // Missing a list
			expectError: true,
			shouldParse: false,
		},
		{
			name:        "FROM子句缺失",
			query:       "SELECT column WHERE value > 1",
			expectError: true,
			shouldParse: false,
		},
		{
			name:        "WHERE条件不完整",
			query:       "SELECT * FROM table WHERE field >",
			expectError: false, // The parser may treat it as an expression
			shouldParse: true,
		},
		{
			name:        "括号不匹配表达式",
			query:       "SELECT func(field FROM table",
			expectError: true, // The parser detects syntax errors
			shouldParse: false,
		},
		{
			name:        "空查询",
			query:       "",
			expectError: true,
			shouldParse: false,
		},
		{
			name:        "只有SELECT",
			query:       "SELECT",
			expectError: true,
			shouldParse: false,
		},
		{
			name:        "长标识符",
			query:       "SELECT " + strings.Repeat("a", 100) + " FROM table",
			expectError: false,
			shouldParse: true, // The parser supports long identifiers
		},
		{
			name:        "子查询语法",
			query:       "SELECT column FROM (SELECT * FROM table) AS sub",
			expectError: true, // The current parser does not support subqueries
			shouldParse: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.query)
			_, err := parser.Parse()

			if (err != nil) != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}

			// Verify whether further analysis is possible
			if !tt.shouldParse && err == nil {
				t.Errorf("expected parsing to fail, but it succeeded")
			}
		})
	}
}

// TestParserCaseExpressionErrors Tests CASE expression error handling
// Note: The current parser treats CASE expressions as regular expressions and does not perform special syntax verification
func TestParserCaseExpressionErrors(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "CASE作为表达式",
			query:       "SELECT CASE WHEN condition THEN value END FROM table",
			expectError: false, // The current parser treats it as an expression
		},
		{
			name:        "简单CASE表达式",
			query:       "SELECT CASE field WHEN 1 THEN 'one' ELSE 'other' END FROM table",
			expectError: false,
		},
		{
			name:        "CASE表达式在WHERE中",
			query:       "SELECT * FROM table WHERE CASE WHEN field > 0 THEN 1 ELSE 0 END = 1",
			expectError: false,
		},
		{
			name:        "嵌套CASE表达式",
			query:       "SELECT CASE WHEN CASE WHEN field > 0 THEN 1 ELSE 0 END = 1 THEN 'positive' ELSE 'negative' END FROM table",
			expectError: false,
		},
		{
			name:        "CASE表达式语法错误",
			query:       "SELECT CASE WHEN value > 10 AND < 20 THEN 'A' END FROM table",
			expectError: false, // The parser does not verify the internal syntax of the expression
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.query)
			_, err := parser.Parse()

			if (err != nil) != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}
		})
	}
}

// TestParserComplexFieldAccess tests complex field access error handling
// Note: The current parser treats complex field access as an expression
func TestParserComplexFieldAccess(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "数组索引语法错误",
			query:       "SELECT field[ FROM table",
			expectError: false, // The parser treats it as an expression
		},
		{
			name:        "嵌套数组访问",
			query:       "SELECT field[0][1] FROM table",
			expectError: false,
		},
		{
			name:        "字符串键访问",
			query:       "SELECT field['key'] FROM table",
			expectError: false,
		},
		{
			name:        "双引号字符串键",
			query:       "SELECT field[\"key\"] FROM table",
			expectError: false,
		},
		{
			name:        "嵌套字段访问",
			query:       "SELECT field.nested.deep FROM table",
			expectError: false,
		},
		{
			name:        "混合访问表达式",
			query:       "SELECT field.nested[0].deep FROM table",
			expectError: false, // Lexer now supports dot marks in expressions
		},
		{
			name:        "标识符数组索引",
			query:       "SELECT field[abc] FROM table",
			expectError: false,
		},
		{
			name:        "未闭合括号",
			query:       "SELECT field[0 FROM table",
			expectError: false, // The parser treats it as an expression
		},
		{
			name:        "空数组索引",
			query:       "SELECT field[] FROM table",
			expectError: false, // The parser treats it as an expression
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.query)
			_, err := parser.Parse()

			if (err != nil) != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}
		})
	}
}

// TestParserBoundaryConditions tests the parser boundary conditions
func TestParserBoundaryConditions(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "超长查询",
			query:       "SELECT " + strings.Repeat("column", 100) + " FROM table",
			expectError: false,
		},
		{
			name:        "大量字段",
			query:       "SELECT " + strings.Repeat("col,", 50) + "last FROM table",
			expectError: false, // The parser actually has no field limit
		},
		{
			name:        "复杂WHERE条件",
			query:       "SELECT * FROM table WHERE " + strings.Repeat("field > 0 AND ", 100) + "true",
			expectError: false,
		},
		{
			name:        "特殊字符标识符",
			query:       "SELECT `column with spaces`, \"quoted column\" FROM table",
			expectError: false,
		},
		{
			name:        "GROUP BY大量字段",
			query:       "SELECT * FROM table GROUP BY " + strings.Repeat("field,", 100) + "last",
			expectError: false,
		},
		{
			name:        "HAVING子句",
			query:       "SELECT * FROM table HAVING COUNT(*) > 0",
			expectError: false,
		},
		{
			name:        "ORDER BY子句",
			query:       "SELECT * FROM table ORDER BY field",
			expectError: false,
		},
		{
			name:        "LIMIT子句",
			query:       "SELECT * FROM table LIMIT 100",
			expectError: false,
		},
		{
			name:        "窗口函数",
			query:       "SELECT * FROM table TUMBLINGWINDOW(ss, 5)",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.query)
			_, err := parser.Parse()

			if (err != nil) != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}
		})
	}
}
