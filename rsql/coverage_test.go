package rsql

import (
	"strings"
	"testing"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/types"
)

// TestParseSmartParameters 测试智能参数解析函数
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

// TestIsIdentifier 测试标识符验证函数
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

// TestExtractSimpleField 测试简单字段提取函数
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

// TestParseWindowParams 测试窗口参数解析函数
func TestParseWindowParams(t *testing.T) {
	tests := []struct {
		name        string
		params      []interface{}
		windowType  string
		expectError bool
	}{
		{
			name:        "会话窗口参数",
			params:      []interface{}{"10s", "5s"},
			windowType:  "SESSIONWINDOW",
			expectError: false,
		},
		{
			name:        "滚动窗口参数",
			params:      []interface{}{"30s"},
			windowType:  "TUMBLINGWINDOW",
			expectError: false,
		},
		{
			name:        "滑动窗口参数",
			params:      []interface{}{"60s", "30s"},
			windowType:  "SLIDINGWINDOW",
			expectError: false,
		},
		{
			name:        "无效持续时间",
			params:      []interface{}{"invalid"},
			windowType:  "TUMBLINGWINDOW",
			expectError: true,
		},
		{
			name:        "非字符串参数",
			params:      []interface{}{123},
			windowType:  "TUMBLINGWINDOW",
			expectError: true,
		},
		{
			name:        "空参数",
			params:      []interface{}{},
			windowType:  "TUMBLINGWINDOW",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result map[string]interface{}
			var err error

			if tt.windowType == "SESSIONWINDOW" {
				result, err = parseWindowParamsWithType(tt.params, "SESSIONWINDOW")
			} else {
				result, err = parseWindowParams(tt.params)
			}

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

// TestParseAggregateExpression 测试聚合表达式解析函数
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

// TestExpectToken 测试期望token函数
func TestExpectToken(t *testing.T) {
	// 测试正常情况
	parser := NewParser("SELECT field FROM table")
	tok, err := parser.expectToken(TokenSELECT, "SELECT clause")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if tok.Type != TokenSELECT {
		t.Errorf("Expected SELECT token, got %v", tok.Type)
	}

	// 直接测试getTokenTypeName函数
	result := parser.getTokenTypeName(TokenSELECT)
	if result != "SELECT" {
		t.Errorf("Expected 'SELECT', got %v", result)
	}

	// 测试getTokenTypeName的其他分支
	result2 := parser.getTokenTypeName(TokenType(999))
	if result2 != "unknown" {
		t.Errorf("Expected 'unknown', got %v", result2)
	}
}

// TestGetTokenTypeName 测试获取token类型名称函数
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
		{TokenType(999), "unknown"}, // 未知类型
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

// TestSkipToNextDelimiter 测试跳转到下一个分隔符函数
func TestSkipToNextDelimiter(t *testing.T) {
	// 测试正常跳转
	parser := NewParser("field1, field2 FROM table")
	er := NewErrorRecovery(parser)
	er.parser = parser

	// 跳过到逗号
	success := er.skipToNextDelimiter()
	if !success {
		t.Error("Expected successful skip to delimiter")
	}

	// 测试到达EOF
	parser2 := NewParser("field1 field2")
	er2 := NewErrorRecovery(parser2)
	er2.parser = parser2

	success = er2.skipToNextDelimiter()
	if success {
		t.Error("Expected failure when reaching EOF")
	}
}

// TestCreateSemanticError 测试创建语义错误函数
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

// TestFormatErrorContext 测试格式化错误上下文函数
func TestFormatErrorContext(t *testing.T) {
	input := "SELECT field FROM table WHERE condition"
	position := 10
	contextLength := 5

	result := FormatErrorContext(input, position, contextLength)
	if result == "" {
		t.Error("Expected non-empty error context")
	}

	// 测试边界情况
	result2 := FormatErrorContext("", 0, 0)
	if result2 != "" {
		t.Error("Expected empty result for empty input")
	}

	result3 := FormatErrorContext("short", 100, 10)
	if result3 != "" {
		t.Error("Expected empty result for out-of-bounds position")
	}
}

// TestConvertValue 测试值转换函数
func TestConvertValue(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
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

// TestHandleLimitToken 测试处理LIMIT token函数
func TestHandleLimitToken(t *testing.T) {
	// 测试正常情况
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

	// 测试无效LIMIT值
	parser2 := NewParser("invalid")
	stmt2 := &SelectStatement{}
	err = parser2.handleLimitToken(stmt2, limitToken)
	if err == nil {
		t.Error("Expected error for invalid limit value")
	}

	// 测试负数LIMIT值
	parser3 := NewParser("-5")
	stmt3 := &SelectStatement{}
	err = parser3.handleLimitToken(stmt3, limitToken)
	if err == nil {
		t.Error("Expected error for negative limit value")
	}

	// 测试减号后跟非数字
	parser4 := NewParser("- abc")
	stmt4 := &SelectStatement{}
	err = parser4.handleLimitToken(stmt4, limitToken)
	if err == nil {
		t.Error("Expected error for minus followed by non-number")
	}

	// 测试减号后跟数字（负数情况）
	parser5 := NewParser("- 10")
	stmt5 := &SelectStatement{}
	err = parser5.handleLimitToken(stmt5, limitToken)
	if err == nil {
		t.Error("Expected error for negative number")
	}
}

// TestReadString 测试读取字符串函数
func TestReadString(t *testing.T) {
	// 测试正常字符串
	lexer := NewLexer("'hello world'")
	lexer.readChar() // 跳过开始的引号

	result := lexer.readString()
	if result != "hello world'" {
		t.Errorf("Expected 'hello world', got %v", result)
	}

	// 测试未闭合的字符串
	lexer2 := NewLexer("'unclosed string")
	lexer2.readChar()

	result2 := lexer2.readString()
	if result2 != "unclosed string" {
		t.Errorf("Expected partial string, got %v", result2)
	}
}

// TestParseLimit 测试解析LIMIT子句函数
func TestParseLimit(t *testing.T) {
	// 测试正常LIMIT
	parser := NewParser("SELECT * FROM table LIMIT 10")
	stmt := &SelectStatement{}
	err := parser.parseLimit(stmt)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if stmt.Limit != 10 {
		t.Errorf("Expected limit 10, got %v", stmt.Limit)
	}

	// 测试没有LIMIT子句
	parser2 := NewParser("SELECT * FROM table")
	stmt2 := &SelectStatement{}
	err = parser2.parseLimit(stmt2)
	if err != nil {
		t.Errorf("Expected no error for missing LIMIT, got %v", err)
	}
	if stmt2.Limit != 0 {
		t.Errorf("Expected limit 0, got %v", stmt2.Limit)
	}

	// 测试LIMIT后没有数字
	parser3 := NewParser("SELECT * FROM table LIMIT")
	stmt3 := &SelectStatement{}
	err = parser3.parseLimit(stmt3)
	if err == nil {
		t.Error("Expected error for LIMIT without number")
	}

	// 测试LIMIT后跟无效值
	parser4 := NewParser("SELECT * FROM table LIMIT abc")
	stmt4 := &SelectStatement{}
	err = parser4.parseLimit(stmt4)
	if err == nil {
		t.Error("Expected error for invalid LIMIT value")
	}

	// 测试LIMIT负数
	parser5 := NewParser("SELECT * FROM table LIMIT -5")
	stmt5 := &SelectStatement{}
	err = parser5.parseLimit(stmt5)
	if err == nil {
		t.Error("Expected error for negative LIMIT")
	}

	// 测试已设置LIMIT的情况
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

// TestParseHaving 测试解析HAVING子句函数
func TestParseHaving(t *testing.T) {
	// 测试正常HAVING子句
	parser := NewParser("SELECT COUNT(*) FROM table GROUP BY id HAVING COUNT(*) > 5")
	stmt := &SelectStatement{}
	err := parser.parseHaving(stmt)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// 测试没有HAVING子句
	parser2 := NewParser("SELECT * FROM table")
	stmt2 := &SelectStatement{}
	err = parser2.parseHaving(stmt2)
	if err != nil {
		t.Errorf("Expected no error for missing HAVING, got %v", err)
	}

	// 测试HAVING子句中的各种条件
	parser3 := NewParser("HAVING field = 'value' AND count > 10 OR status LIKE 'active%'")
	stmt3 := &SelectStatement{}
	err = parser3.parseHaving(stmt3)
	if err != nil {
		t.Errorf("Expected no error for complex HAVING, got %v", err)
	}

	// 测试HAVING后遇到LIMIT
	parser4 := NewParser("HAVING count > 5 LIMIT 10")
	stmt4 := &SelectStatement{}
	err = parser4.parseHaving(stmt4)
	if err != nil {
		t.Errorf("Expected no error when HAVING followed by LIMIT, got %v", err)
	}

	// 测试HAVING后遇到WITH
	parser5 := NewParser("HAVING count > 5 WITH TUMBLING")
	stmt5 := &SelectStatement{}
	err = parser5.parseHaving(stmt5)
	if err != nil {
		t.Errorf("Expected no error when HAVING followed by WITH, got %v", err)
	}
}

// TestParseWithErrorRecovery 测试Parse函数的错误恢复
func TestParseWithErrorRecovery(t *testing.T) {
	// 测试基本的错误情况
	parser := NewParser("SELECT * FROM table WHERE id = 1")
	stmt, err := parser.Parse()
	if err != nil {
		t.Errorf("Unexpected error for valid syntax: %v", err)
	}
	if stmt == nil {
		t.Error("Expected statement for valid syntax")
	}

	// 测试错误恢复后的继续解析
	parser2 := NewParser("SELECT * FROM table GROUP BY field")
	stmt2, err := parser2.Parse()
	// 这应该是有效的语法
	if err != nil {
		t.Errorf("Unexpected error for valid GROUP BY: %v", err)
	}
	if stmt2 == nil {
		t.Error("Expected statement for valid syntax")
	}

	// 测试完全无效的语法
	parser3 := NewParser("COMPLETELY INVALID SYNTAX")
	_, err = parser3.Parse()
	if err == nil {
		t.Error("Expected error for completely invalid syntax")
	}
}

// TestGenerateFunctionSuggestions 测试生成函数建议函数
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

// TestBuildSelectFieldsWithExpressions 测试带表达式的选择字段构建函数
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
			aggMap, fieldMap, expressions := buildSelectFieldsWithExpressions(tt.fields)
			tt.checkFunc(t, aggMap, fieldMap, expressions)
		})
	}
}

// TestParserLexerErrorHandling 测试词法分析器错误处理
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
			expectError: false, // 解析为数字123和标识符abc
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

			// 读取所有token
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

// TestParserErrorRecoveryCases 测试解析器错误恢复
// 注意：当前解析器的错误恢复机制可能与预期不同
func TestParserErrorRecoveryCases(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
		shouldParse bool
	}{
		{
			name:        "语法错误恢复",
			query:       "SELECT FROM table WHERE", // 缺少列名
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
			expectError: false, // 解析器可能将其作为表达式处理
			shouldParse: true,
		},
		{
			name:        "括号不匹配表达式",
			query:       "SELECT func(field FROM table",
			expectError: true, // 解析器检测到语法错误
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
			shouldParse: true, // 解析器支持长标识符
		},
		{
			name:        "子查询语法",
			query:       "SELECT column FROM (SELECT * FROM table) AS sub",
			expectError: true, // 当前解析器不支持子查询
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

			// 验证是否能继续解析
			if !tt.shouldParse && err == nil {
				t.Errorf("expected parsing to fail, but it succeeded")
			}
		})
	}
}

// TestParserCaseExpressionErrors 测试CASE表达式错误处理
// 注意：当前解析器将CASE表达式作为普通表达式处理，不进行特殊语法验证
func TestParserCaseExpressionErrors(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "CASE作为表达式",
			query:       "SELECT CASE WHEN condition THEN value END FROM table",
			expectError: false, // 当前解析器将其作为表达式处理
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
			expectError: false, // 解析器不验证表达式内部语法
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

// TestParserComplexFieldAccess 测试复杂字段访问错误处理
// 注意：当前解析器将复杂字段访问作为表达式处理
func TestParserComplexFieldAccess(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "数组索引语法错误",
			query:       "SELECT field[ FROM table",
			expectError: false, // 解析器将其作为表达式处理
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
			expectError: true, // lexer不支持点号在表达式中
		},
		{
			name:        "标识符数组索引",
			query:       "SELECT field[abc] FROM table",
			expectError: false,
		},
		{
			name:        "未闭合括号",
			query:       "SELECT field[0 FROM table",
			expectError: false, // 解析器将其作为表达式处理
		},
		{
			name:        "空数组索引",
			query:       "SELECT field[] FROM table",
			expectError: false, // 解析器将其作为表达式处理
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

// TestParserBoundaryConditions 测试解析器边界条件
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
			expectError: false, // 解析器实际上没有字段数量限制
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
