package rsql

import (
	"fmt"
	"strings"
)

// ErrorType 定义错误类型
type ErrorType int

const (
	ErrorTypeSyntax ErrorType = iota
	ErrorTypeLexical
	ErrorTypeSemantics
	ErrorTypeUnexpectedToken
	ErrorTypeMissingToken
	ErrorTypeInvalidExpression
	ErrorTypeUnknownKeyword
	ErrorTypeInvalidNumber
	ErrorTypeUnterminatedString
	ErrorTypeMaxIterations
	ErrorTypeUnknownFunction
)

// ParseError 增强的解析错误结构
type ParseError struct {
	Type        ErrorType
	Message     string
	Position    int
	Line        int
	Column      int
	Token       string
	Expected    []string
	Suggestions []string
	Context     string
	Recoverable bool
}

// Error 实现 error 接口
func (e *ParseError) Error() string {
	var builder strings.Builder
	
	// 基本错误信息
	builder.WriteString(fmt.Sprintf("[%s] %s", e.getErrorTypeName(), e.Message))
	
	// 位置信息
	if e.Line > 0 && e.Column > 0 {
		builder.WriteString(fmt.Sprintf(" at line %d, column %d", e.Line, e.Column))
	} else if e.Position >= 0 {
		builder.WriteString(fmt.Sprintf(" at position %d", e.Position))
	}
	
	// 当前token信息
	if e.Token != "" {
		builder.WriteString(fmt.Sprintf(" (found '%s')", e.Token))
	}
	
	// 期望的token
	if len(e.Expected) > 0 {
		builder.WriteString(fmt.Sprintf(", expected: %s", strings.Join(e.Expected, ", ")))
	}
	
	// 上下文信息
	if e.Context != "" {
		builder.WriteString(fmt.Sprintf("\nContext: %s", e.Context))
	}
	
	// 建议
	if len(e.Suggestions) > 0 {
		builder.WriteString(fmt.Sprintf("\nSuggestions: %s", strings.Join(e.Suggestions, "; ")))
	}
	
	return builder.String()
}

// getErrorTypeName 获取错误类型名称
func (e *ParseError) getErrorTypeName() string {
	switch e.Type {
	case ErrorTypeSyntax:
		return "SYNTAX_ERROR"
	case ErrorTypeLexical:
		return "LEXICAL_ERROR"
	case ErrorTypeSemantics:
		return "SEMANTIC_ERROR"
	case ErrorTypeUnexpectedToken:
		return "UNEXPECTED_TOKEN"
	case ErrorTypeMissingToken:
		return "MISSING_TOKEN"
	case ErrorTypeInvalidExpression:
		return "INVALID_EXPRESSION"
	case ErrorTypeUnknownKeyword:
		return "UNKNOWN_KEYWORD"
	case ErrorTypeInvalidNumber:
		return "INVALID_NUMBER"
	case ErrorTypeUnterminatedString:
		return "UNTERMINATED_STRING"
	case ErrorTypeMaxIterations:
		return "MAX_ITERATIONS"
	case ErrorTypeUnknownFunction:
		return "UNKNOWN_FUNCTION"
	default:
		return "UNKNOWN_ERROR"
	}
}

// IsRecoverable 检查错误是否可恢复
func (e *ParseError) IsRecoverable() bool {
	return e.Recoverable
}

// ErrorRecovery 错误恢复策略
type ErrorRecovery struct {
	parser *Parser
	errors []*ParseError
}

// NewErrorRecovery 创建错误恢复实例
func NewErrorRecovery(parser *Parser) *ErrorRecovery {
	return &ErrorRecovery{
		parser: parser,
		errors: make([]*ParseError, 0),
	}
}

// AddError 添加错误
func (er *ErrorRecovery) AddError(err *ParseError) {
	er.errors = append(er.errors, err)
}

// GetErrors 获取所有错误
func (er *ErrorRecovery) GetErrors() []*ParseError {
	return er.errors
}

// HasErrors 检查是否有错误
func (er *ErrorRecovery) HasErrors() bool {
	return len(er.errors) > 0
}

// RecoverFromError 从错误中恢复
func (er *ErrorRecovery) RecoverFromError(errorType ErrorType) bool {
	switch errorType {
	case ErrorTypeUnexpectedToken:
		// 跳过当前token，尝试继续解析
		er.parser.lexer.NextToken()
		return true
	case ErrorTypeMissingToken:
		// 插入默认token或跳过
		return true
	case ErrorTypeInvalidExpression:
		// 跳到下一个逗号或关键字
		return er.skipToNextDelimiter()
	case ErrorTypeSyntax:
		// 语法错误也尝试恢复，继续解析
		return true
	case ErrorTypeUnknownKeyword:
		// 未知关键字错误也尝试恢复
		return true
	default:
		return false
	}
}

// skipToNextDelimiter 跳到下一个分隔符
func (er *ErrorRecovery) skipToNextDelimiter() bool {
	maxSkip := 10
	skipped := 0
	
	for skipped < maxSkip {
		tok := er.parser.lexer.NextToken()
		if tok.Type == TokenEOF {
			return false
		}
		if tok.Type == TokenComma || tok.Type == TokenFROM || 
		   tok.Type == TokenWHERE || tok.Type == TokenGROUP {
			return true
		}
		skipped++
	}
	return false
}

// CreateSyntaxError 创建语法错误
func CreateSyntaxError(message string, position int, token string, expected []string) *ParseError {
	line, column := calculateLineColumn(position)
	return &ParseError{
		Type:        ErrorTypeSyntax,
		Message:     message,
		Position:    position,
		Line:        line,
		Column:      column,
		Token:       token,
		Expected:    expected,
		Suggestions: generateSuggestions(token, expected),
		Recoverable: true,
	}
}

// CreateLexicalError 创建词法错误
func CreateLexicalError(message string, position int, char byte) *ParseError {
	line, column := calculateLineColumn(position)
	return &ParseError{
		Type:        ErrorTypeLexical,
		Message:     message,
		Position:    position,
		Line:        line,
		Column:      column,
		Token:       string(char),
		Suggestions: []string{"Check for invalid characters", "Ensure strings are properly closed"},
		Recoverable: false,
	}
}

// CreateLexicalErrorWithPosition 创建词法错误（带准确位置信息）
func CreateLexicalErrorWithPosition(message string, position int, line int, column int, char byte) *ParseError {
	return &ParseError{
		Type:        ErrorTypeLexical,
		Message:     message,
		Position:    position,
		Line:        line,
		Column:      column,
		Token:       string(char),
		Suggestions: []string{"Check for invalid characters", "Ensure strings are properly closed"},
		Recoverable: false,
	}
}

// CreateUnexpectedTokenError 创建意外token错误
func CreateUnexpectedTokenError(found string, expected []string, position int) *ParseError {
	line, column := calculateLineColumn(position)
	return &ParseError{
		Type:        ErrorTypeUnexpectedToken,
		Message:     fmt.Sprintf("Unexpected token '%s'", found),
		Position:    position,
		Line:        line,
		Column:      column,
		Token:       found,
		Expected:    expected,
		Suggestions: generateSuggestions(found, expected),
		Recoverable: true,
	}
}

// CreateMissingTokenError 创建缺失token错误
func CreateMissingTokenError(expected string, position int) *ParseError {
	line, column := calculateLineColumn(position)
	return &ParseError{
		Type:        ErrorTypeMissingToken,
		Message:     fmt.Sprintf("Missing required token '%s'", expected),
		Position:    position,
		Line:        line,
		Column:      column,
		Expected:    []string{expected},
		Suggestions: []string{fmt.Sprintf("Add missing '%s'", expected)},
		Recoverable: true,
	}
}

// CreateUnknownFunctionError 创建未知函数错误
func CreateUnknownFunctionError(functionName string, position int) *ParseError {
	line, column := calculateLineColumn(position)
	return &ParseError{
		Type:        ErrorTypeUnknownFunction,
		Message:     fmt.Sprintf("Unknown function '%s'", functionName),
		Position:    position,
		Line:        line,
		Column:      column,
		Token:       functionName,
		Suggestions: generateFunctionSuggestions(functionName),
		Recoverable: true,
	}
}

// calculateLineColumn 计算行列号
// 注意：这是一个简化的实现，实际的行列号应该由lexer提供
func calculateLineColumn(position int) (int, int) {
	// 简化实现，实际应该基于输入文本计算
	// 这里返回基于位置的估算值
	line := position/50 + 1  // 假设平均每行50个字符
	column := position%50 + 1
	return line, column
}

// generateSuggestions 生成建议
func generateSuggestions(found string, expected []string) []string {
	suggestions := make([]string, 0)
	
	if len(expected) > 0 {
		suggestions = append(suggestions, fmt.Sprintf("Try using '%s' instead of '%s'", expected[0], found))
	}
	
	// 基于常见错误模式生成建议
	switch strings.ToUpper(found) {
	case "SELCT":
		suggestions = append(suggestions, "Did you mean 'SELECT'?")
	case "FORM":
		suggestions = append(suggestions, "Did you mean 'FROM'?")
	case "WHER":
		suggestions = append(suggestions, "Did you mean 'WHERE'?")
	case "GROPU":
		suggestions = append(suggestions, "Did you mean 'GROUP'?")
	case "ODER":
		suggestions = append(suggestions, "Did you mean 'ORDER'?")
	}
	
	return suggestions
}

// generateFunctionSuggestions 生成函数建议
func generateFunctionSuggestions(functionName string) []string {
	suggestions := make([]string, 0)
	
	// 基于常见函数名拼写错误生成建议
	funcLower := strings.ToLower(functionName)
	switch {
	case strings.Contains(funcLower, "coun"):
		suggestions = append(suggestions, "Did you mean 'COUNT' function?")
	case strings.Contains(funcLower, "su") && strings.Contains(funcLower, "m"):
		suggestions = append(suggestions, "Did you mean 'SUM' function?")
	case strings.Contains(funcLower, "av") && strings.Contains(funcLower, "g"):
		suggestions = append(suggestions, "Did you mean 'AVG' function?")
	case strings.Contains(funcLower, "ma") && strings.Contains(funcLower, "x"):
		suggestions = append(suggestions, "Did you mean 'MAX' function?")
	case strings.Contains(funcLower, "mi") && strings.Contains(funcLower, "n"):
		suggestions = append(suggestions, "Did you mean 'MIN' function?")
	case strings.Contains(funcLower, "upp"):
		suggestions = append(suggestions, "Did you mean 'UPPER' function?")
	case strings.Contains(funcLower, "low"):
		suggestions = append(suggestions, "Did you mean 'LOWER' function?")
	case strings.Contains(funcLower, "len"):
		suggestions = append(suggestions, "Did you mean 'LENGTH' function?")
	case strings.Contains(funcLower, "sub"):
		suggestions = append(suggestions, "Did you mean 'SUBSTRING' function?")
	case strings.Contains(funcLower, "con"):
		suggestions = append(suggestions, "Did you mean 'CONCAT' function?")
	case strings.Contains(funcLower, "abs"):
		suggestions = append(suggestions, "Did you mean 'ABS' function?")
	case strings.Contains(funcLower, "sqrt"):
		suggestions = append(suggestions, "Did you mean 'SQRT' function?")
	case strings.Contains(funcLower, "round"):
		suggestions = append(suggestions, "Did you mean 'ROUND' function?")
	case strings.Contains(funcLower, "floor"):
		suggestions = append(suggestions, "Did you mean 'FLOOR' function?")
	case strings.Contains(funcLower, "ceil"):
		suggestions = append(suggestions, "Did you mean 'CEILING' function?")
	}
	
	// 通用建议
	suggestions = append(suggestions, "Check if the function name is spelled correctly")
	suggestions = append(suggestions, "Confirm that the function is registered or is a built-in function")
	suggestions = append(suggestions, "View the list of available functions")
	
	return suggestions
}

// FormatErrorContext 格式化错误上下文
func FormatErrorContext(input string, position int, contextLength int) string {
	if position < 0 || position >= len(input) {
		return ""
	}
	
	start := position - contextLength
	if start < 0 {
		start = 0
	}
	
	end := position + contextLength
	if end > len(input) {
		end = len(input)
	}
	
	context := input[start:end]
	pointer := strings.Repeat(" ", position-start) + "^"
	
	return fmt.Sprintf("%s\n%s", context, pointer)
}