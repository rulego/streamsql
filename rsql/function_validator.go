package rsql

import (
	"regexp"
	"strings"

	"github.com/rulego/streamsql/functions"
)

// FunctionValidator 函数验证器
type FunctionValidator struct {
	errorRecovery *ErrorRecovery
}

// NewFunctionValidator 创建函数验证器
func NewFunctionValidator(errorRecovery *ErrorRecovery) *FunctionValidator {
	return &FunctionValidator{
		errorRecovery: errorRecovery,
	}
}

// ValidateExpression 验证表达式中的函数
func (fv *FunctionValidator) ValidateExpression(expression string, position int) {
	functionCalls := fv.extractFunctionCalls(expression)
	
	for _, funcCall := range functionCalls {
		funcName := funcCall.Name
		
		// 检查函数是否在注册表中
		if _, exists := functions.Get(funcName); !exists {
			// 检查是否是内置函数
			if !fv.isBuiltinFunction(funcName) {
				// 检查是否是expr-lang函数
				bridge := functions.GetExprBridge()
				if !bridge.IsExprLangFunction(funcName) {
					// 创建未知函数错误
					err := CreateUnknownFunctionError(funcName, position+funcCall.Position)
					fv.errorRecovery.AddError(err)
				}
			}
		}
	}
}

// FunctionCall 函数调用信息
type FunctionCall struct {
	Name     string
	Position int
}

// extractFunctionCalls 从表达式中提取函数调用
func (fv *FunctionValidator) extractFunctionCalls(expression string) []FunctionCall {
	var functionCalls []FunctionCall
	
	// 使用正则表达式匹配函数调用模式: identifier(
	funcPattern := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*\(`)
	matches := funcPattern.FindAllStringSubmatchIndex(expression, -1)
	
	for _, match := range matches {
		// match[0] 是整个匹配的开始位置
		// match[1] 是整个匹配的结束位置
		// match[2] 是第一个捕获组（函数名）的开始位置
		// match[3] 是第一个捕获组（函数名）的结束位置
		funcName := expression[match[2]:match[3]]
		position := match[2]
		
		// 过滤掉关键字（如 CASE、IF 等）
		if !fv.isKeyword(funcName) {
			functionCalls = append(functionCalls, FunctionCall{
				Name:     funcName,
				Position: position,
			})
		}
	}
	
	return functionCalls
}

// isBuiltinFunction 检查是否是内置函数
func (fv *FunctionValidator) isBuiltinFunction(funcName string) bool {
	builtinFunctions := []string{
		"abs", "sqrt", "sin", "cos", "tan", "floor", "ceil", "round",
		"log", "log10", "exp", "pow", "mod",
	}
	
	funcLower := strings.ToLower(funcName)
	for _, builtin := range builtinFunctions {
		if funcLower == builtin {
			return true
		}
	}
	return false
}

// isKeyword 检查是否是SQL关键字
func (fv *FunctionValidator) isKeyword(word string) bool {
	keywords := []string{
		"SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER",
		"LIMIT", "DISTINCT", "AS", "AND", "OR", "NOT", "IN", "LIKE",
		"BETWEEN", "IS", "NULL", "TRUE", "FALSE", "CASE", "WHEN",
		"THEN", "ELSE", "END", "IF", "CAST", "CONVERT",
	}
	
	wordUpper := strings.ToUpper(word)
	for _, keyword := range keywords {
		if wordUpper == keyword {
			return true
		}
	}
	return false
}