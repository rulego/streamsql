package condition

import (
	"fmt"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

type Condition interface {
	Evaluate(env interface{}) bool
}

type ExprCondition struct {
	program *vm.Program
}

func NewExprCondition(expression string) (Condition, error) {
	// 添加自定义字符串函数支持（startsWith、endsWith、contains是内置操作符）
	options := []expr.Option{
		expr.Function("like_match", func(params ...any) (any, error) {
			if len(params) != 2 {
				return false, fmt.Errorf("like_match function requires 2 parameters")
			}
			text, ok1 := params[0].(string)
			pattern, ok2 := params[1].(string)
			if !ok1 || !ok2 {
				return false, fmt.Errorf("like_match function requires string parameters")
			}
			return matchesLikePattern(text, pattern), nil
		}),
		expr.AllowUndefinedVariables(),
		expr.AsBool(),
	}

	program, err := expr.Compile(expression, options...)
	if err != nil {
		return nil, err
	}
	return &ExprCondition{program: program}, nil
}

func (ec *ExprCondition) Evaluate(env interface{}) bool {
	result, err := expr.Run(ec.program, env)
	if err != nil {
		return false
	}
	return result.(bool)
}

// matchesLikePattern 实现LIKE模式匹配
// 支持%（匹配任意字符序列）和_（匹配单个字符）
func matchesLikePattern(text, pattern string) bool {
	return likeMatch(text, pattern, 0, 0)
}

// likeMatch 递归实现LIKE匹配算法
func likeMatch(text, pattern string, textIndex, patternIndex int) bool {
	// 如果模式已经匹配完成
	if patternIndex >= len(pattern) {
		return textIndex >= len(text) // 文本也应该匹配完成
	}

	// 如果文本已经结束，但模式还有非%字符，则不匹配
	if textIndex >= len(text) {
		// 检查剩余的模式是否都是%
		for i := patternIndex; i < len(pattern); i++ {
			if pattern[i] != '%' {
				return false
			}
		}
		return true
	}

	// 处理当前模式字符
	patternChar := pattern[patternIndex]

	if patternChar == '%' {
		// %可以匹配0个或多个字符
		// 尝试匹配0个字符（跳过%）
		if likeMatch(text, pattern, textIndex, patternIndex+1) {
			return true
		}
		// 尝试匹配1个或多个字符
		for i := textIndex; i < len(text); i++ {
			if likeMatch(text, pattern, i+1, patternIndex+1) {
				return true
			}
		}
		return false
	} else if patternChar == '_' {
		// _匹配恰好一个字符
		return likeMatch(text, pattern, textIndex+1, patternIndex+1)
	} else {
		// 普通字符必须精确匹配
		if text[textIndex] == patternChar {
			return likeMatch(text, pattern, textIndex+1, patternIndex+1)
		}
		return false
	}
}
