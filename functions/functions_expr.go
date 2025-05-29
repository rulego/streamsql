package functions

import (
	"fmt"

	"github.com/rulego/streamsql/utils/cast"
)

// ExprFunction expr函数，用于在SQL中执行表达式
type ExprFunction struct {
	*BaseFunction
}

func NewExprFunction() *ExprFunction {
	return &ExprFunction{
		BaseFunction: NewBaseFunction("expr", TypeString, "表达式函数", "执行表达式并返回结果", 1, 1),
	}
}

func (f *ExprFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ExprFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("expr function requires exactly 1 argument")
	}

	// 获取表达式字符串
	expressionStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("expr function argument must be a string: %v", err)
	}

	// 使用 ExprBridge 执行表达式
	bridge := GetExprBridge()
	result, err := bridge.EvaluateExpression(expressionStr, ctx.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression '%s': %v", expressionStr, err)
	}

	return result, nil
}
