package functions

import (
	"fmt"

	"github.com/rulego/streamsql/utils/cast"
)

// ExprFunction is an expr function used to execute expressions in SQL
type ExprFunction struct {
	*BaseFunction
}

func NewExprFunction() *ExprFunction {
	return &ExprFunction{
		BaseFunction: NewBaseFunction("expr", TypeString, "表达式函数", "执行表达式并返回结果", 1, 1),
	}
}

func (f *ExprFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *ExprFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("expr function requires exactly 1 argument")
	}

	// Get the expression string
	expressionStr, err := cast.ToStringE(args[0])
	if err != nil {
		return nil, fmt.Errorf("expr function argument must be a string: %v", err)
	}

	// Executing expressions using ExprBridge
	bridge := GetExprBridge()
	result, err := bridge.EvaluateExpression(expressionStr, ctx.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression '%s': %v", expressionStr, err)
	}

	return result, nil
}
