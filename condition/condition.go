package condition

import (
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
	program, err := expr.Compile(expression)
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
