package functions

import (
	"fmt"
)

// BaseFunction 基础函数实现，提供通用功能
type BaseFunction struct {
	name        string
	fnType      FunctionType
	category    string
	description string
	minArgs     int
	maxArgs     int // -1 表示无限制
}

// NewBaseFunction 创建基础函数
func NewBaseFunction(name string, fnType FunctionType, category, description string, minArgs, maxArgs int) *BaseFunction {
	return &BaseFunction{
		name:        name,
		fnType:      fnType,
		category:    category,
		description: description,
		minArgs:     minArgs,
		maxArgs:     maxArgs,
	}
}

func (bf *BaseFunction) GetName() string {
	return bf.name
}

func (bf *BaseFunction) GetType() FunctionType {
	return bf.fnType
}

func (bf *BaseFunction) GetCategory() string {
	return bf.category
}

func (bf *BaseFunction) GetDescription() string {
	return bf.description
}

// ValidateArgCount 验证参数数量
func (bf *BaseFunction) ValidateArgCount(args []interface{}) error {
	argCount := len(args)

	if argCount < bf.minArgs {
		return fmt.Errorf("function %s requires at least %d arguments, got %d", bf.name, bf.minArgs, argCount)
	}

	if bf.maxArgs != -1 && argCount > bf.maxArgs {
		return fmt.Errorf("function %s accepts at most %d arguments, got %d", bf.name, bf.maxArgs, argCount)
	}

	return nil
}
