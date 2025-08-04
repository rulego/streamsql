package functions

import (
	"fmt"
)

// BaseFunction provides basic function implementation with common functionality
type BaseFunction struct {
	name        string
	fnType      FunctionType
	category    string
	description string
	minArgs     int
	maxArgs     int // -1 means unlimited
}

// NewBaseFunction creates a new base function
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

// ValidateArgCount validates the number of arguments
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
