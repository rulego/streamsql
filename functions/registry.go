package functions

import (
	"fmt"
	"strings"
	"sync"
)

// FunctionType defines the enumeration of function types
type FunctionType string

const (
	// Aggregation functions
	TypeAggregation FunctionType = "aggregation"
	// Window functions
	TypeWindow FunctionType = "window"
	// Date and time functions
	TypeDateTime FunctionType = "datetime"
	// Conversion functions
	TypeConversion FunctionType = "conversion"
	// Math functions
	TypeMath FunctionType = "math"
	// String functions
	TypeString FunctionType = "string"
	// Analytical functions
	TypeAnalytical FunctionType = "analytical"
	// User-defined functions
	TypeCustom FunctionType = "custom"
)

// FunctionContext represents the execution context for functions
type FunctionContext struct {
	// Current data row
	Data map[string]interface{}
	// Window information (if applicable)
	WindowInfo *WindowInfo
	// Additional context information
	Extra map[string]interface{}
}

// WindowInfo contains window-related information
type WindowInfo struct {
	WindowStart int64
	WindowEnd   int64
	RowCount    int
}

// Function defines the interface for all functions
type Function interface {
	// GetName returns the function name
	GetName() string
	// GetType returns the function type
	GetType() FunctionType
	// GetCategory returns the function category
	GetCategory() string
	// GetAliases returns the function aliases
	GetAliases() []string
	// Validate validates the arguments
	Validate(args []interface{}) error
	// Execute executes the function
	Execute(ctx *FunctionContext, args []interface{}) (interface{}, error)
	// GetDescription returns the function description
	GetDescription() string

	// GetMinArgs returns the minimum number of arguments
	GetMinArgs() int
	// GetMaxArgs returns the maximum number of arguments (-1 means unlimited)
	GetMaxArgs() int
}

// FunctionRegistry manages function registration and retrieval
type FunctionRegistry struct {
	mu         sync.RWMutex
	functions  map[string]Function
	categories map[FunctionType][]Function
}

// Global function registry instance
var globalRegistry = NewFunctionRegistry()

// NewFunctionRegistry creates a new function registry
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		functions:  make(map[string]Function),
		categories: make(map[FunctionType][]Function),
	}
}

// Register registers a function
// 注册函数及其别名到注册表中
func (r *FunctionRegistry) Register(fn Function) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := strings.ToLower(fn.GetName())

	// Check if function already exists
	if _, exists := r.functions[name]; exists {
		return fmt.Errorf("function %s already registered", name)
	}

	// 注册主函数名
	r.functions[name] = fn

	// 注册所有别名
	for _, alias := range fn.GetAliases() {
		alias = strings.ToLower(alias)
		if _, exists := r.functions[alias]; exists {
			return fmt.Errorf("function alias %s already registered", alias)
		}
		r.functions[alias] = fn
	}

	r.categories[fn.GetType()] = append(r.categories[fn.GetType()], fn)
	// Register aggregator adapter
	if fn.GetType() == TypeAggregation {
		_ = RegisterAggregatorAdapter(fn.GetName())
	} else if fn.GetType() == TypeAnalytical {
		_ = RegisterAnalyticalAdapter(fn.GetName())
	}
	return nil
}

// Get retrieves a function
func (r *FunctionRegistry) Get(name string) (Function, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fn, exists := r.functions[strings.ToLower(name)]
	return fn, exists
}

// GetByType retrieves functions by type
func (r *FunctionRegistry) GetByType(fnType FunctionType) []Function {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.categories[fnType]
}

// ListAll lists all registered functions
func (r *FunctionRegistry) ListAll() map[string]Function {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]Function)
	for name, fn := range r.functions {
		result[name] = fn
	}
	return result
}

// Unregister removes a function
// 从注册表中移除函数及其所有别名
func (r *FunctionRegistry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	name = strings.ToLower(name)
	fn, exists := r.functions[name]
	if !exists {
		return false
	}

	// 删除主函数名
	delete(r.functions, strings.ToLower(fn.GetName()))

	// 删除所有别名
	for _, alias := range fn.GetAliases() {
		delete(r.functions, strings.ToLower(alias))
	}

	// Remove from categories
	fnType := fn.GetType()
	if funcs, ok := r.categories[fnType]; ok {
		for i, f := range funcs {
			if strings.ToLower(f.GetName()) == strings.ToLower(fn.GetName()) {
				r.categories[fnType] = append(funcs[:i], funcs[i+1:]...)
				break
			}
		}
	}

	return true
}

// Global function registration and retrieval methods
func Register(fn Function) error {
	return globalRegistry.Register(fn)
}

func Get(name string) (Function, bool) {
	return globalRegistry.Get(name)
}

func GetByType(fnType FunctionType) []Function {
	return globalRegistry.GetByType(fnType)
}

func ListAll() map[string]Function {
	return globalRegistry.ListAll()
}

func Unregister(name string) bool {
	return globalRegistry.Unregister(name)
}

// RegisterCustomFunction registers a custom function
func RegisterCustomFunction(name string, fnType FunctionType, category, description string,
	minArgs, maxArgs int, executor func(ctx *FunctionContext, args []interface{}) (interface{}, error)) error {

	// Validate function name
	if name == "" {
		return fmt.Errorf("function name cannot be empty")
	}

	customFunc := &CustomFunction{
		BaseFunction: NewBaseFunction(name, fnType, category, description, minArgs, maxArgs),
		executor:     executor,
	}

	return Register(customFunc)
}

// Execute executes a function
func Execute(name string, ctx *FunctionContext, args []interface{}) (interface{}, error) {
	fn, exists := Get(name)
	if !exists {
		return nil, fmt.Errorf("function %s not found", name)
	}

	if err := fn.Validate(args); err != nil {
		return nil, fmt.Errorf("function %s validation failed: %w", name, err)
	}

	return fn.Execute(ctx, args)
}

// CustomFunction implements custom function
type CustomFunction struct {
	*BaseFunction
	executor func(ctx *FunctionContext, args []interface{}) (interface{}, error)
}

func (f *CustomFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CustomFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return f.executor(ctx, args)
}

func init() {
	// Register math functions
	Register(NewAbsFunction())
	Register(NewSqrtFunction())
	Register(NewPowerFunction())
	Register(NewCeilingFunction())
	Register(NewFloorFunction())
	Register(NewRoundFunction())
	Register(NewModFunction())
	Register(NewMaxFunction())
	Register(NewMinFunction())
	Register(NewRandFunction())

	// Register string functions
	Register(NewUpperFunction())
	Register(NewLowerFunction())
	Register(NewLengthFunction())
	Register(NewSubstringFunction())
	Register(NewConcatFunction())
	Register(NewTrimFunction())
	Register(NewLtrimFunction())
	Register(NewRtrimFunction())
	Register(NewReplaceFunction())
	Register(NewSplitFunction())
	Register(NewStartswithFunction())
	Register(NewEndswithFunction())
	Register(NewRegexpMatchesFunction())
	Register(NewRegexpReplaceFunction())
	Register(NewLpadFunction())
	Register(NewRpadFunction())
	Register(NewIndexofFunction())
	Register(NewFormatFunction())

	// Register date and time functions
	Register(NewNowFunction())
	Register(NewCurrentTimeFunction())
	Register(NewCurrentDateFunction())
	Register(NewDateAddFunction())
	Register(NewDateSubFunction())
	Register(NewDateDiffFunction())
	Register(NewDateFormatFunction())
	Register(NewDateParseFunction())
	Register(NewExtractFunction())
	Register(NewUnixTimestampFunction())
	Register(NewFromUnixtimeFunction())
	Register(NewYearFunction())
	Register(NewMonthFunction())
	Register(NewDayFunction())
	Register(NewHourFunction())
	Register(NewMinuteFunction())
	Register(NewSecondFunction())
	Register(NewDayOfWeekFunction())
	Register(NewDayOfYearFunction())
	Register(NewWeekOfYearFunction())

	// Register conversion functions
	Register(NewCastFunction())
	Register(NewHex2DecFunction())
	Register(NewDec2HexFunction())
	Register(NewEncodeFunction())
	Register(NewDecodeFunction())

	// Register aggregation functions
	Register(NewCountFunction())
	Register(NewSumFunction())
	Register(NewAvgFunction())
	Register(NewMaxFunction())
	Register(NewMinFunction())

	// Register window functions
	Register(NewRowNumberFunction())
	Register(NewLagFunction())
	Register(NewLeadFunction())
	Register(NewFirstValueFunction())
	Register(NewNthValueFunction())

	// Register analytical functions
	Register(NewLatestFunction())
	Register(NewHadChangedFunction())

	// Register JSON functions
	Register(NewJsonExtractFunction())
	Register(NewJsonValidFunction())
	Register(NewJsonTypeFunction())
	Register(NewJsonLengthFunction())
	Register(NewToJsonFunction())
	Register(NewFromJsonFunction())

	// Register hash functions
	Register(NewMd5Function())
	Register(NewSha1Function())
	Register(NewSha256Function())
	Register(NewSha512Function())

	// Register array functions
	Register(NewArrayLengthFunction())
	Register(NewArrayContainsFunction())
	Register(NewArrayPositionFunction())
	Register(NewArrayRemoveFunction())
	Register(NewArrayDistinctFunction())
	Register(NewArrayIntersectFunction())
	Register(NewArrayUnionFunction())
	Register(NewArrayExceptFunction())

	// Register type checking functions
	Register(NewIsNullFunction())
	Register(NewIsNotNullFunction())
	Register(NewIsStringFunction())
	Register(NewIsNumericFunction())
	Register(NewIsBoolFunction())
	Register(NewIsArrayFunction())
	Register(NewIsObjectFunction())

	// Register conditional functions
	Register(NewCoalesceFunction())
	Register(NewNullIfFunction())
	Register(NewGreatestFunction())
	Register(NewLeastFunction())
}
