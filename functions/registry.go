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
	Data map[string]any
	// Window information (if applicable)
	WindowInfo *WindowInfo
	// Additional context information
	Extra map[string]any
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
	Validate(args []any) error
	// Execute executes the function
	Execute(ctx *FunctionContext, args []any) (any, error)
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
	// snapshot caches the results of ListAll; Post-NIL of Register/Unregister fails and is rebuilt on demand.
	// The function set is basically stable after init, preventing ListAll from copying the entire table under a global lock every time.
	snapshot map[string]Function
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
// Register functions and their aliases into the registry
func (r *FunctionRegistry) Register(fn Function) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if function is nil
	if fn == nil {
		return fmt.Errorf("function cannot be nil")
	}

	name := strings.ToLower(fn.GetName())
	aliases := fn.GetAliases()

	// First, check that the primary name and all aliases are not occupying, then write everything together to avoid failure and leaving a semi-registered state.
	if _, exists := r.functions[name]; exists {
		return fmt.Errorf("function %s already registered", name)
	}
	for _, alias := range aliases {
		la := strings.ToLower(alias)
		if _, exists := r.functions[la]; exists {
			return fmt.Errorf("function alias %s already registered", la)
		}
	}

	// After all checks are passed, the primary and alias are uniformly written to the primary and aliases
	r.functions[name] = fn
	for _, alias := range aliases {
		r.functions[strings.ToLower(alias)] = fn
	}

	r.categories[fn.GetType()] = append(r.categories[fn.GetType()], fn)
	r.snapshot = nil // Expired ListAll snapshot
	// Register aggregator adapter
	if fn.GetType() == TypeAggregation {
		_ = RegisterAggregatorAdapter(fn.GetName())
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
// Returns a read-only snapshot from the cache (the caller cannot be modified); Failure and inert reconstruction after Register/Unregister.
func (r *FunctionRegistry) ListAll() map[string]Function {
	r.mu.RLock()
	if r.snapshot != nil {
		s := r.snapshot
		r.mu.RUnlock()
		return s
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snapshot != nil {
		return r.snapshot
	}
	result := make(map[string]Function, len(r.functions))
	for name, fn := range r.functions {
		result[name] = fn
	}
	r.snapshot = result
	return result
}

// Unregister removes a function
// Remove functions and all aliases from the registry
func (r *FunctionRegistry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	name = strings.ToLower(name)
	fn, exists := r.functions[name]
	if !exists {
		return false
	}

	// Delete the main function name
	delete(r.functions, strings.ToLower(fn.GetName()))

	// Delete all aliases
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
	r.snapshot = nil // Expired ListAll snapshot

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

// Validate validates if a function exists in the registry
func Validate(name string) error {
	_, exists := Get(name)
	if !exists {
		return fmt.Errorf("function '%s' not found", name)
	}
	return nil
}

// RegisterCustomFunction registers a custom function
func RegisterCustomFunction(name string, fnType FunctionType, category, description string,
	minArgs, maxArgs int, executor func(ctx *FunctionContext, args []any) (any, error)) error {

	// Validate function name
	if name == "" {
		return fmt.Errorf("function name cannot be empty")
	}
	// The aggregation/analysis function requires implementing the AggregatorFunction / StatefulAnalytic interface; closure format cannot meet the requirements,
	// After registration, it will only become silent and invalid; Please change it to implementing the corresponding interface and then use functions.Register.
	if fnType == TypeAggregation || fnType == TypeAnalytical {
		return fmt.Errorf("RegisterCustomFunction 不支持 %s 类型：聚合/分析函数请实现对应接口后用 Register 注册", fnType)
	}

	customFunc := &CustomFunction{
		BaseFunction: NewBaseFunction(name, fnType, category, description, minArgs, maxArgs),
		executor:     executor,
	}

	return Register(customFunc)
}

// Execute executes a function
func Execute(name string, ctx *FunctionContext, args []any) (any, error) {
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
	executor func(ctx *FunctionContext, args []any) (any, error)
}

func (f *CustomFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *CustomFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return f.executor(ctx, args)
}

// For built-in function registration, see registerBuiltinFunctions in builtin.go (called by init() in init.go).
