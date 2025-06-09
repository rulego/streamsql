package functions

import (
	"fmt"
	"strings"
	"sync"
)

// FunctionType 函数类型枚举
type FunctionType string

const (
	// 聚合函数
	TypeAggregation FunctionType = "aggregation"
	// 窗口函数
	TypeWindow FunctionType = "window"
	// 时间日期函数
	TypeDateTime FunctionType = "datetime"
	// 转换函数
	TypeConversion FunctionType = "conversion"
	// 数学函数
	TypeMath FunctionType = "math"
	// 字符串函数
	TypeString FunctionType = "string"
	// 分析函数
	TypeAnalytical FunctionType = "analytical"
	// 用户自定义函数
	TypeCustom FunctionType = "custom"
)

// FunctionContext 函数执行上下文
type FunctionContext struct {
	// 当前数据行
	Data map[string]interface{}
	// 窗口信息（如果适用）
	WindowInfo *WindowInfo
	// 其他上下文信息
	Extra map[string]interface{}
}

// WindowInfo 窗口信息
type WindowInfo struct {
	WindowStart int64
	WindowEnd   int64
	RowCount    int
}

// Function 函数接口定义
type Function interface {
	// GetName 获取函数名称
	GetName() string
	// GetType 获取函数类型
	GetType() FunctionType
	// GetCategory 获取函数分类
	GetCategory() string
	// Validate 验证参数
	Validate(args []interface{}) error
	// Execute 执行函数
	Execute(ctx *FunctionContext, args []interface{}) (interface{}, error)
	// GetDescription 获取函数描述
	GetDescription() string
}

// FunctionRegistry 函数注册器
type FunctionRegistry struct {
	mu         sync.RWMutex
	functions  map[string]Function
	categories map[FunctionType][]Function
}

// 全局函数注册器实例
var globalRegistry = NewFunctionRegistry()

// NewFunctionRegistry 创建新的函数注册器
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		functions:  make(map[string]Function),
		categories: make(map[FunctionType][]Function),
	}
}

// Register 注册函数
func (r *FunctionRegistry) Register(fn Function) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := strings.ToLower(fn.GetName())

	// 检查函数是否已存在
	if _, exists := r.functions[name]; exists {
		return fmt.Errorf("function %s already registered", name)
	}

	r.functions[name] = fn
	r.categories[fn.GetType()] = append(r.categories[fn.GetType()], fn)
	//注册聚合函数适配器
	if fn.GetType() == TypeAggregation {
		_ = RegisterAggregatorAdapter(fn.GetName())
	} else if fn.GetType() == TypeAnalytical {
		_ = RegisterAnalyticalAdapter(fn.GetName())
	}
	return nil
}

// Get 获取函数
func (r *FunctionRegistry) Get(name string) (Function, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fn, exists := r.functions[strings.ToLower(name)]
	return fn, exists
}

// GetByType 按类型获取函数列表
func (r *FunctionRegistry) GetByType(fnType FunctionType) []Function {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.categories[fnType]
}

// ListAll 列出所有注册的函数
func (r *FunctionRegistry) ListAll() map[string]Function {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]Function)
	for name, fn := range r.functions {
		result[name] = fn
	}
	return result
}

// Unregister 注销函数
func (r *FunctionRegistry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	name = strings.ToLower(name)
	fn, exists := r.functions[name]
	if !exists {
		return false
	}

	delete(r.functions, name)

	// 从分类中移除
	fnType := fn.GetType()
	if funcs, ok := r.categories[fnType]; ok {
		for i, f := range funcs {
			if strings.ToLower(f.GetName()) == name {
				r.categories[fnType] = append(funcs[:i], funcs[i+1:]...)
				break
			}
		}
	}

	return true
}

// 全局函数注册和获取方法
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

// RegisterCustomFunction 注册自定义函数
func RegisterCustomFunction(name string, fnType FunctionType, category, description string,
	minArgs, maxArgs int, executor func(ctx *FunctionContext, args []interface{}) (interface{}, error)) error {

	customFunc := &CustomFunction{
		BaseFunction: NewBaseFunction(name, fnType, category, description, minArgs, maxArgs),
		executor:     executor,
	}

	return Register(customFunc)
}

// Execute 执行函数
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

// CustomFunction 自定义函数实现
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
	// 注册数学函数
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

	// 注册字符串函数
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

	// 注册时间日期函数
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

	// 注册转换函数
	Register(NewCastFunction())
	Register(NewHex2DecFunction())
	Register(NewDec2HexFunction())
	Register(NewEncodeFunction())
	Register(NewDecodeFunction())

	// 注册聚合函数
	Register(NewCountFunction())
	Register(NewSumFunction())
	Register(NewAvgFunction())
	Register(NewMaxFunction())
	Register(NewMinFunction())

	// 注册窗口函数
	Register(NewRowNumberFunction())
	Register(NewLagFunction())
	Register(NewLeadFunction())
	Register(NewFirstValueFunction())
	Register(NewNthValueFunction())

	// 注册分析函数
	Register(NewLatestFunction())
	Register(NewHadChangedFunction())

	// 注册JSON函数
	Register(NewJsonExtractFunction())
	Register(NewJsonValidFunction())
	Register(NewJsonTypeFunction())
	Register(NewJsonLengthFunction())
	Register(NewToJsonFunction())
	Register(NewFromJsonFunction())

	// 注册哈希函数
	Register(NewMd5Function())
	Register(NewSha1Function())
	Register(NewSha256Function())
	Register(NewSha512Function())

	// 注册数组函数
	Register(NewArrayLengthFunction())
	Register(NewArrayContainsFunction())
	Register(NewArrayPositionFunction())
	Register(NewArrayRemoveFunction())
	Register(NewArrayDistinctFunction())
	Register(NewArrayIntersectFunction())
	Register(NewArrayUnionFunction())
	Register(NewArrayExceptFunction())

	// 注册类型检查函数
	Register(NewIsNullFunction())
	Register(NewIsNotNullFunction())
	Register(NewIsStringFunction())
	Register(NewIsNumericFunction())
	Register(NewIsBoolFunction())
	Register(NewIsArrayFunction())
	Register(NewIsObjectFunction())

	// 注册条件函数
	Register(NewCoalesceFunction())
	Register(NewNullIfFunction())
	Register(NewGreatestFunction())
	Register(NewLeastFunction())
}
