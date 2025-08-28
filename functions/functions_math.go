package functions

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/rulego/streamsql/utils/cast"
)

// AbsFunction calculates absolute value
type AbsFunction struct {
	*BaseFunction
}

func NewAbsFunction() *AbsFunction {
	return &AbsFunction{
		BaseFunction: NewBaseFunction("abs", TypeMath, "math", "Calculate absolute value", 1, 1),
	}
}

func (f *AbsFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *AbsFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Abs(val), nil
}

// SqrtFunction calculates square root
type SqrtFunction struct {
	*BaseFunction
}

func NewSqrtFunction() *SqrtFunction {
	return &SqrtFunction{
		BaseFunction: NewBaseFunction("sqrt", TypeMath, "math", "Calculate square root", 1, 1),
	}
}

func (f *SqrtFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *SqrtFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	if val < 0 {
		return nil, fmt.Errorf("sqrt of negative number")
	}
	return math.Sqrt(val), nil
}

// AcosFunction calculates arccosine
type AcosFunction struct {
	*BaseFunction
}

func NewAcosFunction() *AcosFunction {
	return &AcosFunction{
		BaseFunction: NewBaseFunction("acos", TypeMath, "math", "Calculate arccosine value", 1, 1),
	}
}

func (f *AcosFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *AcosFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	if val < -1 || val > 1 {
		return nil, fmt.Errorf("acos: value out of range [-1,1]")
	}
	return math.Acos(val), nil
}

// AsinFunction calculates arcsine
type AsinFunction struct {
	*BaseFunction
}

func NewAsinFunction() *AsinFunction {
	return &AsinFunction{
		BaseFunction: NewBaseFunction("asin", TypeMath, "math", "Calculate arcsine value", 1, 1),
	}
}

func (f *AsinFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *AsinFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	if val < -1 || val > 1 {
		return nil, fmt.Errorf("asin: value out of range [-1,1]")
	}
	return math.Asin(val), nil
}

// AtanFunction 反正切函数
type AtanFunction struct {
	*BaseFunction
}

func NewAtanFunction() *AtanFunction {
	return &AtanFunction{
		BaseFunction: NewBaseFunction("atan", TypeMath, "数学函数", "计算反正切值", 1, 1),
	}
}

func (f *AtanFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *AtanFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Atan(val), nil
}

// Atan2Function 两个参数的反正切函数
type Atan2Function struct {
	*BaseFunction
}

func NewAtan2Function() *Atan2Function {
	return &Atan2Function{
		BaseFunction: NewBaseFunction("atan2", TypeMath, "数学函数", "计算两个参数的反正切值", 2, 2),
	}
}

func (f *Atan2Function) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *Atan2Function) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	y, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	x, err := cast.ToFloat64E(args[1])
	if err != nil {
		return nil, err
	}
	return math.Atan2(y, x), nil
}

// BitAndFunction 按位与函数
type BitAndFunction struct {
	*BaseFunction
}

func NewBitAndFunction() *BitAndFunction {
	return &BitAndFunction{
		BaseFunction: NewBaseFunction("bitand", TypeMath, "数学函数", "计算两个整数的按位与", 2, 2),
	}
}

func (f *BitAndFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *BitAndFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	a, err := cast.ToInt64E(args[0])
	if err != nil {
		return nil, err
	}
	b, err := cast.ToInt64E(args[1])
	if err != nil {
		return nil, err
	}
	return a & b, nil
}

// BitOrFunction 按位或函数
type BitOrFunction struct {
	*BaseFunction
}

func NewBitOrFunction() *BitOrFunction {
	return &BitOrFunction{
		BaseFunction: NewBaseFunction("bitor", TypeMath, "数学函数", "计算两个整数的按位或", 2, 2),
	}
}

func (f *BitOrFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *BitOrFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	a, err := cast.ToInt64E(args[0])
	if err != nil {
		return nil, err
	}
	b, err := cast.ToInt64E(args[1])
	if err != nil {
		return nil, err
	}
	return a | b, nil
}

// BitXorFunction 按位异或函数
type BitXorFunction struct {
	*BaseFunction
}

func NewBitXorFunction() *BitXorFunction {
	return &BitXorFunction{
		BaseFunction: NewBaseFunction("bitxor", TypeMath, "数学函数", "计算两个整数的按位异或", 2, 2),
	}
}

func (f *BitXorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *BitXorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	a, err := cast.ToInt64E(args[0])
	if err != nil {
		return nil, err
	}
	b, err := cast.ToInt64E(args[1])
	if err != nil {
		return nil, err
	}
	return a ^ b, nil
}

// BitNotFunction 按位非函数
type BitNotFunction struct {
	*BaseFunction
}

func NewBitNotFunction() *BitNotFunction {
	return &BitNotFunction{
		BaseFunction: NewBaseFunction("bitnot", TypeMath, "数学函数", "计算整数的按位非", 1, 1),
	}
}

func (f *BitNotFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *BitNotFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	a, err := cast.ToInt64E(args[0])
	if err != nil {
		return nil, err
	}
	return ^a, nil
}

// CeilingFunction 向上取整函数
type CeilingFunction struct {
	*BaseFunction
}

func NewCeilingFunction() *CeilingFunction {
	return &CeilingFunction{
		BaseFunction: NewBaseFunctionWithAliases("ceiling", TypeMath, "数学函数", "向上取整", 1, 1, []string{"ceil"}),
	}
}

func (f *CeilingFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CeilingFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Ceil(val), nil
}

// CosFunction 余弦函数
type CosFunction struct {
	*BaseFunction
}

func NewCosFunction() *CosFunction {
	return &CosFunction{
		BaseFunction: NewBaseFunction("cos", TypeMath, "数学函数", "计算余弦值", 1, 1),
	}
}

func (f *CosFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CosFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Cos(val), nil
}

// CoshFunction 双曲余弦函数
type CoshFunction struct {
	*BaseFunction
}

func NewCoshFunction() *CoshFunction {
	return &CoshFunction{
		BaseFunction: NewBaseFunction("cosh", TypeMath, "数学函数", "计算双曲余弦值", 1, 1),
	}
}

func (f *CoshFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CoshFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Cosh(val), nil
}

// ExpFunction 指数函数
type ExpFunction struct {
	*BaseFunction
}

func NewExpFunction() *ExpFunction {
	return &ExpFunction{
		BaseFunction: NewBaseFunction("exp", TypeMath, "数学函数", "计算e的幂", 1, 1),
	}
}

func (f *ExpFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ExpFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Exp(val), nil
}

// FloorFunction 向下取整函数
type FloorFunction struct {
	*BaseFunction
}

func NewFloorFunction() *FloorFunction {
	return &FloorFunction{
		BaseFunction: NewBaseFunction("floor", TypeMath, "数学函数", "向下取整", 1, 1),
	}
}

func (f *FloorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *FloorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Floor(val), nil
}

// LnFunction 自然对数函数
type LnFunction struct {
	*BaseFunction
}

func NewLnFunction() *LnFunction {
	return &LnFunction{
		BaseFunction: NewBaseFunction("ln", TypeMath, "数学函数", "计算自然对数", 1, 1),
	}
}

func (f *LnFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *LnFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	if val <= 0 {
		return nil, fmt.Errorf("ln: value must be positive")
	}
	return math.Log(val), nil
}

// LogFunction 以10为底的对数函数 (log的别名)
type LogFunction struct {
	*BaseFunction
}

func NewLogFunction() *LogFunction {
	return &LogFunction{
		BaseFunction: NewBaseFunction("log", TypeMath, "数学函数", "计算以10为底的对数", 1, 1),
	}
}

func (f *LogFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *LogFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	if val <= 0 {
		return nil, fmt.Errorf("log: value must be positive")
	}
	return math.Log10(val), nil
}

// Log10Function 以10为底的对数函数
type Log10Function struct {
	*BaseFunction
}

func NewLog10Function() *Log10Function {
	return &Log10Function{
		BaseFunction: NewBaseFunction("log10", TypeMath, "数学函数", "计算以10为底的对数", 1, 1),
	}
}

func (f *Log10Function) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *Log10Function) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	if val <= 0 {
		return nil, fmt.Errorf("log10: value must be positive")
	}
	return math.Log10(val), nil
}

// Log2Function 以2为底的对数函数
type Log2Function struct {
	*BaseFunction
}

func NewLog2Function() *Log2Function {
	return &Log2Function{
		BaseFunction: NewBaseFunction("log2", TypeMath, "数学函数", "计算以2为底的对数", 1, 1),
	}
}

func (f *Log2Function) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *Log2Function) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	if val <= 0 {
		return nil, fmt.Errorf("log2: value must be positive")
	}
	return math.Log2(val), nil
}

// ModFunction 取模函数
type ModFunction struct {
	*BaseFunction
}

func NewModFunction() *ModFunction {
	return &ModFunction{
		BaseFunction: NewBaseFunction("mod", TypeMath, "数学函数", "取模运算", 2, 2),
	}
}

func (f *ModFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *ModFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	x, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	y, err := cast.ToFloat64E(args[1])
	if err != nil {
		return nil, err
	}
	if y == 0 {
		return nil, fmt.Errorf("mod: division by zero")
	}
	return math.Mod(x, y), nil
}

// RandFunction 随机数函数
type RandFunction struct {
	*BaseFunction
}

func NewRandFunction() *RandFunction {
	return &RandFunction{
		BaseFunction: NewBaseFunction("rand", TypeMath, "数学函数", "生成0-1之间的随机数", 0, 0),
	}
}

func (f *RandFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *RandFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 使用当前时间作为种子
	rand.Seed(time.Now().UnixNano())
	return rand.Float64(), nil
}

// RoundFunction 四舍五入函数
type RoundFunction struct {
	*BaseFunction
}

func NewRoundFunction() *RoundFunction {
	return &RoundFunction{
		BaseFunction: NewBaseFunction("round", TypeMath, "数学函数", "四舍五入", 1, 2),
	}
}

func (f *RoundFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *RoundFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 检查第一个参数是否为nil
	if args[0] == nil {
		return nil, nil
	}

	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}

	if len(args) == 1 {
		return math.Round(val), nil
	}

	// 检查第二个参数是否为nil（如果存在）
	if args[1] == nil {
		return nil, nil
	}

	precision, err := cast.ToIntE(args[1])
	if err != nil {
		return nil, err
	}

	shift := math.Pow(10, float64(precision))
	return math.Round(val*shift) / shift, nil
}

// SignFunction 符号函数
type SignFunction struct {
	*BaseFunction
}

func NewSignFunction() *SignFunction {
	return &SignFunction{
		BaseFunction: NewBaseFunction("sign", TypeMath, "数学函数", "返回数字的符号", 1, 1),
	}
}

func (f *SignFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *SignFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}

	if val > 0 {
		return 1, nil
	} else if val < 0 {
		return -1, nil
	}
	return 0, nil
}

// SinFunction 正弦函数
type SinFunction struct {
	*BaseFunction
}

func NewSinFunction() *SinFunction {
	return &SinFunction{
		BaseFunction: NewBaseFunction("sin", TypeMath, "数学函数", "计算正弦值", 1, 1),
	}
}

func (f *SinFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *SinFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Sin(val), nil
}

// SinhFunction 双曲正弦函数
type SinhFunction struct {
	*BaseFunction
}

func NewSinhFunction() *SinhFunction {
	return &SinhFunction{
		BaseFunction: NewBaseFunction("sinh", TypeMath, "数学函数", "计算双曲正弦值", 1, 1),
	}
}

func (f *SinhFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *SinhFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Sinh(val), nil
}

// TanFunction 正切函数
type TanFunction struct {
	*BaseFunction
}

func NewTanFunction() *TanFunction {
	return &TanFunction{
		BaseFunction: NewBaseFunction("tan", TypeMath, "数学函数", "计算正切值", 1, 1),
	}
}

func (f *TanFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *TanFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Tan(val), nil
}

// TanhFunction 双曲正切函数
type TanhFunction struct {
	*BaseFunction
}

func NewTanhFunction() *TanhFunction {
	return &TanhFunction{
		BaseFunction: NewBaseFunction("tanh", TypeMath, "数学函数", "计算双曲正切值", 1, 1),
	}
}

func (f *TanhFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *TanhFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	val, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	return math.Tanh(val), nil
}

// PowerFunction 幂函数
type PowerFunction struct {
	*BaseFunction
}

func NewPowerFunction() *PowerFunction {
	return &PowerFunction{
		BaseFunction: NewBaseFunctionWithAliases("power", TypeMath, "数学函数", "计算x的y次幂", 2, 2, []string{"pow"}),
	}
}

func (f *PowerFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *PowerFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	x, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	y, err := cast.ToFloat64E(args[1])
	if err != nil {
		return nil, err
	}
	return math.Pow(x, y), nil
}
