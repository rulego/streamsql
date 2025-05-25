package functions

import (
	"fmt"
	"github.com/rulego/streamsql/utils/cast"
	"math"
)

// AbsFunction 绝对值函数
type AbsFunction struct {
	*BaseFunction
}

func NewAbsFunction() *AbsFunction {
	return &AbsFunction{
		BaseFunction: NewBaseFunction("abs", TypeMath, "数学函数", "计算绝对值", 1, 1),
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

// SqrtFunction 平方根函数
type SqrtFunction struct {
	*BaseFunction
}

func NewSqrtFunction() *SqrtFunction {
	return &SqrtFunction{
		BaseFunction: NewBaseFunction("sqrt", TypeMath, "数学函数", "计算平方根", 1, 1),
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

// AcosFunction 反余弦函数
type AcosFunction struct {
	*BaseFunction
}

func NewAcosFunction() *AcosFunction {
	return &AcosFunction{
		BaseFunction: NewBaseFunction("acos", TypeMath, "数学函数", "计算反余弦值", 1, 1),
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

// AsinFunction 反正弦函数
type AsinFunction struct {
	*BaseFunction
}

func NewAsinFunction() *AsinFunction {
	return &AsinFunction{
		BaseFunction: NewBaseFunction("asin", TypeMath, "数学函数", "计算反正弦值", 1, 1),
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
		BaseFunction: NewBaseFunction("ceiling", TypeMath, "数学函数", "向上取整", 1, 1),
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

// PowerFunction 幂函数
type PowerFunction struct {
	*BaseFunction
}

func NewPowerFunction() *PowerFunction {
	return &PowerFunction{
		BaseFunction: NewBaseFunction("power", TypeMath, "数学函数", "计算x的y次幂", 2, 2),
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
