package functions

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/rulego/streamsql/utils/cast"
)

// SumFunction calculates the sum of numeric values
type SumFunction struct {
	*BaseFunction
	value     float64
	hasValues bool // Flag to track if there are non-NULL values
}

func NewSumFunction() *SumFunction {
	return &SumFunction{
		BaseFunction: NewBaseFunction("sum", TypeAggregation, "aggregation", "Calculate sum of numeric values", 1, -1),
		hasValues:    false,
	}
}

func (f *SumFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *SumFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	sum := 0.0
	hasValues := false
	for _, arg := range args {
		if arg == nil {
			continue // Ignore NULL values
		}
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue // Ignore values that cannot be converted
		}
		sum += val
		hasValues = true
	}
	if !hasValues {
		return nil, nil // Return NULL when no valid values
	}
	return sum, nil
}

// Implement AggregatorFunction interface
func (f *SumFunction) New() AggregatorFunction {
	return &SumFunction{
		BaseFunction: f.BaseFunction,
		value:        0,
		hasValues:    false,
	}
}

func (f *SumFunction) Add(value interface{}) {
	// Enhanced Add method: ignore NULL values
	if value == nil {
		return // Ignore NULL values
	}

	if val, err := cast.ToFloat64E(value); err == nil {
		f.value += val
		f.hasValues = true
	}
	// Ignore values that fail conversion
}

func (f *SumFunction) Result() interface{} {
	if !f.hasValues {
		return nil // Return NULL when no valid values instead of 0.0
	}
	return f.value
}

func (f *SumFunction) Reset() {
	f.value = 0
	f.hasValues = false
}

func (f *SumFunction) Clone() AggregatorFunction {
	return &SumFunction{
		BaseFunction: f.BaseFunction,
		value:        f.value,
		hasValues:    f.hasValues,
	}
}

// AvgFunction calculates the average of numeric values
type AvgFunction struct {
	*BaseFunction
	sum   float64
	count int
}

func NewAvgFunction() *AvgFunction {
	return &AvgFunction{
		BaseFunction: NewBaseFunction("avg", TypeAggregation, "aggregation", "Calculate average of numeric values", 1, -1),
	}
}

func (f *AvgFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *AvgFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	sum := 0.0
	count := 0
	for _, arg := range args {
		if arg == nil {
			continue // Ignore NULL values
		}
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue // Ignore values that cannot be converted
		}
		sum += val
		count++
	}
	if count == 0 {
		return nil, nil // Return nil when no valid values
	}
	return sum / float64(count), nil
}

// 实现AggregatorFunction接口
func (f *AvgFunction) New() AggregatorFunction {
	return &AvgFunction{
		BaseFunction: f.BaseFunction,
		sum:          0,
		count:        0,
	}
}

func (f *AvgFunction) Add(value interface{}) {
	// Enhanced Add method: ignore NULL values
	if value == nil {
		return // Ignore NULL values
	}

	if val, err := cast.ToFloat64E(value); err == nil {
		f.sum += val
		f.count++
	}
	// Ignore values that fail conversion
}

func (f *AvgFunction) Result() interface{} {
	if f.count == 0 {
		return nil // Return NULL when no valid values according to SQL standard
	}
	return f.sum / float64(f.count)
}

func (f *AvgFunction) Reset() {
	f.sum = 0
	f.count = 0
}

func (f *AvgFunction) Clone() AggregatorFunction {
	return &AvgFunction{
		BaseFunction: f.BaseFunction,
		sum:          f.sum,
		count:        f.count,
	}
}

// MinFunction calculates the minimum value
type MinFunction struct {
	*BaseFunction
	value float64
	first bool
}

func NewMinFunction() *MinFunction {
	return &MinFunction{
		BaseFunction: NewBaseFunction("min", TypeAggregation, "aggregation", "Calculate minimum value", 1, -1),
		first:        true,
	}
}

func (f *MinFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *MinFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 检查是否有nil参数
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
	}
	
	min := math.Inf(1)
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			return nil, err
		}
		if val < min {
			min = val
		}
	}
	return min, nil
}

// Implement AggregatorFunction interface
func (f *MinFunction) New() AggregatorFunction {
	return &MinFunction{
		BaseFunction: f.BaseFunction,
		first:        true,
	}
}

func (f *MinFunction) Add(value interface{}) {
	// Enhanced Add method: ignore NULL values
	if value == nil {
		return // Ignore NULL values
	}

	if val, err := cast.ToFloat64E(value); err == nil {
		if f.first || val < f.value {
			f.value = val
			f.first = false
		}
	}
}

func (f *MinFunction) Result() interface{} {
	if f.first {
		return nil // Return NULL when no data according to SQL standard
	}
	return f.value
}

func (f *MinFunction) Reset() {
	f.first = true
	f.value = 0
}

func (f *MinFunction) Clone() AggregatorFunction {
	return &MinFunction{
		BaseFunction: f.BaseFunction,
		value:        f.value,
		first:        f.first,
	}
}

// MaxFunction calculates the maximum value
type MaxFunction struct {
	*BaseFunction
	value float64
	first bool
}

func NewMaxFunction() *MaxFunction {
	return &MaxFunction{
		BaseFunction: NewBaseFunction("max", TypeAggregation, "aggregation", "Calculate maximum value", 1, -1),
		first:        true,
	}
}

func (f *MaxFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *MaxFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 检查是否有nil参数
	for _, arg := range args {
		if arg == nil {
			return nil, nil
		}
	}
	
	max := math.Inf(-1)
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			return nil, err
		}
		if val > max {
			max = val
		}
	}
	return max, nil
}

// Implement AggregatorFunction interface
func (f *MaxFunction) New() AggregatorFunction {
	return &MaxFunction{
		BaseFunction: f.BaseFunction,
		first:        true,
	}
}

func (f *MaxFunction) Add(value interface{}) {
	// Enhanced Add method: ignore NULL values
	if value == nil {
		return // Ignore NULL values
	}

	if val, err := cast.ToFloat64E(value); err == nil {
		if f.first || val > f.value {
			f.value = val
			f.first = false
		}
	}
}

func (f *MaxFunction) Result() interface{} {
	if f.first {
		return nil // Return NULL when no data according to SQL standard
	}
	return f.value
}

func (f *MaxFunction) Reset() {
	f.first = true
	f.value = 0
}

func (f *MaxFunction) Clone() AggregatorFunction {
	return &MaxFunction{
		BaseFunction: f.BaseFunction,
		value:        f.value,
		first:        f.first,
	}
}

// CountFunction 计数函数
type CountFunction struct {
	*BaseFunction
	count int
}

func NewCountFunction() *CountFunction {
	return &CountFunction{
		BaseFunction: NewBaseFunction("count", TypeAggregation, "聚合函数", "计算数值个数", 0, -1),
	}
}

func (f *CountFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CountFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	count := 0
	for _, arg := range args {
		if arg != nil {
			count++
		}
	}
	return int64(count), nil
}

// 实现AggregatorFunction接口
func (f *CountFunction) New() AggregatorFunction {
	return &CountFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
	}
}

func (f *CountFunction) Add(value interface{}) {
	// 增强的Add方法：忽略NULL值
	if value != nil {
		f.count++
	}
}

func (f *CountFunction) Result() interface{} {
	return float64(f.count)
}

func (f *CountFunction) Reset() {
	f.count = 0
}

func (f *CountFunction) Clone() AggregatorFunction {
	return &CountFunction{
		BaseFunction: f.BaseFunction,
		count:        f.count,
	}
}

// StdDevFunction 标准差函数（韦尔福德算法实现）
type StdDevFunction struct {
	*BaseFunction
	count int
	mean  float64
	m2    float64 // 平方差的累计值
}

func NewStdDevFunction() *StdDevFunction {
	return &StdDevFunction{
		BaseFunction: NewBaseFunction("stddev", TypeAggregation, "聚合函数", "计算数值标准差", 1, -1),
	}
}

func (f *StdDevFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *StdDevFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 批量执行模式，回退到传统算法
	sum := 0.0
	count := 0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		sum += val
		count++
	}
	if count == 0 {
		return 0.0, nil
	}
	mean := sum / float64(count)
	variance := 0.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		variance += math.Pow(val-mean, 2)
	}
	return math.Sqrt(variance / float64(count)), nil
}

// 实现AggregatorFunction接口 - 韦尔福德算法
func (f *StdDevFunction) New() AggregatorFunction {
	return &StdDevFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *StdDevFunction) Add(value interface{}) {
	val, err := cast.ToFloat64E(value)
	if err != nil {
		return
	}
	f.count++
	delta := val - f.mean
	f.mean += delta / float64(f.count)
	delta2 := val - f.mean
	f.m2 += delta * delta2
}

func (f *StdDevFunction) Result() interface{} {
	if f.count < 1 {
		return 0.0
	}
	variance := f.m2 / float64(f.count)
	return math.Sqrt(variance)
}

func (f *StdDevFunction) Reset() {
	f.count = 0
	f.mean = 0
	f.m2 = 0
}

func (f *StdDevFunction) Clone() AggregatorFunction {
	return &StdDevFunction{
		BaseFunction: f.BaseFunction,
		count:        f.count,
		mean:         f.mean,
		m2:           f.m2,
	}
}

// MedianFunction 中位数函数
type MedianFunction struct {
	*BaseFunction
}

func NewMedianFunction() *MedianFunction {
	return &MedianFunction{
		BaseFunction: NewBaseFunction("median", TypeAggregation, "聚合函数", "计算数值中位数", 1, -1),
	}
}

func (f *MedianFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *MedianFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	values := make([]float64, len(args))
	for i, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	sort.Float64s(values)
	mid := len(values) / 2
	if len(values)%2 == 0 {
		return (values[mid-1] + values[mid]) / 2, nil
	}
	return values[mid], nil
}

// PercentileFunction 百分位数函数
type PercentileFunction struct {
	*BaseFunction
}

func NewPercentileFunction() *PercentileFunction {
	return &PercentileFunction{
		BaseFunction: NewBaseFunction("percentile", TypeAggregation, "聚合函数", "计算数值百分位数", 2, 2),
	}
}

func (f *PercentileFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *PercentileFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	values := make([]float64, len(args))
	for i, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	sort.Float64s(values)
	p, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	index := int(math.Floor(p * float64(len(values)-1)))
	return values[index], nil
}

// CollectFunction 收集函数 - 获取当前窗口所有消息的列值组成的数组
type CollectFunction struct {
	*BaseFunction
	values []interface{}
}

func NewCollectFunction() *CollectFunction {
	return &CollectFunction{
		BaseFunction: NewBaseFunction("collect", TypeAggregation, "聚合函数", "收集所有值组成数组", 1, -1),
		values:       make([]interface{}, 0),
	}
}

func (f *CollectFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CollectFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 直接返回所有参数组成的数组
	result := make([]interface{}, len(args))
	copy(result, args)
	return result, nil
}

// 实现AggregatorFunction接口
func (f *CollectFunction) New() AggregatorFunction {
	return &CollectFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, 0),
	}
}

func (f *CollectFunction) Add(value interface{}) {
	f.values = append(f.values, value)
}

func (f *CollectFunction) Result() interface{} {
	result := make([]interface{}, len(f.values))
	copy(result, f.values)
	return result
}

func (f *CollectFunction) Reset() {
	f.values = make([]interface{}, 0)
}

func (f *CollectFunction) Clone() AggregatorFunction {
	newFunc := &CollectFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, len(f.values)),
	}
	copy(newFunc.values, f.values)
	return newFunc
}

// FirstValueFunction 首个值函数 - 返回组中第一行的值
type FirstValueFunction struct {
	*BaseFunction
	firstValue interface{}
	hasValue   bool
}

func NewFirstValueFunction() *FirstValueFunction {
	return &FirstValueFunction{
		BaseFunction: NewBaseFunction("first_value", TypeAggregation, "聚合函数", "返回第一个值", 1, -1),
		firstValue:   nil,
		hasValue:     false,
	}
}

func (f *FirstValueFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *FirstValueFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("function %s requires at least one argument", f.GetName())
	}
	// 返回第一个值
	return args[0], nil
}

// 实现AggregatorFunction接口
func (f *FirstValueFunction) New() AggregatorFunction {
	return &FirstValueFunction{
		BaseFunction: f.BaseFunction,
		firstValue:   nil,
		hasValue:     false,
	}
}

func (f *FirstValueFunction) Add(value interface{}) {
	if !f.hasValue {
		f.firstValue = value
		f.hasValue = true
	}
}

func (f *FirstValueFunction) Result() interface{} {
	return f.firstValue
}

func (f *FirstValueFunction) Reset() {
	f.firstValue = nil
	f.hasValue = false
}

func (f *FirstValueFunction) Clone() AggregatorFunction {
	return &FirstValueFunction{
		BaseFunction: f.BaseFunction,
		firstValue:   f.firstValue,
		hasValue:     f.hasValue,
	}
}

// LastValueFunction 最后值函数 - 返回组中最后一行的值
type LastValueFunction struct {
	*BaseFunction
	lastValue interface{}
}

func NewLastValueFunction() *LastValueFunction {
	return &LastValueFunction{
		BaseFunction: NewBaseFunction("last_value", TypeAggregation, "聚合函数", "返回最后一个值", 1, -1),
		lastValue:    nil,
	}
}

func (f *LastValueFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *LastValueFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("function %s requires at least one argument", f.GetName())
	}
	// 返回最后一个值
	return args[len(args)-1], nil
}

// 实现AggregatorFunction接口
func (f *LastValueFunction) New() AggregatorFunction {
	return &LastValueFunction{
		BaseFunction: f.BaseFunction,
		lastValue:    nil,
	}
}

func (f *LastValueFunction) Add(value interface{}) {
	f.lastValue = value
}

func (f *LastValueFunction) Result() interface{} {
	return f.lastValue
}

func (f *LastValueFunction) Reset() {
	f.lastValue = nil
}

func (f *LastValueFunction) Clone() AggregatorFunction {
	return &LastValueFunction{
		BaseFunction: f.BaseFunction,
		lastValue:    f.lastValue,
	}
}

// MergeAggFunction 合并聚合函数 - 将组中的值合并为单个值
type MergeAggFunction struct {
	*BaseFunction
	values []interface{}
}

func NewMergeAggFunction() *MergeAggFunction {
	return &MergeAggFunction{
		BaseFunction: NewBaseFunction("merge_agg", TypeAggregation, "聚合函数", "合并所有值", 1, -1),
		values:       make([]interface{}, 0),
	}
}

func (f *MergeAggFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *MergeAggFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, nil
	}

	// 尝试合并为字符串
	var result strings.Builder
	for i, arg := range args {
		if i > 0 {
			result.WriteString(",")
		}
		result.WriteString(cast.ToString(arg))
	}
	return result.String(), nil
}

// 实现AggregatorFunction接口
func (f *MergeAggFunction) New() AggregatorFunction {
	return &MergeAggFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, 0),
	}
}

func (f *MergeAggFunction) Add(value interface{}) {
	f.values = append(f.values, value)
}

func (f *MergeAggFunction) Result() interface{} {
	if len(f.values) == 0 {
		return nil
	}

	// 尝试合并为字符串
	var result strings.Builder
	for i, arg := range f.values {
		if i > 0 {
			result.WriteString(",")
		}
		result.WriteString(cast.ToString(arg))
	}
	return result.String()
}

func (f *MergeAggFunction) Reset() {
	f.values = make([]interface{}, 0)
}

func (f *MergeAggFunction) Clone() AggregatorFunction {
	newFunc := &MergeAggFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, len(f.values)),
	}
	copy(newFunc.values, f.values)
	return newFunc
}

// StdDevSFunction 样本标准差函数（韦尔福德算法实现）
type StdDevSFunction struct {
	*BaseFunction
	count int
	mean  float64
	m2    float64
}

func NewStdDevSFunction() *StdDevSFunction {
	return &StdDevSFunction{
		BaseFunction: NewBaseFunction("stddevs", TypeAggregation, "聚合函数", "计算样本标准差", 1, -1),
	}
}

func (f *StdDevSFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *StdDevSFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 批量执行模式
	sum := 0.0
	count := 0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		sum += val
		count++
	}
	if count <= 1 {
		return 0.0, nil
	}
	mean := sum / float64(count)
	variance := 0.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		variance += math.Pow(val-mean, 2)
	}
	return math.Sqrt(variance / float64(count-1)), nil
}

// 实现AggregatorFunction接口 - 韦尔福德算法
func (f *StdDevSFunction) New() AggregatorFunction {
	return &StdDevSFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *StdDevSFunction) Add(value interface{}) {
	val, err := cast.ToFloat64E(value)
	if err != nil {
		return
	}
	f.count++
	delta := val - f.mean
	f.mean += delta / float64(f.count)
	delta2 := val - f.mean
	f.m2 += delta * delta2
}

func (f *StdDevSFunction) Result() interface{} {
	if f.count < 2 {
		return 0.0
	}
	variance := f.m2 / float64(f.count-1)
	return math.Sqrt(variance)
}

func (f *StdDevSFunction) Reset() {
	f.count = 0
	f.mean = 0
	f.m2 = 0
}

func (f *StdDevSFunction) Clone() AggregatorFunction {
	return &StdDevSFunction{
		BaseFunction: f.BaseFunction,
		count:        f.count,
		mean:         f.mean,
		m2:           f.m2,
	}
}

// DeduplicateFunction 去重函数
type DeduplicateFunction struct {
	*BaseFunction
}

func NewDeduplicateFunction() *DeduplicateFunction {
	return &DeduplicateFunction{
		BaseFunction: NewBaseFunction("deduplicate", TypeAggregation, "聚合函数", "去除重复值", 1, -1),
	}
}

func (f *DeduplicateFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *DeduplicateFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	seen := make(map[string]bool)
	var result []interface{}

	for _, arg := range args {
		key := fmt.Sprintf("%v", arg)
		if !seen[key] {
			seen[key] = true
			result = append(result, arg)
		}
	}

	return result, nil
}

// VarFunction 总体方差函数（韦尔福德算法实现）
type VarFunction struct {
	*BaseFunction
	count int
	mean  float64
	m2    float64
}

func NewVarFunction() *VarFunction {
	return &VarFunction{
		BaseFunction: NewBaseFunction("var", TypeAggregation, "聚合函数", "计算总体方差", 1, -1),
	}
}

func (f *VarFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *VarFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 批量执行模式
	sum := 0.0
	count := 0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		sum += val
		count++
	}
	if count == 0 {
		return 0.0, nil
	}
	mean := sum / float64(count)
	variance := 0.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		variance += math.Pow(val-mean, 2)
	}
	return variance / float64(count), nil
}

// 实现AggregatorFunction接口 - 韦尔福德算法
func (f *VarFunction) New() AggregatorFunction {
	return &VarFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *VarFunction) Add(value interface{}) {
	val, err := cast.ToFloat64E(value)
	if err != nil {
		return
	}
	f.count++
	delta := val - f.mean
	f.mean += delta / float64(f.count)
	delta2 := val - f.mean
	f.m2 += delta * delta2
}

func (f *VarFunction) Result() interface{} {
	if f.count < 1 {
		return 0.0
	}
	return f.m2 / float64(f.count)
}

func (f *VarFunction) Reset() {
	f.count = 0
	f.mean = 0
	f.m2 = 0
}

func (f *VarFunction) Clone() AggregatorFunction {
	return &VarFunction{
		BaseFunction: f.BaseFunction,
		count:        f.count,
		mean:         f.mean,
		m2:           f.m2,
	}
}

// VarSFunction 样本方差函数（韦尔福德算法实现）
type VarSFunction struct {
	*BaseFunction
	count int
	mean  float64
	m2    float64
}

func NewVarSFunction() *VarSFunction {
	return &VarSFunction{
		BaseFunction: NewBaseFunction("vars", TypeAggregation, "聚合函数", "计算样本方差", 1, -1),
	}
}

func (f *VarSFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *VarSFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	// 批量执行模式
	sum := 0.0
	count := 0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		sum += val
		count++
	}
	if count <= 1 {
		return 0.0, nil
	}
	mean := sum / float64(count)
	variance := 0.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue
		}
		variance += math.Pow(val-mean, 2)
	}
	return variance / float64(count-1), nil
}

// 实现AggregatorFunction接口 - 韦尔福德算法
func (f *VarSFunction) New() AggregatorFunction {
	return &VarSFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *VarSFunction) Add(value interface{}) {
	val, err := cast.ToFloat64E(value)
	if err != nil {
		return
	}
	f.count++
	delta := val - f.mean
	f.mean += delta / float64(f.count)
	delta2 := val - f.mean
	f.m2 += delta * delta2
}

func (f *VarSFunction) Result() interface{} {
	if f.count < 2 {
		return 0.0
	}
	return f.m2 / float64(f.count-1)
}

func (f *VarSFunction) Reset() {
	f.count = 0
	f.mean = 0
	f.m2 = 0
}

func (f *VarSFunction) Clone() AggregatorFunction {
	return &VarSFunction{
		BaseFunction: f.BaseFunction,
		count:        f.count,
		mean:         f.mean,
		m2:           f.m2,
	}
}

// 为StdDevFunction添加AggregatorFunction接口实现
type StdDevAggregatorFunction struct {
	*BaseFunction
	values []float64
}

func NewStdDevAggregatorFunction() *StdDevAggregatorFunction {
	return &StdDevAggregatorFunction{
		BaseFunction: NewBaseFunction("stddev", TypeAggregation, "聚合函数", "计算数值标准差", 1, -1),
		values:       make([]float64, 0),
	}
}

func (f *StdDevAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *StdDevAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return NewStdDevFunction().Execute(ctx, args)
}

func (f *StdDevAggregatorFunction) New() AggregatorFunction {
	return &StdDevAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
	}
}

func (f *StdDevAggregatorFunction) Add(value interface{}) {
	if val, err := cast.ToFloat64E(value); err == nil {
		f.values = append(f.values, val)
	}
}

func (f *StdDevAggregatorFunction) Result() interface{} {
	if len(f.values) < 2 {
		return 0.0
	}

	// 计算平均值
	sum := 0.0
	for _, v := range f.values {
		sum += v
	}
	mean := sum / float64(len(f.values))

	// 计算方差
	variance := 0.0
	for _, v := range f.values {
		variance += math.Pow(v-mean, 2)
	}

	return math.Sqrt(variance / float64(len(f.values)-1))
}

func (f *StdDevAggregatorFunction) Reset() {
	f.values = make([]float64, 0)
}

func (f *StdDevAggregatorFunction) Clone() AggregatorFunction {
	clone := &StdDevAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// 为MedianFunction添加AggregatorFunction接口实现
type MedianAggregatorFunction struct {
	*BaseFunction
	values []float64
}

func NewMedianAggregatorFunction() *MedianAggregatorFunction {
	return &MedianAggregatorFunction{
		BaseFunction: NewBaseFunction("median", TypeAggregation, "聚合函数", "计算数值中位数", 1, -1),
		values:       make([]float64, 0),
	}
}

func (f *MedianAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *MedianAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return NewMedianFunction().Execute(ctx, args)
}

func (f *MedianAggregatorFunction) New() AggregatorFunction {
	return &MedianAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
	}
}

func (f *MedianAggregatorFunction) Add(value interface{}) {
	if val, err := cast.ToFloat64E(value); err == nil {
		f.values = append(f.values, val)
	}
}

func (f *MedianAggregatorFunction) Result() interface{} {
	if len(f.values) == 0 {
		return 0.0
	}

	sorted := make([]float64, len(f.values))
	copy(sorted, f.values)
	sort.Float64s(sorted)

	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

func (f *MedianAggregatorFunction) Reset() {
	f.values = make([]float64, 0)
}

func (f *MedianAggregatorFunction) Clone() AggregatorFunction {
	clone := &MedianAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// 为PercentileFunction添加AggregatorFunction接口实现
type PercentileAggregatorFunction struct {
	*BaseFunction
	values []float64
	p      float64
}

func NewPercentileAggregatorFunction() *PercentileAggregatorFunction {
	return &PercentileAggregatorFunction{
		BaseFunction: NewBaseFunction("percentile", TypeAggregation, "聚合函数", "计算数值百分位数", 2, 2),
		values:       make([]float64, 0),
		p:            0.95, // 默认95%分位数
	}
}

func (f *PercentileAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *PercentileAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return NewPercentileFunction().Execute(ctx, args)
}

func (f *PercentileAggregatorFunction) New() AggregatorFunction {
	return &PercentileAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
		p:            f.p,
	}
}

func (f *PercentileAggregatorFunction) Add(value interface{}) {
	if val, err := cast.ToFloat64E(value); err == nil {
		f.values = append(f.values, val)
	}
}

func (f *PercentileAggregatorFunction) Result() interface{} {
	if len(f.values) == 0 {
		return 0.0
	}

	sorted := make([]float64, len(f.values))
	copy(sorted, f.values)
	sort.Float64s(sorted)

	index := int(math.Floor(f.p * float64(len(sorted)-1)))
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func (f *PercentileAggregatorFunction) Reset() {
	f.values = make([]float64, 0)
}

func (f *PercentileAggregatorFunction) Clone() AggregatorFunction {
	clone := &PercentileAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, len(f.values)),
		p:            f.p,
	}
	copy(clone.values, f.values)
	return clone
}

// 为CollectFunction添加AggregatorFunction接口实现
type CollectAggregatorFunction struct {
	*BaseFunction
	values []interface{}
}

func NewCollectAggregatorFunction() *CollectAggregatorFunction {
	return &CollectAggregatorFunction{
		BaseFunction: NewBaseFunction("collect", TypeAggregation, "聚合函数", "收集所有值组成数组", 1, -1),
		values:       make([]interface{}, 0),
	}
}

func (f *CollectAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *CollectAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return NewCollectFunction().Execute(ctx, args)
}

func (f *CollectAggregatorFunction) New() AggregatorFunction {
	return &CollectAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, 0),
	}
}

func (f *CollectAggregatorFunction) Add(value interface{}) {
	f.values = append(f.values, value)
}

func (f *CollectAggregatorFunction) Result() interface{} {
	return f.values
}

func (f *CollectAggregatorFunction) Reset() {
	f.values = make([]interface{}, 0)
}

func (f *CollectAggregatorFunction) Clone() AggregatorFunction {
	clone := &CollectAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// 为LastValueFunction添加AggregatorFunction接口实现
type LastValueAggregatorFunction struct {
	*BaseFunction
	lastValue interface{}
}

func NewLastValueAggregatorFunction() *LastValueAggregatorFunction {
	return &LastValueAggregatorFunction{
		BaseFunction: NewBaseFunction("last_value", TypeAggregation, "聚合函数", "返回最后一个值", 1, -1),
	}
}

func (f *LastValueAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *LastValueAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return NewLastValueFunction().Execute(ctx, args)
}

func (f *LastValueAggregatorFunction) New() AggregatorFunction {
	return &LastValueAggregatorFunction{
		BaseFunction: f.BaseFunction,
	}
}

func (f *LastValueAggregatorFunction) Add(value interface{}) {
	f.lastValue = value
}

func (f *LastValueAggregatorFunction) Result() interface{} {
	return f.lastValue
}

func (f *LastValueAggregatorFunction) Reset() {
	f.lastValue = nil
}

func (f *LastValueAggregatorFunction) Clone() AggregatorFunction {
	return &LastValueAggregatorFunction{
		BaseFunction: f.BaseFunction,
		lastValue:    f.lastValue,
	}
}

// 为MergeAggFunction添加AggregatorFunction接口实现
type MergeAggAggregatorFunction struct {
	*BaseFunction
	values []interface{}
}

func NewMergeAggAggregatorFunction() *MergeAggAggregatorFunction {
	return &MergeAggAggregatorFunction{
		BaseFunction: NewBaseFunction("merge_agg", TypeAggregation, "聚合函数", "合并所有值", 1, -1),
		values:       make([]interface{}, 0),
	}
}

func (f *MergeAggAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *MergeAggAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return NewMergeAggFunction().Execute(ctx, args)
}

func (f *MergeAggAggregatorFunction) New() AggregatorFunction {
	return &MergeAggAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, 0),
	}
}

func (f *MergeAggAggregatorFunction) Add(value interface{}) {
	f.values = append(f.values, value)
}

func (f *MergeAggAggregatorFunction) Result() interface{} {
	if len(f.values) == 0 {
		return ""
	}

	var result strings.Builder
	for i, v := range f.values {
		if i > 0 {
			result.WriteString(",")
		}
		result.WriteString(cast.ToString(v))
	}
	return result.String()
}

func (f *MergeAggAggregatorFunction) Reset() {
	f.values = make([]interface{}, 0)
}

func (f *MergeAggAggregatorFunction) Clone() AggregatorFunction {
	clone := &MergeAggAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]interface{}, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// 为StdDevSFunction添加AggregatorFunction接口实现
type StdDevSAggregatorFunction struct {
	*BaseFunction
	values []float64
}

func NewStdDevSAggregatorFunction() *StdDevSAggregatorFunction {
	return &StdDevSAggregatorFunction{
		BaseFunction: NewBaseFunction("stddevs", TypeAggregation, "聚合函数", "计算样本标准差", 1, -1),
		values:       make([]float64, 0),
	}
}

func (f *StdDevSAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *StdDevSAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return NewStdDevSFunction().Execute(ctx, args)
}

func (f *StdDevSAggregatorFunction) New() AggregatorFunction {
	return &StdDevSAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
	}
}

func (f *StdDevSAggregatorFunction) Add(value interface{}) {
	if value != nil {
		if val, err := cast.ToFloat64E(value); err == nil {
			f.values = append(f.values, val)
		}
	}
}

func (f *StdDevSAggregatorFunction) Result() interface{} {
	if len(f.values) < 2 {
		return 0.0
	}

	// 计算平均值
	sum := 0.0
	for _, v := range f.values {
		sum += v
	}
	mean := sum / float64(len(f.values))

	// 计算样本方差
	variance := 0.0
	for _, v := range f.values {
		variance += math.Pow(v-mean, 2)
	}
	variance = variance / float64(len(f.values)-1) // 样本标准差使用n-1

	return math.Sqrt(variance)
}

func (f *StdDevSAggregatorFunction) Reset() {
	f.values = make([]float64, 0)
}

func (f *StdDevSAggregatorFunction) Clone() AggregatorFunction {
	clone := &StdDevSAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// 为DeduplicateFunction添加AggregatorFunction接口实现
type DeduplicateAggregatorFunction struct {
	*BaseFunction
	seen   map[string]bool
	values []interface{}
}

func NewDeduplicateAggregatorFunction() *DeduplicateAggregatorFunction {
	return &DeduplicateAggregatorFunction{
		BaseFunction: NewBaseFunction("deduplicate", TypeAggregation, "聚合函数", "去除重复值", 1, -1),
		seen:         make(map[string]bool),
		values:       make([]interface{}, 0),
	}
}

func (f *DeduplicateAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *DeduplicateAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return NewDeduplicateFunction().Execute(ctx, args)
}

func (f *DeduplicateAggregatorFunction) New() AggregatorFunction {
	return &DeduplicateAggregatorFunction{
		BaseFunction: f.BaseFunction,
		seen:         make(map[string]bool),
		values:       make([]interface{}, 0),
	}
}

func (f *DeduplicateAggregatorFunction) Add(value interface{}) {
	key := fmt.Sprintf("%v", value)
	if !f.seen[key] {
		f.seen[key] = true
		f.values = append(f.values, value)
	}
}

func (f *DeduplicateAggregatorFunction) Result() interface{} {
	return f.values
}

func (f *DeduplicateAggregatorFunction) Reset() {
	f.seen = make(map[string]bool)
	f.values = make([]interface{}, 0)
}

func (f *DeduplicateAggregatorFunction) Clone() AggregatorFunction {
	clone := &DeduplicateAggregatorFunction{
		BaseFunction: f.BaseFunction,
		seen:         make(map[string]bool),
		values:       make([]interface{}, len(f.values)),
	}
	for k, v := range f.seen {
		clone.seen[k] = v
	}
	copy(clone.values, f.values)
	return clone
}

// 为VarFunction添加AggregatorFunction接口实现
type VarAggregatorFunction struct {
	*BaseFunction
	values []float64
}

func NewVarAggregatorFunction() *VarAggregatorFunction {
	return &VarAggregatorFunction{
		BaseFunction: NewBaseFunction("var", TypeAggregation, "聚合函数", "计算总体方差", 1, -1),
		values:       make([]float64, 0),
	}
}

func (f *VarAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *VarAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return NewVarFunction().Execute(ctx, args)
}

func (f *VarAggregatorFunction) New() AggregatorFunction {
	return &VarAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
	}
}

func (f *VarAggregatorFunction) Add(value interface{}) {
	if value != nil {
		if val, err := cast.ToFloat64E(value); err == nil {
			f.values = append(f.values, val)
		}
	}
}

func (f *VarAggregatorFunction) Result() interface{} {
	if len(f.values) < 1 {
		return 0.0
	}

	// 计算平均值
	sum := 0.0
	for _, v := range f.values {
		sum += v
	}
	mean := sum / float64(len(f.values))

	// 计算总体方差
	variance := 0.0
	for _, v := range f.values {
		variance += math.Pow(v-mean, 2)
	}
	variance = variance / float64(len(f.values)) // 总体方差使用n

	return variance
}

func (f *VarAggregatorFunction) Reset() {
	f.values = make([]float64, 0)
}

func (f *VarAggregatorFunction) Clone() AggregatorFunction {
	clone := &VarAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// 为VarSFunction添加AggregatorFunction接口实现
type VarSAggregatorFunction struct {
	*BaseFunction
	values []float64
}

func NewVarSAggregatorFunction() *VarSAggregatorFunction {
	return &VarSAggregatorFunction{
		BaseFunction: NewBaseFunction("vars", TypeAggregation, "聚合函数", "计算样本方差", 1, -1),
		values:       make([]float64, 0),
	}
}

func (f *VarSAggregatorFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *VarSAggregatorFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
	return NewVarSFunction().Execute(ctx, args)
}

func (f *VarSAggregatorFunction) New() AggregatorFunction {
	return &VarSAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
	}
}

func (f *VarSAggregatorFunction) Add(value interface{}) {
	if value != nil {
		if val, err := cast.ToFloat64E(value); err == nil {
			f.values = append(f.values, val)
		}
	}
}

func (f *VarSAggregatorFunction) Result() interface{} {
	if len(f.values) < 2 {
		return 0.0
	}

	// 计算平均值
	sum := 0.0
	for _, v := range f.values {
		sum += v
	}
	mean := sum / float64(len(f.values))

	// 计算样本方差
	variance := 0.0
	for _, v := range f.values {
		variance += math.Pow(v-mean, 2)
	}
	variance = variance / float64(len(f.values)-1) // 样本方差使用n-1

	return variance
}

func (f *VarSAggregatorFunction) Reset() {
	f.values = make([]float64, 0)
}

func (f *VarSAggregatorFunction) Clone() AggregatorFunction {
	clone := &VarSAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}
