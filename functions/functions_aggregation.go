package functions

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/rulego/streamsql/utils/cast"
)

// SumFunction 求和函数
type SumFunction struct {
	*BaseFunction
	value     float64
	hasValues bool // 标记是否有非NULL值
}

func NewSumFunction() *SumFunction {
	return &SumFunction{
		BaseFunction: NewBaseFunction("sum", TypeAggregation, "聚合函数", "计算数值总和", 1, -1),
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
			continue // 忽略NULL值
		}
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue // 忽略无法转换的值
		}
		sum += val
		hasValues = true
	}
	if !hasValues {
		return nil, nil // 当没有有效值时返回NULL
	}
	return sum, nil
}

// 实现AggregatorFunction接口
func (f *SumFunction) New() AggregatorFunction {
	return &SumFunction{
		BaseFunction: f.BaseFunction,
		value:        0,
		hasValues:    false,
	}
}

func (f *SumFunction) Add(value interface{}) {
	// 增强的Add方法：忽略NULL值
	if value == nil {
		return // 忽略NULL值
	}

	if val, err := cast.ToFloat64E(value); err == nil {
		f.value += val
		f.hasValues = true
	}
	// 如果转换失败，也忽略该值
}

func (f *SumFunction) Result() interface{} {
	if !f.hasValues {
		return nil // 当没有有效值时返回NULL而不是0.0
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

// AvgFunction 求平均值函数
type AvgFunction struct {
	*BaseFunction
	sum   float64
	count int
}

func NewAvgFunction() *AvgFunction {
	return &AvgFunction{
		BaseFunction: NewBaseFunction("avg", TypeAggregation, "聚合函数", "计算数值平均值", 1, -1),
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
			continue // 忽略NULL值
		}
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			continue // 忽略无法转换的值
		}
		sum += val
		count++
	}
	if count == 0 {
		return nil, nil // 当没有有效值时返回nil
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
	// 增强的Add方法：忽略NULL值
	if value == nil {
		return // 忽略NULL值
	}

	if val, err := cast.ToFloat64E(value); err == nil {
		f.sum += val
		f.count++
	}
	// 如果转换失败，也忽略该值
}

func (f *AvgFunction) Result() interface{} {
	if f.count == 0 {
		return nil // 当没有有效值时返回nil而不是0.0
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

// MinFunction 求最小值函数
type MinFunction struct {
	*BaseFunction
	value float64
	first bool
}

func NewMinFunction() *MinFunction {
	return &MinFunction{
		BaseFunction: NewBaseFunction("min", TypeAggregation, "聚合函数", "计算数值最小值", 1, -1),
		first:        true,
	}
}

func (f *MinFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *MinFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
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

// 实现AggregatorFunction接口
func (f *MinFunction) New() AggregatorFunction {
	return &MinFunction{
		BaseFunction: f.BaseFunction,
		first:        true,
	}
}

func (f *MinFunction) Add(value interface{}) {
	// 增强的Add方法：忽略NULL值
	if value == nil {
		return // 忽略NULL值
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
		return nil
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

// MaxFunction 求最大值函数
type MaxFunction struct {
	*BaseFunction
	value float64
	first bool
}

func NewMaxFunction() *MaxFunction {
	return &MaxFunction{
		BaseFunction: NewBaseFunction("max", TypeAggregation, "聚合函数", "计算数值最大值", 1, -1),
		first:        true,
	}
}

func (f *MaxFunction) Validate(args []interface{}) error {
	return f.ValidateArgCount(args)
}

func (f *MaxFunction) Execute(ctx *FunctionContext, args []interface{}) (interface{}, error) {
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

// 实现AggregatorFunction接口
func (f *MaxFunction) New() AggregatorFunction {
	return &MaxFunction{
		BaseFunction: f.BaseFunction,
		first:        true,
	}
}

func (f *MaxFunction) Add(value interface{}) {
	// 增强的Add方法：忽略NULL值
	if value == nil {
		return // 忽略NULL值
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
		return nil
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
		BaseFunction: NewBaseFunction("count", TypeAggregation, "聚合函数", "计算数值个数", 1, -1),
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

// StdDevFunction 标准差函数
type StdDevFunction struct {
	*BaseFunction
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
	sum := 0.0
	count := 0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			return nil, err
		}
		sum += val
		count++
	}
	if count == 0 {
		return nil, fmt.Errorf("no data to calculate standard deviation")
	}
	mean := sum / float64(count)
	variance := 0.0
	for _, arg := range args {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			return nil, err
		}
		variance += math.Pow(val-mean, 2)
	}
	return math.Sqrt(variance / float64(count)), nil
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
	if len(args) == 0 {
		return nil, nil
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

// StdDevSFunction 样本标准差函数
type StdDevSFunction struct {
	*BaseFunction
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
	if len(args) < 2 {
		return 0.0, nil
	}

	// 过滤非空值
	var values []float64
	for _, arg := range args {
		if arg != nil {
			if val, err := cast.ToFloat64E(arg); err == nil {
				values = append(values, val)
			}
		}
	}

	if len(values) < 2 {
		return 0.0, nil
	}

	// 计算平均值
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))

	// 计算样本方差
	variance := 0.0
	for _, v := range values {
		variance += math.Pow(v-mean, 2)
	}
	variance = variance / float64(len(values)-1) // 样本标准差使用n-1

	return math.Sqrt(variance), nil
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

// VarFunction 总体方差函数
type VarFunction struct {
	*BaseFunction
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
	if len(args) < 1 {
		return 0.0, nil
	}

	// 过滤非空值
	var values []float64
	for _, arg := range args {
		if arg != nil {
			if val, err := cast.ToFloat64E(arg); err == nil {
				values = append(values, val)
			}
		}
	}

	if len(values) < 1 {
		return 0.0, nil
	}

	// 计算平均值
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))

	// 计算总体方差
	variance := 0.0
	for _, v := range values {
		variance += math.Pow(v-mean, 2)
	}
	variance = variance / float64(len(values)) // 总体方差使用n

	return variance, nil
}

// VarSFunction 样本方差函数
type VarSFunction struct {
	*BaseFunction
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
	if len(args) < 2 {
		return 0.0, nil
	}

	// 过滤非空值
	var values []float64
	for _, arg := range args {
		if arg != nil {
			if val, err := cast.ToFloat64E(arg); err == nil {
				values = append(values, val)
			}
		}
	}

	if len(values) < 2 {
		return 0.0, nil
	}

	// 计算平均值
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))

	// 计算样本方差
	variance := 0.0
	for _, v := range values {
		variance += math.Pow(v-mean, 2)
	}
	variance = variance / float64(len(values)-1) // 样本方差使用n-1

	return variance, nil
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
