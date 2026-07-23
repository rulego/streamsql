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

func (f *SumFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *SumFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

func (f *SumFunction) Add(value any) {
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

func (f *SumFunction) Result() any {
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

func (f *AvgFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *AvgFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// Implement the AggregatorFunction interface
func (f *AvgFunction) New() AggregatorFunction {
	return &AvgFunction{
		BaseFunction: f.BaseFunction,
		sum:          0,
		count:        0,
	}
}

func (f *AvgFunction) Add(value any) {
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

func (f *AvgFunction) Result() any {
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

func (f *MinFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *MinFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// Check if there are nil parameters
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

func (f *MinFunction) Add(value any) {
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

func (f *MinFunction) Result() any {
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

func (f *MaxFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *MaxFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// Check if there are nil parameters
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

func (f *MaxFunction) Add(value any) {
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

func (f *MaxFunction) Result() any {
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

// CountFunction
type CountFunction struct {
	*BaseFunction
	count int
}

func NewCountFunction() *CountFunction {
	return &CountFunction{
		BaseFunction: NewBaseFunction("count", TypeAggregation, "聚合函数", "计算数值个数", 0, -1),
	}
}

func (f *CountFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *CountFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	count := 0
	for _, arg := range args {
		if arg != nil {
			count++
		}
	}
	return int64(count), nil
}

// Implement the AggregatorFunction interface
func (f *CountFunction) New() AggregatorFunction {
	return &CountFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
	}
}

func (f *CountFunction) Add(value any) {
	// Enhanced Add method: ignores NULL values
	if value != nil {
		f.count++
	}
}

func (f *CountFunction) Result() any {
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

// StdDevFunction Standard Deviation Function (implemented by the Welford algorithm)
type StdDevFunction struct {
	*BaseFunction
	count int
	mean  float64
	m2    float64 // The cumulative value of the difference of squares
}

func NewStdDevFunction() *StdDevFunction {
	return &StdDevFunction{
		BaseFunction: NewBaseFunction("stddev", TypeAggregation, "聚合函数", "计算数值标准差", 1, -1),
	}
}

func (f *StdDevFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *StdDevFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// Batch execution mode reverts to traditional algorithms
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

// Implementing the AggregatorFunction interface - Wilford's algorithm
func (f *StdDevFunction) New() AggregatorFunction {
	return &StdDevFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *StdDevFunction) Add(value any) {
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

func (f *StdDevFunction) Result() any {
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

// MedianFunction median function
type MedianFunction struct {
	*BaseFunction
}

func NewMedianFunction() *MedianFunction {
	return &MedianFunction{
		BaseFunction: NewBaseFunction("median", TypeAggregation, "聚合函数", "计算数值中位数", 1, -1),
	}
}

func (f *MedianFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *MedianFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
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

// PercentileFunction: Percentile function
type PercentileFunction struct {
	*BaseFunction
}

func NewPercentileFunction() *PercentileFunction {
	return &PercentileFunction{
		BaseFunction: NewBaseFunction("percentile", TypeAggregation, "聚合函数", "计算数值百分位数", 2, 2),
	}
}

func (f *PercentileFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *PercentileFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("percentile requires a percentile and at least one value")
	}
	// args[0] is the percentile p in [0,1]; the remaining args are the data.
	p, err := cast.ToFloat64E(args[0])
	if err != nil {
		return nil, err
	}
	if p < 0 || p > 1 {
		return nil, fmt.Errorf("percentile p must be in [0,1], got %v", p)
	}
	values := make([]float64, 0, len(args)-1)
	for _, arg := range args[1:] {
		val, err := cast.ToFloat64E(arg)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	sort.Float64s(values)
	index := int(math.Floor(p * float64(len(values)-1)))
	if index < 0 {
		index = 0
	} else if index >= len(values) {
		index = len(values) - 1
	}
	return values[index], nil
}

// CollectFunction - An array composed of column values for all messages in the current window
type CollectFunction struct {
	*BaseFunction
	values []any
}

func NewCollectFunction() *CollectFunction {
	return &CollectFunction{
		BaseFunction: NewBaseFunction("collect", TypeAggregation, "聚合函数", "收集所有值组成数组", 1, -1),
		values:       make([]any, 0),
	}
}

func (f *CollectFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *CollectFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// Directly returns an array composed of all parameters
	result := make([]any, len(args))
	copy(result, args)
	return result, nil
}

// Implement the AggregatorFunction interface
func (f *CollectFunction) New() AggregatorFunction {
	return &CollectFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, 0),
	}
}

func (f *CollectFunction) Add(value any) {
	f.values = append(f.values, value)
}

func (f *CollectFunction) Result() any {
	result := make([]any, len(f.values))
	copy(result, f.values)
	return result
}

func (f *CollectFunction) Reset() {
	f.values = make([]any, 0)
}

func (f *CollectFunction) Clone() AggregatorFunction {
	newFunc := &CollectFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, len(f.values)),
	}
	copy(newFunc.values, f.values)
	return newFunc
}

// FirstValueFunction - Returns the value of the first row in the group
type FirstValueFunction struct {
	*BaseFunction
	firstValue any
	hasValue   bool
}

func NewFirstValueFunction() *FirstValueFunction {
	return &FirstValueFunction{
		BaseFunction: NewBaseFunction("first_value", TypeAggregation, "聚合函数", "返回第一个值", 1, -1),
		firstValue:   nil,
		hasValue:     false,
	}
}

func (f *FirstValueFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *FirstValueFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("function %s requires at least one argument", f.GetName())
	}
	// Return the first value
	return args[0], nil
}

// Implement the AggregatorFunction interface
func (f *FirstValueFunction) New() AggregatorFunction {
	return &FirstValueFunction{
		BaseFunction: f.BaseFunction,
		firstValue:   nil,
		hasValue:     false,
	}
}

func (f *FirstValueFunction) Add(value any) {
	if !f.hasValue {
		f.firstValue = value
		f.hasValue = true
	}
}

func (f *FirstValueFunction) Result() any {
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

// LastValueFunction - Returns the value of the last row in the group
type LastValueFunction struct {
	*BaseFunction
	lastValue any
}

func NewLastValueFunction() *LastValueFunction {
	return &LastValueFunction{
		BaseFunction: NewBaseFunction("last_value", TypeAggregation, "聚合函数", "返回最后一个值", 1, -1),
		lastValue:    nil,
	}
}

func (f *LastValueFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *LastValueFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if err := f.Validate(args); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("function %s requires at least one argument", f.GetName())
	}
	// Return the last value
	return args[len(args)-1], nil
}

// Implement the AggregatorFunction interface
func (f *LastValueFunction) New() AggregatorFunction {
	return &LastValueFunction{
		BaseFunction: f.BaseFunction,
		lastValue:    nil,
	}
}

func (f *LastValueFunction) Add(value any) {
	f.lastValue = value
}

func (f *LastValueFunction) Result() any {
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

// MergeAggFunction Merge Aggregator - Merge values from a group into a single value
type MergeAggFunction struct {
	*BaseFunction
	values []any
}

func NewMergeAggFunction() *MergeAggFunction {
	return &MergeAggFunction{
		BaseFunction: NewBaseFunction("merge_agg", TypeAggregation, "聚合函数", "合并所有值", 1, -1),
		values:       make([]any, 0),
	}
}

func (f *MergeAggFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *MergeAggFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	if len(args) == 0 {
		return nil, nil
	}

	// Try merging into a string
	var result strings.Builder
	for i, arg := range args {
		if i > 0 {
			result.WriteString(",")
		}
		result.WriteString(cast.ToString(arg))
	}
	return result.String(), nil
}

// Implement the AggregatorFunction interface
func (f *MergeAggFunction) New() AggregatorFunction {
	return &MergeAggFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, 0),
	}
}

func (f *MergeAggFunction) Add(value any) {
	f.values = append(f.values, value)
}

func (f *MergeAggFunction) Result() any {
	if len(f.values) == 0 {
		return nil
	}

	// Try merging into a string
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
	f.values = make([]any, 0)
}

func (f *MergeAggFunction) Clone() AggregatorFunction {
	newFunc := &MergeAggFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, len(f.values)),
	}
	copy(newFunc.values, f.values)
	return newFunc
}

// StdDevSFunction Sample Standard Deviation Function (Implemented by the Welford Algorithm)
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

func (f *StdDevSFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *StdDevSFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// Batch execution mode
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

// Implementing the AggregatorFunction interface - Wilford's algorithm
func (f *StdDevSFunction) New() AggregatorFunction {
	return &StdDevSFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *StdDevSFunction) Add(value any) {
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

func (f *StdDevSFunction) Result() any {
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

// DeduplicateFunction
type DeduplicateFunction struct {
	*BaseFunction
}

func NewDeduplicateFunction() *DeduplicateFunction {
	return &DeduplicateFunction{
		BaseFunction: NewBaseFunction("deduplicate", TypeAggregation, "聚合函数", "去除重复值", 1, -1),
	}
}

func (f *DeduplicateFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *DeduplicateFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	seen := make(map[string]bool)
	var result []any

	for _, arg := range args {
		key := fmt.Sprintf("%v", arg)
		if !seen[key] {
			seen[key] = true
			result = append(result, arg)
		}
	}

	return result, nil
}

// VarFunction (Implemented by the Wellford Algorithm)
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

func (f *VarFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *VarFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// Batch execution mode
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

// Implementing the AggregatorFunction interface - Wilford's algorithm
func (f *VarFunction) New() AggregatorFunction {
	return &VarFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *VarFunction) Add(value any) {
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

func (f *VarFunction) Result() any {
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

// VarSFunction sample variance function (implemented by Welford's algorithm)
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

func (f *VarSFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *VarSFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	// Batch execution mode
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

// Implementing the AggregatorFunction interface - Wilford's algorithm
func (f *VarSFunction) New() AggregatorFunction {
	return &VarSFunction{
		BaseFunction: f.BaseFunction,
		count:        0,
		mean:         0,
		m2:           0,
	}
}

func (f *VarSFunction) Add(value any) {
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

func (f *VarSFunction) Result() any {
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

// Adds the AggregatorFunction interface implementation to StdDevFunction
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

func (f *StdDevAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *StdDevAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return NewStdDevFunction().Execute(ctx, args)
}

func (f *StdDevAggregatorFunction) New() AggregatorFunction {
	return &StdDevAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
	}
}

func (f *StdDevAggregatorFunction) Add(value any) {
	if val, err := cast.ToFloat64E(value); err == nil {
		f.values = append(f.values, val)
	}
}

func (f *StdDevAggregatorFunction) Result() any {
	if len(f.values) < 2 {
		return 0.0
	}

	// Calculate the average
	sum := 0.0
	for _, v := range f.values {
		sum += v
	}
	mean := sum / float64(len(f.values))

	// Calculate variance
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

// Adds the AggregatorFunction interface implementation to MedianFunction
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

func (f *MedianAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *MedianAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return NewMedianFunction().Execute(ctx, args)
}

func (f *MedianAggregatorFunction) New() AggregatorFunction {
	return &MedianAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
	}
}

func (f *MedianAggregatorFunction) Add(value any) {
	if val, err := cast.ToFloat64E(value); err == nil {
		f.values = append(f.values, val)
	}
}

func (f *MedianAggregatorFunction) Result() any {
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

// Adds an AggregatorFunction interface implementation to PercentileFunction
type PercentileAggregatorFunction struct {
	*BaseFunction
	values []float64
	p      float64
}

func NewPercentileAggregatorFunction() *PercentileAggregatorFunction {
	return &PercentileAggregatorFunction{
		BaseFunction: NewBaseFunction("percentile", TypeAggregation, "聚合函数", "计算数值百分位数", 2, 2),
		values:       make([]float64, 0),
		p:            0.95, // Default: 95% percentile
	}
}

func (f *PercentileAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *PercentileAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return NewPercentileFunction().Execute(ctx, args)
}

func (f *PercentileAggregatorFunction) New() AggregatorFunction {
	return &PercentileAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
		p:            f.p,
	}
}

func (f *PercentileAggregatorFunction) Add(value any) {
	if val, err := cast.ToFloat64E(value); err == nil {
		f.values = append(f.values, val)
	}
}

func (f *PercentileAggregatorFunction) Result() any {
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

// Init implements ParameterizedFunction: takes the quantile p from the SQL second argument.
// In percentile(field, p), p∈[0,1] is the second parameter (args[1]), and the field data is accumulated by adding.
// When this interface is not implemented, window aggregation uses the backend branch New() of CreateParameterizedAggregator, and p degenerates to the default 0.95.
func (f *PercentileAggregatorFunction) Init(args []any) error {
	if len(args) < 2 {
		return fmt.Errorf("percentile requires (field, p); got %v", args)
	}
	switch p := args[1].(type) {
	case float64:
		f.p = p
	case int:
		f.p = float64(p)
	case int64:
		f.p = float64(p)
	default:
		return fmt.Errorf("percentile p must be a number in [0,1], got %T (%v)", args[1], args[1])
	}
	if f.p < 0 || f.p > 1 {
		return fmt.Errorf("percentile p must be in [0,1], got %v", f.p)
	}
	return nil
}

// Adds the AggregatorFunction interface implementation to CollectFunction
type CollectAggregatorFunction struct {
	*BaseFunction
	values []any
}

func NewCollectAggregatorFunction() *CollectAggregatorFunction {
	return &CollectAggregatorFunction{
		BaseFunction: NewBaseFunction("collect", TypeAggregation, "聚合函数", "收集所有值组成数组", 1, -1),
		values:       make([]any, 0),
	}
}

func (f *CollectAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *CollectAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return NewCollectFunction().Execute(ctx, args)
}

func (f *CollectAggregatorFunction) New() AggregatorFunction {
	return &CollectAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, 0),
	}
}

func (f *CollectAggregatorFunction) Add(value any) {
	f.values = append(f.values, value)
}

func (f *CollectAggregatorFunction) Result() any {
	return f.values
}

func (f *CollectAggregatorFunction) Reset() {
	f.values = make([]any, 0)
}

func (f *CollectAggregatorFunction) Clone() AggregatorFunction {
	clone := &CollectAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// Adds the AggregatorFunction interface implementation to LastValueFunction
type LastValueAggregatorFunction struct {
	*BaseFunction
	lastValue any
}

func NewLastValueAggregatorFunction() *LastValueAggregatorFunction {
	return &LastValueAggregatorFunction{
		BaseFunction: NewBaseFunction("last_value", TypeAggregation, "聚合函数", "返回最后一个值", 1, -1),
	}
}

func (f *LastValueAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *LastValueAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return NewLastValueFunction().Execute(ctx, args)
}

func (f *LastValueAggregatorFunction) New() AggregatorFunction {
	return &LastValueAggregatorFunction{
		BaseFunction: f.BaseFunction,
	}
}

func (f *LastValueAggregatorFunction) Add(value any) {
	f.lastValue = value
}

func (f *LastValueAggregatorFunction) Result() any {
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

// Adds the AggregatorFunction interface implementation for MergeAggFunction
type MergeAggAggregatorFunction struct {
	*BaseFunction
	values []any
}

func NewMergeAggAggregatorFunction() *MergeAggAggregatorFunction {
	return &MergeAggAggregatorFunction{
		BaseFunction: NewBaseFunction("merge_agg", TypeAggregation, "聚合函数", "合并所有值", 1, -1),
		values:       make([]any, 0),
	}
}

func (f *MergeAggAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *MergeAggAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return NewMergeAggFunction().Execute(ctx, args)
}

func (f *MergeAggAggregatorFunction) New() AggregatorFunction {
	return &MergeAggAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, 0),
	}
}

func (f *MergeAggAggregatorFunction) Add(value any) {
	f.values = append(f.values, value)
}

func (f *MergeAggAggregatorFunction) Result() any {
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
	f.values = make([]any, 0)
}

func (f *MergeAggAggregatorFunction) Clone() AggregatorFunction {
	clone := &MergeAggAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]any, len(f.values)),
	}
	copy(clone.values, f.values)
	return clone
}

// Adds the AggregatorFunction interface implementation for StdDevSFunction
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

func (f *StdDevSAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *StdDevSAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return NewStdDevSFunction().Execute(ctx, args)
}

func (f *StdDevSAggregatorFunction) New() AggregatorFunction {
	return &StdDevSAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
	}
}

func (f *StdDevSAggregatorFunction) Add(value any) {
	if value != nil {
		if val, err := cast.ToFloat64E(value); err == nil {
			f.values = append(f.values, val)
		}
	}
}

func (f *StdDevSAggregatorFunction) Result() any {
	if len(f.values) < 2 {
		return 0.0
	}

	// Calculate the average
	sum := 0.0
	for _, v := range f.values {
		sum += v
	}
	mean := sum / float64(len(f.values))

	// Calculate sample variance
	variance := 0.0
	for _, v := range f.values {
		variance += math.Pow(v-mean, 2)
	}
	variance = variance / float64(len(f.values)-1) // The sample standard deviation is used as n-1

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

// Adds the AggregatorFunction interface implementation to DeduplicateFunction
type DeduplicateAggregatorFunction struct {
	*BaseFunction
	seen   map[string]bool
	values []any
}

func NewDeduplicateAggregatorFunction() *DeduplicateAggregatorFunction {
	return &DeduplicateAggregatorFunction{
		BaseFunction: NewBaseFunction("deduplicate", TypeAggregation, "聚合函数", "去除重复值", 1, -1),
		seen:         make(map[string]bool),
		values:       make([]any, 0),
	}
}

func (f *DeduplicateAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *DeduplicateAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return NewDeduplicateFunction().Execute(ctx, args)
}

func (f *DeduplicateAggregatorFunction) New() AggregatorFunction {
	return &DeduplicateAggregatorFunction{
		BaseFunction: f.BaseFunction,
		seen:         make(map[string]bool),
		values:       make([]any, 0),
	}
}

func (f *DeduplicateAggregatorFunction) Add(value any) {
	key := fmt.Sprintf("%v", value)
	if !f.seen[key] {
		f.seen[key] = true
		f.values = append(f.values, value)
	}
}

func (f *DeduplicateAggregatorFunction) Result() any {
	return f.values
}

func (f *DeduplicateAggregatorFunction) Reset() {
	f.seen = make(map[string]bool)
	f.values = make([]any, 0)
}

func (f *DeduplicateAggregatorFunction) Clone() AggregatorFunction {
	clone := &DeduplicateAggregatorFunction{
		BaseFunction: f.BaseFunction,
		seen:         make(map[string]bool),
		values:       make([]any, len(f.values)),
	}
	for k, v := range f.seen {
		clone.seen[k] = v
	}
	copy(clone.values, f.values)
	return clone
}

// Adds the AggregatorFunction interface implementation to VarFunction
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

func (f *VarAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *VarAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return NewVarFunction().Execute(ctx, args)
}

func (f *VarAggregatorFunction) New() AggregatorFunction {
	return &VarAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
	}
}

func (f *VarAggregatorFunction) Add(value any) {
	if value != nil {
		if val, err := cast.ToFloat64E(value); err == nil {
			f.values = append(f.values, val)
		}
	}
}

func (f *VarAggregatorFunction) Result() any {
	if len(f.values) < 1 {
		return 0.0
	}

	// Calculate the average
	sum := 0.0
	for _, v := range f.values {
		sum += v
	}
	mean := sum / float64(len(f.values))

	// Calculate the population variance
	variance := 0.0
	for _, v := range f.values {
		variance += math.Pow(v-mean, 2)
	}
	variance = variance / float64(len(f.values)) // The population variance is expressed using n

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

// Adds the AggregatorFunction interface implementation for VarSFunction
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

func (f *VarSAggregatorFunction) Validate(args []any) error {
	return f.ValidateArgCount(args)
}

func (f *VarSAggregatorFunction) Execute(ctx *FunctionContext, args []any) (any, error) {
	return NewVarSFunction().Execute(ctx, args)
}

func (f *VarSAggregatorFunction) New() AggregatorFunction {
	return &VarSAggregatorFunction{
		BaseFunction: f.BaseFunction,
		values:       make([]float64, 0),
	}
}

func (f *VarSAggregatorFunction) Add(value any) {
	if value != nil {
		if val, err := cast.ToFloat64E(value); err == nil {
			f.values = append(f.values, val)
		}
	}
}

func (f *VarSAggregatorFunction) Result() any {
	if len(f.values) < 2 {
		return 0.0
	}

	// Calculate the average
	sum := 0.0
	for _, v := range f.values {
		sum += v
	}
	mean := sum / float64(len(f.values))

	// Calculate sample variance
	variance := 0.0
	for _, v := range f.values {
		variance += math.Pow(v-mean, 2)
	}
	variance = variance / float64(len(f.values)-1) // Sample variance is used in n-1

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
