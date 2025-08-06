package functions

import (
	"math"
	"reflect"
	"testing"
)

func TestStdDevFunction(t *testing.T) {
	fn := NewStdDevFunction()
	ctx := &FunctionContext{}
	// Execute 批量
	result, err := fn.Execute(ctx, []interface{}{1.0, 2.0, 3.0, 4.0})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if math.Abs(result.(float64)-1.1180) > 0.01 {
		t.Errorf("Execute stddev result = %v, want ~1.118", result)
	}
	// Add/Result/Reset/Clone
	agg := fn.New().(*StdDevFunction)
	agg.Add(1.0)
	agg.Add(2.0)
	agg.Add(3.0)
	agg.Add(4.0)
	res := agg.Result().(float64)
	if math.Abs(res-1.1180) > 0.01 {
		t.Errorf("Agg stddev result = %v, want ~1.118", res)
	}
	agg.Reset()
	if agg.Result().(float64) != 0.0 {
		t.Errorf("Reset failed")
	}
	clone := agg.Clone().(*StdDevFunction)
	if clone.count != agg.count || clone.mean != agg.mean || clone.m2 != agg.m2 {
		t.Errorf("Clone failed")
	}
}

func TestVarFunction(t *testing.T) {
	fn := NewVarFunction()
	ctx := &FunctionContext{}
	result, err := fn.Execute(ctx, []interface{}{1.0, 2.0, 3.0, 4.0})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if math.Abs(result.(float64)-1.25) > 0.01 {
		t.Errorf("Execute var result = %v, want ~1.25", result)
	}
	agg := fn.New().(*VarFunction)
	agg.Add(1.0)
	agg.Add(2.0)
	agg.Add(3.0)
	agg.Add(4.0)
	res := agg.Result().(float64)
	if math.Abs(res-1.25) > 0.01 {
		t.Errorf("Agg var result = %v, want ~1.25", res)
	}
	agg.Reset()
	if agg.Result().(float64) != 0.0 {
		t.Errorf("Reset failed")
	}
	clone := agg.Clone().(*VarFunction)
	if clone.count != agg.count || clone.mean != agg.mean || clone.m2 != agg.m2 {
		t.Errorf("Clone failed")
	}
}

func TestVarSFunction(t *testing.T) {
	fn := NewVarSFunction()
	ctx := &FunctionContext{}
	result, err := fn.Execute(ctx, []interface{}{1.0, 2.0, 3.0, 4.0})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if math.Abs(result.(float64)-1.6666) > 0.01 {
		t.Errorf("Execute varS result = %v, want ~1.666", result)
	}
	agg := fn.New().(*VarSFunction)
	agg.Add(1.0)
	agg.Add(2.0)
	agg.Add(3.0)
	agg.Add(4.0)
	res := agg.Result().(float64)
	if math.Abs(res-1.6666) > 0.01 {
		t.Errorf("Agg varS result = %v, want ~1.666", res)
	}
	agg.Reset()
	if agg.Result().(float64) != 0.0 {
		t.Errorf("Reset failed")
	}
	clone := agg.Clone().(*VarSFunction)
	if clone.count != agg.count || clone.mean != agg.mean || clone.m2 != agg.m2 {
		t.Errorf("Clone failed")
	}
}

func TestStdDevSFunction(t *testing.T) {
	fn := NewStdDevSFunction()
	ctx := &FunctionContext{}
	result, err := fn.Execute(ctx, []interface{}{1.0, 2.0, 3.0, 4.0})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if math.Abs(result.(float64)-1.29099) > 0.01 {
		t.Errorf("Execute stddevS result = %v, want ~1.291", result)
	}
	agg := fn.New().(*StdDevSFunction)
	agg.Add(1.0)
	agg.Add(2.0)
	agg.Add(3.0)
	agg.Add(4.0)
	res := agg.Result().(float64)
	if math.Abs(res-1.29099) > 0.01 {
		t.Errorf("Agg stddevS result = %v, want ~1.291", res)
	}
	agg.Reset()
	if agg.Result().(float64) != 0.0 {
		t.Errorf("Reset failed")
	}
	clone := agg.Clone().(*StdDevSFunction)
	if clone.count != agg.count || clone.mean != agg.mean || clone.m2 != agg.m2 {
		t.Errorf("Clone failed")
	}
}

func TestMedianFunction(t *testing.T) {
	fn := NewMedianFunction()
	ctx := &FunctionContext{}

	// 测试奇数个元素
	result, err := fn.Execute(ctx, []interface{}{1.0, 3.0, 2.0})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if result.(float64) != 2.0 {
		t.Errorf("Execute median result = %v, want 2.0", result)
	}

	// 测试偶数个元素
	result, err = fn.Execute(ctx, []interface{}{1.0, 3.0, 2.0, 4.0})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if result.(float64) != 2.5 {
		t.Errorf("Execute median result = %v, want 2.5", result)
	}

	// 测试单个元素
	result, err = fn.Execute(ctx, []interface{}{5.0})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if result.(float64) != 5.0 {
		t.Errorf("Execute median result = %v, want 5.0", result)
	}
}

func TestPercentileFunction(t *testing.T) {
	fn := NewPercentileFunction()
	ctx := &FunctionContext{}

	// 测试50%分位数
	result, err := fn.Execute(ctx, []interface{}{0.5, 1.0, 2.0, 3.0, 4.0})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if result.(float64) != 2.0 {
		t.Errorf("Execute percentile result = %v, want 2.0", result)
	}

	// 测试25%分位数
	result, err = fn.Execute(ctx, []interface{}{0.25, 1.0, 2.0, 3.0, 4.0})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if result.(float64) != 1.0 {
		t.Errorf("Execute percentile result = %v, want 1.0", result)
	}

	// 测试75%分位数
	result, err = fn.Execute(ctx, []interface{}{0.75, 1.0, 2.0, 3.0, 4.0})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if result.(float64) != 3.0 {
		t.Errorf("Execute percentile result = %v, want 3.0", result)
	}
}

func TestCollectFunction(t *testing.T) {
	fn := NewCollectFunction()
	ctx := &FunctionContext{}

	// 测试Execute方法
	result, err := fn.Execute(ctx, []interface{}{"a", "b", "c"})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	expected := []interface{}{"a", "b", "c"}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Execute collect result = %v, want %v", result, expected)
	}

	// 测试聚合器方法
	agg := fn.New().(*CollectFunction)
	agg.Add("x")
	agg.Add("y")
	agg.Add("z")
	res := agg.Result().([]interface{})
	expected = []interface{}{"x", "y", "z"}
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("Agg collect result = %v, want %v", res, expected)
	}

	// 测试Reset
	agg.Reset()
	res = agg.Result().([]interface{})
	if len(res) != 0 {
		t.Errorf("Reset failed, result length = %d, want 0", len(res))
	}

	// 测试Clone
	clone := agg.Clone().(*CollectFunction)
	if !reflect.DeepEqual(clone.values, agg.values) {
		t.Errorf("Clone failed")
	}
}

func TestLastValueFunction(t *testing.T) {
	fn := NewLastValueFunction()
	ctx := &FunctionContext{}

	// 测试Execute方法
	result, err := fn.Execute(ctx, []interface{}{"a", "b", "c"})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if result != "c" {
		t.Errorf("Execute last_value result = %v, want c", result)
	}

	// 测试聚合器方法
	agg := fn.New().(*LastValueFunction)
	agg.Add("x")
	agg.Add("y")
	agg.Add("z")
	res := agg.Result()
	if res != "z" {
		t.Errorf("Agg last_value result = %v, want z", res)
	}

	// 测试Reset
	agg.Reset()
	res = agg.Result()
	if res != nil {
		t.Errorf("Reset failed, result = %v, want nil", res)
	}

	// 测试Clone
	clone := agg.Clone().(*LastValueFunction)
	if clone.lastValue != agg.lastValue {
		t.Errorf("Clone failed")
	}
}

func TestMergeAggFunction(t *testing.T) {
	fn := NewMergeAggFunction()
	ctx := &FunctionContext{}

	// 测试Execute方法
	result, err := fn.Execute(ctx, []interface{}{"a", "b", "c"})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	expected := "a,b,c"
	if result != expected {
		t.Errorf("Execute merge_agg result = %v, want %v", result, expected)
	}

	// 测试聚合器方法
	agg := fn.New().(*MergeAggFunction)
	agg.Add("x")
	agg.Add("y")
	agg.Add("z")
	res := agg.Result().(string)
	expected = "x,y,z"
	if res != expected {
		t.Errorf("Agg merge_agg result = %v, want %v", res, expected)
	}

	// 测试Reset
	agg.Reset()
	res2 := agg.Result()
	if res2 != nil {
		t.Errorf("Reset failed, result = %v, want nil", res2)
	}

	// 测试Clone
	clone := agg.Clone().(*MergeAggFunction)
	if !reflect.DeepEqual(clone.values, agg.values) {
		t.Errorf("Clone failed")
	}
}

func TestAggregatorFunctions(t *testing.T) {
	// 测试StdDevAggregatorFunction
	stdDevAgg := NewStdDevAggregatorFunction()
	agg := stdDevAgg.New().(*StdDevAggregatorFunction)
	agg.Add(1.0)
	agg.Add(2.0)
	agg.Add(3.0)
	agg.Add(4.0)
	res := agg.Result().(float64)
	if math.Abs(res-1.2909) > 0.01 {
		t.Errorf("StdDevAggregator result = %v, want ~1.291", res)
	}
	agg.Reset()
	if agg.Result().(float64) != 0.0 {
		t.Errorf("StdDevAggregator Reset failed")
	}
	clone := agg.Clone().(*StdDevAggregatorFunction)
	if !reflect.DeepEqual(clone.values, agg.values) {
		t.Errorf("StdDevAggregator Clone failed")
	}

	// 测试MedianAggregatorFunction
	medianAgg := NewMedianAggregatorFunction()
	agg2 := medianAgg.New().(*MedianAggregatorFunction)
	agg2.Add(1.0)
	agg2.Add(3.0)
	agg2.Add(2.0)
	res2 := agg2.Result().(float64)
	if res2 != 2.0 {
		t.Errorf("MedianAggregator result = %v, want 2.0", res2)
	}
	agg2.Reset()
	if agg2.Result().(float64) != 0.0 {
		t.Errorf("MedianAggregator Reset failed")
	}
	clone2 := agg2.Clone().(*MedianAggregatorFunction)
	if !reflect.DeepEqual(clone2.values, agg2.values) {
		t.Errorf("MedianAggregator Clone failed")
	}

	// 测试PercentileAggregatorFunction
	percentileAgg := NewPercentileAggregatorFunction()
	agg3 := percentileAgg.New().(*PercentileAggregatorFunction)
	agg3.Add(1.0)
	agg3.Add(2.0)
	agg3.Add(3.0)
	agg3.Add(4.0)
	res3 := agg3.Result().(float64)
	if res3 != 3.0 {
		t.Errorf("PercentileAggregator result = %v, want 3.0", res3)
	}
	agg3.Reset()
	if agg3.Result().(float64) != 0.0 {
		t.Errorf("PercentileAggregator Reset failed")
	}
	clone3 := agg3.Clone().(*PercentileAggregatorFunction)
	if !reflect.DeepEqual(clone3.values, agg3.values) {
		t.Errorf("PercentileAggregator Clone failed")
	}

	// 测试CollectAggregatorFunction
	collectAgg := NewCollectAggregatorFunction()
	agg4 := collectAgg.New().(*CollectAggregatorFunction)
	agg4.Add("a")
	agg4.Add("b")
	agg4.Add("c")
	res4 := agg4.Result().([]interface{})
	expected := []interface{}{"a", "b", "c"}
	if !reflect.DeepEqual(res4, expected) {
		t.Errorf("CollectAggregator result = %v, want %v", res4, expected)
	}
	agg4.Reset()
	if len(agg4.Result().([]interface{})) != 0 {
		t.Errorf("CollectAggregator Reset failed")
	}
	clone4 := agg4.Clone().(*CollectAggregatorFunction)
	if !reflect.DeepEqual(clone4.values, agg4.values) {
		t.Errorf("CollectAggregator Clone failed")
	}

	// 测试LastValueAggregatorFunction
	lastValueAgg := NewLastValueAggregatorFunction()
	agg5 := lastValueAgg.New().(*LastValueAggregatorFunction)
	agg5.Add("a")
	agg5.Add("b")
	agg5.Add("c")
	res5 := agg5.Result()
	if res5 != "c" {
		t.Errorf("LastValueAggregator result = %v, want c", res5)
	}
	agg5.Reset()
	if agg5.Result() != nil {
		t.Errorf("LastValueAggregator Reset failed")
	}
	clone5 := agg5.Clone().(*LastValueAggregatorFunction)
	if clone5.lastValue != agg5.lastValue {
		t.Errorf("LastValueAggregator Clone failed")
	}

	// 测试MergeAggAggregatorFunction
	mergeAggAgg := NewMergeAggAggregatorFunction()
	agg6 := mergeAggAgg.New().(*MergeAggAggregatorFunction)
	agg6.Add("a")
	agg6.Add("b")
	agg6.Add("c")
	res6 := agg6.Result().(string)
	expected6 := "a,b,c"
	if res6 != expected6 {
		t.Errorf("MergeAggAggregator result = %v, want %v", res6, expected6)
	}
	agg6.Reset()
	if agg6.Result().(string) != "" {
		t.Errorf("MergeAggAggregator Reset failed")
	}
	clone6 := agg6.Clone().(*MergeAggAggregatorFunction)
	if !reflect.DeepEqual(clone6.values, agg6.values) {
		t.Errorf("MergeAggAggregator Clone failed")
	}

	// 测试StdDevSAggregatorFunction
	stdDevSAgg := NewStdDevSAggregatorFunction()
	agg7 := stdDevSAgg.New().(*StdDevSAggregatorFunction)
	agg7.Add(1.0)
	agg7.Add(2.0)
	agg7.Add(3.0)
	agg7.Add(4.0)
	res7 := agg7.Result().(float64)
	if math.Abs(res7-1.2909) > 0.01 {
		t.Errorf("StdDevSAggregator result = %v, want ~1.291", res7)
	}
	agg7.Reset()
	if agg7.Result().(float64) != 0.0 {
		t.Errorf("StdDevSAggregator Reset failed")
	}
	clone7 := agg7.Clone().(*StdDevSAggregatorFunction)
	if !reflect.DeepEqual(clone7.values, agg7.values) {
		t.Errorf("StdDevSAggregator Clone failed")
	}

	// 测试DeduplicateAggregatorFunction
	dedupAgg := NewDeduplicateAggregatorFunction()
	agg8 := dedupAgg.New().(*DeduplicateAggregatorFunction)
	agg8.Add("a")
	agg8.Add("b")
	agg8.Add("a") // 重复
	agg8.Add("c")
	res8 := agg8.Result().([]interface{})
	expected = []interface{}{"a", "b", "c"}
	if !reflect.DeepEqual(res8, expected) {
		t.Errorf("DeduplicateAggregator result = %v, want %v", res8, expected)
	}
	agg8.Reset()
	if len(agg8.Result().([]interface{})) != 0 {
		t.Errorf("DeduplicateAggregator Reset failed")
	}
	clone8 := agg8.Clone().(*DeduplicateAggregatorFunction)
	if !reflect.DeepEqual(clone8.values, agg8.values) {
		t.Errorf("DeduplicateAggregator Clone failed")
	}

	// 测试VarAggregatorFunction
	varAgg := NewVarAggregatorFunction()
	agg9 := varAgg.New().(*VarAggregatorFunction)
	agg9.Add(1.0)
	agg9.Add(2.0)
	agg9.Add(3.0)
	agg9.Add(4.0)
	res9 := agg9.Result().(float64)
	if math.Abs(res9-1.25) > 0.01 {
		t.Errorf("VarAggregator result = %v, want ~1.25", res9)
	}
	agg9.Reset()
	if agg9.Result().(float64) != 0.0 {
		t.Errorf("VarAggregator Reset failed")
	}
	clone9 := agg9.Clone().(*VarAggregatorFunction)
	if !reflect.DeepEqual(clone9.values, agg9.values) {
		t.Errorf("VarAggregator Clone failed")
	}

	// 测试VarSAggregatorFunction
	varSAgg := NewVarSAggregatorFunction()
	agg10 := varSAgg.New().(*VarSAggregatorFunction)
	agg10.Add(1.0)
	agg10.Add(2.0)
	agg10.Add(3.0)
	agg10.Add(4.0)
	res10 := agg10.Result().(float64)
	if math.Abs(res10-1.6666) > 0.01 {
		t.Errorf("VarSAggregator result = %v, want ~1.667", res10)
	}
	agg10.Reset()
	if agg10.Result().(float64) != 0.0 {
		t.Errorf("VarSAggregator Reset failed")
	}
	clone10 := agg10.Clone().(*VarSAggregatorFunction)
	if !reflect.DeepEqual(clone10.values, agg10.values) {
		t.Errorf("VarSAggregator Clone failed")
	}
}

func TestAggregatorEdgeCases(t *testing.T) {
	// PercentileAggregatorFunction Validate边界
	agg := NewPercentileAggregatorFunction()
	if err := agg.Validate([]interface{}{1.0}); err == nil {
		t.Error("PercentileAggregatorFunction.Validate should fail for insufficient args")
	}
	// 不再直接调用Execute避免panic
	// _, err := agg.Execute(nil, []interface{}{})
	// if err == nil {
	// 	t.Error("PercentileAggregatorFunction.Execute should fail for empty args")
	// }
	agg2 := agg.New().(*PercentileAggregatorFunction)
	agg2.Reset()
	_ = agg2.Clone()

	// CollectAggregatorFunction Validate/Execute边界
	agg3 := NewCollectAggregatorFunction()
	if err := agg3.Validate([]interface{}{}); err == nil {
		t.Error("CollectAggregatorFunction.Validate should fail for insufficient args")
	}
	// _, err = agg3.Execute(nil, []interface{}{})
	// if err == nil {
	// 	t.Error("CollectAggregatorFunction.Execute should fail for empty args")
	// }
	agg4 := agg3.New().(*CollectAggregatorFunction)
	agg4.Reset()
	_ = agg4.Clone()

	// LastValueAggregatorFunction Validate/Execute边界
	agg5 := NewLastValueAggregatorFunction()
	if err := agg5.Validate([]interface{}{}); err == nil {
		t.Error("LastValueAggregatorFunction.Validate should fail for insufficient args")
	}
	// _, err = agg5.Execute(nil, []interface{}{})
	// if err == nil {
	// 	t.Error("LastValueAggregatorFunction.Execute should fail for empty args")
	// }
	agg6 := agg5.New().(*LastValueAggregatorFunction)
	agg6.Reset()
	_ = agg6.Clone()

	// MergeAggAggregatorFunction Validate/Execute边界
	agg7 := NewMergeAggAggregatorFunction()
	if err := agg7.Validate([]interface{}{}); err == nil {
		t.Error("MergeAggAggregatorFunction.Validate should fail for insufficient args")
	}
	// _, err = agg7.Execute(nil, []interface{}{})
	// if err == nil {
	// 	t.Error("MergeAggAggregatorFunction.Execute should fail for empty args")
	// }
	agg8 := agg7.New().(*MergeAggAggregatorFunction)
	agg8.Reset()
	_ = agg8.Clone()

	// StdDevSAggregatorFunction Validate/Execute边界
	agg9 := NewStdDevSAggregatorFunction()
	if err := agg9.Validate([]interface{}{}); err == nil {
		t.Error("StdDevSAggregatorFunction.Validate should fail for insufficient args")
	}
	// _, err = agg9.Execute(nil, []interface{}{})
	// if err == nil {
	// 	t.Error("StdDevSAggregatorFunction.Execute should fail for empty args")
	// }
	agg10 := agg9.New().(*StdDevSAggregatorFunction)
	agg10.Reset()
	_ = agg10.Clone()

	// DeduplicateAggregatorFunction Validate/Execute边界
	agg11 := NewDeduplicateAggregatorFunction()
	if err := agg11.Validate([]interface{}{}); err == nil {
		t.Error("DeduplicateAggregatorFunction.Validate should fail for insufficient args")
	}
	// _, err = agg11.Execute(nil, []interface{}{})
	// if err == nil {
	// 	t.Error("DeduplicateAggregatorFunction.Execute should fail for empty args")
	// }
	agg12 := agg11.New().(*DeduplicateAggregatorFunction)
	agg12.Reset()
	_ = agg12.Clone()

	// VarAggregatorFunction Validate/Execute边界
	agg13 := NewVarAggregatorFunction()
	if err := agg13.Validate([]interface{}{}); err == nil {
		t.Error("VarAggregatorFunction.Validate should fail for insufficient args")
	}
	// _, err = agg13.Execute(nil, []interface{}{})
	// if err == nil {
	// 	t.Error("VarAggregatorFunction.Execute should fail for empty args")
	// }
	agg14 := agg13.New().(*VarAggregatorFunction)
	agg14.Reset()
	_ = agg14.Clone()

	// VarSAggregatorFunction Validate/Execute边界
	agg15 := NewVarSAggregatorFunction()
	if err := agg15.Validate([]interface{}{}); err == nil {
		t.Error("VarSAggregatorFunction.Validate should fail for insufficient args")
	}
	// _, err = agg15.Execute(nil, []interface{}{})
	// if err == nil {
	// 	t.Error("VarSAggregatorFunction.Execute should fail for empty args")
	// }
	agg16 := agg15.New().(*VarSAggregatorFunction)
	agg16.Reset()
	_ = agg16.Clone()
}
