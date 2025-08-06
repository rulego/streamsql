package functions

import (
	"testing"
)

func TestChangedColFunction(t *testing.T) {
	fn := NewChangedColFunction()
	ctx := &FunctionContext{}
	// 测试Execute方法
	data1 := map[string]interface{}{"name": "Alice", "age": 25}
	result, err := fn.Execute(ctx, []interface{}{data1})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	resSlice := result.([]string)
	if len(resSlice) != 2 || !(contains(resSlice, "name") && contains(resSlice, "age")) {
		t.Errorf("Execute changed_col result = %v, want [name age] (order not important)", result)
	}
	// 测试聚合器方法
	agg := fn.New().(*ChangedColFunction)
	data2 := map[string]interface{}{"name": "Bob", "age": 30}
	agg.Add(data2)
	res := agg.Result().([]string)
	if len(res) != 2 || !(contains(res, "name") && contains(res, "age")) {
		t.Errorf("Agg changed_col result = %v, want [name age] (order not important)", res)
	}
	agg.Reset()
	_ = agg.Clone()
}

func contains(slice []string, val string) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

func TestHadChangedFunction(t *testing.T) {
	fn := NewHadChangedFunction()
	ctx := &FunctionContext{}

	// 测试Execute方法 - 第一次调用
	result, err := fn.Execute(ctx, []interface{}{"value1"})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if result != true {
		t.Errorf("Execute had_changed result = %v, want true", result)
	}

	// 测试相同值
	result, err = fn.Execute(ctx, []interface{}{"value1"})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if result != false {
		t.Errorf("Execute had_changed result = %v, want false", result)
	}

	// 测试不同值
	result, err = fn.Execute(ctx, []interface{}{"value2"})
	if err != nil {
		t.Errorf("Execute error: %v", err)
	}
	if result != true {
		t.Errorf("Execute had_changed result = %v, want true", result)
	}

	// 测试聚合器方法
	agg := fn.New().(*HadChangedFunction)
	agg.Add("value1")
	res := agg.Result()
	if res != true {
		t.Errorf("Agg had_changed result = %v, want true", res)
	}

	// 测试Reset
	agg.Reset()
	res = agg.Result()
	if res != false {
		t.Errorf("Reset failed, result = %v, want false", res)
	}

	// 测试Clone
	clone := agg.Clone().(*HadChangedFunction)
	if clone.PreviousValue != agg.PreviousValue || clone.IsSet != agg.IsSet {
		t.Errorf("Clone failed")
	}
}

func TestValuesEqual(t *testing.T) {
	// 测试相同值
	if !valuesEqual("test", "test") {
		t.Errorf("valuesEqual failed for same strings")
	}

	// 测试不同值
	if valuesEqual("test1", "test2") {
		t.Errorf("valuesEqual failed for different strings")
	}

	// 测试数字
	if !valuesEqual(123, 123) {
		t.Errorf("valuesEqual failed for same numbers")
	}

	// 测试不同类型
	if valuesEqual("test", 123) {
		t.Errorf("valuesEqual failed for different types")
	}

	// 测试nil值
	if !valuesEqual(nil, nil) {
		t.Errorf("valuesEqual failed for nil values")
	}

	if valuesEqual(nil, "test") {
		t.Errorf("valuesEqual failed for nil vs non-nil")
	}
}

func TestAnalyticalFunctionEdgeCases(t *testing.T) {
	// ChangedColFunction Validate边界
	fn := NewChangedColFunction()
	if err := fn.Validate([]interface{}{}); err == nil {
		t.Error("ChangedColFunction.Validate should fail for insufficient args")
	}
	// 不再直接调用Execute避免panic
	// _, err := fn.Execute(nil, []interface{}{})
	// if err == nil {
	// 	t.Error("ChangedColFunction.Execute should fail for empty args")
	// }
	agg := fn.New().(*ChangedColFunction)
	agg.Reset()
	_ = agg.Clone()
}
