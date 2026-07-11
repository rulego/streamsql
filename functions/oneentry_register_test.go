package functions

import (
	"testing"

	"github.com/rulego/streamsql/utils/cast"
	"github.com/stretchr/testify/assert"
)

// oneEntrySum 完整实现 functions.AggregatorFunction，用于验证：
// 自定义聚合函数只需 functions.Register 一个入口即可被运行期使用。
type oneEntrySum struct {
	*BaseFunction
	sum float64
	ok  bool
}

func newOneEntrySum() *oneEntrySum {
	return &oneEntrySum{BaseFunction: NewBaseFunction("zz_oneentry_sum", TypeAggregation, "test", "one-entry sum", 1, -1)}
}

func (f *oneEntrySum) Validate(args []any) error { return f.ValidateArgCount(args) }
func (f *oneEntrySum) Execute(ctx *FunctionContext, args []any) (any, error) {
	s := 0.0
	for _, a := range args {
		if a == nil {
			continue
		}
		if v, err := cast.ToFloat64E(a); err == nil {
			s += v
		}
	}
	return s, nil
}
func (f *oneEntrySum) New() AggregatorFunction { return &oneEntrySum{BaseFunction: f.BaseFunction} }
func (f *oneEntrySum) Add(v any) {
	if v == nil {
		return
	}
	if x, err := cast.ToFloat64E(v); err == nil {
		f.sum += x
		f.ok = true
	}
}
func (f *oneEntrySum) Result() any {
	if !f.ok {
		return nil
	}
	return f.sum
}
func (f *oneEntrySum) Reset()                                   { f.sum = 0; f.ok = false }
func (f *oneEntrySum) Clone() AggregatorFunction {
	return &oneEntrySum{BaseFunction: f.BaseFunction, sum: f.sum, ok: f.ok}
}

// 验证主断言：自定义聚合函数只需 functions.Register 一个入口，
// 即可同时满足解析期校验与运行期聚合消费。
func TestAggregatorSingleEntryRegister(t *testing.T) {
	err := Register(newOneEntrySum())
	assert.NoError(t, err)
	defer Unregister("zz_oneentry_sum")

	// 解析期入口：function_validator.go 只查 functions.Get
	_, exists := Get("zz_oneentry_sum")
	assert.True(t, exists, "functions.Get 应能找到已注册的聚合函数")

	// 运行期入口：聚合消费方走 CreateLegacyAggregator，由 registry.go 的自动
	// adapter 接通，无需手动 RegisterAggregatorAdapter / aggregator.Register。
	agg := CreateLegacyAggregator("zz_oneentry_sum")
	assert.NotNil(t, agg, "应通过自动 adapter 返回有效聚合器")

	agg.Add(1.0)
	agg.Add(2.0)
	agg.Add(3.0)
	assert.Equal(t, 6.0, agg.Result())
}

// legacyOnlySum 只实现 LegacyAggregatorFunction（3 方法接口）。
// 注意：它与 oneEntrySum 不能合并，因为两套接口的 New() 返回类型不同
// （AggregatorFunction vs LegacyAggregatorFunction），同一结构体无法同时实现。
type legacyOnlySum struct {
	sum float64
	ok  bool
}

func (f *legacyOnlySum) New() LegacyAggregatorFunction { return &legacyOnlySum{} }
func (f *legacyOnlySum) Add(v any) {
	if v == nil {
		return
	}
	if x, err := cast.ToFloat64E(v); err == nil {
		f.sum += x
		f.ok = true
	}
}
func (f *legacyOnlySum) Result() any {
	if !f.ok {
		return nil
	}
	return f.sum
}

// 反证：仅走 aggregator.Register（RegisterLegacyAggregator）而不进 functions 表，
// SQL 解析期 functions.Get 找不到 —— 证明 aggregator.Register 单独不够。
func TestLegacyOnlyRegisterNotVisibleToParser(t *testing.T) {
	RegisterLegacyAggregator("zz_legacy_only_sum", func() LegacyAggregatorFunction {
		return &legacyOnlySum{}
	})

	_, exists := Get("zz_legacy_only_sum")
	assert.False(t, exists, "仅 aggregator.Register 不进 functions 表，解析期会判为 unknown function")
}

type aliasedTestFunc struct {
	*BaseFunction
}

func (f *aliasedTestFunc) Validate(args []any) error                 { return nil }
func (f *aliasedTestFunc) Execute(ctx *FunctionContext, args []any) (any, error) { return nil, nil }

func newAliasedTestFunc(name string, aliases ...string) *aliasedTestFunc {
	return &aliasedTestFunc{BaseFunction: NewBaseFunctionWithAliases(name, TypeCustom, "test", "alias test", 0, 0, aliases)}
}

// 验证 P1：Register 别名原子化 —— 别名冲突时主名不残留、已注册函数不受影响。
func TestRegisterAliasAtomicNoDirtyState(t *testing.T) {
	a := newAliasedTestFunc("zz_alias_a", "zz_alias_shared")
	assert.NoError(t, Register(a))
	defer Unregister("zz_alias_a")

	// B 的别名与 A 冲突，注册应失败
	b := newAliasedTestFunc("zz_alias_b", "zz_alias_shared")
	err := Register(b)
	assert.Error(t, err)

	// 不留脏：B 主名未进表
	_, bExists := Get("zz_alias_b")
	assert.False(t, bExists, "别名冲突时主名不应残留")

	// A 完整：主名与别名都在，且别名仍指向 A
	_, aExists := Get("zz_alias_a")
	assert.True(t, aExists)
	got, sharedExists := Get("zz_alias_shared")
	assert.True(t, sharedExists)
	assert.Equal(t, "zz_alias_a", got.GetName())
}

// 验证 P1：RegisterCustomFunction 拒绝聚合/分析类型，避免 closure 静默废函数。
func TestRegisterCustomFunctionRejectsAggregation(t *testing.T) {
	err := RegisterCustomFunction("zz_reject_agg", TypeAggregation, "test", "should reject", 1, 1,
		func(ctx *FunctionContext, args []any) (any, error) { return args[0], nil })
	assert.Error(t, err)
	_, exists := Get("zz_reject_agg")
	assert.False(t, exists, "被拒的聚合类型不应进入注册表")

	err = RegisterCustomFunction("zz_reject_ana", TypeAnalytical, "test", "should reject", 1, 1,
		func(ctx *FunctionContext, args []any) (any, error) { return args[0], nil })
	assert.Error(t, err)
	_, exists = Get("zz_reject_ana")
	assert.False(t, exists)
}
