package functions

import (
	"testing"

	"github.com/rulego/streamsql/utils/cast"
	"github.com/stretchr/testify/assert"
)

// oneEntrySum fully implements functions.AggregatorFunction, used to verify:
// To customize an aggregate function, just use functions.Register can be used by runtime with just one entry point.
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
func (f *oneEntrySum) Reset() { f.sum = 0; f.ok = false }
func (f *oneEntrySum) Clone() AggregatorFunction {
	return &oneEntrySum{BaseFunction: f.BaseFunction, sum: f.sum, ok: f.ok}
}

// Verify the main assertion: Custom aggregation functions only need functions.Register a single entry point,
// This can simultaneously satisfy the aggregation consumption of parsing period validation and runtime periods.
func TestAggregatorSingleEntryRegister(t *testing.T) {
	err := Register(newOneEntrySum())
	assert.NoError(t, err)
	defer Unregister("zz_oneentry_sum")

	// Resolution period entry: function_validator.go only checks functions.Get
	_, exists := Get("zz_oneentry_sum")
	assert.True(t, exists, "functions.Get 应能找到已注册的聚合函数")

	// Runtime entry: The aggregator consumer uses CreateLegacyAggregator, automated by registry.go
	// adapter is turned on, no need to manually RegisterAggregatorAdapter / aggregator.Register.
	agg := CreateLegacyAggregator("zz_oneentry_sum")
	assert.NotNil(t, agg, "应通过自动 adapter 返回有效聚合器")

	agg.Add(1.0)
	agg.Add(2.0)
	agg.Add(3.0)
	assert.Equal(t, 6.0, agg.Result())
}

// legacyOnlySum only implements the LegacyAggregatorFunction (3 method interface).
// Note: It cannot be merged with oneEntrySum because the New() return types of the two interfaces are different
// (AggregatorFunction vs LegacyAggregatorFunction), the same structure cannot be implemented simultaneously.
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

// Counterproof: Only go aggregator.Register(RegisterLegacyAggregator) without entering the functions table,
// SQL parsing phase functions.Get can't find — prove aggregator.Register alone is not enough.
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

func (f *aliasedTestFunc) Validate(args []any) error                             { return nil }
func (f *aliasedTestFunc) Execute(ctx *FunctionContext, args []any) (any, error) { return nil, nil }

func newAliasedTestFunc(name string, aliases ...string) *aliasedTestFunc {
	return &aliasedTestFunc{BaseFunction: NewBaseFunctionWithAliases(name, TypeCustom, "test", "alias test", 0, 0, aliases)}
}

// Register Alias Atomization: When there is an alias conflict, the primary name does not remain, and registered functions are not affected.
func TestRegisterAliasAtomicNoDirtyState(t *testing.T) {
	a := newAliasedTestFunc("zz_alias_a", "zz_alias_shared")
	assert.NoError(t, Register(a))
	defer Unregister("zz_alias_a")

	// B's alias conflicts with A, so registration should fail
	b := newAliasedTestFunc("zz_alias_b", "zz_alias_shared")
	err := Register(b)
	assert.Error(t, err)

	// No Left Organs: B Main Name Not Submitted to the Table
	_, bExists := Get("zz_alias_b")
	assert.False(t, bExists, "别名冲突时主名不应残留")

	// A Complete: Both the main name and the alias are present, and the alias still points to A
	_, aExists := Get("zz_alias_a")
	assert.True(t, aExists)
	got, sharedExists := Get("zz_alias_shared")
	assert.True(t, sharedExists)
	assert.Equal(t, "zz_alias_a", got.GetName())
}

// RegisterCustomFunction rejects aggregation/analysis types to avoid closure silence obsolete functions.
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
