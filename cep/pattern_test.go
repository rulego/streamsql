package cep

import (
	"testing"

	"github.com/rulego/streamsql/types"
)

// Compile 合法模式：返回非空 NFA（start/accept 非 nil）且无错误。
func TestCompile_ValidPatterns(t *testing.T) {
	cases := []struct {
		name string
		node *types.PatternNode
	}{
		{"literal", lit("A")},
		{"sequence", seq(lit("A"), lit("B"), lit("C"))},
		{"alternation", altNode(lit("A"), lit("B"))},
		{"group", &types.PatternNode{Kind: types.PatternGroup, Children: []*types.PatternNode{seq(lit("A"), lit("B"))}}},
		{"star", rep(lit("A"), 0, -1)},
		{"plus", rep(lit("A"), 1, -1)},
		{"optional", rep(lit("A"), 0, 1)},
		{"exact", rep(lit("A"), 3, 3)},
		{"range", rep(lit("A"), 2, 5)},
		{"atLeast", rep(lit("A"), 2, -1)},
		{"permute", &types.PatternNode{Kind: types.PatternPermute, Children: []*types.PatternNode{lit("A"), lit("B")}}},
		{"nested", seq(rep(altNode(lit("A"), lit("B")), 1, -1), lit("C"))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			nfa, err := Compile(c.node)
			if err != nil {
				t.Fatalf("Compile(%s) error: %v", c.name, err)
			}
			if nfa == nil || nfa.start == nil || nfa.accept == nil {
				t.Fatalf("Compile(%s) returned incomplete NFA: %+v", c.name, nfa)
			}
		})
	}
}

// Compile 非法模式：明确报错。
func TestCompile_InvalidPatterns(t *testing.T) {
	cases := []struct {
		name string
		node *types.PatternNode
	}{
		{"nil", nil},
		{"empty literal", &types.PatternNode{Kind: types.PatternLiteral, Symbol: ""}},
		{"exclusion", &types.PatternNode{Kind: types.PatternExclusion, Children: []*types.PatternNode{lit("A")}}},
		{"negative min", rep(lit("A"), -1, -1)},
		{"max < min", rep(lit("A"), 5, 3)},
		{"repetition no child", &types.PatternNode{Kind: types.PatternRepetition, Quant: &types.Quantifier{Min: 1, Max: 1}}},
		{"repetition no quant", &types.PatternNode{Kind: types.PatternRepetition, Children: []*types.PatternNode{lit("A")}}},
		{"unknown kind", &types.PatternNode{Kind: types.PatternKind(999)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Compile(c.node); err == nil {
				t.Errorf("Compile(%s) want error, got nil", c.name)
			}
		})
	}
}

// {0} 量词匹配空：应编译成功（空片段），NFA 仍可达 accept。
func TestCompile_ZeroQuantifier(t *testing.T) {
	nfa, err := Compile(rep(lit("A"), 0, 0))
	if err != nil {
		t.Fatalf("Compile({0}) error: %v", err)
	}
	if nfa == nil || nfa.start == nil || nfa.accept == nil {
		t.Fatalf("Compile({0}) incomplete NFA")
	}
}

// PERMUTE 多符号：编译成功（排列数为 N!，符号多时状态膨胀，这里仅验证 3 符号可编译）。
func TestCompile_PermuteThree(t *testing.T) {
	node := &types.PatternNode{Kind: types.PatternPermute, Children: []*types.PatternNode{
		lit("A"), lit("B"), lit("C"),
	}}
	if _, err := Compile(node); err != nil {
		t.Fatalf("Compile(PERMUTE A,B,C) error: %v", err)
	}
}

// PERMUTE 超过 6 符号：阶乘级状态膨胀，编译期拒绝。
func TestCompile_PermuteLimit(t *testing.T) {
	kids := make([]*types.PatternNode, 7)
	for i := range kids {
		kids[i] = lit(string(rune('A' + i)))
	}
	node := &types.PatternNode{Kind: types.PatternPermute, Children: kids}
	if _, err := Compile(node); err == nil {
		t.Errorf("Compile(PERMUTE 7 symbols) want error, got nil")
	}
}
