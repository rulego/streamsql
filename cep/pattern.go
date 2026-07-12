package cep

import (
	"fmt"

	"github.com/rulego/streamsql/types"
)

// Compile 把模式树编译为 NFA（Thompson 构造）。组合式节点：序列/选择/分组/PERMUTE/量词。
// PatternExclusion（{- -}，absence）暂不支持，返回明确错误（P2）。
func Compile(node *types.PatternNode) (*NFA, error) {
	if node == nil {
		return nil, fmt.Errorf("MATCH_RECOGNIZE requires a PATTERN")
	}
	f, err := compileNode(node)
	if err != nil {
		return nil, err
	}
	accept := &state{kind: stAccept}
	patch(f, accept)
	return &NFA{start: f.start, accept: accept}, nil
}

func compileNode(n *types.PatternNode) (*frag, error) {
	switch n.Kind {
	case types.PatternLiteral:
		if n.Symbol == "" {
			return nil, fmt.Errorf("pattern variable name is empty")
		}
		return newMatchFrag(n.Symbol), nil
	case types.PatternSequence:
		var f *frag
		for _, c := range n.Children {
			cf, err := compileNode(c)
			if err != nil {
				return nil, err
			}
			if f == nil {
				f = cf
			} else {
				f = concat(f, cf)
			}
		}
		if f == nil {
			return newEpsFrag(), nil
		}
		return f, nil
	case types.PatternGroup:
		// Group 透明：编译其内部序列。量词由外层 Repetition 处理。
		if len(n.Children) == 0 {
			return newEpsFrag(), nil
		}
		return compileNode(n.Children[0])
	case types.PatternAlternation:
		if len(n.Children) == 0 {
			return newEpsFrag(), nil
		}
		var f *frag
		for _, c := range n.Children {
			cf, err := compileNode(c)
			if err != nil {
				return nil, err
			}
			if f == nil {
				f = cf
			} else {
				f = alt(f, cf)
			}
		}
		return f, nil
	case types.PatternRepetition:
		if len(n.Children) == 0 || n.Quant == nil {
			return nil, fmt.Errorf("repetition requires a child and quantifier")
		}
		return compileRepeat(n.Children[0], n.Quant)
	case types.PatternPermute:
		return compilePermute(n.Children)
	case types.PatternExclusion:
		return nil, fmt.Errorf("pattern exclusion {- -} is not supported yet (planned for a later phase)")
	}
	return nil, fmt.Errorf("unknown pattern node kind %d", n.Kind)
}

// compileRepeat 展开量词为显式 NFA：{n}=n 份；{n,}=n 份 + 星；{n,m}=n 份 + (m-n) 份可选；
// 每份重新编译子节点（独立状态），避免回环打结。
func compileRepeat(child *types.PatternNode, q *types.Quantifier) (*frag, error) {
	if q.Min < 0 {
		return nil, fmt.Errorf("quantifier min must be >= 0")
	}
	var f *frag
	for i := 0; i < q.Min; i++ {
		cf, err := compileNode(child)
		if err != nil {
			return nil, err
		}
		if f == nil {
			f = cf
		} else {
			f = concat(f, cf)
		}
	}
	if q.Max < 0 {
		cf, err := compileNode(child)
		if err != nil {
			return nil, err
		}
		star := starFrag(cf)
		if f == nil {
			return star, nil
		}
		return concat(f, star), nil
	}
	if q.Max < q.Min {
		return nil, fmt.Errorf("quantifier max %d < min %d", q.Max, q.Min)
	}
	for i := 0; i < q.Max-q.Min; i++ {
		cf, err := compileNode(child)
		if err != nil {
			return nil, err
		}
		opt := optFrag(cf)
		if f == nil {
			f = opt
		} else {
			f = concat(f, opt)
		}
	}
	if f == nil {
		return newEpsFrag(), nil // {0}：匹配空
	}
	return f, nil
}

// compilePermute 把 PERMUTE(A,B,...) 编译为所有排列的交替（任一顺序匹配）。
func compilePermute(children []*types.PatternNode) (*frag, error) {
	if len(children) == 0 {
		return newEpsFrag(), nil
	}
	// 排列数为 N!，符号过多会导致 NFA 状态阶乘级膨胀，设上限保护。
	if len(children) > 6 {
		return nil, fmt.Errorf("PERMUTE supports at most 6 symbols (got %d): factorial state blow-up", len(children))
	}
	var result *frag
	for _, perm := range permutations(len(children)) {
		// 每个排列独立状态：按索引顺序重新编译子节点。
		var f *frag
		for _, idx := range perm {
			cf, err := compileNode(children[idx])
			if err != nil {
				return nil, err
			}
			if f == nil {
				f = cf
			} else {
				f = concat(f, cf)
			}
		}
		if result == nil {
			result = f
		} else {
			result = alt(result, f)
		}
	}
	return result, nil
}

// permutations 返回 [0,n) 的所有排列索引。
func permutations(n int) [][]int {
	if n == 0 {
		return [][]int{{}}
	}
	subs := permutations(n - 1)
	var out [][]int
	for _, s := range subs {
		for i := 0; i <= len(s); i++ {
			p := make([]int, 0, len(s)+1)
			p = append(p, s[:i]...)
			p = append(p, n-1)
			p = append(p, s[i:]...)
			out = append(out, p)
		}
	}
	return out
}
