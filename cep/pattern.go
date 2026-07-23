package cep

import (
	"fmt"

	"github.com/rulego/streamsql/types"
)

// Compile compiles the pattern tree into an NFA (Thompson construct). Composable nodes: sequence/selection/grouping/PERMUTE/quantifier.
// PatternExclusion({- -}, absence) is not supported at this time and returns a definite error.
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
		// Group transparency: compiles its internal sequence. Quantifiers are processed by the outer Repetition.
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

// compileRepeat expands the quantifier to explicit NFA: {n} = n parts; {n,} = n parts + stars; {n,m} = n parts + (m-n) parts optional;
// Each copy is recompiled to recompile child nodes (independent state) to avoid looping knots.
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
		return newEpsFrag(), nil // {0}: Match empty
	}
	return f, nil
}

// compilePermute compiles PERMUTE(A, B,...) into all permutations (any order match).
func compilePermute(children []*types.PatternNode) (*frag, error) {
	if len(children) == 0 {
		return newEpsFrag(), nil
	}
	// If the number of permutations is N!, too many symbols will cause the NFA state to expand exponentially, so limit protection is set.
	if len(children) > 6 {
		return nil, fmt.Errorf("PERMUTE supports at most 6 symbols (got %d): factorial state blow-up", len(children))
	}
	var result *frag
	for _, perm := range permutations(len(children)) {
		// Each permutation has its own independent state: Recompile child nodes in index order.
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

// permutations returns all permutation indexes for [0,n).
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
