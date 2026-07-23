package cep

// stateKind identifies the NFA state category.
type stateKind int

const (
	stEpsilon stateKind = iota // Don't spend money, just move forward along out1/out2
	stMatch                    // Consuming a line: When a line satisfies the DEFINE of the symbol, it moves forward along out1
	stAccept                   // Acceptance state: a complete match at once
)

// state is a state of the NFA. The DEFINE condition is checked in the engine by the symbol, so state only carries the symbol.
type state struct {
	kind   stateKind
	symbol string // stMatch: Name of the mode variable
	out1   *state
	out2   *state // Only stEpsilon is selected/recycled
}

// NFA is a compiled mode automaton: start is transferred via epsilon/match to accept.
type NFA struct {
	start  *state
	accept *state
}

// frag is a piece constructed by Thompson: start and several out-of-the-line edges to be joined (dots point to the out slot of nil).
type frag struct {
	start *state
	dots  []**state
}

func patch(f *frag, target *state) {
	for _, d := range f.dots {
		*d = target
	}
}

func newMatchFrag(symbol string) *frag {
	s := &state{kind: stMatch, symbol: symbol}
	return &frag{start: s, dots: []**state{&s.out1}}
}

func newEpsFrag() *frag {
	s := &state{kind: stEpsilon}
	return &frag{start: s, dots: []**state{&s.out1}}
}

func concat(a, b *frag) *frag {
	patch(a, b.start)
	return &frag{start: a.start, dots: b.dots}
}

func alt(a, b *frag) *frag {
	s := &state{kind: stEpsilon, out1: a.start, out2: b.start}
	return &frag{start: s, dots: append(a.dots, b.dots...)}
}

// starFrag constructs child* (greedy: priority loop).
func starFrag(child *frag) *frag {
	s := &state{kind: stEpsilon}
	patch(child, s) // After the subsegment ends, return to the branching point
	s.out1 = child.start
	return &frag{start: s, dots: []**state{&s.out2}} // out2=Exit (to be continued)
}

// optFrag constructs child?(0 or 1).
func optFrag(child *frag) *frag {
	s := &state{kind: stEpsilon, out1: child.start}
	return &frag{start: s, dots: append(child.dots, &s.out2)}
}

// closure calculates all states that can be reached via epsilon transfer (including match/accept final states).
func closure(starts ...*state) []*state {
	seen := make(map[*state]bool, len(starts))
	var stack []*state
	push := func(s *state) {
		if s != nil && !seen[s] {
			seen[s] = true
			stack = append(stack, s)
		}
	}
	for _, s := range starts {
		push(s)
	}
	for len(stack) > 0 {
		s := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if s.kind == stEpsilon {
			push(s.out1)
			push(s.out2)
		}
	}
	out := make([]*state, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	return out
}

// hasAccept reports whether the state set contains the acceptance state.
func hasAccept(states []*state) bool {
	for _, s := range states {
		if s.kind == stAccept {
			return true
		}
	}
	return false
}

// isComplete reports whether the state set has 'reached the accepting state and can no longer be extended' (Greed Ends).
// Items containing accept but still having match-state (such as A* continuation) do not end and should continue to be greedily extended.
func isComplete(states []*state) bool {
	return hasAccept(states) && len(matchStates(states)) == 0
}

// matchStates extracts all stMatch (pending line testing) from the state set.
func matchStates(states []*state) []*state {
	var ms []*state
	for _, s := range states {
		if s.kind == stMatch {
			ms = append(ms, s)
		}
	}
	return ms
}
