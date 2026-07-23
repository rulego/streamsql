package cep

import (
	"testing"
)

// Construct a minimal NFA fragment for low-level testing: start --epsilon--> m(A) --out--> accept
func buildLinearA() (start, accept *state, matchA *state) {
	matchA = &state{kind: stMatch, symbol: "A"}
	accept = &state{kind: stAccept}
	matchA.out1 = accept
	start = &state{kind: stEpsilon, out1: matchA}
	return
}

// closure Transition via epsilon should include the start and the reachable match final state, but **does not cross the match state** (accept
// After the match state, one line must be consumed before reaching it, so it is not included in the start epsilon closure).
func TestClosure_Epsilon(t *testing.T) {
	start, accept, matchA := buildLinearA()
	got := closure(start)
	if !containsState(got, start) || !containsState(got, matchA) {
		t.Errorf("closure must include start + reachable match-state: %+v", got)
	}
	if containsState(got, accept) {
		t.Errorf("closure must NOT cross a match-state (accept unreachable without consuming)")
	}
	if dup := dupCount(got); dup != 0 {
		t.Errorf("closure has %d duplicate state pointers", dup)
	}
}

// closure When stMatch terminates, it proceeds along that branch (match does not consume, so it does not follow its out).
func TestClosure_StopsAtMatch(t *testing.T) {
	_, _, matchA := buildLinearA()
	// Starting from matchA: The match state itself is in the closure, but its out (accept) should not be brought in by the epsilon closure.
	got := closure(matchA)
	if containsState(got, &state{kind: stAccept}) {
		// The above '%state{} is a new instance and won't be inside 'got'; Only used to trigger the containsState semantics.
	}
	if len(got) != 1 {
		t.Errorf("closure of a lone match-state want size 1, got %d", len(got))
	}
}

// Select branch: Split epsilon into two matches, closure should include both.
func TestClosure_AltSplit(t *testing.T) {
	a := &state{kind: stMatch, symbol: "A"}
	b := &state{kind: stMatch, symbol: "B"}
	split := &state{kind: stEpsilon, out1: a, out2: b}
	got := closure(split)
	if !containsState(got, a) || !containsState(got, b) {
		t.Errorf("alternation closure must include both branches: %+v", got)
	}
}

// isComplete: contains accept and no match-state → true; Contains accept and has match-state → false.
func TestIsComplete(t *testing.T) {
	accept := &state{kind: stAccept}
	if !isComplete([]*state{accept}) {
		t.Errorf("accept-only set should be complete")
	}
	matchA := &state{kind: stMatch, symbol: "A"}
	if isComplete([]*state{accept, matchA}) {
		t.Errorf("accept+match set should NOT be complete (still extendable)")
	}
	if isComplete([]*state{matchA}) {
		t.Errorf("match-only set should not be complete (no accept)")
	}
}

// hasAccept / matchStates.
func TestHasAcceptAndMatchStates(t *testing.T) {
	accept := &state{kind: stAccept}
	matchA := &state{kind: stMatch, symbol: "A"}
	states := []*state{matchA, accept}
	if !hasAccept(states) {
		t.Errorf("hasAccept should be true")
	}
	if ms := matchStates(states); len(ms) != 1 || ms[0] != matchA {
		t.Errorf("matchStates=%+v want [matchA]", ms)
	}
	if hasAccept([]*state{matchA}) {
		t.Errorf("hasAccept should be false without accept")
	}
}

// Patch points the pending edge of the clip to the target.
func TestPatch(t *testing.T) {
	s := &state{kind: stMatch, symbol: "A"}
	f := &frag{start: s, dots: []**state{&s.out1}}
	target := &state{kind: stAccept}
	patch(f, target)
	if s.out1 != target {
		t.Errorf("patch did not set out1 to target")
	}
}

func containsState(states []*state, want *state) bool {
	for _, s := range states {
		if s == want {
			return true
		}
	}
	return false
}

func dupCount(states []*state) int {
	seen := make(map[*state]int)
	for _, s := range states {
		seen[s]++
	}
	dups := 0
	for _, c := range seen {
		if c > 1 {
			dups += c - 1
		}
	}
	return dups
}
