package cep

import (
	"testing"
)

// 构造一个最小 NFA 片段用于低层测试：start --epsilon--> m(A) --out--> accept
func buildLinearA() (start, accept *state, matchA *state) {
	matchA = &state{kind: stMatch, symbol: "A"}
	accept = &state{kind: stAccept}
	matchA.out1 = accept
	start = &state{kind: stEpsilon, out1: matchA}
	return
}

// closure 经 epsilon 转移应含起点与可达的 match 终态，但**不穿越 match 态**（accept
// 在 match 态之后，需消费一行后才可达，故不在 start 的 epsilon 闭包里）。
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

// closure 遇 stMatch 终止沿该分支前进（match 不消费，故不跟出其 out）。
func TestClosure_StopsAtMatch(t *testing.T) {
	_, _, matchA := buildLinearA()
	// 从 matchA 出发：match 态本身在闭包里，但其 out（accept）不应被 epsilon 闭包带入。
	got := closure(matchA)
	if containsState(got, &state{kind: stAccept}) {
		// 上面的 &state{} 是新建实例，不会在 got 里；仅用于触发 containsState 语义。
	}
	if len(got) != 1 {
		t.Errorf("closure of a lone match-state want size 1, got %d", len(got))
	}
}

// 选择分支：epsilon 分裂到两条 match，closure 应含两者。
func TestClosure_AltSplit(t *testing.T) {
	a := &state{kind: stMatch, symbol: "A"}
	b := &state{kind: stMatch, symbol: "B"}
	split := &state{kind: stEpsilon, out1: a, out2: b}
	got := closure(split)
	if !containsState(got, a) || !containsState(got, b) {
		t.Errorf("alternation closure must include both branches: %+v", got)
	}
}

// isComplete：含 accept 且无 match-state → true；含 accept 且有 match-state → false。
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

// hasAccept / matchStates。
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

// patch 把片段的待接续出边指向目标。
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
