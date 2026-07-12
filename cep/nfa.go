package cep

// stateKind 标识 NFA 状态类别。
type stateKind int

const (
	stEpsilon stateKind = iota // 不消费行，沿 out1/out2 前进
	stMatch                    // 消费一行：当行满足 symbol 的 DEFINE 时，沿 out1 前进
	stAccept                   // 接受态：一次完整匹配
)

// state 是 NFA 的一个状态。DEFINE 条件按 symbol 在引擎处查表，故 state 只携 symbol。
type state struct {
	kind   stateKind
	symbol string // stMatch：模式变量名
	out1   *state
	out2   *state // 仅 stEpsilon 的选择/循环用到
}

// NFA 是编译后的模式自动机：start 经 epsilon/match 转移到达 accept。
type NFA struct {
	start  *state
	accept *state
}

// frag 是 Thompson 构造的片段：start 与若干待接续的出边（dots 指向 nil 的 out 槽）。
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

// starFrag 构造 child*（贪婪：优先回环）。
func starFrag(child *frag) *frag {
	s := &state{kind: stEpsilon}
	patch(child, s) // 子片段结束后回到分支点
	s.out1 = child.start
	return &frag{start: s, dots: []**state{&s.out2}} // out2=出口（待接续）
}

// optFrag 构造 child?（0 或 1）。
func optFrag(child *frag) *frag {
	s := &state{kind: stEpsilon, out1: child.start}
	return &frag{start: s, dots: append(child.dots, &s.out2)}
}

// closure 计算 starts 经 epsilon 转移可达的全部状态（含 match/accept 终态）。
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

// hasAccept 报告状态集是否含接受态。
func hasAccept(states []*state) bool {
	for _, s := range states {
		if s.kind == stAccept {
			return true
		}
	}
	return false
}

// isComplete 报告状态集是否「到达接受态且无法再延伸」（贪婪终结）。
// 含 accept 但仍有 match-state 的（如 A* 续配）不算终结，应继续贪婪延伸。
func isComplete(states []*state) bool {
	return hasAccept(states) && len(matchStates(states)) == 0
}

// matchStates 抽出状态集中的全部 stMatch（待消费行测试）。
func matchStates(states []*state) []*state {
	var ms []*state
	for _, s := range states {
		if s.kind == stMatch {
			ms = append(ms, s)
		}
	}
	return ms
}
