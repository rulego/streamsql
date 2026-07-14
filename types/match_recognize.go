package types

import "time"

// ExecMode 选择一条查询的执行路径。扩展自原 NeedWindow 二分：直连 / 窗口 / CEP。
// NeedWindow 保留为 ExecMode==ExecWindow 的便捷谓词（向后兼容）。
type ExecMode int

const (
	// ExecDirect 纯直连路径：逐事件 EmitSync/projectDirectRow，含分析函数状态机。
	ExecDirect ExecMode = iota
	// ExecWindow 窗口/聚合路径：Emit 异步入窗，触发时聚合输出。
	ExecWindow
	// ExecCEP MATCH_RECOGNIZE 模式识别路径：逐事件推进 NFA，匹配完成时输出。
	ExecCEP
)

// RowsPerMatch 选择每次匹配的输出形态。
type RowsPerMatch int

const (
	// RowsPerMatchOne 每次匹配输出一行（MEASURES 在匹配末行求值）。默认。
	RowsPerMatchOne RowsPerMatch = iota
	// RowsPerMatchAll 每次匹配输出全部行（含 RUNNING/FINAL 逐行求值）。
	RowsPerMatchAll
)

// AfterMatchSkip 选择匹配完成后下一轮匹配的起点（SQL 标准 AFTER MATCH SKIP）。
type AfterMatchSkip int

const (
	// SkipPastLastRow 从匹配末行的下一行开始（默认）。
	SkipPastLastRow AfterMatchSkip = iota
	// SkipToNextRow 从匹配首行的下一行开始（允许重叠）。
	SkipToNextRow
	// SkipToFirst 跳到指定符号在匹配中的首行位置。
	SkipToFirst
	// SkipToLast 跳到指定符号在匹配中的末行位置。
	SkipToLast
	// SkipToVariable 跳到指定符号（等同 TO LAST 的别名形式）。
	SkipToVariable
)

// Quantifier 描述模式原子的量词边界。Max<0 表示无上界（* / + / {n,}）。
type Quantifier struct {
	Min    int
	Max    int
	Greedy bool // true=贪婪（默认），false=懒惰（量词后跟 ?）
}

// PatternKind 标识组合式模式节点的类型。
type PatternKind int

const (
	PatternLiteral PatternKind = iota
	PatternSequence
	PatternAlternation
	PatternRepetition
	PatternGroup
	PatternPermute
	PatternExclusion
)

// PatternNode 是模式树的一个节点。组合式：Sequence/Alternation/Group/Permute 用
// Children 组合；Repetition 用 Children[0] 携带单个被重复子式 + Quant；Literal 用 Symbol。
type PatternNode struct {
	Kind     PatternKind
	Symbol   string         // Literal：模式变量名
	Children []*PatternNode // Sequence/Alternation/Group/Permute/Repetition 的子节点
	Quant    *Quantifier    // Repetition 的量词
}

// Measure 描述 MEASURES 子句的一项：<expr> AS <alias>。
type Measure struct {
	Expr  string
	Alias string
}

// MatchDefine 描述 DEFINE 子句的一项：<symbol> AS <cond>。
// 未出现在 DEFINE 中的模式变量恒为真（SQL 标准）。
type MatchDefine struct {
	Symbol string
	Cond   string
}

// MatchSubset 描述 SUBSET 子句：<name> = (<sym> [, ...])。
type MatchSubset struct {
	Name    string
	Symbols []string
}

// MatchRecognizeSpec 持有 MATCH_RECOGNIZE 子句的全部子结构。
type MatchRecognizeSpec struct {
	PartitionBy  []string
	OrderBy      []OrderByField
	Measures     []Measure
	RowsPerMatch RowsPerMatch
	Skip         AfterMatchSkip
	SkipSymbol   string        // SKIP TO FIRST/LAST/<symbol> 的目标符号
	Pattern      *PatternNode
	Subsets      []MatchSubset
	Within       time.Duration // 0 表示用默认上限（CEP 强制有界）
	Defines      []MatchDefine
}

// 默认 WITHIN 上限：未显式指定 WITHIN 时强制施加，保证边缘内存有界。
const DefaultMatchWithin = 1 * time.Hour
