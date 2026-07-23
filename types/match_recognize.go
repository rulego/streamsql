package types

import "time"

// ExecMode selects the execution path for a query. Extended from the original NeedWindow binary split: direct connection / window / CEP.
// NeedWindow is reserved as ExecMode==ExecWindow's convenient predicate (backward compatible).
type ExecMode int

const (
	// ExecDirect pure direct connection path: event-by-event EmitSync/projectDirectRow, including analysis function state machine.
	ExecDirect ExecMode = iota
	// ExecWindow window/aggregation path: Emit asynchronously enters the window, and aggregates output when triggered.
	ExecWindow
	// ExecCEP MATCH_RECOGNIZE Pattern Recognition Path: Advances NFA event-by-event and outputs when matching is complete.
	ExecCEP
)

// RowsPerMatch selects the output form for each match.
type RowsPerMatch int

const (
	// RowsPerMatchOne outputs one line per match (MEASURES evaluates the last line of the match). Default.
	RowsPerMatchOne RowsPerMatch = iota
	// RowsPerMatchAll outputs all rows (including RUNNING/FINAL line by line evaluation) for each match.
	RowsPerMatchAll
)

// AfterMatchSkip selects the starting point for the next round of matching after the match is completed (SQL standard AFTER MATCH SKIP).
type AfterMatchSkip int

const (
	// SkipPastLastRow starts from the next row after matching the last line (default).
	SkipPastLastRow AfterMatchSkip = iota
	// SkipToNextRow starts from the next row that matches the first row (overlapping allowed).
	SkipToNextRow
	// SkipToFirst jumps to the position of the specified symbol in the first row of the match.
	SkipToFirst
	// SkipToLast jumps to the last row position of the specified symbol in the match.
	SkipToLast
	// SkipToVariable jumps to the specifier (equivalent to the alias form for TO LAST).
	SkipToVariable
)

// Quantifier describes the quantifier boundary of a model atom. Max<0 represents the Supreme Realm (* / + / {n,}).
type Quantifier struct {
	Min    int
	Max    int
	Greedy bool // true = greed (default), false = laziness (measure word followed by?)
}

// PatternKind identifies the type of composable mode node.
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

// PatternNode is a node in the pattern tree. Combinatorial: used for Sequence/Alternation/Group/Permute
// Children group; Repetition carries a single repeatedn subexpression + Quant with Children[0]; Literal uses Symbol.
type PatternNode struct {
	Kind     PatternKind
	Symbol   string         // Literal: The name of the pattern variable
	Children []*PatternNode // Child nodes of Sequence/Alternation/Group/Permute/Repetition
	Quant    *Quantifier    // Repetition
}

// Measure describes one of the MEASURES clauses:<expr> AS<alias>.
type Measure struct {
	Expr  string
	Alias string
}

// MatchDefine describes a DEFINE clause that is:<symbol> AS<cond>.
// Pattern variables not appearing in DEFINE are always true (SQL standard).
type MatchDefine struct {
	Symbol string
	Cond   string
}

// MatchSubset describes the SUBSET clause:<name> = (<sym> [,...]).
type MatchSubset struct {
	Name    string
	Symbols []string
}

// MatchRecognizeSpec holds the entire substructure of the MATCH_RECOGNIZE clause.
type MatchRecognizeSpec struct {
	PartitionBy  []string
	OrderBy      []OrderByField
	Measures     []Measure
	RowsPerMatch RowsPerMatch
	Skip         AfterMatchSkip
	SkipSymbol   string // The target symbol for SKIP TO FIRST/LAST<symbol>/
	Pattern      *PatternNode
	Subsets      []MatchSubset
	Within       time.Duration // 0 indicates the default limit (CEP is mandatory bounded)
	Defines      []MatchDefine
}

// Default WITHIN limit: Enforced when WITHIN is not explicitly specified, ensuring bounded edge memory.
const DefaultMatchWithin = 1 * time.Hour
