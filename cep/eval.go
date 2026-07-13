// Package cep 实现 MATCH_RECOGNIZE 模式识别（SQL:2016）。
//
// 架构：
//   - pattern.go  组合式模式树 → NFA（Thompson 构造），支持序列/量词/选择/分组/PERMUTE。
//   - nfa.go      NFA 状态机 + epsilon 闭包。
//   - eval.go     DEFINE/MEASURES 求值：表达式在 NewEngine 期预编译（prepare），
//     导航/聚合/符号限定字段改写为占位符，复用手写 expr 引擎求值。
//   - engine.go   分区 NFA 模拟 + LRU + WITHIN/行数上限（有界）+ MEASURES 投影 + 匹配输出。
//
// CEP 是独立子系统，不依赖也不污染现有直连/窗口/分析路径与三套求值器。
package cep

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/utils/cast"
)

// matchCtx 是 DEFINE/MEASURES 求值的上下文：一次匹配尝试的全部行 + 分类标签。
// DEFINE 求值时 candidate=待分类行、cur=len(rows)（candidate 概念上在 cur 位置）；
// MEASURES 求值时 candidate=nil、cur=求值行下标（ONE ROW=末行）。
type matchCtx struct {
	rows        []map[string]any
	labels      []string
	cur         int
	candidate   map[string]any
	candLabel   string
	symbols     map[string]bool
	subsets     map[string][]string // SUBSET 名 → 成员符号（按成员集合过滤；nil=普通符号）
	matchNumber int
}

// placeholder 前缀：导航/聚合/符号限定字段求值后注入 base 的合成键。
const placeholderPrefix = "__cep_"

// ekind 标识表达式 token 的类别（仅 rewriter 内部用）。
type ekind int

const (
	ekIdent ekind = iota
	ekNumber
	ekString
	ekOp
	ekLParen
	ekRParen
	ekComma
	ekDot
)

type etoken struct {
	kind ekind
	val  string
}

// tokenize 表达式为 token 流。仅服务于结构识别（函数调用、符号限定、括号配平）；
// 算术/比较/逻辑的语义交给 expr 引擎，故运算符一律按原样保留。
func tokenize(s string) ([]etoken, error) {
	var toks []etoken
	runes := []rune(s)
	i := 0
	for i < len(runes) {
		c := runes[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			toks = append(toks, etoken{ekLParen, "("})
			i++
		case c == ')':
			toks = append(toks, etoken{ekRParen, ")"})
			i++
		case c == ',':
			toks = append(toks, etoken{ekComma, ","})
			i++
		case c == '.':
			toks = append(toks, etoken{ekDot, "."})
			i++
		case c == '\'' || c == '"' || c == '`':
			j := i + 1
			for j < len(runes) && runes[j] != c {
				j++
			}
			if j >= len(runes) {
				return nil, fmt.Errorf("unterminated quote in CEP expression: %s", s)
			}
			toks = append(toks, etoken{ekString, string(runes[i : j+1])})
			i = j + 1
		case unicode.IsLetter(c) || c == '_':
			j := i
			for j < len(runes) && (unicode.IsLetter(runes[j]) || unicode.IsDigit(runes[j]) || runes[j] == '_') {
				j++
			}
			toks = append(toks, etoken{ekIdent, string(runes[i:j])})
			i = j
		case unicode.IsDigit(c):
			j := i
			for j < len(runes) && (unicode.IsDigit(runes[j]) || runes[j] == '.') {
				j++
			}
			toks = append(toks, etoken{ekNumber, string(runes[i:j])})
			i = j
		default:
			// 运算符（含多字符 >=, <=, !=, ==, &&, ||）一律收集为 op，原样保留。
			j := i
			for j < len(runes) && isOpRune(runes[j]) {
				j++
			}
			if j == i {
				return nil, fmt.Errorf("unexpected character %q in CEP expression", c)
			}
			toks = append(toks, etoken{ekOp, string(runes[i:j])})
			i = j
		}
	}
	return toks, nil
}

func isOpRune(c rune) bool {
	switch c {
	case '>', '<', '=', '!', '&', '|', '+', '-', '*', '/':
		return true
	}
	return false
}

// navFuncs 是 CEP 导航/聚合函数名（rewriter 拦截它们，其余函数调用原样透传给 expr 引擎）。
var navFuncs = map[string]bool{
	"PREV": true, "NEXT": true, "FIRST": true, "LAST": true,
	"CLASSIFIER": true, "MATCH_NUMBER": true,
	"SUM": true, "AVG": true, "COUNT": true, "MIN": true, "MAX": true,
}

// phKind 标识占位符的求值方式（预编译产物）。
type phKind int

const (
	phNav phKind = iota // 导航/聚合：name+args
	phSym               // 符号限定字段：name(symbol)+field
)

// phDesc 描述一个占位符如何从 matchCtx 求值。占位符键 = placeholderPrefix+i+"__"。
type phDesc struct {
	kind  phKind
	name  string   // phNav: 函数名；phSym: 符号名
	args  []string // phNav: 参数片段
	field string   // phSym: 字段名（已去引号/反引号）
}

// preparedExpr 是预编译的 DEFINE/MEASURES 表达式：改写后的字符串经 expr.NewExpression
// 编译一次缓存；求值时只重算各占位符值与裸字段，复用编译产物（避免热路径重复解析）。
type preparedExpr struct {
	src        string
	compiled   *expr.Expression
	phs        []phDesc  // 占位符描述（phs[i] → key placeholderPrefix+i+"__"）
	bareFields []string  // 透传裸字段名（求值时灌入当前行字段）
	needsHist  bool      // 含导航/聚合/符号限定 → 求值需历史行
}

// placeholderKey 返回第 i 个占位符的 base 键。
func placeholderKey(i int) string {
	return placeholderPrefix + strconv.Itoa(i) + "__"
}

// prepare 把表达式编译为 preparedExpr（tokenize → 改写计划 → NewExpression）。
// 纯结构性，不求值；失败（词法/改写/编译）返回 error，供 Validate fail-fast。
func prepare(src string, symbols map[string]bool) (*preparedExpr, error) {
	src = strings.TrimSpace(src)
	toks, err := tokenize(src)
	if err != nil {
		return nil, err
	}
	p := &preparedExpr{src: src}
	var pieces []string
	push := func(d phDesc) string {
		k := placeholderKey(len(p.phs))
		p.phs = append(p.phs, d)
		return k
	}
	i := 0
	for i < len(toks) {
		t := toks[i]
		if t.kind == ekIdent {
			up := strings.ToUpper(t.val)
			// 函数调用：PREV(...) / SUM(...) 等。
			if navFuncs[up] && i+1 < len(toks) && toks[i+1].kind == ekLParen {
				args, consumed, err := readCallArgs(toks, i+1)
				if err != nil {
					return nil, err
				}
				pieces = append(pieces, push(phDesc{kind: phNav, name: up, args: args}))
				p.needsHist = true
				i += 1 + consumed
				continue
			}
			// SYMBOL.field（符号限定）：SYMBOL 是已声明模式变量且后跟 .field。
			// field 可为标识符或反引号/引号包裹的保留字列名。
			if symbols[t.val] && i+2 < len(toks) && toks[i+1].kind == ekDot &&
				(toks[i+2].kind == ekIdent || toks[i+2].kind == ekString) {
				fld := fieldName(toks[i+2].val)
				pieces = append(pieces, push(phDesc{kind: phSym, name: t.val, field: fld}))
				p.needsHist = true
				i += 3
				continue
			}
		}
		// 其余原样透传（裸字段、运算符、标量函数、字面量）。
		pieces = append(pieces, t.val)
		if t.kind == ekIdent {
			p.bareFields = append(p.bareFields, t.val)
		}
		i++
	}
	joined := joinTokens(pieces)
	if joined == "" {
		return &preparedExpr{src: src, compiled: nil}, nil // 空表达式（未定义符号）
	}
	compiled, err := expr.NewExpression(joined)
	if err != nil {
		return nil, err
	}
	p.compiled = compiled
	return p, nil
}

// evalPrepared 用 ctx 求值预编译表达式：重算占位符值 + 灌入裸字段，复用编译产物。
func evalPrepared(p *preparedExpr, ctx *matchCtx) (any, bool, error) {
	if p == nil || p.compiled == nil {
		return nil, true, nil // 空表达式 → NULL（调用方按未定义符号恒真处理）
	}
	base := make(map[string]any, len(p.phs)+len(p.bareFields))
	for i, d := range p.phs {
		base[placeholderKey(i)] = evalDesc(d, ctx)
	}
	if row := currentRow(ctx); row != nil {
		for _, f := range p.bareFields {
			if _, used := base[f]; !used {
				if v, ok := row[f]; ok {
					base[f] = v
				}
			}
		}
	}
	return p.compiled.EvaluateValueWithNull(base)
}

// evalDesc 求值一个占位符描述。
func evalDesc(d phDesc, ctx *matchCtx) any {
	switch d.kind {
	case phNav:
		v, _ := evalNav(d.name, d.args, ctx)
		return v
	case phSym:
		return resolveSymbolField(ctx, d.name, d.field)
	}
	return nil
}

func currentRow(ctx *matchCtx) map[string]any {
	if ctx.candidate != nil {
		return ctx.candidate
	}
	if ctx.cur >= 0 && ctx.cur < len(ctx.rows) {
		return ctx.rows[ctx.cur]
	}
	return nil
}

// readCallArgs 从 toks[at]（应为 ekLParen）读取到配平的 ekRParen，返回括号内按顶层逗号
// 切分的参数片段（每段是一组 token，原样拼接为字符串）与消耗的 token 数（含括号）。
func readCallArgs(toks []etoken, at int) ([]string, int, error) {
	if at >= len(toks) || toks[at].kind != ekLParen {
		return nil, 0, fmt.Errorf("expected '(' after CEP function")
	}
	depth := 0
	var segs []string
	var cur []string
	for j := at; j < len(toks); j++ {
		t := toks[j]
		switch t.kind {
		case ekLParen:
			depth++
		case ekRParen:
			depth--
			if depth == 0 {
				if len(cur) > 0 || len(segs) > 0 {
					segs = append(segs, joinTokens(cur))
				}
				return segs, j - at + 1, nil
			}
		case ekComma:
			if depth == 1 {
				segs = append(segs, joinTokens(cur))
				cur = nil
				continue
			}
		}
		if t.kind == ekLParen && j == at {
			continue
		}
		cur = append(cur, t.val)
	}
	return nil, 0, fmt.Errorf("unterminated '(' in CEP function call")
}

func joinTokens(parts []string) string {
	var sb strings.Builder
	for i, p := range parts {
		if i > 0 {
			// 标识符/数字与标识符/数字之间需要空格，避免粘成新 token；运算符与点号不加。
			if needSpace(parts[i-1], p) {
				sb.WriteByte(' ')
			}
		}
		sb.WriteString(p)
	}
	return sb.String()
}

func needSpace(a, b string) bool {
	return isAlphaNumLike(a) && isAlphaNumLike(b)
}

func isAlphaNumLike(s string) bool {
	if s == "" {
		return false
	}
	c := s[0]
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// evalNav 求值一个导航/聚合调用。args 为括号内顶层参数片段（已去括号、去外层逗号）。
func evalNav(name string, args []string, ctx *matchCtx) (any, error) {
	switch name {
	case "CLASSIFIER":
		if ctx.candidate != nil {
			return ctx.candLabel, nil
		}
		if ctx.cur >= 0 && ctx.cur < len(ctx.labels) {
			return ctx.labels[ctx.cur], nil
		}
		return nil, nil
	case "MATCH_NUMBER":
		return ctx.matchNumber, nil
	case "PREV":
		return positionalField(ctx, args, -1), nil
	case "NEXT":
		return positionalField(ctx, args, +1), nil
	case "FIRST":
		return fromEndField(ctx, args, true), nil
	case "LAST":
		return fromEndField(ctx, args, false), nil
	case "SUM", "AVG", "COUNT", "MIN", "MAX":
		return aggregate(name, args, ctx), nil
	}
	return nil, fmt.Errorf("unsupported CEP function %s", name)
}

// posIndex 返回当前行下标：DEFINE=len(rows)（候选位置），MEASURES=cur。
func posIndex(ctx *matchCtx) int {
	if ctx.candidate != nil {
		return len(ctx.rows)
	}
	return ctx.cur
}

// stripQuotes 去除首尾的反引号/单引号/双引号。
func stripQuotes(a string) string {
	a = strings.TrimSpace(a)
	a = strings.Trim(a, "`")
	if len(a) >= 2 && (a[0] == '\'' && a[len(a)-1] == '\'' || a[0] == '"' && a[len(a)-1] == '"') {
		return a[1 : len(a)-1]
	}
	return a
}

// fieldName 从参数片段提取字段名（去 SYMBOL. 前缀、去引号/反引号）。COUNT(*) 的 '*' 原样返回。
func fieldName(arg string) string {
	a := stripQuotes(arg)
	if dot := strings.LastIndex(a, "."); dot >= 0 {
		a = a[dot+1:]
	}
	return a
}

// fieldAndSymbol 拆分聚合参数为（字段, 符号）。
// "A.v" → ("v","A")；"v" → ("v","")；"*" → ("*","")；反引号/引号包裹均去除。
func fieldAndSymbol(arg string) (field, symbol string) {
	a := strings.TrimSpace(arg)
	if dot := strings.IndexByte(a, '.'); dot >= 0 {
		return stripQuotes(a[dot+1:]), stripQuotes(a[:dot])
	}
	return stripQuotes(a), ""
}

func optInt(args []string, idx, def int) int {
	if idx >= len(args) {
		return def
	}
	a := strings.TrimSpace(args[idx])
	if n, err := strconv.Atoi(a); err == nil {
		return n
	}
	return def
}

// matchRowsLabels 返回求值行集（与标签对齐）：DEFINE 含候选；MEASURES 截到当前行（RUNNING）。
// aggregate 与 fromEndField 共用，保证 RUNNING 语义一致。
func matchRowsLabels(ctx *matchCtx) ([]map[string]any, []string) {
	rows := ctx.rows
	labels := ctx.labels
	if ctx.candidate != nil {
		cr := make([]map[string]any, 0, len(rows)+1)
		cl := make([]string, 0, len(rows)+1)
		cr = append(cr, rows...)
		cl = append(cl, labels...)
		cr = append(cr, ctx.candidate)
		cl = append(cl, ctx.candLabel)
		return cr, cl
	}
	if ctx.cur >= 0 && ctx.cur < len(rows) {
		return rows[:ctx.cur+1], labels[:ctx.cur+1]
	}
	return rows, labels
}

// positionalField 求 PREV/NEXT：从当前位置偏移取行。越界返回 nil。
func positionalField(ctx *matchCtx, args []string, sign int) any {
	if len(args) == 0 {
		return nil
	}
	f := fieldName(args[0])
	idx := posIndex(ctx) + sign*optInt(args, 1, 1)
	rows := ctx.rows
	if idx < 0 || idx >= len(rows) {
		return nil
	}
	return rows[idx][f]
}

// fromEndField 求 FIRST/LAST：从头/尾取第 n 个（n 从 1 起）。n<1 钳为 1，避免越界。
func fromEndField(ctx *matchCtx, args []string, fromHead bool) any {
	if len(args) == 0 {
		return nil
	}
	f := fieldName(args[0])
	n := optInt(args, 1, 1)
	if n < 1 {
		n = 1
	}
	rows, _ := matchRowsLabels(ctx)
	if len(rows) == 0 {
		return nil
	}
	if fromHead {
		idx := n - 1
		if idx >= len(rows) {
			idx = len(rows) - 1
		}
		return rows[idx][f]
	}
	idx := len(rows) - n
	if idx < 0 {
		idx = 0
	}
	return rows[idx][f]
}

// aggregate 求匹配范围内的聚合（RUNNING 语义）。
// 符号限定（SUM(A.x)）仅对该符号标签行聚合；COUNT(expr) 计非 NULL 值，COUNT(*) 计行数。
// 数值转换用 cast.ToFloat64E（支持 uint/字符串数字等），非数值按 NULL 跳过。
func aggregate(name string, args []string, ctx *matchCtx) any {
	rows, labels := matchRowsLabels(ctx)
	f, symbol := "", ""
	if len(args) > 0 {
		f, symbol = fieldAndSymbol(args[0])
	}
	star := f == "" || f == "*"
	// matchLabel 报告某行标签是否属于目标符号（普通符号等值；SUBSET 名匹配任一成员）。
	matchLabel := func(lbl string) bool {
		if symbol == "" || lbl == symbol {
			return true
		}
		for _, m := range ctx.subsets[symbol] {
			if lbl == m {
				return true
			}
		}
		return false
	}
	var vals []float64
	cntNonNull := 0
	cntRows := 0
	for i, r := range rows {
		if !matchLabel(labels[i]) {
			continue // 符号/SUBSET 限定：只计成分标签行
		}
		cntRows++
		if star {
			continue
		}
		rv, has := r[f]
		if !has || rv == nil {
			continue // NULL 不计
		}
		cntNonNull++
		if x, err := cast.ToFloat64E(rv); err == nil {
			vals = append(vals, x)
		}
	}
	switch name {
	case "COUNT":
		if star {
			return float64(cntRows)
		}
		return float64(cntNonNull)
	case "SUM":
		var s float64
		for _, v := range vals {
			s += v
		}
		return s
	case "AVG":
		if len(vals) == 0 {
			return nil
		}
		var s float64
		for _, v := range vals {
			s += v
		}
		return s / float64(len(vals))
	case "MIN":
		if len(vals) == 0 {
			return nil
		}
		m := vals[0]
		for _, v := range vals[1:] {
			if v < m {
				m = v
			}
		}
		return m
	case "MAX":
		if len(vals) == 0 {
			return nil
		}
		m := vals[0]
		for _, v := range vals[1:] {
			if v > m {
				m = v
			}
		}
		return m
	}
	return nil
}

// resolveSymbolField 取符号 SYMBOL（或 SUBSET 的任一成分）在匹配中最末出现行的字段值。
// DEFINE 时若候选标签属于该符号/成分，取候选行该字段。
func resolveSymbolField(ctx *matchCtx, symbol, field string) any {
	matchLabel := func(lbl string) bool {
		if lbl == symbol {
			return true
		}
		for _, m := range ctx.subsets[symbol] {
			if lbl == m {
				return true
			}
		}
		return false
	}
	if ctx.candidate != nil && matchLabel(ctx.candLabel) {
		return ctx.candidate[field]
	}
	for i := len(ctx.labels) - 1; i >= 0; i-- {
		if matchLabel(ctx.labels[i]) {
			return ctx.rows[i][field]
		}
	}
	return nil
}

// truthy SQL 布尔语义：数值非零、布尔真、非空字符串为真。
func truthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case float64:
		return x != 0
	case int:
		return x != 0
	case int64:
		return x != 0
	case string:
		return x != ""
	}
	return v != nil
}

// EvalDefine 即时编译并求值符号的 DEFINE 条件（布尔）。仅供测试/外部使用；
// 引擎热路径用 Engine.evalDefine（预编译产物）。buffer=已匹配行，candidate=待分类行。
func EvalDefine(cond string, buffer []map[string]any, labels []string, candidate map[string]any, candLabel string, symbols map[string]bool) bool {
	cond = strings.TrimSpace(cond)
	if cond == "" {
		return true // 未定义的符号恒为真（SQL 标准）
	}
	p, err := prepare(cond, symbols)
	if err != nil {
		return false
	}
	ctx := &matchCtx{rows: buffer, labels: labels, cur: len(buffer), candidate: candidate, candLabel: candLabel, symbols: symbols}
	v, isNull, err := evalPrepared(p, ctx)
	if err != nil || isNull || v == nil {
		return false
	}
	return truthy(v)
}

// EvalMeasure 即时编译并求值 MEASURES 表达式（值）。仅供测试/外部使用。
func EvalMeasure(expression string, rows []map[string]any, labels []string, cur, matchNumber int, symbols map[string]bool) (any, bool) {
	p, err := prepare(expression, symbols)
	if err != nil {
		return nil, true
	}
	ctx := &matchCtx{rows: rows, labels: labels, cur: cur, symbols: symbols, matchNumber: matchNumber}
	v, isNull, err := evalPrepared(p, ctx)
	if err != nil {
		return nil, true
	}
	return v, isNull
}
