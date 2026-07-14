package rsql

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/rulego/streamsql/functions"
	"github.com/rulego/streamsql/types"
	"github.com/rulego/streamsql/utils/cast"
	"github.com/rulego/streamsql/window"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/expr"
	"github.com/rulego/streamsql/logger"
)

type SelectStatement struct {
	Fields      []Field
	Distinct    bool
	SelectAll   bool // Flag to indicate if this is a SELECT * query
	Source      string
	SourceAlias string // optional FROM alias (e.g. "s" in "FROM stream AS s")
	Condition   string
	Window      WindowDefinition
	GroupBy     []string
	Limit       int
	Having      string
	OrderBy     []types.OrderByField
	JoinConfigs []types.JoinConfig
	// MatchRecognize 携带 MATCH_RECOGNIZE 子句（FROM 后、WHERE 前）。非空时走 CEP 路径。
	MatchRecognize *types.MatchRecognizeSpec
}

type Field struct {
	Expression string
	Alias      string
	AggType    string
	OverSpec   *types.OverSpec // 分析函数 OVER 子句，nil 表示无
}

type WindowDefinition struct {
	Type              string
	Params            []any
	TsProp            string
	TimeUnit          time.Duration
	MaxOutOfOrderness time.Duration // Maximum allowed out-of-orderness for event time
	AllowedLateness   time.Duration // Maximum allowed lateness for event time windows
	IdleTimeout       time.Duration // Idle source timeout: when no data arrives within this duration, watermark advances based on processing time
	CountStateTTL     time.Duration // Counting-window keyed state TTL; inactive keys reaped after this (0 = disabled)
	TriggerCondition  string        // Global-window TRIGGER WHEN predicate (raw string)
	Over              *types.OverSpec // GROUP BY window OVER(...) 子句（仅 WHEN 输入门控）
}

// ToStreamConfig converts AST to Stream configuration
func (s *SelectStatement) ToStreamConfig() (*types.Config, string, error) {
	if s.Source == "" {
		return nil, "", fmt.Errorf("missing FROM clause")
	}

	// Parse window configuration
	windowType := window.TypeTumbling
	switch strings.ToUpper(s.Window.Type) {
	case "TUMBLINGWINDOW":
		windowType = window.TypeTumbling
	case "SLIDINGWINDOW":
		windowType = window.TypeSliding
	case "COUNTINGWINDOW":
		windowType = window.TypeCounting
	case "SESSIONWINDOW":
		windowType = window.TypeSession
	case "GLOBALWINDOW":
		windowType = window.TypeGlobal
		// Global window with no TRIGGER WHEN would never emit.
		// Reject at parse time rather than silently swallowing data.
		if strings.TrimSpace(s.Window.TriggerCondition) == "" {
			return nil, "", fmt.Errorf("GLOBAL WINDOW requires a TRIGGER WHEN clause (without it the window never emits)")
		}
	}

	// Parse window parameters - now returns array directly
	params := s.Window.Params

	// Validate and convert parameters based on window type
	if len(params) > 0 {
		var err error
		params, err = validateWindowParams(params, windowType)
		if err != nil {
			return nil, "", fmt.Errorf("failed to validate window parameters: %w", err)
		}
	}

	// Check if window processing is needed
	needWindow := s.Window.Type != ""
	var simpleFields []string

	// 分离分析函数字段：分析函数走直连路径状态机，不进聚合路径。
	// 剩余字段（真聚合 + 普通字段）保持原解析逻辑。
	analyticFields := make([]types.AnalyticField, 0, len(s.Fields))
	otherFields := make([]Field, 0, len(s.Fields))
	for _, f := range s.Fields {
		if isAnalyticField(f) {
			// 校验分析函数自身的嵌套：分析套分析、聚合套分析均不允许
			// （分析套聚合在窗口查询里允许，由 extractInlineAggregates 处理）。
			if err := detectNestedAggregation(f.Expression); err != nil {
				return nil, "", err
			}
			analyticFields = append(analyticFields, buildAnalyticField(f))
			continue
		}
		// 表达式包分析函数：算术（ts-lag(ts)）、标量套（coalesce(lag(temp))、UPPER(lag)）、
		// CASE(lag) 等。顶层非裸分析调用，isAnalyticField 为假；含分析调用即路由进分析路径，
		// splitAnalyticExprMulti 抽出分析调用、外层表达式作 WrapperExpr 回代。聚合套分析
		// （如 count(lag)）由 detectNestedAggregation 拦截。
		if containsAnalyticCall(f.Expression) {
			if err := detectNestedAggregation(f.Expression); err != nil {
				return nil, "", err
			}
			analyticFields = append(analyticFields, buildAnalyticField(f))
			continue
		}
		otherFields = append(otherFields, f)
	}

	// Check if there are aggregation functions
	hasAggregation := false
	for _, field := range otherFields {
		if isAggregationFunction(field.Expression) {
			hasAggregation = true
			break
		}
	}

	// If no window is specified but has aggregation functions, use tumbling window by default
	if !needWindow && hasAggregation {
		needWindow = true
		windowType = window.TypeTumbling
		params = []any{10 * time.Second} // Default 10-second window
	}

	// 窗口查询里允许分析函数：分析函数在窗口产出行上求值，状态跨窗口保留
	// （见 stream.processAggregationResults）。分析函数参数里的内联聚合
	// （如 changed_cols("t", true, avg(temperature))）在下方提取为隐藏计算字段。

	// If no aggregation functions, collect simple fields
	if !hasAggregation {
		// If SELECT * query, set special marker
		if s.SelectAll {
			simpleFields = append(simpleFields, "*")
		} else {
			for _, field := range otherFields {
				fieldName := field.Expression
				if field.Alias != "" {
					// If has alias, use alias as field name
					simpleFields = append(simpleFields, fieldName+":"+field.Alias)
				} else {
					// For fields without alias, check if it's a string literal
					_, n, _, _, err := ParseAggregateTypeWithExpression(fieldName)
					if err != nil {
						return nil, "", err
					}
					if n != "" {
						// If string literal, use parsed field name (remove quotes)
						simpleFields = append(simpleFields, n)
					} else {
						// Otherwise use original expression
						simpleFields = append(simpleFields, fieldName)
					}
				}
			}
		}
		logger.Debug("Collected simple fields: %v", simpleFields)
	}

	// Build field mapping and expression information
	aggs, fields, expressions, postAggExpressions, err := buildSelectFieldsWithExpressions(otherFields)
	if err != nil {
		return nil, "", err
	}

	// 窗口查询里的分析函数：把参数中的内联聚合（如 changed_cols 内的 avg(...)））
	// 提取为隐藏计算字段，重写参数为隐藏键引用，供窗口聚合计算后供分析函数消费。
	if needWindow && len(analyticFields) > 0 {
		extractInlineAggregates(analyticFields, aggs, fields)
		// 分析函数默认按 GROUP BY 键分区：跨窗口为每个分组各自保留状态，
		// 避免不同分组的窗口输出共享状态而串扰。
		gk := extractGroupFields(s)
		if len(gk) > 0 {
			for i := range analyticFields {
				af := &analyticFields[i]
				if af.Over == nil {
					af.Over = &types.OverSpec{}
				}
				if len(af.Over.PartitionBy) == 0 {
					af.Over.PartitionBy = append([]string(nil), gk...)
				}
			}
		}
		// 校验：窗口查询里分析函数的参数必须引用窗口输出字段（聚合或 GROUP BY 键），
		// 不能引用裸原始列——否则求值时取不到值，会静默得到列名字符串而非结果。
		if err := validateWindowAnalyticArgs(analyticFields, gk); err != nil {
			return nil, "", err
		}
	}

	// Extract field order information
	fieldOrder, err := extractFieldOrder(s.Fields)
	if err != nil {
		return nil, "", err
	}

	// D3: an analytic-field alias must not silently override another output
	// column (e.g. "SELECT temperature, lag(temperature) AS temperature").
	// fieldOrder already resolves every SELECT item to its final output name, so
	// an analytic alias appearing more than once collides with another field —
	// reject at parse time instead of letting map writes silently overwrite.
	if len(analyticFields) > 0 {
		nameCount := make(map[string]int, len(fieldOrder))
		for _, n := range fieldOrder {
			nameCount[n]++
		}
		for _, af := range analyticFields {
			if nameCount[af.Alias] > 1 {
				return nil, "", fmt.Errorf("duplicate output column %q: analytic alias collides with another field; use a distinct AS alias", af.Alias)
			}
		}
	}

	// Determine time characteristic based on whether TIMESTAMP is specified in WITH clause
	// If TsProp is set, use EventTime; otherwise use ProcessingTime (default)
	timeCharacteristic := types.ProcessingTime
	if s.Window.TsProp != "" {
		timeCharacteristic = types.EventTime
	}

	// GROUP BY 窗口不支持 OVER(...)：窗口 OVER 的输入门控语义会隐藏 dip、破坏检测，
	// 阈值/持续检测用 HAVING（如 HAVING min(concurrency) > 200）。
	if s.Window.Over != nil {
		return nil, "", fmt.Errorf("OVER(...) on a GROUP BY window is not supported; for threshold/sustained detection use HAVING (e.g. HAVING min(concurrency) > 200)")
	}

	// HAVING 可引用未选出的聚合（标准 SQL）。把 HAVING 文本里的聚合调用
	// 映射到已选 alias，或注册为隐藏聚合 __having_N__ 让 aggregator 补算；aggs/fields 原地扩充。
	selectAlias := buildSelectAliasMap(s.Fields)
	havingRewritten := extractHavingAggregates(s.Having, aggs, fields, selectAlias)

	// 执行路径模式：MATCH_RECOGNIZE→CEP；窗口/聚合→Window；否则 Direct。
	// 拦截 MATCH_RECOGNIZE 与 GROUP/聚合、JOIN 的组合（后续阶段支持）。
	mode := types.ExecDirect
	if needWindow {
		mode = types.ExecWindow
	}
	if s.MatchRecognize != nil {
		if needWindow {
			return nil, "", fmt.Errorf("MATCH_RECOGNIZE cannot be combined with GROUP BY/aggregation yet")
		}
		if len(s.JoinConfigs) > 0 {
			return nil, "", fmt.Errorf("MATCH_RECOGNIZE with JOIN is not supported yet")
		}
		if s.MatchRecognize.Pattern == nil {
			return nil, "", fmt.Errorf("MATCH_RECOGNIZE requires a PATTERN clause")
		}
		if len(s.MatchRecognize.OrderBy) == 0 {
			return nil, "", fmt.Errorf("MATCH_RECOGNIZE requires ORDER BY (provides event ordering)")
		}
		// ORDER BY 在 CEP 提供事件时序字段；DESC 流式下无意义（按到达序），拒绝以免静默忽略。
		for _, ob := range s.MatchRecognize.OrderBy {
			if ob.Direction == types.SortDesc {
				return nil, "", fmt.Errorf("MATCH_RECOGNIZE ORDER BY 暂不支持 DESC（流式按到达序处理）")
			}
		}
		mode = types.ExecCEP
	}

	// Build Stream configuration
	config := types.Config{
		WindowConfig: types.WindowConfig{
			Type:               windowType,
			Params:             params,
			TsProp:             s.Window.TsProp,
			TimeUnit:           s.Window.TimeUnit,
			TimeCharacteristic: timeCharacteristic,
			MaxOutOfOrderness:  s.Window.MaxOutOfOrderness,
			AllowedLateness:    s.Window.AllowedLateness,
			IdleTimeout:        s.Window.IdleTimeout,
			CountStateTTL:      s.Window.CountStateTTL,
			GroupByKeys:        extractGroupFields(s),
			// Global-window fields (no-op for other window types).
			TriggerCondition: s.Window.TriggerCondition,
			SelectFields:     aggs,
			FieldAlias:       fields,
		},
		GroupFields:        extractGroupFields(s),
		SelectFields:       aggs,
		FieldAlias:         fields,
		SelectAlias:        selectAlias,
		Distinct:           s.Distinct,
		Limit:              s.Limit,
		NeedWindow:         needWindow,
		Mode:               mode,
		MatchRecognize:     s.MatchRecognize,
		AnalyticFields:     analyticFields,
		SimpleFields:       simpleFields,
		Having:             havingRewritten,
		FieldExpressions:   expressions,
		PostAggExpressions: postAggExpressions,
		FieldOrder:         fieldOrder,
		OrderBy:            s.OrderBy,
		JoinConfigs:        s.JoinConfigs,
		SourceAlias:        s.SourceAlias,
	}

	// 提取 WHERE 中的分析函数调用（含 OVER），替换为占位符，供直连路径状态机求值。
	rewrittenCondition, whereCalls, err := extractWhereAnalyticCalls(s.Condition)
	if err != nil {
		return nil, "", err
	}
	config.WhereAnalyticCalls = whereCalls

	return &config, rewrittenCondition, nil
}

// isAnalyticField 判断 Field 是否为分析函数（TypeAnalytical）。
func isAnalyticField(f Field) bool {
	funcName := extractFunctionName(f.Expression)
	if funcName == "" {
		return false
	}
	if fn, exists := functions.Get(strings.ToLower(funcName)); exists {
		return fn.GetType() == functions.TypeAnalytical
	}
	return false
}

// containsAnalyticCall 判断表达式里是否含分析函数（TypeAnalytical）调用。
// 用于拦截把分析函数嵌进标量函数的写法（如 UPPER(changed_col(...))）：
// 分析函数需跨行状态，走无状态标量路径会静默求错，必须在解析期拒绝。
// 先去掉字符串字面量，避免把字面量里形如 "lag(" 的文本误判为函数调用。
func containsAnalyticCall(expr string) bool {
	for _, name := range extractAllFunctions(stripStringLiterals(expr)) {
		if fn, exists := functions.Get(name); exists && fn.GetType() == functions.TypeAnalytical {
			return true
		}
	}
	return false
}

// stripStringLiterals 去掉字符串字面量内容，仅保留字面量外的表达式文本。
// 本方言单引号 '...' 与双引号 "..." 都是字符串字面量（如 changed_cols("t",...)），
// 二者都要剥离，否则 "lag(x)" 这类双引号字面量里的分析函数名会被误判为调用。
// 处理 SQL 转义的两个连续引号（'' 或 ""）。
func stripStringLiterals(expr string) string {
	var b strings.Builder
	b.Grow(len(expr))
	var quote byte // 0=不在字面量内；'\''|'"'=当前字面量定界符
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		if c == '\'' || c == '"' {
			if quote == c && i+1 < len(expr) && expr[i+1] == c {
				i++ // 转义引号，跳过，仍处于字面量内
				continue
			}
			if quote == 0 {
				quote = c // 进入字面量
			} else if quote == c {
				quote = 0 // 字面量结束
			}
			// 异类引号（如 " 内的 '）当作字面量内容跳过，不改状态
			continue
		}
		if quote == 0 {
			b.WriteByte(c)
		}
	}
	return b.String()
}

// buildAnalyticField 将分析函数 Field 转为 AnalyticField，保留 OVER 子句。
// 支持"表达式包分析函数"（如 ts - lag(ts)）：拆出裸分析调用供状态机计算，外层表达式
// 存为 WrapperExpr（分析调用替换为 types.AnalyticSelfToken）供求值期回代。
// 同一表达式含多个分析调用（如 acc_max(v) - acc_min(v)）时抽出全部，各分配独立占位。
func buildAnalyticField(f Field) types.AnalyticField {
	calls, wrapper := splitAnalyticExprMulti(f.Expression)
	alias := f.Alias
	if alias == "" {
		alias = f.Expression
	}
	af := types.AnalyticField{
		Alias:       alias,
		Over:        f.OverSpec,
		WrapperExpr: wrapper,
		Calls:       calls,
	}
	if len(calls) > 0 {
		af.FuncName = strings.ToLower(calls[0].FuncName)
		af.Expression = calls[0].BareCall
		af.Args = calls[0].Args
		// changed_cols 输出多列（动态列名 prefix+colname），仅 SELECT。
		if af.FuncName == "changed_cols" {
			af.MultiColumn = true
		}
	}
	return af
}

// splitAnalyticExprMulti 抽出表达式里的全部 Analytic 调用（按出现顺序），各调用子串替换为
// types.AnalyticSelfTokenN(i) 占位构成 wrapper。纯单调用且覆盖整式时 wrapper=""（纯分析字段语义）；
// 否则 wrapper 含占位。不含分析调用时返回 (nil, "")。
func splitAnalyticExprMulti(expr string) (calls []types.AnalyticCall, wrapper string) {
	pattern := regexp.MustCompile(`(?i)\b([a-z_]+)\s*\(`)
	type span struct{ start, closeParen int }
	var spans []span
	for _, m := range pattern.FindAllStringSubmatchIndex(expr, -1) {
		nm := strings.ToLower(expr[m[2]:m[3]])
		fn, exists := functions.Get(nm)
		if !exists || fn.GetType() != functions.TypeAnalytical {
			continue
		}
		openParen := m[1] - 1
		cp := findMatchingParenInternal(expr, openParen)
		if cp < 0 {
			continue
		}
		// 跳过嵌套在已记录调用内的匹配（分析套分析不允许，稳妥起见不计入）。
		nested := false
		for _, s := range spans {
			if m[0] > s.start && cp < s.closeParen {
				nested = true
				break
			}
		}
		if nested {
			continue
		}
		spans = append(spans, span{m[0], cp})
		calls = append(calls, types.AnalyticCall{
			FuncName: nm,
			BareCall: expr[m[0] : cp+1],
			Args:     splitCallArgs(expr[m[0] : cp+1]),
		})
	}
	if len(calls) == 0 {
		return nil, ""
	}
	// 纯单调用且覆盖整式：wrapper 留空（保持纯分析字段语义）。
	if len(calls) == 1 {
		leading := len(expr) - len(strings.TrimLeft(expr, " \t"))
		trailing := len(strings.TrimRight(expr, " \t"))
		s := spans[0]
		if s.start == leading && s.closeParen == trailing-1 && strings.TrimSpace(expr) == strings.TrimSpace(calls[0].BareCall) {
			return calls, ""
		}
	}
	// 从右向左替换每个调用 span 为占位，避免索引位移。
	wrapper = expr
	for i := len(spans) - 1; i >= 0; i-- {
		s := spans[i]
		wrapper = wrapper[:s.start] + types.AnalyticSelfTokenN(i) + wrapper[s.closeParen+1:]
	}
	return calls, wrapper
}

// extractInlineAggregates 把分析函数参数里的内联聚合提取为隐藏计算字段。
// 如 changed_cols("t", true, avg(temperature)) → 注册 aggs[__winagg_0__]=avg(temperature)，
// 参数重写为 __winagg_0__，InlineAggDisplay 记录 {__winagg_0__:"avg"}。
// 复合表达式参数（如 avg(temp) + 1）只替换其中的聚合调用 span，外层运算符保留，
// 参数变为 __winagg_0__ + 1，运行期由表达式求值器计算。
// 隐藏键前缀 __winagg_ 在窗口产出后被剥离，不进最终输出；显示名用于 changed_cols 输出列名。
func extractInlineAggregates(analyticFields []types.AnalyticField, aggs map[string]aggregator.AggregateType, fieldMap map[string]string) {
	pattern := regexp.MustCompile(`(?i)\b([a-z_]+)\s*\(`)
	seq := 0
	for i := range analyticFields {
		af := &analyticFields[i]
		// 遍历字段内每个分析调用的参数，抽内联聚合并就地重写该调用（Args+BareCall）。
		// 无 Calls（不应发生在 SELECT 字段，稳妥兜底）退化为单调用。
		if len(af.Calls) == 0 {
			af.Calls = []types.AnalyticCall{{FuncName: af.FuncName, BareCall: af.Expression, Args: af.Args}}
		}
		for ci := range af.Calls {
			args := af.Calls[ci].Args
			for j, arg := range args {
				trimmed := strings.TrimSpace(arg)
				idx := pattern.FindStringSubmatchIndex(trimmed)
				if idx == nil {
					continue
				}
				funcName := strings.ToLower(trimmed[idx[2]:idx[3]])
				fn, ok := functions.Get(funcName)
				if !ok || fn.GetType() != functions.TypeAggregation {
					continue
				}
				// 聚合调用 span：从函数名起点到匹配的右括号（idx[1]-1 是 '(' 的位置）。
				openParen := idx[1] - 1
				closeParen := findMatchingParenInternal(trimmed, openParen)
				if closeParen < 0 {
					continue
				}
				aggCall := trimmed[idx[0] : closeParen+1]
				// 只解析聚合调用本身（不含外层运算符），避免整参被误判为 "expression" 型聚合。
				aggType, name, _, _, perr := ParseAggregateTypeWithExpression(aggCall)
				if perr != nil || aggType == "" {
					continue
				}
				hidden := fmt.Sprintf("__winagg_%d__", seq)
				seq++
				aggs[hidden] = aggType
				// 输入字段：name 为聚合的输入字段（如 avg(temperature) 的 temperature）；
				// 无显式字段时（如 count(*)）用隐藏键本身，聚合器按需处理。
				if name != "" {
					fieldMap[hidden] = name
				} else {
					fieldMap[hidden] = hidden
				}
				if af.InlineAggDisplay == nil {
					af.InlineAggDisplay = make(map[string]string)
				}
				af.InlineAggDisplay[hidden] = funcName
				// 仅替换聚合调用子串，保留外层运算符（复合表达式）。
				args[j] = trimmed[:idx[0]] + hidden + trimmed[closeParen+1:]
				af.Calls[ci].BareCall = strings.Replace(af.Calls[ci].BareCall, aggCall, hidden, 1)
			}
		}
		// 旧路径（changed_cols 多列扇出等）仍读 Expression/Args，与首个调用保持同步。
		af.Expression = af.Calls[0].BareCall
		af.Args = af.Calls[0].Args
	}
}

// collapseSpacesOutsideQuotes 去掉引号外的空白，引号内（字符串字面量）保留原样。
// 用于归一化 HAVING 里带空格的聚合调用文本（parser 存为 "max ( v )"），便于复用解析函数。
func collapseSpacesOutsideQuotes(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inQuote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' || c == '"' {
			if inQuote == c {
				inQuote = 0
			} else if inQuote == 0 {
				inQuote = c
			}
			b.WriteByte(c)
			continue
		}
		if c == ' ' && inQuote == 0 {
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// extractHavingAggregates 处理 HAVING 引用的聚合（标准 SQL：HAVING 可引用任意聚合，不必在 SELECT）。
// 对 HAVING 文本里每个聚合调用 ac：
//   - selectAlias[ac] 命中（SELECT 里 ac AS alias）→ 改写 HAVING 里 ac 为 alias（聚合已在算）。
//   - aggs[ac] 命中（无别名选出，键恰为调用文本）→ 不动。
//   - 否则（未选出）→ 注册隐藏聚合 __having_N__（aggs/fieldMap 原地扩充），ac 改写为 __having_N__。
// 返回改写后的 HAVING 文本。aggs/fieldMap 为 map 引用，原地修改。
func extractHavingAggregates(having string, aggs map[string]aggregator.AggregateType, fieldMap map[string]string, selectAlias map[string]string) string {
	if strings.TrimSpace(having) == "" {
		return having
	}
	pattern := regexp.MustCompile(`(?i)\b([a-z_]+)\s*\(`)
	type span struct{ start, closeParen int }
	var spans []span
	var calls []string
	for _, m := range pattern.FindAllStringSubmatchIndex(having, -1) {
		nm := strings.ToLower(having[m[2]:m[3]])
		fn, ok := functions.Get(nm)
		if !ok || fn.GetType() != functions.TypeAggregation {
			continue
		}
		openParen := m[1] - 1
		cp := findMatchingParenInternal(having, openParen)
		if cp < 0 {
			continue
		}
		spans = append(spans, span{m[0], cp})
		calls = append(calls, having[m[0]:cp+1])
	}
	if len(spans) == 0 {
		return having
	}
	repl := make([]string, len(calls))
	seq := 0
	for i, ac := range calls {
		if a, ok := selectAlias[ac]; ok && a != "" {
			repl[i] = a
			continue
		}
		if _, ok := aggs[ac]; ok {
			repl[i] = ac
			continue
		}
		aggType, name, _, _, perr := ParseAggregateTypeWithExpression(collapseSpacesOutsideQuotes(ac))
		if perr != nil || aggType == "" {
			repl[i] = ac // 解析失败原样保留（求值落空但不破坏文本）
			continue
		}
		hidden := fmt.Sprintf("__having_%d__", seq)
		seq++
		aggs[hidden] = aggType
		if name != "" {
			fieldMap[hidden] = name
		} else {
			fieldMap[hidden] = hidden
		}
		repl[i] = hidden
	}
	out := having
	for i := len(spans) - 1; i >= 0; i-- {
		s := spans[i]
		out = out[:s.start] + repl[i] + out[s.closeParen+1:]
	}
	return out
}

// validateWindowAnalyticArgs 校验窗口查询里分析函数参数不得引用裸原始列：
// 窗口产出行只含聚合与 GROUP BY 键，裸列取不到值会静默得到列名字符串。
// 允许：字面量、__winagg_ 隐藏聚合键、GROUP BY 键、函数调用、复杂表达式（含运算符）。
// 仅拦截"裸列名且非 GROUP BY 键"这一最常见误用。
func validateWindowAnalyticArgs(analyticFields []types.AnalyticField, groupKeys []string) error {
	keySet := make(map[string]bool, len(groupKeys))
	for _, k := range groupKeys {
		keySet[k] = true
	}
	for _, af := range analyticFields {
		for _, arg := range af.Args {
			a := strings.TrimSpace(arg)
			if a == "" || strings.HasPrefix(a, "__winagg_") {
				continue
			}
			if isLiteralToken(a) {
				continue
			}
			// 函数调用或含运算符的复杂表达式：聚合提取已处理 func()，此处不深判。
			if strings.ContainsAny(a, " ()<>=+-*/%") {
				continue
			}
			last := a
			if dot := strings.LastIndex(a, "."); dot >= 0 {
				last = a[dot+1:]
			}
			if !keySet[last] {
				return fmt.Errorf("analytic argument %q in a windowed query must reference an aggregate or a GROUP BY field, not a raw column", a)
			}
		}
	}
	return nil
}

// isLiteralToken 判断是否为字面量（数字/布尔/nil/引号字符串）。
func isLiteralToken(s string) bool {
	if s == "true" || s == "false" || s == "nil" {
		return true
	}
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		return true
	}
	if s == "" {
		return false
	}
	c := s[0]
	return (c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.'
}

// splitCallArgs 从函数调用文本中拆出顶层参数表达式片段（未求值）。
// 如 changed_cols("c_", true, temperature, humidity) → ["\"c_\"", "true", "temperature", "humidity"]。
// 解析失败或无参时返回 nil。
func splitCallArgs(expr string) []string {
	open := strings.IndexByte(expr, '(')
	if open < 0 {
		return nil
	}
	close := strings.LastIndexByte(expr, ')')
	if close <= open {
		return nil
	}
	body := expr[open+1 : close]
	return splitTopLevelCommas(body)
}

// splitTopLevelCommas 按顶层逗号拆分，忽略嵌套括号与字符串字面量内的逗号。
func splitTopLevelCommas(s string) []string {
	var args []string
	depth := 0
	last := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case '"':
			i++
			for i < len(s) && s[i] != '"' {
				i++
			}
		case '\'':
			i++
			for i < len(s) && s[i] != '\'' {
				i++
			}
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(s[last:i]))
				last = i + 1
			}
		}
	}
	tail := strings.TrimSpace(s[last:])
	if tail == "" && len(args) == 0 {
		return nil
	}
	args = append(args, tail)
	return args
}

// groupKeyIsScalarFunctionExpr reports whether expr is a function expression whose
// top-level function is a registered scalar (非聚合/分析/窗口) function。用于放行
// GROUP BY upper(device) 这类函数表达式分组键，同时拒绝拼错窗口函数的泄漏（如 Foo(5)）。
func groupKeyIsScalarFunctionExpr(expr string) bool {
	fn, exists := functions.Get(strings.ToLower(extractFunctionName(expr)))
	if !exists {
		return false
	}
	switch fn.GetType() {
	case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
		return false
	}
	return true
}

// Check if expression is an aggregation function
func isAggregationFunction(expr string) bool {
	// Extract function name
	funcName := extractFunctionName(expr)
	if funcName == "" {
		return false
	}

	// Check if it's a registered function
	if fn, exists := functions.Get(funcName); exists {
		// Determine if aggregation processing is needed based on function type
		switch fn.GetType() {
		case functions.TypeAggregation:
			// Aggregation function needs aggregation processing
			return true
		case functions.TypeAnalytical:
			// Analytical function also needs aggregation processing (state management)
			return true
		case functions.TypeWindow:
			// Window function needs aggregation processing
			return true

		default:
			// Other types of functions (string, conversion, etc.) don't need aggregation processing
			return false
		}
	}

	// For unregistered functions, check if it's expr-lang built-in function
	// These functions are handled through ExprBridge, don't need aggregation mode
	bridge := functions.GetExprBridge()
	if bridge.IsExprLangFunction(funcName) {
		return false
	}

	// If not registered function and not expr-lang function, but contains parentheses, conservatively assume it might be aggregation function
	if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
		return true
	}
	return false
}

// extractFieldOrder extracts original order of fields from Fields slice
// Returns field names list in order of appearance in SELECT statement
func extractFieldOrder(fields []Field) ([]string, error) {
	var fieldOrder []string

	for _, field := range fields {
		// If has alias, use alias as field name
		if field.Alias != "" {
			fieldOrder = append(fieldOrder, field.Alias)
		} else {
			// Without alias, try to parse expression to get field name
			_, fieldName, _, _, err := ParseAggregateTypeWithExpression(field.Expression)
			if err != nil {
				return nil, err
			}
			if fieldName != "" {
				// If parsed field name (like string literal), use parsed name
				fieldOrder = append(fieldOrder, fieldName)
			} else {
				// Otherwise use original expression as field name
				fieldOrder = append(fieldOrder, field.Expression)
			}
		}
	}

	return fieldOrder, nil
}
func extractGroupFields(s *SelectStatement) []string {
	var fields []string
	for _, f := range s.GroupBy {
		// 保留裸列与标量函数表达式（如 upper(device)）；只排除聚合函数当分组键（无意义）。
		if isAggregationFunction(f) {
			continue
		}
		fields = append(fields, f)
	}
	return fields
}

// buildSelectAliasMap maps each SELECT item's raw expression to its AS alias.
// Items without an alias are omitted. Used by the aggregation path to name
// output columns for grouped non-aggregate columns (e.g. "m.location AS loc"),
// matching the direct path.
func buildSelectAliasMap(fields []Field) map[string]string {
	m := make(map[string]string, len(fields))
	for _, f := range fields {
		if f.Alias != "" {
			m[f.Expression] = f.Alias
		}
	}
	return m
}

func buildSelectFields(fields []Field) (aggMap map[string]aggregator.AggregateType, fieldMap map[string]string, err error) {
	selectFields := make(map[string]aggregator.AggregateType)
	fieldMap = make(map[string]string)

	for _, f := range fields {
		if alias := f.Alias; alias != "" {
			t, n, _, _, parseErr := ParseAggregateTypeWithExpression(f.Expression)
			if parseErr != nil {
				return nil, nil, parseErr
			}
			if t != "" {
				// Use alias as key for aggregator, not field name
				selectFields[alias] = t

				// Field mapping: output field name(alias) -> input field name (consistent with buildSelectFieldsWithExpressions)
				if n != "" {
					fieldMap[alias] = n
				} else {
					// If no field name extracted, use alias itself
					fieldMap[alias] = alias
				}
			}
		} else {
			// Without alias, use expression itself as field name
			t, n, _, _, parseErr := ParseAggregateTypeWithExpression(f.Expression)
			if parseErr != nil {
				return nil, nil, parseErr
			}
			if t != "" && n != "" {
				selectFields[n] = t
				fieldMap[n] = n
			}
		}
	}
	return selectFields, fieldMap, nil
}

// detectNestedAggregation 检测表达式中是否存在聚合函数嵌套聚合函数的情况
// 如果发现嵌套聚合函数，返回错误信息
func detectNestedAggregation(expr string) error {
	return detectNestedAggregationRecursive(expr, false, false)
}

// detectNestedAggregationRecursive 递归检测嵌套聚合/分析函数。
// inAggregation：当前在真聚合（TypeAggregation）内部；inAnalytic：当前在分析函数内部。
// 规则：聚合套聚合 → 报错；分析套分析 → 报错；聚合套分析 → 报错；
//       分析套聚合 → 允许（如 changed_cols(avg(...))，分析函数对窗口聚合输出求值）。
func detectNestedAggregationRecursive(expr string, inAggregation, inAnalytic bool) error {
	pattern := regexp.MustCompile(`(?i)([a-z_]+)\s*\(`)
	matches := pattern.FindAllStringSubmatchIndex(expr, -1)

	for _, match := range matches {
		funcStart := match[0]
		funcName := strings.ToLower(expr[match[2]:match[3]])

		if fn, exists := functions.Get(funcName); exists {
			ft := fn.GetType()
			if ft == functions.TypeAggregation || ft == functions.TypeAnalytical || ft == functions.TypeWindow {
				switch ft {
				case functions.TypeAggregation:
					// 聚合函数内部不能再套聚合函数。
					if inAggregation {
						return fmt.Errorf("aggregate function calls cannot be nested")
					}
				case functions.TypeAnalytical:
					// 分析套分析、或聚合套分析 → 报错（分析函数只可包裹聚合）。
					if inAnalytic || inAggregation {
						return fmt.Errorf("analytic functions cannot be nested in %s", funcName)
					}
				case functions.TypeWindow:
					if inAggregation || inAnalytic {
						return fmt.Errorf("window function %s cannot be nested here", funcName)
					}
				}
				funcEnd := findMatchingParenInternal(expr, funcStart+len(funcName))
				if funcEnd > funcStart {
					paramStart := funcStart + len(funcName) + 1
					params := expr[paramStart:funcEnd]
					// 进入真聚合：标记 inAggregation；进入分析：标记 inAnalytic（不标记 inAggregation，
					// 这样分析函数内部允许再出现聚合，即"分析套聚合"）。
					nextAgg := inAggregation || ft == functions.TypeAggregation
					nextAna := inAnalytic || ft == functions.TypeAnalytical
					if err := detectNestedAggregationRecursive(params, nextAgg, nextAna); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// Parse aggregation function and return expression information
func ParseAggregateTypeWithExpression(exprStr string) (aggType aggregator.AggregateType, name string, expression string, allFields []string, err error) {
	// 首先检测是否存在嵌套聚合函数
	if err := detectNestedAggregation(exprStr); err != nil {
		// 如果发现嵌套聚合，返回错误
		return "", "", "", nil, err
	}

	// Special handling for CASE expressions
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(exprStr)), "CASE") {
		// CASE expressions are handled as special expressions
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", "", exprStr, allFields, nil
	}

	// Check if it's an expression containing operators with functions
	if containsOperatorsOutsideFunctions(exprStr) && containsFunctions(exprStr) {
		// This is a complex expression with functions and operators
		// Extract all fields referenced in the expression
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		// Return as expression type for post-aggregation evaluation
		return "expression", "", exprStr, allFields, nil
	}

	// Original logic for single function (moved up to prioritize outer function detection)
	// Extract function name
	funcName := extractFunctionName(exprStr)

	// Check if it's nested functions without operators
	hasNested := hasNestedFunctions(exprStr)
	if hasNested && funcName != "" {
		// For nested functions, check if the outer function is an aggregation function
		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				// Outer function is aggregation - handle as aggregation with expression parameter
				name, expression, allFields := extractAggFieldWithExpression(exprStr, funcName)

				return aggregator.AggregateType(funcName), name, expression, allFields, nil
			}
		}
		// Multiple functions but no operators and outer function is not aggregation - treat as expression
		if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", "", exprStr, allFields, nil
	}
	if funcName == "" {
		// Special handling for SELECT * case
		if strings.TrimSpace(exprStr) == "*" {
			return "", "", "", nil, nil // Don't treat * as expression
		}

		// Check if it's a string literal
		trimmed := strings.TrimSpace(exprStr)
		if (strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'")) ||
			(strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"")) {
			// String literal: use content without quotes as field name
			fieldName := trimmed[1 : len(trimmed)-1]
			return "expression", fieldName, exprStr, nil, nil
		}

		// If not a function call but contains operators or keywords, it might be an expression
		if strings.ContainsAny(exprStr, "+-*/<>=!&|") ||
			strings.Contains(strings.ToUpper(exprStr), "AND") ||
			strings.Contains(strings.ToUpper(exprStr), "OR") {
			// Handle as expression
			if parsedExpr, err := expr.NewExpression(exprStr); err == nil {
				allFields = parsedExpr.GetFields()
			}
			return "expression", "", exprStr, allFields, nil
		}
		return "", "", "", nil, nil
	}

	// Check if it's a registered function
	fn, exists := functions.Get(funcName)
	if !exists {
		return "", "", "", nil, nil
	}

	// Extract function parameters and expression information
	name, expression, allFields = extractAggFieldWithExpression(exprStr, funcName)

	// Determine aggregation type based on function type
	switch fn.GetType() {
	case functions.TypeAggregation:
		// Aggregation function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields, nil

	case functions.TypeAnalytical:
		// Analytical function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields, nil

	case functions.TypeWindow:
		// Window function: use function name as aggregation type
		return aggregator.AggregateType(funcName), name, expression, allFields, nil

	case functions.TypeString, functions.TypeConversion, functions.TypeCustom, functions.TypeMath:
		// String, conversion, custom, math functions: handle as expressions in aggregation queries
		// Use "expression" as special aggregation type, indicating this is an expression calculation
		// For these functions, should save complete function call as expression, not just parameter part
		fullExpression := exprStr
		if parsedExpr, err := expr.NewExpression(fullExpression); err == nil {
			allFields = parsedExpr.GetFields()
		}
		return "expression", name, fullExpression, allFields, nil

	default:
		// Other types of functions don't use aggregation
		// These functions will be handled directly in non-window mode
		return "", "", "", nil, nil
	}
}

// extractFunctionName extracts function name from expression
func extractFunctionName(expr string) string {
	// Find first left parenthesis
	parenIndex := strings.Index(expr, "(")
	if parenIndex == -1 {
		return ""
	}

	// Extract function name part
	funcName := strings.TrimSpace(expr[:parenIndex])

	// If function name contains other operators or spaces, it's not a simple function call
	if strings.ContainsAny(funcName, " +-*/=<>!&|") {
		return ""
	}

	return funcName
}

// Extract all function names from expression
func extractAllFunctions(expr string) []string {
	var funcNames []string

	// Simple function name matching
	i := 0
	for i < len(expr) {
		// Find function name pattern
		start := i
		for i < len(expr) && (expr[i] >= 'a' && expr[i] <= 'z' || expr[i] >= 'A' && expr[i] <= 'Z' || expr[i] == '_') {
			i++
		}

		if i < len(expr) && expr[i] == '(' && i > start {
			// Found possible function name
			funcName := expr[start:i]
			if _, exists := functions.Get(funcName); exists {
				funcNames = append(funcNames, funcName)
			}
		}

		if i < len(expr) {
			i++
		}
	}

	return funcNames
}

// Check if expression contains nested functions
func hasNestedFunctions(expr string) bool {
	funcs := extractAllFunctions(expr)
	return len(funcs) > 1
}

// containsOperators checks if expression contains arithmetic or comparison operators
func containsOperators(expr string) bool {
	return strings.ContainsAny(expr, "+-*/<>=!&|")
}

// containsFunctions checks if expression contains function calls
func containsFunctions(expr string) bool {
	funcs := extractAllFunctions(expr)
	return len(funcs) > 0
}

// Extract aggregation function fields and parse expression information
func extractAggFieldWithExpression(exprStr string, funcName string) (fieldName string, expression string, allFields []string) {

	start := strings.Index(strings.ToLower(exprStr), strings.ToLower(funcName)+"(")
	if start < 0 {
		return "", "", nil
	}
	start += len(funcName) + 1

	end := strings.LastIndex(exprStr, ")")
	if end <= start {
		return "", "", nil
	}

	// Extract expression within parentheses
	fieldExpr := strings.TrimSpace(exprStr[start:end])

	// Special handling for count(*) case
	if strings.ToLower(funcName) == "count" && fieldExpr == "*" {
		return "*", "", nil
	}

	// Check if it's a registered function and get its type
	if fn, exists := functions.Get(funcName); exists {
		// For string functions that need special parameter parsing
		if fn.GetType() == functions.TypeString {
			// Intelligently parse function parameters to extract field names
			var fields []string
			params := parseSmartParameters(fieldExpr)
			for _, param := range params {
				param = strings.TrimSpace(param)
				// If parameter is not string constant (not surrounded by quotes), consider it as field name
				if !((strings.HasPrefix(param, "'") && strings.HasSuffix(param, "'")) ||
					(strings.HasPrefix(param, "\"") && strings.HasSuffix(param, "\""))) {
					if isIdentifier(param) {
						fields = append(fields, param)
					}
				}
			}
			if len(fields) > 0 {
				// For string functions, save complete function call as expression
				// Return all extracted fields as allFields
				return fields[0], strings.ToLower(funcName) + "(" + fieldExpr + ")", fields
			}
			// If no field found, return empty field name but keep expression
			return "", strings.ToLower(funcName) + "(" + fieldExpr + ")", nil
		}
	}

	// Check if it's a multi-parameter function call (contains comma)
	if strings.Contains(fieldExpr, ",") {
		// For multi-parameter functions, extract the first parameter as the field name
		params := strings.Split(fieldExpr, ",")
		if len(params) > 0 {
			firstParam := strings.TrimSpace(params[0])
			// Return first parameter as field name, and full expression for parameter processing
			return firstParam, fieldExpr, nil
		}
	}

	// Check if it's a simple field name (only letters, numbers, underscores)
	isSimpleField := true
	for _, char := range fieldExpr {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			isSimpleField = false
			break
		}
	}

	// If simple field, return field name directly, don't create expression
	if isSimpleField {
		return fieldExpr, "", nil
	}

	// For complex expressions, including multi-parameter function calls
	expression = fieldExpr

	// Use expression engine to parse
	parsedExpr, err := expr.NewExpression(fieldExpr)
	if err != nil {
		// If expression parsing fails, try manual parameter parsing
		// This is mainly used to handle multi-parameter functions like distance(x1, y1, x2, y2)
		if strings.Contains(fieldExpr, ",") {
			// Split parameters
			params := strings.Split(fieldExpr, ",")
			var fields []string
			for _, param := range params {
				param = strings.TrimSpace(param)
				if isIdentifier(param) {
					fields = append(fields, param)
				}
			}
			if len(fields) > 0 {
				// For multi-parameter functions, use all parameter fields, main field name is first parameter
				return fields[0], expression, fields
			}
		}

		// If still fails to parse, try simple extraction method
		fieldName = extractSimpleField(fieldExpr)
		return fieldName, expression, []string{fieldName}
	}

	// Get all fields referenced in expression
	allFields = parsedExpr.GetFields()

	// If only one field, return directly
	if len(allFields) == 1 {
		return allFields[0], expression, allFields
	}

	// If multiple fields, use first field name as main field
	if len(allFields) > 0 {
		// Record complete expression and all fields
		return allFields[0], expression, allFields
	}

	// If no fields (pure constant expression), return entire expression as field name

	return fieldExpr, expression, nil
}

// parseSmartParameters intelligently parses function parameters, correctly handles commas within quotes
func parseSmartParameters(paramsStr string) []string {
	var params []string
	var current strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(paramsStr); i++ {
		ch := paramsStr[i]

		if !inQuotes {
			if ch == '\'' || ch == '"' {
				inQuotes = true
				quoteChar = ch
				current.WriteByte(ch)
			} else if ch == ',' {
				// Parameter separator
				params = append(params, current.String())
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		} else {
			if ch == quoteChar {
				inQuotes = false
				quoteChar = 0
			}
			current.WriteByte(ch)
		}
	}

	// Add the last parameter
	if current.Len() > 0 {
		params = append(params, current.String())
	}

	return params
}

// isIdentifier checks if string is a valid identifier
func isIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}
	// First character must be letter or underscore
	if !((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') || s[0] == '_') {
		return false
	}
	// Remaining characters must be letters, numbers, or underscores
	for i := 1; i < len(s); i++ {
		char := s[i]
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	return true
}

// extractSimpleField for backward compatibility
func extractSimpleField(fieldExpr string) string {
	// If contains operators, extract first operand as field name
	for _, op := range []string{"/", "*", "+", "-"} {
		if opIndex := strings.Index(fieldExpr, op); opIndex > 0 {
			return strings.TrimSpace(fieldExpr[:opIndex])
		}
	}
	return fieldExpr
}

// validateWindowParams validates and converts window parameters based on window type
// Returns validated parameters array with proper types
func validateWindowParams(params []any, windowType string) ([]any, error) {
	if len(params) == 0 {
		return params, nil
	}

	validated := make([]any, 0, len(params))

	if windowType == window.TypeCounting {
		// CountingWindow expects integer count as first parameter
		if len(params) == 0 {
			return nil, fmt.Errorf("counting window requires at least one parameter")
		}

		// Convert first parameter to int using cast utility
		count, err := cast.ToIntE(params[0])
		if err != nil {
			return nil, fmt.Errorf("invalid count parameter: %w", err)
		}

		if count <= 0 {
			return nil, fmt.Errorf("counting window count must be positive, got: %d", count)
		}

		validated = append(validated, count)

		// Add any additional parameters
		if len(params) > 1 {
			validated = append(validated, params[1:]...)
		}

		return validated, nil
	}

	// Helper function to convert a value to time.Duration
	// For numeric types, treats them as seconds
	// For strings, uses time.ParseDuration
	convertToDuration := func(val any) (time.Duration, error) {
		switch v := val.(type) {
		case time.Duration:
			return v, nil
		case string:
			// Use ToDurationE which handles string parsing
			return cast.ToDurationE(v)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			// Treat numeric integers as seconds
			return time.Duration(cast.ToInt(v)) * time.Second, nil
		case float32, float64:
			// Treat numeric floats as seconds
			return time.Duration(int(cast.ToFloat64(v))) * time.Second, nil
		default:
			// Try ToDurationE as fallback
			return cast.ToDurationE(v)
		}
	}

	if windowType == window.TypeSession {
		// SessionWindow expects timeout duration as first parameter
		if len(params) == 0 {
			return nil, fmt.Errorf("session window requires at least one parameter")
		}

		timeout, err := convertToDuration(params[0])
		if err != nil {
			return nil, fmt.Errorf("invalid timeout duration: %w", err)
		}

		if timeout <= 0 {
			return nil, fmt.Errorf("session window timeout must be positive, got: %v", timeout)
		}

		validated = append(validated, timeout)

		// Add any additional parameters
		if len(params) > 1 {
			validated = append(validated, params[1:]...)
		}

		return validated, nil
	}

	// For TumblingWindow and SlidingWindow, convert parameters to time.Duration
	for index, v := range params {
		dur, err := convertToDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid duration parameter at index %d: %w", index, err)
		}

		if dur <= 0 {
			return nil, fmt.Errorf("duration parameter at index %d must be positive, got: %v", index, dur)
		}

		validated = append(validated, dur)
	}

	return validated, nil
}

func parseAggregateExpression(expr string) string {
	if strings.Contains(expr, functions.AvgStr+"(") {
		return functions.AvgStr
	}
	if strings.Contains(expr, functions.SumStr+"(") {
		return functions.SumStr
	}
	if strings.Contains(expr, functions.MaxStr+"(") {
		return functions.MaxStr
	}
	if strings.Contains(expr, functions.MinStr+"(") {
		return functions.MinStr
	}
	return ""
}

// Parse field information including expressions with post-aggregation support
func buildSelectFieldsWithExpressions(fields []Field) (
	aggMap map[string]aggregator.AggregateType,
	fieldMap map[string]string,
	expressions map[string]types.FieldExpression,
	postAggExpressions []types.PostAggregationExpression,
	err error) {

	selectFields := make(map[string]aggregator.AggregateType)
	fieldMap = make(map[string]string)
	expressions = make(map[string]types.FieldExpression)
	postAggExpressions = make([]types.PostAggregationExpression, 0)

	for _, f := range fields {
		alias := f.Alias
		if alias == "" {
			// For string literals without alias, use the content without quotes as alias
			trimmed := strings.TrimSpace(f.Expression)
			if (strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'")) ||
				(strings.HasPrefix(trimmed, "\"") && strings.HasSuffix(trimmed, "\"")) {
				alias = trimmed[1 : len(trimmed)-1] // Remove quotes
			} else {
				alias = f.Expression
			}
		}

		// Check if this is a complex aggregation expression
		if isComplexAggregationExpression(f.Expression) {
			// Parse complex aggregation expression
			aggFields, exprTemplate, err := parseComplexAggregationExpression(f.Expression)
			if err == nil && len(aggFields) > 0 {
				// Add individual aggregation functions
				for _, aggField := range aggFields {
					selectFields[aggField.Placeholder] = aggField.AggType
					fieldMap[aggField.Placeholder] = aggField.InputField
				}

				// Add post-aggregation expression
				postAggExpressions = append(postAggExpressions, types.PostAggregationExpression{
					OutputField:        alias,
					OriginalExpr:       f.Expression,
					ExpressionTemplate: exprTemplate,
					RequiredFields:     aggFields,
				})

				// Mark the main field as post-aggregation
				selectFields[alias] = "post_aggregation"
				fieldMap[alias] = alias
				continue
			}
		}

		// Handle as regular expression
		t, n, expression, allFields, parseErr := ParseAggregateTypeWithExpression(f.Expression)
		if parseErr != nil {
			// 如果检测到嵌套聚合函数，返回错误
			return nil, nil, nil, nil, parseErr
		}
		if t != "" {
			// Check if this is a multi-parameter function that needs special handling
			isMultiParamFunction := false
			if expression != "" && strings.Contains(expression, ",") {
				// Check if the function needs multi-parameter handling
				funcName := extractFunctionName(f.Expression)
				if fn, exists := functions.Get(funcName); exists {
					minArgs := fn.GetMinArgs()
					maxArgs := fn.GetMaxArgs()
					// Function needs multi-parameter handling if it has multiple parameters
					isMultiParamFunction = minArgs > 1 || (maxArgs > minArgs && minArgs >= 1)
				}
			}

			// For multi-parameter functions, treat as post-aggregation expression
			if isMultiParamFunction {
				// Parse as single aggregation function with parameters
				aggFields := []types.AggregationFieldInfo{{
					FuncName:    extractFunctionName(f.Expression),
					InputField:  n,
					Placeholder: "__" + extractFunctionName(f.Expression) + "_" + alias + "__",
					AggType:     aggregator.AggregateType(extractFunctionName(f.Expression)),
					FullCall:    f.Expression,
				}}

				// Add the aggregation function
				selectFields[aggFields[0].Placeholder] = aggFields[0].AggType
				fieldMap[aggFields[0].Placeholder] = aggFields[0].InputField

				// Add post-aggregation expression (which just returns the placeholder value)
				postAggExpressions = append(postAggExpressions, types.PostAggregationExpression{
					OutputField:        alias,
					OriginalExpr:       f.Expression,
					ExpressionTemplate: aggFields[0].Placeholder,
					RequiredFields:     aggFields,
				})

				// Mark the main field as post-aggregation
				selectFields[alias] = "post_aggregation"
				fieldMap[alias] = alias
				continue
			}

			// Use alias as key so each aggregation function has unique key
			selectFields[alias] = t

			// Field mapping: output field name -> input field name (prepare correct mapping for aggregator)
			if n != "" {
				fieldMap[alias] = n
			} else {
				// If no field name extracted, use alias itself
				fieldMap[alias] = alias
			}

			// If expression exists, save expression information
			if expression != "" {
				expressions[alias] = types.FieldExpression{
					Field:      n,
					Expression: expression,
					Fields:     allFields,
				}
			}
		}
	}
	return selectFields, fieldMap, expressions, postAggExpressions, nil
}

// isComplexAggregationExpression checks if an expression contains multiple aggregation functions or operators with aggregation functions
func isComplexAggregationExpression(expr string) bool {
	// Check if expression contains aggregation functions
	funcs := extractAllFunctions(expr)
	aggCount := 0
	nonAggCount := 0

	for _, funcName := range funcs {
		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				aggCount++
			default:
				nonAggCount++
			}
		} else {
			nonAggCount++
		}
	}

	// Determine the outermost function name (if any)
	outerFuncName := ""
	if m := regexp.MustCompile(`(?i)^\s*([a-z_][a-z0-9_]*)\s*\(`).FindStringSubmatch(expr); len(m) == 2 {
		outerFuncName = strings.ToLower(m[1])
	}
	outerIsAggregation := false
	if outerFuncName != "" {
		if fn, ok := functions.Get(outerFuncName); ok {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				outerIsAggregation = true
			}
		}
	}

	// Special case: single aggregation function with nested expression (only when OUTER is aggregation)
	isSingleAggWithNestedFunc := false
	if aggCount == 1 && outerIsAggregation {
		start := strings.Index(expr, "(")
		end := strings.LastIndex(expr, ")")
		if start != -1 && end != -1 && end > start {
			innerExpr := strings.TrimSpace(expr[start+1 : end])
			if !containsOperators(innerExpr) {
				isSingleAggWithNestedFunc = true
			}
		}
	}

	result := (aggCount > 1) ||
		(aggCount > 0 && containsOperatorsOutsideFunctions(expr) && !isSingleAggWithNestedFunc) ||
		(aggCount > 0 && nonAggCount > 0 && !isSingleAggWithNestedFunc)

	return result
}

// containsOperatorsOutsideFunctions checks if expression contains operators outside function calls
func containsOperatorsOutsideFunctions(expr string) bool {
	// Remove function calls first, then check for operators
	// Simple approach: if it's just a single function call, it shouldn't be treated as complex
	trimmed := strings.TrimSpace(expr)

	// If it starts with a function name and ends with ), it's likely a simple function call
	if match := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\s*\([^)]*\)$`).FindString(trimmed); match == trimmed {
		return false
	}

	// Check for operators
	return containsOperators(expr)
}

// parseComplexAggregationExpression parses expressions containing multiple aggregation functions
func parseComplexAggregationExpression(expr string) ([]types.AggregationFieldInfo, string, error) {
	return parseComplexAggExpressionInternal(expr)
}

// parseComplexAggExpressionInternal implements the actual parsing logic
func parseComplexAggExpressionInternal(expr string) ([]types.AggregationFieldInfo, string, error) {
	// 首先检测嵌套聚合
	if err := detectNestedAggregation(expr); err != nil {
		return nil, "", err
	}

	// 使用改进的递归解析方法
	aggFields, exprTemplate := parseNestedFunctionsInternal(expr, make([]types.AggregationFieldInfo, 0))
	return aggFields, exprTemplate, nil
}

// parseNestedFunctionsInternal 递归解析嵌套函数调用
func parseNestedFunctionsInternal(expr string, aggFields []types.AggregationFieldInfo) ([]types.AggregationFieldInfo, string) {
	// 匹配函数调用，支持大小写不敏感
	pattern := regexp.MustCompile(`(?i)([a-z_]+)\s*\(`)

	// 找到所有函数调用的起始位置
	matches := pattern.FindAllStringSubmatchIndex(expr, -1)
	if len(matches) == 0 {
		return aggFields, expr
	}

	// 从右到左处理，避免索引偏移问题
	for i := len(matches) - 1; i >= 0; i-- {
		match := matches[i]
		funcStart := match[0]
		funcName := strings.ToLower(expr[match[2]:match[3]])

		// 找到匹配的右括号
		parenStart := match[3]
		parenEnd := findMatchingParenInternal(expr, parenStart)
		if parenEnd == -1 {
			continue
		}

		fullFuncCall := expr[funcStart : parenEnd+1]
		funcParam := expr[parenStart+1 : parenEnd]

		// 检查是否是聚合函数
		if fn, exists := functions.Get(funcName); exists {
			switch fn.GetType() {
			case functions.TypeAggregation, functions.TypeAnalytical, functions.TypeWindow:
				// 生成唯一占位符
				callHash := 0
				for _, c := range fullFuncCall {
					callHash = callHash*31 + int(c)
				}
				if callHash < 0 {
					callHash = -callHash
				}
				placeholder := fmt.Sprintf("__%s_%d__", funcName, callHash)

				// 解析函数参数
				inputField := strings.TrimSpace(funcParam)
				// 对于聚合函数，如果参数包含嵌套函数调用，保留完整参数
				// 只有在参数是简单的逗号分隔列表时才进行分割
				if strings.Contains(funcParam, ",") && !containsNestedFunctions(funcParam) {
					params := strings.Split(funcParam, ",")
					if len(params) > 0 {
						inputField = strings.TrimSpace(params[0])
					}
				}

				// 添加到聚合字段列表
				fieldInfo := types.AggregationFieldInfo{
					FuncName:    funcName,
					InputField:  inputField,
					Placeholder: placeholder,
					AggType:     aggregator.AggregateType(funcName),
					FullCall:    fullFuncCall,
				}
				aggFields = append(aggFields, fieldInfo)

				// 替换表达式中的聚合函数调用
				expr = expr[:funcStart] + placeholder + expr[parenEnd+1:]
			}
		}
	}

	return aggFields, expr
}

// containsNestedFunctions 检查参数字符串是否包含嵌套函数调用
func containsNestedFunctions(param string) bool {
	// 简单检查：如果包含函数名模式后跟括号，则认为是嵌套函数
	pattern := regexp.MustCompile(`[a-zA-Z_][a-zA-Z0-9_]*\s*\(`)
	return pattern.MatchString(param)
}

// findMatchingParenInternal 找到匹配的右括号
func findMatchingParenInternal(s string, start int) int {
	if start >= len(s) || s[start] != '(' {
		return -1
	}

	count := 1
	for i := start + 1; i < len(s); i++ {
		switch s[i] {
		case '(':
			count++
		case ')':
			count--
			if count == 0 {
				return i
			}
		}
	}
	return -1 // 未找到匹配的右括号
}
