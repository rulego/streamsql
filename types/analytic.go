package types

import "strconv"

// AnalyticSelfToken 是"表达式包分析函数"回代模板里的占位（如 ts - __analytic_self__）：
// 求值期把分析函数结果写入行的该键，再求整个外层表达式。
const AnalyticSelfToken = "__analytic_self__"

// AnalyticSelfTokenN 返回表达式内第 i 个分析调用在回代模板里的占位标识符。
// i==0 沿用 AnalyticSelfToken（单调用向后兼容），i>0 用 __analytic_self_<i>__。
// 同一表达式含多个分析调用时（如 acc_max(v) - acc_min(v)），每个调用各分配一个占位。
func AnalyticSelfTokenN(i int) string {
	if i == 0 {
		return AnalyticSelfToken
	}
	return "__analytic_self_" + strconv.Itoa(i) + "__"
}

// AnalyticCall 描述"表达式包分析函数"里的单个分析调用（一个字段可含多个，如 acc_max(v) - acc_min(v)）。
type AnalyticCall struct {
	FuncName string   // 函数名，如 "acc_max"
	BareCall string   // 完整调用文本（不含 OVER），如 "acc_max(v)"
	Args     []string // 原始参数表达式片段（未求值），如 ["v"]
}

// OverSpec 描述分析函数的 OVER 子句。
// 仅支持 PARTITION BY 和 WHEN，不支持 ORDER BY / ROWS frame（那是 Flink 模型）。
type OverSpec struct {
	PartitionBy []string // 分区字段，状态按分区独立维护
	When        string   // WHEN 条件表达式；满足才更新状态，否则复用旧值
}

// AnalyticField 描述 SELECT 中的分析函数字段（带可选 OVER）。
// 走直连路径（EmitSync），由流级状态机逐条求值，不进聚合路径。
type AnalyticField struct {
	FuncName string    // 函数名，如 "lag"
	Args     []string  // 原始参数表达式片段（未求值），如 ["temp","1"]
	Expression string  // 完整调用文本（不含 OVER），如 "lag(temp, 1)"
	Alias    string    // 输出列名（多列函数仅作内部句柄，实际列名由结果决定）
	Over     *OverSpec // OVER 子句，nil 表示无
	// MultiColumn 标记多列动态输出函数（changed_cols）：其求值结果为 map[colname]value，
	// 投影时按 prefix+colname 扇出为多个输出列。仅 SELECT。
	MultiColumn bool
	// WrapperExpr 外层算术/表达式回代模板：当字段是"表达式包分析函数"（如 ts - lag(ts)）
	// 时，分析调用子串被替换为占位 __analytic_self__，得 "ts - __analytic_self__"。
	// 求值期先算出分析函数值，代入占位再求整个表达式。空表示纯分析字段。
	WrapperExpr string
	// Calls 表达式内全部 Analytic 调用（按出现顺序）。纯单调用字段长度为 1；
	// 多调用（acc_max(v) - acc_min(v)）长度 ≥ 2，每个调用在 WrapperExpr 里对应
	// types.AnalyticSelfTokenN(i) 占位。FuncName/Args/Expression 仍保留首个调用值供旧路径。
	Calls []AnalyticCall
	// InlineAggDisplay 窗口查询里分析函数参数内联聚合的重写映射：隐藏键→显示名。
	// 如 changed_cols("t",true,avg(temperature)) 在窗口查询里，avg(temperature) 被提取为
	// 隐藏计算字段 __winagg_0__，这里记录 {"__winagg_0__":"avg"}，输出列名取显示名（→ tavg）。
	InlineAggDisplay map[string]string
}

// WhereAnalyticCall 描述 WHERE 条件中出现的分析函数调用。
// 解析期从 WHERE 文本提取，分配占位符；求值期在 WHERE 之前算出值，
// 注入 dataMap[Placeholder]，WHERE 文本中的原始调用已被替换为占位符。
type WhereAnalyticCall struct {
	Placeholder string    // 合成键，如 "__analytic_0__"
	FuncName    string    // 函数名
	Args        []string  // 原始参数片段
	Expression  string    // 完整调用文本（不含 OVER）
	Over        *OverSpec // OVER 子句，nil 表示无
}
